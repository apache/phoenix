/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.ViewIndexIdRetrieveUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import java.io.IOException;
import java.sql.Types;
import java.util.List;

import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;

/**
 * Coprocessor that checks whether the VIEW_INDEX_ID needs to retrieve.
 */
public class SyscatRegionObserver extends BaseRegionObserver {
    public static final String VIEW_INDEX_ID_CQ = DEFAULT_COLUMN_FAMILY + ":" + VIEW_INDEX_ID;
    public static final String VIEW_INDEX_ID_DATA_TYPE_CQ =
            DEFAULT_COLUMN_FAMILY + ":" + VIEW_INDEX_ID_DATA_TYPE;
    public static final int VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN = 9;
    public static final int VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN = 3;
    public static final int OFF_RANGE_POS = -1;
    public static final int NULL_DATA_TYPE_VALUE = 0;

    @Override public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
    }

    @Override public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
                                         RegionScanner s) throws IOException {
        return new SyscatRegionScanner(c.getEnvironment(), scan, s);
    }

    /**
     * A region scanner that retrieve the right data type back to the client
     */
    private static class SyscatRegionScanner implements RegionScanner {

        private final RegionScanner scanner;
        private final Scan scan;
        private final Region region;

        public SyscatRegionScanner(RegionCoprocessorEnvironment env, Scan scan,
                                   RegionScanner scanner) throws IOException {
            this.scan = scan;
            this.scanner = scanner;
            region = env.getRegion();

            byte[] txnScn = scan.getAttribute(BaseScannerRegionObserver.TX_SCN);
            if (txnScn != null) {
                TimeRange timeRange = scan.getTimeRange();
                scan.setTimeRange(timeRange.getMin(), Bytes.toLong(txnScn));
            }
        }

        @Override public int getBatch() {
            return scanner.getBatch();
        }

        @Override public long getMaxResultSize() {
            return scanner.getMaxResultSize();
        }

        @Override public boolean next(List<Cell> result) throws IOException {
            return doNext(result, false);
        }

        @Override public boolean next(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
            throw new IOException(
                    "next with scannerContext should not be called in Phoenix environment");
        }

        @Override public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
            throw new IOException(
                    "NextRaw with scannerContext should not be called in Phoenix environment");
        }

        @Override public void close() throws IOException {
            scanner.close();
        }

        @Override public HRegionInfo getRegionInfo() {
            return scanner.getRegionInfo();
        }

        @Override public boolean isFilterDone() throws IOException {
            return scanner.isFilterDone();
        }

        @Override public boolean reseek(byte[] row) throws IOException {
            return scanner.reseek(row);
        }

        @Override public long getMvccReadPoint() {
            return scanner.getMvccReadPoint();
        }

        @Override public boolean nextRaw(List<Cell> result) throws IOException {
            return doNext(result, true);
        }

        private boolean doNext(List<Cell> result, boolean raw) throws IOException {
            boolean hasMore;
            try {
                hasMore = raw ? scanner.nextRaw(result) : scanner.next(result);
                if (result.isEmpty()) {
                    return false;
                }
                // logic to change the cell
                int pos = OFF_RANGE_POS;
                int type = NULL_DATA_TYPE_VALUE;
                for (int i = 0; i < result.size(); i++) {
                    Cell cell = result.get(i);
                    if (cell.toString().contains(VIEW_INDEX_ID_DATA_TYPE_CQ)) {
                        if (cell.getValueArray().length > 0) {
                            type = (Integer) PInteger.INSTANCE.toObject(
                                    cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength(), PInteger.INSTANCE, SortOrder.ASC);
                        }
                    } else if (cell.toString().contains(VIEW_INDEX_ID_CQ)) {
                        pos = i;
                    }
                }
                if (pos != OFF_RANGE_POS) {
                    // only 3 cases VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE, CLIENT
                    //                SMALLINT,        NULL,                PRE-4.15
                    //                SMALLINT,         5,                  POST-4.15
                    //                 BIGINT,         -5,                  POST-4.15

                    // if VIEW_INDEX_ID IS presenting
                    if (ScanUtil.getClientVersion(this.scan) < MIN_SPLITTABLE_SYSTEM_CATALOG) {
                        // pre-splittable client should always using SMALLINT
                        if (type == NULL_DATA_TYPE_VALUE && result.get(pos).getValueLength() >
                                VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN) {
                            Cell keyValue = ViewIndexIdRetrieveUtil.
                                    getViewIndexIdKeyValueInShortDataFormat(result.get(pos));
                            result.set(pos, keyValue);
                        }
                    } else {
                        // post-splittable client should always using BIGINT
                        if (type != Types.BIGINT && result.get(pos).getValueLength() <
                                VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN) {
                            Cell keyValue = ViewIndexIdRetrieveUtil.
                                    getViewIndexIdKeyValueInLongDataFormat(result.get(pos));
                            result.set(pos, keyValue);
                        }
                    }
                }
            } catch (Throwable t) {
                ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
            }
            return hasMore;
        }
    }
}