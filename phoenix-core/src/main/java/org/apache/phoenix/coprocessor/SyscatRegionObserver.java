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
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.ViewIndexIdRetrieveUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import java.io.IOException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE_BYTES;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.NULL_DATA_TYPE_VALUE;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.SYSCATA_COPROC_IGNORE_TAG;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN;

/**
 * Coprocessor that checks whether the VIEW_INDEX_ID needs to retrieve.
 */
public class SyscatRegionObserver extends BaseRegionObserver {
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
        private final boolean ignore;

        public SyscatRegionScanner(RegionCoprocessorEnvironment env, Scan scan,
                                   RegionScanner scanner) throws IOException {
            this.scan = scan;
            this.scanner = scanner;
            this.region = env.getRegion();
            this.ignore = this.scan.getAttribute(SYSCATA_COPROC_IGNORE_TAG) != null;

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
            return doHBaseScanNext(result, false);
        }

        @Override public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
            return doHBaseScanNext(result, true);
        }

        // we don't want to modify the cell when we have the HBase scan
        private boolean doHBaseScanNext(List<Cell> result, boolean raw) throws IOException {
            boolean hasMore;
            try {
                hasMore = raw ? scanner.nextRaw(result) : scanner.next(result);
                if (result.isEmpty()) {
                    return false;
                }
            }catch (Throwable t) {
                ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
            }

            return hasMore;
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
                if (!ignore) {
                    Cell viewIndexIdCell = KeyValueUtil.getColumnLatest(
                            GenericKeyValueBuilder.INSTANCE, result,
                            DEFAULT_COLUMN_FAMILY_BYTES, VIEW_INDEX_ID_BYTES);
                    if (viewIndexIdCell != null) {
                        int type = NULL_DATA_TYPE_VALUE;
                        Cell viewIndexIdDataTypeCell = KeyValueUtil.getColumnLatest(
                                GenericKeyValueBuilder.INSTANCE, result,
                                DEFAULT_COLUMN_FAMILY_BYTES, VIEW_INDEX_ID_DATA_TYPE_BYTES);
                        if (viewIndexIdDataTypeCell != null) {
                            type = (Integer) PInteger.INSTANCE.toObject(
                                    viewIndexIdDataTypeCell.getValueArray(),
                                    viewIndexIdDataTypeCell.getValueOffset(),
                                    viewIndexIdDataTypeCell.getValueLength(),
                                    PInteger.INSTANCE,
                                    SortOrder.ASC);
                        }
                        if (ScanUtil.getClientVersion(this.scan) < MIN_SPLITTABLE_SYSTEM_CATALOG) {
                            // pre-splittable client should always using SMALLINT
                            if (type == NULL_DATA_TYPE_VALUE && viewIndexIdCell.getValueLength() >
                                    VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN) {
                                Cell keyValue = ViewIndexIdRetrieveUtil.
                                        getViewIndexIdKeyValueInShortDataFormat(viewIndexIdCell);
                                Collections.replaceAll(result, viewIndexIdCell, keyValue);
                            }
                        } else {
                            // post-splittable client should always using BIGINT
                            if (type != Types.BIGINT && viewIndexIdCell.getValueLength() <
                                    VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN) {
                                Cell keyValue = ViewIndexIdRetrieveUtil.
                                        getViewIndexIdKeyValueInLongDataFormat(viewIndexIdCell);
                                Collections.replaceAll(result, viewIndexIdCell, keyValue);
                            }
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