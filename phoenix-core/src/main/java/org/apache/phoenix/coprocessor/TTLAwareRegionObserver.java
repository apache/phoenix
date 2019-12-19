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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.filter.EncodedQualifiersColumnProjectionFilter;
import org.apache.phoenix.filter.MultiEncodedCQKeyValueComparisonFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.query.QueryConstants.ENCODED_EMPTY_COLUMN_NAME;

/**
 * 
 * Coprocessor that checks whether the row is expired based on the TTL spec.
 *
 */
public class TTLAwareRegionObserver extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(TTLAwareRegionObserver.class);

    /**
     * A region scanner that checks the TTL expiration of rows
     */
    private static class TTLAwareRegionScanner implements RegionScanner {
        RegionScanner scanner;
        private Scan scan;
        private byte[] emptyCF;
        private byte[] emptyCQ;
        private Region region;
        private long minTimestamp;
        private long maxTimestamp;
        private long now;
        private boolean deleteIfExpired;
        private boolean maskIfExpired;

        public TTLAwareRegionScanner(RegionCoprocessorEnvironment env,
                                  Scan scan,
                                  RegionScanner scanner) throws IOException {
            this.scan = scan;
            this.scanner = scanner;

            deleteIfExpired = ScanUtil.isDeleteTTLExpiredRows(scan) ? true : false;
            maskIfExpired = !deleteIfExpired && ScanUtil.isMaskTTLExpiredRows(scan) ? true : false;;

            region = env.getRegion();
            emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
            emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
            byte[] txnScn = scan.getAttribute(BaseScannerRegionObserver.TX_SCN);
            if (txnScn!=null) {
                TimeRange timeRange = scan.getTimeRange();
                scan.setTimeRange(timeRange.getMin(), Bytes.toLong(txnScn));
            }
            minTimestamp = scan.getTimeRange().getMin();
            maxTimestamp = scan.getTimeRange().getMax();
            now = maxTimestamp != HConstants.LATEST_TIMESTAMP ? maxTimestamp : EnvironmentEdgeManager.currentTimeMillis();
        }

        @Override
        public int getBatch() {
            return scanner.getBatch();
        }

        @Override
        public long getMaxResultSize() {
            return scanner.getMaxResultSize();
        }

        @Override
        public boolean next(List<Cell> result) throws IOException {
            try {
                boolean hasMore;
                do {
                    hasMore = scanner.next(result);
                    if (result.isEmpty()) {
                        break;
                    }

                    if (maskIfExpired && checkRowNotExpired(result)) {
                        break;
                    }

                    if (deleteIfExpired && deleteRowIfExpired(result)) {
                        break;
                    }
                    // skip this row
                    // 1. if the row has expired (checkRowNotExpired returned false)
                    // 2. if the row was not deleted (deleteRowIfExpired returned false and
                    //  do not want it to count towards the deleted count)
                    result.clear();
                } while (hasMore);
                return hasMore;
            } catch (Throwable t) {
                ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
            }
        }

        @Override
        public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
            throw new IOException("next with scannerContext should not be called in Phoenix environment");
        }

        @Override
        public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
            throw new IOException("NextRaw with scannerContext should not be called in Phoenix environment");
        }

        @Override
        public void close() throws IOException {
            scanner.close();
        }

        @Override
        public HRegionInfo getRegionInfo() {
            return scanner.getRegionInfo();
        }

        @Override
        public boolean isFilterDone() throws IOException {
            return scanner.isFilterDone();
        }

        @Override
        public boolean reseek(byte[] row) throws IOException {
            return scanner.reseek(row);
        }

        @Override
        public long getMvccReadPoint() {
            return scanner.getMvccReadPoint();
        }

        @Override
        public boolean nextRaw(List<Cell> result) throws IOException {
            try {
                boolean hasMore;
                do {
                    hasMore = scanner.nextRaw(result);
                    if (result.isEmpty()) {
                        break;
                    }
                    if (maskIfExpired && checkRowNotExpired(result)) {
                        break;
                    }

                    if (deleteIfExpired && deleteRowIfExpired(result)) {
                        break;
                    }
                    // skip this row
                    // 1. if the row has expired (checkRowNotExpired returned false)
                    // 2. if the row was not deleted (deleteRowIfExpired returned false and
                    //  do not want it to count towards the deleted count)
                    result.clear();
                } while (hasMore);
                return hasMore;
            } catch (Throwable t) {
                ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
            }
        }

        /**
         * @param cellList is an input and output parameter and will either include a valid row or be an empty list
         * @return true if row expired and deleted or empty, otherwise false
         * @throws IOException
         */
        private boolean deleteRowIfExpired(List<Cell> cellList) throws IOException {

            long cellListSize = cellList.size();
            if (cellListSize == 0) {
                return true;
            }

            Iterator<Cell> cellIterator = cellList.iterator();
            Cell firstCell = cellIterator.next();
            byte[] rowKey = new byte[firstCell.getRowLength()];
            System.arraycopy(firstCell.getRowArray(), firstCell.getRowOffset(), rowKey, 0, firstCell.getRowLength());

            boolean isRowExpired = !checkRowNotExpired(cellList);
            if (isRowExpired) {
                long ttl = ScanUtil.getViewTTL(this.scan) ;
                long ts = getMaxTimestamp(cellList);
                LOG.debug(String.format("***** VIEW-TTL: Deleting region = %s, row = %s, delete-ts = %d, max-ts = %d ****** ",
                        region.getRegionInfo().getTable().getNameAsString(),
                        Bytes.toString(rowKey),
                        now-ttl, ts));
                Delete del = new Delete(rowKey, now-ttl);
                Mutation[] mutations = new Mutation[]{del};
                region.batchMutate(mutations, HConstants.NO_NONCE, HConstants.NO_NONCE);
                return true;
            }
            return false;
        }


        private boolean isEmptyColumn(Cell cell) {
            return Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                    emptyCF, 0, emptyCF.length) == 0 &&
                    Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                            emptyCQ, 0, emptyCQ.length) == 0;
        }

        // TODO : Remove it after we verify all SQLs include the empty column.
        // Before we added ScanUtil.addEmptyColumnToScan some queries like select count(*) did not include
        // the empty column in scan, thus this method was the fallback in those cases.
        private boolean checkEmptyColumnNotExpired(byte[] rowKey) throws IOException {
            LOG.warn("Scan " + scan + " did not return the empty column for " + region.getRegionInfo().getTable().getNameAsString());
            Get get = new Get(rowKey);
            get.setTimeRange(minTimestamp, maxTimestamp);
            get.addColumn(emptyCF, emptyCQ);
            Result result = region.get(get);
            if (result.isEmpty()) {
                LOG.warn("The empty column does not exist in a row in " + region.getRegionInfo().getTable().getNameAsString());
                return false;
            }
            return !isTTLExpired(result.getColumnLatestCell(emptyCF, emptyCQ));
        }

        /**
         * @param cellList is an input and output parameter and will either include a valid row or be an empty list
         * @return true if row not expired, otherwise false
         * @throws IOException
         */
        private boolean checkRowNotExpired(List<Cell> cellList) throws IOException {
            long cellListSize = cellList.size();
            Cell cell = null;
            if (cellListSize == 0) {
                return true;
            }
            Iterator<Cell> cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                cell = cellIterator.next();
                if (isEmptyColumn(cell)) {
                    LOG.debug(String.format("**********VIEW-TTL: Row expired for [%s], expired = %s ***************", cell.toString(), isTTLExpired(cell)));
                    // Empty column is not supposed to be returned to the client except it is the only column included
                    // in the scan
                    if (cellListSize > 1) {
                        cellIterator.remove();
                    }
                    return !isTTLExpired(cell);
                }
            }
            byte[] rowKey = new byte[cell.getRowLength()];
            System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowKey, 0, cell.getRowLength());
            return checkEmptyColumnNotExpired(rowKey);
        }

        private long getMaxTimestamp(List<Cell> cellList) {
            long maxTs = 0;
            long ts = 0;
            Iterator<Cell> cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                ts = cell.getTimestamp();
                if (ts > maxTs) {
                    maxTs = ts;
                }
            }
            return maxTs;
        }

        private boolean isTTLExpired(Cell cell) {
            long ts = cell.getTimestamp();
            long ttl = ScanUtil.getViewTTL(this.scan) ;
            if (ts + ttl < now) {
                return true;
            }
            return false;
        }
    }



    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
                                         Scan scan, RegionScanner s) throws IOException {

        if (!ScanUtil.isMaskTTLExpiredRows(scan) && !ScanUtil.isDeleteTTLExpiredRows(scan)) {
            return s;
        }

        LOG.debug(String.format(
                "********** VIEW-TTL: TTLAwareRegionObserver::postScannerOpen TTL for table = [%s], scan = [%s], VIEW_TTL = %d ***************",
                s.getRegionInfo().getTable().getNameAsString(),
                scan.toJSON(Integer.MAX_VALUE),
                ScanUtil.getViewTTL(scan)));
        return new TTLAwareRegionScanner(c.getEnvironment(), scan, s);
    }
}
