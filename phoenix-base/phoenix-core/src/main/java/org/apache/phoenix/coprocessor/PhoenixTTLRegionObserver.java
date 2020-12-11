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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.coprocessor.metrics.MetricsPhoenixTTLSource;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME;

/**
 * Coprocessor that checks whether the row is expired based on the TTL spec.
 */
public class PhoenixTTLRegionObserver extends BaseRegionObserver {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixTTLRegionObserver.class);
    private MetricsPhoenixTTLSource metricSource;

    @Override public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        metricSource = MetricsPhoenixCoprocessorSourceFactory.getInstance().getPhoenixTTLSource();
    }

    @Override public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
            RegionScanner s) throws IOException {

        if (!ScanUtil.isMaskTTLExpiredRows(scan) && !ScanUtil.isDeleteTTLExpiredRows(scan)) {
            return s;
        } else if (ScanUtil.isMaskTTLExpiredRows(scan) && ScanUtil.isDeleteTTLExpiredRows(scan)) {
            throw new IOException("Both mask and delete expired rows property cannot be set");
        } else if (ScanUtil.isMaskTTLExpiredRows(scan)) {
            metricSource.incrementMaskExpiredRequestCount();
            scan.setAttribute(PhoenixTTLRegionScanner.MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR,
                    Bytes.toBytes(String.format("MASK-EXPIRED-%d",
                            metricSource.getMaskExpiredRequestCount())));
        } else if (ScanUtil.isDeleteTTLExpiredRows(scan)) {
            metricSource.incrementDeleteExpiredRequestCount();
            scan.setAttribute(PhoenixTTLRegionScanner.MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR,
                    Bytes.toBytes(String.format("DELETE-EXPIRED-%d",
                            metricSource.getDeleteExpiredRequestCount())));
        }
        LOG.trace(String.format(
                "********** PHOENIX-TTL: PhoenixTTLRegionObserver::postScannerOpen TTL for table = "
                        + "[%s], scan = [%s], PHOENIX_TTL = %d ***************, "
                        + "numMaskExpiredRequestCount=%d, "
                        + "numDeleteExpiredRequestCount=%d",
                s.getRegionInfo().getTable().getNameAsString(),
                scan.toJSON(Integer.MAX_VALUE),
                ScanUtil.getPhoenixTTL(scan),
                metricSource.getMaskExpiredRequestCount(),
                metricSource.getDeleteExpiredRequestCount()
        ));
        return new PhoenixTTLRegionScanner(c.getEnvironment(), scan, s);
    }

    /**
     * A region scanner that checks the TTL expiration of rows
     */
    private static class PhoenixTTLRegionScanner implements RegionScanner {
        private static final String MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR =
                "MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID";

        private final RegionScanner scanner;
        private final Scan scan;
        private final byte[] emptyCF;
        private final byte[] emptyCQ;
        private final Region region;
        private final long minTimestamp;
        private final long maxTimestamp;
        private final long now;
        private final boolean deleteIfExpired;
        private final boolean maskIfExpired;
        private final String requestId;
        private final byte[] scanTableName;
        private long numRowsExpired;
        private long numRowsScanned;
        private long numRowsDeleted;
        private boolean reported = false;

        public PhoenixTTLRegionScanner(RegionCoprocessorEnvironment env, Scan scan,
                RegionScanner scanner) throws IOException {
            this.scan = scan;
            this.scanner = scanner;
            byte[] requestIdBytes = scan.getAttribute(MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR);
            this.requestId = Bytes.toString(requestIdBytes);

            deleteIfExpired = ScanUtil.isDeleteTTLExpiredRows(scan);
            maskIfExpired = !deleteIfExpired && ScanUtil.isMaskTTLExpiredRows(scan);

            region = env.getRegion();
            emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
            emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
            scanTableName = scan.getAttribute(BaseScannerRegionObserver.PHOENIX_TTL_SCAN_TABLE_NAME);

            byte[] txnScn = scan.getAttribute(BaseScannerRegionObserver.TX_SCN);
            if (txnScn != null) {
                TimeRange timeRange = scan.getTimeRange();
                scan.setTimeRange(timeRange.getMin(), Bytes.toLong(txnScn));
            }
            minTimestamp = scan.getTimeRange().getMin();
            maxTimestamp = scan.getTimeRange().getMax();
            now = maxTimestamp != HConstants.LATEST_TIMESTAMP ?
                            maxTimestamp :
                            EnvironmentEdgeManager.currentTimeMillis();
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
            if (!reported) {
                LOG.debug(String.format(
                        "PHOENIX-TTL-SCAN-STATS-ON-CLOSE: " + "request-id:[%s,%s] = [%d, %d, %d]",
                        this.requestId, Bytes.toString(scanTableName),
                        this.numRowsScanned, this.numRowsExpired, this.numRowsDeleted));
                reported = true;
            }
            scanner.close();
        }

        @Override public RegionInfo getRegionInfo() {
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
            try {
                boolean hasMore;
                do {
                    hasMore = raw ? scanner.nextRaw(result) : scanner.next(result);
                    if (result.isEmpty()) {
                        break;
                    }
                    numRowsScanned++;
                    if (maskIfExpired && checkRowNotExpired(result)) {
                        break;
                    }

                    if (deleteIfExpired && deleteRowIfExpired(result)) {
                        numRowsDeleted++;
                        break;
                    }
                    // skip this row
                    // 1. if the row has expired (checkRowNotExpired returned false)
                    // 2. if the row was not deleted (deleteRowIfExpired returned false and
                    //  do not want it to count towards the deleted count)
                    if (maskIfExpired) {
                        numRowsExpired++;
                    }
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
            System.arraycopy(firstCell.getRowArray(), firstCell.getRowOffset(), rowKey, 0,
                    firstCell.getRowLength());

            boolean isRowExpired = !checkRowNotExpired(cellList);
            if (isRowExpired) {
                long ttl = ScanUtil.getPhoenixTTL(this.scan);
                long ts = ScanUtil.getMaxTimestamp(cellList);
                LOG.trace(String.format(
                        "PHOENIX-TTL: Deleting row = [%s] belonging to table = %s, "
                                + "scn = %s, now = %d, delete-ts = %d, max-ts = %d",
                        Bytes.toString(rowKey),
                        Bytes.toString(scanTableName),
                        maxTimestamp != HConstants.LATEST_TIMESTAMP,
                        now, now - ttl, ts));
                Delete del = new Delete(rowKey, now - ttl);
                Mutation[] mutations = new Mutation[] { del };
                region.batchMutate(mutations);
                return true;
            }
            return false;
        }

        /**
         * @param cellList is an input and output parameter and will either include a valid row
         *                 or be an empty list
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
                if (ScanUtil.isEmptyColumn(cell, this.emptyCF, this.emptyCQ)) {
                    LOG.trace(String.format("** PHOENIX-TTL: Row expired for [%s], expired = %s **",
                            cell.toString(), ScanUtil.isTTLExpired(cell, this.scan, this.now)));
                    // Empty column is not supposed to be returned to the client
                    // except when it is the only column included in the scan.
                    if (cellListSize > 1) {
                        cellIterator.remove();
                    }
                    return !ScanUtil.isTTLExpired(cell, this.scan, this.now);
                }
            }
            LOG.warn("The empty column does not exist in a row in " + region.getRegionInfo()
                    .getTable().getNameAsString());
            return true;
        }

    }
}
