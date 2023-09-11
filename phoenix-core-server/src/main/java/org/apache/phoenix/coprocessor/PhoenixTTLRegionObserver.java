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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixTTLSource;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;

/**
 * Coprocessor that checks whether the row is expired based on the TTL spec.
 */
public class PhoenixTTLRegionObserver extends BaseScannerRegionObserver implements RegionCoprocessor {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixTTLRegionObserver.class);
    private MetricsPhoenixTTLSource metricSource;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        metricSource = MetricsPhoenixCoprocessorSourceFactory.getInstance().getPhoenixTTLSource();
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return ScanUtil.isMaskTTLExpiredRows(scan) || ScanUtil.isDeleteTTLExpiredRows(scan);
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
                                              final RegionScanner s) throws IOException, SQLException {
        if (ScanUtil.isMaskTTLExpiredRows(scan) && ScanUtil.isDeleteTTLExpiredRows(scan)) {
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
    private static class PhoenixTTLRegionScanner extends BaseRegionScanner {
        private static final String MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR =
                "MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID";

        private final RegionCoprocessorEnvironment env;
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
        private long pageSizeMs;

        public PhoenixTTLRegionScanner(RegionCoprocessorEnvironment env, Scan scan,
                RegionScanner scanner) throws IOException {
            super(scanner);
            this.env = env;
            this.scan = scan;
            this.scanner = scanner;
            byte[] requestIdBytes = scan.getAttribute(MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR);
            this.requestId = Bytes.toString(requestIdBytes);

            deleteIfExpired = ScanUtil.isDeleteTTLExpiredRows(scan);
            maskIfExpired = !deleteIfExpired && ScanUtil.isMaskTTLExpiredRows(scan);

            region = env.getRegion();
            emptyCF = scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME);
            emptyCQ = scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME);
            scanTableName = scan.getAttribute(BaseScannerRegionObserverConstants.PHOENIX_TTL_SCAN_TABLE_NAME);

            byte[] txnScn = scan.getAttribute(BaseScannerRegionObserverConstants.TX_SCN);
            if (txnScn != null) {
                TimeRange timeRange = scan.getTimeRange();
                scan.setTimeRange(timeRange.getMin(), Bytes.toLong(txnScn));
            }
            minTimestamp = scan.getTimeRange().getMin();
            maxTimestamp = scan.getTimeRange().getMax();
            now = maxTimestamp != HConstants.LATEST_TIMESTAMP ?
                            maxTimestamp :
                            EnvironmentEdgeManager.currentTimeMillis();
            pageSizeMs = getPageSizeMsForRegionScanner(scan);
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
                long startTime = EnvironmentEdgeManager.currentTimeMillis();
                boolean hasMore;
                do {
                    hasMore = raw ? scanner.nextRaw(result) : scanner.next(result);
                    if (result.isEmpty()) {
                        break;
                    }
                    if (isDummy(result)) {
                        return true;
                    }

                    /**
                     Note : That both MaskIfExpiredRequest and DeleteIfExpiredRequest cannot be set at the same time.
                     Case : MaskIfExpiredRequest, If row not expired then return.
                     */
                    numRowsScanned++;
                    if (maskIfExpired && checkRowNotExpired(result)) {
                        break;
                    }

                    /**
                     Case : DeleteIfExpiredRequest, If deleted then return.
                     So that it will count towards the aggregate deleted count.
                     */
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
                    if (hasMore && (EnvironmentEdgeManager.currentTimeMillis() - startTime) >= pageSizeMs) {
                        byte[] rowKey = CellUtil.cloneRow(result.get(0));
                        result.clear();
                        getDummyResult(rowKey, result);
                        return true;
                    }
                    result.clear();
                } while (hasMore);
                return hasMore;
            } catch (Throwable t) {
                ClientUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
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
                    return !ScanUtil.isTTLExpired(cell, this.scan, this.now);
                }
            }
            LOG.warn("The empty column does not exist in a row in " + region.getRegionInfo()
                    .getTable().getNameAsString());
            return true;
        }

        @Override
        public RegionScanner getNewRegionScanner(Scan scan) throws IOException {
            try {
                return new PhoenixTTLRegionScanner(env, scan,
                        ((DelegateRegionScanner)delegate).getNewRegionScanner(scan));
            } catch (ClassCastException e) {
                throw new DoNotRetryIOException(e);
            }
        }
    }
}
