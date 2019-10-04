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
package org.apache.phoenix.index;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CHECK_VERIFY_COLUMN;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.PHYSICAL_DATA_TABLE_NAME;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.VERIFIED_BYTES;
import static org.apache.phoenix.index.IndexMaintainer.getIndexMaintainer;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hbase.index.metrics.GlobalIndexCheckerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ServerUtil;

/**
 * 
 * Coprocessor that verifies the scanned rows of a non-transactional global index.
 *
 */
public class GlobalIndexChecker implements RegionCoprocessor, RegionObserver {
    private static final Log LOG = LogFactory.getLog(GlobalIndexChecker.class);
    private GlobalIndexCheckerSource metricsSource;

    /**
     * Class that verifies a given row of a non-transactional global index.
     * An instance of this class is created for each scanner on an index
     * and used to verify individual rows and rebuild them if they are not valid
     */
    private class GlobalIndexScanner implements RegionScanner {
        RegionScanner scanner;
        private long ageThreshold;
        private Scan scan;
        private Scan indexScan;
        private Scan singleRowIndexScan;
        private Scan buildIndexScan = null;
        private Table dataHTable = null;
        private byte[] emptyCF;
        private byte[] emptyCQ;
        private IndexMaintainer indexMaintainer = null;
        private byte[][] viewConstants = null;
        private RegionCoprocessorEnvironment env;
        private Region region;
        private long minTimestamp;
        private long maxTimestamp;
        private GlobalIndexCheckerSource metricsSource;

        public GlobalIndexScanner(RegionCoprocessorEnvironment env,
                                  Scan scan,
                                  RegionScanner scanner,
                                  GlobalIndexCheckerSource metricsSource) throws IOException {
            this.env = env;
            this.scan = scan;
            this.scanner = scanner;
            this.metricsSource = metricsSource;

            region = env.getRegion();
            emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
            emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
            ageThreshold = env.getConfiguration().getLong(
                    QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
                    QueryServicesOptions.DEFAULT_GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS);
            minTimestamp = scan.getTimeRange().getMin();
            maxTimestamp = scan.getTimeRange().getMax();
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
                    if (verifyRowAndRepairIfNecessary(result)) {
                        break;
                    }
                    // skip this row as it is invalid
                    // if there is no more row, then result will be an empty list
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
            if (dataHTable != null) {
                dataHTable.close();
            }
        }

        @Override
        public RegionInfo getRegionInfo() {
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
                    if (verifyRowAndRepairIfNecessary(result)) {
                        break;
                    }
                    // skip this row as it is invalid
                    // if there is no more row, then result will be an empty list
                } while (hasMore);
                return hasMore;
            } catch (Throwable t) {
                ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
            }
        }

        private void deleteRowIfAgedEnough(byte[] indexRowKey, List<Cell> row, long ts) throws IOException {
            if ((EnvironmentEdgeManager.currentTimeMillis() - ts) > ageThreshold) {
                Delete del = new Delete(indexRowKey, ts);
                // We are deleting a specific version of a row so the flowing loop is for that
                for (Cell cell : row) {
                    del.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp());
                }
                Mutation[] mutations = new Mutation[]{del};
                region.batchMutate(mutations);
            }
        }

        private void repairIndexRows(byte[] indexRowKey, long ts, List<Cell> row) throws IOException {
            // Build the data table row key from the index table row key
            if (buildIndexScan == null) {
                buildIndexScan = new Scan();
                indexScan = new Scan(scan);
                singleRowIndexScan = new Scan(scan);
                byte[] dataTableName = scan.getAttribute(PHYSICAL_DATA_TABLE_NAME);
                byte[] indexTableName = region.getRegionInfo().getTable().getName();
                dataHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.DEFAULT_SERVER_CONNECTION,
                        env).getTable(TableName.valueOf(dataTableName));
                if (indexMaintainer == null) {
                    byte[] md = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
                    List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(md, true);
                    indexMaintainer = getIndexMaintainer(maintainers, indexTableName);
                }
                if (indexMaintainer == null) {
                    throw new DoNotRetryIOException(
                            "repairIndexRows: IndexMaintainer is not included in scan attributes for " +
                                    region.getRegionInfo().getTable().getNameAsString());
                }
                if (viewConstants == null) {
                    viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
                }
                // The following attributes are set to instruct UngroupedAggregateRegionObserver to do partial index rebuild
                // i.e., rebuild a subset of index rows.
                buildIndexScan.setAttribute(BaseScannerRegionObserver.UNGROUPED_AGG, TRUE_BYTES);
                buildIndexScan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD));
                buildIndexScan.setAttribute(BaseScannerRegionObserver.REBUILD_INDEXES, TRUE_BYTES);
                buildIndexScan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
            }
            // Rebuild the index row from the corresponding the row in the the data table
            // Get the data row key from the index row key
            byte[] dataRowKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
            buildIndexScan.withStartRow(dataRowKey, true);
            buildIndexScan.withStopRow(dataRowKey, true);
            buildIndexScan.setTimeRange(0, maxTimestamp);
            // If the data table row has been deleted then we want to delete the corresponding index row too.
            // Thus, we are using a raw scan
            buildIndexScan.setRaw(true);
            try (ResultScanner resultScanner = dataHTable.getScanner(buildIndexScan)){
                resultScanner.next();
            } catch (Throwable t) {
                ServerUtil.throwIOException(dataHTable.getName().toString(), t);
            }
            // Close the current scanner as the newly build row will not be visible to it
            scanner.close();
            // Open a new scanner starting from the current row
            indexScan.withStartRow(indexRowKey, true);
            scanner = region.getScanner(indexScan);
            // Scan the newly build index row
            scanner.next(row);
            if (row.isEmpty()) {
                return;
            }
            boolean indexRowExists = false;
            // Check if the index row still exist after rebuild
            while (Bytes.compareTo(row.get(0).getRowArray(), row.get(0).getRowOffset(), row.get(0).getRowLength(),
                    indexRowKey, 0, indexRowKey.length) == 0) {
                indexRowExists = true;
                if (verifyRowAndRemoveEmptyColumn(row)) {
                    // The index row status is "verified". This row is good to return to the client. We are done here.
                    return;
                }
                // The index row is still "unverified" after rebuild. This means either that the data table row timestamp is
                // lower than than the timestamp of the unverified index row (ts) and the index row that is built from
                // the data table row is masked by this unverified row, or that the corresponding data table row does
                // exist
                // First delete the unverified row from index if it is old enough
                deleteRowIfAgedEnough(indexRowKey, row, ts);
                // Now we will do a single row scan to retrieve the verified index row build from the data table row
                // if such an index row exists. Note we cannot read all versions in one scan as the max number of row
                // versions for an index table can be 1. In that case, we will get only one (i.e., the most recent
                // version instead of all versions
                singleRowIndexScan.withStartRow(indexRowKey, true);
                singleRowIndexScan.withStopRow(indexRowKey, true);
                singleRowIndexScan.setTimeRange(minTimestamp, ts);
                RegionScanner singleRowScanner = region.getScanner(singleRowIndexScan);
                row.clear();
                singleRowScanner.next(row);
                singleRowScanner.close();
                if (row.isEmpty()) {
                    // This means that the data row did not exist, so we need to skip this unverified row (i.e., do not
                    // return it to the client). Just retuning empty row is sufficient to do that
                    return;
                }
                ts = getMaxTimestamp(row);
            }
            if (indexRowExists) {
                // This means there does not exist a data row for the unverified index row. Skip this row. To do that
                // just return empty row.
                row.clear();
                return;
            } else {
                // This means the index row has been deleted. We got the next row
                // If the next row is "verified" (or empty) then we are good to go.
                if (verifyRowAndRemoveEmptyColumn(row)) {
                    return;
                }
                // The next row is "unverified". Rewind the scanner and let the row be scanned again
                // so that it can be repaired
                scanner = region.getScanner(indexScan);
                row.clear();
            }
        }

        private boolean isEmptyColumn(Cell cell) {
            return Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                    emptyCF, 0, emptyCF.length) == 0 &&
                    Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                            emptyCQ, 0, emptyCQ.length) == 0;
        }

        private boolean verifyRow(byte[] rowKey) throws IOException {
            LOG.warn("Scan " + scan + " did not return the empty column for " + region.getRegionInfo().getTable().getNameAsString());
            Get get = new Get(rowKey);
            get.setTimeRange(minTimestamp, maxTimestamp);
            get.addColumn(emptyCF, emptyCQ);
            Result result = region.get(get);
            if (result.isEmpty()) {
                LOG.warn("The empty column does not exist in a row in " + region.getRegionInfo().getTable().getNameAsString());
                return false;
            }
            if (Bytes.compareTo(result.getValue(emptyCF, emptyCQ), 0, VERIFIED_BYTES.length,
                    VERIFIED_BYTES, 0, VERIFIED_BYTES.length) != 0) {
                return false;
            }
            return true;
        }

        private boolean verifyRowAndRemoveEmptyColumn(List<Cell> cellList) throws IOException {
            long cellListSize = cellList.size();
            Cell cell = null;
            if (cellListSize == 0) {
                return true;
            }
            Iterator<Cell> cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                cell = cellIterator.next();
                if (isEmptyColumn(cell)) {
                    if (Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            VERIFIED_BYTES, 0, VERIFIED_BYTES.length) != 0) {
                        return false;
                    }
                    // Empty column is not supposed to be returned to the client except it is the only column included
                    // in the scan
                    if (cellListSize > 1) {
                        cellIterator.remove();
                    }
                    return true;
                }
            }
            byte[] rowKey = new byte[cell.getRowLength()];
            System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowKey, 0, cell.getRowLength());
            return verifyRow(rowKey);
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

        /**
         * @param cellList is an input and output parameter and will either include a valid row or be an empty list
         * @return true if there exists more rows, otherwise false
         * @throws IOException
         */
        private boolean verifyRowAndRepairIfNecessary(List<Cell> cellList) throws IOException {
            Cell cell = cellList.get(0);
            if (verifyRowAndRemoveEmptyColumn(cellList)) {
                return true;
            } else {
                long repairStart = EnvironmentEdgeManager.currentTimeMillis();

                byte[] rowKey = new byte[cell.getRowLength()];
                System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowKey, 0, cell.getRowLength());
                long ts = getMaxTimestamp(cellList);
                cellList.clear();

                try {
                    repairIndexRows(rowKey, ts, cellList);
                    metricsSource.incrementIndexRepairs();
                    metricsSource.updateIndexRepairTime(EnvironmentEdgeManager.currentTimeMillis() - repairStart);
                } catch (IOException e) {
                    metricsSource.incrementIndexRepairFailures();
                    metricsSource.updateIndexRepairFailureTime(EnvironmentEdgeManager.currentTimeMillis() - repairStart);
                    throw e;
                }

                if (cellList.isEmpty()) {
                    // This means that the index row is invalid. Return false to tell the caller that this row should be skipped
                    return false;
                }
                return true;
            }
        }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
                                         Scan scan, RegionScanner s) throws IOException {
        if (scan.getAttribute(CHECK_VERIFY_COLUMN) == null) {
            return s;
        }
        return new GlobalIndexScanner(c.getEnvironment(), scan, s, metricsSource);
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        this.metricsSource = MetricsIndexerSourceFactory.getInstance().getGlobalIndexCheckerSource();
    }
}
