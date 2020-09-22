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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.metrics.GlobalIndexCheckerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Coprocessor that verifies the scanned rows of a non-transactional global index.
 *
 * If an index row is unverified (i.e., the row status is unverified), the following steps are taken :
 * (1) We generate the data row key from the index row key, and check if the data row exists. If not, this unverified
 * index row is skipped (i.e., not returned to the client), and it is deleted if it is old enough. The age check is
 * necessary in order not to delete the index rows that are currently being updated. If the data row exists,
 * we continue with the rest of the steps.
 * (2) The index row is rebuilt from the data row.
 * (3) The current scanner is closed as the newly rebuilt row will not be visible to the current scanner.
 * (4) if the data row does not point back to the unverified index row (i.e., the index row key generated from the data
 * row does not match with the row key of the unverified index row), this unverified row is skipped and and it is
 * deleted if it is old enough. A new scanner is opened starting form the index row after this unverified index row.
 * (5) if the data points back to the unverified index row then, a new scanner is opened starting form the index row.
 * The next row is scanned to check if it is verified. if it is verified, it is returned to the client. If not, then
 * it means the data table row timestamp is lower than than the timestamp of the unverified index row, and
 * the index row that has been rebuilt from the data table row is masked by this unverified row. This happens if the
 * first phase updates (i.e., unverified index row updates) complete but the second phase updates (i.e., data table
 * row updates) fail. There could be back to back such events so we need to scan older versions to retrieve
 * the verified version that is masked by the unverified version(s).
 *
 */
public class GlobalIndexChecker extends BaseRegionObserver implements RegionCoprocessor, RegionObserver {
    private static final Logger LOG =
        LoggerFactory.getLogger(GlobalIndexChecker.class);
    private GlobalIndexCheckerSource metricsSource;
    private CoprocessorEnvironment env;

    public enum RebuildReturnCode {
        NO_DATA_ROW(0),
        NO_INDEX_ROW(1),
        INDEX_ROW_EXISTS(2);
        private int value;

        RebuildReturnCode(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Class that verifies a given row of a non-transactional global index.
     * An instance of this class is created for each scanner on an index
     * and used to verify individual rows and rebuild them if they are not valid
     */
    private class GlobalIndexScanner implements RegionScanner {
        private RegionScanner scanner;
        private RegionScanner deleteRowScanner;
        private long ageThreshold;
        private Scan scan;
        private Scan indexScan;
        private Scan deleteRowScan;
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
        private long rowCount = 0;
        private long pageSize = Long.MAX_VALUE;
        private boolean restartScanDueToPageFilterRemoval = false;
        private boolean hasMore;
        private String indexName;

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
            byte[] indexTableNameBytes = region.getRegionInfo().getTable().getName();
            this.indexName = Bytes.toString(indexTableNameBytes);
            byte[] md = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
            List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(md, true);
            indexMaintainer = getIndexMaintainer(maintainers, indexTableNameBytes);
            if (indexMaintainer == null) {
                throw new DoNotRetryIOException(
                        "repairIndexRows: IndexMaintainer is not included in scan attributes for " +
                                region.getRegionInfo().getTable().getNameAsString());
            }
        }

        @Override
        public int getBatch() {
            return scanner.getBatch();
        }

        @Override
        public long getMaxResultSize() {
            return scanner.getMaxResultSize();
        }

        public boolean next(List<Cell> result, boolean raw) throws IOException {
            try {
                do {
                    if (raw) {
                        hasMore = scanner.nextRaw(result);
                    } else {
                        hasMore = scanner.next(result);
                    }
                    if (result.isEmpty()) {
                        break;
                    }
                    if (verifyRowAndRepairIfNecessary(result)) {
                        break;
                    }
                    // skip this row as it is invalid
                    // if there is no more row, then result will be an empty list
                } while (hasMore);
                rowCount++;
                if (rowCount == pageSize) {
                    return false;
                }
                return hasMore;
            } catch (Throwable t) {
                ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
            }
        }

        @Override
        public boolean next(List<Cell> result) throws IOException {
           return next(result, false);
        }

        @Override
        public boolean nextRaw(List<Cell> result) throws IOException {
            return next(result, true);
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


        private void deleteRowIfAgedEnough(byte[] indexRowKey, List<Cell> row, long ts, boolean specific) throws IOException {
            if ((EnvironmentEdgeManager.currentTimeMillis() - ts) > ageThreshold) {
                Delete del;
                if (specific) {
                    del = indexMaintainer.buildRowDeleteMutation(indexRowKey,
                            IndexMaintainer.DeleteType.SINGLE_VERSION, ts);
                } else {
                    del = indexMaintainer.buildRowDeleteMutation(indexRowKey,
                            IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                }
                Mutation[] mutations = new Mutation[]{del};
                region.batchMutate(mutations);
            }
        }

        private PageFilter removePageFilterFromFilterList(FilterList filterList) {
            Iterator<Filter> filterIterator = filterList.getFilters().iterator();
            while (filterIterator.hasNext()) {
                Filter filter = filterIterator.next();
                if (filter instanceof PageFilter) {
                    filterIterator.remove();
                    return (PageFilter) filter;
                } else if (filter instanceof FilterList) {
                    PageFilter pageFilter = removePageFilterFromFilterList((FilterList) filter);
                    if (pageFilter != null) {
                        return pageFilter;
                    }
                }
            }
            return null;
        }

        // This method assumes that there is at most one instance of PageFilter in a scan
        private PageFilter removePageFilter(Scan scan) {
            Filter filter = scan.getFilter();
            if (filter != null) {
                if (filter instanceof PageFilter) {
                    scan.setFilter(null);
                    return (PageFilter) filter;
                } else if (filter instanceof FilterList) {
                    return removePageFilterFromFilterList((FilterList) filter);
                }
            }
            return null;
        }

        private void repairIndexRows(byte[] indexRowKey, long ts, List<Cell> row) throws IOException {
            if (buildIndexScan == null) {
                PageFilter pageFilter = removePageFilter(scan);
                if (pageFilter != null) {
                    pageSize = pageFilter.getPageSize();
                    restartScanDueToPageFilterRemoval = true;
                }
                buildIndexScan = new Scan();
                indexScan = new Scan(scan);
                deleteRowScan = new Scan();
                singleRowIndexScan = new Scan(scan);
                byte[] dataTableName = scan.getAttribute(PHYSICAL_DATA_TABLE_NAME);
                dataHTable =
                    ServerUtil.ConnectionFactory.
                        getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION, env).
                        getTable(TableName.valueOf(dataTableName));

                viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
                // The following attributes are set to instruct UngroupedAggregateRegionObserver to do partial index rebuild
                // i.e., rebuild a subset of index rows.
                buildIndexScan.setAttribute(BaseScannerRegionObserver.UNGROUPED_AGG, TRUE_BYTES);
                buildIndexScan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD));
                buildIndexScan.setAttribute(BaseScannerRegionObserver.REBUILD_INDEXES, TRUE_BYTES);
                buildIndexScan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
                // Scan only columns included in the index table plus the empty column
                for (ColumnReference column : indexMaintainer.getAllColumns()) {
                    buildIndexScan.addColumn(column.getFamily(), column.getQualifier());
                }
                buildIndexScan.addColumn(indexMaintainer.getDataEmptyKeyValueCF(), indexMaintainer.getEmptyKeyValueQualifier());
            }
            // Rebuild the index row from the corresponding the row in the the data table
            // Get the data row key from the index row key
            byte[] dataRowKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
            buildIndexScan.withStartRow(dataRowKey, true);
            buildIndexScan.withStopRow(dataRowKey, true);
            buildIndexScan.setTimeRange(0, maxTimestamp);
            // Pass the index row key to the partial index builder which will rebuild the index row and check if the
            // row key of this rebuilt index row matches with the passed index row key
            buildIndexScan.setAttribute(BaseScannerRegionObserver.INDEX_ROW_KEY, indexRowKey);
            Result result = null;
            try (ResultScanner resultScanner = dataHTable.getScanner(buildIndexScan)){
                result = resultScanner.next();
            } catch (Throwable t) {
                ServerUtil.throwIOException(dataHTable.getName().toString(), t);
            }
            // A single cell will be returned. We decode that here
            byte[] value = result.value();
            long code = PLong.INSTANCE.getCodec().decodeLong(new ImmutableBytesWritable(value), SortOrder.getDefault());
            if (code == RebuildReturnCode.NO_DATA_ROW.getValue()) {
                // This means there does not exist a data table row for the data row key derived from
                // this unverified index row. So, no index row has been built
                // Delete the unverified row from index if it is old enough
                deleteRowIfAgedEnough(indexRowKey, row, ts, false);
                // Skip this unverified row (i.e., do not return it to the client). Just retuning empty row is
                // sufficient to do that
                row.clear();
                if (restartScanDueToPageFilterRemoval) {
                    scanner.close();
                    indexScan.withStartRow(indexRowKey, false);
                    scanner = region.getScanner(indexScan);
                    hasMore = true;
                    // Set restartScanDueToPageFilterRemoval to false as we do not restart the scan unnecessarily next time
                    restartScanDueToPageFilterRemoval = false;
                }
                return;
            }
            // An index row has been built. Close the current scanner as the newly built row will not be visible to it
            scanner.close();
            // Set restartScanDueToPageFilterRemoval to false as we do not restart the scan unnecessarily next time
            restartScanDueToPageFilterRemoval = false;
            if (code == RebuildReturnCode.NO_INDEX_ROW.getValue()) {
                // This means there exists a data table row for the data row key derived from this unverified index row
                // but the data table row does not point back to the index row.
                // Delete the unverified row from index if it is old enough
                deleteRowIfAgedEnough(indexRowKey, row, ts, false);
                // Open a new scanner starting from the row after the current row
                indexScan.withStartRow(indexRowKey, false);
                scanner = region.getScanner(indexScan);
                hasMore = true;
                // Skip this unverified row (i.e., do not return it to the client). Just retuning empty row is
                // sufficient to do that
                row.clear();
                return;
            }
            // code == RebuildReturnCode.INDEX_ROW_EXISTS.getValue()
            // Open a new scanner starting from the current row
            indexScan.withStartRow(indexRowKey, true);
            scanner = region.getScanner(indexScan);
            hasMore = scanner.next(row);
            if (row.isEmpty()) {
                // This means the index row has been deleted before opening the new scanner.
                return;
            }
            // Check if the index row still exist after rebuild
            if  (Bytes.compareTo(row.get(0).getRowArray(), row.get(0).getRowOffset(), row.get(0).getRowLength(),
                    indexRowKey, 0, indexRowKey.length) != 0) {
                // This means the index row has been deleted before opening the new scanner. We got a different row
                // If this row is "verified" (or empty) then we are good to go.
                if (verifyRowAndRemoveEmptyColumn(row)) {
                    return;
                }
                // The row is "unverified". Rewind the scanner and let the row be scanned again
                // so that it can be repaired
                scanner.close();
                scanner = region.getScanner(indexScan);
                hasMore = true;
                row.clear();
                return;
            }
            // The index row still exist after rebuild
            // Check if the index row is still unverified
            if (verifyRowAndRemoveEmptyColumn(row)) {
                // The index row status is "verified". This row is good to return to the client. We are done here.
                return;
            }
            // The index row is still "unverified" after rebuild. This means that the data table row timestamp is
            // lower than than the timestamp of the unverified index row (ts) and the index row that is built from
            // the data table row is masked by this unverified row. This happens if the first phase updates (i.e.,
            // unverified index row updates) complete but the second phase updates (i.e., data table updates) fail.
            // There could be back to back such events so we need a loop to go through them
            do {
                // First delete the unverified row from index if it is old enough
                deleteRowIfAgedEnough(indexRowKey, row, ts, true);
                // Now we will do a single row scan to retrieve the verified index row built from the data table row.
                // Note we cannot read all versions in one scan as the max number of row versions for an index table
                // can be 1. In that case, we will get only one (i.e., the most recent) version instead of all versions
                singleRowIndexScan.withStartRow(indexRowKey, true);
                singleRowIndexScan.withStopRow(indexRowKey, true);
                singleRowIndexScan.setTimeRange(minTimestamp, ts);
                RegionScanner singleRowScanner = region.getScanner(singleRowIndexScan);
                row.clear();
                singleRowScanner.next(row);
                singleRowScanner.close();
                if (row.isEmpty()) {
                    LOG.error("Could not find the newly rebuilt index row with row key " +
                            Bytes.toStringBinary(indexRowKey) + " for table " +
                            region.getRegionInfo().getTable().getNameAsString());
                    // This was not expected. The new build index row must be deleted before opening the new scanner
                    // possibly by compaction
                    return;
                }
                if (verifyRowAndRemoveEmptyColumn(row)) {
                    // The index row status is "verified". This row is good to return to the client. We are done here.
                    return;
                }
                ts = getMaxTimestamp(row);
            } while (Bytes.compareTo(row.get(0).getRowArray(), row.get(0).getRowOffset(), row.get(0).getRowLength(),
                    indexRowKey, 0, indexRowKey.length) == 0);
            // This should not happen at all
            Cell cell = row.get(0);
            byte[] rowKey = CellUtil.cloneRow(cell);
            throw new DoNotRetryIOException("The scan returned a row with row key (" + Bytes.toStringBinary(rowKey) +
                     ") different than indexRowKey (" + Bytes.toStringBinary(indexRowKey) + ") for table " +
                        region.getRegionInfo().getTable().getNameAsString());
        }

        private boolean isEmptyColumn(Cell cell) {
            return Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                    emptyCF, 0, emptyCF.length) == 0 &&
                    Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                            emptyCQ, 0, emptyCQ.length) == 0;
        }

        /**
         *  An index row is composed of cells with the same timestamp. However, if there are multiple versions of an
         *  index row, HBase can return an index row with cells from multiple versions, and thus it can return cells
         *  with different timestamps. This happens if the version of the row we are reading does not have a value
         *  (i.e., effectively has null value) for a column whereas an older version has a value for the column.
         *  In this case, we need to remove the older cells for correctness.
         */
        private void removeOlderCells(List<Cell> cellList) {
            Iterator<Cell> cellIterator = cellList.iterator();
            if (!cellIterator.hasNext()) {
                return;
            }
            Cell cell = cellIterator.next();
            long maxTs = cell.getTimestamp();
            long ts;
            boolean allTheSame = true;
            while (cellIterator.hasNext()) {
                cell = cellIterator.next();
                ts = cell.getTimestamp();
                if (ts != maxTs) {
                    if (ts > maxTs) {
                        maxTs = ts;
                    }
                    allTheSame = false;
                }
            }
            if (allTheSame) {
                return;
            }
            cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                cell = cellIterator.next();
                if (cell.getTimestamp() != maxTs) {
                    cellIterator.remove();
                }
            }
        }

        private boolean verifyRowAndRemoveEmptyColumn(List<Cell> cellList) throws IOException {
            if (!indexMaintainer.isImmutableRows()) {
                removeOlderCells(cellList);
            }
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
            // This index row does not have an empty column cell. It must be removed by compaction. This row will
            // be treated as unverified so that it can be repaired
            return false;
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
            metricsSource.incrementIndexInspections(indexName);
            Cell cell = cellList.get(0);
            if (verifyRowAndRemoveEmptyColumn(cellList)) {
                return true;
            } else {
                long repairStart = EnvironmentEdgeManager.currentTimeMillis();

                byte[] rowKey = CellUtil.cloneRow(cell);
                long ts = cellList.get(0).getTimestamp();
                cellList.clear();

                try {
                    repairIndexRows(rowKey, ts, cellList);
                    metricsSource.incrementIndexRepairs(indexName);
                    metricsSource.updateIndexRepairTime(indexName,
                        EnvironmentEdgeManager.currentTimeMillis() - repairStart);
                } catch (IOException e) {
                    metricsSource.incrementIndexRepairFailures(indexName);
                    metricsSource.updateIndexRepairFailureTime(indexName,
                        EnvironmentEdgeManager.currentTimeMillis() - repairStart);
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
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
                                         Scan scan, RegionScanner s) throws IOException {
        if (scan.getAttribute(CHECK_VERIFY_COLUMN) == null) {
            return s;
        }
        return new GlobalIndexScanner(c.getEnvironment(), scan, s, metricsSource);
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        this.env = e;
        this.metricsSource = MetricsIndexerSourceFactory.getInstance().getGlobalIndexCheckerSource();
    }

}
