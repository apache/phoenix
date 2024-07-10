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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.INDEX_PAGE_ROWS;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.SERVER_PAGE_SIZE_MS;
import static org.apache.phoenix.query.QueryServices.INDEX_PAGE_SIZE_IN_ROWS;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.isDummy;

public abstract class UncoveredIndexRegionScanner extends BaseRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UncoveredIndexRegionScanner.class);
    /**
     * The states of the processing a page of index rows
     */
    protected enum State {
        INITIAL, SCANNING_INDEX, SCANNING_DATA, SCANNING_DATA_INTERRUPTED, READY
    }
    protected State state = State.INITIAL;
    protected final byte[][] viewConstants;
    protected final RegionCoprocessorEnvironment env;
    protected long pageSizeInRows;
    protected final long ageThreshold;
    protected byte[] emptyCF;
    protected byte[] emptyCQ;
    protected final Scan scan;
    protected final Scan dataTableScan;
    protected final RegionScanner innerScanner;
    protected final Region region;
    protected final IndexMaintainer indexMaintainer;
    protected final TupleProjector tupleProjector;
    protected final ImmutableBytesWritable ptr;
    protected List<List<Cell>> indexRows = null;
    protected Map<ImmutableBytesPtr, Result> dataRows = null;
    protected Iterator<List<Cell>> indexRowIterator = null;
    protected Map<byte[], byte[]> indexToDataRowKeyMap = null;
    protected int indexRowCount = 0;
    protected final long pageSizeMs;
    protected byte[] lastIndexRowKey = null;
    private byte[] previousResultRowKey = null;
    private final byte[] initStartRowKey;
    private final boolean includeInitStartRowKey;

    public UncoveredIndexRegionScanner(final RegionScanner innerScanner,
                                             final Region region,
                                             final Scan scan,
                                             final RegionCoprocessorEnvironment env,
                                             final Scan dataTableScan,
                                             final TupleProjector tupleProjector,
                                             final IndexMaintainer indexMaintainer,
                                             final byte[][] viewConstants,
                                             final ImmutableBytesWritable ptr,
                                             final long pageSizeMs,
                                             final long queryLimit) {
        super(innerScanner);
        final Configuration config = env.getConfiguration();

        byte[] pageSizeFromScan =
                scan.getAttribute(INDEX_PAGE_ROWS);
        if (pageSizeFromScan != null) {
            pageSizeInRows = (int) Bytes.toLong(pageSizeFromScan);
        } else {
            pageSizeInRows = (int)
                    config.getLong(INDEX_PAGE_SIZE_IN_ROWS,
                            QueryServicesOptions.DEFAULT_INDEX_PAGE_SIZE_IN_ROWS);
        }
        if (queryLimit != -1) {
            pageSizeInRows = Long.min(pageSizeInRows, queryLimit);
        }

        ageThreshold = env.getConfiguration().getLong(
                QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS);
        emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
        emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
        this.indexMaintainer = indexMaintainer;
        this.viewConstants = viewConstants;
        this.scan = scan;
        this.dataTableScan = dataTableScan;
        this.innerScanner = innerScanner;
        this.region = region;
        this.env = env;
        this.ptr = ptr;
        this.tupleProjector = tupleProjector;
        this.pageSizeMs = pageSizeMs;
        // If scan start rowkey is empty, use region boundaries. Reverse region boundaries
        // for reverse scan.
        this.initStartRowKey =
                ServerUtil.getScanStartRowKeyFromScanOrRegionBoundaries(scan, region);
        this.includeInitStartRowKey = scan.includeStartRow();
    }

    @Override
    public long getMvccReadPoint() {
        return innerScanner.getMvccReadPoint();
    }
    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return false;
    }

    @Override
    public void close() throws IOException {
        innerScanner.close();
    }

    @Override
    public long getMaxResultSize() {
        return innerScanner.getMaxResultSize();
    }

    @Override
    public int getBatch() {
        return innerScanner.getBatch();
    }

    protected abstract void scanDataTableRows(long startTime) throws IOException;

    protected Scan prepareDataTableScan(Collection<byte[]> dataRowKeys) throws IOException {
        List<KeyRange> keys = new ArrayList<>(dataRowKeys.size());
        for (byte[] dataRowKey : dataRowKeys) {
            // If the data table scan was interrupted because of paging we retry the scan
            // but on retry we should only fetch data table rows which we haven't already
            // fetched.
            if (!dataRows.containsKey(new ImmutableBytesPtr(dataRowKey))) {
                keys.add(PVarbinary.INSTANCE.getKeyRange(dataRowKey, SortOrder.ASC));
            }
        }
        if (!keys.isEmpty()) {
            ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
            Scan dataScan = new Scan(dataTableScan);
            dataScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
            scanRanges.initializeScan(dataScan);
            SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
            dataScan.setFilter(new SkipScanFilter(skipScanFilter, false, true));
            dataScan.setAttribute(SERVER_PAGE_SIZE_MS,
                    Bytes.toBytes(Long.valueOf(pageSizeMs)));
            return dataScan;
        } else {
            LOGGER.info("All data rows have already been fetched");
            return null;
        }
    }

    protected boolean scanIndexTableRows(List<Cell> result,
                                         final long startTime,
                                         final byte[] actualStartKey,
                                         final int offset) throws IOException {
        boolean hasMore = false;
        if (actualStartKey != null) {
            do {
                hasMore = innerScanner.nextRaw(result);
                if (result.isEmpty()) {
                    return hasMore;
                }
                if (ScanUtil.isDummy(result)) {
                    return true;
                }
                Cell firstCell = result.get(0);
                if (Bytes.compareTo(firstCell.getRowArray(), firstCell.getRowOffset(),
                        firstCell.getRowLength(), actualStartKey, 0, actualStartKey.length) < 0) {
                    result.clear();
                    if (EnvironmentEdgeManager.currentTimeMillis() - startTime >= pageSizeMs) {
                        byte[] rowKey = CellUtil.cloneRow(firstCell);
                        ScanUtil.getDummyResult(rowKey, result);
                        return true;
                    }
                } else {
                    break;
                }
            } while (hasMore);
        }

        do {
            List<Cell> row = new ArrayList<Cell>();
            if (result.isEmpty()) {
                hasMore = innerScanner.nextRaw(row);
            } else {
                row.addAll(result);
                result.clear();
            }
            if (!row.isEmpty()) {
                if (isDummy(row)) {
                    result.addAll(row);
                    // We got a dummy request from lower layers. This means that
                    // the scan took more than pageSizeMs. Just return true here.
                    // The client will drop this dummy request and continue to scan.
                    // Then the lower layer scanner will continue
                    // wherever it stopped due to this dummy request
                    return true;
                }
                Cell firstCell = row.get(0);
                byte[] indexRowKey = firstCell.getRowArray();
                ptr.set(indexRowKey, firstCell.getRowOffset() + offset,
                        firstCell.getRowLength() - offset);
                lastIndexRowKey = ptr.copyBytes();
                indexToDataRowKeyMap.put(offset == 0 ? lastIndexRowKey :
                                CellUtil.cloneRow(firstCell), indexMaintainer.buildDataRowKey(
                                        new ImmutableBytesWritable(lastIndexRowKey),
                                viewConstants));
                indexRows.add(row);
                indexRowCount++;
                if (hasMore && (EnvironmentEdgeManager.currentTimeMillis() - startTime)
                        >= pageSizeMs) {
                    getDummyResult(lastIndexRowKey, result);
                    // We do not need to change the state, State.SCANNING_INDEX
                    // since we will continue scanning the index table after
                    // the client drops the dummy request and then calls the next
                    // method on its ResultScanner within ScanningResultIterator
                    return true;
                }
            }
        } while (hasMore && indexRowCount < pageSizeInRows);
        return hasMore;
    }

    protected boolean scanIndexTableRows(List<Cell> result,
                                         final long startTime) throws IOException {
        return scanIndexTableRows(result, startTime, null, 0);
    }

    private boolean verifyIndexRowAndRepairIfNecessary(Result dataRow, byte[] indexRowKey,
            long indexTimestamp)
            throws IOException {
        Put put = new Put(dataRow.getRow());
        for (Cell cell : dataRow.rawCells()) {
            put.add(cell);
        }
        if (indexMaintainer.checkIndexRow(indexRowKey, put)) {
            if (IndexUtil.getMaxTimestamp(put) != indexTimestamp) {
                Mutation[] mutations;
                Put indexPut = new Put(indexRowKey);
                indexPut.addColumn(emptyCF, emptyCQ, indexTimestamp, QueryConstants.VERIFIED_BYTES);
                if ((EnvironmentEdgeManager.currentTimeMillis() - indexTimestamp) > ageThreshold) {
                    Delete indexDelete = indexMaintainer.buildRowDeleteMutation(indexRowKey,
                            IndexMaintainer.DeleteType.SINGLE_VERSION, indexTimestamp);
                    mutations = new Mutation[]{indexPut, indexDelete};
                } else {
                    mutations = new Mutation[]{indexPut};
                }
                region.batchMutate(mutations);
            }
            return true;
        }
        if (indexMaintainer.isAgedEnough(IndexUtil.getMaxTimestamp(put), ageThreshold)) {
            region.delete(indexMaintainer.createDelete(indexRowKey, IndexUtil.getMaxTimestamp(put), false));
        }
        return false;
    }

    private boolean getNextCoveredIndexRow(List<Cell> result) throws IOException {
        if (indexRowIterator.hasNext()) {
            List<Cell> indexRow = indexRowIterator.next();
            result.addAll(indexRow);
            try {
                byte[] indexRowKey = CellUtil.cloneRow(indexRow.get(0));
                Result dataRow = dataRows.get(new ImmutableBytesPtr(
                        indexToDataRowKeyMap.get(indexRowKey)));
                if (dataRow != null) {
                    long ts = indexRow.get(0).getTimestamp();
                    if (!indexMaintainer.isUncovered()
                            || verifyIndexRowAndRepairIfNecessary(dataRow, indexRowKey, ts)) {
                        if (tupleProjector != null) {
                            IndexUtil.addTupleAsOneCell(result, new ResultTuple(dataRow),
                                    tupleProjector, ptr);
                        }
                    } else {
                        result.clear();
                    }
                } else {
                    if (indexMaintainer.isUncovered()) {
                        long ts = indexRow.get(0).getTimestamp();
                        // Since we also scan the empty column for uncovered global indexes, this mean the data row
                        // does not exist. Delete the index row if the index is an uncovered global index
                        if (indexMaintainer.isAgedEnough(ts, ageThreshold)) {
                            region.delete(indexMaintainer.createDelete(indexRowKey, ts, false));
                        }
                        result.clear();
                    } else {
                        // The data row satisfying the scan does not exist. This could be because
                        // the data row may not include the columns corresponding to the uncovered
                        // index columns either. Just return the index row. Nothing to do here
                    }
                }
            } catch (Throwable e) {
                LOGGER.error("Exception in UncoveredIndexRegionScanner for region "
                        + region.getRegionInfo().getRegionNameAsString(), e);
                throw e;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * A page of index rows are scanned and then their corresponding data table rows are retrieved
     * from the data table regions in parallel. These data rows are then joined with index rows.
     * The join is for adding uncovered columns to index rows.
     *
     * This implementation conforms to server paging such that if the server side operation takes
     * more than pageSizeInMs, a dummy result is returned to signal the client that more work
     * to do on the server side. This is done to prevent RPC timeouts.
     *
     * @param result
     * @return boolean to indicate if there are more rows to scan
     * @throws IOException
     */
    @Override
    public boolean next(List<Cell> result) throws IOException {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        boolean hasMore;
        region.startRegionOperation();
        try {
            synchronized (innerScanner) {
                if (state == State.READY && !indexRowIterator.hasNext()) {
                    state = State.INITIAL;
                }
                if (state == State.INITIAL) {
                    indexRowCount = 0;
                    indexRows = new ArrayList<>();
                    dataRows = Maps.newConcurrentMap();
                    indexToDataRowKeyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                    state = State.SCANNING_INDEX;
                }
                if (state == State.SCANNING_INDEX) {
                    hasMore = scanIndexTableRows(result, startTime);
                    if (isDummy(result)) {
                        updateDummyWithPrevRowKey(result, initStartRowKey, includeInitStartRowKey,
                                scan);
                        return hasMore;
                    }
                    state = State.SCANNING_DATA;
                }
                if (state == State.SCANNING_DATA) {
                    scanDataTableRows(startTime);
                    indexRowIterator = indexRows.iterator();
                }
                if (state == State.READY) {
                    boolean moreRows = getNextCoveredIndexRow(result);
                    if (!result.isEmpty()) {
                        previousResultRowKey = CellUtil.cloneRow(result.get(0));
                    }
                    return moreRows;
                } else {
                    updateDummyWithPrevRowKey(result, initStartRowKey, includeInitStartRowKey,
                            scan);
                    return true;
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Exception in UncoveredIndexRegionScanner for region "
                    + region.getRegionInfo().getRegionNameAsString(), e);
            throw e;
        } finally {
            region.closeRegionOperation();
        }
    }

    /**
     * Add dummy cell to the result list based on either the previous rowkey returned to the
     * client or the start rowkey and start rowkey include params.
     *
     * @param result result to add the dummy cell to.
     * @param initStartRowKey scan start rowkey.
     * @param includeInitStartRowKey scan start rowkey included.
     * @param scan scan object.
     */
    private void updateDummyWithPrevRowKey(List<Cell> result, byte[] initStartRowKey,
                                           boolean includeInitStartRowKey, Scan scan) {
        result.clear();
        if (previousResultRowKey != null) {
            getDummyResult(previousResultRowKey, result);
        } else {
            if (includeInitStartRowKey && initStartRowKey.length > 0) {
                byte[] prevKey;
                // In order to generate largest possible rowkey that is less than
                // initStartRowKey, we need to check size of the region name that can be
                // used by hbase client for meta lookup, in case meta cache is expired at client.
                // Once we know regionLookupInMetaLen, use it to generate largest possible
                // rowkey that is lower than initStartRowKey by using
                // ByteUtil#previousKeyWithLength function, which appends "\\xFF" bytes to
                // prev rowkey upto the length provided. e.g. for the given key
                // "\\x01\\xC1\\x06", the previous key with length 5 would be
                // "\\x01\\xC1\\x05\\xFF\\xFF" by padding 2 bytes "\\xFF".
                // The length of the largest scan start rowkey should not exceed
                // HConstants#MAX_ROW_LENGTH.
                int regionLookupInMetaLen =
                        RegionInfo.createRegionName(region.getTableDescriptor().getTableName(),
                                new byte[1], HConstants.NINES, false).length;
                if (Bytes.compareTo(initStartRowKey, initStartRowKey.length - 1,
                        1, ByteUtil.ZERO_BYTE, 0, 1) == 0) {
                    // If initStartRowKey has last byte as "\\x00", we can discard the last
                    // byte and send the key as dummy rowkey.
                    prevKey = new byte[initStartRowKey.length - 1];
                    System.arraycopy(initStartRowKey, 0, prevKey, 0, prevKey.length);
                } else if (initStartRowKey.length <
                        (HConstants.MAX_ROW_LENGTH - 1 - regionLookupInMetaLen)) {
                    prevKey = ByteUtil.previousKeyWithLength(ByteUtil.concat(initStartRowKey,
                                    new byte[HConstants.MAX_ROW_LENGTH
                                            - initStartRowKey.length - 1 - regionLookupInMetaLen]),
                            HConstants.MAX_ROW_LENGTH - 1 - regionLookupInMetaLen);
                } else {
                    prevKey = initStartRowKey;
                }
                getDummyResult(prevKey, result);
            } else {
                getDummyResult(initStartRowKey, result);
            }
        }
    }

}
