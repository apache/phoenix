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
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    protected byte[][] regionEndKeys;
    protected final int pageSizeInRows;
    protected final Scan scan;
    protected final Scan dataTableScan;
    protected final RegionScanner innerScanner;
    protected final Region region;
    protected final IndexMaintainer indexMaintainer;
    protected final TupleProjector tupleProjector;
    protected final ImmutableBytesWritable ptr;
    protected String exceptionMessage;
    protected List<List<Cell>> indexRows = null;
    protected Map<ImmutableBytesPtr, Result> dataRows = null;
    protected Iterator<List<Cell>> indexRowIterator = null;
    protected Map<byte[], byte[]> indexToDataRowKeyMap = null;
    protected int indexRowCount = 0;
    protected final long pageSizeMs;
    protected byte[] lastIndexRowKey = null;

    public UncoveredIndexRegionScanner(final RegionScanner innerScanner,
                                             final Region region,
                                             final Scan scan,
                                             final RegionCoprocessorEnvironment env,
                                             final Scan dataTableScan,
                                             final TupleProjector tupleProjector,
                                             final IndexMaintainer indexMaintainer,
                                             final byte[][] viewConstants,
                                             final ImmutableBytesWritable ptr,
                                             final long pageSizeMs) {
        super(innerScanner);
        final Configuration config = env.getConfiguration();

        byte[] pageSizeFromScan =
                scan.getAttribute(BaseScannerRegionObserver.INDEX_PAGE_ROWS);
        if (pageSizeFromScan != null) {
            pageSizeInRows = (int) Bytes.toLong(pageSizeFromScan);
        } else {
            pageSizeInRows = (int)
                    config.getLong(INDEX_PAGE_SIZE_IN_ROWS,
                            QueryServicesOptions.DEFAULT_INDEX_PAGE_SIZE_IN_ROWS);
        }

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
            keys.add(PVarbinary.INSTANCE.getKeyRange(dataRowKey));
        }
        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        Scan dataScan = new Scan(dataTableScan);
        dataScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        scanRanges.initializeScan(dataScan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        dataScan.setFilter(new SkipScanFilter(skipScanFilter, false));
        scan.setAttribute(BaseScannerRegionObserver.SERVER_PAGE_SIZE_MS,
                Bytes.toBytes(Long.valueOf(pageSizeMs)));
        return dataScan;
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

    private boolean getNextCoveredIndexRow(List<Cell> result) {
        if (indexRowIterator.hasNext()) {
            List<Cell> indexRow = indexRowIterator.next();
            result.addAll(indexRow);
            try {
                Result dataRow = dataRows.get(new ImmutableBytesPtr(
                        indexToDataRowKeyMap.get(CellUtil.cloneRow(indexRow.get(0)))));
                if (dataRow != null) {
                    IndexUtil.addTupleAsOneCell(result, new ResultTuple(dataRow),
                            tupleProjector, ptr);
                } else {
                    // The data row satisfying the scan does not exist. This could be because
                    // the data row may not include the columns corresponding to the uncovered
                    // index columns either. Just return the index row. Nothing to do here
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
                        return hasMore;
                    }
                    state = State.SCANNING_DATA;
                }
                if (state == State.SCANNING_DATA) {
                    scanDataTableRows(startTime);
                    indexRowIterator = indexRows.iterator();
                }
                if (state == State.READY) {
                    return getNextCoveredIndexRow(result);
                } else {
                    getDummyResult(lastIndexRowKey, result);
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
}
