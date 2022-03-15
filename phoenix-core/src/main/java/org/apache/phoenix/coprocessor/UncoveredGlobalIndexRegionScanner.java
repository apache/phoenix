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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.PHYSICAL_DATA_TABLE_NAME;
import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY;
import static org.apache.phoenix.query.QueryServices.INDEX_PAGE_SIZE_IN_ROWS;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.parallel.*;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an index table region scanner which scans index table rows locally and then extracts
 * data table row keys from them. Using the data table row keys, the data table rows are scanned
 * using the HBase client available to region servers.
 */
public class UncoveredGlobalIndexRegionScanner extends BaseRegionScanner {
    public static final String NUM_CONCURRENT_INDEX_THREADS_CONF_KEY = "index.threads.max";
    public static final int DEFAULT_CONCURRENT_INDEX_THREADS = 16;
    public static final String INDEX_ROW_COUNTS_PER_TASK_CONF_KEY = "index.row.count.per.task";
    public static final int DEFAULT_INDEX_ROW_COUNTS_PER_TASK = 2048;

    /**
     * The states of the processing a page of index rows
     */
    private enum State {
        INITIAL, SCANNING_INDEX, SCANNING_DATA, SCANNING_DATA_INTERRUPTED, READY
    }
    State state = State.INITIAL;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UncoveredGlobalIndexRegionScanner.class);
    protected final byte[][] viewConstants;
    protected final RegionCoprocessorEnvironment env;
    protected byte[][] regionEndKeys;
    protected final Table dataHTable;
    protected final int pageSizeInRows;
    protected final int rowCountPerTask;
    protected final Scan scan;
    protected final Scan dataTableScan;
    protected final RegionScanner innerScanner;
    protected final Region region;
    protected final IndexMaintainer indexMaintainer;
    protected final TupleProjector tupleProjector;
    protected final ImmutableBytesWritable ptr;
    protected final TaskRunner pool;
    protected String exceptionMessage;
    protected final HTableFactory hTableFactory;
    protected List<List<Cell>> indexRows = null;
    protected Map<ImmutableBytesPtr, Result> dataRows = null;
    protected Iterator<List<Cell>> indexRowIterator = null;
    protected Map<byte[], byte[]> indexToDataRowKeyMap = null;
    protected int indexRowCount = 0;
    protected final long pageSizeMs;
    protected byte[] lastIndexRowKey = null;

    public UncoveredGlobalIndexRegionScanner(final RegionScanner innerScanner,
                                             final Region region,
                                             final Scan scan,
                                             final RegionCoprocessorEnvironment env,
                                             final Scan dataTableScan,
                                             final TupleProjector tupleProjector,
                                             final IndexMaintainer indexMaintainer,
                                             final byte[][] viewConstants,
                                             final ImmutableBytesWritable ptr,
                                             final long pageSizeMs)
            throws IOException {
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
        hTableFactory = IndexWriterUtils.getDefaultDelegateHTableFactory(env);
        rowCountPerTask = config.getInt(INDEX_ROW_COUNTS_PER_TASK_CONF_KEY,
                DEFAULT_INDEX_ROW_COUNTS_PER_TASK);

        pool = new WaitForCompletionTaskRunner(ThreadPoolManager.getExecutor(
                new ThreadPoolBuilder("Uncovered Global Index",
                        env.getConfiguration()).setMaxThread(NUM_CONCURRENT_INDEX_THREADS_CONF_KEY,
                        DEFAULT_CONCURRENT_INDEX_THREADS).setCoreTimeout(
                        INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env));
        byte[] dataTableName = scan.getAttribute(PHYSICAL_DATA_TABLE_NAME);
        dataHTable = hTableFactory.getTable(new ImmutableBytesPtr(dataTableName));
        try (org.apache.hadoop.hbase.client.Connection connection =
                     HBaseFactoryProvider.getHConnectionFactory().createConnection(
                             env.getConfiguration())) {
            regionEndKeys = connection.getRegionLocator(dataHTable.getName()).getEndKeys();
        }
    }

    @Override
    public long getMvccReadPoint() {
        return innerScanner.getMvccReadPoint();
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return false;
    }

    @Override
    public void close() throws IOException {
        innerScanner.close();
        hTableFactory.shutdown();
        if (dataHTable != null) {
            dataHTable.close();
        }
        this.pool.stop("UncoveredGlobalIndexRegionScanner is closing");
    }

    @Override
    public long getMaxResultSize() {
        return innerScanner.getMaxResultSize();
    }

    @Override
    public int getBatch() {
        return innerScanner.getBatch();
    }

    private void scanDataRows(Set<byte[]> dataRowKeys, long startTime) throws IOException {
        List<KeyRange> keys = new ArrayList<>(dataRowKeys.size());
        for (byte[] dataRowKey: dataRowKeys) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(dataRowKey));
        }
        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        Scan dataScan = new Scan(dataTableScan);
        dataScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        scanRanges.initializeScan(dataScan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        dataScan.setFilter(new SkipScanFilter(skipScanFilter, false));
        try (ResultScanner resultScanner = dataHTable.getScanner(dataScan)) {
            for (Result result = resultScanner.next(); (result != null);
                 result = resultScanner.next()) {
                dataRows.put(new ImmutableBytesPtr(result.getRow()), result);
                if ((EnvironmentEdgeManager.currentTimeMillis() - startTime) >= pageSizeMs) {
                    LOGGER.info("One of the scan tasks in UncoveredGlobalIndexRegionScanner"
                            + " for region " + region.getRegionInfo().getRegionNameAsString()
                            + " could not complete on time (in " + pageSizeMs+ " ms) and"
                            + " will be resubmitted");
                    state = State.SCANNING_DATA_INTERRUPTED;
                    break;
                }
            }
        } catch (Throwable t) {
            exceptionMessage = "scanDataRows fails for at least one task";
            ServerUtil.throwIOException(dataHTable.getName().toString(), t);
        }
    }

    private void addTasksForScanningDataTableRowsInParallel(TaskBatch<Boolean> tasks,
                                                            final Set<byte[]> dataRowKeys,
                                                            final long startTime) {
        tasks.add(new Task<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    //in HBase 1.x we could check if the coproc environment was closed or aborted,
                    //but in HBase 2.x the coproc environment can't check region server services
                    if (Thread.currentThread().isInterrupted()) {
                        exceptionMessage = "Pool closed, not retrieving data table rows for "
                                + region.getRegionInfo().getRegionNameAsString();
                        throw new IOException(exceptionMessage);
                    }
                    scanDataRows(dataRowKeys, startTime);
                } catch (Exception e) {
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }


    protected void submitTasks(TaskBatch<Boolean> tasks) throws IOException {
        Pair<List<Boolean>, List<Future<Boolean>>> resultsAndFutures = null;
        try {
            LOGGER.debug("Waiting on index tasks to complete...");
            resultsAndFutures = this.pool.submitUninterruptible(tasks);
        } catch (ExecutionException e) {
            throw new RuntimeException(
                    "Should not fail on the results while using a WaitForCompletionTaskRunner", e);
        } catch (EarlyExitFailure e) {
            throw new RuntimeException("Stopped while waiting for batch, quitting!", e);
        }
        int index = 0;
        for (Boolean result : resultsAndFutures.getFirst()) {
            if (result == null) {
                Throwable cause = ServerUtil.getExceptionFromFailedFuture(
                        resultsAndFutures.getSecond().get(index));
                // there was a failure
                throw new IOException(exceptionMessage == null ? "" : exceptionMessage, cause);
            }
            index++;
        }
    }

    private void scanDataTableRows(long startTime)
            throws IOException {
        if (indexToDataRowKeyMap.size() == 0) {
            state = State.READY;
            return;
        }
        TreeSet<byte[]> dataRowKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (byte[] dataRowKey: indexToDataRowKeyMap.values()) {
            dataRowKeys.add(dataRowKey);
        }
        List<Set<byte[]>> setList = IndexRepairRegionScanner.getPerTaskDataRowKeys(dataRowKeys,
                regionEndKeys, rowCountPerTask);
        int taskCount = setList.size();
        TaskBatch<Boolean> tasks = new TaskBatch<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            addTasksForScanningDataTableRowsInParallel(tasks, setList.get(i), startTime);
        }
        submitTasks(tasks);
        if (state == State.SCANNING_DATA_INTERRUPTED) {
            state = State.SCANNING_DATA;
        } else {
            state = State.READY;
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
                    indexRows = new ArrayList();
                    dataRows = Maps.newConcurrentMap();
                    indexToDataRowKeyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                    state = State.SCANNING_INDEX;
                }
                if (state == State.SCANNING_INDEX) {
                    do {
                        List<Cell> row = new ArrayList<Cell>();
                        hasMore = innerScanner.nextRaw(row);
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
                            lastIndexRowKey = CellUtil.cloneRow(row.get(0));
                            indexToDataRowKeyMap.put(lastIndexRowKey, indexMaintainer.buildDataRowKey(
                                    new ImmutableBytesWritable(lastIndexRowKey), viewConstants));
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
                    state = State.SCANNING_DATA;
                }
                if (state == State.SCANNING_DATA) {
                    scanDataTableRows(startTime);
                    indexRowIterator = indexRows.iterator();
                }
                if (state == State.READY) {
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
                                // The data row does not exist, we should skip this index row. This can happen
                                // if index row is replicated but the data row has not been
                                result.clear();
                            }
                        } catch (Throwable e) {
                            LOGGER.error("Exception in UncoveredGlobalIndexRegionScanner for region "
                                    + region.getRegionInfo().getRegionNameAsString(), e);
                            throw e;
                        }
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    getDummyResult(lastIndexRowKey, result);
                    return true;
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Exception in UncoveredGlobalIndexRegionScanner for region "
                    + region.getRegionInfo().getRegionNameAsString(), e);
            throw e;
        } finally {
            region.closeRegionOperation();
        }
    }
}
