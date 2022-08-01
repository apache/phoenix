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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.parallel.EarlyExitFailure;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.TaskRunner;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an index table region scanner which scans index table rows locally and then extracts
 * data table row keys from them. Using the data table row keys, the data table rows are scanned
 * using the HBase client available to region servers.
 */
public class UncoveredGlobalIndexRegionScanner extends UncoveredIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UncoveredGlobalIndexRegionScanner.class);
    public static final String NUM_CONCURRENT_INDEX_THREADS_CONF_KEY = "index.threads.max";
    public static final int DEFAULT_CONCURRENT_INDEX_THREADS = 16;
    public static final String INDEX_ROW_COUNTS_PER_TASK_CONF_KEY = "index.row.count.per.task";
    public static final int DEFAULT_INDEX_ROW_COUNTS_PER_TASK = 2048;

    protected byte[][] regionEndKeys;
    protected final Table dataHTable;
    protected final int rowCountPerTask;
    protected final TaskRunner pool;
    protected String exceptionMessage;
    protected final HTableFactory hTableFactory;

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
        super(innerScanner, region, scan, env, dataTableScan, tupleProjector, indexMaintainer,
                viewConstants, ptr, pageSizeMs);
        final Configuration config = env.getConfiguration();
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
    public void close() throws IOException {
        innerScanner.close();
        hTableFactory.shutdown();
        if (dataHTable != null) {
            dataHTable.close();
        }
        this.pool.stop("UncoveredGlobalIndexRegionScanner is closing");
    }

    protected void scanDataRows(Collection<byte[]> dataRowKeys, long startTime) throws IOException {
        Scan dataScan = prepareDataTableScan(dataRowKeys);
        try (ResultScanner resultScanner = dataHTable.getScanner(dataScan)) {
            for (Result result = resultScanner.next(); (result != null);
                 result = resultScanner.next()) {
                if (ScanUtil.isDummy(result)) {
                    state = State.SCANNING_DATA_INTERRUPTED;
                    break;
                }
                dataRows.put(new ImmutableBytesPtr(result.getRow()), result);
                if ((EnvironmentEdgeManager.currentTimeMillis() - startTime) >= pageSizeMs) {
                    state = State.SCANNING_DATA_INTERRUPTED;
                    break;
                }
            }
            if (state == State.SCANNING_DATA_INTERRUPTED) {
                LOGGER.info("One of the scan tasks in UncoveredGlobalIndexRegionScanner"
                        + " for region " + region.getRegionInfo().getRegionNameAsString()
                        + " could not complete on time (in " + pageSizeMs + " ms) and"
                        + " will be resubmitted");
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

    @Override
    protected void scanDataTableRows(long startTime) throws IOException {
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
}
