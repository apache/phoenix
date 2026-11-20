/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.CDC_DATA_TABLE_DEF;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.PHYSICAL_DATA_TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compat.hbase.CompatScanMetrics;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.parallel.EarlyExitFailure;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.TaskRunner;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an index table region scanner which scans index table rows locally and then extracts data
 * table row keys from them. Using the data table row keys, the data table rows are scanned using
 * the HBase client available to region servers.
 */
public class UncoveredGlobalIndexRegionScanner extends UncoveredIndexRegionScanner {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(UncoveredGlobalIndexRegionScanner.class);
  public static final String NUM_CONCURRENT_INDEX_THREADS_CONF_KEY = "phoenix.index.threads.max";
  public static final String INDEX_ROW_COUNTS_PER_TASK_CONF_KEY =
    "phoenix.index.row.count.per.task";
  public static final int DEFAULT_INDEX_ROW_COUNTS_PER_TASK = 2048;
  /**
   * Server-only scan attribute that holds the row limit of the client page filter. The region
   * observer sets or removes this attribute before HBase opens the region scanner. Thus a value
   * from the client has no effect on the scans that the region observer opens.
   */
  static final String PAGE_SIZE_ATTRIB = "_UncoveredGlobalIndexPageSize";

  protected final Table dataHTable;
  protected final int rowCountPerTask;
  // Parallel data scan tasks can set this field, so it is volatile.
  protected volatile String exceptionMessage;
  protected final HTableFactory hTableFactory;
  private final boolean isScanMetricsEnabled;
  // Parallel data scan tasks add to this queue, so the queue must be thread safe.
  private Queue<DataTableScanMetricsWithScanTime> dataTableScanMetrics;
  private final long maxRowCount;
  private long returnedRowCount;

  // This relies on Hadoop Configuration to handle warning about deprecated configs and
  // to set the correct non-deprecated configs when an old one shows up.
  static {
    Configuration.addDeprecation("index.threads.max", NUM_CONCURRENT_INDEX_THREADS_CONF_KEY);
    Configuration.addDeprecation("index.row.count.per.task", INDEX_ROW_COUNTS_PER_TASK_CONF_KEY);
  }

  public UncoveredGlobalIndexRegionScanner(final RegionScanner innerScanner, final Region region,
    final Scan scan, final RegionCoprocessorEnvironment env, final Scan dataTableScan,
    final TupleProjector tupleProjector, final IndexMaintainer indexMaintainer,
    final byte[][] viewConstants, final ImmutableBytesWritable ptr, final long pageSizeMs,
    final long queryLimit, boolean isDistinct) throws IOException {
    // The join with the data table can reject index rows. This scanner owns the row limit, so
    // only verified rows count toward it. Usually the region observer removes the page filter
    // before HBase opens the region scanner. If a page filter remains, this scanner removes it
    // and reopens the region scanner before TTLRegionScanner can take the page filter.
    this(innerScanner, region, scan, env, dataTableScan, tupleProjector, indexMaintainer,
      viewConstants, ptr, pageSizeMs, queryLimit, isDistinct,
      !indexMaintainer.isCDCIndex() ? ScanUtil.removePageFilter(scan) : null);
  }

  private UncoveredGlobalIndexRegionScanner(final RegionScanner innerScanner, final Region region,
    final Scan scan, final RegionCoprocessorEnvironment env, final Scan dataTableScan,
    final TupleProjector tupleProjector, final IndexMaintainer indexMaintainer,
    final byte[][] viewConstants, final ImmutableBytesWritable ptr, final long pageSizeMs,
    final long queryLimit, boolean isDistinct, PageFilter pageFilter) throws IOException {
    // Keep a stable delegate for the superclass, and reopen only inside the guarded body.
    // If superclass initialization fails, the caller still owns the original scanner.
    super(pageFilter == null ? innerScanner : new DelegateRegionScanner(innerScanner), region, scan,
      env, dataTableScan, tupleProjector, indexMaintainer, viewConstants, ptr, pageSizeMs,
      getRowLimit(scan, pageFilter, queryLimit), isDistinct);
    maxRowCount = getRowLimit(scan, pageFilter, Long.MAX_VALUE);
    RegionScanner reopened = null;
    HTableFactory tableFactory = null;
    Table table = null;
    try {
      if (pageFilter != null) {
        reopened = reopenScannerWithoutPageFilter(innerScanner, scan, pageFilter);
        ((DelegateRegionScanner) this.innerScanner).delegate = reopened;
      }
      final Configuration config = env.getConfiguration();
      tableFactory = IndexWriterUtils.getDefaultDelegateHTableFactory(env);
      rowCountPerTask =
        config.getInt(INDEX_ROW_COUNTS_PER_TASK_CONF_KEY, DEFAULT_INDEX_ROW_COUNTS_PER_TASK);
      byte[] dataTableName = scan.getAttribute(PHYSICAL_DATA_TABLE_NAME);
      table = tableFactory.getTable(new ImmutableBytesPtr(dataTableName));
      // Include the empty column to verify the index row against its data row and repair it.
      ScanUtil.addEmptyColumnToScan(dataTableScan, indexMaintainer.getDataEmptyKeyValueCF(),
        indexMaintainer.getEmptyKeyValueQualifierForDataTable());
      isScanMetricsEnabled =
        scan.isScanMetricsEnabled() && CompatScanMetrics.supportsFineGrainedReadMetrics();
      if (isScanMetricsEnabled) {
        dataTableScanMetrics = new ConcurrentLinkedQueue<>();
      }
      hTableFactory = tableFactory;
      dataHTable = table;
    } catch (IOException | RuntimeException | Error failure) {
      // Until construction succeeds, the caller cannot see or close the replacement scanner.
      // The caller still owns the original scanner, so close only what was opened here.
      if (reopened != null) {
        closeOnFailure(reopened, failure);
      }
      if (table != null) {
        closeOnFailure(table, failure);
      }
      if (tableFactory != null) {
        closeOnFailure(tableFactory::shutdown, failure);
      }
      throw failure;
    }
  }

  /**
   * Moves the row limit of the client page filter into a server-only scan attribute. The region
   * observer calls this method before HBase opens the region scanner. This scanner then owns the
   * row limit and does not replace the region scanner. HBase ships only the scanner that it opens.
   * Only a shipped scanner switches from pread to stream read on a long scan.
   */
  static void removePageFilterBeforeScannerOpen(Scan scan) {
    PageFilter pageFilter = ownsRowLimit(scan) ? ScanUtil.removePageFilter(scan) : null;
    scan.setAttribute(PAGE_SIZE_ATTRIB,
      pageFilter == null ? null : Bytes.toBytes(pageFilter.getPageSize()));
  }

  /**
   * Returns true if this scanner owns the row limit of the scan. These scan attributes select this
   * scanner class in the region scanner factory. This scanner can reject an index row after the
   * index scan returns it, so the row limit must apply after the join.
   */
  public static boolean ownsRowLimit(Scan scan) {
    return ScanUtil.isUncoveredGlobalIndex(scan) && !ScanUtil.isLocalIndex(scan)
      && scan.getAttribute(CDC_DATA_TABLE_DEF) == null;
  }

  private static long getRowLimit(Scan scan, PageFilter pageFilter, long defaultLimit) {
    if (pageFilter != null) {
      return pageFilter.getPageSize();
    }
    byte[] pageSizeBytes = scan.getAttribute(PAGE_SIZE_ATTRIB);
    return pageSizeBytes == null ? defaultLimit : Bytes.toLong(pageSizeBytes);
  }

  private static void closeOnFailure(AutoCloseable resource, Throwable failure) {
    try {
      resource.close();
    } catch (Throwable cleanupFailure) {
      if (cleanupFailure != failure) {
        failure.addSuppressed(cleanupFailure);
      }
    }
  }

  private static RegionScanner reopenScannerWithoutPageFilter(RegionScanner scanner, Scan scan,
    PageFilter pageFilter) throws IOException {
    if (pageFilter == null) {
      return scanner;
    }
    try {
      DelegateRegionScanner delegate = (DelegateRegionScanner) scanner;
      scanner.close();
      return delegate.getNewRegionScanner(scan);
    } catch (ClassCastException e) {
      throw new DoNotRetryIOException(e);
    }
  }

  @Override
  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    if (returnedRowCount >= maxRowCount) {
      result.clear();
      return false;
    }
    boolean hasMore = super.next(result, scannerContext);
    if (!result.isEmpty() && !ScanUtil.isDummy(result)) {
      returnedRowCount++;
      if (returnedRowCount >= maxRowCount) {
        return false;
      }
    }
    return hasMore;
  }

  /**
   * The constructor adds the data empty column to the data table scan for every index, not only for
   * an uncovered index. A covered index in a server merge needs it too. A missing data row then
   * means that the row does not exist, so the merge can skip its index row.
   */
  @Override
  protected boolean scansDataEmptyColumn() {
    return true;
  }

  @Override
  public void close() throws IOException {
    innerScanner.close();
    hTableFactory.shutdown();
    if (dataHTable != null) {
      dataHTable.close();
    }
  }

  protected void scanDataRows(Collection<byte[]> dataRowKeys, long startTime) throws IOException {
    Scan dataScan = prepareDataTableScan(dataRowKeys);
    if (dataScan == null) {
      return;
    }
    dataScan.setScanMetricsEnabled(isScanMetricsEnabled);
    try (ResultScanner resultScanner = dataHTable.getScanner(dataScan)) {
      for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
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
        LOGGER.info("One of the scan tasks in UncoveredGlobalIndexRegionScanner" + " for region "
          + region.getRegionInfo().getRegionNameAsString() + " could not complete on time (in "
          + pageSizeMs + " ms) and" + " will be resubmitted");
      }
      if (isScanMetricsEnabled) {
        ScanMetrics scanMetrics = resultScanner.getScanMetrics();
        long scanTimeInMs = EnvironmentEdgeManager.currentTimeMillis() - startTime;
        // Capture scan time to identify slowest parallel scan later as that's the one which slows
        // down the whole merge operation from data table.
        dataTableScanMetrics.add(buildDataTableScanMetrics(scanMetrics, scanTimeInMs));
      }
    } catch (Throwable t) {
      exceptionMessage = "scanDataRows fails for at least one task";
      ClientUtil.throwIOException(dataHTable.getName().toString(), t);
    }
  }

  private void addTasksForScanningDataTableRowsInParallel(TaskBatch<Boolean> tasks,
    final Set<byte[]> dataRowKeys, final long startTime) {
    tasks.add(new Task<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          // in HBase 1.x we could check if the coproc environment was closed or aborted,
          // but in HBase 2.x the coproc environment can't check region server services
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
      TaskRunner pool = PhoenixRegionServerEndpoint.getUncoveredIndexThreadPool();
      if (pool == null) {
        throw new IOException(
          "PhoenixRegionServerEndpoint should be loaded to use Uncovered Indexes.");
      }
      resultsAndFutures = pool.submitUninterruptible(tasks);
    } catch (ExecutionException e) {
      throw new RuntimeException(
        "Should not fail on the results while using a WaitForCompletionTaskRunner", e);
    } catch (EarlyExitFailure e) {
      throw new RuntimeException("Stopped while waiting for batch, quitting!", e);
    }
    int index = 0;
    for (Boolean result : resultsAndFutures.getFirst()) {
      if (result == null) {
        Throwable cause =
          ServerUtil.getExceptionFromFailedFuture(resultsAndFutures.getSecond().get(index));
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
    for (byte[] dataRowKey : indexToDataRowKeyMap.values()) {
      dataRowKeys.add(dataRowKey);
    }
    List<Set<byte[]>> setList = IndexRepairRegionScanner.getPerTaskDataRowKeys(dataRowKeys,
      getRegionEndKeys(dataRowKeys), rowCountPerTask);
    int taskCount = setList.size();
    TaskBatch<Boolean> tasks = new TaskBatch<>(taskCount);
    for (int i = 0; i < taskCount; i++) {
      addTasksForScanningDataTableRowsInParallel(tasks, setList.get(i), startTime);
    }
    if (isScanMetricsEnabled) {
      // Each batch reports only the slowest data scan among its own tasks.
      dataTableScanMetrics.clear();
    }
    submitTasks(tasks);
    if (isScanMetricsEnabled) {
      DataTableScanMetricsWithScanTime dataTableScanMetricsForSlowestScan = null;
      for (DataTableScanMetricsWithScanTime dataTableScanMetrics : dataTableScanMetrics) {
        if (dataTableScanMetricsForSlowestScan == null) {
          dataTableScanMetricsForSlowestScan = dataTableScanMetrics;
        } else if (
          dataTableScanMetricsForSlowestScan.getScanTimeInMs()
              < dataTableScanMetrics.getScanTimeInMs()
        ) {
          dataTableScanMetricsForSlowestScan = dataTableScanMetrics;
        }
      }
      if (dataTableScanMetricsForSlowestScan != null) {
        dataTableScanMetricsForSlowestScan.populateThreadLocalServerSideScanMetrics();
      }
    }
    if (state == State.SCANNING_DATA_INTERRUPTED) {
      state = State.SCANNING_DATA;
    } else {
      state = State.READY;
    }
  }

  // Returns the end keys of the data table regions that hold the given row keys, in key order. The
  // locations come from the location cache of the connection. A cache miss reads only the meta row
  // of that region, not the meta rows of all the regions of the table. A stale location changes
  // only how the row keys go into tasks. Each task scans through the client, so its scan is correct
  // across region boundaries
  private byte[][] getRegionEndKeys(TreeSet<byte[]> dataRowKeys) throws IOException {
    List<byte[]> endKeys = new ArrayList<>();
    try (RegionLocator locator =
      hTableFactory.getConnection().getRegionLocator(dataHTable.getName())) {
      byte[] rowKey = dataRowKeys.first();
      while (rowKey != null) {
        byte[] endKey = locator.getRegionLocation(rowKey).getRegion().getEndKey();
        endKeys.add(endKey);
        // A location holds its row key, so its end key is after the row key. The check stops the
        // walk at a location that does not hold its row key. Then one task set holds the rest
        rowKey = endKey.length == 0 || Bytes.compareTo(endKey, rowKey) <= 0
          ? null
          : dataRowKeys.ceiling(endKey);
      }
    }
    return endKeys.toArray(new byte[endKeys.size()][]);
  }

  private static DataTableScanMetricsWithScanTime buildDataTableScanMetrics(ScanMetrics scanMetrics,
    long scanTimeInMs) {
    DataTableScanMetricsWithScanTime.Builder builder =
      new DataTableScanMetricsWithScanTime.Builder();
    builder.setScanTimeInMs(scanTimeInMs);
    DataTableScanMetrics.populateDataTableScanMetrics(scanMetrics, builder);
    return builder.build();
  }

  private static class DataTableScanMetricsWithScanTime extends DataTableScanMetrics {
    private final long scanTimeInMs;

    DataTableScanMetricsWithScanTime(long scanTimeInMs, long fsReadTimeInMs, long bytesReadFromFS,
      long bytesReadFromMemstore, long bytesReadFromBlockcache, long blockReadOps) {
      super(fsReadTimeInMs, bytesReadFromFS, bytesReadFromMemstore, bytesReadFromBlockcache,
        blockReadOps);
      this.scanTimeInMs = scanTimeInMs;
    }

    long getScanTimeInMs() {
      return scanTimeInMs;
    }

    private static class Builder extends DataTableScanMetrics.Builder {
      private long scanTimeInMs = 0;

      public Builder setScanTimeInMs(long scanTimeInMs) {
        this.scanTimeInMs = scanTimeInMs;
        return this;
      }

      @Override
      public DataTableScanMetricsWithScanTime build() {
        return new DataTableScanMetricsWithScanTime(scanTimeInMs, fsReadTimeInMs, bytesReadFromFS,
          bytesReadFromMemstore, bytesReadFromBlockcache, blockReadOps);
      }
    }
  }
}
