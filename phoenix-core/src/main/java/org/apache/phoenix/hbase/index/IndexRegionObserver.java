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
package org.apache.phoenix.hbase.index;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver.ReplayWrite;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.LockManager.RowLock;
import org.apache.phoenix.hbase.index.builder.FatalIndexBuildingFailureException;
import org.apache.phoenix.hbase.index.builder.IndexBuildManager;
import org.apache.phoenix.hbase.index.builder.IndexBuilder;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.LazyParallelWriterIndexCommitter;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.adjustGlobalIndexDeleteMutation;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.getCollection;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.getTimestamp;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.merge;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.mergeNew;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.prepareIndexMutationsForRebuild;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.removeColumn;
import static org.apache.phoenix.hbase.index.util.IndexManagementUtil.rethrowIndexingException;
import static org.apache.phoenix.query.QueryConstants.EMPTY_COLUMN_VALUE_BYTES;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make.
 * We don't need to implement {@link #postPut(ObserverContext, Put, WALEdit, Durability)} and
 * {@link #postDelete(ObserverContext, Delete, WALEdit, Durability)} hooks because
 * Phoenix always does batch mutations.
 * <p>
 */
public class IndexRegionObserver implements RegionObserver, RegionCoprocessor {

  private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class);
  private static final OperationStatus IGNORE = new OperationStatus(OperationStatusCode.SUCCESS);
  private static final OperationStatus NOWRITE = new OperationStatus(OperationStatusCode.SUCCESS);
    protected static final byte VERIFIED_BYTE = 1;
    protected static final byte UNVERIFIED_BYTE = 2;
    public static final byte[] UNVERIFIED_BYTES = new byte[] { UNVERIFIED_BYTE };
    public static final byte[] VERIFIED_BYTES = new byte[] { VERIFIED_BYTE };

  /**
   * Class to represent pending data table rows
   */
  private static class PendingRow {
      private boolean concurrent = false;
      private long count = 1;

      public void add() {
          count++;
          concurrent = true;
      }

      public void remove() {
          count--;
      }

      public long getCount() {
          return count;
      }

      public boolean isConcurrent() {
          return concurrent;
      }
  }

  private static boolean ignoreIndexRebuildForTesting  = false;
  private static boolean failPreIndexUpdatesForTesting = false;
  private static boolean failPostIndexUpdatesForTesting = false;
  private static boolean failDataTableUpdatesForTesting = false;

  public static void setIgnoreIndexRebuildForTesting(boolean ignore) { ignoreIndexRebuildForTesting = ignore; }

  public static void setFailPreIndexUpdatesForTesting(boolean fail) { failPreIndexUpdatesForTesting = fail; }

  public static void setFailPostIndexUpdatesForTesting(boolean fail) { failPostIndexUpdatesForTesting = fail; }

  public static void setFailDataTableUpdatesForTesting(boolean fail) {
      failDataTableUpdatesForTesting = fail;
  }

  // Hack to get around not being able to save any state between
  // coprocessor calls. TODO: remove after HBASE-18127 when available
  private static class BatchMutateContext {
      private final int clientVersion;
      // The collection of index mutations that will be applied before the data table mutations. The empty column (i.e.,
      // the verified column) will have the value false ("unverified") on these mutations
      private ListMultimap<HTableInterfaceReference, Mutation> preIndexUpdates;
      // The collection of index mutations that will be applied after the data table mutations. The empty column (i.e.,
      // the verified column) will have the value true ("verified") on the put mutations
      private ListMultimap<HTableInterfaceReference, Mutation> postIndexUpdates;
      // The collection of candidate index mutations that will be applied after the data table mutations
      private ListMultimap<HTableInterfaceReference, Pair<Mutation, byte[]>> indexUpdates;
      private List<RowLock> rowLocks = Lists.newArrayListWithExpectedSize(QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
      private HashSet<ImmutableBytesPtr> rowsToLock = new HashSet<>();
      private boolean rebuild;
      // The current state of the data rows corresponding to the pending mutations
      private HashMap<ImmutableBytesPtr, Put> currentDataRowStates;
      // The merged state of the current data rows and the pending mutations
      private HashMap<ImmutableBytesPtr, Put> nextDataRowStates;
      private BatchMutateContext(int clientVersion) {
          this.clientVersion = clientVersion;
      }
  }

  private ThreadLocal<BatchMutateContext> batchMutateContext =
          new ThreadLocal<BatchMutateContext>();

  /** Configuration key for the {@link IndexBuilder} to use */
  public static final String INDEX_BUILDER_CONF_KEY = "index.builder";

  /**
   * Configuration key for if the indexer should check the version of HBase is running. Generally,
   * you only want to ignore this for testing or for custom versions of HBase.
   */
  public static final String CHECK_VERSION_CONF_KEY = "com.saleforce.hbase.index.checkversion";

  public static final String INDEX_LAZY_POST_BATCH_WRITE = "org.apache.hadoop.hbase.index.lazy.post_batch.write";
  private static final boolean INDEX_LAZY_POST_BATCH_WRITE_DEFAULT = false;

  private static final String INDEXER_INDEX_WRITE_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.post.batch.mutate.threshold";
  private static final long INDEXER_INDEX_WRITE_SLOW_THRESHOLD_DEFAULT = 3_000;
  private static final String INDEXER_PRE_INCREMENT_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.pre.increment";
  private static final long INDEXER_PRE_INCREMENT_SLOW_THRESHOLD_DEFAULT = 3_000;

  // Index writers get invoked before and after data table updates
  protected IndexWriter preWriter;
  protected IndexWriter postWriter;

  protected IndexBuildManager builder;
  private LockManager lockManager;

  // The collection of pending data table rows
  private Map<ImmutableBytesPtr, PendingRow> pendingRows = new ConcurrentHashMap<>();

  private MetricsIndexerSource metricSource;

  private boolean stopped;
  private boolean disabled;
  private long slowIndexPrepareThreshold;
  private long slowPreIncrementThreshold;
  private int rowLockWaitDuration;

  private static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
      try {
        final RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
        String serverName = env.getServerName().getServerName();
        if (env.getConfiguration().getBoolean(CHECK_VERSION_CONF_KEY, true)) {
          // make sure the right version <-> combinations are allowed.
          String errormsg = Indexer.validateVersion(env.getHBaseVersion(), env.getConfiguration());
          if (errormsg != null) {
              throw new FatalIndexBuildingFailureException(errormsg);
          }
        }

        this.builder = new IndexBuildManager(env);
        // Clone the config since it is shared
        DelegateRegionCoprocessorEnvironment indexWriterEnv = new DelegateRegionCoprocessorEnvironment(env, ConnectionType.INDEX_WRITER_CONNECTION);
        // setup the actual index preWriter
        this.preWriter = new IndexWriter(indexWriterEnv, serverName + "-index-preWriter", false);
        if (env.getConfiguration().getBoolean(INDEX_LAZY_POST_BATCH_WRITE, INDEX_LAZY_POST_BATCH_WRITE_DEFAULT)) {
            this.postWriter = new IndexWriter(indexWriterEnv, new LazyParallelWriterIndexCommitter(), serverName + "-index-postWriter", false);
        }
        else {
            this.postWriter = this.preWriter;
        }

        this.rowLockWaitDuration = env.getConfiguration().getInt("hbase.rowlock.wait.duration",
                DEFAULT_ROWLOCK_WAIT_DURATION);
        this.lockManager = new LockManager();

        // Metrics impl for the Indexer -- avoiding unnecessary indirection for hadoop-1/2 compat
        this.metricSource = MetricsIndexerSourceFactory.getInstance().getIndexerSource();
        setSlowThresholds(e.getConfiguration());

      } catch (NoSuchMethodError ex) {
          disabled = true;
          LOG.error("Must be too early a version of HBase. Disabled coprocessor ", ex);
      }
  }

  /**
   * Extracts the slow call threshold values from the configuration.
   */
  private void setSlowThresholds(Configuration c) {
      slowIndexPrepareThreshold = c.getLong(INDEXER_INDEX_WRITE_SLOW_THRESHOLD_KEY,
          INDEXER_INDEX_WRITE_SLOW_THRESHOLD_DEFAULT);
      slowPreIncrementThreshold = c.getLong(INDEXER_PRE_INCREMENT_SLOW_THRESHOLD_KEY,
          INDEXER_PRE_INCREMENT_SLOW_THRESHOLD_DEFAULT);
  }

  private String getCallTooSlowMessage(String callName, long duration, long threshold) {
      StringBuilder sb = new StringBuilder(64);
      sb.append("(callTooSlow) ").append(callName).append(" duration=").append(duration);
      sb.append("ms, threshold=").append(threshold).append("ms");
      return sb.toString();
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    if (this.stopped) {
      return;
    }
    if (this.disabled) {
        return;
      }
    this.stopped = true;
    String msg = "Indexer is being stopped";
    this.builder.stop(msg);
    this.preWriter.stop(msg);
    this.postWriter.stop(msg);
  }

  /**
   * We use an Increment to serialize the ON DUPLICATE KEY clause so that the HBase plumbing
   * sets up the necessary locks and mvcc to allow an atomic update. The Increment is not a
   * real increment, though, it's really more of a Put. We translate the Increment into a
   * list of mutations, at most a single Put and Delete that are the changes upon executing
   * the list of ON DUPLICATE KEY clauses for this row.
   */
  @Override
  public Result preIncrementAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> e,
          final Increment inc) throws IOException {
      long start = EnvironmentEdgeManager.currentTimeMillis();
      try {
          List<Mutation> mutations = this.builder.executeAtomicOp(inc);
          if (mutations == null) {
              return null;
          }

          // Causes the Increment to be ignored as we're committing the mutations
          // ourselves below.
          e.bypass();
          // ON DUPLICATE KEY IGNORE will return empty list if row already exists
          // as no action is required in that case.
          if (!mutations.isEmpty()) {
              Region region = e.getEnvironment().getRegion();
              // Otherwise, submit the mutations directly here
                region.batchMutate(mutations.toArray(new Mutation[0]));
          }
          return Result.EMPTY_RESULT;
      } catch (Throwable t) {
          throw ServerUtil.createIOException(
                  "Unable to process ON DUPLICATE IGNORE for " +
                  e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString() +
                  "(" + Bytes.toStringBinary(inc.getRow()) + ")", t);
      } finally {
          long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
          if (duration >= slowIndexPrepareThreshold) {
              if (LOG.isDebugEnabled()) {
                  LOG.debug(getCallTooSlowMessage("preIncrementAfterRowLock", duration, slowPreIncrementThreshold));
              }
              metricSource.incrementSlowDuplicateKeyCheckCalls();
          }
          metricSource.updateDuplicateKeyCheckTime(duration);
      }
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      if (this.disabled) {
          return;
      }
      try {
          preBatchMutateWithExceptions(c, miniBatchOp);
          return;
      } catch (Throwable t) {
          rethrowIndexingException(t);
      }
      throw new RuntimeException(
        "Somehow didn't return an index update but also didn't propagate the failure to the client!");
  }

  public static long getMaxTimestamp(Mutation m) {
      long maxTs = 0;
      long ts;
      for (List<Cell> cells : m.getFamilyCellMap().values()) {
          for (Cell cell : cells) {
              ts = cell.getTimestamp();
              if (ts > maxTs) {
                  maxTs = ts;
              }
          }
      }
      return maxTs;
  }

  private void ignoreAtomicOperations (MiniBatchOperationInProgress<Mutation> miniBatchOp) {
      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          if (this.builder.isAtomicOp(m)) {
              miniBatchOp.setOperationStatus(i, IGNORE);
              continue;
          }
      }
  }

  private void populateRowsToLock(MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context) {
      for (int i = 0; i < miniBatchOp.size(); i++) {
          if (miniBatchOp.getOperationStatus(i) == IGNORE) {
              continue;
          }
          Mutation m = miniBatchOp.getOperation(i);
          if (this.builder.isEnabled(m)) {
              ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
              if (!context.rowsToLock.contains(row)) {
                  context.rowsToLock.add(row);
              }
          }
      }
  }

  private void lockRows(BatchMutateContext context) throws IOException {
      for (ImmutableBytesPtr rowKey : context.rowsToLock) {
          context.rowLocks.add(lockManager.lockRow(rowKey, rowLockWaitDuration));
      }
  }

  private void populatePendingRows(BatchMutateContext context) {
      for (RowLock rowLock : context.rowLocks) {
          ImmutableBytesPtr rowKey = rowLock.getRowKey();
          PendingRow pendingRow = pendingRows.get(rowKey);
          if (pendingRow == null) {
              pendingRows.put(rowKey, new PendingRow());
          } else {
              // m is a mutation on a row that has already a pending mutation in progress from another batch
              pendingRow.add();
          }
      }
  }

  public static void setTimestamp(Mutation m, long ts) throws IOException {
      for (List<Cell> cells : m.getFamilyCellMap().values()) {
          for (Cell cell : cells) {
              CellUtil.setTimestamp(cell, ts);
          }
      }
  }

  /**
   * When there are delete and put mutations on the data same row within the same batch, this method applies deletes
   * on put mutations, that is, effectively merges them.
   */
  private void applyPendingDeleteMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                           BatchMutateContext context,
                                           Map<ImmutableBytesPtr, Integer> pendingPuts) throws IOException {
      for (Integer i = 0; i < miniBatchOp.size(); i++) {
          if (miniBatchOp.getOperationStatus(i) == IGNORE || miniBatchOp.getOperationStatus(i) == NOWRITE) {
              continue;
          }
          Mutation m = miniBatchOp.getOperation(i);
          if (!this.builder.isEnabled(m)) {
              continue;
          }
          if (!(m instanceof Delete)) {
              continue;
          }
          ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(m.getRow());
          Integer opIndex = pendingPuts.get(rowKeyPtr);
          Put mergedRow = context.nextDataRowStates.get(rowKeyPtr);
          if (opIndex == null && mergedRow == null) {
              continue;
          }
          Put put = (opIndex != null) ? (Put) miniBatchOp.getOperation(opIndex) : null;
          for (List<Cell> cells : m.getFamilyCellMap().values()) {
              for (Cell cell : cells) {
                  switch (cell.getType()) {
                      case DeleteFamily:
                      case DeleteFamilyVersion:
                          if (put != null) {
                              put.getFamilyCellMap().remove(CellUtil.cloneFamily(cell));
                          }
                          if (put != mergedRow) {
                              mergedRow.getFamilyCellMap().remove(CellUtil.cloneFamily(cell));
                          }
                          break;
                      case DeleteColumn:
                      case Delete:
                          if (put != null) {
                              removeColumn(put, cell);
                          }
                          if (put != mergedRow) {
                              removeColumn(mergedRow, cell);
                          }
                  }
                  if (put != null && put.getFamilyCellMap().size() == 0) {
                      pendingPuts.remove(rowKeyPtr);
                      miniBatchOp.setOperationStatus(opIndex, NOWRITE);
                  }
                  if (mergedRow != null && mergedRow.getFamilyCellMap().size() == 0) {
                      context.nextDataRowStates.remove(rowKeyPtr);
                  }
              }
          }
      }
  }

  private void populateMutationList(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                    Collection<Mutation> mutationList, boolean isPut) {
      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          if ((isPut && m instanceof Put) || (!isPut && m instanceof Delete)) {
              if (miniBatchOp.getOperationStatus(i) != IGNORE &&
                      miniBatchOp.getOperationStatus(i) != NOWRITE && this.builder.isEnabled(m)) {
                  mutationList.add(m);
              }
          }
      }
  }

    /**
     * When there are multiple put mutations on the data same row within the same batch, this method merges them into
     * one mutation.
     */
    private void mergePendingPutMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                          Map<ImmutableBytesPtr, Integer> pendingPuts,
                                          long now) throws IOException {
        for (Integer i = 0; i < miniBatchOp.size(); i++) {
            if (miniBatchOp.getOperationStatus(i) == IGNORE) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            // skip this mutation if we aren't enabling indexing
            if (!this.builder.isEnabled(m)) {
                continue;
            }
            // Unless we're replaying edits to rebuild the index, we update the time stamp
            // of the data table to prevent overlapping time stamps (which prevents index
            // inconsistencies as this case isn't handled correctly currently).
            setTimestamp(m, now);
            if (m instanceof Put) {
                ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(m.getRow());
                Integer opIndex = pendingPuts.get(rowKeyPtr);
                if (opIndex != null) {
                    merge((Put) m, (Put) miniBatchOp.getOperation(opIndex));
                    miniBatchOp.setOperationStatus(opIndex, NOWRITE);
                    pendingPuts.remove(opIndex);
                }
                pendingPuts.put(rowKeyPtr, i);
            }
        }
    }

    /**
   * * Merges the mutations on the same row
   */
  private Collection<? extends Mutation> mergeMutations(ObserverContext<RegionCoprocessorEnvironment> c,
                                                        MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                                        BatchMutateContext context,
                                                        long now) throws IOException {
      if (context.rowsToLock.size() == 0) {
          return null;
      }
      Map<ImmutableBytesPtr, Integer> pendingPuts = new HashMap<>(miniBatchOp.size());
      mergePendingPutMutations(miniBatchOp, pendingPuts, now);
      getCurrentRowStates(c, context);
      context.nextDataRowStates = new HashMap<ImmutableBytesPtr, Put>(miniBatchOp.size());
      // Merge pending put mutations with current row states into new merged states
      for (Integer i = 0; i < miniBatchOp.size(); i++) {
          if (miniBatchOp.getOperationStatus(i) == IGNORE || miniBatchOp.getOperationStatus(i) == NOWRITE) {
              continue;
          }
          Mutation m = miniBatchOp.getOperation(i);
          if (!this.builder.isEnabled(m)) {
              continue;
          }
          if (m instanceof Put) {
              ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(m.getRow());
              Put currentRow = context.currentDataRowStates.get(rowKeyPtr);
              Put mergedRow = (currentRow != null) ? mergeNew((Put) m, currentRow) : (Put)m;
              context.nextDataRowStates.put(rowKeyPtr, mergedRow);
          }
      }
      // Apply delete mutations on the pending put mutations and merged row states
      applyPendingDeleteMutations(miniBatchOp, context, pendingPuts);
      Collection<Mutation> pendingMutations = Lists.newArrayListWithExpectedSize(miniBatchOp.size());
      // Add put mutations into the collection first. This ordering of puts first and then deletes is important for
      // obtaining correct index updates
      populateMutationList(miniBatchOp, pendingMutations, true);
      // Add delete mutations into the collection (after the put mutations if any)
      populateMutationList(miniBatchOp, pendingMutations, false);
      return pendingMutations;
  }

  public static void removeEmptyColumn(Mutation m, byte[] emptyCF, byte[] emptyCQ) {
      List<Cell> cellList = m.getFamilyCellMap().get(emptyCF);
      if (cellList == null) {
          return;
      }
      Iterator<Cell> cellIterator = cellList.iterator();
      while (cellIterator.hasNext()) {
          Cell cell = cellIterator.next();
          if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                  emptyCQ, 0, emptyCQ.length) == 0) {
              cellIterator.remove();
              return;
          }
      }
  }

    private void handleLocalIndexUpdates(ObserverContext<RegionCoprocessorEnvironment> c,
                                         MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                         ListMultimap<HTableInterfaceReference, Pair<Mutation, byte[]>> indexUpdates) {
        byte[] tableName = c.getEnvironment().getRegion().getTableDescriptor().getTableName().getName();
        HTableInterfaceReference hTableInterfaceReference =
                new HTableInterfaceReference(new ImmutableBytesPtr(tableName));
        List<Pair<Mutation, byte[]>> localIndexUpdates = indexUpdates.removeAll(hTableInterfaceReference);
        if (localIndexUpdates == null || localIndexUpdates.isEmpty()) {
            return;
        }
        List<Mutation> localUpdates = new ArrayList<Mutation>();
        Iterator<Pair<Mutation, byte[]>> indexUpdatesItr = localIndexUpdates.iterator();
        while (indexUpdatesItr.hasNext()) {
            Pair<Mutation, byte[]> next = indexUpdatesItr.next();
            localUpdates.add(next.getFirst());
        }
        if (!localUpdates.isEmpty()) {
            miniBatchOp.addOperationsFromCP(0, localUpdates.toArray(new Mutation[localUpdates.size()]));
        }
    }
    /**
     * Retrieve the the last committed data row state. This method is called only for regular data mutations since for
     * rebuild (i.e., index replay) mutations include all row versions.
     */
    private void getCurrentRowStates(ObserverContext<RegionCoprocessorEnvironment> c,
                                     BatchMutateContext context) throws IOException {
        Set<KeyRange> keys = new HashSet<KeyRange>(context.rowsToLock.size());
        for (ImmutableBytesPtr rowKeyPtr : context.rowsToLock) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(rowKeyPtr.get()));
        }
        Scan scan = new Scan();
        ScanRanges scanRanges = ScanRanges.createPointLookup(new ArrayList<KeyRange>(keys));
        scanRanges.initializeScan(scan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        scan.setFilter(skipScanFilter);
        context.currentDataRowStates = new HashMap<ImmutableBytesPtr, Put>(context.rowsToLock.size());
        try (RegionScanner scanner = c.getEnvironment().getRegion().getScanner(scan)) {
            boolean more = true;
            while(more) {
                List<Cell> cells = new ArrayList<Cell>();
                more = scanner.next(cells);
                if (cells.isEmpty()) {
                    continue;
                }
                byte[] rowKey = CellUtil.cloneRow(cells.get(0));
                Put put = new Put(rowKey);
                for (Cell cell : cells) {
                    put.add(cell);
                }
                context.currentDataRowStates.put(new ImmutableBytesPtr(rowKey), put);
            }
        }
    }

    /**
     * Generate the index update for a data row from the mutation that are obtained by merging the previous data row
     * state with the pending row mutation.
     */
    private void prepareIndexMutations(ObserverContext<RegionCoprocessorEnvironment> c,
                                       BatchMutateContext context,
                                       List<IndexMaintainer> maintainers,
                                       Collection<? extends Mutation> pendingMutations) throws IOException {
        byte[] regionStartKey = c.getEnvironment().getRegion().getRegionInfo().getStartKey();
        byte[] regionEndKey = c.getEnvironment().getRegion().getRegionInfo().getEndKey();
        List<Pair<IndexMaintainer, HTableInterfaceReference>> indexTables = new ArrayList<>(maintainers.size());
        for (IndexMaintainer indexMaintainer : maintainers) {
            HTableInterfaceReference hTableInterfaceReference =
                    new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
            indexTables.add(new Pair<>(indexMaintainer, hTableInterfaceReference));
        }
        for (Mutation mutation : pendingMutations) {
            ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(mutation.getRow());
            Put currentDataRow = context.currentDataRowStates.get(rowKeyPtr);
            ValueGetter currentDataRowVG = null;
            if (currentDataRow != null) {
                currentDataRowVG = (currentDataRow == null) ? null :
                        new IndexRebuildRegionScanner.SimpleValueGetter(currentDataRow);
            }
            long ts = getTimestamp(mutation);
            if (mutation instanceof Put) {
                Put mergedRow = context.nextDataRowStates.get(rowKeyPtr);
                ValueGetter mergedRowVG = new IndexRebuildRegionScanner.SimpleValueGetter(mergedRow);
                for (Pair<IndexMaintainer, HTableInterfaceReference> pair : indexTables) {
                    IndexMaintainer indexMaintainer = pair.getFirst();
                    HTableInterfaceReference hTableInterfaceReference = pair.getSecond();
                    Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                            mergedRowVG, rowKeyPtr, ts, regionStartKey, regionEndKey);
                    if (indexPut == null) {
                        // No covered column. Just prepare an index row with the empty column
                        byte[] indexRowKey = indexMaintainer.buildRowKey(mergedRowVG, rowKeyPtr,
                                regionStartKey, regionEndKey, HConstants.LATEST_TIMESTAMP);
                        indexPut = new Put(indexRowKey);
                    }
                    if (indexMaintainer.isLocalIndex()) {
                        indexPut.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                                indexMaintainer.getEmptyKeyValueQualifier(), ts, EMPTY_COLUMN_VALUE_BYTES);
                    } else {
                        indexPut.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                                indexMaintainer.getEmptyKeyValueQualifier(), ts, UNVERIFIED_BYTES);
                    }
                    context.indexUpdates.put(hTableInterfaceReference, new Pair<>(indexPut, mutation.getRow()));
                    // Delete the current index row if the new index key is different than the current one
                    if (currentDataRow != null) {
                        byte[] indexRowKeyForCurrentDataRow = indexMaintainer.buildRowKey(currentDataRowVG, rowKeyPtr,
                                regionStartKey, regionEndKey, HConstants.LATEST_TIMESTAMP);
                        if (Bytes.compareTo(indexPut.getRow(), indexRowKeyForCurrentDataRow) != 0) {
                            Mutation del = indexMaintainer.buildRowDeleteMutation(indexRowKeyForCurrentDataRow,
                                    IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                            context.indexUpdates.put(hTableInterfaceReference, new Pair<>(del, mutation.getRow()));
                        }
                    }
                }
            } else {
                if (currentDataRow != null) {
                    for (Pair<IndexMaintainer, HTableInterfaceReference> pair : indexTables) {
                        IndexMaintainer indexMaintainer = pair.getFirst();
                        HTableInterfaceReference hTableInterfaceReference = pair.getSecond();
                        Mutation del = indexMaintainer.buildDeleteMutation(GenericKeyValueBuilder.INSTANCE, currentDataRowVG,
                                rowKeyPtr, getCollection(mutation), ts, regionStartKey, regionEndKey);
                        if (del == null) {
                            continue;
                        }
                        if (!indexMaintainer.isLocalIndex()) {
                            del = adjustGlobalIndexDeleteMutation((Delete) del);
                        }
                        if (del == null) {
                            continue;
                        }
                        context.indexUpdates.put(hTableInterfaceReference, new Pair<>(del, mutation.getRow()));
                    }
                }
            }
        }
    }
    /**
     * This method prepares unverified index mutations which are applied to index tables before the data table is
     * updated. In the three phase update approach, in phase 1, the status of existing index rows is set to "unverified"
     * (these rows will be deleted from the index table in phase 3), and/or new put mutations are added with the
     * unverified status. In phase 2, data table mutations are applied. In phase 3, the status for an index table row is
     * either set to "verified" or the row is deleted.
     */
    private void preparePreIndexMutations(ObserverContext<RegionCoprocessorEnvironment> c,
                                          MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                          BatchMutateContext context,
                                          Collection<? extends Mutation> pendingMutations,
                                          long now,
                                          PhoenixIndexMetaData indexMetaData) throws Throwable {
        List<IndexMaintainer> maintainers = indexMetaData.getIndexMaintainers();
        // get the current span, or just use a null-span to avoid a bunch of if statements
        try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
            Span current = scope.getSpan();
            if (current == null) {
                current = NullSpan.INSTANCE;
            }
            // get the index updates for all elements in this batch
            context.indexUpdates = ArrayListMultimap.<HTableInterfaceReference, Pair<Mutation, byte[]>>create();
            prepareIndexMutations(c, context, maintainers, pendingMutations);
            current.addTimelineAnnotation("Built index updates, doing preStep");
            handleLocalIndexUpdates(c, miniBatchOp, context.indexUpdates);
            context.preIndexUpdates = ArrayListMultimap.<HTableInterfaceReference, Mutation>create();
            int updateCount = 0;
            for (IndexMaintainer indexMaintainer : maintainers) {
                updateCount++;
                byte[] emptyCF = indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary();
                byte[] emptyCQ = indexMaintainer.getEmptyKeyValueQualifier();
                HTableInterfaceReference hTableInterfaceReference =
                        new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
                List <Pair<Mutation, byte[]>> updates = context.indexUpdates.get(hTableInterfaceReference);
                for (Pair<Mutation, byte[]> update : updates) {
                    Mutation m = update.getFirst();
                    if (m instanceof Put) {
                        // This will be done before the data table row is updated (i.e., in the first write phase)
                        context.preIndexUpdates.put(hTableInterfaceReference, m);
                    } else {
                        // Set the status of the index row to "unverified"
                        Put unverifiedPut = new Put(m.getRow());
                        unverifiedPut.addColumn(emptyCF, emptyCQ, now, UNVERIFIED_BYTES);
                        // This will be done before the data table row is updated (i.e., in the first write phase)
                        context.preIndexUpdates.put(hTableInterfaceReference, unverifiedPut);
                    }
                }
            }
            TracingUtils.addAnnotation(current, "index update count", updateCount);
        }
    }

  protected PhoenixIndexMetaData getPhoenixIndexMetaData(ObserverContext<RegionCoprocessorEnvironment> observerContext,
                                                         MiniBatchOperationInProgress<Mutation> miniBatchOp)
          throws IOException {
      IndexMetaData indexMetaData = this.builder.getIndexMetaData(miniBatchOp);
      if (!(indexMetaData instanceof PhoenixIndexMetaData)) {
          throw new DoNotRetryIOException(
                  "preBatchMutateWithExceptions: indexMetaData is not an instance of "+PhoenixIndexMetaData.class.getName() +
                          ", current table is:" +
                          observerContext.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString());
      }
      return (PhoenixIndexMetaData)indexMetaData;
  }

  private void preparePostIndexMutations(BatchMutateContext context, long now, PhoenixIndexMetaData indexMetaData)
          throws Throwable {
      context.postIndexUpdates = ArrayListMultimap.<HTableInterfaceReference, Mutation>create();
      List<IndexMaintainer> maintainers = indexMetaData.getIndexMaintainers();
      // Check if we need to skip post index update for any of the rows
      for (IndexMaintainer indexMaintainer : maintainers) {
          byte[] emptyCF = indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary();
          byte[] emptyCQ = indexMaintainer.getEmptyKeyValueQualifier();
          HTableInterfaceReference hTableInterfaceReference =
                  new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
          List <Pair<Mutation, byte[]>> updates = context.indexUpdates.get(hTableInterfaceReference);
          for (Pair<Mutation, byte[]> update : updates) {
              // Are there concurrent updates on the data table row? if so, skip post index updates
              // and let read repair resolve conflicts
              ImmutableBytesPtr rowKey = new ImmutableBytesPtr(update.getSecond());
              PendingRow pendingRow = pendingRows.get(rowKey);
              if (!pendingRow.isConcurrent()) {
                  Mutation m = update.getFirst();
                  if (m instanceof Put) {
                      Put verifiedPut = new Put(m.getRow());
                      // Set the status of the index row to "verified"
                      verifiedPut.addColumn(emptyCF, emptyCQ, now, VERIFIED_BYTES);
                      context.postIndexUpdates.put(hTableInterfaceReference, verifiedPut);
                  }
                  else {
                      context.postIndexUpdates.put(hTableInterfaceReference, m);
                  }
              }
          }
      }
      // We are done with handling concurrent mutations. So we can remove the rows of this batch from
      // the collection of pending rows
      removePendingRows(context);
      context.indexUpdates.clear();
  }

  /**
   * There are at most two rebuild mutation for every row, one put and one delete. They are listed in indexMutations
   * next to each other such that put comes before delete by {@link IndexRebuildRegionScanner}. This method is called
   * only for global indexes.
   */
  private void preBatchMutateWithExceptionsForRebuild(ObserverContext<RegionCoprocessorEnvironment> c,
                                                      MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                                      BatchMutateContext context,
                                                      IndexMaintainer indexMaintainer) throws Throwable {
        Put put = null;
        List <Mutation> indexMutations = new ArrayList<>();
        for (int i = 0; i < miniBatchOp.size(); i++) {
            if (miniBatchOp.getOperationStatus(i) == IGNORE) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            if (!this.builder.isEnabled(m)) {
                continue;
            }
            if (m instanceof Put) {
                if (put != null) {
                    indexMutations.addAll(prepareIndexMutationsForRebuild(indexMaintainer, put, null));
                }
                put = (Put)m;
            } else {
                indexMutations.addAll(prepareIndexMutationsForRebuild(indexMaintainer, put, (Delete)m));
                put = null;
            }
            miniBatchOp.setOperationStatus(i, NOWRITE);
        }
        if (put != null) {
            indexMutations.addAll(prepareIndexMutationsForRebuild(indexMaintainer, put, null));
        }
        HTableInterfaceReference hTableInterfaceReference =
              new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
        context.preIndexUpdates = ArrayListMultimap.<HTableInterfaceReference, Mutation>create();
        for (Mutation m : indexMutations) {
            context.preIndexUpdates.put(hTableInterfaceReference, m);
        }
        doPre(c, context, miniBatchOp);
        // For rebuild updates, no post index update is prepared. Just create an empty list.
        context.postIndexUpdates = ArrayListMultimap.<HTableInterfaceReference, Mutation>create();
  }

  public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
                                           MiniBatchOperationInProgress<Mutation> miniBatchOp) throws Throwable {
      ignoreAtomicOperations(miniBatchOp);
      PhoenixIndexMetaData indexMetaData = getPhoenixIndexMetaData(c, miniBatchOp);
      BatchMutateContext context = new BatchMutateContext(indexMetaData.getClientVersion());
      setBatchMutateContext(c, context);
      Mutation firstMutation = miniBatchOp.getOperation(0);
      ReplayWrite replayWrite = this.builder.getReplayWrite(firstMutation);
      context.rebuild = replayWrite != null;
      if (context.rebuild) {
          preBatchMutateWithExceptionsForRebuild(c, miniBatchOp, context, indexMetaData.getIndexMaintainers().get(0));
          return;
      }
      /*
       * Exclusively lock all rows so we get a consistent read
       * while determining the index updates
       */
      long now;
      Collection<? extends Mutation> pendingMutations;
      populateRowsToLock(miniBatchOp, context);
      lockRows(context);
      now = EnvironmentEdgeManager.currentTimeMillis();
      // Add the table rows in the mini batch to the collection of pending rows. This will be used to detect
      // concurrent updates
      populatePendingRows(context);
      // Merge all the updates for a single row into a single update to be processed
      pendingMutations = mergeMutations(c, miniBatchOp, context, now);
      // early exit if it turns out we don't have any edits
      if (pendingMutations == null || pendingMutations.isEmpty()) {
          return;
      }
      long start = EnvironmentEdgeManager.currentTimeMillis();
      preparePreIndexMutations(c, miniBatchOp, context, pendingMutations, now, indexMetaData);
      metricSource.updateIndexPrepareTime(EnvironmentEdgeManager.currentTimeMillis() - start);
      // Sleep for one millisecond if we have prepared the index updates in less than 1 ms. The sleep is necessary to
      // get different timestamps for concurrent batches that share common rows. It is very rare that the index updates
      // can be prepared in less than one millisecond
      if (!context.rowLocks.isEmpty() && now == EnvironmentEdgeManager.currentTimeMillis()) {
          Thread.sleep(1);
          LOG.debug("slept 1ms for " + c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString());
      }
      // Release the locks before making RPC calls for index updates
      for (RowLock rowLock : context.rowLocks) {
          rowLock.release();
      }
      // Do the first phase index updates
      doPre(c, context, miniBatchOp);
      // Acquire the locks again before letting the region proceed with data table updates
      List<RowLock> rowLocks = Lists.newArrayListWithExpectedSize(context.rowLocks.size());
      for (RowLock rowLock : context.rowLocks) {
          rowLocks.add(lockManager.lockRow(rowLock.getRowKey(), rowLockWaitDuration));
      }
      context.rowLocks.clear();
      context.rowLocks = rowLocks;
      preparePostIndexMutations(context, now, indexMetaData);
      if (failDataTableUpdatesForTesting) {
          throw new DoNotRetryIOException("Simulating the data table write failure");
      }
  }

  private void setBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c, BatchMutateContext context) {
      this.batchMutateContext.set(context);
  }

  private BatchMutateContext getBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c) {
      return this.batchMutateContext.get();
  }

  private void removeBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c) {
      this.batchMutateContext.remove();
  }

  @Override
  public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp, final boolean success) throws IOException {
      if (this.disabled) {
          return;
      }
      BatchMutateContext context = getBatchMutateContext(c);
      if (context == null) {
          return;
      }
      try {
          for (RowLock rowLock : context.rowLocks) {
              rowLock.release();
          }
          this.builder.batchCompleted(miniBatchOp);

          if (success) { // The pre-index and data table updates are successful, and now, do post index updates
              doPost(c, context);
          }
       } finally {
           removeBatchMutateContext(c);
       }
  }

  private void doPost(ObserverContext<RegionCoprocessorEnvironment> c, BatchMutateContext context) throws IOException {
      long start = EnvironmentEdgeManager.currentTimeMillis();

      try {
          if (failPostIndexUpdatesForTesting) {
              throw new DoNotRetryIOException("Simulating the last (i.e., post) index table write failure");
          }
          doIndexWritesWithExceptions(context, true);
          metricSource.updatePostIndexUpdateTime(EnvironmentEdgeManager.currentTimeMillis() - start);
          return;
      } catch (Throwable e) {
          metricSource.updatePostIndexUpdateFailureTime(EnvironmentEdgeManager.currentTimeMillis() - start);
          metricSource.incrementPostIndexUpdateFailures();
          // Ignore the failures in the third write phase
      }
  }

  private void doIndexWritesWithExceptions(BatchMutateContext context, boolean post)
            throws IOException {
      ListMultimap<HTableInterfaceReference, Mutation> indexUpdates = post ? context.postIndexUpdates : context.preIndexUpdates;
      //short circuit, if we don't need to do any work

      if (context == null || indexUpdates == null || indexUpdates.isEmpty()) {
          return;
      }

      // get the current span, or just use a null-span to avoid a bunch of if statements
      try (TraceScope scope = Trace.startSpan("Completing " + (post ? "post" : "pre") + " index writes")) {
          Span current = scope.getSpan();
          if (current == null) {
              current = NullSpan.INSTANCE;
          }
          current.addTimelineAnnotation("Actually doing " + (post ? "post" : "pre") + " index update for first time");
          if (post) {
              postWriter.write(indexUpdates, false, context.clientVersion);
          } else {
              preWriter.write(indexUpdates, false, context.clientVersion);
          }
      }
  }

  private void removePendingRows(BatchMutateContext context) {
      for (RowLock rowLock : context.rowLocks) {
          ImmutableBytesPtr rowKey = rowLock.getRowKey();
          PendingRow pendingRow = pendingRows.get(rowKey);
          if (pendingRow != null) {
              pendingRow.remove();
              if (pendingRow.getCount() == 0) {
                  pendingRows.remove(rowKey);
              }
          }
      }
  }

  private void doPre(ObserverContext<RegionCoprocessorEnvironment> c, BatchMutateContext context,
                     MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      if (ignoreIndexRebuildForTesting && context.rebuild) {
          return;
      }
      long start = EnvironmentEdgeManager.currentTimeMillis();
      try {
          if (failPreIndexUpdatesForTesting) {
              throw new DoNotRetryIOException("Simulating the first (i.e., pre) index table write failure");
          }
          doIndexWritesWithExceptions(context, false);
          metricSource.updatePreIndexUpdateTime(EnvironmentEdgeManager.currentTimeMillis() - start);
          return;
      } catch (Throwable e) {
          metricSource.updatePreIndexUpdateFailureTime(EnvironmentEdgeManager.currentTimeMillis() - start);
          metricSource.incrementPreIndexUpdateFailures();
          // Remove all locks as they are already unlocked. There is no need to unlock them again later when
          // postBatchMutateIndispensably() is called
          removePendingRows(context);
          context.rowLocks.clear();
          rethrowIndexingException(e);
      }
      throw new RuntimeException(
              "Somehow didn't complete the index update, but didn't return succesfully either!");
  }

  /**
   * Enable indexing on the given table
   * @param descBuilder {@link TableDescriptor} for the table on which indexing should be enabled
 * @param builder class to use when building the index for this table
 * @param properties map of custom configuration options to make available to your
   *          {@link IndexBuilder} on the server-side
 * @param priority TODO
   * @throws IOException the Indexer coprocessor cannot be added
   */
  public static void enableIndexing(TableDescriptorBuilder descBuilder, Class<? extends IndexBuilder> builder,
                                    Map<String, String> properties, int priority) throws IOException {
    if (properties == null) {
      properties = new HashMap<String, String>();
    }
    properties.put(Indexer.INDEX_BUILDER_CONF_KEY, builder.getName());
     descBuilder.addCoprocessor(IndexRegionObserver.class.getName(), null, priority, properties);
  }
}

