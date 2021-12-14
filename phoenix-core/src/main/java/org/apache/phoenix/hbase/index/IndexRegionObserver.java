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

import static org.apache.phoenix.hbase.index.util.IndexManagementUtil.rethrowIndexingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compat.hbase.HbaseCompatCapabilities;
import org.apache.phoenix.compat.hbase.coprocessor.CompatIndexRegionObserver;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.GlobalIndexRegionScanner;
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
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.apache.phoenix.util.WALAnnotationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.applyNew;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.removeColumn;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make.
 * We don't need to implement {@link #postPut(ObserverContext, Put, WALEdit, Durability)} and
 * {@link #postDelete(ObserverContext, Delete, WALEdit, Durability)} hooks because
 * Phoenix always does batch mutations.
 * <p>
 */
public class IndexRegionObserver extends CompatIndexRegionObserver implements RegionCoprocessor,
    RegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRegionObserver.class);
    private static final OperationStatus IGNORE = new OperationStatus(OperationStatusCode.SUCCESS);
    private static final OperationStatus NOWRITE = new OperationStatus(OperationStatusCode.SUCCESS);
    public static final String PHOENIX_APPEND_METADATA_TO_WAL = "phoenix.append.metadata.to.wal";
    public static final boolean DEFAULT_PHOENIX_APPEND_METADATA_TO_WAL = false;

  /**
   * Class to represent pending data table rows
   */
  private static class PendingRow {
      private int count;
      private BatchMutateContext lastContext;

      PendingRow(BatchMutateContext context) {
          count = 1;
          lastContext = context;
      }

      public void add(BatchMutateContext context) {
          count++;
          lastContext = context;
      }

      public void remove() {
          count--;
      }

      public int getCount() {
          return count;
      }

      public BatchMutateContext getLastContext() {
          return lastContext;
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

  public enum BatchMutatePhase {
      PRE, POST, FAILED
  }

  // Hack to get around not being able to save any state between
  // coprocessor calls. TODO: remove after HBASE-18127 when available

  /**
   * The concurrent batch of mutations is a set such that every pair of batches in this set has at least one common row.
   * Since a BatchMutateContext object of a batch is modified only after the row locks for all the rows that are mutated
   * by this batch are acquired, there can be only one thread can acquire the locks for its batch and safely access
   * all the batch contexts in the set of concurrent batches. Because of this, we do not read atomic variables or
   * additional locks to serialize the access to the BatchMutateContext objects.
   */

  public static class BatchMutateContext {
      private BatchMutatePhase currentPhase = BatchMutatePhase.PRE;
      // The max of reference counts on the pending rows of this batch at the time this batch arrives
      private int maxPendingRowCount = 0;
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
      // The current and next states of the data rows corresponding to the pending mutations
      private HashMap<ImmutableBytesPtr, Pair<Put, Put>> dataRowStates;
      // The previous concurrent batch contexts
      private HashMap<ImmutableBytesPtr, BatchMutateContext> lastConcurrentBatchContext = null;
      // The latches of the threads waiting for this batch to complete
      private List<CountDownLatch> waitList = null;
      private Map<ImmutableBytesPtr, MultiMutation> multiMutationMap;

      //list containing the original mutations from the MiniBatchOperationInProgress. Contains
      // any annotations we were sent by the client, and can be used in hooks that don't get
      // passed MiniBatchOperationInProgress, like preWALAppend()
      private List<Mutation> originalMutations;
      public BatchMutateContext() {
          this.clientVersion = 0;
      }
      public BatchMutateContext(int clientVersion) {
          this.clientVersion = clientVersion;
      }

      public void populateOriginalMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp) {
          originalMutations = new ArrayList<Mutation>(miniBatchOp.size());
          for (int k = 0; k < miniBatchOp.size(); k++) {
              originalMutations.add(miniBatchOp.getOperation(k));
          }
      }
      public List<Mutation> getOriginalMutations() {
          return originalMutations;
      }

      public BatchMutatePhase getCurrentPhase() {
          return currentPhase;
      }

      public Put getNextDataRowState(ImmutableBytesPtr rowKeyPtr) {
          Pair<Put, Put> rowState = dataRowStates.get(rowKeyPtr);
          if (rowState != null) {
              return rowState.getSecond();
          }
          return null;
      }

      public CountDownLatch getCountDownLatch() {
          if (waitList == null) {
              waitList = new ArrayList<>();
          }
          CountDownLatch countDownLatch = new CountDownLatch(1);
          waitList.add(countDownLatch);
          return countDownLatch;
      }

      public int getMaxPendingRowCount() {
          return maxPendingRowCount;
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
  private int concurrentMutationWaitDuration;
  private String dataTableName;
  private boolean shouldWALAppend = DEFAULT_PHOENIX_APPEND_METADATA_TO_WAL;
  private boolean isNamespaceEnabled = false;

  private static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;
  private static final int DEFAULT_CONCURRENT_MUTATION_WAIT_DURATION_IN_MS = 100;

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
          this.concurrentMutationWaitDuration = env.getConfiguration().getInt("phoenix.index.concurrent.wait.duration.ms",
                  DEFAULT_CONCURRENT_MUTATION_WAIT_DURATION_IN_MS);
          // Metrics impl for the Indexer -- avoiding unnecessary indirection for hadoop-1/2 compat
          this.metricSource = MetricsIndexerSourceFactory.getInstance().getIndexerSource();
          setSlowThresholds(e.getConfiguration());
          this.dataTableName = env.getRegionInfo().getTable().getNameAsString();
          this.shouldWALAppend = env.getConfiguration().getBoolean(PHOENIX_APPEND_METADATA_TO_WAL,
              DEFAULT_PHOENIX_APPEND_METADATA_TO_WAL);
          this.isNamespaceEnabled = SchemaUtil.isNamespaceMappingEnabled(PTableType.INDEX,
              env.getConfiguration());
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
              metricSource.incrementSlowDuplicateKeyCheckCalls(dataTableName);
          }
          metricSource.updateDuplicateKeyCheckTime(dataTableName, duration);
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

    private void unlockRows(BatchMutateContext context) throws IOException {
        for (RowLock rowLock : context.rowLocks) {
            rowLock.release();
        }
        context.rowLocks.clear();
    }

  private void populatePendingRows(BatchMutateContext context) {
      for (RowLock rowLock : context.rowLocks) {
          ImmutableBytesPtr rowKey = rowLock.getRowKey();
          PendingRow pendingRow = pendingRows.get(rowKey);
          if (pendingRow == null) {
              pendingRows.put(rowKey, new PendingRow(context));
          } else {
              // m is a mutation on a row that has already a pending mutation in progress from another batch
              pendingRow.add(context);
          }
      }
  }

    private Collection<? extends Mutation> groupMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                                          BatchMutateContext context) throws IOException {
        context.multiMutationMap = new HashMap<>();
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            // skip this mutation if we aren't enabling indexing
            // unfortunately, we really should ask if the raw mutation (rather than the combined mutation)
            // should be indexed, which means we need to expose another method on the builder. Such is the
            // way optimization go though.
            if (miniBatchOp.getOperationStatus(i) != IGNORE && this.builder.isEnabled(m)) {
                ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
                MultiMutation stored = context.multiMutationMap.get(row);
                if (stored == null) {
                    // we haven't seen this row before, so add it
                    stored = new MultiMutation(row);
                    context.multiMutationMap.put(row, stored);
                }
                stored.addAll(m);
            }
        }
        return context.multiMutationMap.values();
    }

    public static void setTimestamps(MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexBuildManager builder, long ts) throws IOException {
        for (Integer i = 0; i < miniBatchOp.size(); i++) {
            if (miniBatchOp.getOperationStatus(i) == IGNORE) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            // skip this mutation if we aren't enabling indexing
            if (!builder.isEnabled(m)) {
                continue;
            }
            for (List<Cell> cells : m.getFamilyCellMap().values()) {
                for (Cell cell : cells) {
                    CellUtil.setTimestamp(cell, ts);
                }
            }
        }
    }

    /**
     * This method applies pending delete mutations on the next row states
     */
    private void applyPendingDeleteMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                             BatchMutateContext context) throws IOException {
        for (int i = 0; i < miniBatchOp.size(); i++) {
            if (miniBatchOp.getOperationStatus(i) == IGNORE) {
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
            Pair<Put, Put> dataRowState = context.dataRowStates.get(rowKeyPtr);
            if (dataRowState == null) {
                dataRowState = new Pair<Put, Put>(null, null);
                context.dataRowStates.put(rowKeyPtr, dataRowState);
            }
            Put nextDataRowState = dataRowState.getSecond();
            if (nextDataRowState == null) {
                if (dataRowState.getFirst() == null) {
                    // This is a delete row mutation on a non-existing row. There is no need to apply this mutation
                    // on the data table
                    miniBatchOp.setOperationStatus(i, NOWRITE);
                }
                continue;
            }
            for (List<Cell> cells : m.getFamilyCellMap().values()) {
                for (Cell cell : cells) {
                    switch (KeyValue.Type.codeToType(cell.getTypeByte())) {
                        case DeleteFamily:
                        case DeleteFamilyVersion:
                            nextDataRowState.getFamilyCellMap().remove(CellUtil.cloneFamily(cell));
                            break;
                        case DeleteColumn:
                        case Delete:
                            removeColumn(nextDataRowState, cell);
                    }
                }
            }
            if (nextDataRowState != null && nextDataRowState.getFamilyCellMap().size() == 0) {
                dataRowState.setSecond(null);
            }
        }
    }

    /**
     * This method applies the pending put mutations on the the next row states.
     * Before this method is called, the next row states is set to current row states.
     */
    private void applyPendingPutMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                          BatchMutateContext context, long now) throws IOException {
        for (Integer i = 0; i < miniBatchOp.size(); i++) {
            if (miniBatchOp.getOperationStatus(i) == IGNORE) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            // skip this mutation if we aren't enabling indexing
            if (!this.builder.isEnabled(m)) {
                continue;
            }
            if (m instanceof Put) {
                ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(m.getRow());
                Pair<Put, Put> dataRowState = context.dataRowStates.get(rowKeyPtr);
                if (dataRowState == null) {
                    dataRowState = new Pair<Put, Put>(null, null);
                    context.dataRowStates.put(rowKeyPtr, dataRowState);
                }
                Put nextDataRowState = dataRowState.getSecond();
                dataRowState.setSecond((nextDataRowState != null) ? applyNew((Put) m, nextDataRowState) : new Put((Put) m));
            }
        }
    }

    /**
     * * Prepares data row current and next row states
     */
    private void prepareDataRowStates(ObserverContext<RegionCoprocessorEnvironment> c,
                                      MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                      BatchMutateContext context,
                                      long now) throws IOException {
        if (context.rowsToLock.size() == 0) {
            return;
        }
        // Retrieve the current row states from the data table
        getCurrentRowStates(c, context);
        applyPendingPutMutations(miniBatchOp, context, now);
        applyPendingDeleteMutations(miniBatchOp, context);
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

    /**
     * The index update generation for local indexes uses the existing index update generation code (i.e.,
     * the {@link IndexBuilder} implementation).
     */
    private void handleLocalIndexUpdates(TableName table,
                                         MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                         Collection<? extends Mutation> pendingMutations,
                                         PhoenixIndexMetaData indexMetaData) throws Throwable {
        ListMultimap<HTableInterfaceReference, Pair<Mutation, byte[]>> indexUpdates = ArrayListMultimap.<HTableInterfaceReference, Pair<Mutation, byte[]>>create();
        this.builder.getIndexUpdates(indexUpdates, miniBatchOp, pendingMutations, indexMetaData);
        byte[] tableName = table.getName();
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
        context.dataRowStates = new HashMap<ImmutableBytesPtr, Pair<Put, Put>>(context.rowsToLock.size());
        for (ImmutableBytesPtr rowKeyPtr : context.rowsToLock) {
            PendingRow pendingRow = pendingRows.get(rowKeyPtr);
            if (pendingRow != null && pendingRow.getLastContext().getCurrentPhase() == BatchMutatePhase.PRE) {
                if (context.lastConcurrentBatchContext == null) {
                    context.lastConcurrentBatchContext = new HashMap<>();
                }
                context.lastConcurrentBatchContext.put(rowKeyPtr, pendingRow.getLastContext());
                if (context.maxPendingRowCount < pendingRow.getCount()) {
                    context.maxPendingRowCount = pendingRow.getCount();
                }
                Put put = pendingRow.getLastContext().getNextDataRowState(rowKeyPtr);
                if (put != null) {
                    context.dataRowStates.put(rowKeyPtr, new Pair<Put, Put>(put, new Put(put)));
                }
            }
            else {
                keys.add(PVarbinary.INSTANCE.getKeyRange(rowKeyPtr.get()));
            }
        }
        if (keys.isEmpty()) {
            return;
        }
        Scan scan = new Scan();
        ScanRanges scanRanges = ScanRanges.createPointLookup(new ArrayList<KeyRange>(keys));
        scanRanges.initializeScan(scan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        scan.setFilter(skipScanFilter);
        try (RegionScanner scanner = c.getEnvironment().getRegion().getScanner(scan)) {
            boolean more = true;
            while (more) {
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
                context.dataRowStates.put(new ImmutableBytesPtr(rowKey), new Pair<Put, Put>(put, new Put(put)));
            }
        }
    }

    /**
     * Generate the index update for a data row from the mutation that are obtained by merging the previous data row
     * state with the pending row mutation.
     */
    private void prepareIndexMutations(BatchMutateContext context, List<IndexMaintainer> maintainers, long ts)
            throws IOException {
        List<Pair<IndexMaintainer, HTableInterfaceReference>> indexTables = new ArrayList<>(maintainers.size());
        for (IndexMaintainer indexMaintainer : maintainers) {
            if (indexMaintainer.isLocalIndex()) {
                continue;
            }
            HTableInterfaceReference hTableInterfaceReference =
                    new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
            indexTables.add(new Pair<>(indexMaintainer, hTableInterfaceReference));
        }
        for (Map.Entry<ImmutableBytesPtr, Pair<Put, Put>> entry : context.dataRowStates.entrySet()) {
            ImmutableBytesPtr rowKeyPtr = entry.getKey();
            Pair<Put, Put> dataRowState =  entry.getValue();
            Put currentDataRowState = dataRowState.getFirst();
            Put nextDataRowState = dataRowState.getSecond();
            if (currentDataRowState == null && nextDataRowState == null) {
                continue;
            }
            for (Pair<IndexMaintainer, HTableInterfaceReference> pair : indexTables) {
                IndexMaintainer indexMaintainer = pair.getFirst();
                HTableInterfaceReference hTableInterfaceReference = pair.getSecond();
                if (nextDataRowState != null) {
                    ValueGetter nextDataRowVG = new GlobalIndexRegionScanner.SimpleValueGetter(nextDataRowState);
                    Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                            nextDataRowVG, rowKeyPtr, ts, null, null, false);
                    if (indexPut == null) {
                        // No covered column. Just prepare an index row with the empty column
                        byte[] indexRowKey = indexMaintainer.buildRowKey(nextDataRowVG, rowKeyPtr,
                                null, null, ts);
                        indexPut = new Put(indexRowKey);
                    } else {
                        removeEmptyColumn(indexPut, indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                                indexMaintainer.getEmptyKeyValueQualifier());
                    }
                    indexPut.addColumn(
                            indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                            indexMaintainer.getEmptyKeyValueQualifier(), ts,
                            QueryConstants.UNVERIFIED_BYTES);
                    context.indexUpdates.put(hTableInterfaceReference,
                            new Pair<Mutation, byte[]>(indexPut, rowKeyPtr.get()));
                    // Delete the current index row if the new index key is different than the current one
                    if (currentDataRowState != null) {
                        ValueGetter currentDataRowVG = new GlobalIndexRegionScanner.SimpleValueGetter(currentDataRowState);
                        byte[] indexRowKeyForCurrentDataRow = indexMaintainer.buildRowKey(currentDataRowVG, rowKeyPtr,
                                null, null, ts);
                        if (Bytes.compareTo(indexPut.getRow(), indexRowKeyForCurrentDataRow) != 0) {
                            Mutation del = indexMaintainer.buildRowDeleteMutation(indexRowKeyForCurrentDataRow,
                                    IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                            context.indexUpdates.put(hTableInterfaceReference,
                                    new Pair<Mutation, byte[]>(del, rowKeyPtr.get()));
                        }
                    }
                } else if (currentDataRowState != null) {
                    ValueGetter currentDataRowVG = new GlobalIndexRegionScanner.SimpleValueGetter(currentDataRowState);
                    byte[] indexRowKeyForCurrentDataRow = indexMaintainer.buildRowKey(currentDataRowVG, rowKeyPtr,
                            null, null, ts);
                    Mutation del = indexMaintainer.buildRowDeleteMutation(indexRowKeyForCurrentDataRow,
                            IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                    context.indexUpdates.put(hTableInterfaceReference,
                            new Pair<Mutation, byte[]>(del, rowKeyPtr.get()));
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
    private void preparePreIndexMutations(BatchMutateContext context,
                                          long now,
                                          PhoenixIndexMetaData indexMetaData) throws Throwable {
        List<IndexMaintainer> maintainers = indexMetaData.getIndexMaintainers();
        // get the current span, or just use a null-span to avoid a bunch of if statements
        try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
            Span current = scope.getSpan();
            if (current == null) {
                current = NullSpan.INSTANCE;
            }
            current.addTimelineAnnotation("Built index updates, doing preStep");
            // The rest of this method is for handling global index updates
            context.indexUpdates = ArrayListMultimap.<HTableInterfaceReference, Pair<Mutation, byte[]>>create();
            prepareIndexMutations(context, maintainers, now);

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
                        unverifiedPut.addColumn(
                            emptyCF, emptyCQ, now, QueryConstants.UNVERIFIED_BYTES);
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

    private void preparePostIndexMutations(BatchMutateContext context,
                                           long now,
                                           PhoenixIndexMetaData indexMetaData) {
        context.postIndexUpdates = ArrayListMultimap.<HTableInterfaceReference, Mutation>create();
        List<IndexMaintainer> maintainers = indexMetaData.getIndexMaintainers();
        for (IndexMaintainer indexMaintainer : maintainers) {
            byte[] emptyCF = indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary();
            byte[] emptyCQ = indexMaintainer.getEmptyKeyValueQualifier();
            HTableInterfaceReference hTableInterfaceReference =
                    new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
            List<Pair<Mutation, byte[]>> updates = context.indexUpdates.get(hTableInterfaceReference);
            for (Pair<Mutation, byte[]> update : updates) {
                Mutation m = update.getFirst();
                if (m instanceof Put) {
                    Put verifiedPut = new Put(m.getRow());
                    // Set the status of the index row to "verified"
                    verifiedPut.addColumn(emptyCF, emptyCQ, now, QueryConstants.VERIFIED_BYTES);
                    context.postIndexUpdates.put(hTableInterfaceReference, verifiedPut);
                } else {
                    context.postIndexUpdates.put(hTableInterfaceReference, m);
                }
            }
        }
        removePendingRows(context);
        context.indexUpdates.clear();
    }

    private static boolean hasGlobalIndex(PhoenixIndexMetaData indexMetaData) {
        for (IndexMaintainer indexMaintainer : indexMetaData.getIndexMaintainers()) {
            if (!indexMaintainer.isLocalIndex()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasLocalIndex(PhoenixIndexMetaData indexMetaData) {
        for (IndexMaintainer indexMaintainer : indexMetaData.getIndexMaintainers()) {
            if (indexMaintainer.isLocalIndex()) {
                return true;
            }
        }
        return false;
    }

    private void waitForPreviousConcurrentBatch(TableName table, BatchMutateContext context)
            throws Throwable {
        boolean done;
        BatchMutatePhase phase;
        done = true;
        for (BatchMutateContext lastContext : context.lastConcurrentBatchContext.values()) {
            phase = lastContext.getCurrentPhase();
            if (phase == BatchMutatePhase.FAILED) {
                done = false;
                break;
            }
            if (phase == BatchMutatePhase.PRE) {
                CountDownLatch countDownLatch = lastContext.getCountDownLatch();
                // Release the locks so that the previous concurrent mutation can go into the post phase
                unlockRows(context);
                // Wait for at most one concurrentMutationWaitDuration for each level in the dependency tree of batches.
                // lastContext.getMaxPendingRowCount() is the depth of the subtree rooted at the batch pointed by lastContext
                if (!countDownLatch.await((lastContext.getMaxPendingRowCount() + 1) * concurrentMutationWaitDuration,
                        TimeUnit.MILLISECONDS)) {
                    done = false;
                    break;
                }
                // Acquire the locks again before letting the region proceed with data table updates
                lockRows(context);
            }
        }
        if (!done) {
            // This batch needs to be retried since one of the previous concurrent batches has not completed yet.
            // Throwing an IOException will result in retries of this batch. Before throwing exception,
            // we need to remove reference counts and locks for the rows of this batch
            removePendingRows(context);
            context.indexUpdates.clear();
            for (RowLock rowLock : context.rowLocks) {
                rowLock.release();
            }
            context.rowLocks.clear();
            throw new IOException("One of the previous concurrent mutations has not completed. " +
                    "The batch needs to be retried " + table.getNameAsString());
        }
    }

    public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
                                             MiniBatchOperationInProgress<Mutation> miniBatchOp) throws Throwable {
        ignoreAtomicOperations(miniBatchOp);
        PhoenixIndexMetaData indexMetaData = getPhoenixIndexMetaData(c, miniBatchOp);
        BatchMutateContext context = new BatchMutateContext(indexMetaData.getClientVersion());
        setBatchMutateContext(c, context);
        context.populateOriginalMutations(miniBatchOp);
        // Need to add cell tags to Delete Marker before we do any index processing
        // since we add tags to tables which doesn't have indexes also.
        IndexUtil.setDeleteAttributes(miniBatchOp);

        /*
         * Exclusively lock all rows so we get a consistent read
         * while determining the index updates
         */
        populateRowsToLock(miniBatchOp, context);
        // early exit if it turns out we don't have any update for indexes
        if (context.rowsToLock.isEmpty()) {
            return;
        }
        lockRows(context);
        long now = EnvironmentEdgeManager.currentTimeMillis();
        // Update the timestamps of the data table mutations to prevent overlapping timestamps (which prevents index
        // inconsistencies as this case isn't handled correctly currently).
        setTimestamps(miniBatchOp, builder, now);

        TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
        if (hasGlobalIndex(indexMetaData)) {
            // Prepare current and next data rows states for pending mutations (for global indexes)
            prepareDataRowStates(c, miniBatchOp, context, now);
            // Add the table rows in the mini batch to the collection of pending rows. This will be used to detect
            // concurrent updates
            populatePendingRows(context);
            // early exit if it turns out we don't have any edits
            long start = EnvironmentEdgeManager.currentTimeMillis();
            preparePreIndexMutations(context, now, indexMetaData);
            metricSource.updateIndexPrepareTime(dataTableName,
                EnvironmentEdgeManager.currentTimeMillis() - start);
            // Sleep for one millisecond if we have prepared the index updates in less than 1 ms. The sleep is necessary to
            // get different timestamps for concurrent batches that share common rows. It is very rare that the index updates
            // can be prepared in less than one millisecond
            if (!context.rowLocks.isEmpty() && now == EnvironmentEdgeManager.currentTimeMillis()) {
                Thread.sleep(1);
                LOG.debug("slept 1ms for " + table.getNameAsString());
            }
            // Release the locks before making RPC calls for index updates
            unlockRows(context);
            // Do the first phase index updates
            doPre(c, context, miniBatchOp);
            // Acquire the locks again before letting the region proceed with data table updates
            lockRows(context);
            if (context.lastConcurrentBatchContext != null) {
                waitForPreviousConcurrentBatch(table, context);
            }
            preparePostIndexMutations(context, now, indexMetaData);
        }
        if (hasLocalIndex(indexMetaData)) {
            // Group all the updates for a single row into a single update to be processed (for local indexes)
            Collection<? extends Mutation> mutations = groupMutations(miniBatchOp, context);
            handleLocalIndexUpdates(table, miniBatchOp, mutations, indexMetaData);
        }
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
    public void preWALAppend(ObserverContext<RegionCoprocessorEnvironment> c, WALKey key,
                             WALEdit edit) {
        if (HbaseCompatCapabilities.hasPreWALAppend() && shouldWALAppend) {
            BatchMutateContext context = getBatchMutateContext(c);
            WALAnnotationUtil.appendMutationAttributesToWALKey(key, context);
        }
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
          if (success) {
              context.currentPhase = BatchMutatePhase.POST;
          } else {
              context.currentPhase = BatchMutatePhase.FAILED;
          }
          if (context.waitList != null) {
              for (CountDownLatch countDownLatch : context.waitList) {
                  countDownLatch.countDown();
              }
          }
          unlockRows(context);
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
          metricSource.updatePostIndexUpdateTime(dataTableName,
              EnvironmentEdgeManager.currentTimeMillis() - start);
          return;
      } catch (Throwable e) {
          metricSource.updatePostIndexUpdateFailureTime(dataTableName,
              EnvironmentEdgeManager.currentTimeMillis() - start);
          metricSource.incrementPostIndexUpdateFailures(dataTableName);
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
      long start = EnvironmentEdgeManager.currentTimeMillis();
      try {
          if (failPreIndexUpdatesForTesting) {
              throw new DoNotRetryIOException("Simulating the first (i.e., pre) index table write failure");
          }
          doIndexWritesWithExceptions(context, false);
          metricSource.updatePreIndexUpdateTime(dataTableName,
              EnvironmentEdgeManager.currentTimeMillis() - start);
          return;
      } catch (Throwable e) {
          metricSource.updatePreIndexUpdateFailureTime(dataTableName,
              EnvironmentEdgeManager.currentTimeMillis() - start);
          metricSource.incrementPreIndexUpdateFailures(dataTableName);
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

