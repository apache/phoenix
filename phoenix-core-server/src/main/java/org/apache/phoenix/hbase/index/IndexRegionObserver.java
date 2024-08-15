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


import static org.apache.hadoop.hbase.HConstants.OperationStatusCode.SUCCESS;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.UPSERT_CF;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.UPSERT_STATUS_CQ;
import static org.apache.phoenix.hbase.index.util.IndexManagementUtil.rethrowIndexingException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.index.PhoenixIndexBuilderHelper;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.LockManager.RowLock;
import org.apache.phoenix.hbase.index.builder.FatalIndexBuildingFailureException;
import org.apache.phoenix.hbase.index.builder.IndexBuildManager;
import org.apache.phoenix.hbase.index.builder.IndexBuilder;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
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
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerIndexUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.applyNew;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.removeColumn;
import static org.apache.phoenix.index.PhoenixIndexBuilderHelper.ATOMIC_OP_ATTRIB;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make.
 * We don't need to implement {@link #postPut(ObserverContext, Put, WALEdit, Durability)} and
 * {@link #postDelete(ObserverContext, Delete, WALEdit, Durability)} hooks because
 * Phoenix always does batch mutations.
 * <p>
 */
public class IndexRegionObserver implements RegionCoprocessor, RegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRegionObserver.class);
    private static final OperationStatus IGNORE = new OperationStatus(SUCCESS);
    private static final OperationStatus NOWRITE = new OperationStatus(SUCCESS);
    public static final String PHOENIX_APPEND_METADATA_TO_WAL = "phoenix.append.metadata.to.wal";
    public static final boolean DEFAULT_PHOENIX_APPEND_METADATA_TO_WAL = false;
    /**
     * Class to represent pending data table rows
     * */
    private class PendingRow {
        private int count;
        private boolean usable;
        private ImmutableBytesPtr rowKey;
        private BatchMutateContext lastContext;

        PendingRow(ImmutableBytesPtr rowKey, BatchMutateContext context) {
            count = 1;
            usable = true;
            lastContext = context;
            this.rowKey = rowKey;
        }

        public boolean add(BatchMutateContext context) {
            synchronized (this) {
                if (usable) {
                    count++;
                    lastContext = context;
                    return true;
                }
            }
            return false;
        }

        public void remove() {
            synchronized (this) {
                count--;
                if (count == 0) {
                    pendingRows.remove(rowKey);
                    usable = false;
                }
            }
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
      private volatile BatchMutatePhase currentPhase = BatchMutatePhase.PRE;
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
      //  TreeSet to improve locking efficiency and avoid deadlock (PHOENIX-6871 and HBASE-17924) 
      private Set<ImmutableBytesPtr> rowsToLock = new TreeSet<>();
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
      private boolean hasAtomic;
      private boolean hasDelete;
      private boolean hasUncoveredIndex;
      private boolean hasGlobalIndex;
      private boolean hasLocalIndex;
      private boolean hasTransform;
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
          synchronized (this) {
              if (currentPhase != BatchMutatePhase.PRE) {
                  return null;
              }
              if (waitList == null) {
                  waitList = new ArrayList<>();
              }
              CountDownLatch countDownLatch = new CountDownLatch(1);
              waitList.add(countDownLatch);
              return countDownLatch;
          }
      }

      public void countDownAllLatches() {
          synchronized (this) {
              if (waitList != null) {
                  for (CountDownLatch countDownLatch : waitList) {
                      countDownLatch.countDown();
                  }
              }
          }
      }

      public int getMaxPendingRowCount() {
          return maxPendingRowCount;
      }
  }

  private ThreadLocal<BatchMutateContext> batchMutateContext =
          new ThreadLocal<BatchMutateContext>();

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
  private boolean useBloomFilter = false;
  private long lastTimestamp = 0;
  private List<Set<ImmutableBytesPtr>> batchesWithLastTimestamp = new ArrayList<>();
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
          TableDescriptor tableDescriptor = env.getRegion().getTableDescriptor();
          BloomType bloomFilterType = tableDescriptor.getColumnFamilies()[0].getBloomFilterType();
          // when the table descriptor changes, the coproc is reloaded
          this.useBloomFilter = bloomFilterType == BloomType.ROW;
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
          throw ClientUtil.createIOException(
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

  private void ignoreAtomicOperations (MiniBatchOperationInProgress<Mutation> miniBatchOp) {
      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          if (this.builder.isAtomicOp(m)) {
              miniBatchOp.setOperationStatus(i, IGNORE);
          }
      }
  }

  private void populateRowsToLock(MiniBatchOperationInProgress<Mutation> miniBatchOp,
          BatchMutateContext context) {
      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          if (this.builder.isAtomicOp(m) || this.builder.isEnabled(m)) {
              ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
              context.rowsToLock.add(row);

          }
      }
  }

    /**
     * Add the mutations generated by the ON DUPLICATE KEY UPDATE to the current batch.
     * MiniBatchOperationInProgress#addOperationsFromCP() allows coprocessors to attach additional mutations
     * to the incoming mutation. These additional mutations are only executed if the status of the original
     * mutation is set to NOT_RUN. For atomic mutations, we want HBase to ignore the incoming mutation and
     * instead execute the mutations generated by the server for that atomic mutation. But we can’t achieve
     * this behavior just by setting the status of the original mutation to IGNORE because that will also
     * ignore the additional mutations added by the coprocessors. To get around this, we need to do a fixup
     * of the original mutation in the batch. Since we always generate one Put mutation from the incoming atomic
     * Put mutation, we can transfer the cells from the generated Put mutation to the original atomic Put mutation in the batch.
     * The additional mutations (Delete) can then be added to the operationsFromCoprocessors array.
     */
  private void addOnDupMutationsToBatch(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                          int index, List<Mutation> mutations) {
      List<Delete> deleteMutations = Lists.newArrayListWithExpectedSize(mutations.size());
      for (Mutation m : mutations) {
          if (m instanceof Put) {
              // fix the incoming atomic mutation
              Mutation original = miniBatchOp.getOperation(index);
              original.getFamilyCellMap().putAll(m.getFamilyCellMap());
          } else if (m instanceof Delete) {
              deleteMutations.add((Delete)m);
          }
      }

      if (!deleteMutations.isEmpty()) {
          miniBatchOp.addOperationsFromCP(index,
              deleteMutations.toArray(new Mutation[deleteMutations.size()]));
      }
  }

    private void addOnDupMutationsToBatch(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                        BatchMutateContext context) throws IOException {
      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          if (this.builder.isAtomicOp(m) && m instanceof Put) {
              List<Mutation> mutations = generateOnDupMutations(context, (Put)m);
              if (!mutations.isEmpty()) {
                  addOnDupMutationsToBatch(miniBatchOp, i, mutations);
              } else {
                  // empty list of generated mutations implies
                  // 1) ON DUPLICATE KEY IGNORE if row already exists, OR
                  // 2) ON DUPLICATE KEY UPDATE if CASE expression is specified and in each of
                  // them the new value is the same as the old value in the ELSE-clause (empty
                  // cell timestamp will NOT be updated)
                  byte[] retVal = PInteger.INSTANCE.toBytes(0);
                  Cell cell = PhoenixKeyValueUtil.newKeyValue(m.getRow(), Bytes.toBytes(UPSERT_CF),
                          Bytes.toBytes(UPSERT_STATUS_CQ), 0, retVal, 0, retVal.length);
                  // put Result in OperationStatus for returning update status from conditional
                  // upserts, where 0 represents the row is not updated
                  Result result = Result.create(new ArrayList<>(Arrays.asList(cell)));
                  miniBatchOp.setOperationStatus(i,
                          new OperationStatus(SUCCESS, result));
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

    private Collection<? extends Mutation> groupMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                                          BatchMutateContext context) throws IOException {
        context.multiMutationMap = new HashMap<>();
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            // skip this mutation if we aren't enabling indexing
            // unfortunately, we really should ask if the raw mutation (rather than the combined mutation)
            // should be indexed, which means we need to expose another method on the builder. Such is the
            // way optimization go though.
            if (!isAtomicOperationComplete(miniBatchOp.getOperationStatus(i)) && this.builder.isEnabled(m)) {
                ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
                MultiMutation stored = context.multiMutationMap.get(row);
                if (stored == null) {
                    // we haven't seen this row before, so add it
                    stored = new MultiMutation(row);
                    context.multiMutationMap.put(row, stored);
                }
                stored.addAll(m);
                Mutation[] mutationsAddedByCP = miniBatchOp.getOperationsFromCoprocessors(i);
                if (mutationsAddedByCP != null) {
                    for (Mutation addedMutation : mutationsAddedByCP) {
                        stored.addAll(addedMutation);
                    }
                }
            }
        }
        return context.multiMutationMap.values();
    }

    public static void setTimestamps(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                     IndexBuildManager builder, long ts) throws IOException {
        for (Integer i = 0; i < miniBatchOp.size(); i++) {
            if (isAtomicOperationComplete(miniBatchOp.getOperationStatus(i))) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            // skip this mutation if we aren't enabling indexing or not an atomic op
            // or if it is an atomic op and its timestamp is already set(not LATEST)
            if (!builder.isEnabled(m) &&
                !(builder.isAtomicOp(m) && IndexUtil.getMaxTimestamp(m) == HConstants.LATEST_TIMESTAMP)) {
                continue;
            }
            setTimestampOnMutation(m, ts);

            // set the timestamps on any additional mutations added
            Mutation[] mutationsAddedByCP = miniBatchOp.getOperationsFromCoprocessors(i);
            if (mutationsAddedByCP != null) {
                for (Mutation addedMutation : mutationsAddedByCP) {
                    setTimestampOnMutation(addedMutation, ts);
                }
            }
        }
    }

    private static void setTimestampOnMutation(Mutation m, long ts) throws IOException {
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                CellUtil.setTimestamp(cell, ts);
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

            if (!applyOnePendingDeleteMutation(context, (Delete) m)) {
                miniBatchOp.setOperationStatus(i, NOWRITE);
            }
        }
    }

    /**
     * This method returns true if the pending delete mutation needs to be applied
     * and false f the delete mutation can be ignored for example in the case of
     * delete on non-existing row.
     */
    private boolean applyOnePendingDeleteMutation(BatchMutateContext context, Delete delete) {
        ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(delete.getRow());
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
                return false;
            }
        }

        for (List<Cell> cells : delete.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                switch (cell.getType()) {
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
        return true;
    }

    /**
     * This method applies the pending put mutations on the the next row states.
     * Before this method is called, the next row states is set to current row states.
     */
    private void applyPendingPutMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                          BatchMutateContext context, long now) throws IOException {
        for (Integer i = 0; i < miniBatchOp.size(); i++) {
            if (isAtomicOperationComplete(miniBatchOp.getOperationStatus(i))) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            // skip this mutation if we aren't enabling indexing
            if (!this.builder.isEnabled(m)) {
                continue;
            }

            if (!(m instanceof Put)) {
                continue;
            }

            ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(m.getRow());
            Pair<Put, Put> dataRowState = context.dataRowStates.get(rowKeyPtr);
            if (dataRowState == null) {
                dataRowState = new Pair<Put, Put>(null, null);
                context.dataRowStates.put(rowKeyPtr, dataRowState);
            }
            Put nextDataRowState = dataRowState.getSecond();
            dataRowState.setSecond((nextDataRowState != null) ? applyNew((Put) m, nextDataRowState) : new Put((Put) m));

            Mutation[] mutationsAddedByCP = miniBatchOp.getOperationsFromCoprocessors(i);
            if (mutationsAddedByCP != null) {
                // all added mutations are of type delete corresponding to set nulls
                for (Mutation addedMutation : mutationsAddedByCP) {
                    applyOnePendingDeleteMutation(context, (Delete)addedMutation);
                }
            }
        }
    }

    /**
     * * Prepares next data row state
     */
    private void prepareDataRowStates(ObserverContext<RegionCoprocessorEnvironment> c,
                                      MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                      BatchMutateContext context,
                                      long now) throws IOException {
        if (context.rowsToLock.size() == 0) {
            return;
        }
        applyPendingPutMutations(miniBatchOp, context, now);
        applyPendingDeleteMutations(miniBatchOp, context);
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
            Mutation[] mutationsAddedByCP = miniBatchOp.getOperationsFromCoprocessors(0);
            if (mutationsAddedByCP != null) {
                localUpdates.addAll(Arrays.asList(mutationsAddedByCP));
            }
            miniBatchOp.addOperationsFromCP(0, localUpdates.toArray(new Mutation[localUpdates.size()]));
        }
    }

    /**
     * Determines if any of the data table mutations in the given batch does not include all
     * the indexed columns or the where clause columns for partial uncovered indexes.
     */
    private boolean isPartialUncoveredIndexMutation(PhoenixIndexMetaData indexMetaData,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) {
        int indexedColumnCount = 0;
        for (IndexMaintainer indexMaintainer : indexMetaData.getIndexMaintainers()) {
            indexedColumnCount += indexMaintainer.getIndexedColumns().size();
            if (indexMaintainer.getIndexWhereColumns() != null) {
                indexedColumnCount += indexMaintainer.getIndexWhereColumns().size();
            }
        }
        Set<ColumnReference> columns = new HashSet<ColumnReference>(indexedColumnCount);
        for (IndexMaintainer indexMaintainer : indexMetaData.getIndexMaintainers()) {
            columns.addAll(indexMaintainer.getIndexedColumns());
            if (indexMaintainer.getIndexWhereColumns() != null) {
                columns.addAll(indexMaintainer.getIndexWhereColumns());
            }
        }
        for (int i = 0; i < miniBatchOp.size(); i++) {
            if (isAtomicOperationComplete(miniBatchOp.getOperationStatus(i))) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            if (!this.builder.isEnabled(m)) {
                continue;
            }
            for (ColumnReference column : columns) {
                if (m.get(column.getFamily(), column.getQualifier()).isEmpty()) {
                    // The returned list is empty, which means the indexed column is not
                    // included. This mutation would result in partial index update (and thus
                    // index column values should be retrieved from the existing data table row)
                    return true;
                }
            }
        }
        return false;
    }
    /**
     * Retrieve the data row state either from memory or disk. The rows are locked by the caller.
     */
    private void getCurrentRowStates(ObserverContext<RegionCoprocessorEnvironment> c,
                                     BatchMutateContext context) throws IOException {
        Set<KeyRange> keys = new HashSet<KeyRange>(context.rowsToLock.size());
        for (ImmutableBytesPtr rowKeyPtr : context.rowsToLock) {
            PendingRow pendingRow = new PendingRow(rowKeyPtr, context);
            // Add the data table rows in the mini batch to the per region collection of pending
            // rows. This will be used to detect concurrent updates
            PendingRow existingPendingRow = pendingRows.putIfAbsent(rowKeyPtr, pendingRow);
            if (existingPendingRow == null) {
                // There was no pending row for this row key. We need to retrieve this row from disk
                keys.add(PVarbinary.INSTANCE.getKeyRange(rowKeyPtr.get(), SortOrder.ASC));
            } else {
                // There is a pending row for this row key. We need to retrieve the row from memory
                BatchMutateContext lastContext = existingPendingRow.getLastContext();
                if (existingPendingRow.add(context)) {
                    BatchMutatePhase phase = lastContext.getCurrentPhase();
                    Preconditions.checkArgument(phase != BatchMutatePhase.POST,
                            "the phase of the last batch cannot be POST");
                    if (phase == BatchMutatePhase.PRE) {
                        if (context.lastConcurrentBatchContext == null) {
                            context.lastConcurrentBatchContext = new HashMap<>();
                        }
                        context.lastConcurrentBatchContext.put(rowKeyPtr, lastContext);
                        if (context.maxPendingRowCount < existingPendingRow.getCount()) {
                            context.maxPendingRowCount = existingPendingRow.getCount();
                        }
                        Put put = lastContext.getNextDataRowState(rowKeyPtr);
                        if (put != null) {
                            context.dataRowStates.put(rowKeyPtr, new Pair<>(put, new Put(put)));
                        }
                    } else {
                        // The last batch for this row key failed. We cannot use the memory state.
                        // So we need to retrieve this row from disk
                        keys.add(PVarbinary.INSTANCE.getKeyRange(rowKeyPtr.get(), SortOrder.ASC));
                    }
                } else {
                    // The existing pending row is removed from the map. That means there is no
                    // pending row for this row key anymore. We need to add the new one to the map
                    pendingRows.put(rowKeyPtr, pendingRow);
                    keys.add(PVarbinary.INSTANCE.getKeyRange(rowKeyPtr.get(), SortOrder.ASC));
                }
            }
        }
        if (keys.isEmpty()) {
            return;
        }

        if (this.useBloomFilter) {
            for (KeyRange key : keys) {
                // Scan.java usage alters scan instances, safer to create scan instance per usage
                Scan scan = new Scan();
                // create a scan with same start/stop row key scan#isGetScan()
                // for bloom filters scan should be a get
                scan.withStartRow(key.getLowerRange(), true);
                scan.withStopRow(key.getLowerRange(), true);
                readDataTableRows(c, context, scan);
            }
        }
        else {
            Scan scan = new Scan();
            ScanRanges scanRanges = ScanRanges.createPointLookup(new ArrayList<KeyRange>(keys));
            scanRanges.initializeScan(scan);
            SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
            scan.setFilter(skipScanFilter);
            readDataTableRows(c, context, scan);
        }
    }

    private void readDataTableRows(ObserverContext<RegionCoprocessorEnvironment> c,
                                   BatchMutateContext context, Scan scan) throws IOException {
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
                if (nextDataRowState != null
                        && indexMaintainer.shouldPrepareIndexMutations(nextDataRowState)) {
                    ValueGetter nextDataRowVG = new IndexUtil.SimpleValueGetter(nextDataRowState);
                    Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                            nextDataRowVG, rowKeyPtr, ts, null, null, false);
                    if (indexPut == null) {
                        // No covered column. Just prepare an index row with the empty column
                        byte[] indexRowKey = indexMaintainer.buildRowKey(nextDataRowVG, rowKeyPtr,
                                null, null, ts);
                        indexPut = new Put(indexRowKey);
                    } else {
                        IndexUtil.removeEmptyColumn(indexPut, indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
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
                        ValueGetter currentDataRowVG = new IndexUtil.SimpleValueGetter(currentDataRowState);
                        byte[] indexRowKeyForCurrentDataRow = indexMaintainer.buildRowKey(currentDataRowVG, rowKeyPtr,
                                null, null, ts);
                        if (Bytes.compareTo(indexPut.getRow(), indexRowKeyForCurrentDataRow) != 0) {
                            Mutation del = indexMaintainer.buildRowDeleteMutation(indexRowKeyForCurrentDataRow,
                                    IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                            context.indexUpdates.put(hTableInterfaceReference,
                                    new Pair<Mutation, byte[]>(del, rowKeyPtr.get()));
                        }
                    }
                } else if (currentDataRowState != null
                        && indexMaintainer.shouldPrepareIndexMutations(currentDataRowState)) {
                    ValueGetter currentDataRowVG = new IndexUtil.SimpleValueGetter(currentDataRowState);
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
     * updated. In the three-phase update approach, in phase 1, the status of existing index rows is set to "unverified"
     * (these rows will be deleted from the index table in phase 3), and/or new put mutations are added with the
     * unverified status. In phase 2, data table mutations are applied. In phase 3, the status for an index table row is
     * either set to "verified" or the row is deleted.
     */
    private void preparePreIndexMutations(BatchMutateContext context,
                                          long batchTimestamp,
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
            prepareIndexMutations(context, maintainers, batchTimestamp);

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
                            emptyCF, emptyCQ, batchTimestamp, QueryConstants.UNVERIFIED_BYTES);
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
                                           long batchTimestamp,
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
                    if (!indexMaintainer.isUncovered()) {
                        Put verifiedPut = new Put(m.getRow());
                        // Set the status of the index row to "verified"
                        verifiedPut.addColumn(emptyCF, emptyCQ, batchTimestamp,
                                QueryConstants.VERIFIED_BYTES);
                        context.postIndexUpdates.put(hTableInterfaceReference, verifiedPut);
                    }
                } else {
                    context.postIndexUpdates.put(hTableInterfaceReference, m);
                }
            }
        }
        // all cleanup will be done in postBatchMutateIndispensably()
    }

    private static void identifyIndexMaintainerTypes(PhoenixIndexMetaData indexMetaData, BatchMutateContext context) {
        for (IndexMaintainer indexMaintainer : indexMetaData.getIndexMaintainers()) {
            if (indexMaintainer instanceof TransformMaintainer) {
                context.hasTransform = true;
            } else if (indexMaintainer.isLocalIndex()) {
                context.hasLocalIndex = true;
            } else if (indexMaintainer.isUncovered()) {
                context.hasUncoveredIndex = true;
            } else {
                context.hasGlobalIndex = true;
            }
        }
    }

    private void identifyMutationTypes(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                              BatchMutateContext context) {
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            if (this.builder.isAtomicOp(m)) {
                context.hasAtomic = true;
                if (context.hasDelete) {
                    return;
                }
            } else if (m instanceof Delete)
                context.hasDelete = true;
            if (context.hasAtomic) {
                return;
            }
        }
    }

    /**
     * Wait for the previous batches to complete. If any of the previous batch fails then this
     * batch will fail too and needs to be retried. The rows are locked by the caller.
     * @param table
     * @param context
     * @throws Throwable
     */
    private void waitForPreviousConcurrentBatch(TableName table, BatchMutateContext context)
            throws Throwable {
        for (BatchMutateContext lastContext : context.lastConcurrentBatchContext.values()) {
            BatchMutatePhase phase = lastContext.getCurrentPhase();
            if (phase == BatchMutatePhase.FAILED) {
                context.currentPhase = BatchMutatePhase.FAILED;
                break;
            } else if (phase == BatchMutatePhase.PRE) {
                CountDownLatch countDownLatch = lastContext.getCountDownLatch();
                if (countDownLatch == null) {
                    // phase changed from PRE to either FAILED or POST
                    if (phase == BatchMutatePhase.FAILED) {
                        context.currentPhase = BatchMutatePhase.FAILED;
                        break;
                    }
                    continue;
                }
                // Release the locks so that the previous concurrent mutation can go into the post phase
                unlockRows(context);
                // Wait for at most one concurrentMutationWaitDuration for each level in the dependency tree of batches.
                // lastContext.getMaxPendingRowCount() is the depth of the subtree rooted at the batch pointed by lastContext
                if (!countDownLatch.await((lastContext.getMaxPendingRowCount() + 1) * concurrentMutationWaitDuration,
                        TimeUnit.MILLISECONDS)) {
                    context.currentPhase = BatchMutatePhase.FAILED;
                    LOG.debug(String.format("latch timeout context %s last %s", context, lastContext));
                    break;
                }
                if (lastContext.getCurrentPhase() == BatchMutatePhase.FAILED) {
                    context.currentPhase = BatchMutatePhase.FAILED;
                    break;
                }
                // Acquire the locks again before letting the region proceed with data table updates
                lockRows(context);
                LOG.debug(String.format("context %s last %s exit phase %s", context, lastContext,
                        lastContext.getCurrentPhase()));
            }
        }
        if (context.currentPhase == BatchMutatePhase.FAILED) {
            // This batch needs to be retried since one of the previous concurrent batches has not completed yet.
            // Throwing an IOException will result in retries of this batch. Removal of reference counts and
            // locks for the rows of this batch will be done in postBatchMutateIndispensably()
            throw new IOException("One of the previous concurrent mutations has not completed. " +
                    "The batch needs to be retried " + table.getNameAsString());
        }
    }

    private boolean shouldSleep(BatchMutateContext context) {
        for (ImmutableBytesPtr ptr : context.rowsToLock) {
            for (Set set : batchesWithLastTimestamp) {
                if (set.contains(ptr)) {
                    return true;
                }
            }
        }
        return false;
    }
    private long getBatchTimestamp(BatchMutateContext context, TableName table)
            throws InterruptedException {
        synchronized (this) {
            long ts = EnvironmentEdgeManager.currentTimeMillis();
            if (ts != lastTimestamp) {
                // The timestamp for this batch will be different from the last batch processed.
                lastTimestamp = ts;
                batchesWithLastTimestamp.clear();
                batchesWithLastTimestamp.add(context.rowsToLock);
                return ts;
            } else {
                if (!shouldSleep(context)) {
                    // There is no need to sleep as the last batches with the same timestamp
                    // do not have a common row this batch
                    batchesWithLastTimestamp.add(context.rowsToLock);
                    return ts;
                }
            }
        }
        // Sleep for one millisecond. The sleep is necessary to get different timestamps
        // for concurrent batches that share common rows.
        Thread.sleep(1);
        LOG.debug("slept 1ms for " + table.getNameAsString());
        synchronized (this) {
            long ts = EnvironmentEdgeManager.currentTimeMillis();
            if (ts != lastTimestamp) {
                // The timestamp for this batch will be different from the last batch processed.
                lastTimestamp = ts;
                batchesWithLastTimestamp.clear();
            }
            // We do not have to check again if we need to sleep again since we got the next
            // timestamp while holding the row locks. This mean there cannot be a new
            // mutation with the same row attempting get the same timestamp
            batchesWithLastTimestamp.add(context.rowsToLock);
            return ts;
        }
    }
    public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
                                             MiniBatchOperationInProgress<Mutation> miniBatchOp) throws Throwable {
        PhoenixIndexMetaData indexMetaData = getPhoenixIndexMetaData(c, miniBatchOp);
        BatchMutateContext context = new BatchMutateContext(indexMetaData.getClientVersion());
        setBatchMutateContext(c, context);
        identifyIndexMaintainerTypes(indexMetaData, context);
        identifyMutationTypes(miniBatchOp, context);
        context.populateOriginalMutations(miniBatchOp);

        if (context.hasDelete) {
            // Need to add cell tags to Delete Marker before we do any index processing
            // since we add tags to tables which doesn't have indexes also.
            ServerIndexUtil.setDeleteAttributes(miniBatchOp);
        }

        // Exclusively lock all rows to do consistent writes over multiple tables
        // (i.e., the data and its index tables)
        populateRowsToLock(miniBatchOp, context);
        // early exit if it turns out we don't have any update for indexes
        if (context.rowsToLock.isEmpty()) {
            return;
        }
        lockRows(context);
        long onDupCheckTime = 0;

        if (context.hasAtomic || context.hasGlobalIndex || context.hasUncoveredIndex || context.hasTransform) {
            // Retrieve the current row states from the data table while holding the lock.
            // This is needed for both atomic mutations and global indexes
            long start = EnvironmentEdgeManager.currentTimeMillis();
            context.dataRowStates = new HashMap<ImmutableBytesPtr, Pair<Put, Put>>(context.rowsToLock.size());
            if (context.hasGlobalIndex || context.hasTransform || context.hasAtomic ||
                    context.hasDelete ||  (context.hasUncoveredIndex &&
                    isPartialUncoveredIndexMutation(indexMetaData, miniBatchOp))) {
                getCurrentRowStates(c, context);
            }
            onDupCheckTime += (EnvironmentEdgeManager.currentTimeMillis() - start);
        }

        if (context.hasAtomic) {
            long start = EnvironmentEdgeManager.currentTimeMillis();
            // add the mutations for conditional updates to the mini batch
            addOnDupMutationsToBatch(miniBatchOp, context);

            // release locks for ON DUPLICATE KEY IGNORE since we won't be changing those rows
            // this is needed so that we can exit early
            releaseLocksForOnDupIgnoreMutations(miniBatchOp, context);
            onDupCheckTime += (EnvironmentEdgeManager.currentTimeMillis() - start);
            metricSource.updateDuplicateKeyCheckTime(dataTableName, onDupCheckTime);

            // early exit if we are not changing any rows
            if (context.rowsToLock.isEmpty()) {
                return;
            }
        }

        TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
        long batchTimestamp = getBatchTimestamp(context, table);
        // Update the timestamps of the data table mutations to prevent overlapping timestamps
        // (which prevents index inconsistencies as this case is not handled).
        setTimestamps(miniBatchOp, builder, batchTimestamp);
        if (context.hasGlobalIndex || context.hasUncoveredIndex || context.hasTransform) {
            // Prepare next data rows states for pending mutations (for global indexes)
            prepareDataRowStates(c, miniBatchOp, context, batchTimestamp);
            // early exit if it turns out we don't have any edits
            long start = EnvironmentEdgeManager.currentTimeMillis();
            preparePreIndexMutations(context, batchTimestamp, indexMetaData);
            metricSource.updateIndexPrepareTime(dataTableName,
                EnvironmentEdgeManager.currentTimeMillis() - start);
            // Release the locks before making RPC calls for index updates
            unlockRows(context);
            // Do the first phase index updates
            doPre(context);
            // Acquire the locks again before letting the region proceed with data table updates
            lockRows(context);
            if (context.lastConcurrentBatchContext != null) {
                waitForPreviousConcurrentBatch(table, context);
            }
            preparePostIndexMutations(context, batchTimestamp, indexMetaData);
        }
        if (context.hasLocalIndex) {
            // Group all the updates for a single row into a single update to be processed (for local indexes)
            Collection<? extends Mutation> mutations = groupMutations(miniBatchOp, context);
            handleLocalIndexUpdates(table, miniBatchOp, mutations, indexMetaData);
        }
        if (failDataTableUpdatesForTesting) {
            throw new DoNotRetryIOException("Simulating the data table write failure");
        }
    }

    /**
     * In case of ON DUPLICATE KEY IGNORE, if the row already exists no mutations will be
     * generated so release the row lock.
     */
    private void releaseLocksForOnDupIgnoreMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                                     BatchMutateContext context) {
        for (int i = 0; i < miniBatchOp.size(); i++) {
            if (!isAtomicOperationComplete(miniBatchOp.getOperationStatus(i))) {
                continue;
            }
            Mutation m = miniBatchOp.getOperation(i);
            if (!this.builder.isAtomicOp(m)) {
                continue;
            }
            ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
            Iterator<RowLock> rowLockIterator = context.rowLocks.iterator();
            while(rowLockIterator.hasNext()){
                RowLock rowLock = rowLockIterator.next();
                ImmutableBytesPtr rowKey = rowLock.getRowKey();
                if (row.equals(rowKey)) {
                    PendingRow pendingRow = pendingRows.get(rowKey);
                    if (pendingRow != null) {
                        pendingRow.remove();
                    }
                    rowLock.release();
                    rowLockIterator.remove();
                    context.rowsToLock.remove(row);
                    break;
                }
            }
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
        if (shouldWALAppend) {
            BatchMutateContext context = getBatchMutateContext(c);
            appendMutationAttributesToWALKey(key, context);
        }
    }

    public void appendMutationAttributesToWALKey(WALKey key,
                                                        IndexRegionObserver.BatchMutateContext context) {
        if (context != null && context.getOriginalMutations().size() > 0) {
            Mutation firstMutation = context.getOriginalMutations().get(0);
            Map<String, byte[]> attrMap = firstMutation.getAttributesMap();
            for (MutationState.MutationMetadataType metadataType :
                    MutationState.MutationMetadataType.values()) {
                String metadataTypeKey = metadataType.toString();
                if (attrMap.containsKey(metadataTypeKey)) {
                    IndexRegionObserver.appendToWALKey(key, metadataTypeKey,
                            attrMap.get(metadataTypeKey));
                }
            }
        }
    }

    /**
     * When this hook is called, all the rows in the batch context are locked if the batch of
     * mutations is successful. Because the rows are locked, we can safely make updates to
     * pending row states in memory and perform the necessary cleanup in that case.
     *
     * However, when the batch fails, then some of the rows may not be locked. In that case,
     * we remove the pending row states from the concurrent hash map without updating them since
     * pending rows states become invalid when a batch fails.
     */
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
              if(context.hasAtomic && miniBatchOp.size() == 1) {
                  if (!isAtomicOperationComplete(miniBatchOp.getOperationStatus(0))) {
                      byte[] retVal = PInteger.INSTANCE.toBytes(1);
                      Cell cell = PhoenixKeyValueUtil.newKeyValue(
                              miniBatchOp.getOperation(0).getRow(), Bytes.toBytes(UPSERT_CF),
                              Bytes.toBytes(UPSERT_STATUS_CQ), 0, retVal, 0, retVal.length);
                      Result result = Result.create(new ArrayList<>(Arrays.asList(cell)));
                      miniBatchOp.setOperationStatus(0,
                              new OperationStatus(SUCCESS, result));
                  }
              }
          } else {
              context.currentPhase = BatchMutatePhase.FAILED;
          }
          context.countDownAllLatches();
          removePendingRows(context);
          if (context.indexUpdates != null) {
              context.indexUpdates.clear();
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
      for (ImmutableBytesPtr rowKey : context.rowsToLock) {
          PendingRow pendingRow = pendingRows.get(rowKey);
          if (pendingRow != null) {
              pendingRow.remove();
          }
      }
  }

  private void doPre(BatchMutateContext context) throws IOException {
      long start = 0;
      try {
          start = EnvironmentEdgeManager.currentTimeMillis();
          if (failPreIndexUpdatesForTesting) {
              throw new DoNotRetryIOException("Simulating the first (i.e., pre) index table write failure");
          }
          doIndexWritesWithExceptions(context, false);
          metricSource.updatePreIndexUpdateTime(dataTableName,
              EnvironmentEdgeManager.currentTimeMillis() - start);
      } catch (Throwable e) {
          metricSource.updatePreIndexUpdateFailureTime(dataTableName,
              EnvironmentEdgeManager.currentTimeMillis() - start);
          metricSource.incrementPreIndexUpdateFailures(dataTableName);
          // Re-acquire all locks since we released them before making index updates
          // Removal of reference counts and locks for the rows of this batch will be
          // done in postBatchMutateIndispensably()
          lockRows(context);
          rethrowIndexingException(e);
      }
  }

  private void extractExpressionsAndColumns(DataInputStream input,
                              List<Pair<PTable, List<Expression>>> operations,
                              final Set<ColumnReference> colsReadInExpr) throws IOException {
      while (true) {
          ExpressionVisitor<Void> visitor = new StatelessTraverseAllExpressionVisitor<Void>() {
              @Override
              public Void visit(KeyValueColumnExpression expression) {
                  colsReadInExpr.add(new ColumnReference(expression.getColumnFamily(), expression.getColumnQualifier()));
                  return null;
              }
          };
          try {
              int nExpressions = WritableUtils.readVInt(input);
              List<Expression> expressions = Lists.newArrayListWithExpectedSize(nExpressions);
              for (int i = 0; i < nExpressions; i++) {
                  Expression expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
                  expression.readFields(input);
                  expressions.add(expression);
                  expression.accept(visitor);
              }
              PTableProtos.PTable tableProto = PTableProtos.PTable.parseDelimitedFrom(input);
              PTable table = PTableImpl.createFromProto(tableProto);
              operations.add(new Pair<>(table, expressions));
          } catch (EOFException e) {
              break;
          }
      }
  }

    /**
     * This function has been adapted from PhoenixIndexBuilder#executeAtomicOp().
     * The critical difference being that the code in PhoenixIndexBuilder#executeAtomicOp()
     * generates the mutations by reading the latest data table row from HBase but in order
     * to correctly support concurrent index mutations we need to always read the latest
     * data table row from memory.
     * It takes in an atomic Put mutation and generates a list of Put and Delete mutations.
     * The mutation list will be empty in two cases:
     * 1) ON DUPLICATE KEY IGNORE and the row already exists;
     * 2) ON DUPLICATE KEY UPDATE if CASE expression is specified and in each of them the new
     * value is the same as the old value in the ELSE-clause.
     * Otherwise, we will generate one Put mutation and optionally one Delete mutation (with
     * DeleteColumn type cells for all columns set to null).
     */
  private List<Mutation> generateOnDupMutations(BatchMutateContext context, Put atomicPut) throws IOException {
      List<Mutation> mutations = Lists.newArrayListWithExpectedSize(2);
      byte[] opBytes = atomicPut.getAttribute(ATOMIC_OP_ATTRIB);
      if (opBytes == null) { // Unexpected
          return null;
      }
      Put put = null;
      Delete delete = null;

      // mutations returned by this function will have the LATEST timestamp
      // later these timestamps will be updated by the IndexRegionObserver#setTimestamps() function
      long ts = HConstants.LATEST_TIMESTAMP;

      byte[] rowKey = atomicPut.getRow();
      ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(rowKey);
      // Get the latest data row state
      Pair<Put, Put> dataRowState = context.dataRowStates.get(rowKeyPtr);
      Put currentDataRowState = dataRowState != null ? dataRowState.getFirst() : null;

      if (PhoenixIndexBuilderHelper.isDupKeyIgnore(opBytes)) {
          if (currentDataRowState == null) {
              // new row
              mutations.add(atomicPut);
          }
          return mutations;
      }

      ByteArrayInputStream stream = new ByteArrayInputStream(opBytes);
      DataInputStream input = new DataInputStream(stream);
      boolean skipFirstOp = input.readBoolean();
      short repeat = input.readShort();

      List<Pair<PTable, List<Expression>>> operations = Lists.newArrayListWithExpectedSize(3);
      final Set<ColumnReference> colsReadInExpr = new HashSet<>();
      // deserialize the conditional update expressions and
      // extract the columns that are read in the conditional expressions
      extractExpressionsAndColumns(input, operations, colsReadInExpr);
      int estimatedSize = colsReadInExpr.size();

      // initialized to either the incoming new row or the current row
      // stores the intermediate values as we apply conditional update expressions
      List<Cell> flattenedCells;
      // read the column values requested in the get from the current data row
      List<Cell> cells = IndexUtil.readColumnsFromRow(currentDataRowState, colsReadInExpr);

      // store current cells into a map where the key is ColumnReference of the column family and
      // column qualifier, and value is a pair of cell and a boolean. The value of the boolean
      // will be true if the expression is CaseExpression and Else-clause is evaluated to be
      // true, will be null if there is no expression on this column, otherwise false
      Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap = new HashMap<>();

      if (currentDataRowState == null) { // row doesn't exist
          if (skipFirstOp) {
              if (operations.size() <= 1 && repeat <= 1) {
                  // early exit since there is only one ON DUPLICATE KEY UPDATE
                  // clause which is ignored because the row doesn't exist so
                  // simply use the values in UPSERT VALUES
                  mutations.add(atomicPut);
                  return mutations;
              }
              // If there are multiple ON DUPLICATE KEY UPDATE on a new row,
              // the first one is skipped
              repeat--;
          }
          // Base current state off of new row
          flattenedCells = flattenCells(atomicPut);
      } else {
          // Base current state off of existing row
          flattenedCells = cells;
          // store all current cells from currentDataRowState
          for (Map.Entry<byte[], List<Cell>> entry :
                  currentDataRowState.getFamilyCellMap().entrySet()) {
              for (Cell cell : new ArrayList<>(entry.getValue())) {
                  byte[] family = CellUtil.cloneFamily(cell);
                  byte[] qualifier = CellUtil.cloneQualifier(cell);
                  ColumnReference colRef = new ColumnReference(family, qualifier);
                  currColumnCellExprMap.put(colRef, new Pair<>(cell, null));
              }
          }
      }

      MultiKeyValueTuple tuple = new MultiKeyValueTuple(flattenedCells);
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();

      // for each conditional upsert in the batch
      for (int opIndex = 0; opIndex < operations.size(); opIndex++) {
          Pair<PTable, List<Expression>> operation = operations.get(opIndex);
          PTable table = operation.getFirst();
          List<Expression> expressions = operation.getSecond();
          for (int j = 0; j < repeat; j++) { // repeater loop
              ptr.set(rowKey);
              // Sort the list of cells (if they've been flattened in which case they're
              // not necessarily ordered correctly).
              if (flattenedCells != null) {
                  Collections.sort(flattenedCells, CellComparator.getInstance());
              }
              PRow row = table.newRow(GenericKeyValueBuilder.INSTANCE, ts, ptr, false);
              int adjust = table.getBucketNum() == null ? 1 : 2;
              for (int i = 0; i < expressions.size(); i++) {
                  Expression expression = expressions.get(i);
                  ptr.set(EMPTY_BYTE_ARRAY);
                  expression.evaluate(tuple, ptr);
                  PColumn column = table.getColumns().get(i + adjust);
                  Object value = expression.getDataType().toObject(ptr, column.getSortOrder());
                  // We are guaranteed that the two column will have the same type
                  if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(),
                      expression.getSortOrder(), expression.getMaxLength(), expression.getScale(),
                      column.getMaxLength(), column.getScale())) {
                      throw new DataExceedsCapacityException(column.getDataType(), column.getMaxLength(),
                          column.getScale(), column.getName().getString());
                  }
                  column.getDataType().coerceBytes(ptr, value, expression.getDataType(), expression.getMaxLength(),
                      expression.getScale(), expression.getSortOrder(), column.getMaxLength(), column.getScale(),
                      column.getSortOrder(), table.rowKeyOrderOptimizable());
                  byte[] bytes = ByteUtil.copyKeyBytesIfNecessary(ptr);
                  row.setValue(column, bytes);

                  // If the column exist in currColumnCellExprMap, set the boolean value in the
                  // map to be true if the expression is CaseExpression and the Else-clause is
                  // evaluated to be true
                  ColumnReference colRef = new ColumnReference(column.getFamilyName().getBytes(),
                          column.getColumnQualifierBytes());
                  if (currColumnCellExprMap.containsKey(colRef)) {
                      Pair<Cell, Boolean> valuePair = currColumnCellExprMap.get(colRef);
                      if (expression instanceof CaseExpression
                              && ((CaseExpression) expression).evaluateIndexOf(tuple, ptr)
                              == expression.getChildren().size() - 1) {
                          valuePair.setSecond(true);
                      } else {
                          valuePair.setSecond(false);
                      }
                  }
              }
              List<Cell> updatedCells = Lists.newArrayListWithExpectedSize(estimatedSize);
              List<Mutation> newMutations = row.toRowMutations();
              for (Mutation source : newMutations) {
                  flattenCells(source, updatedCells);
              }
              // update the cells to the latest values calculated above
              flattenedCells = mergeCells(flattenedCells, updatedCells);
              // we need to retrieve empty cell later on which relies on binary search
              flattenedCells.sort(CellComparator.getInstance());
              tuple.setKeyValues(flattenedCells);
          }
          // Repeat only applies to first statement
          repeat = 1;
      }

      put = new Put(rowKey);
      delete = new Delete(rowKey);
      transferAttributes(atomicPut, put);
      transferAttributes(atomicPut, delete);
      for (int i = 0; i < tuple.size(); i++) {
          Cell cell = tuple.getValue(i);
          if (cell.getType() == Cell.Type.Put) {
              if (checkCellNeedUpdate(cell, currColumnCellExprMap)) {
                  put.add(cell);
              }
          } else {
              delete.add(cell);
          }
      }

      if (!put.isEmpty() || !delete.isEmpty()) {
          PTable table = operations.get(0).getFirst();
          addEmptyKVCellToPut(put, tuple, table);
      }

      if (!put.isEmpty()) {
          mutations.add(put);
      }
      if (!delete.isEmpty()) {
          mutations.add(delete);
      }

      return mutations;
  }

    private void addEmptyKVCellToPut(Put put, MultiKeyValueTuple tuple, PTable table) throws IOException {
        byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table);
        byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(table).getFirst();
        Cell emptyKVCell = tuple.getValue(emptyCF, emptyCQ);
        if (emptyKVCell != null) {
            put.add(emptyKVCell);
        }
    }

    private static List<Cell> flattenCells(Mutation m) {
        List<Cell> flattenedCells = new ArrayList<>();
        flattenCells(m, flattenedCells);
        return flattenedCells;
    }

    private static void flattenCells(Mutation m, List<Cell> flattenedCells) {
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            flattenedCells.addAll(cells);
        }
    }

    /**
     * This function is to check if a cell need to be updated, based on the current cells' values.
     * The cell will not be updated only if the column exist in the expression in which CASE is
     * specified and the new value is the same as the old value in the ELSE-clause, otherwise it
     * should be updated.
     *
     * @param cell the cell with new value to be checked
     * @param colCellExprMap the column reference map with cell current value
     * @return true if the cell need update, false otherwise
     */
    private boolean checkCellNeedUpdate(Cell cell,
                                        Map<ColumnReference, Pair<Cell, Boolean>> colCellExprMap) {
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        ColumnReference colRef = new ColumnReference(family, qualifier);

        // if cell not exist in the map, meaning that they are new and need update
        if (colCellExprMap.isEmpty() || !colCellExprMap.containsKey(colRef)) {
            return true;
        }

        Pair<Cell, Boolean> valuePair = colCellExprMap.get(colRef);
        Boolean isInCaseExpressionElseClause = valuePair.getSecond();
        if (isInCaseExpressionElseClause == null) {
            return false;
        }
        if (!isInCaseExpressionElseClause) {
            return true;
        }
        Cell oldCell = valuePair.getFirst();
        ImmutableBytesPtr newValuePtr = new ImmutableBytesPtr(cell.getValueArray(),
                cell.getValueOffset(), cell.getValueLength());
        ImmutableBytesPtr oldValuePtr = new ImmutableBytesPtr(oldCell.getValueArray(),
                oldCell.getValueOffset(), oldCell.getValueLength());
        return !Bytes.equals(oldValuePtr.get(), oldValuePtr.getOffset(), oldValuePtr.getLength(),
                newValuePtr.get(), newValuePtr.getOffset(), newValuePtr.getLength());
    }

    /**
     * ensure that the generated mutations have all the attributes like schema
     */
    private static void transferAttributes(Mutation source, Mutation target) {
        for (Map.Entry<String, byte[]> entry : source.getAttributesMap().entrySet()) {
            target.setAttribute(entry.getKey(), entry.getValue());
        }
    }

    /**
     * First take all the cells that are present in the latest. Then look at current
     * and any cell not present in latest is taken.
     */
    private static List<Cell> mergeCells(List<Cell> current, List<Cell> latest) {
        Map<ColumnReference, Cell> latestColVals = Maps.newHashMapWithExpectedSize(latest.size() + current.size());

        // first take everything present in latest
        for (Cell cell : latest) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            ColumnReference colInfo = new ColumnReference(family, qualifier);
            latestColVals.put(colInfo, cell);
        }

        // check for any leftovers in current
        for (Cell cell : current) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            ColumnReference colInfo = new ColumnReference(family, qualifier);
            if (!latestColVals.containsKey(colInfo)) {
                latestColVals.put(colInfo, cell);
            }
        }
        return Lists.newArrayList(latestColVals.values());
    }

    public static void appendToWALKey(WALKey key, String attrKey, byte[] attrValue) {
        key.addExtendedAttribute(attrKey, attrValue);
    }

    public static byte[] getAttributeValueFromWALKey(WALKey key, String attrKey) {
        return key.getExtendedAttribute(attrKey);
    }

    public static Map<String, byte[]> getAttributeValuesFromWALKey(WALKey key) {
        return new HashMap<String, byte[]>(key.getExtendedAttributes());
    }

    /**
     * Determines whether the atomic operation is complete based on the operation status.
     * HBase returns null Result by default for successful Put and Delete mutations, only for
     * Increment and Append mutations, non-null Result is returned by default.
     * @param status the operation status.
     * @return true if the atomic operation is completed, false otherwise.
     */
    public static boolean isAtomicOperationComplete(OperationStatus status) {
        return status.getOperationStatusCode() == SUCCESS && status.getResult() != null;
    }
}