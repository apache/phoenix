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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver.ReplayWrite;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.LockManager.RowLock;
import org.apache.phoenix.hbase.index.builder.FatalIndexBuildingFailureException;
import org.apache.phoenix.hbase.index.builder.IndexBuildManager;
import org.apache.phoenix.hbase.index.builder.IndexBuilder;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.LazyParallelWriterIndexCommitter;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make.
 * We don't need to implement {@link #postPut(ObserverContext, Put, WALEdit, Durability)} and
 * {@link #postDelete(ObserverContext, Delete, WALEdit, Durability)} hooks because
 * Phoenix always does batch mutations.
 * <p>
 */
public class IndexRegionObserver implements RegionObserver, RegionCoprocessor {

  private static final Logger LOG = LoggerFactory.getLogger(IndexRegionObserver.class);
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

  private static final String INDEX_RECOVERY_FAILURE_POLICY_KEY = "org.apache.hadoop.hbase.index.recovery.failurepolicy";

  public static final String INDEX_LAZY_POST_BATCH_WRITE = "org.apache.hadoop.hbase.index.lazy.post_batch.write";
  private static final boolean INDEX_LAZY_POST_BATCH_WRITE_DEFAULT = false;

  private static final String INDEXER_INDEX_WRITE_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.post.batch.mutate.threshold";
  private static final long INDEXER_INDEX_WRITE_SLOW_THRESHOLD_DEFAULT = 3_000;
  private static final String INDEXER_INDEX_PREPARE_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.pre.batch.mutate.threshold";
  private static final long INDEXER_INDEX_PREPARE_SLOW_THREHSOLD_DEFAULT = 3_000;
  private static final String INDEXER_POST_OPEN_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.open.threshold";
  private static final long INDEXER_POST_OPEN_SLOW_THRESHOLD_DEFAULT = 3_000;
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
      long ts = 0;
      Iterator iterator = m.getFamilyCellMap().entrySet().iterator();
      while (iterator.hasNext()) {
          Map.Entry<byte[], List<Cell>> entry = (Map.Entry) iterator.next();
          Iterator<Cell> cellIterator = entry.getValue().iterator();
          while (cellIterator.hasNext()) {
              Cell cell = cellIterator.next();
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

  private Collection<? extends Mutation> groupMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                                        long now, ReplayWrite replayWrite) throws IOException {
      Map<ImmutableBytesPtr, MultiMutation> mutationsMap = new HashMap<>();
      boolean copyMutations = false;
      for (int i = 0; i < miniBatchOp.size(); i++) {
          if (miniBatchOp.getOperationStatus(i) == IGNORE) {
              continue;
          }
          Mutation m = miniBatchOp.getOperation(i);
          if (this.builder.isEnabled(m)) {
              // Track whether or not we need to
              ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
              if (mutationsMap.containsKey(row)) {
                  copyMutations = true;
              } else {
                  mutationsMap.put(row, null);
              }
          }
      }
      // early exit if it turns out we don't have any edits
      if (mutationsMap.isEmpty()) {
          return null;
      }
      // If we're copying the mutations
      Collection<Mutation> originalMutations;
      Collection<? extends Mutation> mutations;
      if (copyMutations) {
          originalMutations = null;
          mutations = mutationsMap.values();
      } else {
          originalMutations = Lists.newArrayListWithExpectedSize(mutationsMap.size());
          mutations = originalMutations;
      }

      boolean resetTimeStamp = replayWrite == null;

      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          // skip this mutation if we aren't enabling indexing
          // unfortunately, we really should ask if the raw mutation (rather than the combined mutation)
          // should be indexed, which means we need to expose another method on the builder. Such is the
          // way optimization go though.
          if (miniBatchOp.getOperationStatus(i) != IGNORE && this.builder.isEnabled(m)) {
              if (resetTimeStamp) {
                  // Unless we're replaying edits to rebuild the index, we update the time stamp
                  // of the data table to prevent overlapping time stamps (which prevents index
                  // inconsistencies as this case isn't handled correctly currently).
                  for (List<Cell> cells : m.getFamilyCellMap().values()) {
                      for (Cell cell : cells) {
                          CellUtil.setTimestamp(cell, now);
                      }
                  }
              }
              // No need to write the table mutations when we're rebuilding
              // the index as they're already written and just being replayed.
              if (replayWrite == ReplayWrite.INDEX_ONLY
                      || replayWrite == ReplayWrite.REBUILD_INDEX_ONLY) {
                  miniBatchOp.setOperationStatus(i, NOWRITE);
              }

              // Only copy mutations if we found duplicate rows
              // which only occurs when we're partially rebuilding
              // the index (since we'll potentially have both a
              // Put and a Delete mutation for the same row).
              if (copyMutations) {
                  // Add the mutation to the batch set

                  ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
                  MultiMutation stored = mutationsMap.get(row);
                  // we haven't seen this row before, so add it
                  if (stored == null) {
                      stored = new MultiMutation(row);
                      mutationsMap.put(row, stored);
                  }
                  stored.addAll(m);
              } else {
                  originalMutations.add(m);
              }
          }
      }

      if (copyMutations || replayWrite != null) {
          mutations = IndexManagementUtil.flattenMutationsByTimestamp(mutations);
      }
      return mutations;
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

  private void prepareIndexMutations(
          ObserverContext<RegionCoprocessorEnvironment> c,
          MiniBatchOperationInProgress<Mutation> miniBatchOp,
          BatchMutateContext context,
          Collection<? extends Mutation> mutations,
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
          this.builder.getIndexUpdates(context.indexUpdates, miniBatchOp, mutations, indexMetaData);
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
                  // add the VERIFIED cell, which is the empty cell
                  Mutation m = update.getFirst();
                  if (context.rebuild) {
                      if (m instanceof Put) {
                          long ts = getMaxTimestamp(m);
                          // Remove the empty column prepared by Index codec as we need to change its value
                          removeEmptyColumn(m, emptyCF, emptyCQ);
                          ((Put) m).addColumn(emptyCF, emptyCQ, ts, VERIFIED_BYTES);
                      }
                      context.preIndexUpdates.put(hTableInterfaceReference, m);
                  } else {
                      if (m instanceof Put) {
                          // Remove the empty column prepared by Index codec as we need to change its value
                          removeEmptyColumn(m, emptyCF, emptyCQ);
                          // Set the status of the index row to "unverified"
                          ((Put) m).addColumn(emptyCF, emptyCQ, now, UNVERIFIED_BYTES);
                          // This will be done before the data table row is updated (i.e., in the first write phase)
                          context.preIndexUpdates.put(hTableInterfaceReference, m);
                      }
                      else {
                          // Set the status of the index row to "unverified"
                          Put unverifiedPut = new Put(m.getRow());
                          unverifiedPut.addColumn(emptyCF, emptyCQ, now, UNVERIFIED_BYTES);
                          // This will be done before the data table row is updated (i.e., in the first write phase)
                          context.preIndexUpdates.put(hTableInterfaceReference, unverifiedPut);
                      }
                  }
              }
          }
          TracingUtils.addAnnotation(current, "index update count", updateCount);
      }
  }

  protected PhoenixIndexMetaData getPhoenixIndexMetaData(
          ObserverContext<RegionCoprocessorEnvironment> observerContext,
          MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      IndexMetaData indexMetaData = this.builder.getIndexMetaData(miniBatchOp);
      if (!(indexMetaData instanceof PhoenixIndexMetaData)) {
          throw new DoNotRetryIOException(
                  "preBatchMutateWithExceptions: indexMetaData is not an instance of "+PhoenixIndexMetaData.class.getName() +
                          ", current table is:" +
                          observerContext.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString());
      }
      return (PhoenixIndexMetaData)indexMetaData;
  }

  private void preparePostIndexMutations(
          BatchMutateContext context,
          long now,
          PhoenixIndexMetaData indexMetaData) throws Throwable {
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

  public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
          MiniBatchOperationInProgress<Mutation> miniBatchOp) throws Throwable {
      ignoreAtomicOperations(miniBatchOp);
      PhoenixIndexMetaData indexMetaData = getPhoenixIndexMetaData(c, miniBatchOp);
      BatchMutateContext context = new BatchMutateContext(indexMetaData.getClientVersion());
      setBatchMutateContext(c, context);
      Mutation firstMutation = miniBatchOp.getOperation(0);
      ReplayWrite replayWrite = this.builder.getReplayWrite(firstMutation);
      context.rebuild = replayWrite != null;
      /*
       * Exclusively lock all rows so we get a consistent read
       * while determining the index updates
       */
      long now;
      if (!context.rebuild) {
          populateRowsToLock(miniBatchOp, context);
          lockRows(context);
          now = EnvironmentEdgeManager.currentTimeMillis();
          // Add the table rows in the mini batch to the collection of pending rows. This will be used to detect
          // concurrent updates
          populatePendingRows(context);
      }
      else {
          now = EnvironmentEdgeManager.currentTimeMillis();
      }
      // First group all the updates for a single row into a single update to be processed
      Collection<? extends Mutation> mutations = groupMutations(miniBatchOp, now, replayWrite);
      // early exit if it turns out we don't have any edits
      if (mutations == null) {
          return;
      }

      long start = EnvironmentEdgeManager.currentTimeMillis();
      prepareIndexMutations(c, miniBatchOp, context, mutations, now, indexMetaData);
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
      if (!context.rebuild) {
          // Acquire the locks again before letting the region proceed with data table updates
          List<RowLock> rowLocks = Lists.newArrayListWithExpectedSize(context.rowLocks.size());
          for (RowLock rowLock : context.rowLocks) {
              rowLocks.add(lockManager.lockRow(rowLock.getRowKey(), rowLockWaitDuration));
          }
          context.rowLocks.clear();
          context.rowLocks = rowLocks;
          preparePostIndexMutations(context, now, indexMetaData);
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
   * Exposed for testing!
   * @return the currently instantiated index builder
   */
  public IndexBuilder getBuilderForTesting() {
    return this.builder.getBuilderForTesting();
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

