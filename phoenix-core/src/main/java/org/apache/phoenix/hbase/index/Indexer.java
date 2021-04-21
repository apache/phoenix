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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
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
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.apache.phoenix.hbase.index.write.IndexFailurePolicy;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.RecoveryIndexWriter;
import org.apache.phoenix.hbase.index.write.recovery.PerRegionIndexWriteCache;
import org.apache.phoenix.hbase.index.write.recovery.StoreFailuresInCachePolicy;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make.
 * <p>
 * If the WAL is enabled, these updates are then added to the WALEdit and attempted to be written to
 * the WAL after the WALEdit has been saved. If any of the index updates fail, this server is
 * immediately terminated and we rely on WAL replay to attempt the index updates again (see
 * #preWALRestore(ObserverContext, HRegionInfo, HLogKey, WALEdit)).
 * <p>
 * If the WAL is disabled, the updates are attempted immediately. No consistency guarantees are made
 * if the WAL is disabled - some or none of the index updates may be successful. All updates in a
 * single batch must have the same durability level - either everything gets written to the WAL or
 * nothing does. Currently, we do not support mixed-durability updates within a single batch. If you
 * want to have different durability levels, you only need to split the updates into two different
 * batches.
 * <p>
 * We don't need to implement {@link #postPut(ObserverContext, Put, WALEdit, Durability)} and
 * {@link #postDelete(ObserverContext, Delete, WALEdit, Durability)} hooks because 
 * Phoenix always does batch mutations.
 * <p>
 */
public class Indexer implements RegionObserver, RegionCoprocessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Indexer.class);
  private static final OperationStatus IGNORE = new OperationStatus(OperationStatusCode.SUCCESS);
  private static final OperationStatus NOWRITE = new OperationStatus(OperationStatusCode.SUCCESS);
  

  protected IndexWriter writer;
  protected IndexBuildManager builder;
  private LockManager lockManager;

  // Hack to get around not being able to save any state between
  // coprocessor calls. TODO: remove after HBASE-18127 when available
  private static class BatchMutateContext {
      public final int clientVersion;
      public Collection<Pair<Mutation, byte[]>> indexUpdates = Collections.emptyList();
      public List<RowLock> rowLocks = Lists.newArrayListWithExpectedSize(QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);

      public BatchMutateContext(int clientVersion) {
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

  private static final String INDEXER_INDEX_WRITE_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.post.batch.mutate.threshold";
  private static final long INDEXER_INDEX_WRITE_SLOW_THRESHOLD_DEFAULT = 3_000;
  private static final String INDEXER_INDEX_PREPARE_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.pre.batch.mutate.threshold";
  private static final long INDEXER_INDEX_PREPARE_SLOW_THREHSOLD_DEFAULT = 3_000;
  private static final String INDEXER_PRE_WAL_RESTORE_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.pre.wal.restore.threshold";
  private static final long INDEXER_PRE_WAL_RESTORE_SLOW_THRESHOLD_DEFAULT = 3_000;
  private static final String INDEXER_POST_OPEN_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.open.threshold";
  private static final long INDEXER_POST_OPEN_SLOW_THRESHOLD_DEFAULT = 3_000;
  private static final String INDEXER_PRE_INCREMENT_SLOW_THRESHOLD_KEY = "phoenix.indexer.slow.pre.increment";
  private static final long INDEXER_PRE_INCREMENT_SLOW_THRESHOLD_DEFAULT = 3_000;

  /**
   * cache the failed updates to the various regions. Used for making the WAL recovery mechanisms
   * more robust in the face of recoverying index regions that were on the same server as the
   * primary table region
   */
  private PerRegionIndexWriteCache failedIndexEdits = new PerRegionIndexWriteCache();

  /**
   * IndexWriter for writing the recovered index edits. Separate from the main indexer since we need
   * different write/failure policies
   */
  private IndexWriter recoveryWriter;

  private MetricsIndexerSource metricSource;

  private boolean stopped;
  private boolean disabled;
  private long slowIndexWriteThreshold;
  private long slowIndexPrepareThreshold;
  private long slowPreWALRestoreThreshold;
  private long slowPostOpenThreshold;
  private long slowPreIncrementThreshold;
  private int rowLockWaitDuration;
  private String dataTableName;
  
  public static final String RecoveryFailurePolicyKeyForTesting = INDEX_RECOVERY_FAILURE_POLICY_KEY;

  public static final int INDEXING_SUPPORTED_MAJOR_VERSION = VersionUtil
            .encodeMaxPatchVersion(0, 94);
  public static final int INDEXING_SUPPORTED__MIN_MAJOR_VERSION = VersionUtil
            .encodeVersion("0.94.0");
  private static final int INDEX_WAL_COMPRESSION_MINIMUM_SUPPORTED_VERSION = VersionUtil
            .encodeVersion("0.94.9");

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
        // setup the actual index writer
        this.writer = new IndexWriter(indexWriterEnv, serverName + "-index-writer");
        
        this.rowLockWaitDuration = env.getConfiguration().getInt("hbase.rowlock.wait.duration",
                DEFAULT_ROWLOCK_WAIT_DURATION);
        this.lockManager = new LockManager();

        // Metrics impl for the Indexer -- avoiding unnecessary indirection for hadoop-1/2 compat
        this.metricSource = MetricsIndexerSourceFactory.getInstance().getIndexerSource();
        setSlowThresholds(e.getConfiguration());
        this.dataTableName = env.getRegionInfo().getTable().getNameAsString();
        try {
          // get the specified failure policy. We only ever override it in tests, but we need to do it
          // here
          Class<? extends IndexFailurePolicy> policyClass =
              env.getConfiguration().getClass(INDEX_RECOVERY_FAILURE_POLICY_KEY,
                StoreFailuresInCachePolicy.class, IndexFailurePolicy.class);
          IndexFailurePolicy policy =
              policyClass.getConstructor(PerRegionIndexWriteCache.class).newInstance(failedIndexEdits);
          LOGGER.debug("Setting up recovery writter with failure policy: " + policy.getClass());
          recoveryWriter =
              new RecoveryIndexWriter(policy, indexWriterEnv, serverName + "-recovery-writer");
        } catch (Exception ex) {
          throw new IOException("Could not instantiate recovery failure policy!", ex);
        }
      } catch (NoSuchMethodError ex) {
          disabled = true;
          LOGGER.error("Must be too early a version of HBase. Disabled coprocessor ", ex);
      }
  }

  /**
   * Extracts the slow call threshold values from the configuration.
   */
  private void setSlowThresholds(Configuration c) {
      slowIndexPrepareThreshold = c.getLong(INDEXER_INDEX_WRITE_SLOW_THRESHOLD_KEY,
          INDEXER_INDEX_WRITE_SLOW_THRESHOLD_DEFAULT);
      slowIndexWriteThreshold = c.getLong(INDEXER_INDEX_PREPARE_SLOW_THRESHOLD_KEY,
          INDEXER_INDEX_PREPARE_SLOW_THREHSOLD_DEFAULT);
      slowPreWALRestoreThreshold = c.getLong(INDEXER_PRE_WAL_RESTORE_SLOW_THRESHOLD_KEY,
          INDEXER_PRE_WAL_RESTORE_SLOW_THRESHOLD_DEFAULT);
      slowPostOpenThreshold = c.getLong(INDEXER_POST_OPEN_SLOW_THRESHOLD_KEY,
          INDEXER_POST_OPEN_SLOW_THRESHOLD_DEFAULT);
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
    this.writer.stop(msg);
    this.recoveryWriter.stop(msg);
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
              if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(getCallTooSlowMessage("preIncrementAfterRowLock",
                          duration, slowPreIncrementThreshold));
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
      long start = EnvironmentEdgeManager.currentTimeMillis();
      try {
          preBatchMutateWithExceptions(c, miniBatchOp);
          return;
      } catch (Throwable t) {
          rethrowIndexingException(t);
      } finally {
          long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
          if (duration >= slowIndexPrepareThreshold) {
              if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(getCallTooSlowMessage("preBatchMutate",
                          duration, slowIndexPrepareThreshold));
              }
              metricSource.incrementNumSlowIndexPrepareCalls(dataTableName);
          }
          metricSource.updateIndexPrepareTime(dataTableName, duration);
      }
      throw new RuntimeException(
        "Somehow didn't return an index update but also didn't propagate the failure to the client!");
  }

  private static void setTimeStamp(KeyValue kv, byte[] tsBytes) {
      int tsOffset = kv.getTimestampOffset();
      System.arraycopy(tsBytes, 0, kv.getBuffer(), tsOffset, Bytes.SIZEOF_LONG);
  }

  public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
          MiniBatchOperationInProgress<Mutation> miniBatchOp) throws Throwable {

    // Need to add cell tags to Delete Marker before we do any index processing
    // since we add tags to tables which doesn't have indexes also.
    IndexUtil.setDeleteAttributes(miniBatchOp);
      // first group all the updates for a single row into a single update to be processed
      Map<ImmutableBytesPtr, MultiMutation> mutationsMap =
              new HashMap<ImmutableBytesPtr, MultiMutation>();
          
      Durability defaultDurability = Durability.SYNC_WAL;
      if (c.getEnvironment().getRegion() != null) {
          defaultDurability = c.getEnvironment().getRegion().getTableDescriptor().getDurability();
          defaultDurability = (defaultDurability == Durability.USE_DEFAULT) ? 
                  Durability.SYNC_WAL : defaultDurability;
      }
      /*
       * Exclusively lock all rows so we get a consistent read
       * while determining the index updates
       */
      BatchMutateContext context = new BatchMutateContext(this.builder.getIndexMetaData(miniBatchOp).getClientVersion());
      setBatchMutateContext(c, context);
      Durability durability = Durability.SKIP_WAL;
      boolean copyMutations = false;
      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          if (this.builder.isAtomicOp(m)) {
              miniBatchOp.setOperationStatus(i, IGNORE);
              continue;
          }
          if (this.builder.isEnabled(m)) {
              context.rowLocks.add(lockManager.lockRow(m.getRow(), rowLockWaitDuration));
              Durability effectiveDurablity = (m.getDurability() == Durability.USE_DEFAULT) ? 
                      defaultDurability : m.getDurability();
              if (effectiveDurablity.ordinal() > durability.ordinal()) {
                  durability = effectiveDurablity;
              }
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
          return;
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
      
      Mutation firstMutation = miniBatchOp.getOperation(0);
      ReplayWrite replayWrite = this.builder.getReplayWrite(firstMutation);
      boolean resetTimeStamp = replayWrite == null;
      long now = EnvironmentEdgeManager.currentTimeMillis();
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
    
      // dump all the index updates into a single WAL. They will get combined in the end anyways, so
      // don't worry which one we get
      WALEdit edit = miniBatchOp.getWalEdit(0);
      if (edit == null) {
          edit = new WALEdit();
          miniBatchOp.setWalEdit(0, edit);
      }
  
      if (copyMutations || replayWrite != null) {
          mutations = IndexManagementUtil.flattenMutationsByTimestamp(mutations);
      }

      // get the current span, or just use a null-span to avoid a bunch of if statements
      try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
          Span current = scope.getSpan();
          if (current == null) {
              current = NullSpan.INSTANCE;
          }
          long start = EnvironmentEdgeManager.currentTimeMillis();

          // get the index updates for all elements in this batch
          Collection<Pair<Mutation, byte[]>> indexUpdates =
                  this.builder.getIndexUpdate(miniBatchOp, mutations);


          long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
          if (duration >= slowIndexPrepareThreshold) {
              if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(getCallTooSlowMessage(
                          "indexPrepare", duration, slowIndexPrepareThreshold));
              }
              metricSource.incrementNumSlowIndexPrepareCalls(dataTableName);
          }
          metricSource.updateIndexPrepareTime(dataTableName, duration);
          current.addTimelineAnnotation("Built index updates, doing preStep");
          TracingUtils.addAnnotation(current, "index update count", indexUpdates.size());
          byte[] tableName = c.getEnvironment().getRegion().getTableDescriptor().getTableName().getName();
          Iterator<Pair<Mutation, byte[]>> indexUpdatesItr = indexUpdates.iterator();
          List<Mutation> localUpdates = new ArrayList<Mutation>(indexUpdates.size());
          while (indexUpdatesItr.hasNext()) {
              Pair<Mutation, byte[]> next = indexUpdatesItr.next();
              if (Bytes.compareTo(next.getSecond(), tableName) == 0) {
                  localUpdates.add(next.getFirst());
                  indexUpdatesItr.remove();
              }
          }
          if (!localUpdates.isEmpty()) {
              miniBatchOp.addOperationsFromCP(0,
                  localUpdates.toArray(new Mutation[localUpdates.size()]));
          }
          if (!indexUpdates.isEmpty()) {
              context.indexUpdates = indexUpdates;
              // write index updates to WAL
              if (durability != Durability.SKIP_WAL) {
                  // we have all the WAL durability, so we just update the WAL entry and move on
                  for (Pair<Mutation, byte[]> entry : indexUpdates) {
                      edit.add(IndexedKeyValue.newIndexedKeyValue(entry.getSecond(),
                          entry.getFirst()));
                      }
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
  public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp, final boolean success) throws IOException {
      if (this.disabled) {
          return;
      }
      long start = EnvironmentEdgeManager.currentTimeMillis();
      BatchMutateContext context = getBatchMutateContext(c);
      if (context == null) {
          return;
      }
      try {
          for (RowLock rowLock : context.rowLocks) {
              rowLock.release();
          }
          this.builder.batchCompleted(miniBatchOp);

          if (success) { // if miniBatchOp was successfully written, write index updates
              doPost(c, context);
          }
       } finally {
           removeBatchMutateContext(c);
           long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
           if (duration >= slowIndexWriteThreshold) {
               if (LOGGER.isDebugEnabled()) {
                   LOGGER.debug(getCallTooSlowMessage("postBatchMutateIndispensably",
                           duration, slowIndexWriteThreshold));
               }
               metricSource.incrementNumSlowIndexWriteCalls(dataTableName);
           }
           metricSource.updateIndexWriteTime(dataTableName, duration);
       }
  }

  private void doPost(ObserverContext<RegionCoprocessorEnvironment> c, BatchMutateContext context) throws IOException {
      try {
        doPostWithExceptions(c,context);
        return;
      } catch (Throwable e) {
        rethrowIndexingException(e);
      }
      throw new RuntimeException(
          "Somehow didn't complete the index update, but didn't return succesfully either!");
    }

  private void doPostWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c, BatchMutateContext context)
          throws IOException {
      //short circuit, if we don't need to do any work
      if (context == null || context.indexUpdates.isEmpty()) {
          return;
      }

      // get the current span, or just use a null-span to avoid a bunch of if statements
      try (TraceScope scope = Trace.startSpan("Completing index writes")) {
          Span current = scope.getSpan();
          if (current == null) {
              current = NullSpan.INSTANCE;
          }
          long start = EnvironmentEdgeManager.currentTimeMillis();
          
          current.addTimelineAnnotation("Actually doing index update for first time");
          writer.writeAndHandleFailure(context.indexUpdates, false, context.clientVersion);

          long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
          if (duration >= slowIndexWriteThreshold) {
              if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(getCallTooSlowMessage("indexWrite",
                          duration, slowIndexWriteThreshold));
              }
              metricSource.incrementNumSlowIndexWriteCalls(dataTableName);
          }
          metricSource.updateIndexWriteTime(dataTableName, duration);
      }
  }

  /**
   * Extract the index updates from the WAL Edit
   * @param edit to search for index updates
   * @return the mutations to apply to the index tables
   */
  private Collection<Pair<Mutation, byte[]>> extractIndexUpdate(WALEdit edit) {
    // Avoid multiple internal array resizings. Initial size of 64, unless we have fewer cells in the edit
    int initialSize = Math.min(edit.size(), 64);
    Collection<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>(initialSize);
    for (Cell kv : edit.getCells()) {
      if (kv instanceof IndexedKeyValue) {
        IndexedKeyValue ikv = (IndexedKeyValue) kv;
        indexUpdates.add(new Pair<Mutation, byte[]>(ikv.getMutation(), ikv.getIndexTable()));
      }
    }

    return indexUpdates;
  }

  @Override
  public void postOpen(final ObserverContext<RegionCoprocessorEnvironment> c) {
    Multimap<HTableInterfaceReference, Mutation> updates = failedIndexEdits.getEdits(c.getEnvironment().getRegion());
    
    if (this.disabled) {
        return;
    }

    long start = EnvironmentEdgeManager.currentTimeMillis();
    try {
        //if we have no pending edits to complete, then we are done
        if (updates == null || updates.size() == 0) {
          return;
        }

        LOGGER.info("Found some outstanding index updates that didn't succeed during"
                + " WAL replay - attempting to replay now.");

        // do the usual writer stuff, killing the server again, if we can't manage to make the index
        // writes succeed again
        try {
            writer.writeAndHandleFailure(updates, true, ScanUtil.UNKNOWN_CLIENT_VERSION);
        } catch (IOException e) {
                LOGGER.error("During WAL replay of outstanding index updates, "
                        + "Exception is thrown instead of killing server during index writing", e);
        }
    } finally {
         long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
         if (duration >= slowPostOpenThreshold) {
             if (LOGGER.isDebugEnabled()) {
                 LOGGER.debug(getCallTooSlowMessage("postOpen", duration, slowPostOpenThreshold));
             }
             metricSource.incrementNumSlowPostOpenCalls(dataTableName);
         }
         metricSource.updatePostOpenTime(dataTableName, duration);
    }
  }

  @Override
    public void preWALRestore(
            org.apache.hadoop.hbase.coprocessor.ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
            org.apache.hadoop.hbase.client.RegionInfo info, org.apache.hadoop.hbase.wal.WALKey logKey, WALEdit logEdit)
            throws IOException {
  
      if (this.disabled) {
          return;
      }

    // TODO check the regions in transition. If the server on which the region lives is this one,
    // then we should rety that write later in postOpen.
    // we might be able to get even smarter here and pre-split the edits that are server-local
    // into their own recovered.edits file. This then lets us do a straightforward recovery of each
    // region (and more efficiently as we aren't writing quite as hectically from this one place).

      long start = EnvironmentEdgeManager.currentTimeMillis();
      try {
          /*
           * Basically, we let the index regions recover for a little while long before retrying in the
           * hopes they come up before the primary table finishes.
           */
          Collection<Pair<Mutation, byte[]>> indexUpdates = extractIndexUpdate(logEdit);
          recoveryWriter.writeAndHandleFailure(indexUpdates, true, ScanUtil.UNKNOWN_CLIENT_VERSION);
      } finally {
          long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
          if (duration >= slowPreWALRestoreThreshold) {
              if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(getCallTooSlowMessage("preWALRestore",
                          duration, slowPreWALRestoreThreshold));
              }
              metricSource.incrementNumSlowPreWALRestoreCalls(dataTableName);
          }
          metricSource.updatePreWALRestoreTime(dataTableName, duration);
      }
  }


  /**
   * Exposed for testing!
   * @return the currently instantiated index builder
   */
  public IndexBuilder getBuilderForTesting() {
    return this.builder.getBuilderForTesting();
  }

    /**
     * Validate that the version and configuration parameters are supported
     * @param hbaseVersion current version of HBase on which <tt>this</tt> coprocessor is installed
     * @param conf configuration to check for allowed parameters (e.g. WAL Compression only {@code if >=
     *            0.94.9) }
     * @return <tt>null</tt> if the version is supported, the error message to display otherwise
     */
    public static String validateVersion(String hbaseVersion, Configuration conf) {
        int encodedVersion = VersionUtil.encodeVersion(hbaseVersion);
        // above 0.94 everything should be supported
        if (encodedVersion > INDEXING_SUPPORTED_MAJOR_VERSION) {
            return null;
        }
        // check to see if its at least 0.94
        if (encodedVersion < INDEXING_SUPPORTED__MIN_MAJOR_VERSION) {
            return "Indexing not supported for versions older than 0.94.X";
        }
        // if less than 0.94.9, we need to check if WAL Compression is enabled
        if (encodedVersion < INDEX_WAL_COMPRESSION_MINIMUM_SUPPORTED_VERSION) {
            if (conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false)) {
                return "Indexing not supported with WAL Compression for versions of HBase older than 0.94.9 - found version:"
                        + hbaseVersion;
            }
        }
        return null;
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
     descBuilder.addCoprocessor(Indexer.class.getName(), null, priority, properties);
  }
}

