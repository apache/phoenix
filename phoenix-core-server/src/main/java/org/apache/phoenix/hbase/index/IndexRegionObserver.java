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
package org.apache.phoenix.hbase.index;

import static org.apache.hadoop.hbase.HConstants.OperationStatusCode.SUCCESS;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.applyNew;
import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.removeColumn;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.UPSERT_CF;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.UPSERT_STATUS_CQ;
import static org.apache.phoenix.hbase.index.util.IndexManagementUtil.rethrowIndexingException;
import static org.apache.phoenix.index.PhoenixIndexBuilderHelper.ATOMIC_OP_ATTRIB;
import static org.apache.phoenix.index.PhoenixIndexBuilderHelper.RETURN_RESULT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.query.QueryServices.SYNCHRONOUS_REPLICATION_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_SYNCHRONOUS_REPLICATION_ENABLED;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

import com.google.protobuf.ByteString;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.generated.IndexMutationsProtos;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.exception.StaleClusterRoleRecordException;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.CaseExpression;
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
import org.apache.phoenix.hbase.index.covered.data.CachedLocalTable;
import org.apache.phoenix.hbase.index.covered.data.LocalHBaseState;
import org.apache.phoenix.hbase.index.covered.data.PreImageLocalTable;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.metrics.MetricsHaBypassSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.LazyParallelWriterIndexCommitter;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexBuilderHelper;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.index.PhoenixIndexMetaDataBuilder;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.replication.MutationCellGrouper;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.apache.phoenix.replication.SystemCatalogWALEntryFilter;
import org.apache.phoenix.schema.CompiledConditionalTTLExpression;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TTLExpressionFactory;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.MutationUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerIndexUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make. We don't need to implement
 * {@link #postPut(ObserverContext, Put, WALEdit, Durability)} and
 * {@link #postDelete(ObserverContext, Delete, WALEdit, Durability)} hooks because Phoenix always
 * does batch mutations.
 * <p>
 */
@CoreCoprocessor
public class IndexRegionObserver implements RegionCoprocessor, RegionObserver {

  private static final Logger LOG = LoggerFactory.getLogger(IndexRegionObserver.class);
  private static final OperationStatus IGNORE = new OperationStatus(SUCCESS);
  private static final OperationStatus NOWRITE = new OperationStatus(SUCCESS);
  public static final String PHOENIX_APPEND_METADATA_TO_WAL = "phoenix.append.metadata.to.wal";
  public static final boolean DEFAULT_PHOENIX_APPEND_METADATA_TO_WAL = false;
  // Mutation attribute to ignore the mutation for replication
  public static final String IGNORE_REPLICATION_ATTRIB = "_IGNORE_REPLICATION";
  private static final byte[] IGNORE_REPLICATION_ATTRIB_VAL = new byte[] { 0 };
  public static final String PHOENIX_INDEX_CDC_CONSUMER_ENABLED =
    "phoenix.index.cdc.consumer.enabled";
  public static final boolean DEFAULT_PHOENIX_INDEX_CDC_CONSUMER_ENABLED = true;
  public static final String PHOENIX_INDEX_CDC_MUTATIONS_COMPRESS_ENABLED =
    "phoenix.index.cdc.mutations.compress.enabled";
  public static final boolean DEFAULT_PHOENIX_INDEX_CDC_MUTATIONS_COMPRESS_ENABLED = false;
  /**
   * Controls which approach is used for implementing eventually consistent global secondary indexes
   * via the {@link IndexCDCConsumer}.
   * <p>
   * <b>Approach 1: Serialized mutations (value = true)</b>
   * </p>
   * <p>
   * During {@code preBatchMutate}, {@link IndexRegionObserver} generates index mutations for each
   * data table mutation and serializes them into a Protobuf {@code IndexMutations} message. This
   * serialized payload is written as a column value in the CDC index table row alongside the CDC
   * event. The {@link IndexCDCConsumer} later reads these pre-computed mutations from the CDC
   * index, deserializes them, and applies them directly to the index table(s). In this approach,
   * the consumer does not need to understand index structure or re-derive mutations — it simply
   * replays what was already computed on the write path. The trade-off is increased CDC index row
   * size due to the serialized mutation payload, and additional write IO on the CDC index table.
   * </p>
   * <p>
   * <b>Approach 2: Generated mutations from data row states (default, value = false)</b>
   * </p>
   * <p>
   * During {@code preBatchMutate}, {@link IndexRegionObserver} writes only a lightweight CDC index
   * entry without serialized index mutations. Instead, the CDC event is created with the
   * {@code DATA_ROW_STATE} scope. When the {@link IndexCDCConsumer} processes these events, it
   * reads the CDC index rows which trigger a server-side scan of the data table (via
   * {@code CDCGlobalIndexRegionScanner}) to reconstruct the before-image
   * ({@code currentDataRowState}) and after-image ({@code nextDataRowState}) of the data row at the
   * change timestamp. These raw row states are returned as a Protobuf {@code DataRowStates}
   * message. The consumer then feeds these states into {@code generateIndexMutationsForRow()} — the
   * same core utility used by {@link IndexRegionObserver#prepareIndexMutations} on the write path —
   * to derive index mutations at consume time. This approach keeps CDC index rows small, avoids
   * additional write IO, and generates mutations based on the current index definition, but
   * requires an additional data table read per CDC event and is sensitive to data visibility
   * timing. Make sure max lookback age is long enough to retain before and after images of the row.
   * </p>
   * <p>
   * <b>When to use which approach:</b>
   * </p>
   * <ul>
   * <li>Use <b>Approach 2</b> (serialize = false, default) to minimize write IO: no serialized
   * mutations are written to the CDC index, keeping CDC index rows small and write latency uniform.
   * The trade-off is higher read IO at consume time — the consumer performs an additional data
   * table point-lookup with a raw scan per CDC event to reconstruct row states.</li>
   * <li>Use <b>Approach 1</b> (serialize = true) to minimize read IO: the consumer reads
   * pre-computed mutations from the CDC index and applies them directly, with no data table scan
   * required at consume time. The trade-off is higher write IO — serialized index mutations are
   * written alongside each CDC index entry, increasing CDC index row size and write-path latency.
   * Although CDC index is expected to have TTL same as the data table max lookback age.</li>
   * </ul>
   */
  public static final String PHOENIX_INDEX_CDC_MUTATION_SERIALIZE =
    "phoenix.index.cdc.mutation.serialize";
  public static final boolean DEFAULT_PHOENIX_INDEX_CDC_MUTATION_SERIALIZE = false;
  // Generic marker attribute set on every mutation produced by the standby reader from a
  // replication-log record. Value is opaque (presence is the signal). Detected in
  // preBatchMutateWithExceptions to set context.isReplication, which gates the primary-side clock
  // work (getBatchTimestamp/setTimestamps) off so the cell timestamps shipped from the active
  // cluster are preserved. Never set on primary-side mutations.
  public static final String REPLICATED_MUTATION = "_ReplicatedMutation";
  // Per-row mutation attribute that the standby reader synthesizes from the pre-image cell and
  // attaches to each reconstructed mutation when its row had a pre-image entry. Value is the
  // PB-encoded primary-side Put (or empty bytes when the primary observed an empty row at lock
  // time). IRO on the standby consumes this to derive each (row, ts) group's data-row state and
  // write index updates directly, instead of scanning the data table — that scan is unsafe under
  // out-of-order replay. Absent when the row had no pre-image (e.g. local-index-only or pure-data
  // tables on the active).
  public static final String PRE_IMAGE = "_PhoenixPreImage";
  // Qualifier for the per-row pre-image cell injected into the replication cell stream and the
  // WAL edit at PRE phase. Cells with this (METAFAMILY, qualifier) pair carry a serialized PB Put
  // representing the row's state on the primary before the current batch was applied. The standby
  // reader peels these cells off and attaches the bytes as {@link #PRE_IMAGE} on the reconstructed
  // mutation. Namespaced with a "PHOENIX::" prefix to mirror HBase's own METAFAMILY qualifiers
  // (HBASE::COMPACTION, HBASE::FLUSH, ...) so it stays clear of the reserved HBASE:: space and of
  // any future qualifier-prefix enforcement (see HBASE-8457). The METAFAMILY family is what makes
  // HBase skip this cell on recovered-edits replay (HRegion.replayRecoveredEdits keys the skip on
  // family, not qualifier), which is intended: the pre-image must never land in a data store.
  public static final byte[] PRE_IMAGE_WAL_QUALIFIER = Bytes.toBytes("PHOENIX::PRE_IMAGE");

  /**
   * Class to represent pending data table rows
   */
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

    public BatchMutateContext addAndGetPrevCtx(BatchMutateContext context) {
      synchronized (this) {
        if (usable) {
          BatchMutateContext previousContext = lastContext;
          count++;
          lastContext = context;
          return previousContext;
        }
      }
      return null;
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

  }

  private static boolean ignoreIndexRebuildForTesting = false;
  private static boolean failPreIndexUpdatesForTesting = false;
  private static boolean failPostIndexUpdatesForTesting = false;
  private static boolean failDataTableUpdatesForTesting = false;
  private static boolean ignoreWritingDeleteColumnsToIndex = false;
  private static boolean ignoreSyncReplicationForTesting = false;

  public static void setIgnoreIndexRebuildForTesting(boolean ignore) {
    ignoreIndexRebuildForTesting = ignore;
  }

  public static void setFailPreIndexUpdatesForTesting(boolean fail) {
    failPreIndexUpdatesForTesting = fail;
  }

  public static void setFailPostIndexUpdatesForTesting(boolean fail) {
    failPostIndexUpdatesForTesting = fail;
  }

  public static void setFailDataTableUpdatesForTesting(boolean fail) {
    failDataTableUpdatesForTesting = fail;
  }

  public static void setIgnoreWritingDeleteColumnsToIndex(boolean ignore) {
    ignoreWritingDeleteColumnsToIndex = ignore;
  }

  public static void setIgnoreSyncReplicationForTesting(boolean ignore) {
    ignoreSyncReplicationForTesting = ignore;
  }

  public enum BatchMutatePhase {
    INIT,
    PRE,
    POST,
    FAILED
  }

  /**
   * Composite key for {@link BatchMutateContext#cdcPreMutationsBytes} and
   * {@link BatchMutateContext#cdcPostMutationsBytes} and for the standby's per-(row, ts) grouping.
   * The active path always has ts == batchTimestamp, so the {@code (row, ts)} key behaves like
   * {@code (row)} on the active. The standby can have multiple entries per row when records from
   * two active-side batches for the same row coalesce in one mini-batch.
   */
  public static final class RowTsKey {
    private final ImmutableBytesPtr row;
    private final long ts;

    public RowTsKey(ImmutableBytesPtr row, long ts) {
      this.row = row;
      this.ts = ts;
    }

    public ImmutableBytesPtr getRow() {
      return row;
    }

    public long getTs() {
      return ts;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof RowTsKey)) {
        return false;
      }
      RowTsKey other = (RowTsKey) o;
      return ts == other.ts && row.equals(other.row);
    }

    @Override
    public int hashCode() {
      return 31 * row.hashCode() + Long.hashCode(ts);
    }

    @Override
    public String toString() {
      return "RowTsKey [row=" + Bytes.toStringBinary(row.copyBytesIfNecessary()) + ", ts=" + ts
        + "]";
    }
  }

  // Hack to get around not being able to save any state between
  // coprocessor calls. TODO: remove after HBASE-18127 when available

  /*
   * The concurrent batch of mutations is a set such that every pair of batches in this set has at
   * least one common row. Since a BatchMutateContext object of a batch is modified only after the
   * row locks for all the rows that are mutated by this batch are acquired, there can be only one
   * thread can acquire the locks for its batch and safely access all the batch contexts in the set
   * of concurrent batches. Because of this, we do not read atomic variables or additional locks to
   * serialize the access to the BatchMutateContext objects.
   */
  public static class BatchMutateContext {
    private volatile BatchMutatePhase currentPhase = BatchMutatePhase.INIT;
    // The max of reference counts on the pending rows of this batch at the time this
    // batch arrives.
    private int maxPendingRowCount = 0;
    private final int clientVersion;
    // The collection of index mutations that will be applied before the data table mutations.
    // The empty column (i.e. the verified column) will have the value false ("unverified")
    // on these mutations.
    private ListMultimap<HTableInterfaceReference, Mutation> preIndexUpdates;
    // The collection of index mutations that will be applied after the data table mutations.
    // The empty column (i.e. the verified column) will have the value true ("verified")
    // on the put mutations.
    private ListMultimap<HTableInterfaceReference, Mutation> postIndexUpdates;
    // The collection of candidate index mutations that will be applied after the data table
    // mutations.
    private ListMultimap<HTableInterfaceReference, Pair<Mutation, byte[]>> indexUpdates;
    // Map of (data table row key, group ts) to IndexMutations bytes containing pre-index mutations
    // for eventually consistent indexes (UNVERIFIED Puts only, no Deletes). Keyed by (row, ts) so
    // multiple per-row entries from the standby's per-(row, ts) grouping don't collide. On the
    // active path each row produces exactly one entry (ts == batchTimestamp), so lookup is
    // unchanged.
    private Map<RowTsKey, byte[]> cdcPreMutationsBytes;
    // Map of (data table row key, group ts) to IndexMutations bytes containing post-index
    // mutations for eventually consistent indexes (VERIFIED Puts for covered, no Put mutations
    // for uncovered, and Deletes if needed).
    private Map<RowTsKey, byte[]> cdcPostMutationsBytes;
    private List<RowLock> rowLocks =
      Lists.newArrayListWithExpectedSize(QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
    // TreeSet to improve locking efficiency and avoid deadlock (PHOENIX-6871 and HBASE-17924)
    private Set<ImmutableBytesPtr> rowsToLock = new TreeSet<>();
    // The current and next states of the data rows corresponding to the pending mutations
    private HashMap<ImmutableBytesPtr, Pair<Put, Put>> dataRowStates;
    // The previous concurrent batch contexts
    private HashMap<ImmutableBytesPtr, BatchMutateContext> lastConcurrentBatchContext = null;
    // The latches of the threads waiting for this batch to complete
    private List<CountDownLatch> waitList = null;
    private Map<ImmutableBytesPtr, MultiMutation> multiMutationMap;
    // store current cells into a map where the key is ColumnReference of the column family and
    // column qualifier, and value is a pair of cell and a boolean. The value of the boolean
    // will be true if the expression is CaseExpression and Else-clause is evaluated to be
    // true, will be null if there is no expression on this column, otherwise false
    // This is only initialized for single row atomic mutation.
    private Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap;
    // store old row cells into a map for OLD_ROW return result. This preserves the original
    // state of the row before any conditional updates are applied.
    private Map<ColumnReference, Pair<Cell, Boolean>> oldRowColumnCellExprMap;
    // list containing the original mutations from the MiniBatchOperationInProgress. Contains
    // any annotations we were sent by the client, and can be used in hooks that don't get
    // passed MiniBatchOperationInProgress, like preWALAppend()
    private List<Mutation> originalMutations;
    private boolean hasAtomic;
    private boolean hasRowDelete;
    // Has uncovered global indexes which are not CDC Indexes
    private boolean hasUncoveredIndex;
    private boolean hasGlobalIndex; // Covered global index
    private boolean hasLocalIndex;
    private boolean hasTransform;
    private boolean returnResult;
    private boolean returnOldRow;
    private boolean hasConditionalTTL; // table has Conditional TTL
    private boolean immutableRows;
    // True when this batch was produced by the standby reader from a replication-log record (i.e.
    // every mutation carries the {@link IndexRegionObserver#REPLICATED_MUTATION} marker). Batch-
    // uniform by construction: the standby reader stamps every reconstructed mutation, so checking
    // the first one is sufficient. The standby uses this to skip the data-table scan in the PRE
    // phase (pre-image cells are carried as the per-row {@link IndexRegionObserver#PRE_IMAGE}
    // attribute when the table has a global/uncovered/transform index — same schema on both
    // clusters, so when we're inside that branch on the standby we always have pre-images).
    private boolean isReplication;
    // HAGroup associated with the batch
    private Optional<ReplicationLogGroup> logGroup = Optional.empty();
    // Per-(row, ts) groups folded from this replicated batch, computed once and shared by the
    // global-index path (prepareReplicatedIndexMutations) and the local-index path. Null until
    // first built; only populated on the standby replay path (isReplication).
    private List<ReplicatedRowGroup> replicatedRowGroups;

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

    /** True if the batch's table carries any index the standby must regenerate. */
    public boolean hasIndex() {
      return hasGlobalIndex || hasUncoveredIndex || hasLocalIndex || hasTransform;
    }
  }

  private ThreadLocal<BatchMutateContext> batchMutateContext =
    new ThreadLocal<BatchMutateContext>();

  /**
   * Configuration key for if the indexer should check the version of HBase is running. Generally,
   * you only want to ignore this for testing or for custom versions of HBase.
   */
  public static final String CHECK_VERSION_CONF_KEY = "com.saleforce.hbase.index.checkversion";

  public static final String INDEX_LAZY_POST_BATCH_WRITE =
    "org.apache.hadoop.hbase.index.lazy.post_batch.write";
  private static final boolean INDEX_LAZY_POST_BATCH_WRITE_DEFAULT = false;

  private static final String INDEXER_INDEX_WRITE_SLOW_THRESHOLD_KEY =
    "phoenix.indexer.slow.post.batch.mutate.threshold";
  private static final long INDEXER_INDEX_WRITE_SLOW_THRESHOLD_DEFAULT = 3_000;
  private static final String INDEXER_PRE_INCREMENT_SLOW_THRESHOLD_KEY =
    "phoenix.indexer.slow.pre.increment";
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
  private boolean indexCDCConsumerEnabled = DEFAULT_PHOENIX_INDEX_CDC_CONSUMER_ENABLED;
  private boolean compressCDCMutations = DEFAULT_PHOENIX_INDEX_CDC_MUTATIONS_COMPRESS_ENABLED;
  private boolean serializeCDCMutations = DEFAULT_PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;
  private boolean isNamespaceEnabled = false;
  private boolean useBloomFilter = false;
  private long lastTimestamp = 0;
  private List<Set<ImmutableBytesPtr>> batchesWithLastTimestamp = new ArrayList<>();
  private IndexCDCConsumer indexCDCConsumer;
  private static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;
  private static final int DEFAULT_CONCURRENT_MUTATION_WAIT_DURATION_IN_MS = 100;
  private byte[] encodedRegionName;
  private boolean shouldReplicate;
  private Abortable abortable;

  // Don't replicate the mutation if this attribute is set
  private static final Predicate<Mutation> IGNORE_REPLICATION =
    mutation -> mutation.getAttribute(IGNORE_REPLICATION_ATTRIB) != null;

  // Don't replicate the mutation for syscat/child link if the tenantid is not
  // leading in the row key
  private static final Predicate<Mutation> NOT_TENANT_ID_ROW_KEY_PREFIX =
    mutation -> !SystemCatalogWALEntryFilter.isTenantIdLeadingInKey(mutation.getRow(), 0);

  // Don't replicate the mutation for child link if child is not a tenant view
  private static final Predicate<Mutation> NOT_CHILD_LINK_TENANT_VIEW = mutation -> {
    boolean isChildLinkToTenantView = false;
    for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        if (SystemCatalogWALEntryFilter.isCellChildLinkToTenantView(cell)) {
          isChildLinkToTenantView = true;
          break;
        }
      }
    }
    return !isChildLinkToTenantView;
  };

  /**
   * If the replication filter evaluates to true, the mutation is ignored from replication
   */
  private static Predicate<Mutation> getSynchronousReplicationFilter(byte[] tableName) {
    Predicate<Mutation> filter = IGNORE_REPLICATION;
    if (SchemaUtil.isMetaTable(tableName)) {
      filter = IGNORE_REPLICATION.or(NOT_TENANT_ID_ROW_KEY_PREFIX);
    } else if (SchemaUtil.isChildLinkTable(tableName)) {
      filter = IGNORE_REPLICATION.or(NOT_TENANT_ID_ROW_KEY_PREFIX.and(NOT_CHILD_LINK_TENANT_VIEW));
    }
    return filter;
  }

  private Predicate<Mutation> ignoreReplicationFilter;

  public IndexRegionObserver() {
  }

  @VisibleForTesting
  IndexRegionObserver(String dataTableName) {
    this.dataTableName = dataTableName;
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    try {
      final RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      encodedRegionName = env.getRegion().getRegionInfo().getEncodedNameAsBytes();
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
      DelegateRegionCoprocessorEnvironment indexWriterEnv =
        new DelegateRegionCoprocessorEnvironment(env, ConnectionType.INDEX_WRITER_CONNECTION);
      // setup the actual index preWriter
      this.preWriter = new IndexWriter(indexWriterEnv, serverName + "-index-preWriter", false);
      if (
        env.getConfiguration().getBoolean(INDEX_LAZY_POST_BATCH_WRITE,
          INDEX_LAZY_POST_BATCH_WRITE_DEFAULT)
      ) {
        this.postWriter = new IndexWriter(indexWriterEnv, new LazyParallelWriterIndexCommitter(),
          serverName + "-index-postWriter", false);
      } else {
        this.postWriter = this.preWriter;
      }

      this.rowLockWaitDuration =
        env.getConfiguration().getInt("hbase.rowlock.wait.duration", DEFAULT_ROWLOCK_WAIT_DURATION);
      this.lockManager = new LockManager();
      this.concurrentMutationWaitDuration =
        env.getConfiguration().getInt("phoenix.index.concurrent.wait.duration.ms",
          DEFAULT_CONCURRENT_MUTATION_WAIT_DURATION_IN_MS);
      // Metrics impl for the Indexer -- avoiding unnecessary indirection for hadoop-1/2 compat
      this.metricSource = MetricsIndexerSourceFactory.getInstance().getIndexerSource();
      setSlowThresholds(e.getConfiguration());
      this.dataTableName = env.getRegionInfo().getTable().getNameAsString();
      this.shouldWALAppend = env.getConfiguration().getBoolean(PHOENIX_APPEND_METADATA_TO_WAL,
        DEFAULT_PHOENIX_APPEND_METADATA_TO_WAL);
      this.indexCDCConsumerEnabled = env.getConfiguration()
        .getBoolean(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, DEFAULT_PHOENIX_INDEX_CDC_CONSUMER_ENABLED);
      this.compressCDCMutations =
        env.getConfiguration().getBoolean(PHOENIX_INDEX_CDC_MUTATIONS_COMPRESS_ENABLED,
          DEFAULT_PHOENIX_INDEX_CDC_MUTATIONS_COMPRESS_ENABLED);
      this.serializeCDCMutations = env.getConfiguration().getBoolean(
        PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, DEFAULT_PHOENIX_INDEX_CDC_MUTATION_SERIALIZE);
      this.isNamespaceEnabled =
        SchemaUtil.isNamespaceMappingEnabled(PTableType.INDEX, env.getConfiguration());
      TableDescriptor tableDescriptor = env.getRegion().getTableDescriptor();
      BloomType bloomFilterType = tableDescriptor.getColumnFamilies()[0].getBloomFilterType();
      // when the table descriptor changes, the coproc is reloaded
      this.useBloomFilter = bloomFilterType == BloomType.ROW;
      byte[] tableName = env.getRegionInfo().getTable().getName();
      this.shouldReplicate = env.getConfiguration().getBoolean(SYNCHRONOUS_REPLICATION_ENABLED,
        DEFAULT_SYNCHRONOUS_REPLICATION_ENABLED);
      if (this.shouldReplicate) {
        // replication feature is enabled, check if it is enabled for the table
        this.shouldReplicate = SchemaUtil.shouldReplicateTable(tableName);
      }
      if (this.shouldReplicate) {
        this.ignoreReplicationFilter = getSynchronousReplicationFilter(tableName);
      }
      // @CoreCoprocessor guarantees HasRegionServerServices, but guard for testability
      if (e instanceof HasRegionServerServices) {
        this.abortable = ((HasRegionServerServices) e).getRegionServerServices();
      }
      if (
        this.indexCDCConsumerEnabled && !this.dataTableName.startsWith("SYSTEM.")
          && !this.dataTableName.startsWith("SYSTEM:")
      ) {
        this.indexCDCConsumer =
          new IndexCDCConsumer(env, this.dataTableName, serverName, this.serializeCDCMutations);
        this.indexCDCConsumer.start();
      }
    } catch (NoSuchMethodError ex) {
      disabled = true;
      LOG.error("Must be too early a version of HBase. Disabled coprocessor ", ex);
    }
  }

  /**
   * Extracts the slow call threshold values from the configuration.
   */
  private void setSlowThresholds(Configuration c) {
    slowIndexPrepareThreshold =
      c.getLong(INDEXER_INDEX_WRITE_SLOW_THRESHOLD_KEY, INDEXER_INDEX_WRITE_SLOW_THRESHOLD_DEFAULT);
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
    String msg = "IndexRegionObserver is being stopped";
    this.builder.stop(msg);
    this.preWriter.stop(msg);
    this.postWriter.stop(msg);
    if (this.indexCDCConsumer != null) {
      this.indexCDCConsumer.stop();
    }
  }

  /**
   * We use an Increment to serialize the ON DUPLICATE KEY clause so that the HBase plumbing sets up
   * the necessary locks and mvcc to allow an atomic update. The Increment is not a real increment,
   * though, it's really more of a Put. We translate the Increment into a list of mutations, at most
   * a single Put and Delete that are the changes upon executing the list of ON DUPLICATE KEY
   * clauses for this row.
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
      throw ClientUtil.createIOException("Unable to process ON DUPLICATE IGNORE for "
        + e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString() + "("
        + Bytes.toStringBinary(inc.getRow()) + ")", t);
    } finally {
      long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
      if (duration >= slowIndexPrepareThreshold) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            getCallTooSlowMessage("preIncrementAfterRowLock", duration, slowPreIncrementThreshold));
        }
        metricSource.incrementSlowDuplicateKeyCheckCalls(dataTableName);
      }
      metricSource.updateDuplicateKeyCheckTime(dataTableName, duration);
    }
  }

  /*
   * Also checks for mutationBlockEnabled if CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED is enabled.
   */
  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (this.disabled) {
      return;
    }
    try {
      final Configuration conf = c.getEnvironment().getConfiguration();
      final HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(conf);
      if (haGroupStoreManager == null) {
        throw new IOException(
          "HAGroupStoreManager is null " + "for current cluster, check configuration");
      }
      // Extract HAGroupName from the mutations
      Optional<ReplicationLogGroup> logGroup = getHAGroupFromBatch(c.getEnvironment(), miniBatchOp);

      // Path-coverage counter: increments whenever a mutation batch reaches preBatchMutate
      // without a resolvable HA group attribute, so the cluster-role-based mutation-block gate
      // has no haGroupName to evaluate against and is skipped. This counts the code path being
      // short-circuited — it does NOT imply any safety property was breached (when the block
      // feature is disabled or no block window is active, there is no property to breach).
      // Tracked globally rather than per-table so operators can compare baseline vs.
      // post-deploy delta to spot new write paths that forgot to attach _HAGroupName.
      // Intentionally scoped to !logGroup.isPresent() regardless of dataTableName —
      // system-HA-group writes WITH a haGroup are an intended gate exemption (state writes
      // must proceed during a block window) and are not counted here.
      if (!logGroup.isPresent()) {
        try {
          MetricsHaBypassSourceFactory.getInstance().incrementBypassedMutationBlockCount();
        } catch (Throwable t) {
          LOG.warn("Failed to increment bypassed mutation block count metric; continuing", t);
        }
      }

      // We don't want to check for mutation blocking for the system ha group table
      if (!dataTableName.equals(SYSTEM_HA_GROUP_NAME) && logGroup.isPresent()) {
        // Check if mutation is blocked for the HA Group
        String haGroupName = logGroup.get().getHAGroupName();
        // TODO: Below approach might be slow need to figure out faster way,
        // slower part is getting haGroupStoreClient We can also cache
        // roleRecord (I tried it and still it's slow due to haGroupStoreClient
        // initialization) and caching will give us old result in case one cluster
        // is unreachable instead of UNKNOWN.

        boolean isHAGroupOnClientStale = haGroupStoreManager.isHAGroupOnClientStale(haGroupName);
        if (StringUtils.isNotBlank(haGroupName) && isHAGroupOnClientStale) {
          throw new StaleClusterRoleRecordException(String
            .format("HAGroupStoreRecord is stale for haGroup %s on " + "client", haGroupName));
        }

        // Check if mutation's haGroup is stale
        if (
          StringUtils.isNotBlank(haGroupName) && haGroupStoreManager.isMutationBlocked(haGroupName)
        ) {
          throw new MutationBlockedIOException(
            "Blocking Mutation as Some CRRs " + "are in ACTIVE_TO_STANDBY state and "
              + "CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED is true");
        }
      }
      preBatchMutateWithExceptions(c, miniBatchOp, logGroup);
      return;
    } catch (Throwable t) {
      rethrowIndexingException(t);
    }
    throw new RuntimeException(
      "Somehow didn't return an index update but also didn't propagate the failure to the client!");
  }

  /**
   * Get the HA group associated with the batch. We assume that all the mutations in the batch will
   * have the same HA group.
   * @return HA group if present or empty if missing
   */
  private Optional<ReplicationLogGroup> getHAGroupFromBatch(RegionCoprocessorEnvironment env,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (miniBatchOp.size() > 0) {
      Mutation m = miniBatchOp.getOperation(0);
      byte[] haGroupName = m.getAttribute(BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB);
      if (haGroupName != null) {
        ReplicationLogGroup logGroup = ReplicationLogGroup.get(env.getConfiguration(),
          env.getServerName(), Bytes.toString(haGroupName), abortable);
        return Optional.of(logGroup);
      }
    }
    return Optional.empty();
  }

  /**
   * Get the HA group associated with the WAL key. A batch of mutations is recorded in a single WAL
   * edit.
   * @return HA group if present or empty if missing
   */
  private Optional<ReplicationLogGroup> getHAGroupFromWALKey(RegionCoprocessorEnvironment env,
    Map<String, byte[]> walKeyAttrs) throws IOException {
    byte[] haGroupName = walKeyAttrs.get(BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB);
    if (haGroupName != null) {
      ReplicationLogGroup logGroup = ReplicationLogGroup.get(env.getConfiguration(),
        env.getServerName(), Bytes.toString(haGroupName), abortable);
      return Optional.of(logGroup);
    }
    return Optional.empty();
  }

  @Override
  public void preWALRestore(
    org.apache.hadoop.hbase.coprocessor.ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    org.apache.hadoop.hbase.client.RegionInfo info, org.apache.hadoop.hbase.wal.WALKey logKey,
    WALEdit logEdit) throws IOException {
    if (this.disabled) {
      return;
    }
    if (!shouldReplicate) {
      return;
    }
    Map<String, byte[]> walKeyAttrs = getAttributeValuesFromWALKey(logKey);
    Optional<ReplicationLogGroup> logGroup =
      getHAGroupFromWALKey(ctx.getEnvironment(), walKeyAttrs);
    if (!logGroup.isPresent()) {
      return;
    }
    long start = EnvironmentEdgeManager.currentTimeMillis();
    try {
      replicateEditOnWALRestore(logGroup.get(), logKey, walKeyAttrs, logEdit);
    } finally {
      long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
      metricSource.updatePreWALRestoreTime(dataTableName, duration);
    }
  }

  /**
   * Forward the WAL edit's cell stream to the replication log as a single batch, filtered through
   * {@link #isReplicableCell} — the same predicate the synchronous {@link #replicateMutations} path
   * uses. The persisted WAL edit carries the local-index (L#) cells HBase merged into the data
   * mutation's family map; those must be dropped (the standby regenerates its own local index),
   * while the METAFAMILY pre-image cells injected at PRE phase are kept. The standby reader peels
   * the pre-image cells and reconstructs Put/Delete mutations on the way out.
   * @param logGroup HA Group
   * @param logKey   WAL log key
   * @param logEdit  WAL edit record
   */
  private void replicateEditOnWALRestore(ReplicationLogGroup logGroup, WALKey logKey,
    Map<String, byte[]> walKeyAttrs, WALEdit logEdit) throws IOException {
    String tableName = logKey.getTableName().getNameAsString();
    List<Cell> cells = logEdit.getCells();
    if (cells == null || cells.isEmpty()) {
      return;
    }
    // The persisted WAL edit carries the local-index (L#) cells HBase merged into the data
    // mutation's family map, plus our METAFAMILY pre-image cells and possibly foreign coprocessor
    // markers. Filter through the same predicate the synchronous path uses so both ship exactly the
    // data cells and our pre-image, and never a local-index cell (the standby regenerates its own).
    List<Cell> replicable =
      cells.stream().filter(IndexRegionObserver::isReplicableCell).collect(Collectors.toList());
    if (replicable.isEmpty()) {
      return;
    }
    Map<String, byte[]> replicationAttrs = new HashMap<>();
    for (String attrKey : ReplicationLogGroup.REPLICATION_ATTR_KEYS) {
      byte[] val = walKeyAttrs.get(attrKey);
      if (val != null) {
        replicationAttrs.put(attrKey, val);
      }
    }
    // INDEX_UUID rides the WAL key only when appendReplicationAttributesToWALKey stamped it (i.e.
    // the batch's table was indexed, gated on hasIndex()); copy it through verbatim so this path
    // follows the same server-PTable resolution the synchronous path triggers.
    byte[] indexUuid = walKeyAttrs.get(PhoenixIndexCodec.INDEX_UUID);
    if (indexUuid != null) {
      replicationAttrs.put(PhoenixIndexCodec.INDEX_UUID, indexUuid);
    }
    logGroup.append(tableName, -1, replicable, replicationAttrs);
    logGroup.sync();
  }

  private void populateRowsToLock(MiniBatchOperationInProgress<Mutation> miniBatchOp,
    BatchMutateContext context) {
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (
        this.builder.isAtomicOp(m) || context.returnResult || this.builder.isEnabled(m)
          || (this.builder.hasConditionalTTL(m) && isStrictTTLEnabled(miniBatchOp))
      ) {
        ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
        context.rowsToLock.add(row);
      }
    }
  }

  /**
   * Add the mutations generated by the ON DUPLICATE KEY UPDATE to the current batch.
   * MiniBatchOperationInProgress#addOperationsFromCP() allows coprocessors to attach additional
   * mutations to the incoming mutation. These additional mutations are only executed if the status
   * of the original mutation is set to NOT_RUN. For atomic mutations, we want HBase to ignore the
   * incoming mutation and instead execute the mutations generated by the server for that atomic
   * mutation. But we can’t achieve this behavior just by setting the status of the original
   * mutation to IGNORE because that will also ignore the additional mutations added by the
   * coprocessors. To get around this, we need to do a fixup of the original mutation in the batch.
   * Since we always generate one Put mutation from the incoming atomic Put mutation, we can
   * transfer the cells from the generated Put mutation to the original atomic Put mutation in the
   * batch. The additional mutations (Delete) can then be added to the operationsFromCoprocessors
   * array.
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
        deleteMutations.add((Delete) m);
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
      if ((this.builder.isAtomicOp(m) || this.builder.returnResult(m)) && m instanceof Put) {
        List<Mutation> mutations = generateOnDupMutations(context, (Put) m, miniBatchOp);
        if (!mutations.isEmpty()) {
          addOnDupMutationsToBatch(miniBatchOp, i, mutations);
        } else {
          // empty list of generated mutations implies
          // 1) ON DUPLICATE KEY IGNORE if row already exists, OR
          // 2) ON DUPLICATE KEY UPDATE if CASE expression is specified and in each of
          // them the new value is the same as the old value in the ELSE-clause (empty
          // cell timestamp will NOT be updated)
          byte[] retVal = PInteger.INSTANCE.toBytes(0);
          List<Cell> cells = new ArrayList<>();
          cells.add(PhoenixKeyValueUtil.newKeyValue(m.getRow(), Bytes.toBytes(UPSERT_CF),
            Bytes.toBytes(UPSERT_STATUS_CQ), 0, retVal, 0, retVal.length));

          if (context.returnResult) {
            context.currColumnCellExprMap.forEach((key, value) -> cells.add(value.getFirst()));
            cells.sort(CellComparator.getInstance());
          }

          // put Result in OperationStatus for returning update status from conditional
          // upserts, where 0 represents the row is not updated
          Result result = Result.create(cells);
          miniBatchOp.setOperationStatus(i, new OperationStatus(SUCCESS, result));
          // since this mutation is ignored by setting it's status to success in the coproc
          // it shouldn't be synchronously replicated
          if (this.shouldReplicate) {
            m.setAttribute(IGNORE_REPLICATION_ATTRIB, IGNORE_REPLICATION_ATTRIB_VAL);
          }
        }
      } else if (context.returnResult) {
        Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap = new HashMap<>();
        byte[] rowKey = m.getRow();
        ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(rowKey);
        Pair<Put, Put> dataRowState = context.dataRowStates.get(rowKeyPtr);
        Put currentDataRowState = dataRowState != null ? dataRowState.getFirst() : null;
        if (currentDataRowState != null) {
          updateCurrColumnCellExpr(currentDataRowState, currColumnCellExprMap);
          context.currColumnCellExprMap = currColumnCellExprMap;
        }
      }
    }
  }

  /**
   * If the table has conditional TTL, then before making any update to a row we need to evaluate
   * the ttl expression to check if the current row version has expired. If the current row version
   * has expired then the incoming mutation has to be treated like inserting a new row. This means
   * that when making an update over an expired row, any columns that are not being updated in the
   * new incoming mutation have to be explicitly masked so that the existing column versions are not
   * visible. This is achieved by creating a new Delete mutation and adding DeleteColumn cells for
   * all the columns that have to be masked. This new mutation is then attached to the batch as an
   * additional coproc mutation.
   */
  private void updateMutationsForConditionalTTL(MiniBatchOperationInProgress<Mutation> miniBatchOp,
    BatchMutateContext context) throws IOException {
    // If TTL is not strict, skip conditional TTL processing
    if (!isStrictTTLEnabled(miniBatchOp)) {
      return;
    }
    // mapping from row key to indices in mini batch
    Map<ImmutableBytesPtr, List<Integer>> expiredVersions = Maps.newHashMap();
    Set<ImmutableBytesPtr> notExpiredVersions = Sets.newHashSet();
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (!builder.hasConditionalTTL(m)) {
        continue;
      }
      if (IndexUtil.isDeleteFamily(m)) {
        // no need to fix DeleteFamily mutation
        continue;
      }
      ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
      Pair<Put, Put> dataRowState = context.dataRowStates.get(row);
      if (dataRowState == null) {
        continue;
      }
      Put currentVersion = dataRowState.getFirst();
      if (currentVersion == null) {
        continue;
      }
      if (notExpiredVersions.contains(row)) {
        continue;
      }
      List<Integer> positions = expiredVersions.get(row);
      if (positions != null) {
        positions.add(i);
        continue;
      }
      byte[] ttl = m.getAttribute(BaseScannerRegionObserverConstants.TTL);
      CompiledConditionalTTLExpression ttlExpr =
        (CompiledConditionalTTLExpression) TTLExpressionFactory.create(ttl);
      List<Cell> currentRow = flattenCells(currentVersion);
      // isRaw is false because we are looking at a Put mutation
      if (ttlExpr.isExpired(currentRow, false)) {
        // current version is expired
        positions = Lists.newArrayListWithExpectedSize(2);
        positions.add(i);
        expiredVersions.put(row, positions);
      } else {
        notExpiredVersions.add(row);
      }
    }
    for (Map.Entry<ImmutableBytesPtr, List<Integer>> entry : expiredVersions.entrySet()) {
      ImmutableBytesPtr key = entry.getKey();
      List<Integer> positions = entry.getValue();
      Pair<Put, Put> dataRowState = context.dataRowStates.get(key);
      Put currentVersion = dataRowState.getFirst();
      // keep track of all the columns that have be masked using DeleteColumn
      List<ColumnReference> colsToBeMasked = Lists.newArrayList();
      for (List<Cell> cells : currentVersion.getFamilyCellMap().values()) {
        for (Cell cell : cells) {
          boolean masked = true;
          byte[] family = CellUtil.cloneFamily(cell);
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          for (Integer pos : positions) {
            Mutation m = miniBatchOp.getOperation(pos);
            if (m.has(family, qualifier)) {
              masked = false;
              break;
            }
          }
          if (masked) {
            ColumnReference colRef = new ColumnReference(family, qualifier);
            colsToBeMasked.add(colRef);
          }
        }
      }
      if (!colsToBeMasked.isEmpty()) {
        Mutation m = miniBatchOp.getOperation(positions.get(0));
        // create a new Delete mutation that will have DeleteColumn cells for all the columns
        // that have to be masked.
        Delete masked = new Delete(m.getRow());
        for (ColumnReference col : colsToBeMasked) {
          // build a DeleteColumn cell and it to the new Delete mutation
          KeyValue kv = GenericKeyValueBuilder.INSTANCE.buildDeleteColumns(key,
            col.getFamilyWritable(), col.getQualifierWritable(), HConstants.LATEST_TIMESTAMP);
          masked.add(kv);
        }
        // attach the Delete mutation as an additional coproc mutation to the mini batch
        miniBatchOp.addOperationsFromCP(positions.get(0), new Mutation[] { masked });
      }
      // Since the current version has expired update the in-memory state so that
      // this row is treated as a new row
      context.dataRowStates.put(key, null);
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

  private Collection<? extends Mutation>
    groupMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context)
      throws IOException {
    context.multiMutationMap = new HashMap<>();
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      // skip this mutation if we aren't enabling indexing
      // unfortunately, we really should ask if the raw mutation (rather than the combined mutation)
      // should be indexed, which means we need to expose another method on the builder. Such is the
      // way optimization go though.
      if (
        !isAtomicOperationComplete(miniBatchOp.getOperationStatus(i)) && this.builder.isEnabled(m)
      ) {
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
    IndexBuildManager builder, long ts, boolean isTTLStrict) throws IOException {
    for (Integer i = 0; i < miniBatchOp.size(); i++) {
      if (isAtomicOperationComplete(miniBatchOp.getOperationStatus(i))) {
        continue;
      }
      Mutation m = miniBatchOp.getOperation(i);
      // skip this mutation if we aren't enabling indexing or Conditional TTL
      // or not an atomic op or if it is an atomic op
      // and its timestamp is already set(not LATEST)
      // Also, skip conditional TTL if TTL is not strict
      if (
        !builder.isEnabled(m) && (!builder.hasConditionalTTL(m) || !isTTLStrict)
          && !((builder.isAtomicOp(m) || builder.returnResult(m))
            && IndexUtil.getMaxTimestamp(m) == HConstants.LATEST_TIMESTAMP)
      ) {
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
   * Checks if strict TTL mode is enabled in mutation attributes. Falls back to default value if no
   * attribute is found.
   */
  private boolean isStrictTTLEnabled(MiniBatchOperationInProgress<Mutation> miniBatchOp) {
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      byte[] isStrictTTLBytes = m.getAttribute(BaseScannerRegionObserverConstants.IS_STRICT_TTL);
      if (isStrictTTLBytes != null) {
        try {
          return (Boolean) PBoolean.INSTANCE.toObject(isStrictTTLBytes);
        } catch (Exception e) {
          break;
        }
      }
    }
    return PTable.DEFAULT_IS_STRICT_TTL;
  }

  /**
   * This method returns true if the pending delete mutation needs to be applied and false f the
   * delete mutation can be ignored for example in the case of delete on non-existing row.
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
        // This is a delete row mutation on a non-existing row. There is no need to apply this
        // mutation
        // on the data table
        return false;
      }
    }

    applyDeleteCells(delete, nextDataRowState);
    if (nextDataRowState != null && nextDataRowState.getFamilyCellMap().size() == 0) {
      dataRowState.setSecond(null);
    }
    return true;
  }

  /**
   * This method applies the pending put mutations on the the next row states. Before this method is
   * called, the next row states is set to current row states.
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
      dataRowState.setSecond(
        (nextDataRowState != null) ? applyNew((Put) m, nextDataRowState) : new Put((Put) m));

      Mutation[] mutationsAddedByCP = miniBatchOp.getOperationsFromCoprocessors(i);
      if (mutationsAddedByCP != null) {
        // all added mutations are of type delete corresponding to set nulls
        for (Mutation addedMutation : mutationsAddedByCP) {
          applyOnePendingDeleteMutation(context, (Delete) addedMutation);
        }
      }
    }
  }

  /**
   * * Prepares next data row state
   */
  private void prepareDataRowStates(ObserverContext<RegionCoprocessorEnvironment> c,
    MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context, long now)
    throws IOException {
    if (context.rowsToLock.size() == 0) {
      return;
    }
    applyPendingPutMutations(miniBatchOp, context, now);
    applyPendingDeleteMutations(miniBatchOp, context);
  }

  /**
   * One {@code (row, ts)} group of a replicated mini-batch on the standby: the group's mutations,
   * the per-row pre-image the active shipped for that batch, and the data-row state derived by
   * folding the group's cells onto that pre-image. Built once by {@link #buildReplicatedRowGroups}
   * and consumed by both the global-index path ({@link #prepareReplicatedIndexMutations}, which
   * uses {@code preImage} + {@code nextState}) and the local-index path (which uses
   * {@code preImage} as the builder's prior row state). Different {@code (row, ts)} groups for the
   * same row are kept separate — that's how the standby recovers the active-batch boundary the
   * reader's coalescing can erase.
   */
  static final class ReplicatedRowGroup {
    final ImmutableBytesPtr row;
    final long ts;
    final List<Mutation> mutations;
    final Put preImage;
    final Put nextState;

    ReplicatedRowGroup(ImmutableBytesPtr row, long ts, List<Mutation> mutations, Put preImage,
      Put nextState) {
      this.row = row;
      this.ts = ts;
      this.mutations = mutations;
      this.preImage = preImage;
      this.nextState = nextState;
    }
  }

  /**
   * Group already-enabled replicated mutations by {@code (row, ts)} and, for each group, decode the
   * shipped {@link #PRE_IMAGE} (from the group's first mutation — the active wrote one pre-image
   * cell per row per batch, so all mutations in a group share it) and fold the group's cells onto
   * it to derive the next data-row state. Groups are returned in first-seen order. The caller is
   * responsible for filtering out non-indexed mutations (e.g. via {@code builder.isEnabled}) before
   * calling this.
   */
  @VisibleForTesting
  List<ReplicatedRowGroup> buildReplicatedRowGroups(List<Mutation> enabledMutations)
    throws IOException {
    LinkedHashMap<RowTsKey, List<Mutation>> groups = new LinkedHashMap<>();
    for (Mutation m : enabledMutations) {
      RowTsKey key = new RowTsKey(new ImmutableBytesPtr(m.getRow()), IndexUtil.getMaxTimestamp(m));
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(m);
    }
    List<ReplicatedRowGroup> result = new ArrayList<>(groups.size());
    for (Map.Entry<RowTsKey, List<Mutation>> entry : groups.entrySet()) {
      List<Mutation> groupMutations = entry.getValue();
      Put preImage = decodePreImage(groupMutations.get(0));
      Put nextState = deriveNextState(preImage, groupMutations);
      result.add(new ReplicatedRowGroup(entry.getKey().getRow(), entry.getKey().getTs(),
        groupMutations, preImage, nextState));
    }
    return result;
  }

  /**
   * Collect the mini-batch's index-enabled mutations into a list, preserving order.
   */
  private List<Mutation> enabledMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp) {
    List<Mutation> enabled = new ArrayList<>(miniBatchOp.size());
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (this.builder.isEnabled(m)) {
        enabled.add(m);
      }
    }
    return enabled;
  }

  /**
   * Fold this replicated batch into {@code (row, ts)} groups, once per batch. A table with both a
   * global and a local index reaches this from both index paths; caching on the context avoids
   * re-walking the mini-batch and re-parsing every group's pre-image protobuf a second time.
   */
  private List<ReplicatedRowGroup> getReplicatedRowGroups(
    MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context)
    throws IOException {
    if (context.replicatedRowGroups == null) {
      context.replicatedRowGroups = buildReplicatedRowGroups(enabledMutations(miniBatchOp));
    }
    return context.replicatedRowGroups;
  }

  /**
   * Build the local-index replay inputs from already-grouped replicated mutations: one uniform-ts
   * {@link MultiMutation} per {@code (row, ts)} group (the pending mutations the local builder
   * processes) and, as a side effect, populate {@code preImageCellsByRowTs} mapping each group's
   * {@code (row, ts)} to the pre-image cells the active shipped (the builder's prior row state).
   * The map is keyed by {@code (row, ts)} so a row recurring across concatenated active batches
   * gets each batch's own pre-image rather than collapsing to the earliest. A {@code null}
   * pre-image (active saw an empty row) maps to a {@code null} cell list — the documented "no prior
   * row" sentinel. The returned {@link MultiMutation}s mirror what {@code groupMutations} produces
   * on the active, but grouped by {@code (row, ts)} so each is uniform-ts as
   * {@code NonTxIndexBuilder} requires.
   */
  private Collection<? extends Mutation> buildReplayLocalIndexInputs(
    List<ReplicatedRowGroup> groups, Map<RowTsKey, List<Cell>> preImageCellsByRowTs) {
    List<Mutation> pending = new ArrayList<>(groups.size());
    for (ReplicatedRowGroup group : groups) {
      MultiMutation mm = new MultiMutation(group.row);
      for (Mutation m : group.mutations) {
        mm.addAll(m);
      }
      pending.add(mm);
      List<Cell> priorCells =
        group.preImage == null ? null : MutationCellGrouper.flattenCells(group.preImage);
      preImageCellsByRowTs.put(new RowTsKey(group.row, group.ts), priorCells);
    }
    return pending;
  }

  /**
   * Standby-side counterpart to {@link #prepareIndexMutations}. Groups the mini-batch's mutations
   * by {@code (row, ts)} so all mutations from the same active-side batch on the same row are
   * processed together against one shared pre-image. Different {@code (row, ts)} groups for the
   * same row produce separate index updates — that's how the standby recovers the active-batch
   * boundary that the reader's coalescing can erase.
   * <p>
   * For each group: decode {@link #PRE_IMAGE} from the first mutation (all mutations in a group
   * share one pre-image because the active wrote one pre-image cell per row per batch), apply the
   * group's cells on top to derive {@code nextDataRowState}, then call
   * {@link #generateIndexMutationsForRow} with the group's ts so the resulting index Mutation cells
   * carry the correct timestamp.
   * <p>
   * Skips {@code getCurrentRowStates} (unsafe under out-of-order replay) and writes directly to
   * {@code context.indexUpdates}.
   */
  private void prepareReplicatedIndexMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
    BatchMutateContext context, List<IndexMaintainer> maintainers) throws IOException {
    List<Pair<IndexMaintainer, HTableInterfaceReference>> indexTables =
      buildIndexTablesList(maintainers);
    for (ReplicatedRowGroup group : getReplicatedRowGroups(miniBatchOp, context)) {
      if (group.preImage == null && group.nextState == null) {
        continue;
      }
      ListMultimap<HTableInterfaceReference, Mutation> idxUpdates = ArrayListMultimap.create();
      generateIndexMutationsForRow(group.row, group.preImage, group.nextState, group.ts,
        encodedRegionName, QueryConstants.UNVERIFIED_BYTES, indexTables, idxUpdates);
      for (Map.Entry<HTableInterfaceReference, Mutation> idxUpdate : idxUpdates.entries()) {
        context.indexUpdates.put(idxUpdate.getKey(),
          new Pair<>(idxUpdate.getValue(), group.row.get()));
      }
    }
  }

  /**
   * Decodes the {@link #PRE_IMAGE} attribute on a standby-side replicated mutation. Returns
   * {@code null} when the active observed an empty row at lock time (sentinel: zero-length value).
   * Throws when the attribute is absent — that signals a contract violation: an indexed-table
   * mutation arrived on the standby with no pre-image, which should never happen because the active
   * runs {@code prepareDataRowStates} (and writes a pre-image cell) for every table with a
   * global/uncovered/transform/CDC index.
   * <p>
   * The one way this can legitimately occur is schema skew: the standby carries an index the active
   * lacked when it shipped the batch, so the mutation is index-enabled on replay but no pre-image
   * was captured on the active. That already breaks the feature's foundational assumption — index
   * regeneration requires the active and standby to agree on the set of index maintainers — so we
   * fail loud (a non-retryable {@link DoNotRetryIOException}) rather than silently regenerate the
   * index against a missing prior state, which would corrupt it.
   */
  @VisibleForTesting
  Put decodePreImage(Mutation m) throws IOException {
    byte[] preImageBytes = m.getAttribute(PRE_IMAGE);
    if (preImageBytes == null) {
      throw new DoNotRetryIOException("Replicated mutation on table " + dataTableName
        + " is missing the " + PRE_IMAGE + " attribute: row=" + Bytes.toStringBinary(m.getRow()));
    }
    if (preImageBytes.length == 0) {
      return null;
    }
    return ProtobufUtil.toPut(ClientProtos.MutationProto.parseFrom(preImageBytes));
  }

  /**
   * Encode a pre-image Put into the bytes carried by the {@link #PRE_IMAGE_WAL_QUALIFIER} cell
   * (and, after the reader peels it off, the {@link #PRE_IMAGE} attribute). A {@code null}
   * pre-image means the active side saw an empty row; it encodes to a zero-length array, the
   * sentinel {@link #decodePreImage} maps back to {@code null}.
   */
  @VisibleForTesting
  static byte[] encodePreImage(Put preImage) throws IOException {
    return preImage != null
      ? ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, preImage).toByteArray()
      : HConstants.EMPTY_BYTE_ARRAY;
  }

  /**
   * Build the METAFAMILY pre-image cell that the active appends to the WAL edit (and that the
   * reader peels off into the {@link #PRE_IMAGE} attribute). {@code priorState} is the row state
   * the active observed at lock time; {@code null} encodes to the empty-row sentinel.
   */
  @VisibleForTesting
  public static Cell buildPreImageCell(byte[] row, Put priorState) throws IOException {
    return CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(row)
      .setFamily(WALEdit.METAFAMILY).setQualifier(PRE_IMAGE_WAL_QUALIFIER)
      .setTimestamp(HConstants.LATEST_TIMESTAMP).setType(Cell.Type.Put)
      .setValue(encodePreImage(priorState)).build();
  }

  /**
   * Applies a Delete's cells to a Put, returning the resulting Put or {@code null} if the row goes
   * empty. Used by {@link #prepareReplicatedIndexMutations} to derive {@code nextDataRowState} from
   * a (preImage + cells) sequence within one (row, ts) group.
   */
  @VisibleForTesting
  static Put applyDeleteToPut(Delete delete, Put put) {
    if (put == null) {
      return null;
    }
    applyDeleteCells(delete, put);
    return put.getFamilyCellMap().isEmpty() ? null : put;
  }

  /**
   * Applies a Delete's tombstone cells to {@code put} in place: DeleteFamily/DeleteFamilyVersion
   * drop the whole family, DeleteColumn/Delete drop the column. Shared by {@link #applyDeleteToPut}
   * (replay next-state derivation) and {@link #applyOnePendingDeleteMutation} (active next-state).
   */
  private static void applyDeleteCells(Delete delete, Put put) {
    for (List<Cell> cells : delete.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        switch (cell.getType()) {
          case DeleteFamily:
          case DeleteFamilyVersion:
            put.getFamilyCellMap().remove(CellUtil.cloneFamily(cell));
            break;
          case DeleteColumn:
          case Delete:
            removeColumn(put, cell);
            break;
          default:
            break;
        }
      }
    }
  }

  /**
   * Fold a (row, ts) group's mutations onto its pre-image to derive the next data-row state. Puts
   * are merged on top of the running state ({@code applyNew}); Deletes peel cells back out
   * ({@link #applyDeleteToPut}). Returns {@code null} if there is no pre-image and the group never
   * produces a Put, or if a Delete empties the row. Mirrors the single-row fold Phoenix performs on
   * the active side when building {@code nextDataRowState}.
   */
  @VisibleForTesting
  static Put deriveNextState(Put preImage, List<Mutation> groupMutations) throws IOException {
    Put nextState = preImage != null ? new Put(preImage) : null;
    for (Mutation m : groupMutations) {
      if (m instanceof Put) {
        nextState = nextState != null ? applyNew((Put) m, nextState) : new Put((Put) m);
      } else if (m instanceof Delete) {
        nextState = applyDeleteToPut((Delete) m, nextState);
      }
    }
    return nextState;
  }

  /**
   * True when this batch will be shipped to the replication log: replication is on, not disabled
   * for testing, and an HA log group is present. Gates the active-side pre-image capture — the
   * pre-image exists only so the standby can regenerate its index, so capturing it on a
   * non-replicated batch would be wasted work (and an unnecessary region scan on the local path).
   */
  private boolean isReplicatedBatch(BatchMutateContext context) {
    return shouldReplicate && !ignoreSyncReplicationForTesting && context.logGroup.isPresent();
  }

  /**
   * Emit one pre-image cell per replicated row into {@code miniBatchOp.getWalEdit(0)}. This is the
   * sole producer of pre-image cells for both replication paths: the synchronous
   * {@link #replicateMutations} reads them back from the same WAL slot in POST, and the WAL-restore
   * {@link #replicateEditOnWALRestore} reads them from the persisted WAL edit HBase builds from
   * that slot — so both ship byte-identical pre-images with no re-derivation.
   * <p>
   * We walk the batch (not {@code dataRowStates} directly) and skip {@code ignoreReplicationFilter}
   * mutations, so a row filtered out of replication (e.g. syscat rows without a leading tenant id)
   * gets no pre-image even though it may carry a {@code dataRowStates} entry. Each replicated row
   * is emitted once, from its {@code dataRowStates} entry.
   * <p>
   * Pre-image bytes are PB-encoded {@link Put}; an empty value is the sentinel for "primary
   * observed an empty row at lock time" so the standby can distinguish that from "no primary
   * information shipped". Rows not in {@code dataRowStates} (e.g. row not visited) contribute no
   * pre-image cell — the standby falls back to its no-pre-image code path for those rows.
   * <p>
   * Called from {@link #preBatchMutateWithExceptions} immediately after
   * {@link #prepareDataRowStates} returns, on the global/uncovered/transform-index branch, and from
   * {@link #captureLocalIndexPreImageCells} on the local-only replicated branch.
   */
  private void capturePreImageCells(MiniBatchOperationInProgress<Mutation> miniBatchOp,
    BatchMutateContext context) throws IOException {
    if (context.dataRowStates == null || context.dataRowStates.isEmpty()) {
      return;
    }
    WALEdit walEdit = miniBatchOp.getWalEdit(0);
    if (walEdit == null) {
      walEdit = new WALEdit();
    }
    Set<ImmutableBytesPtr> emitted = new HashSet<>();
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (ignoreReplicationFilter.test(m)) {
        continue;
      }
      ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(m.getRow());
      if (!emitted.add(rowKeyPtr)) {
        continue;
      }
      Pair<Put, Put> rowState = context.dataRowStates.get(rowKeyPtr);
      if (rowState == null) {
        continue;
      }
      walEdit.add(buildPreImageCell(rowKeyPtr.copyBytes(), rowState.getFirst()));
    }
    if (!walEdit.isEmpty()) {
      miniBatchOp.setWalEdit(0, walEdit);
    }
  }

  /**
   * Active-side pre-image capture for a <i>local-only</i> replicated table. Such a table has no
   * global/uncovered/transform index, so it never enters the branch that runs
   * {@link #prepareDataRowStates} + {@link #capturePreImageCells}; without this it would ship no
   * pre-image and the standby's {@link PreImageLocalTable} would have nothing to read. The local
   * index build already reads the prior row state through {@code cachedLocalTable} (a region scan
   * scoped to index-relevant columns), so we reuse that exact state as the pre-image — no extra
   * scan, and the same column scope the standby's local builder consumes. {@code dataRowStates} is
   * populated with the prior row as {@code first} (a {@code null} prior row encodes to the
   * empty-row sentinel) so {@link #capturePreImageCells} can emit one pre-image cell per replicated
   * row.
   */
  private void captureLocalIndexPreImageCells(MiniBatchOperationInProgress<Mutation> miniBatchOp,
    BatchMutateContext context, Collection<? extends Mutation> groupedMutations,
    CachedLocalTable cachedLocalTable) throws IOException {
    context.dataRowStates = new HashMap<>(groupedMutations.size());
    for (Mutation m : groupedMutations) {
      List<Cell> priorCells =
        cachedLocalTable.getCurrentRowState(m, Collections.<ColumnReference> emptyList(), false);
      Put priorState = null;
      if (priorCells != null && !priorCells.isEmpty()) {
        priorState = new Put(m.getRow());
        for (Cell cell : priorCells) {
          priorState.add(cell);
        }
      }
      context.dataRowStates.put(new ImmutableBytesPtr(m.getRow()), new Pair<>(priorState, null));
    }
    capturePreImageCells(miniBatchOp, context);
  }

  /**
   * The index update generation for local indexes uses the existing index update generation code
   * (i.e., the {@link IndexBuilder} implementation).
   */
  private void handleLocalIndexUpdates(TableName table,
    MiniBatchOperationInProgress<Mutation> miniBatchOp,
    Collection<? extends Mutation> pendingMutations, PhoenixIndexMetaData indexMetaData,
    LocalHBaseState localHBaseState) throws Throwable {
    ListMultimap<HTableInterfaceReference, Pair<Mutation, byte[]>> indexUpdates =
      ArrayListMultimap.<HTableInterfaceReference, Pair<Mutation, byte[]>> create();
    if (localHBaseState != null) {
      // Caller supplied the prior-row-state source: the standby replay passes a PreImageLocalTable
      // (prior state from the shipped PRE_IMAGE, not a region scan), and the active local-only
      // replicated path passes the CachedLocalTable it already built (so the pre-image capture and
      // the index build share one scan).
      this.builder.getIndexUpdates(indexUpdates, miniBatchOp, pendingMutations, indexMetaData,
        localHBaseState);
    } else {
      this.builder.getIndexUpdates(indexUpdates, miniBatchOp, pendingMutations, indexMetaData);
    }
    byte[] tableName = table.getName();
    HTableInterfaceReference hTableInterfaceReference =
      new HTableInterfaceReference(new ImmutableBytesPtr(tableName));
    List<Pair<Mutation, byte[]>> localIndexUpdates =
      indexUpdates.removeAll(hTableInterfaceReference);
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
   * Determines if any of the data table mutations in the given batch does not include all the
   * indexed columns or the where clause columns for partial uncovered indexes.
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
        BatchMutateContext lastContext = existingPendingRow.addAndGetPrevCtx(context);
        if (lastContext != null) {
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
              // We have detected a concurrent update so do a deep copy of the
              // previous update but we can skip the attributes
              Put copy = MutationUtil.copyPut(put, true);
              context.dataRowStates.put(rowKeyPtr, new Pair<>(copy, new Put(copy)));
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
    } else {
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
        context.dataRowStates.put(new ImmutableBytesPtr(rowKey),
          new Pair<Put, Put>(put, new Put(put)));
      }
    }
  }

  public static Mutation getDeleteIndexMutation(Put dataRowState, IndexMaintainer indexMaintainer,
    long ts, ImmutableBytesPtr rowKeyPtr, byte[] encodedRegionName) {
    ValueGetter dataRowVG = new IndexUtil.SimpleValueGetter(dataRowState);
    byte[] indexRowKey =
      indexMaintainer.buildRowKey(dataRowVG, rowKeyPtr, null, null, ts, encodedRegionName);
    return indexMaintainer.buildRowDeleteMutation(indexRowKey,
      IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
  }

  public static void generateIndexMutationsForRow(ImmutableBytesPtr rowKeyPtr,
    Put currentDataRowState, Put nextDataRowState, long ts, byte[] encodedRegionName,
    byte[] emptyColumnValue, List<Pair<IndexMaintainer, HTableInterfaceReference>> indexTables,
    ListMultimap<HTableInterfaceReference, Mutation> indexUpdates) throws IOException {
    for (Pair<IndexMaintainer, HTableInterfaceReference> pair : indexTables) {
      IndexMaintainer indexMaintainer = pair.getFirst();
      HTableInterfaceReference hTableInterfaceReference = pair.getSecond();
      if (
        nextDataRowState != null && indexMaintainer.shouldPrepareIndexMutations(nextDataRowState)
      ) {
        ValueGetter nextDataRowVG = new IndexUtil.SimpleValueGetter(nextDataRowState);
        Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
          nextDataRowVG, rowKeyPtr, ts, null, null, false, encodedRegionName);
        if (indexPut == null) {
          // No covered column. Just prepare an index row with the empty column
          byte[] indexRowKey = indexMaintainer.buildRowKey(nextDataRowVG, rowKeyPtr, null, null, ts,
            encodedRegionName);
          indexPut = new Put(indexRowKey);
        } else {
          IndexUtil.removeEmptyColumn(indexPut,
            indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
            indexMaintainer.getEmptyKeyValueQualifier());
        }
        byte[] finalEmptyColumnValue =
          indexMaintainer.isUncovered() ? QueryConstants.UNVERIFIED_BYTES : emptyColumnValue;
        indexPut.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
          indexMaintainer.getEmptyKeyValueQualifier(), ts, finalEmptyColumnValue);
        indexUpdates.put(hTableInterfaceReference, indexPut);
        if (!ignoreWritingDeleteColumnsToIndex) {
          Delete deleteColumn = indexMaintainer.buildDeleteColumnMutation(indexPut, ts);
          if (deleteColumn != null) {
            indexUpdates.put(hTableInterfaceReference, deleteColumn);
          }
        }
        // Delete the current index row if the new index key is different from the
        // current one and the index is not a CDC index
        if (currentDataRowState != null) {
          ValueGetter currentDataRowVG = new IndexUtil.SimpleValueGetter(currentDataRowState);
          byte[] indexRowKeyForCurrentDataRow = indexMaintainer.buildRowKey(currentDataRowVG,
            rowKeyPtr, null, null, ts, encodedRegionName);
          if (
            !indexMaintainer.isCDCIndex()
              && Bytes.compareTo(indexPut.getRow(), indexRowKeyForCurrentDataRow) != 0
          ) {
            Mutation del = indexMaintainer.buildRowDeleteMutation(indexRowKeyForCurrentDataRow,
              IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
            indexUpdates.put(hTableInterfaceReference, del);
          }
        }
      } else if (
        currentDataRowState != null
          && indexMaintainer.shouldPrepareIndexMutations(currentDataRowState)
      ) {
        if (indexMaintainer.isCDCIndex()) {
          // CDC Index needs two a delete marker for referencing the data table
          // delete mutation with the right index row key, that is, the index row key
          // starting with ts
          Put cdcDataRowState = new Put(currentDataRowState.getRow());
          cdcDataRowState.addColumn(indexMaintainer.getDataEmptyKeyValueCF(),
            indexMaintainer.getEmptyKeyValueQualifierForDataTable(), ts, ByteUtil.EMPTY_BYTE_ARRAY);
          indexUpdates.put(hTableInterfaceReference, getDeleteIndexMutation(cdcDataRowState,
            indexMaintainer, ts, rowKeyPtr, encodedRegionName));
        } else {
          indexUpdates.put(hTableInterfaceReference, getDeleteIndexMutation(currentDataRowState,
            indexMaintainer, ts, rowKeyPtr, encodedRegionName));
        }
      }
    }
  }

  /**
   * Generate the index update for a data row from the mutation that are obtained by merging the
   * previous data row state with the pending row mutation.
   */
  private void prepareIndexMutations(BatchMutateContext context, List<IndexMaintainer> maintainers,
    long batchTimestamp) throws IOException {
    List<Pair<IndexMaintainer, HTableInterfaceReference>> indexTables =
      buildIndexTablesList(maintainers);
    for (Map.Entry<ImmutableBytesPtr, Pair<Put, Put>> entry : context.dataRowStates.entrySet()) {
      ImmutableBytesPtr rowKeyPtr = entry.getKey();
      Pair<Put, Put> dataRowState = entry.getValue();
      Put currentDataRowState = dataRowState.getFirst();
      Put nextDataRowState = dataRowState.getSecond();
      if (currentDataRowState == null && nextDataRowState == null) {
        continue;
      }
      ListMultimap<HTableInterfaceReference, Mutation> idxUpdates = ArrayListMultimap.create();
      generateIndexMutationsForRow(rowKeyPtr, currentDataRowState, nextDataRowState, batchTimestamp,
        encodedRegionName, QueryConstants.UNVERIFIED_BYTES, indexTables, idxUpdates);
      for (Map.Entry<HTableInterfaceReference, Mutation> idxUpdate : idxUpdates.entries()) {
        context.indexUpdates.put(idxUpdate.getKey(),
          new Pair<>(idxUpdate.getValue(), rowKeyPtr.get()));
      }
    }
  }

  private List<Pair<IndexMaintainer, HTableInterfaceReference>>
    buildIndexTablesList(List<IndexMaintainer> maintainers) {
    List<Pair<IndexMaintainer, HTableInterfaceReference>> indexTables =
      new ArrayList<>(maintainers.size());
    for (IndexMaintainer indexMaintainer : maintainers) {
      if (indexMaintainer.isLocalIndex()) {
        continue;
      }
      if (
        !serializeCDCMutations && indexMaintainer.getIndexConsistency() != null
          && indexMaintainer.getIndexConsistency().isAsynchronous()
      ) {
        continue;
      }
      HTableInterfaceReference hTableInterfaceReference =
        new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
      indexTables.add(new Pair<>(indexMaintainer, hTableInterfaceReference));
    }
    return indexTables;
  }

  /**
   * This method prepares unverified index mutations which are applied to index tables before the
   * data table is updated. In the three-phase update approach, in phase 1, the status of existing
   * index rows is set to "unverified" these rows will be deleted from the index table in phase 3),
   * and/or new put mutations are added with the unverified status. In phase 2, data table mutations
   * are applied. In phase 3, the status for an index table row is either set to "verified" or the
   * row is deleted.
   */
  private void preparePreIndexMutations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
    BatchMutateContext context, long batchTimestamp, PhoenixIndexMetaData indexMetaData)
    throws Throwable {
    List<IndexMaintainer> maintainers = indexMetaData.getIndexMaintainers();
    // get the current span, or just use a null-span to avoid a bunch of if statements
    try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
      Span current = scope.getSpan();
      if (current == null) {
        current = NullSpan.INSTANCE;
      }
      current.addTimelineAnnotation("Built index updates, doing preStep");
      // The rest of this method is for handling global index updates
      context.indexUpdates =
        ArrayListMultimap.<HTableInterfaceReference, Pair<Mutation, byte[]>> create();
      if (context.isReplication) {
        // Replicated batches carry per-row pre-images and per-cell timestamps from the active.
        // Group by (row, ts) so each active-side batch's mutations are processed against their own
        // pre-image — recovers the active-batch boundary the reader's coalescing can erase.
        prepareReplicatedIndexMutations(miniBatchOp, context, maintainers);
      } else {
        prepareIndexMutations(context, maintainers, batchTimestamp);
      }

      if (serializeCDCMutations) {
        prepareEventuallyConsistentIndexMutations(context, maintainers, compressCDCMutations);
      }

      context.preIndexUpdates = ArrayListMultimap.<HTableInterfaceReference, Mutation> create();
      int updateCount = 0;
      for (IndexMaintainer indexMaintainer : maintainers) {
        if (
          indexMaintainer.getIndexConsistency() != null
            && indexMaintainer.getIndexConsistency().isAsynchronous()
        ) {
          continue;
        }
        updateCount++;
        byte[] emptyCF = indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary();
        byte[] emptyCQ = indexMaintainer.getEmptyKeyValueQualifier();
        HTableInterfaceReference hTableInterfaceReference =
          new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
        List<Pair<Mutation, byte[]>> updates = context.indexUpdates.get(hTableInterfaceReference);
        for (Pair<Mutation, byte[]> update : updates) {
          Mutation m = update.getFirst();
          long ts = IndexUtil.getMaxTimestamp(m);
          RowTsKey cdcKey = new RowTsKey(new ImmutableBytesPtr(update.getSecond()), ts);
          if (m instanceof Put) {
            if (indexMaintainer.isCDCIndex() && context.cdcPreMutationsBytes != null) {
              byte[] cdcMutationsBytes = context.cdcPreMutationsBytes.get(cdcKey);
              if (cdcMutationsBytes != null) {
                ((Put) m).addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                  QueryConstants.CDC_INDEX_PRE_MUTATIONS_CQ_BYTES, ts, cdcMutationsBytes);
              }
            }
            // This will be done before the data table row is updated (i.e., in the first write
            // phase)
            context.preIndexUpdates.put(hTableInterfaceReference, m);
          } else if (IndexUtil.isDeleteFamily(m)) {
            // DeleteColumn is always accompanied by a Put so no need to make the index
            // row unverified again. Only do this for DeleteFamily
            // Set the status of the index row to "unverified"
            Put unverifiedPut = new Put(m.getRow());
            unverifiedPut.addColumn(emptyCF, emptyCQ, ts, QueryConstants.UNVERIFIED_BYTES);
            if (indexMaintainer.isCDCIndex() && context.cdcPreMutationsBytes != null) {
              byte[] cdcMutationsBytes = context.cdcPreMutationsBytes.get(cdcKey);
              if (cdcMutationsBytes != null) {
                unverifiedPut.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                  QueryConstants.CDC_INDEX_PRE_MUTATIONS_CQ_BYTES, ts, cdcMutationsBytes);
              }
            }
            // This will be done before the data table row is updated (i.e., in the first write
            // phase)
            context.preIndexUpdates.put(hTableInterfaceReference, unverifiedPut);
          }
        }
      }
      TracingUtils.addAnnotation(current, "index update count", updateCount);
    }
  }

  /**
   * Prepares pre-phase and post-phase cdc mutations for eventually consistent indexes. Each
   * resulting builder/bytes entry is keyed by {@code (dataRowKey, ts)} where ts is read from the
   * index Mutation's own cells (already stamped by {@link #generateIndexMutationsForRow}). On the
   * active path each row produces one entry (ts == batchTimestamp); on the standby's per-(row, ts)
   * grouping, two batches for the same row produce two entries.
   * @param context           batch mutate context.
   * @param maintainers       the list of index maintainers.
   * @param compressMutations whether to Snappy-compress the serialized proto bytes.
   * @throws IOException if there is an error.
   */
  private static void prepareEventuallyConsistentIndexMutations(BatchMutateContext context,
    List<IndexMaintainer> maintainers, boolean compressMutations) throws IOException {
    // Store pre-index and post-index mutations per (data row, ts) group.
    Map<RowTsKey, IndexMutationsProtos.IndexMutations.Builder> preBuilderMap = new HashMap<>();
    Map<RowTsKey, IndexMutationsProtos.IndexMutations.Builder> postBuilderMap = new HashMap<>();

    for (IndexMaintainer indexMaintainer : maintainers) {
      if (
        indexMaintainer.getIndexConsistency() == null
          || !indexMaintainer.getIndexConsistency().isAsynchronous()
      ) {
        continue;
      }
      byte[] emptyCF = indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary();
      byte[] emptyCQ = indexMaintainer.getEmptyKeyValueQualifier();
      HTableInterfaceReference hTableInterfaceReference =
        new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
      List<Pair<Mutation, byte[]>> updates = context.indexUpdates.get(hTableInterfaceReference);
      for (Pair<Mutation, byte[]> update : updates) {
        Mutation m = update.getFirst();
        byte[] dataRowKey = update.getSecond();
        long ts = IndexUtil.getMaxTimestamp(m);
        RowTsKey key = new RowTsKey(new ImmutableBytesPtr(dataRowKey), ts);
        IndexMutationsProtos.IndexMutations.Builder preBuilder =
          preBuilderMap.computeIfAbsent(key, k -> IndexMutationsProtos.IndexMutations.newBuilder());
        IndexMutationsProtos.IndexMutations.Builder postBuilder = postBuilderMap
          .computeIfAbsent(key, k -> IndexMutationsProtos.IndexMutations.newBuilder());
        if (m instanceof Put) {
          preBuilder.addTables(ByteString.copyFrom(indexMaintainer.getIndexTableName()));
          byte[] preMutation =
            ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, m).toByteArray();
          preBuilder.addMutations(ByteString.copyFrom(preMutation));
          if (!indexMaintainer.isUncovered()) {
            Put verifiedPut = new Put(m.getRow());
            verifiedPut.addColumn(emptyCF, emptyCQ, ts, QueryConstants.VERIFIED_BYTES);
            postBuilder.addTables(ByteString.copyFrom(indexMaintainer.getIndexTableName()));
            byte[] postMutation = ProtobufUtil
              .toMutation(ClientProtos.MutationProto.MutationType.PUT, verifiedPut).toByteArray();
            postBuilder.addMutations(ByteString.copyFrom(postMutation));
          }
        } else {
          if (IndexUtil.isDeleteFamily(m)) {
            Put unverifiedPut = new Put(m.getRow());
            unverifiedPut.addColumn(emptyCF, emptyCQ, ts, QueryConstants.UNVERIFIED_BYTES);
            preBuilder.addTables(ByteString.copyFrom(indexMaintainer.getIndexTableName()));
            byte[] preMutation = ProtobufUtil
              .toMutation(ClientProtos.MutationProto.MutationType.PUT, unverifiedPut).toByteArray();
            preBuilder.addMutations(ByteString.copyFrom(preMutation));
          }
          postBuilder.addTables(ByteString.copyFrom(indexMaintainer.getIndexTableName()));
          byte[] deleteMutation = ProtobufUtil
            .toMutation(ClientProtos.MutationProto.MutationType.DELETE, m).toByteArray();
          postBuilder.addMutations(ByteString.copyFrom(deleteMutation));
        }
      }
    }

    if (!preBuilderMap.isEmpty()) {
      context.cdcPreMutationsBytes = new HashMap<>();
      for (Map.Entry<RowTsKey, IndexMutationsProtos.IndexMutations.Builder> entry : preBuilderMap
        .entrySet()) {
        RowTsKey key = entry.getKey();
        IndexMutationsProtos.IndexMutations.Builder builder = entry.getValue();
        if (builder.getTablesCount() != builder.getMutationsCount()) {
          throw new DoNotRetryIOException(
            "Pre-phase tables and mutations sizes do not match for row key. Tables size: "
              + builder.getTablesCount() + " , mutations size: " + builder.getMutationsCount());
        }
        if (builder.getTablesCount() > 0) {
          byte[] protoBytes = builder.build().toByteArray();
          context.cdcPreMutationsBytes.put(key,
            compressMutations ? Snappy.compress(protoBytes) : protoBytes);
        }
      }
    }

    if (!postBuilderMap.isEmpty()) {
      context.cdcPostMutationsBytes = new HashMap<>();
      for (Map.Entry<RowTsKey, IndexMutationsProtos.IndexMutations.Builder> entry : postBuilderMap
        .entrySet()) {
        RowTsKey key = entry.getKey();
        IndexMutationsProtos.IndexMutations.Builder builder = entry.getValue();
        if (builder.getTablesCount() != builder.getMutationsCount()) {
          throw new DoNotRetryIOException(
            "Post-phase tables and mutations sizes do not match for row key. Tables size: "
              + builder.getTablesCount() + " , mutations size: " + builder.getMutationsCount());
        }
        if (builder.getTablesCount() > 0) {
          byte[] protoBytes = builder.build().toByteArray();
          context.cdcPostMutationsBytes.put(key,
            compressMutations ? Snappy.compress(protoBytes) : protoBytes);
        }
      }
    }
  }

  protected PhoenixIndexMetaData getPhoenixIndexMetaData(
    ObserverContext<RegionCoprocessorEnvironment> observerContext,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    IndexMetaData indexMetaData = this.builder.getIndexMetaData(miniBatchOp);
    if (!(indexMetaData instanceof PhoenixIndexMetaData)) {
      throw new DoNotRetryIOException(
        "preBatchMutateWithExceptions: indexMetaData is not an instance of "
          + PhoenixIndexMetaData.class.getName() + ", current table is:" + observerContext
            .getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString());
    }
    return (PhoenixIndexMetaData) indexMetaData;
  }

  private void preparePostIndexMutations(BatchMutateContext context,
    PhoenixIndexMetaData indexMetaData) {
    context.postIndexUpdates = ArrayListMultimap.<HTableInterfaceReference, Mutation> create();
    List<IndexMaintainer> maintainers = indexMetaData.getIndexMaintainers();
    for (IndexMaintainer indexMaintainer : maintainers) {
      if (
        indexMaintainer.getIndexConsistency() != null
          && indexMaintainer.getIndexConsistency().isAsynchronous()
      ) {
        continue;
      }
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
            verifiedPut.addColumn(emptyCF, emptyCQ, IndexUtil.getMaxTimestamp(m),
              QueryConstants.VERIFIED_BYTES);
            context.postIndexUpdates.put(hTableInterfaceReference, verifiedPut);
          }
        } else {
          context.postIndexUpdates.put(hTableInterfaceReference, m);
        }
      }
    }

    if (context.cdcPostMutationsBytes != null && !context.cdcPostMutationsBytes.isEmpty()) {
      for (IndexMaintainer indexMaintainer : maintainers) {
        if (!indexMaintainer.isCDCIndex()) {
          continue;
        }
        HTableInterfaceReference hTableInterfaceReference =
          new HTableInterfaceReference(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
        List<Pair<Mutation, byte[]>> updates = context.indexUpdates.get(hTableInterfaceReference);
        for (Pair<Mutation, byte[]> update : updates) {
          Mutation m = update.getFirst();
          if (m instanceof Put) {
            long ts = IndexUtil.getMaxTimestamp(m);
            RowTsKey cdcKey = new RowTsKey(new ImmutableBytesPtr(update.getSecond()), ts);
            byte[] cdcMutationsBytes = context.cdcPostMutationsBytes.get(cdcKey);
            if (cdcMutationsBytes != null) {
              Put postPut = new Put(m.getRow());
              postPut.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                QueryConstants.CDC_INDEX_POST_MUTATIONS_CQ_BYTES, ts, cdcMutationsBytes);
              context.postIndexUpdates.put(hTableInterfaceReference, postPut);
            }
          }
        }
      }
    }
    // all cleanup will be done in postBatchMutateIndispensably()
  }

  private static void identifyIndexMaintainerTypes(PhoenixIndexMetaData indexMetaData,
    BatchMutateContext context) {
    for (IndexMaintainer indexMaintainer : indexMetaData.getIndexMaintainers()) {
      if (indexMaintainer.isImmutableRows()) {
        // Here we care if index is immutable in order to skip reading data table rows. However, if
        // the data table storage scheme does not agree with the index table storage scheme, we
        // cannot skip reading data table rows, and thus we cannot treat the index as immutable.
        // Consider the case where data table uses the single cell per column format and index
        // uses the single cell format. If the data table row is updated partially, we need to
        // read the data table row on disk to retrieve missing columns in the partial update to
        // build the full index row. Please note with the single cell format, the row has single
        // cell (and the empty cell)
        if (
          indexMaintainer.getDataImmutableStorageScheme() == indexMaintainer.getIndexStorageScheme()
        ) {
          context.immutableRows = true;
        }
      }
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
    BatchMutateContext context) throws IOException {
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (this.builder.returnResult(m) && miniBatchOp.size() == 1) {
        context.returnResult = true;
        byte[] returnResult = m.getAttribute(PhoenixIndexBuilderHelper.RETURN_RESULT);
        if (
          returnResult != null
            && Arrays.equals(returnResult, PhoenixIndexBuilderHelper.RETURN_RESULT_OLD_ROW)
        ) {
          context.returnOldRow = true;
        }
      }
      if (this.builder.hasConditionalTTL(m) && isStrictTTLEnabled(miniBatchOp)) {
        context.hasConditionalTTL = true;
      }
      if (this.builder.isAtomicOp(m) || this.builder.returnResult(m)) {
        context.hasAtomic = true;
        if (context.hasRowDelete) {
          return;
        }
      } else if (m instanceof Delete) {
        CellScanner scanner = m.cellScanner();
        if (m.isEmpty()) {
          context.hasRowDelete = true;
        } else {
          while (scanner.advance()) {
            if (scanner.current().getType() == Cell.Type.DeleteFamily) {
              context.hasRowDelete = true;
              break;
            }
          }
        }
      }
      if (context.hasAtomic || context.returnResult) {
        return;
      }
    }
  }

  /**
   * Wait for the previous batches to complete. If any of the previous batch fails then this batch
   * will fail too and needs to be retried. The rows are locked by the caller.
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
          if (lastContext.getCurrentPhase() == BatchMutatePhase.FAILED) {
            context.currentPhase = BatchMutatePhase.FAILED;
            break;
          }
          continue;
        }
        // Release the locks so that the previous concurrent mutation can go into the post phase
        unlockRows(context);
        // Wait for at most one concurrentMutationWaitDuration for each level in the dependency tree
        // of batches.
        // lastContext.getMaxPendingRowCount() is the depth of the subtree rooted at the batch
        // pointed by lastContext
        if (
          !countDownLatch.await(
            (lastContext.getMaxPendingRowCount() + 1) * concurrentMutationWaitDuration,
            TimeUnit.MILLISECONDS)
        ) {
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
      // This batch needs to be retried since one of the previous concurrent batches has not
      // completed yet.
      // Throwing an IOException will result in retries of this batch. Removal of reference counts
      // and
      // locks for the rows of this batch will be done in postBatchMutateIndispensably()
      throw new IOException("One of the previous concurrent mutations has not completed. "
        + "The batch needs to be retried " + table.getNameAsString());
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
    MiniBatchOperationInProgress<Mutation> miniBatchOp, Optional<ReplicationLogGroup> logGroup)
    throws Throwable {
    PhoenixIndexMetaData indexMetaData = getPhoenixIndexMetaData(c, miniBatchOp);
    BatchMutateContext context = new BatchMutateContext(indexMetaData.getClientVersion());
    context.logGroup = logGroup;
    setBatchMutateContext(c, context);
    identifyIndexMaintainerTypes(indexMetaData, context);
    identifyMutationTypes(miniBatchOp, context);
    context.populateOriginalMutations(miniBatchOp);
    // The standby reader stamps every reconstructed mutation with REPLICATED_MUTATION; checking
    // the first one is sufficient since the marker is batch-uniform by construction.
    context.isReplication = !context.getOriginalMutations().isEmpty()
      && context.getOriginalMutations().get(0).getAttribute(REPLICATED_MUTATION) != null;
    // Replicated batches must not carry active-side resolution flags. These are resolved on the
    // active cluster before replication, so cells reach the standby already in their final form.
    Preconditions.checkState(
      !context.isReplication
        || (!context.hasAtomic && !context.returnResult && !context.hasConditionalTTL),
      "replicated batch must not carry active-side resolution flags");

    if (context.hasRowDelete) {
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
    // acquired the locks, move to the next phase PRE
    context.currentPhase = BatchMutatePhase.PRE;

    // The standby replay path is deliberately separate: it regenerates index updates from the
    // per-row PRE_IMAGE the active shipped, never reading prior state from the data-table region.
    // Routing it here keeps every active-only step (row-state scans, timestamp assignment, atomic
    // resolution, replication capture) out of a batch that must not run any of them.
    if (context.isReplication) {
      preBatchMutateReplication(c, miniBatchOp, context, indexMetaData);
      return;
    }

    long onDupCheckTime = 0;
    if (
      context.hasAtomic || context.returnResult || context.hasGlobalIndex
        || context.hasUncoveredIndex || context.hasTransform || context.hasConditionalTTL
    ) {
      // Retrieve the current row states from the data table while holding the lock.
      // This is needed for both atomic mutations and global indexes
      long start = EnvironmentEdgeManager.currentTimeMillis();
      context.dataRowStates =
        new HashMap<ImmutableBytesPtr, Pair<Put, Put>>(context.rowsToLock.size());
      if (
        !context.immutableRows && context.hasGlobalIndex || context.hasTransform
          || context.hasAtomic || context.returnResult || context.hasRowDelete
          || context.hasConditionalTTL
          || !context.immutableRows && context.hasUncoveredIndex
            && isPartialUncoveredIndexMutation(indexMetaData, miniBatchOp)
      ) {
        getCurrentRowStates(c, context);
      }
      onDupCheckTime += (EnvironmentEdgeManager.currentTimeMillis() - start);
    }

    if (context.hasConditionalTTL) {
      // If the table has conditional TTL, then before making any update to a row
      // we need to evaluate the ttl expression to check if the current row version has
      // expired. If the current row version has expired then the incoming mutation has to
      // be treated like inserting a new row. Do this before applying atomic upserts since
      // this can affect ON DUPLICATE KEY clauses in the upsert statement.
      updateMutationsForConditionalTTL(miniBatchOp, context);
    }

    if (context.hasAtomic || context.returnResult) {
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
    setTimestamps(miniBatchOp, builder, batchTimestamp, isStrictTTLEnabled(miniBatchOp));
    if (context.hasGlobalIndex || context.hasUncoveredIndex || context.hasTransform) {
      // Prepare next data rows states for pending mutations (for global indexes).
      prepareDataRowStates(c, miniBatchOp, context, batchTimestamp);
      // dataRowStates is now populated; on a replicated batch write per-row pre-image cells to the
      // WAL edit so both replication paths (replicateMutations and replicateEditOnWALRestore) ship
      // them. Skip on a non-replicated batch — the pre-image would be unused work.
      if (isReplicatedBatch(context)) {
        capturePreImageCells(miniBatchOp, context);
      }
      prepareAndCommitGlobalIndexUpdates(table, miniBatchOp, context, batchTimestamp,
        indexMetaData);
    }
    if (context.hasLocalIndex) {
      // Group all the updates for a single row into a single update to be processed (for local
      // indexes).
      Collection<? extends Mutation> mutations = groupMutations(miniBatchOp, context);
      // dataRowStates is populated only by the global/uncovered/transform/atomic/... branch, so a
      // null map here means this table has only a local index and never ran the global-style
      // pre-image capture. Combined with a replicated batch, that identifies a replicated
      // local-only table, which must ship a pre-image so the standby can regenerate its local
      // index. Build the prior-state scan once and reuse it for both the pre-image capture and
      // the index build. Mixed tables already captured a (superset) pre-image in the earlier
      // branch; non-replicated local-only tables keep the unchanged path (no extra work).
      if (context.dataRowStates == null && isReplicatedBatch(context)) {
        CachedLocalTable cachedLocalTable =
          CachedLocalTable.build(mutations, indexMetaData, c.getEnvironment().getRegion());
        captureLocalIndexPreImageCells(miniBatchOp, context, mutations, cachedLocalTable);
        handleLocalIndexUpdates(table, miniBatchOp, mutations, indexMetaData, cachedLocalTable);
      } else {
        handleLocalIndexUpdates(table, miniBatchOp, mutations, indexMetaData, null);
      }
    }
    if (failDataTableUpdatesForTesting) {
      throw new DoNotRetryIOException("Simulating the data table write failure");
    }
  }

  /**
   * Standby replay path for {@link #preBatchMutateWithExceptions}. Reached only for replicated
   * batches (batches whose mutations carry the {@link #REPLICATED_MUTATION} marker), after the
   * shared prologue has locked the rows and set the PRE phase. Deliberately runs none of the
   * active-only steps: no timestamp assignment (cells already carry the active's final per-cell
   * timestamps), no data-table row-state scan (index updates are regenerated from the per-row
   * PRE_IMAGE the active shipped), no replication capture (a replayed batch is not re-replicated),
   * and no concurrent-batch wait (each replicated batch is self-sufficient via its PRE_IMAGE, so
   * concurrent standby batches on the same row need no ordering).
   */
  private void preBatchMutateReplication(ObserverContext<RegionCoprocessorEnvironment> c,
    MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context,
    PhoenixIndexMetaData indexMetaData) throws Throwable {
    TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
    if (context.hasGlobalIndex || context.hasUncoveredIndex || context.hasTransform) {
      // batchTimestamp is unused on this path: prepareReplicatedIndexMutations derives each index
      // update's timestamp from its (row, ts) group, not from a batch-wide timestamp.
      prepareAndCommitGlobalIndexUpdates(table, miniBatchOp, context, 0, indexMetaData);
    }
    if (context.hasLocalIndex) {
      // Group by (row, ts) so each replayed active-side batch's cells stay in their own uniform-ts
      // mutation for NonTxIndexBuilder, and serve the builder's prior row state from each group's
      // shipped PRE_IMAGE instead of a (not-yet-written) region scan.
      List<ReplicatedRowGroup> groups = getReplicatedRowGroups(miniBatchOp, context);
      Map<RowTsKey, List<Cell>> preImageCellsByRowTs = new HashMap<>();
      Collection<? extends Mutation> mutations =
        buildReplayLocalIndexInputs(groups, preImageCellsByRowTs);
      handleLocalIndexUpdates(table, miniBatchOp, mutations, indexMetaData,
        new PreImageLocalTable(dataTableName, preImageCellsByRowTs));
    }
    if (failDataTableUpdatesForTesting) {
      throw new DoNotRetryIOException("Simulating the data table write failure");
    }
  }

  /**
   * Shared two-phase index-commit sequence for the global/uncovered/transform branch, run
   * identically on the active and standby paths. The two paths differ only in how
   * {@link #preparePreIndexMutations} fills {@code context.indexUpdates} (computed from
   * {@code dataRowStates} on the active, regenerated from the per-row PRE_IMAGE on the standby);
   * from there the prepare -> unlock -> doPre -> lock -> wait -> post protocol is the same.
   */
  private void prepareAndCommitGlobalIndexUpdates(TableName table,
    MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context,
    long batchTimestamp, PhoenixIndexMetaData indexMetaData) throws Throwable {
    // early exit if it turns out we don't have any edits
    long start = EnvironmentEdgeManager.currentTimeMillis();
    preparePreIndexMutations(miniBatchOp, context, batchTimestamp, indexMetaData);
    metricSource.updateIndexPrepareTime(dataTableName,
      EnvironmentEdgeManager.currentTimeMillis() - start);
    // Release the locks before making RPC calls for index updates
    unlockRows(context);
    // Do the first phase index updates
    doPre(context);
    // Acquire the locks again before letting the region proceed with data table updates
    lockRows(context);
    // Only populated by getCurrentRowStates, which the standby skips, so this is always null on
    // replication: each replicated batch is self-sufficient via its PRE_IMAGE and needs no wait.
    if (context.lastConcurrentBatchContext != null) {
      waitForPreviousConcurrentBatch(table, context);
    }
    preparePostIndexMutations(context, indexMetaData);
  }

  /**
   * In case of ON DUPLICATE KEY IGNORE, if the row already exists no mutations will be generated so
   * release the row lock.
   */
  private void releaseLocksForOnDupIgnoreMutations(
    MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context) {
    for (int i = 0; i < miniBatchOp.size(); i++) {
      if (!isAtomicOperationComplete(miniBatchOp.getOperationStatus(i))) {
        continue;
      }
      Mutation m = miniBatchOp.getOperation(i);
      if (!this.builder.isAtomicOp(m) && !this.builder.returnResult(m)) {
        continue;
      }
      ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
      Iterator<RowLock> rowLockIterator = context.rowLocks.iterator();
      while (rowLockIterator.hasNext()) {
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

  private void setBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c,
    BatchMutateContext context) {
    this.batchMutateContext.set(context);
  }

  private BatchMutateContext
    getBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c) {
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

    if (shouldReplicate) {
      BatchMutateContext context = getBatchMutateContext(c);
      appendHAGroupAttributeToWALKey(key, context);
      appendReplicationAttributesToWALKey(key, context);
    }
  }

  private void appendMutationAttributesToWALKey(WALKey key,
    IndexRegionObserver.BatchMutateContext context) {
    if (context != null && context.getOriginalMutations().size() > 0) {
      Mutation firstMutation = context.getOriginalMutations().get(0);
      Map<String, byte[]> attrMap = firstMutation.getAttributesMap();
      for (MutationState.MutationMetadataType metadataType : MutationState.MutationMetadataType
        .values()) {
        String metadataTypeKey = metadataType.toString();
        if (attrMap.containsKey(metadataTypeKey)) {
          IndexRegionObserver.appendToWALKey(key, metadataTypeKey, attrMap.get(metadataTypeKey));
        }
      }
    }
  }

  /**
   * Save the HA group name if present in the WAL key so that we can use it when restoring from the
   * WAL
   */
  private void appendHAGroupAttributeToWALKey(WALKey key,
    IndexRegionObserver.BatchMutateContext context) {
    if (context != null && context.logGroup.isPresent()) {
      String haGroupName = context.logGroup.get().getHAGroupName();
      IndexRegionObserver.appendToWALKey(key,
        BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB, Bytes.toBytes(haGroupName));
    }
  }

  private void appendReplicationAttributesToWALKey(WALKey key,
    IndexRegionObserver.BatchMutateContext context) {
    if (context == null || context.getOriginalMutations().isEmpty()) {
      return;
    }
    Map<String, byte[]> replicationAttributes = buildReplicationAttributes(context);
    for (Map.Entry<String, byte[]> e : replicationAttributes.entrySet()) {
      IndexRegionObserver.appendToWALKey(key, e.getKey(), e.getValue());
    }
  }

  /**
   * When this hook is called, all the rows in the batch context are locked if the batch of
   * mutations is successful. Because the rows are locked, we can safely make updates to pending row
   * states in memory and perform the necessary cleanup in that case. However, when the batch fails,
   * then some of the rows may not be locked. In that case, we remove the pending row states from
   * the concurrent hash map without updating them since pending rows states become invalid when a
   * batch fails.
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
      // We add to pending rows only after we have locked all the rows in the batch
      // If we are in the INIT phase that means we failed to acquire the locks before the
      // PRE phase
      if (context.getCurrentPhase() != BatchMutatePhase.INIT) {
        removePendingRows(context);
      }
      if (success) {
        context.currentPhase = BatchMutatePhase.POST;
        if ((context.hasAtomic || context.returnResult) && miniBatchOp.size() == 1) {
          if (!isAtomicOperationComplete(miniBatchOp.getOperationStatus(0))) {
            byte[] retVal = PInteger.INSTANCE.toBytes(1);
            Cell cell = PhoenixKeyValueUtil.newKeyValue(miniBatchOp.getOperation(0).getRow(),
              Bytes.toBytes(UPSERT_CF), Bytes.toBytes(UPSERT_STATUS_CQ), 0, retVal, 0,
              retVal.length);
            List<Cell> cells = new ArrayList<>();
            cells.add(cell);

            if (!context.returnOldRow) {
              addCellsIfResultReturned(miniBatchOp, context.returnResult, cells,
                context.currColumnCellExprMap, false);
            } else {
              addCellsIfResultReturned(miniBatchOp, context.returnResult, cells,
                context.oldRowColumnCellExprMap, true);
            }

            Result result = Result.create(cells);
            miniBatchOp.setOperationStatus(0, new OperationStatus(SUCCESS, result));
          }
        }
      } else {
        context.currentPhase = BatchMutatePhase.FAILED;
      }
      context.countDownAllLatches();
      if (context.indexUpdates != null) {
        context.indexUpdates.clear();
      }
      unlockRows(context);
      this.builder.batchCompleted(miniBatchOp);

      if (success) { // The pre-index and data table updates are successful, and now, do post index
                     // updates
        CompletableFuture<Void> postIndexFuture =
          CompletableFuture.runAsync(() -> doPost(c, context));
        if (isReplicatedBatch(context)) {
          replicateMutations(context.logGroup.get(), miniBatchOp, context);
        }
        FutureUtils.get(postIndexFuture);
      }
    } finally {
      removeBatchMutateContext(c);
    }
  }

  /**
   * If the result needs to be returned for the given update operation, identify the appropriate row
   * cells and add them to the input list of cells. The method can return either the updated row
   * cells (for ROW return type) or the original row cells (for OLD_ROW return type).
   * @param miniBatchOp           Batch of mutations getting applied to region.
   * @param returnResult          Whether the result should be returned to the client.
   * @param cells                 The list of cells to be returned back to the client.
   * @param currColumnCellExprMap The map containing column reference to cell mappings. This can be
   *                              either the current/updated state (for ROW) or the original state
   *                              (for OLD_ROW) depending on the return type requested.
   */
  private static void addCellsIfResultReturned(MiniBatchOperationInProgress<Mutation> miniBatchOp,
    boolean returnResult, List<Cell> cells,
    Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap, boolean retainOldRow) {
    if (returnResult) {
      if (currColumnCellExprMap == null) {
        return;
      }
      Mutation mutation = miniBatchOp.getOperation(0);
      if (mutation instanceof Put && !retainOldRow) {
        updateColumnCellExprMap(mutation, currColumnCellExprMap);
      }
      Mutation[] mutations = miniBatchOp.getOperationsFromCoprocessors(0);
      if (mutations != null && !retainOldRow) {
        for (Mutation m : mutations) {
          updateColumnCellExprMap(m, currColumnCellExprMap);
        }
      }
      for (Pair<Cell, Boolean> cellPair : currColumnCellExprMap.values()) {
        cells.add(cellPair.getFirst());
      }
      cells.sort(CellComparator.getInstance());
    }
  }

  /**
   * Update the contents of {@code currColumnCellExprMap} based on the mutation that was
   * successfully applied to the row.
   * @param mutation              The Mutation object which is applied to the row.
   * @param currColumnCellExprMap The map of column to cell reference.
   */
  private static void updateColumnCellExprMap(Mutation mutation,
    Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap) {
    if (mutation != null) {
      for (Map.Entry<byte[], List<Cell>> entry : mutation.getFamilyCellMap().entrySet()) {
        for (Cell entryCell : entry.getValue()) {
          byte[] family = CellUtil.cloneFamily(entryCell);
          byte[] qualifier = CellUtil.cloneQualifier(entryCell);
          ColumnReference colRef = new ColumnReference(family, qualifier);
          if (mutation instanceof Put) {
            currColumnCellExprMap.put(colRef, new Pair<>(entryCell, null));
          } else if (mutation instanceof Delete) {
            currColumnCellExprMap.remove(colRef);
          }
        }
      }
    }
  }

  private void doPost(ObserverContext<RegionCoprocessorEnvironment> c, BatchMutateContext context) {
    long start = EnvironmentEdgeManager.currentTimeMillis();

    try {
      if (failPostIndexUpdatesForTesting) {
        throw new DoNotRetryIOException(
          "Simulating the last (i.e., post) index table write failure");
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
    ListMultimap<HTableInterfaceReference, Mutation> indexUpdates =
      post ? context.postIndexUpdates : context.preIndexUpdates;
    // short circuit, if we don't need to do any work

    if (context == null || indexUpdates == null || indexUpdates.isEmpty()) {
      return;
    }

    // get the current span, or just use a null-span to avoid a bunch of if statements
    try (TraceScope scope =
      Trace.startSpan("Completing " + (post ? "post" : "pre") + " index writes")) {
      Span current = scope.getSpan();
      if (current == null) {
        current = NullSpan.INSTANCE;
      }
      current.addTimelineAnnotation(
        "Actually doing " + (post ? "post" : "pre") + " index update for first time");
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
        throw new DoNotRetryIOException(
          "Simulating the first (i.e., pre) index table write failure");
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
    List<Pair<PTable, List<Expression>>> operations, final Set<ColumnReference> colsReadInExpr)
    throws IOException {
    while (true) {
      ExpressionVisitor<Void> visitor = new StatelessTraverseAllExpressionVisitor<Void>() {
        @Override
        public Void visit(KeyValueColumnExpression expression) {
          colsReadInExpr.add(
            new ColumnReference(expression.getColumnFamily(), expression.getColumnQualifier()));
          return null;
        }
      };
      try {
        int nExpressions = WritableUtils.readVInt(input);
        List<Expression> expressions = Lists.newArrayListWithExpectedSize(nExpressions);
        for (int i = 0; i < nExpressions; i++) {
          Expression expression =
            ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
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
   * This function has been adapted from PhoenixIndexBuilder#executeAtomicOp(). The critical
   * difference being that the code in PhoenixIndexBuilder#executeAtomicOp() generates the mutations
   * by reading the latest data table row from HBase but in order to correctly support concurrent
   * index mutations we need to always read the latest data table row from memory. It takes in an
   * atomic Put mutation and generates a list of Put and Delete mutations. The mutation list will be
   * empty in two cases: 1) ON DUPLICATE KEY IGNORE and the row already exists; 2) ON DUPLICATE KEY
   * UPDATE if CASE expression is specified and in each of them the new value is the same as the old
   * value in the ELSE-clause. Otherwise, we will generate one Put mutation and optionally one
   * Delete mutation (with DeleteColumn type cells for all columns set to null).
   */
  private List<Mutation> generateOnDupMutations(BatchMutateContext context, Put atomicPut,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    List<Mutation> mutations = Lists.newArrayListWithExpectedSize(2);
    byte[] opBytes = atomicPut.getAttribute(ATOMIC_OP_ATTRIB);
    byte[] returnResult = atomicPut.getAttribute(RETURN_RESULT);
    if ((opBytes == null && returnResult == null) || (opBytes == null && miniBatchOp.size() != 1)) {
      // Unexpected
      // Either mutation should be atomic by providing non-null ON DUPLICATE KEY, or
      // if the result needs to be returned, only single row must be updated as part of
      // the batch mutation.
      return null;
    }
    Put put = null;
    Delete delete = null;

    // mutations returned by this function will have the LATEST timestamp
    // later these timestamps will be updated by the IndexRegionObserver#setTimestamps() function
    long ts = HConstants.LATEST_TIMESTAMP;

    // store current cells into a map where the key is ColumnReference of the column family and
    // column qualifier, and value is a pair of cell and a boolean. The value of the boolean
    // will be true if the expression is CaseExpression and Else-clause is evaluated to be
    // true, will be null if there is no expression on this column, otherwise false
    Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap = new HashMap<>();

    byte[] rowKey = atomicPut.getRow();
    ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(rowKey);
    // Get the latest data row state
    Pair<Put, Put> dataRowState = context.dataRowStates.get(rowKeyPtr);
    Put currentDataRowState = dataRowState != null ? dataRowState.getFirst() : null;

    // Create separate map for old row data when OLD_ROW is requested
    // This must be done before any conditional update logic to preserve original state
    if (context.returnResult && context.returnOldRow && currentDataRowState != null) {
      context.oldRowColumnCellExprMap = new HashMap<>();
      updateCurrColumnCellExpr(currentDataRowState, context.oldRowColumnCellExprMap);
    }

    // if result needs to be returned but the DML does not have ON DUPLICATE KEY present,
    // perform the mutation and return the result.
    if (opBytes == null) {
      mutations.add(atomicPut);
      updateCurrColumnCellExpr(currentDataRowState != null ? currentDataRowState : atomicPut,
        currColumnCellExprMap);
      if (context.returnResult) {
        context.currColumnCellExprMap = currColumnCellExprMap;
      }
      return mutations;
    }

    if (PhoenixIndexBuilderHelper.isDupKeyIgnore(opBytes)) {
      if (currentDataRowState == null) {
        // new row
        mutations.add(atomicPut);
        updateCurrColumnCellExpr(atomicPut, currColumnCellExprMap);
      } else {
        updateCurrColumnCellExpr(currentDataRowState, currColumnCellExprMap);
      }
      if (context.returnResult) {
        context.currColumnCellExprMap = currColumnCellExprMap;
      }
      return mutations;
    }

    boolean isUpdateOnly =
      atomicPut.getAttribute(PhoenixIndexBuilderHelper.ATOMIC_OP_UPDATE_ONLY_ATTRIB) != null;
    if (isUpdateOnly && currentDataRowState == null) {
      // UPDATE_ONLY: If row doesn't exist, do nothing
      if (context.returnResult) {
        context.currColumnCellExprMap = currColumnCellExprMap;
      }
      return Collections.emptyList();
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

    if (currentDataRowState == null) { // row doesn't exist
      updateCurrColumnCellExpr(atomicPut, currColumnCellExprMap);
      if (skipFirstOp) {
        if (operations.size() <= 1 && repeat <= 1) {
          // early exit since there is only one ON DUPLICATE KEY UPDATE
          // clause which is ignored because the row doesn't exist so
          // simply use the values in UPSERT VALUES
          mutations.add(atomicPut);
          if (context.returnResult) {
            context.currColumnCellExprMap = currColumnCellExprMap;
          }
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
      updateCurrColumnCellExpr(currentDataRowState, currColumnCellExprMap);
    }

    if (context.returnResult) {
      context.currColumnCellExprMap = currColumnCellExprMap;
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
          Object value = expression.isNullable()
            ? null
            : expression.getDataType().toObject(ptr, column.getSortOrder());
          // We are guaranteed that the two column will have the same type
          if (
            !column.getDataType().isSizeCompatible(ptr, value, column.getDataType(),
              expression.getSortOrder(), expression.getMaxLength(), expression.getScale(),
              column.getMaxLength(), column.getScale())
          ) {
            throw new DataExceedsCapacityException(column.getDataType(), column.getMaxLength(),
              column.getScale(), column.getName().getString());
          }
          column.getDataType().coerceBytes(ptr, value, expression.getDataType(),
            expression.getMaxLength(), expression.getScale(), expression.getSortOrder(),
            column.getMaxLength(), column.getScale(), column.getSortOrder(),
            table.rowKeyOrderOptimizable());
          byte[] bytes = ByteUtil.copyKeyBytesIfNecessary(ptr);
          row.setValue(column, bytes);

          // If the column exist in currColumnCellExprMap, set the boolean value in the
          // map to be true if the expression is CaseExpression and the Else-clause is
          // evaluated to be true
          ColumnReference colRef = new ColumnReference(column.getFamilyName().getBytes(),
            column.getColumnQualifierBytes());
          if (currColumnCellExprMap.containsKey(colRef)) {
            Pair<Cell, Boolean> valuePair = currColumnCellExprMap.get(colRef);
            if (
              expression instanceof CaseExpression
                && ((CaseExpression) expression).evaluateIndexOf(tuple, ptr)
                    == expression.getChildren().size() - 1
            ) {
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

  /**
   * Create or Update ColumnRef to Cell map based on the Put mutation.
   * @param put                   The Put mutation representing the current or new/updated state of
   *                              the row.
   * @param currColumnCellExprMap ColumnRef to Cell mapping for all the cells involved in the given
   *                              mutation.
   */
  private static void updateCurrColumnCellExpr(Put put,
    Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap) {
    if (put == null) {
      return;
    }
    for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        ColumnReference colRef = new ColumnReference(family, qualifier);
        currColumnCellExprMap.put(colRef, new Pair<>(cell, null));
      }
    }
  }

  private void addEmptyKVCellToPut(Put put, MultiKeyValueTuple tuple, PTable table)
    throws IOException {
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
   * This function is to check if a cell need to be updated, based on the current cells' values. The
   * cell will not be updated only if the column exist in the expression in which CASE is specified
   * and the new value is the same as the old value in the ELSE-clause, otherwise it should be
   * updated.
   * @param cell           the cell with new value to be checked
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
    ImmutableBytesPtr newValuePtr =
      new ImmutableBytesPtr(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
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
   * First take all the cells that are present in the latest. Then look at current and any cell not
   * present in latest is taken.
   */
  private static List<Cell> mergeCells(List<Cell> current, List<Cell> latest) {
    Map<ColumnReference, Cell> latestColVals =
      Maps.newHashMapWithExpectedSize(latest.size() + current.size());

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
   * Determines whether the atomic operation is complete based on the operation status. HBase
   * returns null Result by default for successful Put and Delete mutations, only for Increment and
   * Append mutations, non-null Result is returned by default.
   * @param status the operation status.
   * @return true if the atomic operation is completed, false otherwise.
   */
  public static boolean isAtomicOperationComplete(OperationStatus status) {
    return status.getOperationStatusCode() == SUCCESS && status.getResult() != null;
  }

  /**
   * A cell crosses the replication wire iff it is data to replicate or our own pre-image marker.
   * Local-index (L#) cells are dropped: the standby regenerates its local index from the data
   * record, and a replicated L# rowkey would carry the active region's start key, meaningless on
   * the standby whose regions are split and assigned independently. METAFAMILY cells are dropped
   * unless they carry our {@link #PRE_IMAGE_WAL_QUALIFIER}, so a foreign coprocessor's WAL
   * contribution (or any other HBase system marker) cannot leak onto the wire. Applied identically
   * by the synchronous ({@link #replicateMutations}) and WAL-restore
   * ({@link #replicateEditOnWALRestore}) paths so both enforce one wire invariant: data plus our
   * pre-image only.
   */
  private static boolean isReplicableCell(Cell c) {
    if (CellUtil.matchingFamily(c, WALEdit.METAFAMILY)) {
      return CellUtil.matchingQualifier(c, PRE_IMAGE_WAL_QUALIFIER);
    }
    return !MetaDataUtil.isLocalIndexFamily(
      new ImmutableBytesPtr(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength()));
  }

  private void replicateMutations(ReplicationLogGroup logGroup,
    MiniBatchOperationInProgress<Mutation> miniBatchOp, BatchMutateContext context)
    throws IOException {
    // Replicated batches on the standby never re-replicate.
    if (context.isReplication) {
      return;
    }
    if (context.getOriginalMutations().isEmpty()) {
      return;
    }
    // Read the batch's now-final cells directly from miniBatchOp in POST. By this point HBase has
    // finalized timestamps and merged every coprocessor-added cell (local index, on-dup, TTL) into
    // the data mutation's family map (checkAndMergeCPMutations). Local-index (L#) cells are merged
    // under their own L# family, so a single family-key check drops the whole list without touching
    // each cell. The pre-image cells live in the WAL edit (slot 0), written by
    // capturePreImageCells;
    // they are filtered through isReplicableCell so a foreign coprocessor's slot-0 contribution
    // cannot leak onto the wire.
    List<Cell> flattened = new ArrayList<>();
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (ignoreReplicationFilter.test(m)) {
        continue;
      }
      for (Map.Entry<byte[], List<Cell>> entry : m.getFamilyCellMap().entrySet()) {
        // Drop L# cells: the standby regenerates its own local index from the data record, and a
        // replicated L# rowkey would carry the active region's start key, which is meaningless on
        // the standby whose regions are split and assigned independently.
        if (!MetaDataUtil.isLocalIndexFamily(entry.getKey())) {
          flattened.addAll(entry.getValue());
        }
      }
    }
    WALEdit preImageEdit = miniBatchOp.getWalEdit(0);
    if (preImageEdit != null) {
      preImageEdit.getCells().stream().filter(IndexRegionObserver::isReplicableCell)
        .forEach(flattened::add);
    }
    if (flattened.isEmpty()) {
      return;
    }
    Map<String, byte[]> replicationAttributes = buildReplicationAttributes(context);
    logGroup.append(dataTableName, -1, flattened, replicationAttributes);
    logGroup.sync();
  }

  /**
   * Build the replication attribute envelope shipped with a batch: the well-known metadata keys
   * carried on the batch's mutations, plus an empty {@link PhoenixIndexCodec#INDEX_UUID} when (and
   * only when) the table carries an index. An empty UUID forces the standby down the server-PTable
   * resolution path (see {@link PhoenixIndexMetaDataBuilder}), which rebuilds index maintainers
   * from the schema/table/tenant attributes in this same envelope. It is stamped only for indexed
   * tables: a non-indexed table needs no regeneration, and an empty UUID there would push the
   * standby into the server-cache branch and fail with INDEX_METADATA_NOT_FOUND. The active's own
   * resolved index maintainers ({@link BatchMutateContext#hasIndex()}) are the source of truth, not
   * the client-set UUID attribute.
   */
  private static Map<String, byte[]> buildReplicationAttributes(BatchMutateContext context) {
    Map<String, byte[]> replicationAttributes =
      MutationCellGrouper.extractReplicationAttributes(context.getOriginalMutations().get(0));
    if (context.hasIndex()) {
      MutationCellGrouper.stampIndexAttribute(replicationAttributes);
    }
    return replicationAttributes;
  }
}
