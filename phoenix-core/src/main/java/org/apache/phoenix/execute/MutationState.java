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
package org.apache.phoenix.execute;

import static org.apache.phoenix.query.QueryServices.SOURCE_OPERATION_ATTRIB;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_FAILED_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_INDEX_COMMIT_FAILURE_COUNT;
import static org.apache.phoenix.query.QueryServices.WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compat.hbase.HbaseCompatCapabilities;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexFailurePolicy;
import org.apache.phoenix.index.PhoenixIndexFailurePolicy.MutateCommand;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.monitoring.MutationMetricQueue;
import org.apache.phoenix.monitoring.MutationMetricQueue.MutationMetric;
import org.apache.phoenix.monitoring.MutationMetricQueue.NoOpMutationMetricsQueue;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.MaxMutationSizeBytesExceededException;
import org.apache.phoenix.schema.MaxMutationSizeExceededException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableRef;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionContext.PhoenixVisibilityLevel;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.apache.phoenix.util.WALAnnotationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Predicate;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterators;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

/**
 * Tracks the uncommitted state
 */
public class MutationState implements SQLCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MutationState.class);
    private static final int[] EMPTY_STATEMENT_INDEX_ARRAY = new int[0];
    private static final int MAX_COMMIT_RETRIES = 3;

    private final PhoenixConnection connection;
    private final int maxSize;
    private final long maxSizeBytes;
    private final long batchSize;
    private final long batchSizeBytes;
    private long batchCount = 0L;
    // For each table, maintain a list of mutation batches. Each element in the
    // list is a set of row mutations which can be sent in a single commit batch.
    // A regular upsert and a conditional upsert on the same row conflict with
    // each other so they are split and send separately in different commit batches.
    private final Map<TableRef, List<MultiRowMutationState>> mutationsMap;
    private final Set<String> uncommittedPhysicalNames = Sets.newHashSetWithExpectedSize(10);

    private long sizeOffset;
    private int numRows = 0;
    private long estimatedSize = 0;
    private int[] uncommittedStatementIndexes = EMPTY_STATEMENT_INDEX_ARRAY;
    private boolean isExternalTxContext = false;
    private Map<TableRef, List<MultiRowMutationState>> txMutations = Collections.emptyMap();

    private PhoenixTransactionContext phoenixTransactionContext = PhoenixTransactionContext.NULL_CONTEXT;

    private final MutationMetricQueue mutationMetricQueue;
    private ReadMetricQueue readMetricQueue;

    public MutationState(int maxSize, long maxSizeBytes, PhoenixConnection connection) {
        this(maxSize, maxSizeBytes, connection, false, null);
    }

    public MutationState(int maxSize, long maxSizeBytes, PhoenixConnection connection,
           PhoenixTransactionContext txContext) {
        this(maxSize, maxSizeBytes, connection, false, txContext);
    }

    public MutationState(MutationState mutationState) {
        this(mutationState, mutationState.connection);
    }

    public MutationState(MutationState mutationState, PhoenixConnection connection) {
        this(mutationState.maxSize, mutationState.maxSizeBytes, connection, true, mutationState
                .getPhoenixTransactionContext());
    }

    public MutationState(int maxSize, long maxSizeBytes, PhoenixConnection connection,
           long sizeOffset) {
        this(maxSize, maxSizeBytes, connection, false, null, sizeOffset);
    }

    private MutationState(int maxSize, long maxSizeBytes, PhoenixConnection connection,
            boolean subTask, PhoenixTransactionContext txContext) {
        this(maxSize, maxSizeBytes, connection, subTask, txContext, 0);
    }

    private MutationState(int maxSize, long maxSizeBytes, PhoenixConnection connection,
            boolean subTask, PhoenixTransactionContext txContext, long sizeOffset) {
        this(maxSize, maxSizeBytes, connection, Maps.<TableRef, List<MultiRowMutationState>> newHashMapWithExpectedSize(5),
                subTask, txContext);
        this.sizeOffset = sizeOffset;
    }

    MutationState(int maxSize, long maxSizeBytes, PhoenixConnection connection,
            Map<TableRef, List<MultiRowMutationState>> mutationsMap, boolean subTask, PhoenixTransactionContext txContext) {
        this.maxSize = maxSize;
        this.maxSizeBytes = maxSizeBytes;
        this.connection = connection;
        this.batchSize = connection.getMutateBatchSize();
        this.batchSizeBytes = connection.getMutateBatchSizeBytes();
        this.mutationsMap = mutationsMap;
        boolean isMetricsEnabled = connection.isRequestLevelMetricsEnabled();
        this.mutationMetricQueue = isMetricsEnabled ? new MutationMetricQueue()
                : NoOpMutationMetricsQueue.NO_OP_MUTATION_METRICS_QUEUE;
        if (subTask) {
            // this code path is only used while running child scans, we can't pass the txContext to child scans
            // as it is not thread safe, so we use the tx member variable
            phoenixTransactionContext = txContext.newTransactionContext(txContext, subTask);
        } else if (txContext != null) {
            isExternalTxContext = true;
            phoenixTransactionContext = txContext.newTransactionContext(txContext, subTask);
        }
    }

    public MutationState(TableRef table, MultiRowMutationState mutations, long sizeOffset,
           int maxSize, long maxSizeBytes, PhoenixConnection connection) throws SQLException {
        this(maxSize, maxSizeBytes, connection, false, null, sizeOffset);
        if (!mutations.isEmpty()) {
            addMutations(this.mutationsMap, table, mutations);
        }
        this.numRows = mutations.size();
        this.estimatedSize = PhoenixKeyValueUtil.getEstimatedRowMutationSizeWithBatch(this.mutationsMap);

        throwIfTooBig();
    }

    // add a new batch of row mutations
    private void addMutations(Map<TableRef, List<MultiRowMutationState>> mutationMap, TableRef table,
            MultiRowMutationState mutations) {
        List<MultiRowMutationState> batches = mutationMap.get(table);
        if (batches == null) {
            batches = Lists.newArrayListWithExpectedSize(1);
        }
        batches.add(mutations);
        mutationMap.put(table, batches);
    }

    // remove a batch of mutations which have been committed
    private void removeMutations(Map<TableRef, List<MultiRowMutationState>> mutationMap, TableRef table){
        List<MultiRowMutationState> batches = mutationMap.get(table);
        if (batches == null || batches.isEmpty()) {
            mutationMap.remove(table);
            return;
        }

        // mutation batches are committed in FIFO order so always remove from the head
        batches.remove(0);
        if (batches.isEmpty()) {
            mutationMap.remove(table);
        }
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    public PhoenixTransactionContext getPhoenixTransactionContext() {
        return phoenixTransactionContext;
    }

    /**
     * Commit a write fence when creating an index so that we can detect when a data table transaction is started before
     * the create index but completes after it. In this case, we need to rerun the data table transaction after the
     * index creation so that the index rows are generated. See TEPHRA-157 for more information.
     * 
     * @param dataTable
     *            the data table upon which an index is being added
     * @throws SQLException
     */
    public void commitDDLFence(PTable dataTable) throws SQLException {
        if (dataTable.isTransactional()) {
            try {
                phoenixTransactionContext.commitDDLFence(dataTable);
            } finally {
                // The client expects a transaction to be in progress on the txContext while the
                // VisibilityFence.prepareWait() starts a new tx and finishes/aborts it. After it's
                // finished, we start a new one here.
                // TODO: seems like an autonomous tx capability in Tephra would be useful here.
                phoenixTransactionContext.begin();
            }
        }
    }

    public boolean checkpointIfNeccessary(MutationPlan plan) throws SQLException {
        if (!phoenixTransactionContext.isTransactionRunning() || plan.getTargetRef() == null
                || plan.getTargetRef().getTable() == null || !plan.getTargetRef().getTable().isTransactional()) { return false; }
        Set<TableRef> sources = plan.getSourceRefs();
        if (sources.isEmpty()) { return false; }
        // For a DELETE statement, we're always querying the table being deleted from. This isn't
        // a problem, but it potentially could be if there are other references to the same table
        // nested in the DELETE statement (as a sub query or join, for example).
        TableRef ignoreForExcludeCurrent = plan.getOperation() == Operation.DELETE && sources.size() == 1 ? plan
                .getTargetRef() : null;
        boolean excludeCurrent = false;
        String targetPhysicalName = plan.getTargetRef().getTable().getPhysicalName().getString();
        for (TableRef source : sources) {
            if (source.getTable().isTransactional() && !source.equals(ignoreForExcludeCurrent)) {
                String sourcePhysicalName = source.getTable().getPhysicalName().getString();
                if (targetPhysicalName.equals(sourcePhysicalName)) {
                    excludeCurrent = true;
                    break;
                }
            }
        }
        // If we're querying the same table we're updating, we must exclude our writes to
        // it from being visible.
        if (excludeCurrent) {
            // If any source tables have uncommitted data prior to last checkpoint,
            // then we must create a new checkpoint.
            boolean hasUncommittedData = false;
            for (TableRef source : sources) {
                String sourcePhysicalName = source.getTable().getPhysicalName().getString();
                // Tracking uncommitted physical table names is an optimization that prevents us from
                // having to do a checkpoint if no data has yet been written. If we're using an
                // external transaction context, it's possible that data was already written at the
                // current transaction timestamp, so we always checkpoint in that case is we're
                // reading and writing to the same table.
                if (source.getTable().isTransactional()
                        && (isExternalTxContext || uncommittedPhysicalNames.contains(sourcePhysicalName))) {
                    hasUncommittedData = true;
                    break;
                }
            }

            phoenixTransactionContext.checkpoint(hasUncommittedData);

            if (hasUncommittedData) {
                uncommittedPhysicalNames.clear();
            }
            return true;
        }
        return false;
    }

    // Though MutationState is not thread safe in general, this method should be because it may
    // be called by TableResultIterator in a multi-threaded manner. Since we do not want to expose
    // the Transaction outside of MutationState, this seems reasonable, as the member variables
    // would not change as these threads are running. We also clone mutationState to ensure that
    // the transaction context won't change due to a commit when auto commit is true.
    public Table getHTable(PTable table) throws SQLException {
        Table htable = this.getConnection().getQueryServices().getTable(table.getPhysicalName().getBytes());
        if (table.isTransactional() && phoenixTransactionContext.isTransactionRunning()) {
            // We're only using this table for reading, so we want it wrapped even if it's an index
            htable = phoenixTransactionContext.getTransactionalTable(htable, table.isImmutableRows() || table.getType() == PTableType.INDEX);
        }
        return htable;
    }

    public PhoenixConnection getConnection() {
        return connection;
    }

    public boolean isTransactionStarted() {
        return phoenixTransactionContext.isTransactionRunning();
    }

    public long getInitialWritePointer() {
        return phoenixTransactionContext.getTransactionId(); // First write pointer - won't change with checkpointing
    }

    // For testing
    public long getWritePointer() {
        return phoenixTransactionContext.getWritePointer();
    }

    // For testing
    public PhoenixVisibilityLevel getVisibilityLevel() {
        return phoenixTransactionContext.getVisibilityLevel();
    }

    public boolean startTransaction(Provider provider) throws SQLException {
        if (provider == null) { return false; }
        if (!connection.getQueryServices().getProps()
                .getBoolean(QueryServices.TRANSACTIONS_ENABLED, QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED)) { throw new SQLExceptionInfo.Builder(
                SQLExceptionCode.CANNOT_START_TXN_IF_TXN_DISABLED).build().buildException(); }
        if (connection.getSCN() != null) { throw new SQLExceptionInfo.Builder(
                SQLExceptionCode.CANNOT_START_TRANSACTION_WITH_SCN_SET).build().buildException(); }

        if (phoenixTransactionContext == PhoenixTransactionContext.NULL_CONTEXT) {
            phoenixTransactionContext = provider.getTransactionProvider().getTransactionContext(connection);
        } else {
            if (provider != phoenixTransactionContext.getProvider()) { throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.CANNOT_MIX_TXN_PROVIDERS)
                    .setMessage(phoenixTransactionContext.getProvider().name() + " and " + provider.name()).build()
                    .buildException(); }
        }
        if (!isTransactionStarted()) {
            // Clear any transactional state in case transaction was ended outside
            // of Phoenix so we don't carry the old transaction state forward. We
            // cannot call reset() here due to the case of having mutations and
            // then transitioning from non transactional to transactional (which
            // would end up clearing our uncommitted state).
            resetTransactionalState();
            phoenixTransactionContext.begin();
            return true;
        }

        return false;
    }

    public static MutationState emptyMutationState(int maxSize, long maxSizeBytes,
                  PhoenixConnection connection) {
        MutationState state = new MutationState(maxSize, maxSizeBytes, connection,
                Collections.<TableRef, List<MultiRowMutationState>> emptyMap(), false, null);
        state.sizeOffset = 0;
        return state;
    }

    private void throwIfTooBig() throws SQLException {
        if (numRows > maxSize) {
            int mutationSize = numRows;
            resetState();
            throw new MaxMutationSizeExceededException(maxSize, mutationSize);
        }
        if (estimatedSize > maxSizeBytes) {
            long mutationSizeByte = estimatedSize;
            resetState();
            throw new MaxMutationSizeBytesExceededException(maxSizeBytes, mutationSizeByte);
        }
    }

    public long getUpdateCount() {
        return sizeOffset + numRows;
    }

    public int getNumRows() {
        return numRows;
    }

    private MultiRowMutationState getLastMutationBatch(Map<TableRef, List<MultiRowMutationState>> mutations, TableRef tableRef) {
        List<MultiRowMutationState> mutationBatches = mutations.get(tableRef);
        if (mutationBatches == null || mutationBatches.isEmpty()) {
            return null;
        }
        return mutationBatches.get(mutationBatches.size() - 1);
    }

    private void joinMutationState(TableRef tableRef, MultiRowMutationState srcRows,
        Map<TableRef, List<MultiRowMutationState>> dstMutations) {
        PTable table = tableRef.getTable();
        boolean isIndex = table.getType() == PTableType.INDEX;
        boolean incrementRowCount = dstMutations == this.mutationsMap;
        // we only need to check if the new mutation batch (srcRows) conflicts with the
        // last mutation batch since we try to merge it with that only
        MultiRowMutationState existingRows = getLastMutationBatch(dstMutations, tableRef);

        if (existingRows == null) { // no rows found for this table
            // Size new map at batch size as that's what it'll likely grow to.
            MultiRowMutationState newRows = new MultiRowMutationState(connection.getMutateBatchSize());
            newRows.putAll(srcRows);
            addMutations(dstMutations, tableRef, newRows);
            if (incrementRowCount && !isIndex) {
                numRows += srcRows.size();
                // if we added all the rows from newMutationState we can just increment the
                // estimatedSize by newMutationState.estimatedSize
                estimatedSize += srcRows.estimatedSize;
            }
            return;
        }

        // for conflicting rows
        MultiRowMutationState conflictingRows = new MultiRowMutationState(connection.getMutateBatchSize());

        // Rows for this table already exist, check for conflicts
        for (Map.Entry<ImmutableBytesPtr, RowMutationState> rowEntry : srcRows.entrySet()) {
            ImmutableBytesPtr key = rowEntry.getKey();
            RowMutationState newRowMutationState = rowEntry.getValue();
            RowMutationState existingRowMutationState = existingRows.get(key);
            if (existingRowMutationState == null) {
                existingRows.put(key, newRowMutationState);
                if (incrementRowCount && !isIndex) { // Don't count index rows in row count
                    numRows++;
                    // increment estimated size by the size of the new row
                    estimatedSize += newRowMutationState.calculateEstimatedSize();
                }
                continue;
            }
            Map<PColumn, byte[]> existingValues = existingRowMutationState.getColumnValues();
            Map<PColumn, byte[]> newValues = newRowMutationState.getColumnValues();
            if (existingValues != PRow.DELETE_MARKER && newValues != PRow.DELETE_MARKER) {
                // Check if we can merge existing column values with new column values
                long beforeMergeSize = existingRowMutationState.calculateEstimatedSize();
                boolean isMerged = existingRowMutationState.join(rowEntry.getValue());
                if (isMerged) {
                    // decrement estimated size by the size of the old row
                    estimatedSize -= beforeMergeSize;
                    // increment estimated size by the size of the new row
                    estimatedSize += existingRowMutationState.calculateEstimatedSize();
                } else {
                    // cannot merge regular upsert and conditional upsert
                    // conflicting row is not a new row so no need to increment numRows
                    conflictingRows.put(key, newRowMutationState);
                }
            } else {
                existingRows.put(key, newRowMutationState);
            }
        }

        if (!conflictingRows.isEmpty()) {
            addMutations(dstMutations, tableRef, conflictingRows);
        }
    }

    private void joinMutationState(Map<TableRef, List<MultiRowMutationState>> srcMutations,
            Map<TableRef, List<MultiRowMutationState>> dstMutations) {
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, List<MultiRowMutationState>> entry : srcMutations.entrySet()) {
            TableRef tableRef = entry.getKey();
            for (MultiRowMutationState srcRows : entry.getValue()) {
                // Replace existing entries for the table with new entries
                joinMutationState(tableRef, srcRows, dstMutations);
            }
        }
    }

    /**
     * Combine a newer mutation with this one, where in the event of overlaps, the newer one will take precedence.
     * Combine any metrics collected for the newer mutation.
     * 
     * @param newMutationState
     *            the newer mutation state
     */
    public void join(MutationState newMutationState) throws SQLException {
        if (this == newMutationState) { // Doesn't make sense
            return;
        }

        phoenixTransactionContext.join(newMutationState.getPhoenixTransactionContext());

        this.sizeOffset += newMutationState.sizeOffset;
        joinMutationState(newMutationState.mutationsMap, this.mutationsMap);
        if (!newMutationState.txMutations.isEmpty()) {
            if (txMutations.isEmpty()) {
                txMutations = Maps.newHashMapWithExpectedSize(this.mutationsMap.size());
            }
            joinMutationState(newMutationState.txMutations, this.txMutations);
        }
        mutationMetricQueue.combineMetricQueues(newMutationState.mutationMetricQueue);
        if (readMetricQueue == null) {
            readMetricQueue = newMutationState.readMetricQueue;
        } else if (readMetricQueue != null && newMutationState.readMetricQueue != null) {
            readMetricQueue.combineReadMetrics(newMutationState.readMetricQueue);
        }
        throwIfTooBig();
    }

    private static ImmutableBytesPtr getNewRowKeyWithRowTimestamp(ImmutableBytesPtr ptr, long rowTimestamp, PTable table) {
        RowKeySchema schema = table.getRowKeySchema();
        int rowTimestampColPos = table.getRowTimestampColPos();
        Field rowTimestampField = schema.getField(rowTimestampColPos);
        byte[] rowTimestampBytes = rowTimestampField.getDataType() == PTimestamp.INSTANCE ?
            PTimestamp.INSTANCE.toBytes(new Timestamp(rowTimestamp), rowTimestampField.getSortOrder()) :
            PLong.INSTANCE.toBytes(rowTimestamp, rowTimestampField.getSortOrder());
        int oldOffset = ptr.getOffset();
        int oldLength = ptr.getLength();
        // Move the pointer to the start byte of the row timestamp pk
        schema.position(ptr, 0, rowTimestampColPos);
        byte[] b = ptr.get();
        int newOffset = ptr.getOffset();
        int length = ptr.getLength();
        for (int i = newOffset; i < newOffset + length; i++) {
            // modify the underlying bytes array with the bytes of the row timestamp
            b[i] = rowTimestampBytes[i - newOffset];
        }
        // move the pointer back to where it was before.
        ptr.set(ptr.get(), oldOffset, oldLength);
        return ptr;
    }

    private Iterator<Pair<PTable, List<Mutation>>> addRowMutations(final TableRef tableRef,
            final MultiRowMutationState values, final long mutationTimestamp, final long serverTimestamp,
            boolean includeAllIndexes, final boolean sendAll) {
        final PTable table = tableRef.getTable();
        final List<PTable> indexList = includeAllIndexes ? 
                Lists.newArrayList(IndexMaintainer.maintainedIndexes(table.getIndexes().iterator())) : 
                    IndexUtil.getClientMaintainedIndexes(table);
        final Iterator<PTable> indexes = indexList.iterator();
        final List<Mutation> mutationList = Lists.newArrayListWithExpectedSize(values.size());
        final List<Mutation> mutationsPertainingToIndex = indexes.hasNext() ? Lists
                .newArrayListWithExpectedSize(values.size()) : null;
        generateMutations(tableRef, mutationTimestamp, serverTimestamp, values, mutationList,
                mutationsPertainingToIndex);
        return new Iterator<Pair<PTable, List<Mutation>>>() {
            boolean isFirst = true;
            Map<byte[], List<Mutation>> indexMutationsMap = null;

            @Override
            public boolean hasNext() {
                return isFirst || indexes.hasNext();
            }

            @Override
            public Pair<PTable, List<Mutation>> next() {
                if (isFirst) {
                    isFirst = false;
                    return new Pair<>(table, mutationList);
                }

                PTable index = indexes.next();

                List<Mutation> indexMutations = null;
                try {
                    if (!mutationsPertainingToIndex.isEmpty()) {
                        if (table.isTransactional()) {
                            if (indexMutationsMap == null) {
                                PhoenixTxIndexMutationGenerator generator = PhoenixTxIndexMutationGenerator.newGenerator(connection, table,
                                        indexList, mutationsPertainingToIndex.get(0).getAttributesMap());
                                try (Table htable = connection.getQueryServices().getTable(
                                        table.getPhysicalName().getBytes())) {
                                    Collection<Pair<Mutation, byte[]>> allMutations = generator.getIndexUpdates(htable,
                                            mutationsPertainingToIndex.iterator());
                                    indexMutationsMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                                    for (Pair<Mutation, byte[]> mutation : allMutations) {
                                        List<Mutation> mutations = indexMutationsMap.get(mutation.getSecond());
                                        if (mutations == null) {
                                            mutations = Lists.newArrayList();
                                            indexMutationsMap.put(mutation.getSecond(), mutations);
                                        }
                                        mutations.add(mutation.getFirst());
                                    }
                                }
                            }
                            indexMutations = indexMutationsMap.get(index.getPhysicalName().getBytes());
                        } else {
                            indexMutations = IndexUtil.generateIndexData(table, index, values,
                                    mutationsPertainingToIndex, connection.getKeyValueBuilder(), connection);
                        }
                    }

                    // we may also have to include delete mutations for immutable tables if we are not processing all
                    // the tables in the mutations map
                    if (!sendAll) {
                        TableRef key = new TableRef(index);
                        List<MultiRowMutationState> multiRowMutationState = mutationsMap.remove(key);
                        if (multiRowMutationState != null) {
                            final List<Mutation> deleteMutations = Lists.newArrayList();
                            // for index table there will only be 1 mutation batch in the list
                            generateMutations(key, mutationTimestamp, serverTimestamp, multiRowMutationState.get(0), deleteMutations, null);
                            if (indexMutations == null) {
                                indexMutations = deleteMutations;
                            } else {
                                indexMutations.addAll(deleteMutations);
                            }
                        }
                    }
                } catch (SQLException | IOException e) {
                    throw new IllegalDataException(e);
                }
                return new Pair<PTable, List<Mutation>>(index,
                        indexMutations == null ? Collections.<Mutation> emptyList()
                                : indexMutations);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    private void generateMutations(final TableRef tableRef, final long mutationTimestamp, final long serverTimestamp,
            final MultiRowMutationState values, final List<Mutation> mutationList,
            final List<Mutation> mutationsPertainingToIndex) {
        final PTable table = tableRef.getTable();
        boolean tableWithRowTimestampCol = table.getRowTimestampColPos() != -1;
        Iterator<Map.Entry<ImmutableBytesPtr, RowMutationState>> iterator = values.entrySet().iterator();
        long timestampToUse = mutationTimestamp;
        MultiRowMutationState modifiedValues = new MultiRowMutationState(16);
        boolean wildcardIncludesDynamicCols = connection.getQueryServices().getProps().getBoolean(
                WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB, DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB);
        while (iterator.hasNext()) {
            Map.Entry<ImmutableBytesPtr, RowMutationState> rowEntry = iterator.next();
            byte[] onDupKeyBytes = rowEntry.getValue().getOnDupKeyBytes();
            boolean hasOnDupKey = onDupKeyBytes != null;
            ImmutableBytesPtr key = rowEntry.getKey();
            RowMutationState state = rowEntry.getValue();
            if (tableWithRowTimestampCol) {
                RowTimestampColInfo rowTsColInfo = state.getRowTimestampColInfo();
                if (rowTsColInfo.useServerTimestamp()) {
                    // regenerate the key with this timestamp.
                    key = getNewRowKeyWithRowTimestamp(key, serverTimestamp, table);
                    // since we are about to modify the byte[] stored in key (which changes its hashcode)
                    // we need to remove the entry from the values map and add a new entry with the modified byte[]
                    modifiedValues.put(key, state);
                    iterator.remove();
                    timestampToUse = serverTimestamp;
                } else {
                    if (rowTsColInfo.getTimestamp() != null) {
                        timestampToUse = rowTsColInfo.getTimestamp();
                    }
                }
            }
            PRow row = table.newRow(connection.getKeyValueBuilder(), timestampToUse, key, hasOnDupKey);
            List<Mutation> rowMutations, rowMutationsPertainingToIndex;
            if (rowEntry.getValue().getColumnValues() == PRow.DELETE_MARKER) { // means delete
                row.delete();
                rowMutations = row.toRowMutations();
                String sourceOfDelete = getConnection().getSourceOfOperation();
                if (sourceOfDelete != null) {
                    byte[] sourceOfDeleteBytes = Bytes.toBytes(sourceOfDelete);
                    // Set the source of operation attribute.
                    for (Mutation mutation: rowMutations) {
                        mutation.setAttribute(SOURCE_OPERATION_ATTRIB, sourceOfDeleteBytes);
                    }
                }
                // The DeleteCompiler already generates the deletes for indexes, so no need to do it again
                rowMutationsPertainingToIndex = Collections.emptyList();

            } else {
                for (Map.Entry<PColumn, byte[]> valueEntry : rowEntry.getValue().getColumnValues().entrySet()) {
                    row.setValue(valueEntry.getKey(), valueEntry.getValue());
                }
                if (wildcardIncludesDynamicCols && row.setAttributesForDynamicColumnsIfReqd()) {
                    row.setAttributeToProcessDynamicColumnsMetadata();
                }
                rowMutations = row.toRowMutations();
                // Pass through ON DUPLICATE KEY info through mutations
                // In the case of the same clause being used on many statements, this will be
                // inefficient because we're transmitting the same information for each mutation.
                // TODO: use our ServerCache
                for (Mutation mutation : rowMutations) {
                    if (onDupKeyBytes != null) {
                        mutation.setAttribute(PhoenixIndexBuilder.ATOMIC_OP_ATTRIB, onDupKeyBytes);
                    }
                }
                rowMutationsPertainingToIndex = rowMutations;
            }
            annotateMutationsWithMetadata(table, rowMutations);
            mutationList.addAll(rowMutations);
            if (mutationsPertainingToIndex != null) mutationsPertainingToIndex.addAll(rowMutationsPertainingToIndex);
        }
        values.putAll(modifiedValues);
    }

    private void annotateMutationsWithMetadata(PTable table, List<Mutation> rowMutations) {
        //only annotate if the change detection flag is on the table and HBase supports
        // preWALAppend coprocs server-side
        if (table == null || !table.isChangeDetectionEnabled()
            || !HbaseCompatCapabilities.hasPreWALAppend()) {
            return;
        }
        //annotate each mutation with enough metadata so that anyone interested can
        // deterministically figure out exactly what Phoenix schema object created the mutation
        // Server-side we can annotate the HBase WAL with these.
        for (Mutation mutation : rowMutations) {
            annotateMutationWithMetadata(table, mutation);
        }

    }

    private void annotateMutationWithMetadata(PTable table, Mutation mutation) {
        byte[] tenantId = table.getTenantId() != null ? table.getTenantId().getBytes() : null;
        byte[] schemaName = table.getSchemaName() != null ? table.getSchemaName().getBytes() : null;
        byte[] tableName = table.getTableName() != null ? table.getTableName().getBytes() : null;
        byte[] tableType = table.getType().getValue().getBytes();
        //Note that we use the _HBase_ byte encoding for a Long, not the Phoenix one, so that
        //downstream consumers don't need to have the Phoenix codecs.
        byte[] lastDDLTimestamp =
            table.getLastDDLTimestamp() != null ? Bytes.toBytes(table.getLastDDLTimestamp()) : null;
        WALAnnotationUtil.annotateMutation(mutation, tenantId, schemaName,
            tableName, tableType, lastDDLTimestamp);
    }

    /**
     * Get the unsorted list of HBase mutations for the tables with uncommitted data.
     * 
     * @return list of HBase mutations for uncommitted data.
     */
    public Iterator<Pair<byte[], List<Mutation>>> toMutations(Long timestamp) {
        return toMutations(false, timestamp);
    }

    public Iterator<Pair<byte[], List<Mutation>>> toMutations() {
        return toMutations(false, null);
    }

    public Iterator<Pair<byte[], List<Mutation>>> toMutations(final boolean includeMutableIndexes) {
        return toMutations(includeMutableIndexes, null);
    }

    public Iterator<Pair<byte[], List<Mutation>>> toMutations(final boolean includeMutableIndexes,
            final Long tableTimestamp) {
        final Iterator<Map.Entry<TableRef, List<MultiRowMutationState>>> iterator = this.mutationsMap.entrySet().iterator();
        if (!iterator.hasNext()) { return Collections.emptyIterator(); }
        Long scn = connection.getSCN();
        final long serverTimestamp = getTableTimestamp(tableTimestamp, scn);
        final long mutationTimestamp = getMutationTimestamp(scn);
        return new Iterator<Pair<byte[], List<Mutation>>>() {
            private Map.Entry<TableRef, List<MultiRowMutationState>> current = iterator.next();
            private int batchOffset = 0;
            private Iterator<Pair<byte[], List<Mutation>>> innerIterator = init();

            private Iterator<Pair<byte[], List<Mutation>>> init() {
                final Iterator<Pair<PTable, List<Mutation>>> mutationIterator =
                    addRowMutations(current.getKey(), current.getValue().get(batchOffset),
                        mutationTimestamp, serverTimestamp, includeMutableIndexes, true);

                return new Iterator<Pair<byte[], List<Mutation>>>() {
                    @Override
                    public boolean hasNext() {
                        return mutationIterator.hasNext();
                    }

                    @Override
                    public Pair<byte[], List<Mutation>> next() {
                        Pair<PTable, List<Mutation>> pair = mutationIterator.next();
                        return new Pair<byte[], List<Mutation>>(pair.getFirst().getPhysicalName()
                                .getBytes(), pair.getSecond());
                    }

                    @Override
                    public void remove() {
                        mutationIterator.remove();
                    }
                };
            }

            @Override
            public boolean hasNext() {
                return innerIterator.hasNext() ||
                    batchOffset + 1 < current.getValue().size() ||
                    iterator.hasNext();
            }

            @Override
            public Pair<byte[], List<Mutation>> next() {
                if (!innerIterator.hasNext()) {
                    ++batchOffset;
                    if (batchOffset == current.getValue().size()) {
                        current = iterator.next();
                        batchOffset = 0;
                    }
                    innerIterator = init();
                }
                return innerIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    public static long getTableTimestamp(final Long tableTimestamp, Long scn) {
        return (tableTimestamp != null && tableTimestamp != QueryConstants.UNSET_TIMESTAMP) ? tableTimestamp
                : (scn == null ? HConstants.LATEST_TIMESTAMP : scn);
    }

    public static long getMutationTimestamp(final Long scn) {
        return scn == null ? HConstants.LATEST_TIMESTAMP : scn;
    }

    /**
     * Validates that the meta data is valid against the server meta data if we haven't yet done so. Otherwise, for
     * every UPSERT VALUES call, we'd need to hit the server to see if the meta data has changed.
     * 
     * @return the server time to use for the upsert
     * @throws SQLException
     *             if the table or any columns no longer exist
     */
    private long[] validateAll(Map<TableRef, MultiRowMutationState> commitBatch) throws SQLException {
        int i = 0;
        long[] timeStamps = new long[commitBatch.size()];
        for (Map.Entry<TableRef, MultiRowMutationState> entry : commitBatch.entrySet()) {
            TableRef tableRef = entry.getKey();
            timeStamps[i++] = validateAndGetServerTimestamp(tableRef, entry.getValue());
        }
        return timeStamps;
    }

    private long validateAndGetServerTimestamp(TableRef tableRef, MultiRowMutationState rowKeyToColumnMap)
            throws SQLException {
        MetaDataClient client = new MetaDataClient(connection);
        long serverTimeStamp = tableRef.getTimeStamp();
        // If we're auto committing, we've already validated the schema when we got the ColumnResolver,
        // so no need to do it again here.
        PTable table = tableRef.getTable();

        // We generally don't re-resolve SYSTEM tables, but if it relies on ROW_TIMESTAMP, we must
        // get the latest timestamp in order to upsert data with the correct server-side timestamp
        // in case the ROW_TIMESTAMP is not provided in the UPSERT statement.
        boolean hitServerForLatestTimestamp =
                table.getRowTimestampColPos() != -1 && table.getType() == PTableType.SYSTEM;
        MetaDataMutationResult result = client.updateCache(table.getSchemaName().getString(),
                table.getTableName().getString(), hitServerForLatestTimestamp);
        PTable resolvedTable = result.getTable();
        if (resolvedTable == null) { throw new TableNotFoundException(table.getSchemaName().getString(), table
                .getTableName().getString()); }
        // Always update tableRef table as the one we've cached may be out of date since when we executed
        // the UPSERT VALUES call and updated in the cache before this.
        tableRef.setTable(resolvedTable);
        List<PTable> indexes = resolvedTable.getIndexes();
        for (PTable idxTtable : indexes) {
            // If index is still active, but has a non zero INDEX_DISABLE_TIMESTAMP value, then infer that
            // our failure mode is block writes on index failure.
            if ((idxTtable.getIndexState() == PIndexState.ACTIVE || idxTtable.getIndexState() == PIndexState.PENDING_ACTIVE)
                    && idxTtable.getIndexDisableTimestamp() > 0) { throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.INDEX_FAILURE_BLOCK_WRITE).setSchemaName(table.getSchemaName().getString())
                    .setTableName(table.getTableName().getString()).build().buildException(); }
        }
        long timestamp = result.getMutationTime();
        if (timestamp != QueryConstants.UNSET_TIMESTAMP) {
            serverTimeStamp = timestamp;
            if (result.wasUpdated()) {
                List<PColumn> columns = Lists.newArrayListWithExpectedSize(table.getColumns().size());
                for (Map.Entry<ImmutableBytesPtr, RowMutationState> rowEntry : rowKeyToColumnMap.entrySet()) {
                    RowMutationState valueEntry = rowEntry.getValue();
                    if (valueEntry != null) {
                        Map<PColumn, byte[]> colValues = valueEntry.getColumnValues();
                        if (colValues != PRow.DELETE_MARKER) {
                            for (PColumn column : colValues.keySet()) {
                                if (!column.isDynamic()) columns.add(column);
                            }
                        }
                    }
                }
                for (PColumn column : columns) {
                    if (column != null) {
                        resolvedTable.getColumnFamily(column.getFamilyName().getString()).getPColumnForColumnName(
                                column.getName().getString());
                    }
                }
            }
        }
        return serverTimeStamp == QueryConstants.UNSET_TIMESTAMP ? HConstants.LATEST_TIMESTAMP : serverTimeStamp;
    }

    private static long calculateMutationSize(List<Mutation> mutations) {
        long byteSize = 0;
        if (GlobalClientMetrics.isMetricsEnabled()) {
            for (Mutation mutation : mutations) {
                byteSize += PhoenixKeyValueUtil.calculateMutationDiskSize(mutation);
            }
        }
        GLOBAL_MUTATION_BYTES.update(byteSize);
        return byteSize;
    }

    public long getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public enum MutationMetadataType {
        TENANT_ID,
        SCHEMA_NAME,
        LOGICAL_TABLE_NAME,
        TIMESTAMP,
        TABLE_TYPE
    }

    private static class TableInfo {

        private final boolean isDataTable;
        @Nonnull
        private final PName hTableName;
        @Nonnull
        private final TableRef origTableRef;
        private final PTable pTable;

        public TableInfo(boolean isDataTable, PName hTableName, TableRef origTableRef, PTable pTable) {
            super();
            checkNotNull(hTableName);
            checkNotNull(origTableRef);
            this.isDataTable = isDataTable;
            this.hTableName = hTableName;
            this.origTableRef = origTableRef;
            this.pTable = pTable;
        }

        public boolean isDataTable() {
            return isDataTable;
        }

        public PName getHTableName() {
            return hTableName;
        }

        public TableRef getOrigTableRef() {
            return origTableRef;
        }

        public PTable getPTable() {
            return pTable;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + hTableName.hashCode();
            result = prime * result + (isDataTable ? 1231 : 1237);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            TableInfo other = (TableInfo)obj;
            if (!hTableName.equals(other.hTableName)) return false;
            if (isDataTable != other.isDataTable) return false;
            if (!pTable.equals(other.pTable)) return false;

            return true;
        }

    }

    /**
     * Split the mutation batches for each table into separate commit batches.
     * Each commit batch contains only one mutation batch (MultiRowMutationState) for a table.
     * @param tableRefIterator
     * @return List of commit batches
     */
    private List<Map<TableRef, MultiRowMutationState>> createCommitBatches(Iterator<TableRef> tableRefIterator) {
        List<Map<TableRef, MultiRowMutationState>> commitBatches = Lists.newArrayList();
        while (tableRefIterator.hasNext()) {
            final TableRef tableRef = tableRefIterator.next();
            List<MultiRowMutationState> batches = this.mutationsMap.get(tableRef);
            if (batches == null) {
                continue;
            }
            for (MultiRowMutationState batch : batches) {
                // get the first commit batch which doesn't have any mutations for the table
                Map<TableRef, MultiRowMutationState> nextCommitBatch = getNextCommitBatchForTable(commitBatches, tableRef);
                // add the next mutation batch of the table to the commit batch
                nextCommitBatch.put(tableRef, batch);
            }
        }
        return commitBatches;
    }

    // visible for testing
    List<Map<TableRef, MultiRowMutationState>> createCommitBatches() {
        return createCommitBatches(this.mutationsMap.keySet().iterator());
    }

    /**
     * Return the first commit batch which doesn't have any mutations for the passed table.
     * If no such commit batch exists, creates a new commit batch, adds it to the list of
     * commit batches and returns it.
     * @param commitBatchesList current list of commit batches
     * @param tableRef
     * @return commit batch
     */
    private Map<TableRef, MultiRowMutationState> getNextCommitBatchForTable(List<Map<TableRef, MultiRowMutationState>> commitBatchesList,
        TableRef tableRef) {
        Map<TableRef, MultiRowMutationState> nextCommitBatch = null;
        for (Map<TableRef, MultiRowMutationState> commitBatch : commitBatchesList) {
            if (commitBatch.get(tableRef) == null) {
                nextCommitBatch = commitBatch;
                break;
            }
        }
        if (nextCommitBatch == null) {
            // create a new commit batch and add it to the list of commit batches
            nextCommitBatch = Maps.newHashMapWithExpectedSize(this.mutationsMap.size());
            commitBatchesList.add(nextCommitBatch);
        }
        return nextCommitBatch;
    }

    private void send(Iterator<TableRef> tableRefIterator) throws SQLException {
        boolean sendAll = false;
        boolean validateServerTimestamps = false;
        List<Map<TableRef, MultiRowMutationState>> commitBatches;
        if (tableRefIterator == null) {
            commitBatches = createCommitBatches(this.mutationsMap.keySet().iterator());
            sendAll = true;
            validateServerTimestamps = true;
        } else {
            commitBatches = createCommitBatches(tableRefIterator);
        }

        for (Map<TableRef, MultiRowMutationState> commitBatch : commitBatches) {
            long [] serverTimestamps = validateServerTimestamps ? validateAll(commitBatch) : null;
            sendBatch(commitBatch, serverTimestamps, sendAll);
        }
    }

    private void sendBatch(Map<TableRef, MultiRowMutationState> commitBatch, long[] serverTimeStamps, boolean sendAll) throws SQLException {
        int i = 0;
        Map<TableInfo, List<Mutation>> physicalTableMutationMap = Maps.newLinkedHashMap();
        // add tracing for this operation
        try (TraceScope trace = Tracing.startNewSpan(connection, "Committing mutations to tables")) {
            Span span = trace.getSpan();
            ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable();
            for (Map.Entry<TableRef, MultiRowMutationState> entry : commitBatch.entrySet()) {
                // at this point we are going through mutations for each table
                final TableRef tableRef = entry.getKey();
                MultiRowMutationState multiRowMutationState = entry.getValue();
                if (multiRowMutationState == null || multiRowMutationState.isEmpty()) {
                    continue;
                }
                // Validate as we go if transactional since we can undo if a problem occurs (which is unlikely)
                long
                    serverTimestamp =
                    serverTimeStamps == null ?
                        validateAndGetServerTimestamp(tableRef, multiRowMutationState) :
                        serverTimeStamps[i++];
                final PTable table = tableRef.getTable();
                Long scn = connection.getSCN();
                long mutationTimestamp = scn == null ?
                    (table.isTransactional() == true ? HConstants.LATEST_TIMESTAMP : EnvironmentEdgeManager.currentTimeMillis())
                    : scn;
                Iterator<Pair<PTable, List<Mutation>>>
                    mutationsIterator =
                    addRowMutations(tableRef, multiRowMutationState, mutationTimestamp,
                        serverTimestamp, false, sendAll);
                // build map from physical table to mutation list
                boolean isDataTable = true;
                while (mutationsIterator.hasNext()) {
                    Pair<PTable, List<Mutation>> pair = mutationsIterator.next();
                    PTable logicalTable = pair.getFirst();
                    List<Mutation> mutationList = pair.getSecond();

                    TableInfo tableInfo = new TableInfo(isDataTable, logicalTable.getPhysicalName(),
                            tableRef, logicalTable);

                    List<Mutation>
                        oldMutationList =
                        physicalTableMutationMap.put(tableInfo, mutationList);
                    if (oldMutationList != null) mutationList.addAll(0, oldMutationList);
                    isDataTable = false;
                }
                // For transactions, track the statement indexes as we send data
                // over because our CommitException should include all statements
                // involved in the transaction since none of them would have been
                // committed in the event of a failure.
                if (table.isTransactional()) {
                    addUncommittedStatementIndexes(multiRowMutationState.values());
                    if (txMutations.isEmpty()) {
                        txMutations = Maps.newHashMapWithExpectedSize(this.mutationsMap.size());
                    }
                    // Keep all mutations we've encountered until a commit or rollback.
                    // This is not ideal, but there's not good way to get the values back
                    // in the event that we need to replay the commit.
                    // Copy TableRef so we have the original PTable and know when the
                    // indexes have changed.
                    joinMutationState(new TableRef(tableRef), multiRowMutationState, txMutations);
                }
            }

            Map<TableInfo, List<Mutation>> unverifiedIndexMutations = new LinkedHashMap<>();
            Map<TableInfo, List<Mutation>> verifiedOrDeletedIndexMutations = new LinkedHashMap<>();
            filterIndexCheckerMutations(physicalTableMutationMap, unverifiedIndexMutations,
                    verifiedOrDeletedIndexMutations);

            // Phase 1: Send index mutations with the empty column value = "unverified"
            sendMutations(unverifiedIndexMutations.entrySet().iterator(), span, indexMetaDataPtr, false);

            // Phase 2: Send data table and other indexes
            sendMutations(physicalTableMutationMap.entrySet().iterator(), span, indexMetaDataPtr, false);

            // Phase 3: Send put index mutations with the empty column value = "verified" and/or delete index mutations
            try {
                sendMutations(verifiedOrDeletedIndexMutations.entrySet().iterator(), span, indexMetaDataPtr, true);
            } catch (SQLException ex) {
                LOGGER.warn(
                        "Ignoring exception that happened during setting index verified value to verified=TRUE ",
                        ex);
            }
        }
    }

    private void sendMutations(Iterator<Entry<TableInfo, List<Mutation>>> mutationsIterator, Span span, ImmutableBytesWritable indexMetaDataPtr, boolean isVerifiedPhase)
            throws SQLException {
        while (mutationsIterator.hasNext()) {
            Entry<TableInfo, List<Mutation>> pair = mutationsIterator.next();
            TableInfo tableInfo = pair.getKey();
            byte[] htableName = tableInfo.getHTableName().getBytes();
            List<Mutation> mutationList = pair.getValue();
            List<List<Mutation>> mutationBatchList =
                    getMutationBatchList(batchSize, batchSizeBytes, mutationList);

            // create a span per target table
            // TODO maybe we can be smarter about the table name to string here?
            Span child = Tracing.child(span, "Writing mutation batch for table: " + Bytes.toString(htableName));

            int retryCount = 0;
            boolean shouldRetry = false;
            long numMutations = 0;
            long mutationSizeBytes = 0;
            long mutationCommitTime = 0;
            long numFailedMutations = 0;
            long numFailedPhase3Mutations = 0;

            long startTime = 0;
            boolean shouldRetryIndexedMutation = false;
            IndexWriteException iwe = null;
            do {
                TableRef origTableRef = tableInfo.getOrigTableRef();
                PTable table = origTableRef.getTable();
                table.getIndexMaintainers(indexMetaDataPtr, connection);
                final ServerCache cache = tableInfo.isDataTable() ?
                        IndexMetaDataCacheClient.setMetaDataOnMutations(connection, table,
                                mutationList, indexMetaDataPtr) : null;
                // If we haven't retried yet, retry for this case only, as it's possible that
                // a split will occur after we send the index metadata cache to all known
                // region servers.
                shouldRetry = cache != null;
                SQLException sqlE = null;
                Table hTable = connection.getQueryServices().getTable(htableName);
                try {
                    if (table.isTransactional()) {
                        // Track tables to which we've sent uncommitted data
                        if (tableInfo.isDataTable()) {
                            uncommittedPhysicalNames.add(table.getPhysicalName().getString());
                            phoenixTransactionContext.markDMLFence(table);
                        }
                        // Only pass true for last argument if the index is being written to on it's own (i.e. initial
                        // index population), not if it's being written to for normal maintenance due to writes to
                        // the data table. This case is different because the initial index population does not need
                        // to be done transactionally since the index is only made active after all writes have
                        // occurred successfully.
                        hTable = phoenixTransactionContext.getTransactionalTableWriter(connection, table, hTable, tableInfo.isDataTable() && table.getType() == PTableType.INDEX);
                    }
                    numMutations = mutationList.size();
                    GLOBAL_MUTATION_BATCH_SIZE.update(numMutations);
                    mutationSizeBytes = calculateMutationSize(mutationList);

                    startTime = EnvironmentEdgeManager.currentTimeMillis();
                    child.addTimelineAnnotation("Attempt " + retryCount);
                    Iterator<List<Mutation>> itrListMutation = mutationBatchList.iterator();
                    while (itrListMutation.hasNext()) {
                        final List<Mutation> mutationBatch = itrListMutation.next();
                        if (shouldRetryIndexedMutation) {
                            // if there was an index write failure, retry the mutation in a loop
                            final Table finalHTable = hTable;
                            final ImmutableBytesWritable finalindexMetaDataPtr =
                                    indexMetaDataPtr;
                            final PTable finalPTable = table;
                            PhoenixIndexFailurePolicy.doBatchWithRetries(new MutateCommand() {
                                @Override
                                public void doMutation() throws IOException {
                                    try {
                                        finalHTable.batch(mutationBatch, null);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new IOException(e);
                                    } catch (IOException e) {
                                        e = updateTableRegionCacheIfNecessary(e);
                                        throw e;
                                    }
                                }

                                @Override
                                public List<Mutation> getMutationList() {
                                    return mutationBatch;
                                }

                                private IOException
                                updateTableRegionCacheIfNecessary(IOException ioe) {
                                    SQLException sqlE =
                                            ServerUtil.parseLocalOrRemoteServerException(ioe);
                                    if (sqlE != null
                                            && sqlE.getErrorCode() == SQLExceptionCode.INDEX_METADATA_NOT_FOUND
                                            .getErrorCode()) {
                                        try {
                                            connection.getQueryServices().clearTableRegionCache(
                                                    finalHTable.getName());
                                            IndexMetaDataCacheClient.setMetaDataOnMutations(
                                                    connection, finalPTable, mutationBatch,
                                                    finalindexMetaDataPtr);
                                        } catch (SQLException e) {
                                            return ServerUtil.createIOException(
                                                    "Exception during updating index meta data cache",
                                                    ioe);
                                        }
                                    }
                                    return ioe;
                                }
                            }, iwe, connection, connection.getQueryServices().getProps());
                            shouldRetryIndexedMutation = false;
                        } else {
                            hTable.batch(mutationBatch, null);
                        }
                        // remove each batch from the list once it gets applied
                        // so when failures happens for any batch we only start
                        // from that batch only instead of doing duplicate reply of already
                        // applied batches from entire list, also we can set
                        // REPLAY_ONLY_INDEX_WRITES for first batch
                        // only in case of 1121 SQLException
                        itrListMutation.remove();

                        batchCount++;
                        if (LOGGER.isDebugEnabled())
                            LOGGER.debug("Sent batch of " + mutationBatch.size() + " for "
                                    + Bytes.toString(htableName));
                    }
                    child.stop();
                    child.stop();
                    shouldRetry = false;
                    mutationCommitTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                    GLOBAL_MUTATION_COMMIT_TIME.update(mutationCommitTime);
                    numFailedMutations = 0;

                    // Remove batches as we process them
                    removeMutations(this.mutationsMap, origTableRef);
                    if (tableInfo.isDataTable()) {
                        numRows -= numMutations;
                        // recalculate the estimated size
                        estimatedSize = PhoenixKeyValueUtil.getEstimatedRowMutationSizeWithBatch(this.mutationsMap);
                    }
                } catch (Exception e) {
                    mutationCommitTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                    long serverTimestamp = ServerUtil.parseServerTimestamp(e);
                    SQLException inferredE = ServerUtil.parseServerExceptionOrNull(e);
                    if (inferredE != null) {
                        if (shouldRetry
                                && retryCount == 0
                                && inferredE.getErrorCode() == SQLExceptionCode.INDEX_METADATA_NOT_FOUND
                                .getErrorCode()) {
                            // Swallow this exception once, as it's possible that we split after sending the index
                            // metadata
                            // and one of the region servers doesn't have it. This will cause it to have it the next
                            // go around.
                            // If it fails again, we don't retry.
                            String msg = "Swallowing exception and retrying after clearing meta cache on connection. "
                                    + inferredE;
                            LOGGER.warn(LogUtil.addCustomAnnotations(msg, connection));
                            connection.getQueryServices().clearTableRegionCache(TableName.valueOf(htableName));

                            // add a new child span as this one failed
                            child.addTimelineAnnotation(msg);
                            child.stop();
                            child = Tracing.child(span, "Failed batch, attempting retry");

                            continue;
                        } else if (inferredE.getErrorCode() == SQLExceptionCode.INDEX_WRITE_FAILURE.getErrorCode()) {
                            iwe = PhoenixIndexFailurePolicy.getIndexWriteException(inferredE);
                            if (iwe != null && !shouldRetryIndexedMutation) {
                                // For an index write failure, the data table write succeeded,
                                // so when we retry we need to set REPLAY_WRITES
                                // for first batch in list only.
                                for (Mutation m : mutationBatchList.get(0)) {
                                    if (!PhoenixIndexMetaData.isIndexRebuild(
                                            m.getAttributesMap())){
                                        m.setAttribute(BaseScannerRegionObserver.REPLAY_WRITES,
                                                BaseScannerRegionObserver.REPLAY_ONLY_INDEX_WRITES
                                        );
                                    }
                                    PhoenixKeyValueUtil.setTimestamp(m, serverTimestamp);
                                }
                                shouldRetry = true;
                                shouldRetryIndexedMutation = true;
                                continue;
                            }
                        }
                        e = inferredE;
                    }
                    // Throw to client an exception that indicates the statements that
                    // were not committed successfully.
                    int[] uncommittedStatementIndexes = getUncommittedStatementIndexes();
                    sqlE = new CommitException(e, uncommittedStatementIndexes, serverTimestamp);
                    numFailedMutations = uncommittedStatementIndexes.length;
                    GLOBAL_MUTATION_BATCH_FAILED_COUNT.update(numFailedMutations);
                    if (isVerifiedPhase) {
                        numFailedPhase3Mutations = numFailedMutations;
                        GLOBAL_MUTATION_INDEX_COMMIT_FAILURE_COUNT.update(numFailedPhase3Mutations);
                    }
                } finally {
                    MutationMetric mutationsMetric = new MutationMetric(numMutations, mutationSizeBytes,
                            mutationCommitTime, numFailedMutations, numFailedPhase3Mutations);
                    mutationMetricQueue.addMetricsForTable(Bytes.toString(htableName), mutationsMetric);
                    try {
                        if (cache != null) cache.close();
                    } finally {
                        try {
                            hTable.close();
                        } catch (IOException e) {
                            if (sqlE != null) {
                                sqlE.setNextException(ServerUtil.parseServerException(e));
                            } else {
                                sqlE = ServerUtil.parseServerException(e);
                            }
                        }
                        if (sqlE != null) { throw sqlE; }
                    }
                }
            } while (shouldRetry && retryCount++ < 1);
        }
    }

    private void filterIndexCheckerMutations(Map<TableInfo, List<Mutation>> mutationMap,
            Map<TableInfo, List<Mutation>> unverifiedIndexMutations,
            Map<TableInfo, List<Mutation>> verifiedOrDeletedIndexMutations) throws SQLException {
        Iterator<Entry<TableInfo, List<Mutation>>> mapIter = mutationMap.entrySet().iterator();

        while (mapIter.hasNext()) {
            Entry<TableInfo, List<Mutation>> pair = mapIter.next();
            TableInfo tableInfo = pair.getKey();
            if (!tableInfo.getPTable().getType().equals(PTableType.INDEX)) {
                continue;
            }
            PTable logicalTable = tableInfo.getPTable();
            if (tableInfo.getOrigTableRef().getTable().isImmutableRows()
                    && IndexUtil.isGlobalIndexCheckerEnabled(connection,
                    tableInfo.getHTableName())) {

                byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(logicalTable);
                byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(logicalTable).getFirst();
                List<Mutation> mutations = pair.getValue();

                for (Mutation m : mutations) {
                    if (m == null) {
                        continue;
                    }
                    if (m instanceof Delete) {
                        Put put = new Put(m.getRow());
                        put.addColumn(emptyCF, emptyCQ, IndexRegionObserver.getMaxTimestamp(m),
                                QueryConstants.UNVERIFIED_BYTES);
                        // The Delete gets marked as unverified in Phase 1 and gets deleted on Phase 3.
                        addToMap(unverifiedIndexMutations, tableInfo, put);
                        addToMap(verifiedOrDeletedIndexMutations, tableInfo, m);
                    } else if (m instanceof Put) {
                        long timestamp = IndexRegionObserver.getMaxTimestamp(m);

                        // Phase 1 index mutations are set to unverified
                        // Send entire mutation with the unverified status
                        // Remove the empty column prepared by Index codec as we need to change its value
                        IndexRegionObserver.removeEmptyColumn(m, emptyCF, emptyCQ);
                        ((Put) m).addColumn(emptyCF, emptyCQ, timestamp,
                                QueryConstants.UNVERIFIED_BYTES);
                        addToMap(unverifiedIndexMutations, tableInfo, m);

                        // Phase 3 mutations are verified
                        Put verifiedPut = new Put(m.getRow());
                        verifiedPut.addColumn(emptyCF, emptyCQ, timestamp,
                                 QueryConstants.VERIFIED_BYTES);
                        addToMap(verifiedOrDeletedIndexMutations, tableInfo, verifiedPut);
                    } else {
                        addToMap(unverifiedIndexMutations, tableInfo, m);
                    }
                }

                mapIter.remove();
            }

        }
    }

    private void addToMap(Map<TableInfo, List<Mutation>> map, TableInfo tableInfo, Mutation mutation) {
        List<Mutation> mutations = null;
        if (map.containsKey(tableInfo)) {
            mutations = map.get(tableInfo);
        } else {
            mutations = Lists.newArrayList();
        }
        mutations.add(mutation);
        map.put(tableInfo, mutations);
    }

    /**
     *
     * Split the list of mutations into multiple lists. since a single row update can contain multiple mutations,
     * we only check if the current batch has exceeded the row or size limit for different rows,
     * so that mutations for a single row don't end up in different batches.
     *
     * @param allMutationList
     *            List of HBase mutations
     * @return List of lists of mutations
     */
    public static List<List<Mutation>> getMutationBatchList(long batchSize, long batchSizeBytes, List<Mutation> allMutationList) {
        Preconditions.checkArgument(batchSize> 1,
                "Mutation types are put or delete, for one row all mutations must be in one batch.");
        Preconditions.checkArgument(batchSizeBytes > 0, "Batch size must be larger than 0");
        List<List<Mutation>> mutationBatchList = Lists.newArrayList();
        List<Mutation> currentList = Lists.newArrayList();
        List<Mutation> sameRowList = Lists.newArrayList();
        long currentBatchSizeBytes = 0L;
        for (int i = 0; i < allMutationList.size(); ) {
            long sameRowBatchSize = 1L;
            Mutation mutation = allMutationList.get(i);
            long sameRowMutationSizeBytes = PhoenixKeyValueUtil.calculateMutationDiskSize(mutation);
            sameRowList.add(mutation);
            while (i + 1 < allMutationList.size() &&
                    Bytes.compareTo(allMutationList.get(i + 1).getRow(), mutation.getRow()) == 0) {
                Mutation sameRowMutation = allMutationList.get(i + 1);
                sameRowList.add(sameRowMutation);
                sameRowMutationSizeBytes += PhoenixKeyValueUtil.calculateMutationDiskSize(sameRowMutation);
                sameRowBatchSize++;
                i++;
            }

            if (currentList.size() + sameRowBatchSize > batchSize ||
                    currentBatchSizeBytes + sameRowMutationSizeBytes > batchSizeBytes) {
                if (currentList.size() > 0) {
                    mutationBatchList.add(currentList);
                    currentList = Lists.newArrayList();
                    currentBatchSizeBytes = 0L;
                }
            }

            currentList.addAll(sameRowList);
            currentBatchSizeBytes += sameRowMutationSizeBytes;
            sameRowList.clear();
            i++;
        }

        if (currentList.size() > 0) {
            mutationBatchList.add(currentList);
        }
        return mutationBatchList;
    }

    public byte[] encodeTransaction() throws SQLException {
        return phoenixTransactionContext.encodeTransaction();
    }

    private void addUncommittedStatementIndexes(Collection<RowMutationState> rowMutations) {
        for (RowMutationState rowMutationState : rowMutations) {
            uncommittedStatementIndexes = joinSortedIntArrays(uncommittedStatementIndexes,
                    rowMutationState.getStatementIndexes());
        }
    }

    private int[] getUncommittedStatementIndexes() {
        for (List<MultiRowMutationState> batches : mutationsMap.values()) {
            for (MultiRowMutationState rowMutationMap : batches) {
                addUncommittedStatementIndexes(rowMutationMap.values());
            }
        }
        return uncommittedStatementIndexes;
    }

    @Override
    public void close() throws SQLException {}

    private void resetState() {
        numRows = 0;
        estimatedSize = 0;
        this.mutationsMap.clear();
        phoenixTransactionContext = PhoenixTransactionContext.NULL_CONTEXT;
    }

    private void resetTransactionalState() {
        phoenixTransactionContext.reset();
        txMutations = Collections.emptyMap();
        uncommittedPhysicalNames.clear();
        uncommittedStatementIndexes = EMPTY_STATEMENT_INDEX_ARRAY;
    }

    public void rollback() throws SQLException {
        try {
            phoenixTransactionContext.abort();
        } finally {
            resetState();
        }
    }

    public void commit() throws SQLException {
        Map<TableRef, List<MultiRowMutationState>> txMutations = Collections.emptyMap();
        int retryCount = 0;
        do {
            boolean sendSuccessful = false;
            boolean retryCommit = false;
            SQLException sqlE = null;
            try {
                send();
                txMutations = this.txMutations;
                sendSuccessful = true;
            } catch (SQLException e) {
                sqlE = e;
            } finally {
                try {
                    boolean finishSuccessful = false;
                    try {
                        if (sendSuccessful) {
                            phoenixTransactionContext.commit();
                            finishSuccessful = true;
                        }
                    } catch (SQLException e) {
                        if (LOGGER.isInfoEnabled())
                            LOGGER.info(e.getClass().getName() + " at timestamp " + getInitialWritePointer()
                                    + " with retry count of " + retryCount);
                        retryCommit = (e.getErrorCode() == SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION
                            .getErrorCode() && retryCount < MAX_COMMIT_RETRIES);
                        if (sqlE == null) {
                            sqlE = e;
                        } else {
                            sqlE.setNextException(e);
                        }
                    } finally {
                        // If send fails or finish fails, abort the tx
                        if (!finishSuccessful) {
                            try {
                                phoenixTransactionContext.abort();
                                if (LOGGER.isInfoEnabled()) LOGGER.info("Abort successful");
                            } catch (SQLException e) {
                                if (LOGGER.isInfoEnabled()) LOGGER.info("Abort failed with " + e);
                                if (sqlE == null) {
                                    sqlE = e;
                                } else {
                                    sqlE.setNextException(e);
                                }
                            }
                        }
                    }
                } finally {
                    TransactionFactory.Provider provider = phoenixTransactionContext.getProvider();
                    try {
                        resetState();
                    } finally {
                        if (retryCommit) {
                            startTransaction(provider);
                            // Add back read fences
                            Set<TableRef> txTableRefs = txMutations.keySet();
                            for (TableRef tableRef : txTableRefs) {
                                PTable dataTable = tableRef.getTable();
                                phoenixTransactionContext.markDMLFence(dataTable);
                            }
                            try {
                                // Only retry if an index was added
                                retryCommit = shouldResubmitTransaction(txTableRefs);
                            } catch (SQLException e) {
                                retryCommit = false;
                                if (sqlE == null) {
                                    sqlE = e;
                                } else {
                                    sqlE.setNextException(e);
                                }
                            }
                        }
                        if (sqlE != null && !retryCommit) { throw sqlE; }
                    }
                }
            }
            // Retry commit once if conflict occurred and index was added
            if (!retryCommit) {
                break;
            }
            retryCount++;
            mutationsMap.putAll(txMutations);
        } while (true);
    }

    /**
     * Determines whether indexes were added to mutated tables while the transaction was in progress.
     * 
     * @return true if indexes were added and false otherwise.
     * @throws SQLException
     */
    private boolean shouldResubmitTransaction(Set<TableRef> txTableRefs) throws SQLException {
        if (LOGGER.isInfoEnabled()) LOGGER.info("Checking for index updates as of " + getInitialWritePointer());
        MetaDataClient client = new MetaDataClient(connection);
        PMetaData cache = connection.getMetaDataCache();
        boolean addedAnyIndexes = false;
        boolean allImmutableTables = !txTableRefs.isEmpty();
        for (TableRef tableRef : txTableRefs) {
            PTable dataTable = tableRef.getTable();
            List<PTable> oldIndexes;
            PTableRef ptableRef = cache.getTableRef(dataTable.getKey());
            oldIndexes = ptableRef.getTable().getIndexes();
            // Always check at server for metadata change, as it's possible that the table is configured to not check
            // for metadata changes
            // but in this case, the tx manager is telling us it's likely that there has been a change.
            MetaDataMutationResult result = client.updateCache(dataTable.getTenantId(), dataTable.getSchemaName()
                    .getString(), dataTable.getTableName().getString(), true);
            long timestamp = TransactionUtil.getResolvedTime(connection, result);
            tableRef.setTimeStamp(timestamp);
            PTable updatedDataTable = result.getTable();
            if (updatedDataTable == null) { throw new TableNotFoundException(dataTable.getSchemaName().getString(),
                    dataTable.getTableName().getString()); }
            allImmutableTables &= updatedDataTable.isImmutableRows();
            tableRef.setTable(updatedDataTable);
            if (!addedAnyIndexes) {
                // TODO: in theory we should do a deep equals check here, as it's possible
                // that an index was dropped and recreated with the same name but different
                // indexed/covered columns.
                addedAnyIndexes = (!oldIndexes.equals(updatedDataTable.getIndexes()));
                if (LOGGER.isInfoEnabled())
                    LOGGER.info((addedAnyIndexes ? "Updates " : "No updates ") + "as of " + timestamp + " to "
                            + updatedDataTable.getName().getString() + " with indexes " + updatedDataTable.getIndexes());
            }
        }
        if (LOGGER.isInfoEnabled())
            LOGGER.info((addedAnyIndexes ? "Updates " : "No updates ") + "to indexes as of " + getInitialWritePointer()
                    + " over " + (allImmutableTables ? " all immutable tables" : " some mutable tables"));
        // If all tables are immutable, we know the conflict we got was due to our DDL/DML fence.
        // If any indexes were added, then the conflict might be due to DDL/DML fence.
        return allImmutableTables || addedAnyIndexes;
    }

    /**
     * Send to HBase any uncommitted data for transactional tables.
     * 
     * @return true if any data was sent and false otherwise.
     * @throws SQLException
     */
    public boolean sendUncommitted() throws SQLException {
        return sendUncommitted(mutationsMap.keySet().iterator());
    }

    /**
     * Support read-your-own-write semantics by sending uncommitted data to HBase prior to running a query. In this way,
     * they are visible to subsequent reads but are not actually committed until commit is called.
     * 
     * @param tableRefs
     * @return true if any data was sent and false otherwise.
     * @throws SQLException
     */
    public boolean sendUncommitted(Iterator<TableRef> tableRefs) throws SQLException {

        if (phoenixTransactionContext.isTransactionRunning()) {
            // Initialize visibility so that transactions see their own writes.
            // The checkpoint() method will set it to not see writes if necessary.
            phoenixTransactionContext.setVisibilityLevel(PhoenixVisibilityLevel.SNAPSHOT);
        }

        Iterator<TableRef> filteredTableRefs = Iterators.filter(tableRefs, new Predicate<TableRef>() {
            @Override
            public boolean apply(TableRef tableRef) {
                return tableRef.getTable().isTransactional();
            }
        });
        if (filteredTableRefs.hasNext()) {
            // FIXME: strip table alias to prevent equality check from failing due to alias mismatch on null alias.
            // We really should be keying the tables based on the physical table name.
            List<TableRef> strippedAliases = Lists.newArrayListWithExpectedSize(mutationsMap.keySet().size());
            while (filteredTableRefs.hasNext()) {
                TableRef tableRef = filteredTableRefs.next();
                // REVIEW: unclear if we need this given we start transactions when resolving a table
                if (tableRef.getTable().isTransactional()) {
                    startTransaction(tableRef.getTable().getTransactionProvider());
                }
                strippedAliases.add(new TableRef(null, tableRef.getTable(), tableRef.getTimeStamp(), tableRef
                        .getLowerBoundTimeStamp(), tableRef.hasDynamicCols()));
            }
            send(strippedAliases.iterator());
            return true;
        }
        return false;
    }

    public void send() throws SQLException {
        send(null);
    }

    public static int[] joinSortedIntArrays(int[] a, int[] b) {
        int[] result = new int[a.length + b.length];
        int i = 0, j = 0, k = 0, current;
        while (i < a.length && j < b.length) {
            current = a[i] < b[j] ? a[i++] : b[j++];
            for (; i < a.length && a[i] == current; i++)
                ;
            for (; j < b.length && b[j] == current; j++)
                ;
            result[k++] = current;
        }
        while (i < a.length) {
            for (current = a[i++]; i < a.length && a[i] == current; i++)
                ;
            result[k++] = current;
        }
        while (j < b.length) {
            for (current = b[j++]; j < b.length && b[j] == current; j++)
                ;
            result[k++] = current;
        }
        return Arrays.copyOf(result, k);
    }

    @Immutable
    public static class RowTimestampColInfo {
        private final boolean useServerTimestamp;
        private final Long rowTimestamp;

        public static final RowTimestampColInfo NULL_ROWTIMESTAMP_INFO = new RowTimestampColInfo(false, null);

        public RowTimestampColInfo(boolean autoGenerate, Long value) {
            this.useServerTimestamp = autoGenerate;
            this.rowTimestamp = value;
        }

        public boolean useServerTimestamp() {
            return useServerTimestamp;
        }

        public Long getTimestamp() {
            return rowTimestamp;
        }
    }

    public static class MultiRowMutationState {
        private Map<ImmutableBytesPtr, RowMutationState> rowKeyToRowMutationState;
        private long estimatedSize;

        public MultiRowMutationState(int size) {
            this.rowKeyToRowMutationState = Maps.newHashMapWithExpectedSize(size);
            this.estimatedSize = 0;
        }

        public RowMutationState put(ImmutableBytesPtr ptr, RowMutationState rowMutationState) {
            estimatedSize += rowMutationState.calculateEstimatedSize();
            return rowKeyToRowMutationState.put(ptr, rowMutationState);
        }

        public RowMutationState get(ImmutableBytesPtr ptr) {
            return rowKeyToRowMutationState.get(ptr);
        }

        public void putAll(MultiRowMutationState other) {
            estimatedSize += other.estimatedSize;
            rowKeyToRowMutationState.putAll(other.rowKeyToRowMutationState);
        }

        public boolean isEmpty() {
            return rowKeyToRowMutationState.isEmpty();
        }

        public int size() {
            return rowKeyToRowMutationState.size();
        }

        public Set<Entry<ImmutableBytesPtr, RowMutationState>> entrySet() {
            return rowKeyToRowMutationState.entrySet();
        }

        public void clear() {
            rowKeyToRowMutationState.clear();
            estimatedSize = 0;
        }

        public Collection<RowMutationState> values() {
            return rowKeyToRowMutationState.values();
        }
    }

    public static class RowMutationState {
        @Nonnull
        private Map<PColumn, byte[]> columnValues;
        private int[] statementIndexes;
        @Nonnull
        private final RowTimestampColInfo rowTsColInfo;
        private byte[] onDupKeyBytes;
        private long colValuesSize;

        public RowMutationState(@Nonnull Map<PColumn, byte[]> columnValues, long colValuesSize, int statementIndex,
                @Nonnull RowTimestampColInfo rowTsColInfo, byte[] onDupKeyBytes) {
            checkNotNull(columnValues);
            checkNotNull(rowTsColInfo);
            this.columnValues = columnValues;
            this.statementIndexes = new int[] { statementIndex };
            this.rowTsColInfo = rowTsColInfo;
            this.onDupKeyBytes = onDupKeyBytes;
            this.colValuesSize = colValuesSize;
        }

        public long calculateEstimatedSize() {
            return colValuesSize + statementIndexes.length * SizedUtil.INT_SIZE + SizedUtil.LONG_SIZE
                    + (onDupKeyBytes != null ? onDupKeyBytes.length : 0);
        }

        byte[] getOnDupKeyBytes() {
            return onDupKeyBytes;
        }

        public Map<PColumn, byte[]> getColumnValues() {
            return columnValues;
        }

        int[] getStatementIndexes() {
            return statementIndexes;
        }

        /**
         * Join the newRow with the current row if it doesn't conflict with it.
         * A regular upsert conflicts with a conditional upsert
         * @param newRow
         * @return True if the rows were successfully joined else False
         */
        boolean join(RowMutationState newRow) {
            if (isConflicting(newRow)) {
                return false;
            }
            // If we already have a row and the new row has an ON DUPLICATE KEY clause
            // ignore the new values (as that's what the server will do).
            if (newRow.onDupKeyBytes == null) {
                // increment the column value size by the new row column value size
                colValuesSize += newRow.colValuesSize;
                for (Map.Entry<PColumn, byte[]> entry : newRow.columnValues.entrySet()) {
                    PColumn col = entry.getKey();
                    byte[] oldValue = columnValues.put(col, entry.getValue());
                    if (oldValue != null) {
                        // decrement column value size by the size of all column values that were replaced
                        colValuesSize -= (col.getEstimatedSize() + oldValue.length);
                    }
                }
            }
            // Concatenate ON DUPLICATE KEY bytes to allow multiple
            // increments of the same row in the same commit batch.
            this.onDupKeyBytes = PhoenixIndexBuilder.combineOnDupKey(this.onDupKeyBytes, newRow.onDupKeyBytes);
            statementIndexes = joinSortedIntArrays(statementIndexes, newRow.getStatementIndexes());
            return true;
        }

        @Nonnull
        RowTimestampColInfo getRowTimestampColInfo() {
            return rowTsColInfo;
        }

        public boolean isConflicting(RowMutationState newRowMutationState) {
            return (this.onDupKeyBytes != null && newRowMutationState.onDupKeyBytes == null ||
                this.onDupKeyBytes == null && newRowMutationState.onDupKeyBytes != null);
        }
    }

    public ReadMetricQueue getReadMetricQueue() {
        return readMetricQueue;
    }

    public void setReadMetricQueue(ReadMetricQueue readMetricQueue) {
        this.readMetricQueue = readMetricQueue;
    }

    public MutationMetricQueue getMutationMetricQueue() {
        return mutationMetricQueue;
    }

}
