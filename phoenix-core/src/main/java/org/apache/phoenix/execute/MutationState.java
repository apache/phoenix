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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_FAILED_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_COMMIT_TIME;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexFailurePolicy;
import org.apache.phoenix.index.PhoenixIndexFailurePolicy.MutateCommand;
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
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Tracks the uncommitted state
 */
public class MutationState implements SQLCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MutationState.class);
    private static final int[] EMPTY_STATEMENT_INDEX_ARRAY = new int[0];
    private static final int MAX_COMMIT_RETRIES = 3;

    private final PhoenixConnection connection;
    private final long maxSize;
    private final long maxSizeBytes;
    private final long batchSize;
    private final long batchSizeBytes;
    private long batchCount = 0L;
    private final Map<TableRef, MultiRowMutationState> mutations;
    private final Set<String> uncommittedPhysicalNames = Sets.newHashSetWithExpectedSize(10);

    private long sizeOffset;
    private int numRows = 0;
    private long estimatedSize = 0;
    private int[] uncommittedStatementIndexes = EMPTY_STATEMENT_INDEX_ARRAY;
    private boolean isExternalTxContext = false;
    private Map<TableRef, MultiRowMutationState> txMutations = Collections.emptyMap();

    private PhoenixTransactionContext phoenixTransactionContext = PhoenixTransactionContext.NULL_CONTEXT;

    private final MutationMetricQueue mutationMetricQueue;
    private ReadMetricQueue readMetricQueue;

    public MutationState(long maxSize, long maxSizeBytes, PhoenixConnection connection) {
        this(maxSize, maxSizeBytes, connection, false, null);
    }

    public MutationState(long maxSize, long maxSizeBytes, PhoenixConnection connection,
            PhoenixTransactionContext txContext) {
        this(maxSize, maxSizeBytes, connection, false, txContext);
    }

    public MutationState(MutationState mutationState) {
        this(mutationState.maxSize, mutationState.maxSizeBytes, mutationState.connection, true, mutationState
                .getPhoenixTransactionContext());
    }

    public MutationState(long maxSize, long maxSizeBytes, PhoenixConnection connection, long sizeOffset) {
        this(maxSize, maxSizeBytes, connection, false, null, sizeOffset);
    }

    private MutationState(long maxSize, long maxSizeBytes, PhoenixConnection connection, boolean subTask,
            PhoenixTransactionContext txContext) {
        this(maxSize, maxSizeBytes, connection, subTask, txContext, 0);
    }

    private MutationState(long maxSize, long maxSizeBytes, PhoenixConnection connection, boolean subTask,
            PhoenixTransactionContext txContext, long sizeOffset) {
        this(maxSize, maxSizeBytes, connection, Maps.<TableRef, MultiRowMutationState> newHashMapWithExpectedSize(5),
                subTask, txContext);
        this.sizeOffset = sizeOffset;
    }

    MutationState(long maxSize, long maxSizeBytes, PhoenixConnection connection,
            Map<TableRef, MultiRowMutationState> mutations, boolean subTask, PhoenixTransactionContext txContext) {
        this.maxSize = maxSize;
        this.maxSizeBytes = maxSizeBytes;
        this.connection = connection;
        this.batchSize = connection.getMutateBatchSize();
        this.batchSizeBytes = connection.getMutateBatchSizeBytes();
        this.mutations = mutations;
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

    public MutationState(TableRef table, MultiRowMutationState mutations, long sizeOffset, long maxSize,
            long maxSizeBytes, PhoenixConnection connection) throws SQLException {
        this(maxSize, maxSizeBytes, connection, false, null, sizeOffset);
        if (!mutations.isEmpty()) {
            this.mutations.put(table, mutations);
        }
        this.numRows = mutations.size();
        this.estimatedSize = PhoenixKeyValueUtil.getEstimatedRowMutationSize(this.mutations);
        throwIfTooBig();
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }

    public long getMaxSize() {
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

    public static MutationState emptyMutationState(long maxSize, long maxSizeBytes, PhoenixConnection connection) {
        MutationState state = new MutationState(maxSize, maxSizeBytes, connection,
                Collections.<TableRef, MultiRowMutationState> emptyMap(), false, null);
        state.sizeOffset = 0;
        return state;
    }

    private void throwIfTooBig() throws SQLException {
        if (numRows > maxSize) {
            resetState();
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED).build().buildException();
        }
        if (estimatedSize > maxSizeBytes) {
            resetState();
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MAX_MUTATION_SIZE_BYTES_EXCEEDED).build()
                    .buildException();
        }
    }

    public long getUpdateCount() {
        return sizeOffset + numRows;
    }

    private void joinMutationState(TableRef tableRef, MultiRowMutationState srcRows,
            Map<TableRef, MultiRowMutationState> dstMutations) {
        PTable table = tableRef.getTable();
        boolean isIndex = table.getType() == PTableType.INDEX;
        boolean incrementRowCount = dstMutations == this.mutations;
        MultiRowMutationState existingRows = dstMutations.put(tableRef, srcRows);
        if (existingRows != null) { // Rows for that table already exist
            // Loop through new rows and replace existing with new
            for (Map.Entry<ImmutableBytesPtr, RowMutationState> rowEntry : srcRows.entrySet()) {
                // Replace existing row with new row
                RowMutationState existingRowMutationState = existingRows.put(rowEntry.getKey(), rowEntry.getValue());
                if (existingRowMutationState != null) {
                    Map<PColumn, byte[]> existingValues = existingRowMutationState.getColumnValues();
                    if (existingValues != PRow.DELETE_MARKER) {
                        Map<PColumn, byte[]> newRow = rowEntry.getValue().getColumnValues();
                        // if new row is PRow.DELETE_MARKER, it means delete, and we don't need to merge it with
                        // existing row.
                        if (newRow != PRow.DELETE_MARKER) {
                            // decrement estimated size by the size of the old row
                            estimatedSize -= existingRowMutationState.calculateEstimatedSize();
                            // Merge existing column values with new column values
                            existingRowMutationState.join(rowEntry.getValue());
                            // increment estimated size by the size of the new row
                            estimatedSize += existingRowMutationState.calculateEstimatedSize();
                            // Now that the existing row has been merged with the new row, replace it back
                            // again (since it was merged with the new one above).
                            existingRows.put(rowEntry.getKey(), existingRowMutationState);
                        }
                    }
                } else {
                    if (incrementRowCount && !isIndex) { // Don't count index rows in row count
                        numRows++;
                        // increment estimated size by the size of the new row
                        estimatedSize += rowEntry.getValue().calculateEstimatedSize();
                    }
                }
            }
            // Put the existing one back now that it's merged
            dstMutations.put(tableRef, existingRows);
        } else {
            // Size new map at batch size as that's what it'll likely grow to.
            MultiRowMutationState newRows = new MultiRowMutationState(connection.getMutateBatchSize());
            newRows.putAll(srcRows);
            dstMutations.put(tableRef, newRows);
            if (incrementRowCount && !isIndex) {
                numRows += srcRows.size();
                // if we added all the rows from newMutationState we can just increment the
                // estimatedSize by newMutationState.estimatedSize
                estimatedSize += srcRows.estimatedSize;
            }
        }
    }

    private void joinMutationState(Map<TableRef, MultiRowMutationState> srcMutations,
            Map<TableRef, MultiRowMutationState> dstMutations) {
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, MultiRowMutationState> entry : srcMutations.entrySet()) {
            // Replace existing entries for the table with new entries
            TableRef tableRef = entry.getKey();
            MultiRowMutationState srcRows = entry.getValue();
            joinMutationState(tableRef, srcRows, dstMutations);
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
        joinMutationState(newMutationState.mutations, this.mutations);
        if (!newMutationState.txMutations.isEmpty()) {
            if (txMutations.isEmpty()) {
                txMutations = Maps.newHashMapWithExpectedSize(mutations.size());
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

    private Iterator<Pair<PName, List<Mutation>>> addRowMutations(final TableRef tableRef,
            final MultiRowMutationState values, final long mutationTimestamp, final long serverTimestamp,
            boolean includeAllIndexes, final boolean sendAll) {
        final PTable table = tableRef.getTable();
        final List<PTable> indexList = includeAllIndexes ? 
                Lists.newArrayList(IndexMaintainer.maintainedIndexes(table.getIndexes().iterator())) : 
                    IndexUtil.getClientMaintainedIndexes(table);
        final Iterator<PTable> indexes = indexList.iterator();
        final List<Mutation> mutationList = Lists.newArrayListWithExpectedSize(values.size());
        final List<Mutation> mutationsPertainingToIndex = indexes.hasNext() ? Lists
                .<Mutation> newArrayListWithExpectedSize(values.size()) : null;
        generateMutations(tableRef, mutationTimestamp, serverTimestamp, values, mutationList,
                mutationsPertainingToIndex);
        return new Iterator<Pair<PName, List<Mutation>>>() {
            boolean isFirst = true;
            Map<byte[], List<Mutation>> indexMutationsMap = null;

            @Override
            public boolean hasNext() {
                return isFirst || indexes.hasNext();
            }

            @Override
            public Pair<PName, List<Mutation>> next() {
                if (isFirst) {
                    isFirst = false;
                    return new Pair<PName, List<Mutation>>(table.getPhysicalName(), mutationList);
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
                        MultiRowMutationState multiRowMutationState = mutations.remove(key);
                        if (multiRowMutationState != null) {
                            final List<Mutation> deleteMutations = Lists.newArrayList();
                            generateMutations(key, mutationTimestamp, serverTimestamp, multiRowMutationState, deleteMutations, null);
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
                return new Pair<PName, List<Mutation>>(index.getPhysicalName(),
                        indexMutations == null ? Collections.<Mutation> emptyList() : indexMutations);
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
                // The DeleteCompiler already generates the deletes for indexes, so no need to do it again
                rowMutationsPertainingToIndex = Collections.emptyList();
            } else {
                for (Map.Entry<PColumn, byte[]> valueEntry : rowEntry.getValue().getColumnValues().entrySet()) {
                    row.setValue(valueEntry.getKey(), valueEntry.getValue());
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
            mutationList.addAll(rowMutations);
            if (mutationsPertainingToIndex != null) mutationsPertainingToIndex.addAll(rowMutationsPertainingToIndex);
        }
        values.putAll(modifiedValues);
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
        final Iterator<Map.Entry<TableRef, MultiRowMutationState>> iterator = this.mutations.entrySet().iterator();
        if (!iterator.hasNext()) { return Collections.emptyIterator(); }
        Long scn = connection.getSCN();
        final long serverTimestamp = getTableTimestamp(tableTimestamp, scn);
        final long mutationTimestamp = getMutationTimestamp(scn);
        return new Iterator<Pair<byte[], List<Mutation>>>() {
            private Map.Entry<TableRef, MultiRowMutationState> current = iterator.next();
            private Iterator<Pair<byte[], List<Mutation>>> innerIterator = init();

            private Iterator<Pair<byte[], List<Mutation>>> init() {
                final Iterator<Pair<PName, List<Mutation>>> mutationIterator = addRowMutations(current.getKey(),
                        current.getValue(), mutationTimestamp, serverTimestamp, includeMutableIndexes, true);
                return new Iterator<Pair<byte[], List<Mutation>>>() {
                    @Override
                    public boolean hasNext() {
                        return mutationIterator.hasNext();
                    }

                    @Override
                    public Pair<byte[], List<Mutation>> next() {
                        Pair<PName, List<Mutation>> pair = mutationIterator.next();
                        return new Pair<byte[], List<Mutation>>(pair.getFirst().getBytes(), pair.getSecond());
                    }

                    @Override
                    public void remove() {
                        mutationIterator.remove();
                    }
                };
            }

            @Override
            public boolean hasNext() {
                return innerIterator.hasNext() || iterator.hasNext();
            }

            @Override
            public Pair<byte[], List<Mutation>> next() {
                if (!innerIterator.hasNext()) {
                    current = iterator.next();
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
    private long[] validateAll() throws SQLException {
        int i = 0;
        long[] timeStamps = new long[this.mutations.size()];
        for (Map.Entry<TableRef, MultiRowMutationState> entry : mutations.entrySet()) {
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
        MetaDataMutationResult result = client.updateCache(table.getSchemaName().getString(), table.getTableName()
                .getString());
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

    private static class TableInfo {

        private final boolean isDataTable;
        @Nonnull
        private final PName hTableName;
        @Nonnull
        private final TableRef origTableRef;

        public TableInfo(boolean isDataTable, PName hTableName, TableRef origTableRef) {
            super();
            checkNotNull(hTableName);
            checkNotNull(origTableRef);
            this.isDataTable = isDataTable;
            this.hTableName = hTableName;
            this.origTableRef = origTableRef;
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
            return true;
        }

    }
    private void send(Iterator<TableRef> tableRefIterator) throws SQLException {
        int i = 0;
        long[] serverTimeStamps = null;
        boolean sendAll = false;
        if (tableRefIterator == null) {
            serverTimeStamps = validateAll();
            tableRefIterator = mutations.keySet().iterator();
            sendAll = true;
        }

        MultiRowMutationState multiRowMutationState;
        Map<TableInfo, List<Mutation>> physicalTableMutationMap = Maps.newLinkedHashMap();
        // add tracing for this operation
        try (TraceScope trace = Tracing.startNewSpan(connection, "Committing mutations to tables")) {
            Span span = trace.getSpan();
            ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable();
            while (tableRefIterator.hasNext()) {
                // at this point we are going through mutations for each table
                final TableRef tableRef = tableRefIterator.next();
                multiRowMutationState = mutations.get(tableRef);
                if (multiRowMutationState == null || multiRowMutationState.isEmpty()) {
                    continue;
                }
                // Validate as we go if transactional since we can undo if a problem occurs (which is unlikely)
                long serverTimestamp = serverTimeStamps == null ? validateAndGetServerTimestamp(tableRef,
                        multiRowMutationState) : serverTimeStamps[i++];
                Long scn = connection.getSCN();
                long mutationTimestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                final PTable table = tableRef.getTable();
                Iterator<Pair<PName, List<Mutation>>> mutationsIterator = addRowMutations(tableRef,
                        multiRowMutationState, mutationTimestamp, serverTimestamp, false, sendAll);
                // build map from physical table to mutation list
                boolean isDataTable = true;
                while (mutationsIterator.hasNext()) {
                    Pair<PName, List<Mutation>> pair = mutationsIterator.next();
                    PName hTableName = pair.getFirst();
                    List<Mutation> mutationList = pair.getSecond();
                    TableInfo tableInfo = new TableInfo(isDataTable, hTableName, tableRef);
                    List<Mutation> oldMutationList = physicalTableMutationMap.put(tableInfo, mutationList);
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
                        txMutations = Maps.newHashMapWithExpectedSize(mutations.size());
                    }
                    // Keep all mutations we've encountered until a commit or rollback.
                    // This is not ideal, but there's not good way to get the values back
                    // in the event that we need to replay the commit.
                    // Copy TableRef so we have the original PTable and know when the
                    // indexes have changed.
                    joinMutationState(new TableRef(tableRef), multiRowMutationState, txMutations);
                }
            }
            long serverTimestamp = HConstants.LATEST_TIMESTAMP;
            Iterator<Entry<TableInfo, List<Mutation>>> mutationsIterator = physicalTableMutationMap.entrySet()
                    .iterator();
            while (mutationsIterator.hasNext()) {
                Entry<TableInfo, List<Mutation>> pair = mutationsIterator.next();
                TableInfo tableInfo = pair.getKey();
                byte[] htableName = tableInfo.getHTableName().getBytes();
                List<Mutation> mutationList = pair.getValue();

                // create a span per target table
                // TODO maybe we can be smarter about the table name to string here?
                Span child = Tracing.child(span, "Writing mutation batch for table: " + Bytes.toString(htableName));

                int retryCount = 0;
                boolean shouldRetry = false;
                long numMutations = 0;
                long mutationSizeBytes = 0;
                long mutationCommitTime = 0;
                long numFailedMutations = 0;
                ;
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

                        startTime = System.currentTimeMillis();
                        child.addTimelineAnnotation("Attempt " + retryCount);
                        List<List<Mutation>> mutationBatchList = getMutationBatchList(batchSize, batchSizeBytes,
                                mutationList);
                        for (final List<Mutation> mutationBatch : mutationBatchList) {
                            if (shouldRetryIndexedMutation) {
                                // if there was an index write failure, retry the mutation in a loop
                                final Table finalHTable = hTable;
                                PhoenixIndexFailurePolicy.doBatchWithRetries(new MutateCommand() {
                                    @Override
                                    public void doMutation() throws IOException {
                                        try {
                                            finalHTable.batch(mutationBatch, null);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            throw new IOException(e);
                                        }
                                    }
                                }, iwe, connection, connection.getQueryServices().getProps());
                            } else {
                                hTable.batch(mutationBatch, null);
                            }
                            batchCount++;
                            if (logger.isDebugEnabled())
                                logger.debug("Sent batch of " + mutationBatch.size() + " for "
                                        + Bytes.toString(htableName));
                        }
                        child.stop();
                        child.stop();
                        shouldRetry = false;
                        mutationCommitTime = System.currentTimeMillis() - startTime;
                        GLOBAL_MUTATION_COMMIT_TIME.update(mutationCommitTime);
                        numFailedMutations = 0;

                        // Remove batches as we process them
                        mutations.remove(origTableRef);
                        if (tableInfo.isDataTable()) {
                            numRows -= numMutations;
                            // recalculate the estimated size
                            estimatedSize = PhoenixKeyValueUtil.getEstimatedRowMutationSize(mutations);
                        }
                    } catch (Exception e) {
                        mutationCommitTime = System.currentTimeMillis() - startTime;
                        serverTimestamp = ServerUtil.parseServerTimestamp(e);
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
                                logger.warn(LogUtil.addCustomAnnotations(msg, connection));
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
                                    for (Mutation m : mutationList) {
                                        m.setAttribute(BaseScannerRegionObserver.REPLAY_WRITES, BaseScannerRegionObserver.REPLAY_ONLY_INDEX_WRITES);
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
                    } finally {
                        MutationMetric mutationsMetric = new MutationMetric(numMutations, mutationSizeBytes,
                                mutationCommitTime, numFailedMutations);
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
    }

    /**
     * Split the list of mutations into multiple lists that don't exceed row and byte thresholds
     * 
     * @param allMutationList
     *            List of HBase mutations
     * @return List of lists of mutations
     */
    public static List<List<Mutation>> getMutationBatchList(long batchSize, long batchSizeBytes,
            List<Mutation> allMutationList) {
        List<List<Mutation>> mutationBatchList = Lists.newArrayList();
        List<Mutation> currentList = Lists.newArrayList();
        long currentBatchSizeBytes = 0L;
        for (Mutation mutation : allMutationList) {
            long mutationSizeBytes = PhoenixKeyValueUtil.calculateMutationDiskSize(mutation);
            if (currentList.size() == batchSize || currentBatchSizeBytes + mutationSizeBytes > batchSizeBytes) {
                if (currentList.size() > 0) {
                    mutationBatchList.add(currentList);
                    currentList = Lists.newArrayList();
                    currentBatchSizeBytes = 0L;
                }
            }
            currentList.add(mutation);
            currentBatchSizeBytes += mutationSizeBytes;
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
        for (MultiRowMutationState rowMutationMap : mutations.values()) {
            addUncommittedStatementIndexes(rowMutationMap.values());
        }
        return uncommittedStatementIndexes;
    }

    @Override
    public void close() throws SQLException {}

    private void resetState() {
        numRows = 0;
        estimatedSize = 0;
        this.mutations.clear();
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
        Map<TableRef, MultiRowMutationState> txMutations = Collections.emptyMap();
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
                        if (logger.isInfoEnabled())
                            logger.info(e.getClass().getName() + " at timestamp " + getInitialWritePointer()
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
                                if (logger.isInfoEnabled()) logger.info("Abort successful");
                            } catch (SQLException e) {
                                if (logger.isInfoEnabled()) logger.info("Abort failed with " + e);
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
            mutations.putAll(txMutations);
        } while (true);
    }

    /**
     * Determines whether indexes were added to mutated tables while the transaction was in progress.
     * 
     * @return true if indexes were added and false otherwise.
     * @throws SQLException
     */
    private boolean shouldResubmitTransaction(Set<TableRef> txTableRefs) throws SQLException {
        if (logger.isInfoEnabled()) logger.info("Checking for index updates as of " + getInitialWritePointer());
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
                if (logger.isInfoEnabled())
                    logger.info((addedAnyIndexes ? "Updates " : "No updates ") + "as of " + timestamp + " to "
                            + updatedDataTable.getName().getString() + " with indexes " + updatedDataTable.getIndexes());
            }
        }
        if (logger.isInfoEnabled())
            logger.info((addedAnyIndexes ? "Updates " : "No updates ") + "to indexes as of " + getInitialWritePointer()
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
        return sendUncommitted(mutations.keySet().iterator());
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
            List<TableRef> strippedAliases = Lists.newArrayListWithExpectedSize(mutations.keySet().size());
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

        void join(RowMutationState newRow) {
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
        }

        @Nonnull
        RowTimestampColInfo getRowTimestampColInfo() {
            return rowTsColInfo;
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
