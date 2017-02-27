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
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_COMMIT_TIME;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.monitoring.MutationMetricQueue;
import org.apache.phoenix.monitoring.MutationMetricQueue.MutationMetric;
import org.apache.phoenix.monitoring.MutationMetricQueue.NoOpMutationMetricsQueue;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableRef;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.apache.tephra.Transaction;
import org.apache.tephra.Transaction.VisibilityLevel;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.hbase.TransactionAwareHTable;
import org.apache.tephra.visibility.FenceWait;
import org.apache.tephra.visibility.VisibilityFence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * Tracks the uncommitted state
 *
 */
public class MutationState implements SQLCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MutationState.class);
    private static final TransactionCodec CODEC = new TransactionCodec();
    private static final int[] EMPTY_STATEMENT_INDEX_ARRAY = new int[0];
    private static final int MAX_COMMIT_RETRIES = 3;
    
    private final PhoenixConnection connection;
    private final long maxSize;
    private final long maxSizeBytes;
    private long batchCount = 0L;
    private final Map<TableRef, Map<ImmutableBytesPtr,RowMutationState>> mutations;
    private final List<TransactionAware> txAwares;
    private final TransactionContext txContext;
    private final Set<String> uncommittedPhysicalNames = Sets.newHashSetWithExpectedSize(10);
    
    private Transaction tx;
    private long sizeOffset;
    private int numRows = 0;
    private int[] uncommittedStatementIndexes = EMPTY_STATEMENT_INDEX_ARRAY;
    private boolean isExternalTxContext = false;
    private Map<TableRef, Map<ImmutableBytesPtr,RowMutationState>> txMutations = Collections.emptyMap();
    
    private final MutationMetricQueue mutationMetricQueue;
    private ReadMetricQueue readMetricQueue;

    public MutationState(long maxSize, PhoenixConnection connection) {
        this(maxSize,connection, null, null);
    }
    
    public MutationState(long maxSize, PhoenixConnection connection, TransactionContext txContext) {
        this(maxSize,connection, null, txContext);
    }
    
    public MutationState(MutationState mutationState) {
        this(mutationState.maxSize, mutationState.connection, mutationState.getTransaction(), null);
    }
    
    public MutationState(long maxSize, PhoenixConnection connection, long sizeOffset) {
        this(maxSize, connection, null, null, sizeOffset);
    }
    
    private MutationState(long maxSize, PhoenixConnection connection, Transaction tx, TransactionContext txContext) {
        this(maxSize,connection, tx, txContext, 0);
    }
    
    private MutationState(long maxSize, PhoenixConnection connection, Transaction tx, TransactionContext txContext, long sizeOffset) {
        this(maxSize, connection, Maps.<TableRef, Map<ImmutableBytesPtr,RowMutationState>>newHashMapWithExpectedSize(5), tx, txContext);
        this.sizeOffset = sizeOffset;
    }
    
    MutationState(long maxSize, PhoenixConnection connection,
            Map<TableRef, Map<ImmutableBytesPtr, RowMutationState>> mutations,
            Transaction tx, TransactionContext txContext) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.maxSizeBytes = connection.getMutateBatchSizeBytes();
        this.mutations = mutations;
        boolean isMetricsEnabled = connection.isRequestLevelMetricsEnabled();
        this.mutationMetricQueue = isMetricsEnabled ? new MutationMetricQueue()
                : NoOpMutationMetricsQueue.NO_OP_MUTATION_METRICS_QUEUE;
        this.tx = tx;
        if (tx == null) {
            this.txAwares = Collections.emptyList();
            if (txContext == null) {
                TransactionSystemClient txServiceClient = this.connection
                        .getQueryServices().getTransactionSystemClient();
                this.txContext = new TransactionContext(txServiceClient);
            } else {
                isExternalTxContext = true;
                this.txContext = txContext;
            }
        } else {
            // this code path is only used while running child scans, we can't pass the txContext to child scans
            // as it is not thread safe, so we use the tx member variable
            this.txAwares = Lists.newArrayList();
            this.txContext = null;
        }
    }

    public MutationState(TableRef table, Map<ImmutableBytesPtr,RowMutationState> mutations, long sizeOffset, long maxSize, PhoenixConnection connection) {
        this(maxSize, connection, null, null, sizeOffset);
        this.mutations.put(table, mutations);
        this.numRows = mutations.size();
        this.tx = connection.getMutationState().getTransaction();
        throwIfTooBig();
    }
    
    public long getMaxSize() {
        return maxSize;
    }
    
    /**
     * Commit a write fence when creating an index so that we can detect
     * when a data table transaction is started before the create index
     * but completes after it. In this case, we need to rerun the data
     * table transaction after the index creation so that the index rows
     * are generated. See {@link #addDMLFence(PTable)} and TEPHRA-157
     * for more information.
     * @param dataTable the data table upon which an index is being added
     * @throws SQLException
     */
    public void commitDDLFence(PTable dataTable) throws SQLException {
        if (dataTable.isTransactional()) {
            byte[] key = dataTable.getName().getBytes();
            boolean success = false;
            try {
                FenceWait fenceWait = VisibilityFence.prepareWait(key, connection.getQueryServices().getTransactionSystemClient());
                fenceWait.await(10000, TimeUnit.MILLISECONDS);
                success = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).build().buildException();
            } catch (TimeoutException | TransactionFailureException e) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_UNABLE_TO_GET_WRITE_FENCE)
                .setSchemaName(dataTable.getSchemaName().getString())
                .setTableName(dataTable.getTableName().getString())
                .build().buildException();
            } finally {
                // The client expects a transaction to be in progress on the txContext while the
                // VisibilityFence.prepareWait() starts a new tx and finishes/aborts it. After it's
                // finished, we start a new one here.
                // TODO: seems like an autonomous tx capability in Tephra would be useful here.
                try {
                    txContext.start();
                    if (logger.isInfoEnabled() && success) logger.info("Added write fence at ~" + getTransaction().getReadPointer());
                } catch (TransactionFailureException e) {
                    throw TransactionUtil.getTransactionFailureException(e);
                }
            }
        }
    }
    
    /**
     * Add an entry to the change set representing the DML operation that is starting.
     * These entries will not conflict with each other, but they will conflict with a
     * DDL operation of creating an index. See {@link #addDMLFence(PTable)} and TEPHRA-157
     * for more information.
     * @param table the table which is doing DML
     * @throws SQLException
     */
    private void addDMLFence(PTable table) throws SQLException {
        if (table.getType() == PTableType.INDEX || !table.isTransactional()) {
            return;
        }
        byte[] logicalKey = table.getName().getBytes();
        TransactionAware logicalTxAware = VisibilityFence.create(logicalKey);
        if (this.txContext == null) {
            this.txAwares.add(logicalTxAware);
        } else {
            this.txContext.addTransactionAware(logicalTxAware);
        }
        byte[] physicalKey = table.getPhysicalName().getBytes();
        if (Bytes.compareTo(physicalKey, logicalKey) != 0) {
            TransactionAware physicalTxAware = VisibilityFence.create(physicalKey);
            if (this.txContext == null) {
                this.txAwares.add(physicalTxAware);
            } else {
                this.txContext.addTransactionAware(physicalTxAware);
            }
        }
    }
    
    public boolean checkpointIfNeccessary(MutationPlan plan) throws SQLException {
        Transaction currentTx = getTransaction();
        if (getTransaction() == null || plan.getTargetRef() == null || plan.getTargetRef().getTable() == null || !plan.getTargetRef().getTable().isTransactional()) {
            return false;
        }
        Set<TableRef> sources = plan.getSourceRefs();
        if (sources.isEmpty()) {
            return false;
        }
        // For a DELETE statement, we're always querying the table being deleted from. This isn't
        // a problem, but it potentially could be if there are other references to the same table
        // nested in the DELETE statement (as a sub query or join, for example).
        TableRef ignoreForExcludeCurrent = plan.getOperation() == Operation.DELETE && sources.size() == 1 ? plan.getTargetRef() : null;
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
                if (source.getTable().isTransactional() && (isExternalTxContext || uncommittedPhysicalNames.contains(sourcePhysicalName))) {
                    hasUncommittedData = true;
                    break;
                }
            }
            if (hasUncommittedData) {
                try {
                    if (txContext == null) {
                        currentTx = tx = connection.getQueryServices().getTransactionSystemClient().checkpoint(currentTx);
                    }  else {
                        txContext.checkpoint();
                        currentTx = tx = txContext.getCurrentTransaction();
                    }
                    // Since we've checkpointed, we can clear out uncommitted set, since a statement run afterwards
                    // should see all this data.
                    uncommittedPhysicalNames.clear();
                } catch (TransactionFailureException e) {
                    throw new SQLException(e);
                } 
            }
            // Since we're querying our own table while mutating it, we must exclude
            // see our current mutations, otherwise we can get erroneous results (for DELETE)
            // or get into an infinite loop (for UPSERT SELECT).
            currentTx.setVisibility(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
            return true;
        }
        return false;
    }
    
    private void addTransactionParticipant(TransactionAware txAware) throws SQLException {
        if (txContext == null) {
            txAwares.add(txAware);
            assert(tx != null);
            txAware.startTx(tx);
        } else {
            txContext.addTransactionAware(txAware);
        }
    }
    
    // Though MutationState is not thread safe in general, this method should be because it may
    // be called by TableResultIterator in a multi-threaded manner. Since we do not want to expose
    // the Transaction outside of MutationState, this seems reasonable, as the member variables
    // would not change as these threads are running.
    public HTableInterface getHTable(PTable table) throws SQLException {
        HTableInterface htable = this.getConnection().getQueryServices().getTable(table.getPhysicalName().getBytes());
        Transaction currentTx;
        if (table.isTransactional() && (currentTx=getTransaction()) != null) {
            TransactionAwareHTable txAware = TransactionUtil.getTransactionAwareHTable(htable, table.isImmutableRows());
            // Using cloned mutationState as we may have started a new transaction already
            // if auto commit is true and we need to use the original one here.
            txAware.startTx(currentTx);
            htable = txAware;
        }
        return htable;
    }
    
    public PhoenixConnection getConnection() {
        return connection;
    }
    
    // Kept private as the Transaction may change when check pointed. Keeping it private ensures
    // no one holds on to a stale copy.
    private Transaction getTransaction() {
        return tx != null ? tx : txContext != null ? txContext.getCurrentTransaction() : null;
    }
    
    public boolean isTransactionStarted() {
        return getTransaction() != null;
    }
    
    public long getInitialWritePointer() {
        Transaction tx = getTransaction();
        return tx == null ? HConstants.LATEST_TIMESTAMP : tx.getTransactionId(); // First write pointer - won't change with checkpointing
    }
    
    // For testing
    public long getWritePointer() {
        Transaction tx = getTransaction();
        return tx == null ? HConstants.LATEST_TIMESTAMP : tx.getWritePointer();
    }
    
    // For testing
    public VisibilityLevel getVisibilityLevel() {
        Transaction tx = getTransaction();
        return tx == null ? null : tx.getVisibilityLevel();
    }
    
    public boolean startTransaction() throws SQLException {
        if (txContext == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NULL_TRANSACTION_CONTEXT).build().buildException();
        }
        
        if (connection.getSCN() != null) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.CANNOT_START_TRANSACTION_WITH_SCN_SET)
                    .build().buildException();
        }
        
        try {
            if (!isTransactionStarted()) {
                // Clear any transactional state in case transaction was ended outside
                // of Phoenix so we don't carry the old transaction state forward. We
                // cannot call reset() here due to the case of having mutations and
                // then transitioning from non transactional to transactional (which
                // would end up clearing our uncommitted state).
                resetTransactionalState();
                txContext.start();
                return true;
            }
        } catch (TransactionFailureException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_FAILED).setRootCause(e).build().buildException();
        }
        return false;
    }

    public static MutationState emptyMutationState(long maxSize, PhoenixConnection connection) {
        MutationState state = new MutationState(maxSize, connection, Collections.<TableRef, Map<ImmutableBytesPtr,RowMutationState>>emptyMap(), null, null);
        state.sizeOffset = 0;
        return state;
    }
    
    private void throwIfTooBig() {
        if (numRows > maxSize) {
            // TODO: throw SQLException ?
            throw new IllegalArgumentException("MutationState size of " + numRows + " is bigger than max allowed size of " + maxSize);
        }
    }
    
    public long getUpdateCount() {
        return sizeOffset + numRows;
    }
    
    private void joinMutationState(TableRef tableRef, Map<ImmutableBytesPtr,RowMutationState> srcRows,
            Map<TableRef, Map<ImmutableBytesPtr, RowMutationState>> dstMutations) {
        PTable table = tableRef.getTable();
        boolean isIndex = table.getType() == PTableType.INDEX;
        boolean incrementRowCount = dstMutations == this.mutations;
        Map<ImmutableBytesPtr,RowMutationState> existingRows = dstMutations.put(tableRef, srcRows);
        if (existingRows != null) { // Rows for that table already exist
            // Loop through new rows and replace existing with new
            for (Map.Entry<ImmutableBytesPtr,RowMutationState> rowEntry : srcRows.entrySet()) {
                // Replace existing row with new row
                RowMutationState existingRowMutationState = existingRows.put(rowEntry.getKey(), rowEntry.getValue());
                if (existingRowMutationState != null) {
                    Map<PColumn,byte[]> existingValues = existingRowMutationState.getColumnValues();
                    if (existingValues != PRow.DELETE_MARKER) {
                        Map<PColumn,byte[]> newRow = rowEntry.getValue().getColumnValues();
                        // if new row is PRow.DELETE_MARKER, it means delete, and we don't need to merge it with existing row. 
                        if (newRow != PRow.DELETE_MARKER) {
                            // Merge existing column values with new column values
                            existingRowMutationState.join(rowEntry.getValue());
                            // Now that the existing row has been merged with the new row, replace it back
                            // again (since it was merged with the new one above).
                            existingRows.put(rowEntry.getKey(), existingRowMutationState);
                        }
                    }
                } else {
                    if (incrementRowCount && !isIndex) { // Don't count index rows in row count
                        numRows++;
                    }
                }
            }
            // Put the existing one back now that it's merged
            dstMutations.put(tableRef, existingRows);
        } else {
            // Size new map at batch size as that's what it'll likely grow to.
            Map<ImmutableBytesPtr,RowMutationState> newRows = Maps.newHashMapWithExpectedSize(connection.getMutateBatchSize());
            newRows.putAll(srcRows);
            dstMutations.put(tableRef, newRows);
            if (incrementRowCount && !isIndex) {
                numRows += srcRows.size();
            }
        }
    }
    
    private void joinMutationState(Map<TableRef, Map<ImmutableBytesPtr, RowMutationState>> srcMutations, 
            Map<TableRef, Map<ImmutableBytesPtr, RowMutationState>> dstMutations) {
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,RowMutationState>> entry : srcMutations.entrySet()) {
            // Replace existing entries for the table with new entries
            TableRef tableRef = entry.getKey();
            Map<ImmutableBytesPtr,RowMutationState> srcRows = entry.getValue();
            joinMutationState(tableRef, srcRows, dstMutations);
        }
    }
    /**
     * Combine a newer mutation with this one, where in the event of overlaps, the newer one will take precedence.
     * Combine any metrics collected for the newer mutation.
     * 
     * @param newMutationState the newer mutation state
     */
    public void join(MutationState newMutationState) {
        if (this == newMutationState) { // Doesn't make sense
            return;
        }
        if (txContext != null) {
            for (TransactionAware txAware : newMutationState.txAwares) {
                txContext.addTransactionAware(txAware);
            }
        } else {
            txAwares.addAll(newMutationState.txAwares);
        }
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
        byte[] rowTimestampBytes = PLong.INSTANCE.toBytes(rowTimestamp, rowTimestampField.getSortOrder());
        int oldOffset = ptr.getOffset();
        int oldLength = ptr.getLength();
        // Move the pointer to the start byte of the row timestamp pk
        schema.position(ptr, 0, rowTimestampColPos);
        byte[] b  = ptr.get();
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
    
    private Iterator<Pair<PName,List<Mutation>>> addRowMutations(final TableRef tableRef, final Map<ImmutableBytesPtr, RowMutationState> values,
            final long timestamp, boolean includeAllIndexes, final boolean sendAll) { 
        final PTable table = tableRef.getTable();
        final Iterator<PTable> indexes = // Only maintain tables with immutable rows through this client-side mechanism
                 includeAllIndexes || table.isWALDisabled() ? // TODO: remove check for isWALDisabled once PHOENIX-3137 is fixed.
                     IndexMaintainer.nonDisabledIndexIterator(table.getIndexes().iterator()) :
                         table.isImmutableRows() ?
                            IndexMaintainer.enabledGlobalIndexIterator(table.getIndexes().iterator()) :
                                Iterators.<PTable>emptyIterator();
        final List<Mutation> mutationList = Lists.newArrayListWithExpectedSize(values.size());
        final List<Mutation> mutationsPertainingToIndex = indexes.hasNext() ? Lists.<Mutation>newArrayListWithExpectedSize(values.size()) : null;
        generateMutations(tableRef, timestamp, values, mutationList, mutationsPertainingToIndex);
        return new Iterator<Pair<PName,List<Mutation>>>() {
            boolean isFirst = true;

            @Override
            public boolean hasNext() {
                return isFirst || indexes.hasNext();
            }

            @Override
            public Pair<PName, List<Mutation>> next() {
                if (isFirst) {
                    isFirst = false;
                    return new Pair<PName,List<Mutation>>(table.getPhysicalName(), mutationList);
                }
                PTable index = indexes.next();
                List<Mutation> indexMutations;
                try {
                    indexMutations =
                    		IndexUtil.generateIndexData(table, index, values, mutationsPertainingToIndex,
                                connection.getKeyValueBuilder(), connection);
                    // we may also have to include delete mutations for immutable tables if we are not processing all the tables in the mutations map
                    if (!sendAll) {
                        TableRef key = new TableRef(index);
                        Map<ImmutableBytesPtr, RowMutationState> rowToColumnMap = mutations.remove(key);
                        if (rowToColumnMap!=null) {
                            final List<Mutation> deleteMutations = Lists.newArrayList();
                            generateMutations(tableRef, timestamp, rowToColumnMap, deleteMutations, null);
                            indexMutations.addAll(deleteMutations);
                        }
                    }
                } catch (SQLException e) {
                    throw new IllegalDataException(e);
                }
                return new Pair<PName,List<Mutation>>(index.getPhysicalName(),indexMutations);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }

    private void generateMutations(final TableRef tableRef, long timestamp,
            final Map<ImmutableBytesPtr, RowMutationState> values,
            final List<Mutation> mutationList, final List<Mutation> mutationsPertainingToIndex) {
        final PTable table = tableRef.getTable();
        boolean tableWithRowTimestampCol = table.getRowTimestampColPos() != -1;
        Iterator<Map.Entry<ImmutableBytesPtr, RowMutationState>> iterator =
                values.entrySet().iterator();
        long timestampToUse = timestamp;
        Map<ImmutableBytesPtr, RowMutationState> modifiedValues = Maps.newHashMap();
        while (iterator.hasNext()) {
            Map.Entry<ImmutableBytesPtr, RowMutationState> rowEntry = iterator.next();
            byte[] onDupKeyBytes = rowEntry.getValue().getOnDupKeyBytes();
            boolean hasOnDupKey = onDupKeyBytes != null;
            ImmutableBytesPtr key = rowEntry.getKey();
            RowMutationState state = rowEntry.getValue();
            if (tableWithRowTimestampCol) {
                RowTimestampColInfo rowTsColInfo = state.getRowTimestampColInfo();
                if (rowTsColInfo.useServerTimestamp()) {
                	// since we are about to modify the byte[] stored in key (which changes its hashcode)
                	// we need to remove the entry from the values map and add a new entry with the modified byte[]
                	modifiedValues.put(key, state);
                	iterator.remove();
                    // regenerate the key with this timestamp.
                    key = getNewRowKeyWithRowTimestamp(key, timestampToUse, table);
                } else {
                    if (rowTsColInfo.getTimestamp() != null) {
                        timestampToUse = rowTsColInfo.getTimestamp();
                    }
                }
            }
            PRow row =
                    tableRef.getTable()
                            .newRow(connection.getKeyValueBuilder(), timestampToUse, key, hasOnDupKey);
            List<Mutation> rowMutations, rowMutationsPertainingToIndex;
            if (rowEntry.getValue().getColumnValues() == PRow.DELETE_MARKER) { // means delete
                row.delete();
                rowMutations = row.toRowMutations();
                // Row deletes for index tables are processed by running a re-written query
                // against the index table (as this allows for flexibility in being able to
                // delete rows).
                rowMutationsPertainingToIndex = Collections.emptyList();
            } else {
                for (Map.Entry<PColumn, byte[]> valueEntry : rowEntry.getValue().getColumnValues()
                        .entrySet()) {
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
            if (mutationsPertainingToIndex != null) mutationsPertainingToIndex
                    .addAll(rowMutationsPertainingToIndex);
        }
        values.putAll(modifiedValues);
    }
    
    /**
     * Get the unsorted list of HBase mutations for the tables with uncommitted data.
     * @return list of HBase mutations for uncommitted data.
     */
    public Iterator<Pair<byte[],List<Mutation>>> toMutations(Long timestamp) {
        return toMutations(false, timestamp);
    }
    
    public Iterator<Pair<byte[],List<Mutation>>> toMutations() {
        return toMutations(false, null);
    }
    
    public Iterator<Pair<byte[],List<Mutation>>> toMutations(final boolean includeMutableIndexes) {
        return toMutations(includeMutableIndexes, null);
    }
    
    public Iterator<Pair<byte[],List<Mutation>>> toMutations(final boolean includeMutableIndexes, final Long tableTimestamp) {
        final Iterator<Map.Entry<TableRef, Map<ImmutableBytesPtr,RowMutationState>>> iterator = this.mutations.entrySet().iterator();
        if (!iterator.hasNext()) {
            return Iterators.emptyIterator();
        }
        Long scn = connection.getSCN();
        final long timestamp = getMutationTimestamp(tableTimestamp, scn);
        return new Iterator<Pair<byte[],List<Mutation>>>() {
            private Map.Entry<TableRef, Map<ImmutableBytesPtr,RowMutationState>> current = iterator.next();
            private Iterator<Pair<byte[],List<Mutation>>> innerIterator = init();
                    
            private Iterator<Pair<byte[],List<Mutation>>> init() {
                final Iterator<Pair<PName, List<Mutation>>> mutationIterator = addRowMutations(current.getKey(), current.getValue(), timestamp, includeMutableIndexes, true);
                return new Iterator<Pair<byte[],List<Mutation>>>() {
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
                    innerIterator=init();
                }
                return innerIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }

    public static long getMutationTimestamp(final Long tableTimestamp, Long scn) {
        return (tableTimestamp!=null && tableTimestamp!=QueryConstants.UNSET_TIMESTAMP) ? tableTimestamp : (scn == null ? HConstants.LATEST_TIMESTAMP : scn);
    }

    /**
     * Validates that the meta data is valid against the server meta data if we haven't yet done so.
     * Otherwise, for every UPSERT VALUES call, we'd need to hit the server to see if the meta data
     * has changed.
     * @return the server time to use for the upsert
     * @throws SQLException if the table or any columns no longer exist
     */
    private long[] validateAll() throws SQLException {
        int i = 0;
        long[] timeStamps = new long[this.mutations.size()];
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,RowMutationState>> entry : mutations.entrySet()) {
            TableRef tableRef = entry.getKey();
            timeStamps[i++] = validate(tableRef, entry.getValue());
        }
        return timeStamps;
    }
    
    private long validate(TableRef tableRef, Map<ImmutableBytesPtr, RowMutationState> rowKeyToColumnMap) throws SQLException {
        Long scn = connection.getSCN();
        MetaDataClient client = new MetaDataClient(connection);
        long serverTimeStamp = tableRef.getTimeStamp();
        // If we're auto committing, we've already validated the schema when we got the ColumnResolver,
        // so no need to do it again here.
        PTable table = tableRef.getTable();
        MetaDataMutationResult result = client.updateCache(table.getSchemaName().getString(), table.getTableName().getString());
        PTable resolvedTable = result.getTable();
        if (resolvedTable == null) {
            throw new TableNotFoundException(table.getSchemaName().getString(), table.getTableName().getString());
        }
        // Always update tableRef table as the one we've cached may be out of date since when we executed
        // the UPSERT VALUES call and updated in the cache before this.
        tableRef.setTable(resolvedTable);
        List<PTable> indexes = resolvedTable.getIndexes();
        for (PTable idxTtable : indexes) {
            // If index is still active, but has a non zero INDEX_DISABLE_TIMESTAMP value, then infer that
            // our failure mode is block writes on index failure.
            if (idxTtable.getIndexState() == PIndexState.ACTIVE && idxTtable.getIndexDisableTimestamp() > 0) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INDEX_FAILURE_BLOCK_WRITE)
                .setSchemaName(table.getSchemaName().getString())
                .setTableName(table.getTableName().getString()).build().buildException();
            }
        } 
        long timestamp = result.getMutationTime();
        if (timestamp != QueryConstants.UNSET_TIMESTAMP) {
            serverTimeStamp = timestamp;
            if (result.wasUpdated()) {
                List<PColumn> columns = Lists.newArrayListWithExpectedSize(table.getColumns().size());
                for (Map.Entry<ImmutableBytesPtr,RowMutationState> rowEntry : rowKeyToColumnMap.entrySet()) {
                    RowMutationState valueEntry = rowEntry.getValue();
                    if (valueEntry != null) {
                        Map<PColumn, byte[]> colValues = valueEntry.getColumnValues();
                        if (colValues != PRow.DELETE_MARKER) {
                            for (PColumn column : colValues.keySet()) {
                                if (!column.isDynamic())
                                    columns.add(column);
                            }
                        }
                    }
                }
                for (PColumn column : columns) {
                    if (column != null) {
                        resolvedTable.getColumnFamily(column.getFamilyName().getString()).getPColumnForColumnName(column.getName().getString());
                    }
                }
            }
        }
        return scn == null ? serverTimeStamp == QueryConstants.UNSET_TIMESTAMP ? HConstants.LATEST_TIMESTAMP : serverTimeStamp : scn;
    }
    
    private static long calculateMutationSize(List<Mutation> mutations) {
        long byteSize = 0;
        if (GlobalClientMetrics.isMetricsEnabled()) {
            for (Mutation mutation : mutations) {
                byteSize += mutation.heapSize();
            }
        }
        GLOBAL_MUTATION_BYTES.update(byteSize);
        return byteSize;
    }
    
    private boolean hasKeyValueColumn(PTable table, PTable index) {
        IndexMaintainer maintainer = index.getIndexMaintainer(table, connection);
        return !maintainer.getAllColumns().isEmpty();
    }
    
    private void divideImmutableIndexes(Iterator<PTable> enabledImmutableIndexes, PTable table, List<PTable> rowKeyIndexes, List<PTable> keyValueIndexes) {
        while (enabledImmutableIndexes.hasNext()) {
            PTable index = enabledImmutableIndexes.next();
            if (index.getIndexType() != IndexType.LOCAL) {
                if (hasKeyValueColumn(table, index)) {
                    keyValueIndexes.add(index);
                } else {
                    rowKeyIndexes.add(index);
                }
            }
        }
    }

    public long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    public long getBatchCount() {
        return batchCount;
    }

    private class MetaDataAwareHTable extends DelegateHTable {
        private final TableRef tableRef;
        
        private MetaDataAwareHTable(HTableInterface delegate, TableRef tableRef) {
            super(delegate);
            this.tableRef = tableRef;
        }
        
        /**
         * Called by Tephra when a transaction is aborted. We have this wrapper so that we get an
         * opportunity to attach our index meta data to the mutations such that we can also undo
         * the index mutations.
         */
        @Override
        public void delete(List<Delete> deletes) throws IOException {
            ServerCache cache = null;
            try {
                PTable table = tableRef.getTable();
                List<PTable> indexes = table.getIndexes();
                Iterator<PTable> enabledIndexes = IndexMaintainer.nonDisabledIndexIterator(indexes.iterator());
                if (enabledIndexes.hasNext()) {
                    List<PTable> keyValueIndexes = Collections.emptyList();
                    ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable();
                    boolean attachMetaData = table.getIndexMaintainers(indexMetaDataPtr, connection);
                    if (table.isImmutableRows()) {
                        List<PTable> rowKeyIndexes = Lists.newArrayListWithExpectedSize(indexes.size());
                        keyValueIndexes = Lists.newArrayListWithExpectedSize(indexes.size());
                        divideImmutableIndexes(enabledIndexes, table, rowKeyIndexes, keyValueIndexes);
                        // Generate index deletes for immutable indexes that only reference row key
                        // columns and submit directly here.
                        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                        for (PTable index : rowKeyIndexes) {
                            List<Delete> indexDeletes = IndexUtil.generateDeleteIndexData(table, index, deletes, ptr, connection.getKeyValueBuilder(), connection);
                            HTableInterface hindex = connection.getQueryServices().getTable(index.getPhysicalName().getBytes());
                            hindex.delete(indexDeletes);
                        }
                    }
                    
                    // If we have mutable indexes, local immutable indexes, or global immutable indexes
                    // that reference key value columns, setup index meta data and attach here. In this
                    // case updates to the indexes will be generated on the server side.
                    // An alternative would be to let Tephra track the row keys for the immutable index
                    // by adding it as a transaction participant (soon we can prevent any conflict
                    // detection from occurring) with the downside being the additional memory required.
                    if (!keyValueIndexes.isEmpty()) {
                        attachMetaData = true;
                        IndexMaintainer.serializeAdditional(table, indexMetaDataPtr, keyValueIndexes, connection);
                    }
                    if (attachMetaData) {
                        cache = setMetaDataOnMutations(tableRef, deletes, indexMetaDataPtr);
                    }
                }
                delegate.delete(deletes);
            } catch (SQLException e) {
                throw new IOException(e);
            } finally {
                if (cache != null) {
                    SQLCloseables.closeAllQuietly(Collections.singletonList(cache));
                }
            }
        }
    }
    
    private static class TableInfo {
        
        private final boolean isDataTable;
        @Nonnull private final PName hTableName;
        @Nonnull private final TableRef origTableRef;
        
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
            TableInfo other = (TableInfo) obj;
            if (!hTableName.equals(other.hTableName)) return false;
            if (isDataTable != other.isDataTable) return false;
            return true;
        }

    }
    
    @SuppressWarnings("deprecation")
    private void send(Iterator<TableRef> tableRefIterator) throws SQLException {
        int i = 0;
        long[] serverTimeStamps = null;
        boolean sendAll = false;
        if (tableRefIterator == null) {
            serverTimeStamps = validateAll();
            tableRefIterator = mutations.keySet().iterator();
            sendAll = true;
        }

        Map<ImmutableBytesPtr, RowMutationState> valuesMap;
        List<TableRef> txTableRefs = Lists.newArrayListWithExpectedSize(mutations.size());
        Map<TableInfo,List<Mutation>> physicalTableMutationMap = Maps.newLinkedHashMap(); 
        // add tracing for this operation
        try (TraceScope trace = Tracing.startNewSpan(connection, "Committing mutations to tables")) {
            Span span = trace.getSpan();
            ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable();
            boolean isTransactional;
            while (tableRefIterator.hasNext()) {
                // at this point we are going through mutations for each table
                final TableRef tableRef = tableRefIterator.next();
                valuesMap = mutations.get(tableRef);
                if (valuesMap == null || valuesMap.isEmpty()) {
                    continue;
                }
                // Validate as we go if transactional since we can undo if a problem occurs (which is unlikely)
                long serverTimestamp = serverTimeStamps == null ? validate(tableRef, valuesMap) : serverTimeStamps[i++];
                final PTable table = tableRef.getTable();
                Iterator<Pair<PName,List<Mutation>>> mutationsIterator = addRowMutations(tableRef, valuesMap, serverTimestamp, false, sendAll);
                // build map from physical table to mutation list
                boolean isDataTable = true;
                while (mutationsIterator.hasNext()) {
                    Pair<PName,List<Mutation>> pair = mutationsIterator.next();
                    PName hTableName = pair.getFirst();
                    List<Mutation> mutationList = pair.getSecond();
                    TableInfo tableInfo = new TableInfo(isDataTable, hTableName, tableRef);
                    List<Mutation> oldMutationList = physicalTableMutationMap.put(tableInfo, mutationList);
                    if (oldMutationList!=null)
                        mutationList.addAll(0, oldMutationList);
                    isDataTable = false;
                }
                // For transactions, track the statement indexes as we send data
                // over because our CommitException should include all statements
                // involved in the transaction since none of them would have been
                // committed in the event of a failure.
                if (table.isTransactional()) {
                    addUncommittedStatementIndexes(valuesMap.values());
                    if (txMutations.isEmpty()) {
                        txMutations = Maps.newHashMapWithExpectedSize(mutations.size());
                    }
                    // Keep all mutations we've encountered until a commit or rollback.
                    // This is not ideal, but there's not good way to get the values back
                    // in the event that we need to replay the commit.
                    // Copy TableRef so we have the original PTable and know when the
                    // indexes have changed.
                    joinMutationState(new TableRef(tableRef), valuesMap, txMutations);
                }
            }
            Iterator<Entry<TableInfo, List<Mutation>>> mutationsIterator = physicalTableMutationMap.entrySet().iterator();
            while (mutationsIterator.hasNext()) {
                Entry<TableInfo, List<Mutation>> pair = mutationsIterator.next();
                TableInfo tableInfo = pair.getKey();
                byte[] htableName = tableInfo.getHTableName().getBytes();
                List<Mutation> mutationList = pair.getValue();
                
                //create a span per target table
                //TODO maybe we can be smarter about the table name to string here?
                Span child = Tracing.child(span,"Writing mutation batch for table: "+Bytes.toString(htableName));

                int retryCount = 0;
                boolean shouldRetry = false;
                do {
                    TableRef origTableRef = tableInfo.getOrigTableRef();
                    PTable table = origTableRef.getTable();
                    table.getIndexMaintainers(indexMetaDataPtr, connection);
                    final ServerCache cache = tableInfo.isDataTable() ? setMetaDataOnMutations(origTableRef, mutationList, indexMetaDataPtr) : null;
                    // If we haven't retried yet, retry for this case only, as it's possible that
                    // a split will occur after we send the index metadata cache to all known
                    // region servers.
                    shouldRetry = cache!=null;
                    SQLException sqlE = null;
                    HTableInterface hTable = connection.getQueryServices().getTable(htableName);
                    try {
                        if (table.isTransactional()) {
                            // Track tables to which we've sent uncommitted data
                            txTableRefs.add(origTableRef);
                            addDMLFence(table);
                            uncommittedPhysicalNames.add(table.getPhysicalName().getString());
                            
                            // If we have indexes, wrap the HTable in a delegate HTable that
                            // will attach the necessary index meta data in the event of a
                            // rollback
                            if (!table.getIndexes().isEmpty()) {
                                hTable = new MetaDataAwareHTable(hTable, origTableRef);
                            }
                            TransactionAwareHTable txnAware = TransactionUtil.getTransactionAwareHTable(hTable, table.isImmutableRows());
                            // Don't add immutable indexes (those are the only ones that would participate
                            // during a commit), as we don't need conflict detection for these.
                            if (tableInfo.isDataTable()) {
                                // Even for immutable, we need to do this so that an abort has the state
                                // necessary to generate the rows to delete.
                                addTransactionParticipant(txnAware);
                            } else {
                                txnAware.startTx(getTransaction());
                            }
                            hTable = txnAware;
                        }
                        
                        long numMutations = mutationList.size();
                        GLOBAL_MUTATION_BATCH_SIZE.update(numMutations);
                        
                        long startTime = System.currentTimeMillis();
                        child.addTimelineAnnotation("Attempt " + retryCount);
                        List<List<Mutation>> mutationBatchList = getMutationBatchList(maxSize, maxSizeBytes, mutationList);
                        for (List<Mutation> mutationBatch : mutationBatchList) {
                            hTable.batch(mutationBatch);
                            batchCount++;
                        }
                        if (logger.isDebugEnabled()) logger.debug("Sent batch of " + numMutations + " for " + Bytes.toString(htableName));
                        child.stop();
                        child.stop();
                        shouldRetry = false;
                        long mutationCommitTime = System.currentTimeMillis() - startTime;
                        GLOBAL_MUTATION_COMMIT_TIME.update(mutationCommitTime);
                        
                        long mutationSizeBytes = calculateMutationSize(mutationList);
                        MutationMetric mutationsMetric = new MutationMetric(numMutations, mutationSizeBytes, mutationCommitTime);
                        mutationMetricQueue.addMetricsForTable(Bytes.toString(htableName), mutationsMetric);
                        if (tableInfo.isDataTable()) {
                            numRows -= numMutations;
                        }
                        // Remove batches as we process them
                        mutations.remove(origTableRef);
                    } catch (Exception e) {
                        SQLException inferredE = ServerUtil.parseServerExceptionOrNull(e);
                        if (inferredE != null) {
                            if (shouldRetry && retryCount == 0 && inferredE.getErrorCode() == SQLExceptionCode.INDEX_METADATA_NOT_FOUND.getErrorCode()) {
                                // Swallow this exception once, as it's possible that we split after sending the index metadata
                                // and one of the region servers doesn't have it. This will cause it to have it the next go around.
                                // If it fails again, we don't retry.
                                String msg = "Swallowing exception and retrying after clearing meta cache on connection. " + inferredE;
                                logger.warn(LogUtil.addCustomAnnotations(msg, connection));
                                connection.getQueryServices().clearTableRegionCache(htableName);

                                // add a new child span as this one failed
                                child.addTimelineAnnotation(msg);
                                child.stop();
                                child = Tracing.child(span,"Failed batch, attempting retry");

                                continue;
                            }
                            e = inferredE;
                        }
                        // Throw to client an exception that indicates the statements that
                        // were not committed successfully.
                        sqlE = new CommitException(e, getUncommittedStatementIndexes());
                    } finally {
                        try {
                            if (cache!=null) 
                                cache.close();
                        } finally {
                            try {
                                hTable.close();
                            } 
                            catch (IOException e) {
                                if (sqlE != null) {
                                    sqlE.setNextException(ServerUtil.parseServerException(e));
                                } else {
                                    sqlE = ServerUtil.parseServerException(e);
                                }
                            } 
                            if (sqlE != null) {
                                throw sqlE;
                            }
                        }
                    }
                } while (shouldRetry && retryCount++ < 1);
            }
        }
    }

    /**
     * Split the list of mutations into multiple lists that don't exceed row and byte thresholds
     * @param allMutationList List of HBase mutations
     * @return List of lists of mutations
     */
    public static List<List<Mutation>> getMutationBatchList(long maxSize, long maxSizeBytes, List<Mutation> allMutationList) {
        List<List<Mutation>> mutationBatchList = Lists.newArrayList();
        List<Mutation> currentList = Lists.newArrayList();
        long currentBatchSizeBytes = 0L;
        for (Mutation mutation : allMutationList) {
            long mutationSizeBytes = mutation.heapSize();
            if (currentList.size() == maxSize || currentBatchSizeBytes + mutationSizeBytes > maxSizeBytes) {
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
        try {
            return CODEC.encode(getTransaction());
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
    
    public static Transaction decodeTransaction(byte[] txnBytes) throws IOException {
        return (txnBytes == null || txnBytes.length==0) ? null : CODEC.decode(txnBytes);
    }

    private ServerCache setMetaDataOnMutations(TableRef tableRef, List<? extends Mutation> mutations,
            ImmutableBytesWritable indexMetaDataPtr) throws SQLException {
        PTable table = tableRef.getTable();
        final byte[] tenantIdBytes;
        if(table.isMultiTenant()) {
            tenantIdBytes = connection.getTenantId() == null ? null :
                    ScanUtil.getTenantIdBytes(
                            table.getRowKeySchema(),
                            table.getBucketNum() != null,
                            connection.getTenantId(), table.getViewIndexId() != null);
        } else {
            tenantIdBytes = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
        }
        ServerCache cache = null;
        byte[] attribValue = null;
        byte[] uuidValue = null;
        byte[] txState = ByteUtil.EMPTY_BYTE_ARRAY;
        if (table.isTransactional()) {
            txState = encodeTransaction();
        }
        boolean hasIndexMetaData = indexMetaDataPtr.getLength() > 0;
        if (hasIndexMetaData) {
            if (IndexMetaDataCacheClient.useIndexMetadataCache(connection, mutations, indexMetaDataPtr.getLength() + txState.length)) {
                IndexMetaDataCacheClient client = new IndexMetaDataCacheClient(connection, tableRef);
                cache = client.addIndexMetadataCache(mutations, indexMetaDataPtr, txState);
                uuidValue = cache.getId();
            } else {
                attribValue = ByteUtil.copyKeyBytesIfNecessary(indexMetaDataPtr);
                uuidValue = ServerCacheClient.generateId();
            }
        } else if (txState.length == 0) {
            return null;
        }
        // Either set the UUID to be able to access the index metadata from the cache
        // or set the index metadata directly on the Mutation
        for (Mutation mutation : mutations) {
            if (connection.getTenantId() != null) {
                mutation.setAttribute(PhoenixRuntime.TENANT_ID_ATTRIB, tenantIdBytes);
            }
            mutation.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
            if (attribValue != null) {
                mutation.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, attribValue);
                if (txState.length > 0) {
                    mutation.setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
                }
            } else if (!hasIndexMetaData && txState.length > 0) {
                mutation.setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
            }
        }
        return cache;
    }
    
    private void addUncommittedStatementIndexes(Collection<RowMutationState> rowMutations) {
        for (RowMutationState rowMutationState : rowMutations) {
            uncommittedStatementIndexes = joinSortedIntArrays(uncommittedStatementIndexes, rowMutationState.getStatementIndexes());
        }
    }
    
    private int[] getUncommittedStatementIndexes() {
        for (Map<ImmutableBytesPtr, RowMutationState> rowMutationMap : mutations.values()) {
            addUncommittedStatementIndexes(rowMutationMap.values());
        }
        return uncommittedStatementIndexes;
    }
    
    @Override
    public void close() throws SQLException {
    }

    private void resetState() {
        numRows = 0;
        this.mutations.clear();
        resetTransactionalState();
    }
    
    private void resetTransactionalState() {
        tx = null;
        txAwares.clear();
        txMutations = Collections.emptyMap();
        uncommittedPhysicalNames.clear();
        uncommittedStatementIndexes = EMPTY_STATEMENT_INDEX_ARRAY;
    }
    
    public void rollback() throws SQLException {
        try {
            if (txContext != null && isTransactionStarted()) {
                try {
                    txContext.abort();
                } catch (TransactionFailureException e) {
                    throw TransactionUtil.getTransactionFailureException(e);
                }
            }
        } finally {
            resetState();
        }
    }
    
    public void commit() throws SQLException {
        Map<TableRef, Map<ImmutableBytesPtr,RowMutationState>> txMutations = Collections.emptyMap();
        int retryCount = 0;
        do {
            boolean sendSuccessful=false;
            boolean retryCommit = false;
            SQLException sqlE = null;
            try {
                send();
                txMutations = this.txMutations;
                sendSuccessful=true;
            } catch (SQLException e) {
                sqlE = e;
            } finally {
                try {
                    if (txContext != null && isTransactionStarted()) {
                        TransactionFailureException txFailure = null;
                        boolean finishSuccessful=false;
                        try {
                            if (sendSuccessful) {
                                txContext.finish();
                                finishSuccessful = true;
                            }
                        } catch (TransactionFailureException e) {
                            if (logger.isInfoEnabled()) logger.info(e.getClass().getName() + " at timestamp " + getInitialWritePointer() + " with retry count of " + retryCount);
                            retryCommit = (e instanceof TransactionConflictException && retryCount < MAX_COMMIT_RETRIES);
                            txFailure = e;
                            SQLException nextE = TransactionUtil.getTransactionFailureException(e);
                            if (sqlE == null) {
                                sqlE = nextE;
                            } else {
                                sqlE.setNextException(nextE);
                            }
                        } finally {
                            // If send fails or finish fails, abort the tx
                            if (!finishSuccessful) {
                                try {
                                    txContext.abort(txFailure);
                                    if (logger.isInfoEnabled()) logger.info("Abort successful");
                                } catch (TransactionFailureException e) {
                                    if (logger.isInfoEnabled()) logger.info("Abort failed with " + e);
                                    SQLException nextE = TransactionUtil.getTransactionFailureException(e);
                                    if (sqlE == null) {
                                        sqlE = nextE;
                                    } else {
                                        sqlE.setNextException(nextE);
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    try {
                        resetState();
                    } finally {
                        if (retryCommit) {
                            startTransaction();
                            // Add back read fences
                            Set<TableRef> txTableRefs = txMutations.keySet();
                            for (TableRef tableRef : txTableRefs) {
                                PTable dataTable = tableRef.getTable();
                                addDMLFence(dataTable);
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
                        if (sqlE != null && !retryCommit) {
                            throw sqlE;
                        }
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
     * @return true if indexes were added and false otherwise.
     * @throws SQLException 
     */
    private boolean shouldResubmitTransaction(Set<TableRef> txTableRefs) throws SQLException {
        if (logger.isInfoEnabled()) logger.info("Checking for index updates as of "  + getInitialWritePointer());
        MetaDataClient client = new MetaDataClient(connection);
        PMetaData cache = connection.getMetaDataCache();
        boolean addedAnyIndexes = false;
        boolean allImmutableTables = !txTableRefs.isEmpty();
        for (TableRef tableRef : txTableRefs) {
            PTable dataTable = tableRef.getTable();
            List<PTable> oldIndexes;
            PTableRef ptableRef = cache.getTableRef(dataTable.getKey());
            oldIndexes = ptableRef.getTable().getIndexes();
            // Always check at server for metadata change, as it's possible that the table is configured to not check for metadata changes
            // but in this case, the tx manager is telling us it's likely that there has been a change.
            MetaDataMutationResult result = client.updateCache(dataTable.getTenantId(), dataTable.getSchemaName().getString(), dataTable.getTableName().getString(), true);
            long timestamp = TransactionUtil.getResolvedTime(connection, result);
            tableRef.setTimeStamp(timestamp);
            PTable updatedDataTable = result.getTable();
            if (updatedDataTable == null) {
                throw new TableNotFoundException(dataTable.getSchemaName().getString(), dataTable.getTableName().getString());
            }
            allImmutableTables &= updatedDataTable.isImmutableRows();
            tableRef.setTable(updatedDataTable);
            if (!addedAnyIndexes) {
                // TODO: in theory we should do a deep equals check here, as it's possible
                // that an index was dropped and recreated with the same name but different
                // indexed/covered columns.
                addedAnyIndexes = (!oldIndexes.equals(updatedDataTable.getIndexes()));
                if (logger.isInfoEnabled()) logger.info((addedAnyIndexes ? "Updates " : "No updates ") + "as of "  + timestamp + " to " + updatedDataTable.getName().getString() + " with indexes " + updatedDataTable.getIndexes());
            }
        }
        if (logger.isInfoEnabled()) logger.info((addedAnyIndexes ? "Updates " : "No updates ") + "to indexes as of "  + getInitialWritePointer() + " over " + (allImmutableTables ? " all immutable tables" : " some mutable tables"));
        // If all tables are immutable, we know the conflict we got was due to our DDL/DML fence.
        // If any indexes were added, then the conflict might be due to DDL/DML fence.
        return allImmutableTables || addedAnyIndexes;
    }

    /**
     * Send to HBase any uncommitted data for transactional tables.
     * @return true if any data was sent and false otherwise.
     * @throws SQLException
     */
    public boolean sendUncommitted() throws SQLException {
        return sendUncommitted(mutations.keySet().iterator());
    }
    /**
     * Support read-your-own-write semantics by sending uncommitted data to HBase prior to running a
     * query. In this way, they are visible to subsequent reads but are not actually committed until
     * commit is called.
     * @param tableRefs
     * @return true if any data was sent and false otherwise.
     * @throws SQLException
     */
    public boolean sendUncommitted(Iterator<TableRef> tableRefs) throws SQLException {
        Transaction currentTx = getTransaction();
        if (currentTx != null) {
            // Initialize visibility so that transactions see their own writes.
            // The checkpoint() method will set it to not see writes if necessary.
            currentTx.setVisibility(VisibilityLevel.SNAPSHOT);
        }
        Iterator<TableRef> filteredTableRefs = Iterators.filter(tableRefs, new Predicate<TableRef>(){
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
                strippedAliases.add(new TableRef(null, tableRef.getTable(), tableRef.getTimeStamp(), tableRef.getLowerBoundTimeStamp(), tableRef.hasDynamicCols()));
            }
            startTransaction();
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
            for ( ; i < a.length && a[i] == current; i++);
            for ( ; j < b.length && b[j] == current; j++);
            result[k++] = current;
        }
        while (i < a.length) {
            for (current = a[i++] ; i < a.length && a[i] == current; i++);
            result[k++] = current;
        }
        while (j < b.length) {
            for (current = b[j++] ; j < b.length && b[j] == current; j++);
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
    
    public static class RowMutationState {
        @Nonnull private Map<PColumn,byte[]> columnValues;
        private int[] statementIndexes;
        @Nonnull private final RowTimestampColInfo rowTsColInfo;
        private byte[] onDupKeyBytes;
        
        public RowMutationState(@Nonnull Map<PColumn,byte[]> columnValues, int statementIndex, @Nonnull RowTimestampColInfo rowTsColInfo,
                byte[] onDupKeyBytes) {
            checkNotNull(columnValues);
            checkNotNull(rowTsColInfo);
            this.columnValues = columnValues;
            this.statementIndexes = new int[] {statementIndex};
            this.rowTsColInfo = rowTsColInfo;
            this.onDupKeyBytes = onDupKeyBytes;
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
                getColumnValues().putAll(newRow.getColumnValues());
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
