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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.tephra.Transaction;
import co.cask.tephra.Transaction.VisibilityLevel;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionCodec;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.hbase11.TransactionAwareHTable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * Tracks the uncommitted state
 *
 * 
 * @since 0.1
 */
public class MutationState implements SQLCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MutationState.class);
    private static final TransactionCodec CODEC = new TransactionCodec();
    
    private PhoenixConnection connection;
    private final long maxSize;
    private final Map<TableRef, Map<ImmutableBytesPtr,RowMutationState>> mutations;
    private final List<TransactionAware> txAwares;
    private final TransactionContext txContext;
    private final Set<String> uncommittedPhysicalNames = Sets.newHashSetWithExpectedSize(10);
    
    private Transaction tx;
    private long sizeOffset;
    private int numRows = 0;
    private boolean txStarted = false;
    
    private final MutationMetricQueue mutationMetricQueue;
    private ReadMetricQueue readMetricQueue;
    
    public MutationState(long maxSize, PhoenixConnection connection) {
        this(maxSize,connection, null);
    }
    
    public MutationState(MutationState mutationState) {
        this(mutationState.maxSize, mutationState.connection, mutationState.getTransaction());
    }
    
    public MutationState(long maxSize, PhoenixConnection connection, long sizeOffset) {
        this(maxSize, connection, null, sizeOffset);
    }
    
    private MutationState(long maxSize, PhoenixConnection connection, Transaction tx) {
        this(maxSize,connection, tx, 0);
    }
    
    private MutationState(long maxSize, PhoenixConnection connection, Transaction tx, long sizeOffset) {
    	this(maxSize, connection, Maps.<TableRef, Map<ImmutableBytesPtr,RowMutationState>>newHashMapWithExpectedSize(connection.getMutateBatchSize()), tx);
        this.sizeOffset = sizeOffset;
    }
    
	MutationState(long maxSize, PhoenixConnection connection,
			Map<TableRef, Map<ImmutableBytesPtr, RowMutationState>> mutations,
			Transaction tx) {
		this.maxSize = maxSize;
		this.connection = connection;
		this.mutations = mutations;
		boolean isMetricsEnabled = connection.isRequestLevelMetricsEnabled();
		this.mutationMetricQueue = isMetricsEnabled ? new MutationMetricQueue()
				: NoOpMutationMetricsQueue.NO_OP_MUTATION_METRICS_QUEUE;
		this.tx = tx;
		if (tx == null) {
			this.txAwares = Collections.emptyList();
			TransactionSystemClient txServiceClient = this.connection
					.getQueryServices().getTransactionSystemClient();
			this.txContext = new TransactionContext(txServiceClient);
		} else {
			txAwares = Lists.newArrayList();
			txContext = null;
		}
	}

    public MutationState(TableRef table, Map<ImmutableBytesPtr,RowMutationState> mutations, long sizeOffset, long maxSize, PhoenixConnection connection) {
        this(maxSize, connection, null, sizeOffset);
        this.mutations.put(table, mutations);
        this.numRows = mutations.size();
        this.tx = connection.getMutationState().getTransaction();
        throwIfTooBig();
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
                if (source.getTable().isTransactional() && uncommittedPhysicalNames.contains(sourcePhysicalName)) {
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
            TransactionAwareHTable txAware = TransactionUtil.getTransactionAwareHTable(htable, table);
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
    
    public long getReadPointer() {
    	Transaction tx = getTransaction();
    	return tx == null ? HConstants.LATEST_TIMESTAMP : tx.getReadPointer();
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
            if (!txStarted) {
                txContext.start();
                txStarted = true;
                return true;
            }
        } catch (TransactionFailureException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_FAILED).setRootCause(e).build().buildException();
        }
        return false;
    }

    public static MutationState emptyMutationState(long maxSize, PhoenixConnection connection) {
        MutationState state = new MutationState(maxSize, connection, Collections.<TableRef, Map<ImmutableBytesPtr,RowMutationState>>emptyMap(), null);
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
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,RowMutationState>> entry : newMutationState.mutations.entrySet()) {
            // Replace existing entries for the table with new entries
            TableRef tableRef = entry.getKey();
            PTable table = tableRef.getTable();
            boolean isIndex = table.getType() == PTableType.INDEX;
            Map<ImmutableBytesPtr,RowMutationState> existingRows = this.mutations.put(tableRef, entry.getValue());
            if (existingRows != null) { // Rows for that table already exist
                // Loop through new rows and replace existing with new
                for (Map.Entry<ImmutableBytesPtr,RowMutationState> rowEntry : entry.getValue().entrySet()) {
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
                        if (!isIndex) { // Don't count index rows in row count
                            numRows++;
                        }
                    }
                }
                // Put the existing one back now that it's merged
                this.mutations.put(entry.getKey(), existingRows);
            } else {
                if (!isIndex) {
                    numRows += entry.getValue().size();
                }
            }
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
    
	private Iterator<Pair<byte[],List<Mutation>>> addRowMutations(final TableRef tableRef, final Map<ImmutableBytesPtr, RowMutationState> values, final long timestamp, boolean includeMutableIndexes, final boolean sendAll) { 
        final PTable table = tableRef.getTable();
        final Iterator<PTable> indexes = // Only maintain tables with immutable rows through this client-side mechanism
                (table.isImmutableRows() || includeMutableIndexes) ? 
                        IndexMaintainer.nonDisabledIndexIterator(table.getIndexes().iterator()) : 
                        Iterators.<PTable>emptyIterator();
        final List<Mutation> mutationList = Lists.newArrayListWithExpectedSize(values.size());
        final List<Mutation> mutationsPertainingToIndex = indexes.hasNext() ? Lists.<Mutation>newArrayListWithExpectedSize(values.size()) : null;
        generateMutations(tableRef, timestamp, values, mutationList, mutationsPertainingToIndex);
        return new Iterator<Pair<byte[],List<Mutation>>>() {
            boolean isFirst = true;

            @Override
            public boolean hasNext() {
                return isFirst || indexes.hasNext();
            }

            @Override
            public Pair<byte[], List<Mutation>> next() {
                if (isFirst) {
                    isFirst = false;
                    return new Pair<byte[],List<Mutation>>(table.getPhysicalName().getBytes(), mutationList);
                }
                PTable index = indexes.next();
                List<Mutation> indexMutations;
                try {
                    indexMutations =
                            IndexUtil.generateIndexData(table, index, mutationsPertainingToIndex,
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
                return new Pair<byte[],List<Mutation>>(index.getPhysicalName().getBytes(),indexMutations);
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
        while (iterator.hasNext()) {
            Map.Entry<ImmutableBytesPtr, RowMutationState> rowEntry = iterator.next();
            ImmutableBytesPtr key = rowEntry.getKey();
            RowMutationState state = rowEntry.getValue();
            if (tableWithRowTimestampCol) {
                RowTimestampColInfo rowTsColInfo = state.getRowTimestampColInfo();
                if (rowTsColInfo.useServerTimestamp()) {
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
                            .newRow(connection.getKeyValueBuilder(), timestampToUse, key);
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
                rowMutationsPertainingToIndex = rowMutations;
            }
            mutationList.addAll(rowMutations);
            if (mutationsPertainingToIndex != null) mutationsPertainingToIndex
                    .addAll(rowMutationsPertainingToIndex);
        }
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
        final long timestamp = (tableTimestamp!=null && tableTimestamp!=QueryConstants.UNSET_TIMESTAMP) ? tableTimestamp : (scn == null ? HConstants.LATEST_TIMESTAMP : scn);
        return new Iterator<Pair<byte[],List<Mutation>>>() {
            private Map.Entry<TableRef, Map<ImmutableBytesPtr,RowMutationState>> current = iterator.next();
            private Iterator<Pair<byte[],List<Mutation>>> innerIterator = init();
                    
            private Iterator<Pair<byte[],List<Mutation>>> init() {
                return addRowMutations(current.getKey(), current.getValue(), timestamp, includeMutableIndexes, true);
            }
            
            @Override
            public boolean hasNext() {
                return innerIterator.hasNext() || iterator.hasNext();
            }

            @Override
            public Pair<byte[], List<Mutation>> next() {
                if (!innerIterator.hasNext()) {
                    current = iterator.next();
                }
                return innerIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }
        
	/**
	 * Validates that the meta data is valid against the server meta data if we haven't yet done so.
	 * Otherwise, for every UPSERT VALUES call, we'd need to hit the server to see if the meta data
	 * has changed.
	 * @param connection
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
	    PTable table = tableRef.getTable();
	    // If we're auto committing, we've already validated the schema when we got the ColumnResolver,
	    // so no need to do it again here.
	    if (!connection.getAutoCommit()) {
            MetaDataMutationResult result = client.updateCache(table.getSchemaName().getString(), table.getTableName().getString());
            long timestamp = result.getMutationTime();
            if (timestamp != QueryConstants.UNSET_TIMESTAMP) {
                serverTimeStamp = timestamp;
                if (result.wasUpdated()) {
                    // TODO: use bitset?
                    table = result.getTable();
                    PColumn[] columns = new PColumn[table.getColumns().size()];
                    for (Map.Entry<ImmutableBytesPtr,RowMutationState> rowEntry : rowKeyToColumnMap.entrySet()) {
                    	RowMutationState valueEntry = rowEntry.getValue();
                        if (valueEntry != null) {
                        	Map<PColumn, byte[]> colValues = valueEntry.getColumnValues();
                        	if (colValues != PRow.DELETE_MARKER) {
                                for (PColumn column : colValues.keySet()) {
                                    columns[column.getPosition()] = column;
                                }
                        	}
                        }
                    }
                    for (PColumn column : columns) {
                        if (column != null) {
                            table.getColumnFamily(column.getFamilyName().getString()).getColumn(column.getName().getString());
                        }
                    }
                    tableRef.setTable(table);
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
                        setMetaDataOnMutations(tableRef, deletes, indexMetaDataPtr);
                    }
                }
                delegate.delete(deletes);
            } catch (SQLException e) {
                throw new IOException(e);
            }
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

        // add tracing for this operation
        try (TraceScope trace = Tracing.startNewSpan(connection, "Committing mutations to tables")) {
            Span span = trace.getSpan();
	        ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable();
	        while (tableRefIterator.hasNext()) {
	        	// at this point we are going through mutations for each table
	            TableRef tableRef = tableRefIterator.next();
	            Map<ImmutableBytesPtr, RowMutationState> valuesMap = mutations.get(tableRef);
	            if (valuesMap == null || valuesMap.isEmpty()) {
	                continue;
	            }
	            PTable table = tableRef.getTable();
	            // Track tables to which we've sent uncommitted data
	            if (table.isTransactional()) {
	                uncommittedPhysicalNames.add(table.getPhysicalName().getString());
	            }
	            table.getIndexMaintainers(indexMetaDataPtr, connection);
	            boolean isDataTable = true;
	            // Validate as we go if transactional since we can undo if a problem occurs (which is unlikely)
	            long serverTimestamp = serverTimeStamps == null ? validate(tableRef, valuesMap) : serverTimeStamps[i++];
	            Iterator<Pair<byte[],List<Mutation>>> mutationsIterator = addRowMutations(tableRef, valuesMap, serverTimestamp, false, sendAll);
	            while (mutationsIterator.hasNext()) {
	                Pair<byte[],List<Mutation>> pair = mutationsIterator.next();
	                byte[] htableName = pair.getFirst();
	                List<Mutation> mutationList = pair.getSecond();
	                
	                //create a span per target table
	                //TODO maybe we can be smarter about the table name to string here?
	                Span child = Tracing.child(span,"Writing mutation batch for table: "+Bytes.toString(htableName));
	
	                int retryCount = 0;
	                boolean shouldRetry = false;
	                do {
	                    ServerCache cache = null;
	                    if (isDataTable) {
	                        cache = setMetaDataOnMutations(tableRef, mutationList, indexMetaDataPtr);
	                    }
	                
	                    // If we haven't retried yet, retry for this case only, as it's possible that
	                    // a split will occur after we send the index metadata cache to all known
	                    // region servers.
	                    shouldRetry = cache != null;
	                    SQLException sqlE = null;
	                    HTableInterface hTable = connection.getQueryServices().getTable(htableName);
	                    try {
	                        if (table.isTransactional()) {
	                            // If we have indexes, wrap the HTable in a delegate HTable that
	                            // will attach the necessary index meta data in the event of a
	                            // rollback
	                            if (!table.getIndexes().isEmpty()) {
	                                hTable = new MetaDataAwareHTable(hTable, tableRef);
	                            }
	                            TransactionAwareHTable txnAware = TransactionUtil.getTransactionAwareHTable(hTable, table);
	                            // Don't add immutable indexes (those are the only ones that would participate
	                            // during a commit), as we don't need conflict detection for these.
	                            if (isDataTable) {
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
                            child.addTimelineAnnotation("Attempt " + retryCount);;
	                        hTable.batch(mutationList);
	                        child.stop();
	                        child.stop();
                            shouldRetry = false;
                            long mutationCommitTime = System.currentTimeMillis() - startTime;
                            GLOBAL_MUTATION_COMMIT_TIME.update(mutationCommitTime);
                            
                            long mutationSizeBytes = calculateMutationSize(mutationList);
                            MutationMetric mutationsMetric = new MutationMetric(numMutations, mutationSizeBytes, mutationCommitTime);
                            mutationMetricQueue.addMetricsForTable(Bytes.toString(htableName), mutationsMetric);
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
	                        // Throw to client with both what was committed so far and what is left to be committed.
	                        // That way, client can either undo what was done or try again with what was not done.
	                        sqlE = new CommitException(e, getUncommittedStatementIndexes());
	                    } finally {
	                        try {
	                            if (cache != null) {
	                                cache.close();
	                            }
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
	                            	// clear pending mutations
	                            	mutations.clear();
	                                throw sqlE;
	                            }
	                        }
	                    }
	                } while (shouldRetry && retryCount++ < 1);
	                isDataTable = false;
	            }
	            if (tableRef.getTable().getType() != PTableType.INDEX) {
	                numRows -= valuesMap.size();
	            }
	            // Remove batches as we process them
	            if (sendAll) {
	            	tableRefIterator.remove(); // Iterating through actual map in this case
	            } else {
	            	mutations.remove(tableRef);
	            }
	        }
        }
        // Note that we cannot assume that *all* mutations have been sent, since we've optimized this
        // now to only send the mutations for the tables we're querying, hence we've removed the
        // assertions that we're here before.
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
        byte[] tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
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
            if (tenantId != null) {
                mutation.setAttribute(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            }
            mutation.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
            if (attribValue != null) {
                mutation.setAttribute(PhoenixIndexCodec.INDEX_MD, attribValue);
                if (txState.length > 0) {
                    mutation.setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
                }
            } else if (!hasIndexMetaData && txState.length > 0) {
                mutation.setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
            }
        }
        return cache;
    }
    
    private void clear() throws SQLException {
        this.mutations.clear();
        numRows = 0;
    }
    
    private int[] getUncommittedStatementIndexes() {
    	int[] result = new int[0];
    	for (Map<ImmutableBytesPtr, RowMutationState> rowMutations : mutations.values()) {
    		for (RowMutationState rowMutationState : rowMutations.values()) {
    			result = joinSortedIntArrays(result, rowMutationState.getStatementIndexes());
    		}
    	}
    	return result;
    }
    
    @Override
    public void close() throws SQLException {
    }

    private void reset() {
        txStarted = false;
        tx = null;
        uncommittedPhysicalNames.clear();
    }
    
    public void rollback() throws SQLException {
        clear();
        txAwares.clear();
        if (txContext != null) {
            try {
                if (txStarted) {
                    txContext.abort();
                }
            } catch (TransactionFailureException e) {
                throw new SQLException(e); // TODO: error code
            } finally {
            	reset();
            }
        }
    }
    
    public void commit() throws SQLException {
    	boolean sendMutationsFailed=false;
        try {
            send();
        } catch (Throwable t) {
        	sendMutationsFailed=true;
        	throw t;
        } finally {
            txAwares.clear();
            if (txContext != null) {
                try {
                    if (txStarted && !sendMutationsFailed) {
                        txContext.finish();
                    }
                } catch (TransactionFailureException e) {
                    try {
                        txContext.abort(e);
                        // abort and throw the original commit failure exception
                        throw TransactionUtil.getTransactionFailureException(e);
                    } catch (TransactionFailureException e1) {
                        // if abort fails and throw the abort failure exception
                        throw TransactionUtil.getTransactionFailureException(e1);
                    }
                } finally {
                  	if (!sendMutationsFailed) {
                  		reset();
                  	}
                  }
            }
        }
    }

    /**
     * Support read-your-own-write semantics by sending uncommitted data to HBase prior to running a
     * query. In this way, they are visible to subsequent reads but are not actually committed until
     * commit is called.
     * @param tableRefs
     * @return true if at least partially transactional and false otherwise.
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
        
        public RowMutationState(@Nonnull Map<PColumn,byte[]> columnValues, int statementIndex, @Nonnull RowTimestampColInfo rowTsColInfo) {
            checkNotNull(columnValues);
            checkNotNull(rowTsColInfo);
            this.columnValues = columnValues;
            this.statementIndexes = new int[] {statementIndex};
            this.rowTsColInfo = rowTsColInfo;
        }

        Map<PColumn, byte[]> getColumnValues() {
            return columnValues;
        }

        int[] getStatementIndexes() {
            return statementIndexes;
        }

        void join(RowMutationState newRow) {
            getColumnValues().putAll(newRow.getColumnValues());
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
