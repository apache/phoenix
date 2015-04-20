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

import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MUTATION_COMMIT_TIME;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.hbase98.TransactionAwareHTable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.PhoenixMetrics;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Tracks the uncommitted state
 *
 * 
 * @since 0.1
 */
public class MutationState implements SQLCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MutationState.class);

    private PhoenixConnection connection;
    private final long maxSize;
    private final ImmutableBytesPtr tempPtr = new ImmutableBytesPtr();
    // map from table to rows 
    //   rows - map from rowkey to columns
    //      columns - map from column to value
    private final Map<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> mutations = Maps.newHashMapWithExpectedSize(3); // TODO: Sizing?
    private final Transaction tx;
    private final List<TransactionAware> txAwares;
    private final TransactionContext txContext;
    
    private long sizeOffset;
    private int numRows = 0;
    private boolean txStarted = false;
    
    public MutationState(int maxSize, PhoenixConnection connection) {
        this(maxSize,connection,null);
    }
    
    public MutationState(int maxSize, PhoenixConnection connection, Transaction tx) {
        this(maxSize,connection, tx, 0);
    }
    
    public MutationState(int maxSize, PhoenixConnection connection, long sizeOffset) {
        this(maxSize, connection, null, sizeOffset);
    }
    
    public MutationState(int maxSize, PhoenixConnection connection, Transaction tx, long sizeOffset) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.sizeOffset = sizeOffset;
        this.tx = tx;
        if (tx == null) {
          this.txAwares = Collections.emptyList();
          TransactionSystemClient txServiceClient = this.connection.getQueryServices().getTransactionSystemClient();
          this.txContext = new TransactionContext(txServiceClient);
        } else {
            txAwares = Lists.newArrayList();
            txContext = null;
        }
    }
    
    public MutationState(TableRef table, Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations, long sizeOffset, long maxSize, PhoenixConnection connection) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.mutations.put(table, mutations);
        this.sizeOffset = sizeOffset;
        this.numRows = mutations.size();
        this.txAwares = Lists.newArrayList();
        this.txContext = null;
        this.tx = connection.getMutationState().getTransaction();
        throwIfTooBig();
    }
    
    private void addTxParticipant(TransactionAware txAware) throws SQLException {
        if (txContext == null) {
            txAwares.add(txAware);
            assert(tx != null);
            txAware.startTx(tx);
        } else {
            txContext.addTransactionAware(txAware);
        }
    }
    
    public Transaction getTransaction() {
        return tx != null ? tx : txContext != null ? txContext.getCurrentTransaction() : null;
    }
    
    public boolean startTransaction() throws SQLException {
        if (txContext == null) {
            throw new SQLException("No transaction context"); // TODO: error code
        }
        
        try {
            if (!txStarted) {
                txContext.start();
                txStarted = true;
                return true;
            }
        } catch (TransactionFailureException e) {
            throw new SQLException(e); // TODO: error code
        }
        return false;
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
     * Combine a newer mutation with this one, where in the event of overlaps,
     * the newer one will take precedence.
     * @param newMutation the newer mutation
     */
    public void join(MutationState newMutation) {
        if (this == newMutation) { // Doesn't make sense
            return;
        }
        // TODO: what if new and old have txContext as that's really an error
        // Really it's an error if newMutation txContext is not null
        if (txContext != null) {
            for (TransactionAware txAware : txAwares) {
                txContext.addTransactionAware(txAware);
            }
        } else {
            txAwares.addAll(newMutation.txAwares);
        }
        this.sizeOffset += newMutation.sizeOffset;
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : newMutation.mutations.entrySet()) {
            // Replace existing entries for the table with new entries
            TableRef tableRef = entry.getKey();
            PTable table = tableRef.getTable();
            boolean isIndex = table.getType() == PTableType.INDEX;
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> existingRows = this.mutations.put(tableRef, entry.getValue());
            if (existingRows != null) { // Rows for that table already exist
                // Loop through new rows and replace existing with new
                for (Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry : entry.getValue().entrySet()) {
                    // Replace existing row with new row
                    Map<PColumn,byte[]> existingValues = existingRows.put(rowEntry.getKey(), rowEntry.getValue());
                    if (existingValues != null) {
                        if (existingValues != PRow.DELETE_MARKER) {
                            Map<PColumn,byte[]> newRow = rowEntry.getValue();
                            // if new row is PRow.DELETE_MARKER, it means delete, and we don't need to merge it with existing row. 
                            if (newRow != PRow.DELETE_MARKER) {
                                // Replace existing column values with new column values
                                for (Map.Entry<PColumn,byte[]> valueEntry : newRow.entrySet()) {
                                    existingValues.put(valueEntry.getKey(), valueEntry.getValue());
                                }
                                // Now that the existing row has been merged with the new row, replace it back
                                // again (since it was replaced with the new one above).
                                existingRows.put(rowEntry.getKey(), existingValues);
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
        throwIfTooBig();
    }
    
    private Iterator<Pair<byte[],List<Mutation>>> addRowMutations(final TableRef tableRef, final Map<ImmutableBytesPtr, Map<PColumn, byte[]>> values, long timestamp, boolean includeMutableIndexes) {
        final Iterator<PTable> indexes = // Only maintain tables with immutable rows through this client-side mechanism
                (tableRef.getTable().isImmutableRows() || includeMutableIndexes) ? 
                        IndexMaintainer.nonDisabledIndexIterator(tableRef.getTable().getIndexes().iterator()) : 
                        Iterators.<PTable>emptyIterator();
        final List<Mutation> mutations = Lists.newArrayListWithExpectedSize(values.size());
        final List<Mutation> mutationsPertainingToIndex = indexes.hasNext() ? Lists.<Mutation>newArrayListWithExpectedSize(values.size()) : null;
        Iterator<Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>>> iterator = values.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry = iterator.next();
            ImmutableBytesPtr key = rowEntry.getKey();
            PRow row = tableRef.getTable().newRow(connection.getKeyValueBuilder(), timestamp, key);
            List<Mutation> rowMutations, rowMutationsPertainingToIndex;
            if (rowEntry.getValue() == PRow.DELETE_MARKER) { // means delete
                row.delete();
                rowMutations = row.toRowMutations();
                // Row deletes for index tables are processed by running a re-written query
                // against the index table (as this allows for flexibility in being able to
                // delete rows).
                rowMutationsPertainingToIndex = Collections.emptyList();
            } else {
                for (Map.Entry<PColumn,byte[]> valueEntry : rowEntry.getValue().entrySet()) {
                    row.setValue(valueEntry.getKey(), valueEntry.getValue());
                }
                rowMutations = row.toRowMutations();
                rowMutationsPertainingToIndex = rowMutations;
            }
            mutations.addAll(rowMutations);
            if (mutationsPertainingToIndex != null) mutationsPertainingToIndex.addAll(rowMutationsPertainingToIndex);
        }
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
                    return new Pair<byte[],List<Mutation>>(tableRef.getTable().getPhysicalName().getBytes(),mutations);
                }
                PTable index = indexes.next();
                List<Mutation> indexMutations;
                try {
                    indexMutations =
                            IndexUtil.generateIndexData(tableRef.getTable(), index, mutationsPertainingToIndex,
                                tempPtr, connection.getKeyValueBuilder(), connection);
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
    
    /**
     * Get the unsorted list of HBase mutations for the tables with uncommitted data.
     * @return list of HBase mutations for uncommitted data.
     */
    public Iterator<Pair<byte[],List<Mutation>>> toMutations() {
        return toMutations(false);
    }
    
    public Iterator<Pair<byte[],List<Mutation>>> toMutations(final boolean includeMutableIndexes) {
        final Iterator<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> iterator = this.mutations.entrySet().iterator();
        if (!iterator.hasNext()) {
            return Iterators.emptyIterator();
        }
        Long scn = connection.getSCN();
        final long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        return new Iterator<Pair<byte[],List<Mutation>>>() {
            private Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> current = iterator.next();
            private Iterator<Pair<byte[],List<Mutation>>> innerIterator = init();
                    
            private Iterator<Pair<byte[],List<Mutation>>> init() {
                return addRowMutations(current.getKey(), current.getValue(), timestamp, includeMutableIndexes);
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
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : mutations.entrySet()) {
            TableRef tableRef = entry.getKey();
            timeStamps[i++] = validate(tableRef, entry.getValue());
        }
        return timeStamps;
    }
    
    private long validate(TableRef tableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>> values) throws SQLException {
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
                    for (Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry : values.entrySet()) {
                        Map<PColumn,byte[]> valueEntry = rowEntry.getValue();
                        if (valueEntry != PRow.DELETE_MARKER) {
                            for (PColumn column : valueEntry.keySet()) {
                                columns[column.getPosition()] = column;
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
    
    private static void logMutationSize(HTableInterface htable, List<Mutation> mutations, PhoenixConnection connection) {
        long byteSize = 0;
        int keyValueCount = 0;
        if (PhoenixMetrics.isMetricsEnabled() || logger.isDebugEnabled()) {
            for (Mutation mutation : mutations) {
                byteSize += mutation.heapSize();
            }
            MUTATION_BYTES.update(byteSize);
            if (logger.isDebugEnabled()) {
                logger.debug(LogUtil.addCustomAnnotations("Sending " + mutations.size() + " mutations for " + Bytes.toString(htable.getTableName()) + " with " + keyValueCount + " key values of total size " + byteSize + " bytes", connection));
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    private void send(Iterator<TableRef> tableRefs) throws SQLException {
        int i = 0;
        long[] serverTimeStamps = null;
        byte[] tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
        // Validate up front if not transactional so that we 
        if (tableRefs == null) {
            serverTimeStamps = validateAll();
            tableRefs = mutations.keySet().iterator();
        }

        // add tracing for this operation
        TraceScope trace = Tracing.startNewSpan(connection, "Committing mutations to tables");
        Span span = trace.getSpan();
        while (tableRefs.hasNext()) {
            TableRef tableRef = tableRefs.next();
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> valuesMap = mutations.get(tableRef);
            if (valuesMap == null) {
                continue;
            }
            PTable table = tableRef.getTable();
            table.getIndexMaintainers(tempPtr, connection);
            boolean hasIndexMaintainers = tempPtr.getLength() > 0;
            boolean isDataTable = true;
            // Validate as we go if transactional since we can undo if a problem occurs (which is unlikely)
            long serverTimestamp = serverTimeStamps == null ? validate(tableRef, valuesMap) : serverTimeStamps[i++];
            Iterator<Pair<byte[],List<Mutation>>> mutationsIterator = addRowMutations(tableRef, valuesMap, serverTimestamp, false);
            while (mutationsIterator.hasNext()) {
                Pair<byte[],List<Mutation>> pair = mutationsIterator.next();
                byte[] htableName = pair.getFirst();
                List<Mutation> mutations = pair.getSecond();
                
                //create a span per target table
                //TODO maybe we can be smarter about the table name to string here?
                Span child = Tracing.child(span,"Writing mutation batch for table: "+Bytes.toString(htableName));

                int retryCount = 0;
                boolean shouldRetry = false;
                do {
                    ServerCache cache = null;
                    if (hasIndexMaintainers && isDataTable) {
                        byte[] attribValue = null;
                        byte[] uuidValue;
                        byte[] txState = ByteUtil.EMPTY_BYTE_ARRAY;
                        if (table.isTransactional()) {
                            txState = TransactionUtil.encodeTxnState(getTransaction());
                        }
                        if (IndexMetaDataCacheClient.useIndexMetadataCache(connection, mutations, tempPtr.getLength() + txState.length)) {
                            IndexMetaDataCacheClient client = new IndexMetaDataCacheClient(connection, tableRef);
                            cache = client.addIndexMetadataCache(mutations, tempPtr, txState);
                            child.addTimelineAnnotation("Updated index metadata cache");
                            uuidValue = cache.getId();
                            // If we haven't retried yet, retry for this case only, as it's possible that
                            // a split will occur after we send the index metadata cache to all known
                            // region servers.
                            shouldRetry = true;
                        } else {
                            attribValue = ByteUtil.copyKeyBytesIfNecessary(tempPtr);
                            uuidValue = ServerCacheClient.generateId();
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
                                mutation.setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
                            }
                        }
                    }
                
                    SQLException sqlE = null;
                    HTableInterface hTable = connection.getQueryServices().getTable(htableName);
                    try {
                        if (table.isTransactional()) {
                            TransactionAwareHTable txnAware = TransactionUtil.getTransactionAwareHTable(hTable);
                            // Don't add immutable indexes (those are the only ones that would participate
                            // during a commit), as we don't need conflict detection for these.
                            if (isDataTable) {
                                addTxParticipant(txnAware);
                            }
                            hTable = txnAware;
                        }
                        logMutationSize(hTable, mutations, connection);
                        MUTATION_BATCH_SIZE.update(mutations.size());
                        long startTime = System.currentTimeMillis();
                        child.addTimelineAnnotation("Attempt " + retryCount);
                        hTable.batch(mutations);
                        child.stop();
                        long duration = System.currentTimeMillis() - startTime;
                        MUTATION_COMMIT_TIME.update(duration);
                        shouldRetry = false;
                        if (logger.isDebugEnabled()) logger.debug(LogUtil.addCustomAnnotations("Total time for batch call of  " + mutations.size() + " mutations into " + table.getName().getString() + ": " + duration + " ms", connection));
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
                        sqlE = new CommitException(e, this);
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
            valuesMap.remove(tableRef); // Remove batches as we process them
        }
        trace.close();
        assert(numRows==0);
        assert(this.mutations.isEmpty());
    }
    
    public void clear() throws SQLException {
        this.mutations.clear();
        numRows = 0;
    }
    
    @Override
    public void close() throws SQLException {
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
                txStarted = false;
            }
        }
    }
    
    public void commit() throws SQLException {
        try {
            send();
        } finally {
            txAwares.clear();
            if (txContext != null) {
                try {
                    if (txStarted) {
                        txContext.finish();
                    }
                } catch (TransactionFailureException e) {
                    try {
                        txContext.abort(e);
                        throw TransactionUtil.getSQLException(e);
                    } catch (TransactionFailureException e1) {
                        throw TransactionUtil.getSQLException(e);
                    }
                } finally {
                    txStarted = false;
                }
            }
        }
    }

    /**
     * Send mutations to hbase, so they are visible to subsequent reads,
     * starting a transaction if transactional and one has not yet been started.
     * @param tableRefs
     * @return true if at least partially transactional and false otherwise.
     * @throws SQLException
     */
    public boolean startTransaction(Iterator<TableRef> tableRefs) throws SQLException {
        Iterator<TableRef> filteredTableRefs = Iterators.filter(tableRefs, new Predicate<TableRef>(){
            @Override
            public boolean apply(TableRef tableRef) {
                return tableRef.getTable().isTransactional();
            }
        });
        if (filteredTableRefs.hasNext()) {
            startTransaction();
            send(filteredTableRefs);
            return true;
        }
        return false;
    }
        
    public void send() throws SQLException {
        send(null);
    }
}
