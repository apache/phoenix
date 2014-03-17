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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Map<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> mutations = Maps.newHashMapWithExpectedSize(3); // TODO: Sizing?
    private final long sizeOffset;
    private int numRows = 0;

    public MutationState(int maxSize, PhoenixConnection connection) {
        this(maxSize,connection,0);
    }
    
    public MutationState(int maxSize, PhoenixConnection connection, long sizeOffset) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.sizeOffset = sizeOffset;
    }
    
    public MutationState(TableRef table, Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations, long sizeOffset, long maxSize, PhoenixConnection connection) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.mutations.put(table, mutations);
        this.sizeOffset = sizeOffset;
        this.numRows = mutations.size();
        throwIfTooBig();
    }
    
    private MutationState(List<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> entries, long sizeOffset, long maxSize, PhoenixConnection connection) {
        this.maxSize = maxSize;
        this.connection = connection;
        this.sizeOffset = sizeOffset;
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : entries) {
            numRows += entry.getValue().size();
            this.mutations.put(entry.getKey(), entry.getValue());
        }
        throwIfTooBig();
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
        // Merge newMutation with this one, keeping state from newMutation for any overlaps
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : newMutation.mutations.entrySet()) {
            // Replace existing entries for the table with new entries
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> existingRows = this.mutations.put(entry.getKey(), entry.getValue());
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
                        numRows++;
                    }
                }
                // Put the existing one back now that it's merged
                this.mutations.put(entry.getKey(), existingRows);
            } else {
                numRows += entry.getValue().size();
            }
        }
        throwIfTooBig();
    }
    
    private Iterator<Pair<byte[],List<Mutation>>> addRowMutations(final TableRef tableRef, final Map<ImmutableBytesPtr, Map<PColumn, byte[]>> values, long timestamp, boolean includeMutableIndexes) {
        final List<Mutation> mutations = Lists.newArrayListWithExpectedSize(values.size());
        Iterator<Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>>> iterator = values.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry = iterator.next();
            ImmutableBytesPtr key = rowEntry.getKey();
            PRow row = tableRef.getTable().newRow(connection.getKeyValueBuilder(), timestamp, key);
            if (rowEntry.getValue() == PRow.DELETE_MARKER) { // means delete
                row.delete();
            } else {
                for (Map.Entry<PColumn,byte[]> valueEntry : rowEntry.getValue().entrySet()) {
                    row.setValue(valueEntry.getKey(), valueEntry.getValue());
                }
            }
            mutations.addAll(row.toRowMutations());
        }
        final Iterator<PTable> indexes = // Only maintain tables with immutable rows through this client-side mechanism
                (tableRef.getTable().isImmutableRows() || includeMutableIndexes) ? 
                        IndexMaintainer.nonDisabledIndexIterator(tableRef.getTable().getIndexes().iterator()) : 
                        Iterators.<PTable>emptyIterator();
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
                            IndexUtil.generateIndexData(tableRef.getTable(), index, mutations,
                                tempPtr, connection.getKeyValueBuilder());
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
     * Validates that the meta data is still valid based on the current server time
     * and returns the server time to use for the upsert for each table.
     * @param connection
     * @return the server time to use for the upsert
     * @throws SQLException if the table or any columns no longer exist
     */
    private long[] validate() throws SQLException {
        int i = 0;
        Long scn = connection.getSCN();
        PName tenantId = connection.getTenantId();
        MetaDataClient client = new MetaDataClient(connection);
        long[] timeStamps = new long[this.mutations.size()];
        for (Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry : mutations.entrySet()) {
            TableRef tableRef = entry.getKey();
            long serverTimeStamp = tableRef.getTimeStamp();
            PTable table = tableRef.getTable();
            if (!connection.getAutoCommit()) {
                MetaDataMutationResult result = client.updateCache(table.getSchemaName().getString(), table.getTableName().getString());
                long timestamp = result.getMutationTime();
                if (timestamp != QueryConstants.UNSET_TIMESTAMP) {
                    serverTimeStamp = timestamp;
                    if (result.wasUpdated()) {
                        // TODO: use bitset?
                        PColumn[] columns = new PColumn[table.getColumns().size()];
                        for (Map.Entry<ImmutableBytesPtr,Map<PColumn,byte[]>> rowEntry : entry.getValue().entrySet()) {
                            Map<PColumn,byte[]> valueEntry = rowEntry.getValue();
                            if (valueEntry != PRow.DELETE_MARKER) {
                                for (PColumn column : valueEntry.keySet()) {
                                    columns[column.getPosition()] = column;
                                }
                            }
                        }
                        table = connection.getMetaDataCache().getTable(new PTableKey(tenantId, table.getName().getString()));
                        for (PColumn column : columns) {
                            if (column != null) {
                                table.getColumnFamily(column.getFamilyName().getString()).getColumn(column.getName().getString());
                            }
                        }
                    }
                }
            }
            timeStamps[i++] = scn == null ? serverTimeStamp == QueryConstants.UNSET_TIMESTAMP ? HConstants.LATEST_TIMESTAMP : serverTimeStamp : scn;
        }
        return timeStamps;
    }
    
    private static void logMutationSize(HTableInterface htable, List<Mutation> mutations) {
        long byteSize = 0;
        int keyValueCount = 0;
        for (Mutation mutation : mutations) {
            if (mutation.getFamilyCellMap() != null) { // Not a Delete of the row
                for (Entry<byte[], List<Cell>> entry : mutation.getFamilyCellMap().entrySet()) {
                    if (entry.getValue() != null) {
                        for (Cell kv : entry.getValue()) {
                            byteSize += CellUtil.estimatedSizeOf(kv);
                            keyValueCount++;
                        }
                    }
                }
            }
        }
        logger.debug("Sending " + mutations.size() + " mutations for " + Bytes.toString(htable.getTableName()) + " with " + keyValueCount + " key values of total size " + byteSize + " bytes");
    }
    
    @SuppressWarnings("deprecation")
    public void commit() throws SQLException {
        int i = 0;
        byte[] tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
        long[] serverTimeStamps = validate();
        Iterator<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> iterator = this.mutations.entrySet().iterator();
        List<Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>>> committedList = Lists.newArrayListWithCapacity(this.mutations.size());
        while (iterator.hasNext()) {
            Map.Entry<TableRef, Map<ImmutableBytesPtr,Map<PColumn,byte[]>>> entry = iterator.next();
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> valuesMap = entry.getValue();
            TableRef tableRef = entry.getKey();
            PTable table = tableRef.getTable();
            table.getIndexMaintainers(tempPtr);
            boolean hasIndexMaintainers = tempPtr.getLength() > 0;
            boolean isDataTable = true;
            long serverTimestamp = serverTimeStamps[i++];
            Iterator<Pair<byte[],List<Mutation>>> mutationsIterator = addRowMutations(tableRef, valuesMap, serverTimestamp, false);
            while (mutationsIterator.hasNext()) {
                Pair<byte[],List<Mutation>> pair = mutationsIterator.next();
                byte[] htableName = pair.getFirst();
                List<Mutation> mutations = pair.getSecond();
                
                int retryCount = 0;
                boolean shouldRetry = false;
                do {
                    ServerCache cache = null;
                    if (hasIndexMaintainers && isDataTable) {
                        byte[] attribValue = null;
                        byte[] uuidValue;
                        if (IndexMetaDataCacheClient.useIndexMetadataCache(connection, mutations, tempPtr.getLength())) {
                            IndexMetaDataCacheClient client = new IndexMetaDataCacheClient(connection, tableRef);
                            cache = client.addIndexMetadataCache(mutations, tempPtr);
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
                            }
                        }
                    }
                    
                    SQLException sqlE = null;
                    HTableInterface hTable = connection.getQueryServices().getTable(htableName);
                    try {
                        if (logger.isDebugEnabled()) logMutationSize(hTable, mutations);
                        long startTime = System.currentTimeMillis();
                        hTable.batch(mutations);
                        shouldRetry = false;
                        if (logger.isDebugEnabled()) logger.debug("Total time for batch call of  " + mutations.size() + " mutations into " + table.getName().getString() + ": " + (System.currentTimeMillis() - startTime) + " ms");
                        committedList.add(entry);
                    } catch (Exception e) {
                        SQLException inferredE = ServerUtil.parseServerExceptionOrNull(e);
                        if (inferredE != null) {
                            if (shouldRetry && retryCount == 0 && inferredE.getErrorCode() == SQLExceptionCode.INDEX_METADATA_NOT_FOUND.getErrorCode()) {
                                // Swallow this exception once, as it's possible that we split after sending the index metadata
                                // and one of the region servers doesn't have it. This will cause it to have it the next go around.
                                // If it fails again, we don't retry.
                                logger.warn("Swallowing exception and retrying after clearing meta cache on connection. " + inferredE);
                                connection.getQueryServices().clearTableRegionCache(htableName);
                                continue;
                            }
                            e = inferredE;
                        }
                        // Throw to client with both what was committed so far and what is left to be committed.
                        // That way, client can either undo what was done or try again with what was not done.
                        sqlE = new CommitException(e, this, new MutationState(committedList, this.sizeOffset, this.maxSize, this.connection));
                    } finally {
                        try {
                            hTable.close();
                        } catch (IOException e) {
                            if (sqlE != null) {
                                sqlE.setNextException(ServerUtil.parseServerException(e));
                            } else {
                                sqlE = ServerUtil.parseServerException(e);
                            }
                        } finally {
                            try {
                                if (cache != null) {
                                    cache.close();
                                }
                            } finally {
                                if (sqlE != null) {
                                    throw sqlE;
                                }
                            }
                        }
                    }
                } while (shouldRetry && retryCount++ < 1);
                isDataTable = false;
            }
            numRows -= entry.getValue().size();
            iterator.remove(); // Remove batches as we process them
        }
        assert(numRows==0);
        assert(this.mutations.isEmpty());
    }
    
    public void rollback(PhoenixConnection connection) throws SQLException {
        this.mutations.clear();
        numRows = 0;
    }
    
    @Override
    public void close() throws SQLException {
    }
}
