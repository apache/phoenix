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
package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TimeKeeper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Longs;

/**
 * 
 * Client-side cache of MetaData. Not thread safe, but meant to be used
 * in a copy-on-write fashion. Internally uses a LinkedHashMap that evicts
 * the oldest entries when size grows beyond the maxSize specified at
 * create time.
 *
 */
public class PMetaDataImpl implements PMetaData {
        private static class PMetaDataCache implements Cloneable {
            private static final int MIN_REMOVAL_SIZE = 3;
            private static final Comparator<PTableRef> COMPARATOR = new Comparator<PTableRef>() {
                @Override
                public int compare(PTableRef tableRef1, PTableRef tableRef2) {
                    return Longs.compare(tableRef1.getLastAccessTime(), tableRef2.getLastAccessTime());
                }
            };
            private static final MinMaxPriorityQueue.Builder<PTableRef> BUILDER = MinMaxPriorityQueue.orderedBy(COMPARATOR);
            
            private long currentByteSize;
            private final long maxByteSize;
            private final int expectedCapacity;
            private final TimeKeeper timeKeeper;

            private final Map<PTableKey,PTableRef> tables;
            private final Map<PTableKey,PFunction> functions;
            private final Map<PTableKey,PSchema> schemas;
            
            private static Map<PTableKey,PTableRef> newMap(int expectedCapacity) {
                // Use regular HashMap, as we cannot use a LinkedHashMap that orders by access time
                // safely across multiple threads (as the underlying collection is not thread safe).
                // Instead, we track access time and prune it based on the copy we've made.
                return Maps.newHashMapWithExpectedSize(expectedCapacity);
            }

            private static Map<PTableKey,PFunction> newFunctionMap(int expectedCapacity) {
                // Use regular HashMap, as we cannot use a LinkedHashMap that orders by access time
                // safely across multiple threads (as the underlying collection is not thread safe).
                // Instead, we track access time and prune it based on the copy we've made.
                return Maps.newHashMapWithExpectedSize(expectedCapacity);
            }

            private static Map<PTableKey,PSchema> newSchemaMap(int expectedCapacity) {
                // Use regular HashMap, as we cannot use a LinkedHashMap that orders by access time
                // safely across multiple threads (as the underlying collection is not thread safe).
                // Instead, we track access time and prune it based on the copy we've made.
                return Maps.newHashMapWithExpectedSize(expectedCapacity);
            }

            private static Map<PTableKey,PTableRef> cloneMap(Map<PTableKey,PTableRef> tables, int expectedCapacity) {
                Map<PTableKey,PTableRef> newTables = newMap(Math.max(tables.size(),expectedCapacity));
                // Copy value so that access time isn't changing anymore
                for (PTableRef tableAccess : tables.values()) {
                    newTables.put(tableAccess.getTable().getKey(), new PTableRef(tableAccess));
                }
                return newTables;
            }

            private static Map<PTableKey, PSchema> cloneSchemaMap(Map<PTableKey, PSchema> schemas, int expectedCapacity) {
                Map<PTableKey, PSchema> newSchemas = newSchemaMap(Math.max(schemas.size(), expectedCapacity));
                // Copy value so that access time isn't changing anymore
                for (PSchema schema : schemas.values()) {
                    newSchemas.put(schema.getSchemaKey(), new PSchema(schema));
                }
                return newSchemas;
            }

            private static Map<PTableKey,PFunction> cloneFunctionsMap(Map<PTableKey,PFunction> functions, int expectedCapacity) {
                Map<PTableKey,PFunction> newFunctions = newFunctionMap(Math.max(functions.size(),expectedCapacity));
                for (PFunction functionAccess : functions.values()) {
                    newFunctions.put(functionAccess.getKey(), new PFunction(functionAccess));
                }
                return newFunctions;
            }

            private PMetaDataCache(PMetaDataCache toClone) {
                this.timeKeeper = toClone.timeKeeper;
                this.maxByteSize = toClone.maxByteSize;
                this.currentByteSize = toClone.currentByteSize;
                this.expectedCapacity = toClone.expectedCapacity;
                this.tables = cloneMap(toClone.tables, expectedCapacity);
                this.functions = cloneFunctionsMap(toClone.functions, expectedCapacity);
                this.schemas = cloneSchemaMap(toClone.schemas, expectedCapacity);
            }
            
            public PMetaDataCache(int initialCapacity, long maxByteSize, TimeKeeper timeKeeper) {
                this.currentByteSize = 0;
                this.maxByteSize = maxByteSize;
                this.expectedCapacity = initialCapacity;
                this.tables = newMap(this.expectedCapacity);
                this.functions = newFunctionMap(this.expectedCapacity);
                this.timeKeeper = timeKeeper;
                this.schemas = newSchemaMap(this.expectedCapacity);
            }
            
            public PTableRef get(PTableKey key) {
                PTableRef tableAccess = this.tables.get(key);
                if (tableAccess == null) {
                    return null;
                }
                tableAccess.setLastAccessTime(timeKeeper.getCurrentTime());
                return tableAccess;
            }
            
            @Override
            public PMetaDataCache clone() {
                return new PMetaDataCache(this);
            }

            /**
             * Used when the cache is growing past its max size to clone in a single pass.
             * Removes least recently used tables to get size of cache below its max size by
             * the overage amount.
             */
            public PMetaDataCache cloneMinusOverage(long overage) {
                assert(overage > 0);
                int nToRemove = Math.max(MIN_REMOVAL_SIZE, (int)Math.ceil((currentByteSize-maxByteSize) / ((double)currentByteSize / size())) + 1);
                MinMaxPriorityQueue<PTableRef> toRemove = BUILDER.expectedSize(nToRemove).create();
                PMetaDataCache newCache = new PMetaDataCache(this.size(), this.maxByteSize, this.timeKeeper);
                
                long toRemoveBytes = 0;
                // Add to new cache, but track references to remove when done
                // to bring cache at least overage amount below it's max size.
                for (PTableRef tableRef : this.tables.values()) {
                    newCache.put(tableRef.getTable().getKey(), new PTableRef(tableRef));
                    toRemove.add(tableRef);
                    toRemoveBytes += tableRef.getEstSize();
                    while (toRemoveBytes - toRemove.peekLast().getEstSize() >= overage) {
                        PTableRef removedRef = toRemove.removeLast();
                        toRemoveBytes -= removedRef.getEstSize();
                    }
                }
                for (PTableRef toRemoveRef : toRemove) {
                    newCache.remove(toRemoveRef.getTable().getKey());
                }
                return newCache;
            }

            private PTable put(PTableKey key, PTableRef ref) {
                currentByteSize += ref.getEstSize();
                PTableRef oldTableAccess = this.tables.put(key, ref);
                PTable oldTable = null;
                if (oldTableAccess != null) {
                    currentByteSize -= oldTableAccess.getEstSize();
                    oldTable = oldTableAccess.getTable();
                }
                return oldTable;
            }

            public PTable put(PTableKey key, PTable value, long resolvedTime) {
                return put(key, new PTableRef(value, timeKeeper.getCurrentTime(), resolvedTime));
            }
            
            public PTable putDuplicate(PTableKey key, PTable value, long resolvedTime) {
                return put(key, new PTableRef(value, timeKeeper.getCurrentTime(), 0, resolvedTime));
            }
            
            public long getAge(PTableRef ref) {
                return timeKeeper.getCurrentTime() - ref.getCreateTime();
            }
            
            public PTable remove(PTableKey key) {
                PTableRef value = this.tables.remove(key);
                if (value == null) {
                    return null;
                }
                currentByteSize -= value.getEstSize();
                return value.getTable();
            }
            
            public Iterator<PTable> iterator() {
                final Iterator<PTableRef> iterator = this.tables.values().iterator();
                return new Iterator<PTable>() {

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public PTable next() {
                        return iterator.next().getTable();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                    
                };
            }

            public int size() {
                return this.tables.size();
            }

            public long getCurrentSize() {
                return this.currentByteSize;
            }

            public long getMaxSize() {
                return this.maxByteSize;
            }
        }
            
    private final PMetaDataCache metaData;
    
    public PMetaDataImpl(int initialCapacity, long maxByteSize) {
        this.metaData = new PMetaDataCache(initialCapacity, maxByteSize, TimeKeeper.SYSTEM);
    }

    public PMetaDataImpl(int initialCapacity, long maxByteSize, TimeKeeper timeKeeper) {
        this.metaData = new PMetaDataCache(initialCapacity, maxByteSize, timeKeeper);
    }

    private PMetaDataImpl(PMetaDataCache metaData) {
        this.metaData = metaData.clone();
    }
    
    @Override
    public PMetaDataImpl clone() {
        return new PMetaDataImpl(this.metaData);
    }
    
    @Override
    public PTableRef getTableRef(PTableKey key) throws TableNotFoundException {
        PTableRef ref = metaData.get(key);
        if (ref == null) {
            throw new TableNotFoundException(key.getName());
        }
        return ref;
    }

    @Override
    public PFunction getFunction(PTableKey key) throws FunctionNotFoundException {
        PFunction function = metaData.functions.get(key);
        if (function == null) {
            throw new FunctionNotFoundException(key.getName());
        }
        return function;
    }

    @Override
    public int size() {
        return metaData.size();
    }

    @Override
    public PMetaData updateResolvedTimestamp(PTable table, long resolvedTimestamp) throws SQLException {
    	PMetaDataCache clone = metaData.clone();
    	clone.putDuplicate(table.getKey(), table, resolvedTimestamp);
    	return new PMetaDataImpl(clone);
    }

    @Override
    public PMetaData addTable(PTable table, long resolvedTime) throws SQLException {
        int netGain = 0;
        PTableKey key = table.getKey();
        PTableRef oldTableRef = metaData.get(key);
        if (oldTableRef != null) {
            netGain -= oldTableRef.getEstSize();
        }
        PTable newParentTable = null;
        long parentResolvedTimestamp = resolvedTime;
        if (table.getParentName() != null) { // Upsert new index table into parent data table list
            String parentName = table.getParentName().getString();
            PTableRef oldParentRef = metaData.get(new PTableKey(table.getTenantId(), parentName));
            // If parentTable isn't cached, that's ok we can skip this
            if (oldParentRef != null) {
                List<PTable> oldIndexes = oldParentRef.getTable().getIndexes();
                List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size() + 1);
                newIndexes.addAll(oldIndexes);
                for (int i = 0; i < newIndexes.size(); i++) {
                    PTable index = newIndexes.get(i);
                    if (index.getName().equals(table.getName())) {
                        newIndexes.remove(i);
                        break;
                    }
                }
                newIndexes.add(table);
                netGain -= oldParentRef.getEstSize();
                newParentTable = PTableImpl.makePTable(oldParentRef.getTable(), table.getTimeStamp(), newIndexes);
                netGain += newParentTable.getEstimatedSize();
            }
        }
        if (newParentTable == null) { // Don't count in gain if we found a parent table, as its accounted for in newParentTable
            netGain += table.getEstimatedSize();
        }
        long overage = metaData.getCurrentSize() + netGain - metaData.getMaxSize();
        PMetaDataCache newMetaData = overage <= 0 ? metaData.clone() : metaData.cloneMinusOverage(overage);
        
        if (newParentTable != null) { // Upsert new index table into parent data table list
            newMetaData.put(newParentTable.getKey(), newParentTable, parentResolvedTimestamp);
            newMetaData.putDuplicate(table.getKey(), table, resolvedTime);
        } else {
            newMetaData.put(table.getKey(), table, resolvedTime);
        }
        for (PTable index : table.getIndexes()) {
            newMetaData.putDuplicate(index.getKey(), index, resolvedTime);
        }
        return new PMetaDataImpl(newMetaData);
    }

    @Override
    public PMetaData addColumn(PName tenantId, String tableName, List<PColumn> columnsToAdd, long tableTimeStamp,
            long tableSeqNum, boolean isImmutableRows, boolean isWalDisabled, boolean isMultitenant, boolean storeNulls,
            boolean isTransactional, long updateCacheFrequency, boolean isNamespaceMapped, long resolvedTime)
                    throws SQLException {
        PTableRef oldTableRef = metaData.get(new PTableKey(tenantId, tableName));
        if (oldTableRef == null) {
            return this;
        }
        List<PColumn> oldColumns = PTableImpl.getColumnsToClone(oldTableRef.getTable());
        List<PColumn> newColumns;
        if (columnsToAdd.isEmpty()) {
            newColumns = oldColumns;
        } else {
            newColumns = Lists.newArrayListWithExpectedSize(oldColumns.size() + columnsToAdd.size());
            newColumns.addAll(oldColumns);
            newColumns.addAll(columnsToAdd);
        }
        PTable newTable = PTableImpl.makePTable(oldTableRef.getTable(), tableTimeStamp, tableSeqNum, newColumns,
                isImmutableRows, isWalDisabled, isMultitenant, storeNulls, isTransactional, updateCacheFrequency,
                isNamespaceMapped);
        return addTable(newTable, resolvedTime);
    }

    @Override
    public PMetaData removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp) throws SQLException {
        PMetaDataCache tables = null;
        PTableRef parentTableRef = null;
        PTableKey key = new PTableKey(tenantId, tableName);
        if (metaData.get(key) == null) {
            if (parentTableName != null) {
                parentTableRef = metaData.get(new PTableKey(tenantId, parentTableName));
            }
            if (parentTableRef == null) {
                return this;
            }
        } else {
            tables = metaData.clone();
            PTable table = tables.remove(key);
            for (PTable index : table.getIndexes()) {
                tables.remove(index.getKey());
            }
            if (table.getParentName() != null) {
                parentTableRef = tables.get(new PTableKey(tenantId, table.getParentName().getString()));
            }
        }
        // also remove its reference from parent table
        if (parentTableRef != null) {
            List<PTable> oldIndexes = parentTableRef.getTable().getIndexes();
            if(oldIndexes != null && !oldIndexes.isEmpty()) {
                List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size());
                newIndexes.addAll(oldIndexes);
                for (int i = 0; i < newIndexes.size(); i++) {
                    PTable index = newIndexes.get(i);
                    if (index.getName().getString().equals(tableName)) {
                        newIndexes.remove(i);
                        PTable parentTable = PTableImpl.makePTable(
                                parentTableRef.getTable(),
                                tableTimeStamp == HConstants.LATEST_TIMESTAMP ? parentTableRef.getTable().getTimeStamp() : tableTimeStamp,
                                newIndexes);
                        if (tables == null) { 
                            tables = metaData.clone();
                        }
                        tables.put(parentTable.getKey(), parentTable, parentTableRef.getResolvedTimeStamp());
                        break;
                    }
                }
            }
        }
        return tables == null ? this : new PMetaDataImpl(tables);
    }
    
    @Override
    public PMetaData removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp, long tableSeqNum, long resolvedTime) throws SQLException {
        PTableRef tableRef = metaData.get(new PTableKey(tenantId, tableName));
        if (tableRef == null) {
            return this;
        }
        PTable table = tableRef.getTable();
        PMetaDataCache tables = metaData.clone();
        for (PColumn columnToRemove : columnsToRemove) {
            PColumn column;
            String familyName = columnToRemove.getFamilyName().getString();
            if (familyName == null) {
                column = table.getPKColumn(columnToRemove.getName().getString());
            } else {
                column = table.getColumnFamily(familyName).getPColumnForColumnName(columnToRemove.getName().getString());
            }
            int positionOffset = 0;
            int position = column.getPosition();
            List<PColumn> oldColumns = table.getColumns();
            if (table.getBucketNum() != null) {
                position--;
                positionOffset = 1;
                oldColumns = oldColumns.subList(positionOffset, oldColumns.size());
            }
            List<PColumn> columns = Lists.newArrayListWithExpectedSize(oldColumns.size() - 1);
            columns.addAll(oldColumns.subList(0, position));
            // Update position of columns that follow removed column
            for (int i = position+1; i < oldColumns.size(); i++) {
                PColumn oldColumn = oldColumns.get(i);
                PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1+positionOffset, oldColumn.getSortOrder(), oldColumn.getArraySize(), oldColumn.getViewConstant(), oldColumn.isViewReferenced(), null, oldColumn.isRowTimestamp(), oldColumn.isDynamic(), oldColumn.getEncodedColumnQualifier());
                columns.add(newColumn);
            }
            
            table = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        }
        tables.put(table.getKey(), table, resolvedTime);
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData pruneTables(Pruner pruner) {
        List<PTableKey> keysToPrune = Lists.newArrayListWithExpectedSize(this.size());
        for (PTable table : this) {
            if (pruner.prune(table)) {
                keysToPrune.add(table.getKey());
            }
        }
        if (keysToPrune.isEmpty()) {
            return this;
        }
        PMetaDataCache tables = metaData.clone();
        for (PTableKey key : keysToPrune) {
            tables.remove(key);
        }
        return new PMetaDataImpl(tables);
    }

    @Override
    public Iterator<PTable> iterator() {
        return metaData.iterator();
    }

    @Override
    public PMetaData addFunction(PFunction function) throws SQLException {
        this.metaData.functions.put(function.getKey(), function);
        return this;
    }

    @Override
    public PMetaData removeFunction(PName tenantId, String function, long functionTimeStamp)
            throws SQLException {
        this.metaData.functions.remove(new PTableKey(tenantId, function));
        return this;
    }

    @Override
    public PMetaData pruneFunctions(Pruner pruner) {
        List<PTableKey> keysToPrune = Lists.newArrayListWithExpectedSize(this.size());
        for (PFunction function : this.metaData.functions.values()) {
            if (pruner.prune(function)) {
                keysToPrune.add(function.getKey());
            }
        }
        if (keysToPrune.isEmpty()) {
            return this;
        }
        PMetaDataCache clone = metaData.clone();
        for (PTableKey key : keysToPrune) {
            clone.functions.remove(key);
        }
        return new PMetaDataImpl(clone);
    
    }

    @Override
    public long getAge(PTableRef ref) {
        return this.metaData.getAge(ref);
    }

    @Override
    public PMetaData addSchema(PSchema schema) throws SQLException {
        this.metaData.schemas.put(schema.getSchemaKey(), schema);
        return this;
    }

    @Override
    public PSchema getSchema(PTableKey key) throws SchemaNotFoundException {
        PSchema schema = metaData.schemas.get(key);
        if (schema == null) { throw new SchemaNotFoundException(key.getName()); }
        return schema;
    }

    @Override
    public PMetaData removeSchema(PSchema schema, long schemaTimeStamp) {
        this.metaData.schemas.remove(SchemaUtil.getSchemaKey(schema.getSchemaName()));
        return this;
    }
}
