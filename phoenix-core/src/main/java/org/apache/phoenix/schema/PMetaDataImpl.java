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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * 
 * Client-side cache of MetaData. Not thread safe, but meant to be used
 * in a copy-on-write fashion. Internally uses a LinkedHashMap that evicts
 * the oldest entries when size grows beyond the maxSize specified at
 * create time.
 *
 */
public class PMetaDataImpl implements PMetaData {
    private final Cache metaData;
    
    public PMetaDataImpl(int initialCapacity, long maxByteSize) {
        this.metaData = new CacheImpl(initialCapacity, maxByteSize);
    }

    public PMetaDataImpl(Cache tables) {
        this.metaData = tables.clone();
    }
    
    private static class CacheImpl implements Cache, Cloneable {
        private final long maxByteSize;
        private long currentSize;
        private final LinkedHashMap<PTableKey,PTable> tables;
        
        private CacheImpl(long maxByteSize, long currentSize, LinkedHashMap<PTableKey,PTable> tables) {
            this.maxByteSize = maxByteSize;
            this.currentSize = currentSize;
            this.tables = tables;
        }
        
        public CacheImpl(int initialCapacity, long maxByteSize) {
            this.maxByteSize = maxByteSize;
            this.currentSize = 0;
            this.tables = newLRUMap(initialCapacity);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Cache clone() {
            return new CacheImpl(this.maxByteSize, this.currentSize, (LinkedHashMap<PTableKey, PTable>)this.tables.clone());
        }
        
        @Override
        public PTable get(PTableKey key) {
            return tables.get(key);
        }
        
        private void pruneIfNecessary() {
            if (currentSize > maxByteSize && size() > 1) {
                Iterator<Map.Entry<PTableKey, PTable>> entries = this.tables.entrySet().iterator();
                do {
                    PTable table = entries.next().getValue();
                    if (table.getType() != PTableType.SYSTEM) {
                        currentSize -= table.getEstimatedSize();
                        entries.remove();
                    }
                } while (currentSize > maxByteSize && size() > 1 && entries.hasNext());
            }
        }
        
        @Override
        public PTable put(PTableKey key, PTable value) {
            currentSize += value.getEstimatedSize();
            PTable oldTable = tables.put(key, value);
            if (oldTable != null) {
                currentSize -= oldTable.getEstimatedSize();
            }
            pruneIfNecessary();
            return oldTable;
        }
        
        @Override
        public PTable remove(PTableKey key) {
            PTable value = tables.remove(key);
            if (value != null) {
                currentSize -= value.getEstimatedSize();
            }
            pruneIfNecessary();
            return value;
        }
        
        private LinkedHashMap<PTableKey,PTable> newLRUMap(int estimatedSize) {
            return new LinkedHashMap<PTableKey,PTable>(estimatedSize, 0.75F, true);
        }

        @Override
        public Iterator<PTable> iterator() {
            return Iterators.unmodifiableIterator(tables.values().iterator());
        }

        @Override
        public int size() {
            return tables.size();
        }
    }
    
    @Override
    public PTable getTable(PTableKey key) throws TableNotFoundException {
        PTable table = metaData.get(key);
        if (table == null) {
            throw new TableNotFoundException(key.getName());
        }
        return table;
    }

    @Override
    public Cache getTables() {
        return metaData;
    }


    @Override
    public PMetaData addTable(PTable table) throws SQLException {
        Cache tables = metaData.clone();
        PTable oldTable = tables.put(table.getKey(), table);
        if (table.getParentName() != null) { // Upsert new index table into parent data table list
            String parentName = table.getParentName().getString();
            PTable parentTable = tables.get(new PTableKey(table.getTenantId(), parentName));
            // If parentTable isn't cached, that's ok we can skip this
            if (parentTable != null) {
                List<PTable> oldIndexes = parentTable.getIndexes();
                List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size() + 1);
                newIndexes.addAll(oldIndexes);
                if (oldTable != null) {
                    newIndexes.remove(oldTable);
                }
                newIndexes.add(table);
                parentTable = PTableImpl.makePTable(parentTable, table.getTimeStamp(), newIndexes);
                tables.put(parentTable.getKey(), parentTable);
            }
        }
        for (PTable index : table.getIndexes()) {
            tables.put(index.getKey(), index);
        }
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData addColumn(PName tenantId, String tableName, List<PColumn> columnsToAdd, long tableTimeStamp, long tableSeqNum, boolean isImmutableRows) throws SQLException {
        PTable table = getTable(new PTableKey(tenantId, tableName));
        Cache tables = metaData.clone();
        List<PColumn> oldColumns = PTableImpl.getColumnsToClone(table);
        List<PColumn> newColumns;
        if (columnsToAdd.isEmpty()) {
            newColumns = oldColumns;
        } else {
            newColumns = Lists.newArrayListWithExpectedSize(oldColumns.size() + columnsToAdd.size());
            newColumns.addAll(oldColumns);
            newColumns.addAll(columnsToAdd);
        }
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, newColumns, isImmutableRows);
        tables.put(newTable.getKey(), newTable);
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData removeTable(PName tenantId, String tableName) throws SQLException {
        PTable table;
        Cache tables = metaData.clone();
        if ((table=tables.remove(new PTableKey(tenantId, tableName))) == null) {
            throw new TableNotFoundException(tableName);
        } else {
            for (PTable index : table.getIndexes()) {
                if (tables.remove(index.getKey()) == null) {
                    throw new TableNotFoundException(index.getName().getString());
                }
            }
        }
        return new PMetaDataImpl(tables);
    }
    
    @Override
    public PMetaData removeColumn(PName tenantId, String tableName, String familyName, String columnName, long tableTimeStamp, long tableSeqNum) throws SQLException {
        PTable table = getTable(new PTableKey(tenantId, tableName));
        Cache tables = metaData.clone();
        PColumn column;
        if (familyName == null) {
            column = table.getPKColumn(columnName);
        } else {
            column = table.getColumnFamily(familyName).getColumn(columnName);
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
            PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1+positionOffset, oldColumn.getSortOrder(), oldColumn.getArraySize(), oldColumn.getViewConstant());
            columns.add(newColumn);
        }
        
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        tables.put(newTable.getKey(), newTable);
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData pruneTables(Pruner pruner) {
        for (PTable table : this.getTables()) {
            if (pruner.prune(table)) {
                Cache newCache = this.getTables().clone();
                for (PTable value : this.getTables()) { // Go through old to prevent concurrent modification exception
                    if (pruner.prune(value)) {
                        newCache.remove(value.getKey());
                    }
                }
                return new PMetaDataImpl(newCache);
            }
        }
        return this;
    }
}
