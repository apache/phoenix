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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
    private final Cache metaData;
    
    public PMetaDataImpl(int initialCapacity, long maxByteSize) {
        this.metaData = new CacheImpl(initialCapacity, maxByteSize, TimeKeeper.SYSTEM);
    }

    public PMetaDataImpl(int initialCapacity, long maxByteSize, TimeKeeper timeKeeper) {
        this.metaData = new CacheImpl(initialCapacity, maxByteSize, timeKeeper);
    }

    public PMetaDataImpl(Cache tables) {
        this.metaData = tables.clone();
    }
    
    private static class CacheImpl implements Cache, Cloneable {
        private static final int MIN_REMOVAL_SIZE = 3;
        private static final Comparator<PTableAccess> COMPARATOR = new Comparator<PTableAccess>() {
            @Override
            public int compare(PTableAccess tableAccess1, PTableAccess tableAccess2) {
                return Longs.compare(tableAccess1.lastAccessTime, tableAccess2.lastAccessTime);
            }
        };
        private static final MinMaxPriorityQueue.Builder<PTableAccess> BUILDER = MinMaxPriorityQueue.orderedBy(COMPARATOR);
        
        private long currentSize;
        private final long maxByteSize;
        private final int expectedCapacity;
        private final TimeKeeper timeKeeper;

        // Use regular HashMap, as we cannot use a LinkedHashMap that orders by access time
        // safely across multiple threads (as the underlying collection is not thread safe).
        // Instead, we track access time and prune it based on the copy we've made.
        private final HashMap<PTableKey,PTableAccess> tables;
        
        private static HashMap<PTableKey,PTableAccess> newMap(int expectedCapacity) {
            return Maps.newHashMapWithExpectedSize(expectedCapacity);
        }

        private static HashMap<PTableKey,PTableAccess> cloneMap(HashMap<PTableKey,PTableAccess> tables, int expectedCapacity) {
            HashMap<PTableKey,PTableAccess> newTables = Maps.newHashMapWithExpectedSize(Math.max(tables.size(),expectedCapacity));
            // Copy value so that access time isn't changing anymore
            for (PTableAccess tableAccess : tables.values()) {
                newTables.put(tableAccess.table.getKey(), new PTableAccess(tableAccess));
            }
            return newTables;
        }

        private CacheImpl(CacheImpl toClone) {
            this.timeKeeper = toClone.timeKeeper;
            this.maxByteSize = toClone.maxByteSize;
            this.currentSize = toClone.currentSize;
            this.expectedCapacity = toClone.expectedCapacity;
            this.tables = cloneMap(toClone.tables, toClone.expectedCapacity);
        }
        
        public CacheImpl(int initialCapacity, long maxByteSize, TimeKeeper timeKeeper) {
            this.currentSize = 0;
            this.maxByteSize = maxByteSize;
            this.expectedCapacity = initialCapacity;
            this.tables = newMap(initialCapacity);
            this.timeKeeper = timeKeeper;
        }
        
        @Override
        public Cache clone() {
            return new CacheImpl(this);
        }
        
        @Override
        public PTable get(PTableKey key) {
            PTableAccess tableAccess = tables.get(key);
            if (tableAccess == null) {
                return null;
            }
            tableAccess.lastAccessTime = timeKeeper.getCurrentTime();
            return tableAccess.table;
        }
        
        private void pruneIfNecessary() {
            // We have our own copy of the Map, as we copy on write, so its safe to remove from it.
            while (currentSize > maxByteSize && size() > 1) {
                // Estimate how many we need to remove by dividing the <number of bytes we're over the max>
                // by the <average size of an entry>. We'll keep at least MIN_REMOVAL_SIZE.
                int nToRemove = Math.max(MIN_REMOVAL_SIZE, (int)Math.ceil((currentSize-maxByteSize) / ((double)currentSize / size())));
                MinMaxPriorityQueue<PTableAccess> toRemove = BUILDER.expectedSize(nToRemove+1).create();
                // Make one pass through to find the <nToRemove> least recently accessed tables
                for (PTableAccess tableAccess : this.tables.values()) {
                    toRemove.add(tableAccess);
                    if (toRemove.size() > nToRemove) {
                        toRemove.removeLast();
                    }
                }
                // Of those least recently accessed tables, remove the least recently used
                // until we're under our size capacity
                do {
                    PTableAccess tableAccess = toRemove.removeFirst();
                    remove(tableAccess.table.getKey());
                } while (currentSize > maxByteSize && size() > 1 && !toRemove.isEmpty());
            }
        }
        
        @Override
        public PTable put(PTableKey key, PTable value) {
            currentSize += value.getEstimatedSize();
            PTableAccess oldTableAccess = tables.put(key, new PTableAccess(value, timeKeeper.getCurrentTime()));
            PTable oldTable = null;
            if (oldTableAccess != null) {
                currentSize -= oldTableAccess.table.getEstimatedSize();
                oldTable = oldTableAccess.table;
            }
            pruneIfNecessary();
            return oldTable;
        }
        
        @Override
        public PTable remove(PTableKey key) {
            PTableAccess value = tables.remove(key);
            if (value == null) {
                return null;
            }
            currentSize -= value.table.getEstimatedSize();
            return value.table;
        }
        
        @Override
        public Iterator<PTable> iterator() {
            final Iterator<PTableAccess> iterator = tables.values().iterator();
            return new Iterator<PTable>() {

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public PTable next() {
                    return iterator.next().table;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
                
            };
        }

        @Override
        public int size() {
            return tables.size();
        }
        
        private static class PTableAccess {
            public PTable table;
            public volatile long lastAccessTime;
            
            public PTableAccess(PTable table, long lastAccessTime) {
                this.table = table;
                this.lastAccessTime = lastAccessTime;
            }

            public PTableAccess(PTableAccess tableAccess) {
                this.table = tableAccess.table;
                this.lastAccessTime = tableAccess.lastAccessTime;
            }
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
            PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1+positionOffset, oldColumn.getSortOrder(), oldColumn.getArraySize(), oldColumn.getViewConstant(), oldColumn.isViewReferenced());
            columns.add(newColumn);
        }
        
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        tables.put(newTable.getKey(), newTable);
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData pruneTables(Pruner pruner) {
        List<PTableKey> keysToPrune = Lists.newArrayListWithExpectedSize(this.getTables().size());
        for (PTable table : this.getTables()) {
            if (pruner.prune(table)) {
                keysToPrune.add(table.getKey());
            }
        }
        if (keysToPrune.isEmpty()) {
            return this;
        }
        Cache tables = metaData.clone();
        for (PTableKey key : keysToPrune) {
            tables.remove(key);
        }
        return new PMetaDataImpl(tables);
    }
}
