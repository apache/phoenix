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
        private static final class PTableRef {
            public final PTable table;
            public final int estSize;
            public volatile long lastAccessTime;
            
            public PTableRef(PTable table, long lastAccessTime, int estSize) {
                this.table = table;
                this.lastAccessTime = lastAccessTime;
                this.estSize = estSize;
            }

            public PTableRef(PTable table, long lastAccessTime) {
                this (table, lastAccessTime, table.getEstimatedSize());
            }

            public PTableRef(PTableRef tableRef) {
                this (tableRef.table, tableRef.lastAccessTime, tableRef.estSize);
            }
        }

        private static class PTableCache implements Cloneable {
            private static final int MIN_REMOVAL_SIZE = 3;
            private static final Comparator<PTableRef> COMPARATOR = new Comparator<PTableRef>() {
                @Override
                public int compare(PTableRef tableRef1, PTableRef tableRef2) {
                    return Longs.compare(tableRef1.lastAccessTime, tableRef2.lastAccessTime);
                }
            };
            private static final MinMaxPriorityQueue.Builder<PTableRef> BUILDER = MinMaxPriorityQueue.orderedBy(COMPARATOR);
            
            private long currentByteSize;
            private final long maxByteSize;
            private final int expectedCapacity;
            private final TimeKeeper timeKeeper;

            private final Map<PTableKey,PTableRef> tables;
            
            private static Map<PTableKey,PTableRef> newMap(int expectedCapacity) {
                // Use regular HashMap, as we cannot use a LinkedHashMap that orders by access time
                // safely across multiple threads (as the underlying collection is not thread safe).
                // Instead, we track access time and prune it based on the copy we've made.
                return Maps.newHashMapWithExpectedSize(expectedCapacity);
            }

            private static Map<PTableKey,PTableRef> cloneMap(Map<PTableKey,PTableRef> tables, int expectedCapacity) {
                Map<PTableKey,PTableRef> newTables = newMap(Math.max(tables.size(),expectedCapacity));
                // Copy value so that access time isn't changing anymore
                for (PTableRef tableAccess : tables.values()) {
                    newTables.put(tableAccess.table.getKey(), new PTableRef(tableAccess));
                }
                return newTables;
            }

            private PTableCache(PTableCache toClone) {
                this.timeKeeper = toClone.timeKeeper;
                this.maxByteSize = toClone.maxByteSize;
                this.currentByteSize = toClone.currentByteSize;
                this.expectedCapacity = toClone.expectedCapacity;
                this.tables = cloneMap(toClone.tables, toClone.expectedCapacity);
            }
            
            public PTableCache(int initialCapacity, long maxByteSize, TimeKeeper timeKeeper) {
                this.currentByteSize = 0;
                this.maxByteSize = maxByteSize;
                this.expectedCapacity = initialCapacity;
                this.tables = newMap(initialCapacity);
                this.timeKeeper = timeKeeper;
            }
            
            public PTableRef get(PTableKey key) {
                PTableRef tableAccess = tables.get(key);
                if (tableAccess == null) {
                    return null;
                }
                tableAccess.lastAccessTime = timeKeeper.getCurrentTime();
                return tableAccess;
            }
            
            @Override
            public PTableCache clone() {
                return new PTableCache(this);
            }

            /**
             * Used when the cache is growing past its max size to clone in a single pass.
             * Removes least recently used tables to get size of cache below its max size by
             * the overage amount.
             */
            public PTableCache cloneMinusOverage(long overage) {
                assert(overage > 0);
                int nToRemove = Math.max(MIN_REMOVAL_SIZE, (int)Math.ceil((currentByteSize-maxByteSize) / ((double)currentByteSize / size())) + 1);
                MinMaxPriorityQueue<PTableRef> toRemove = BUILDER.expectedSize(nToRemove).create();
                PTableCache newCache = new PTableCache(this.size(), this.maxByteSize, this.timeKeeper);
                
                long toRemoveBytes = 0;
                // Add to new cache, but track references to remove when done
                // to bring cache at least overage amount below it's max size.
                for (PTableRef tableRef : tables.values()) {
                    newCache.put(tableRef.table.getKey(), new PTableRef(tableRef));
                    toRemove.add(tableRef);
                    toRemoveBytes += tableRef.estSize;
                    if (toRemoveBytes - toRemove.peekLast().estSize > overage) {
                        PTableRef removedRef = toRemove.removeLast();
                        toRemoveBytes -= removedRef.estSize;
                    }
                }
                for (PTableRef toRemoveRef : toRemove) {
                    newCache.remove(toRemoveRef.table.getKey());
                }
                return newCache;
            }

            private PTable put(PTableKey key, PTableRef ref) {
                currentByteSize += ref.estSize;
                PTableRef oldTableAccess = tables.put(key, ref);
                PTable oldTable = null;
                if (oldTableAccess != null) {
                    currentByteSize -= oldTableAccess.estSize;
                    oldTable = oldTableAccess.table;
                }
                return oldTable;
            }

            public PTable put(PTableKey key, PTable value) {
                return put(key, new PTableRef(value, timeKeeper.getCurrentTime()));
            }
            
            public PTable putDuplicate(PTableKey key, PTable value) {
                return put(key, new PTableRef(value, timeKeeper.getCurrentTime(), 0));
            }
            
            public PTable remove(PTableKey key) {
                PTableRef value = tables.remove(key);
                if (value == null) {
                    return null;
                }
                currentByteSize -= value.estSize;
                return value.table;
            }
            
            public Iterator<PTable> iterator() {
                final Iterator<PTableRef> iterator = tables.values().iterator();
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

            public int size() {
                return tables.size();
            }

            public long getCurrentSize() {
                return this.currentByteSize;
            }

            public long getMaxSize() {
                return this.maxByteSize;
            }
        }
            
    private final PTableCache metaData;
    
    public PMetaDataImpl(int initialCapacity, long maxByteSize) {
        this.metaData = new PTableCache(initialCapacity, maxByteSize, TimeKeeper.SYSTEM);
    }

    public PMetaDataImpl(int initialCapacity, long maxByteSize, TimeKeeper timeKeeper) {
        this.metaData = new PTableCache(initialCapacity, maxByteSize, timeKeeper);
    }

    private PMetaDataImpl(PTableCache tables) {
        this.metaData = tables.clone();
    }
    
    @Override
    public PMetaDataImpl clone() {
        return new PMetaDataImpl(this.metaData);
    }
    
    @Override
    public PTable getTable(PTableKey key) throws TableNotFoundException {
        PTableRef ref = metaData.get(key);
        if (ref == null) {
            throw new TableNotFoundException(key.getName());
        }
        return ref.table;
    }

    @Override
    public int size() {
        return metaData.size();
    }


    @Override
    public PMetaData addTable(PTable table) throws SQLException {
        int netGain = 0;
        PTableKey key = table.getKey();
        PTableRef oldTableRef = metaData.get(key);
        if (oldTableRef != null) {
            netGain -= oldTableRef.estSize;
        }
        PTable newParentTable = null;
        if (table.getParentName() != null) { // Upsert new index table into parent data table list
            String parentName = table.getParentName().getString();
            PTableRef oldParentRef = metaData.get(new PTableKey(table.getTenantId(), parentName));
            // If parentTable isn't cached, that's ok we can skip this
            if (oldParentRef != null) {
                List<PTable> oldIndexes = oldParentRef.table.getIndexes();
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
                netGain -= oldParentRef.estSize;
                newParentTable = PTableImpl.makePTable(oldParentRef.table, table.getTimeStamp(), newIndexes);
                netGain += newParentTable.getEstimatedSize();
            }
        }
        if (newParentTable == null) { // Don't count in gain if we found a parent table, as its accounted for in newParentTable
            netGain += table.getEstimatedSize();
        }
        long overage = metaData.getCurrentSize() + netGain - metaData.getMaxSize();
        PTableCache tables = overage <= 0 ? metaData.clone() : metaData.cloneMinusOverage(overage);
        
        if (newParentTable != null) { // Upsert new index table into parent data table list
            tables.put(newParentTable.getKey(), newParentTable);
            tables.putDuplicate(table.getKey(), table);
        } else {
            tables.put(table.getKey(), table);
        }
        for (PTable index : table.getIndexes()) {
            tables.putDuplicate(index.getKey(), index);
        }
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData addColumn(PName tenantId, String tableName, List<PColumn> columnsToAdd, long tableTimeStamp, long tableSeqNum, boolean isImmutableRows, boolean isWalDisabled, boolean isMultitenant, boolean storeNulls) throws SQLException {
        PTableRef oldTableRef = metaData.get(new PTableKey(tenantId, tableName));
        if (oldTableRef == null) {
            return this;
        }
        List<PColumn> oldColumns = PTableImpl.getColumnsToClone(oldTableRef.table);
        List<PColumn> newColumns;
        if (columnsToAdd.isEmpty()) {
            newColumns = oldColumns;
        } else {
            newColumns = Lists.newArrayListWithExpectedSize(oldColumns.size() + columnsToAdd.size());
            newColumns.addAll(oldColumns);
            newColumns.addAll(columnsToAdd);
        }
        PTable newTable = PTableImpl.makePTable(oldTableRef.table, tableTimeStamp, tableSeqNum, newColumns, isImmutableRows, isWalDisabled, isMultitenant, storeNulls);
        return addTable(newTable);
    }

    @Override
    public PMetaData removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp) throws SQLException {
        PTableCache tables = null;
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
            List<PTable> oldIndexes = parentTableRef.table.getIndexes();
            if(oldIndexes != null && !oldIndexes.isEmpty()) {
                List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size());
                newIndexes.addAll(oldIndexes);
                for (int i = 0; i < newIndexes.size(); i++) {
                    PTable index = newIndexes.get(i);
                    if (index.getName().getString().equals(tableName)) {
                        newIndexes.remove(i);
                        PTable parentTable = PTableImpl.makePTable(
                                parentTableRef.table,
                                tableTimeStamp == HConstants.LATEST_TIMESTAMP ? parentTableRef.table.getTimeStamp() : tableTimeStamp,
                                newIndexes);
                        if (tables == null) { 
                            tables = metaData.clone();
                        }
                        tables.put(parentTable.getKey(), parentTable);
                        break;
                    }
                }
            }
        }
        return tables == null ? this : new PMetaDataImpl(tables);
    }
    
    @Override
    public PMetaData removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp, long tableSeqNum) throws SQLException {
        PTableRef tableRef = metaData.get(new PTableKey(tenantId, tableName));
        if (tableRef == null) {
            return this;
        }
        PTable table = tableRef.table;
        PTableCache tables = metaData.clone();
        for (PColumn columnToRemove : columnsToRemove) {
            PColumn column;
            String familyName = columnToRemove.getFamilyName().getString();
            if (familyName == null) {
                column = table.getPKColumn(columnToRemove.getName().getString());
            } else {
                column = table.getColumnFamily(familyName).getColumn(columnToRemove.getName().getString());
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
                PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1+positionOffset, oldColumn.getSortOrder(), oldColumn.getArraySize(), oldColumn.getViewConstant(), oldColumn.isViewReferenced(), null);
                columns.add(newColumn);
            }
            
            table = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        }
        tables.put(table.getKey(), table);
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
        PTableCache tables = metaData.clone();
        for (PTableKey key : keysToPrune) {
            tables.remove(key);
        }
        return new PMetaDataImpl(tables);
    }

    @Override
    public Iterator<PTable> iterator() {
        return metaData.iterator();
    }
}
