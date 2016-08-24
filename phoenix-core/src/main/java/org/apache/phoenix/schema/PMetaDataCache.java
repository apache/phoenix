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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.util.TimeKeeper;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Longs;

class PMetaDataCache implements Cloneable {
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
    private final PTableRefFactory tableRefFactory;

    private final Map<PTableKey,PTableRef> tables;
    final Map<PTableKey,PFunction> functions;
    final Map<PTableKey,PSchema> schemas;
    
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

    private Map<PTableKey,PTableRef> cloneMap(Map<PTableKey,PTableRef> tables, int expectedCapacity) {
        Map<PTableKey,PTableRef> newTables = newMap(Math.max(tables.size(),expectedCapacity));
        // Copy value so that access time isn't changing anymore
        for (PTableRef tableAccess : tables.values()) {
            newTables.put(tableAccess.getTable().getKey(), tableRefFactory.makePTableRef(tableAccess));
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

    PMetaDataCache(PMetaDataCache toClone) {
        this.tableRefFactory = toClone.tableRefFactory;
        this.timeKeeper = toClone.timeKeeper;
        this.maxByteSize = toClone.maxByteSize;
        this.currentByteSize = toClone.currentByteSize;
        this.expectedCapacity = toClone.expectedCapacity;
        this.tables = cloneMap(toClone.tables, expectedCapacity);
        this.functions = cloneFunctionsMap(toClone.functions, expectedCapacity);
        this.schemas = cloneSchemaMap(toClone.schemas, expectedCapacity);
    }
    
    public PMetaDataCache(int initialCapacity, long maxByteSize, TimeKeeper timeKeeper, PTableRefFactory tableRefFactory) {
        this.currentByteSize = 0;
        this.maxByteSize = maxByteSize;
        this.expectedCapacity = initialCapacity;
        this.tables = newMap(this.expectedCapacity);
        this.functions = newFunctionMap(this.expectedCapacity);
        this.timeKeeper = timeKeeper;
        this.schemas = newSchemaMap(this.expectedCapacity);
        this.tableRefFactory = tableRefFactory;
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
        PMetaDataCache newCache = new PMetaDataCache(this.size(), this.maxByteSize, this.timeKeeper, this.tableRefFactory);
        
        long toRemoveBytes = 0;
        // Add to new cache, but track references to remove when done
        // to bring cache at least overage amount below it's max size.
        for (PTableRef tableRef : this.tables.values()) {
            newCache.put(tableRef.getTable().getKey(), tableRefFactory.makePTableRef(tableRef));
            toRemove.add(tableRef);
            toRemoveBytes += tableRef.getEstimatedSize();
            while (toRemoveBytes - toRemove.peekLast().getEstimatedSize() >= overage) {
                PTableRef removedRef = toRemove.removeLast();
                toRemoveBytes -= removedRef.getEstimatedSize();
            }
        }
        for (PTableRef toRemoveRef : toRemove) {
            newCache.remove(toRemoveRef.getTable().getKey());
        }
        return newCache;
    }

    PTable put(PTableKey key, PTableRef ref) {
        currentByteSize += ref.getEstimatedSize();
        PTableRef oldTableAccess = this.tables.put(key, ref);
        PTable oldTable = null;
        if (oldTableAccess != null) {
            currentByteSize -= oldTableAccess.getEstimatedSize();
            oldTable = oldTableAccess.getTable();
        }
        return oldTable;
    }

    public long getAge(PTableRef ref) {
        return timeKeeper.getCurrentTime() - ref.getCreateTime();
    }
    
    public PTable remove(PTableKey key) {
        PTableRef value = this.tables.remove(key);
        if (value == null) {
            return null;
        }
        currentByteSize -= value.getEstimatedSize();
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