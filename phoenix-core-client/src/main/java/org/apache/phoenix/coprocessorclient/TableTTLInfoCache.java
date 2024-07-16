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

package org.apache.phoenix.coprocessorclient;

import java.util.*;
import java.util.concurrent.locks.StampedLock;

/**
 *  Holds a cache of TableTTLInfo objects.
 *  Maps TableTTLInfo to a generated tableId.
 *  The generated tableId is a positive number. (>= 0)
 *  This class is thread-safe.
 */
public class TableTTLInfoCache {

    // Forward mapping from ttlInfo -> tableId (integer)
    private final Map<TableTTLInfo, Integer> tableToTableIdMap = new HashMap<TableTTLInfo, Integer>();

    // Reverse mapping from tableId (integer position) -> ttlInfo
    private final List<TableTTLInfo> cachedInfo = new ArrayList<TableTTLInfo>();
    private final StampedLock lock = new StampedLock();
    private int nextId;

    public int addTable(TableTTLInfo tableRow) {
        return putTableIfAbsent(tableRow);
    }

    public int getNumTablesInCache() {
        if (cachedInfo.size() != tableToTableIdMap.keySet().size()) {
            throw new IllegalStateException();
        }
        return cachedInfo.size();
    }

    public Set<TableTTLInfo> getAllTables() {
        if (cachedInfo.size() != tableToTableIdMap.keySet().size()) {
            throw new IllegalStateException();
        }

        Set<TableTTLInfo> tables = new HashSet<TableTTLInfo>();
        tables.addAll(cachedInfo);
        return tables;
    }

    public TableTTLInfo getTableById(Integer id) {
        if (id == null) {
            return null;
        }
        return cachedInfo.get(id);
    }

    private Integer tryOptimisticGet(TableTTLInfo newRow) {
        long stamp = lock.tryOptimisticRead();
        Integer tableId = tableToTableIdMap.get(newRow);
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                tableId = tableToTableIdMap.get(newRow);
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return tableId;
    }

    private int putTableIfAbsent(TableTTLInfo newRow) {
        if (newRow == null)  {
            throw new IllegalArgumentException();
        }

        // if key does not exists in the forward mapping create one
        Integer tableId = tryOptimisticGet(newRow);
        if (tableId == null) {
            long writeStamp = lock.writeLock();
            try {
                tableId = tableToTableIdMap.get(newRow);
                if (tableId == null) {
                    tableId = nextId++;
                    cachedInfo.add(newRow);
                    if (nextId != cachedInfo.size()) {
                        throw new IllegalStateException();
                    }
                    tableToTableIdMap.put(newRow, tableId);
                }
            }
            finally {
                lock.unlock(writeStamp);
            }
        }
        return tableId;
    }
}
