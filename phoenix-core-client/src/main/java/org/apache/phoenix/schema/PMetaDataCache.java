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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalNotification;
import org.apache.phoenix.thirdparty.com.google.common.cache.Weigher;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TimeKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PMetaDataCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(PMetaDataCache.class);

    private final TimeKeeper timeKeeper;
    private final Cache<PTableKey, PTableRef> tables;
    final Map<PTableKey,PFunction> functions;
    final Map<PTableKey,PSchema> schemas;

    public PMetaDataCache(int initialCapacity, long maxByteSize,
                          TimeKeeper timeKeeper) {
        this.tables = CacheBuilder.newBuilder()
                .removalListener(new RemovalListener<PTableKey, PTableRef>() {
                    @Override
                    public void onRemoval(RemovalNotification<PTableKey, PTableRef> notification) {
                        String key = notification.getKey().toString();
                        LOGGER.debug("Expiring " + key + " because of "
                                + notification.getCause().name());
                        if (notification.wasEvicted()) {
                            GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_EVICTION_COUNTER
                                    .increment();
                        } else {
                            GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_REMOVAL_COUNTER
                                    .increment();
                        }
                        if (notification.getValue() != null) {
                            GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ESTIMATED_USED_SIZE
                                    .update(-notification.getValue().getEstimatedSize());
                        }
                    }
                })
                .maximumWeight(maxByteSize)
                .weigher(new Weigher<PTableKey, PTableRef>() {
                    @Override
                    public int weigh(PTableKey tableKey, PTableRef tableRef) {
                        if (PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA.equals(
                                SchemaUtil.getSchemaNameFromFullName(tableKey.getName()))) {
                            // Ensure there is always room for system tables
                            return 0;
                        }
                        return tableRef.getEstimatedSize();
                    }
                })
                .build();
        this.functions = new ConcurrentHashMap<>(initialCapacity);
        this.schemas = new ConcurrentHashMap<>(initialCapacity);
        this.timeKeeper = timeKeeper;
    }
    
    public PTableRef get(PTableKey key) {
        PTableRef tableAccess = this.tables.getIfPresent(key);
        return tableAccess;
    }

    PTable put(PTableKey key, PTableRef ref) {
        PTableRef oldTableRef = tables.asMap().put(key, ref);
        if (oldTableRef == null) {
            return null;
        }
        return oldTableRef.getTable();
    }

    public long getAge(PTableRef ref) {
        return timeKeeper.getCurrentTime() - ref.getCreateTime();
    }
    
    public PTable remove(PTableKey key) {
        PTableRef value = tables.getIfPresent(key);
        tables.invalidate(key);
        if (value == null) {
            return null;
        }
        return value.getTable();
    }
    
    public Iterator<PTable> iterator() {
        final Iterator<PTableRef> iterator = this.tables.asMap().values().iterator();
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

    public long size() {
        return this.tables.size();
    }
}