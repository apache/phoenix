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
package org.apache.phoenix.cache;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.cache.*;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.util.Closeables;

/**
 * 
 * Cache per tenant on server side.  Tracks memory usage for each
 * tenat as well and rolling up usage to global memory manager.
 * 
 * 
 * @since 0.1
 */
public class TenantCacheImpl implements TenantCache {
    private final int maxTimeToLiveMs;
    private final MemoryManager memoryManager;
    private volatile Cache<ImmutableBytesPtr, Closeable> serverCaches;

    public TenantCacheImpl(MemoryManager memoryManager, int maxTimeToLiveMs) {
        this.memoryManager = memoryManager;
        this.maxTimeToLiveMs = maxTimeToLiveMs;
    }
    
    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    private Cache<ImmutableBytesPtr,Closeable> getServerCaches() {
        /* Delay creation of this map until it's needed */
        if (serverCaches == null) {
            synchronized(this) {
                if (serverCaches == null) {
                    serverCaches = CacheBuilder.newBuilder()
                        .expireAfterAccess(maxTimeToLiveMs, TimeUnit.MILLISECONDS)
                        .removalListener(new RemovalListener<ImmutableBytesPtr, Closeable>(){
                            @Override
                            public void onRemoval(RemovalNotification<ImmutableBytesPtr, Closeable> notification) {
                                Closeables.closeAllQuietly(Collections.singletonList(notification.getValue()));
                            }
                        })
                        .build();
                }
            }
        }
        return serverCaches;
    }
    
    @Override
    public Closeable getServerCache(ImmutableBytesPtr cacheId) {
        return getServerCaches().getIfPresent(cacheId);
    }
    
    @Override
    public Closeable addServerCache(ImmutableBytesPtr cacheId, ImmutableBytesWritable cachePtr, ServerCacheFactory cacheFactory) throws SQLException {
        MemoryChunk chunk = this.getMemoryManager().allocate(cachePtr.getLength());
        boolean success = false;
        try {
            Closeable element = cacheFactory.newCache(cachePtr, chunk);
            getServerCaches().put(cacheId, element);
            success = true;
            return element;
        } finally {
            if (!success) {
                Closeables.closeAllQuietly(Collections.singletonList(chunk));
            }
        }           
    }
    
    @Override
    public void removeServerCache(ImmutableBytesPtr cacheId) throws SQLException {
        getServerCaches().invalidate(cacheId);
    }
}
