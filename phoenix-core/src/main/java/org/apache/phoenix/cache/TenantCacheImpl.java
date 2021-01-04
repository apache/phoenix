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
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.util.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Ticker;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalNotification;

/**
 * 
 * Cache per tenant on server side.  Tracks memory usage for each
 * tenat as well and rolling up usage to global memory manager.
 * 
 * 
 * @since 0.1
 */
public class TenantCacheImpl implements TenantCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantCacheImpl.class);
    private final int maxTimeToLiveMs;
    private final int maxPersistenceTimeToLiveMs;
    private final MemoryManager memoryManager;
    private final Ticker ticker;

    // Two caches exist: the "serverCaches" cache which is used for handling live
    // queries, and the "persistentServerCaches" cache which is used to store data
    // between queries. If we are out of memory, attempt to clear out entries from
    // the persistent cache before throwing an exception.
    private volatile Cache<ImmutableBytesPtr, CacheEntry> serverCaches;
    private volatile Cache<ImmutableBytesPtr, CacheEntry> persistentServerCaches;

    private final long EVICTION_MARGIN_BYTES = 10000000;

    private static class CacheEntry implements Comparable<CacheEntry>, Closeable {
        private ImmutableBytesPtr cacheId;
        private ImmutableBytesWritable cachePtr;
        private int hits;
        private int liveQueriesCount;
        private boolean usePersistentCache;
        private long size;
        private Closeable closeable;

        public CacheEntry(ImmutableBytesPtr cacheId, ImmutableBytesWritable cachePtr, 
                ServerCacheFactory cacheFactory, byte[] txState, MemoryChunk chunk,
                boolean usePersistentCache, boolean useProtoForIndexMaintainer,
                int clientVersion) throws SQLException {
            this.cacheId = cacheId;
            this.cachePtr = cachePtr;
            this.size = cachePtr.getLength();
            this.hits = 0;
            this.liveQueriesCount = 0;
            this.usePersistentCache = usePersistentCache;
            this.closeable = cacheFactory.newCache(cachePtr, txState, chunk, useProtoForIndexMaintainer, clientVersion);
        }

        public void close() throws IOException {
            this.closeable.close();
        }

        synchronized public void incrementLiveQueryCount() {
            liveQueriesCount++;
            hits++;
        }

        synchronized public void decrementLiveQueryCount() {
            liveQueriesCount--;
        }

        synchronized public boolean isLive() {
            return liveQueriesCount > 0;
        }

        public boolean getUsePersistentCache() {
            return usePersistentCache;
        }

        public ImmutableBytesPtr getCacheId() {
            return cacheId;
        }

        private Float rank() {
            return (float)hits;
        }

        @Override
        public int compareTo(CacheEntry o) {
            return rank().compareTo(o.rank());
        }
    }

    public TenantCacheImpl(MemoryManager memoryManager, int maxTimeToLiveMs, int maxPersistenceTimeToLiveMs) {
        this(memoryManager, maxTimeToLiveMs, maxPersistenceTimeToLiveMs, Ticker.systemTicker());
    }
    
    public TenantCacheImpl(MemoryManager memoryManager, int maxTimeToLiveMs, int maxPersistenceTimeToLiveMs, Ticker ticker) {
        this.memoryManager = memoryManager;
        this.maxTimeToLiveMs = maxTimeToLiveMs;
        this.maxPersistenceTimeToLiveMs = maxPersistenceTimeToLiveMs;
        this.ticker = ticker;
    }
    
    public Ticker getTicker() {
        return ticker;
    }
    
    // For testing
    public void cleanUp() {
        synchronized(this) {
            if (serverCaches != null) {
                serverCaches.cleanUp();
            }
            if (persistentServerCaches != null) {
                persistentServerCaches.cleanUp();
            }
        }
    }
    
    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    private Cache<ImmutableBytesPtr,CacheEntry> getServerCaches() {
        /* Delay creation of this map until it's needed */
        if (serverCaches == null) {
            synchronized(this) {
                if (serverCaches == null) {
                    serverCaches = buildCache(maxTimeToLiveMs, false);
                }
            }
        }
        return serverCaches;
    }

    private Cache<ImmutableBytesPtr,CacheEntry> getPersistentServerCaches() {
        /* Delay creation of this map until it's needed */
        if (persistentServerCaches == null) {
            synchronized(this) {
                if (persistentServerCaches == null) {
                    persistentServerCaches = buildCache(maxPersistenceTimeToLiveMs, true);
                }
            }
        }
        return persistentServerCaches;
    }

    private Cache<ImmutableBytesPtr, CacheEntry> buildCache(final int ttl, final boolean isPersistent) {
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
        if (isPersistent) {
            builder.expireAfterWrite(ttl, TimeUnit.MILLISECONDS);
        } else {
            builder.expireAfterAccess(ttl, TimeUnit.MILLISECONDS);
        }
        return builder
            .ticker(getTicker())
            .removalListener(new RemovalListener<ImmutableBytesPtr, CacheEntry>(){
                @Override
                public void onRemoval(RemovalNotification<ImmutableBytesPtr, CacheEntry> notification) {
                    if (isPersistent || !notification.getValue().getUsePersistentCache()) {
                        Closeables.closeAllQuietly(Collections.singletonList(notification.getValue()));
                    }
                }
            })
            .build();
    }

    synchronized private void evictInactiveEntries(long bytesNeeded) {
        LOGGER.debug("Trying to evict inactive cache entries to free up " + bytesNeeded + " bytes");
        CacheEntry[] entries = getPersistentServerCaches().asMap().values().toArray(new CacheEntry[]{});
        Arrays.sort(entries);
        long available = this.getMemoryManager().getAvailableMemory();
        for (int i = 0; i < entries.length && available < bytesNeeded; i++) {
            CacheEntry entry = entries[i];
            ImmutableBytesPtr cacheId = entry.getCacheId();
            getPersistentServerCaches().invalidate(cacheId);
            available = this.getMemoryManager().getAvailableMemory();
            LOGGER.debug("Evicted cache ID " + Bytes.toLong(cacheId.get()) + ", we now have "
                    + available + " bytes available");
        }
    }

    private CacheEntry getIfPresent(ImmutableBytesPtr cacheId) {
        CacheEntry entry = getPersistentServerCaches().getIfPresent(cacheId);
        if (entry != null) {
            return entry;
        }
        return getServerCaches().getIfPresent(cacheId);
    }

	@Override
    public Closeable getServerCache(ImmutableBytesPtr cacheId) {
        getServerCaches().cleanUp();
        CacheEntry entry = getIfPresent(cacheId);
        if (entry == null) {
            return null;
        }
        return entry.closeable;
    }

    @Override
    public Closeable addServerCache(ImmutableBytesPtr cacheId, ImmutableBytesWritable cachePtr, byte[] txState, ServerCacheFactory cacheFactory, boolean useProtoForIndexMaintainer, boolean usePersistentCache, int clientVersion) throws SQLException {
        getServerCaches().cleanUp();
        long available = this.getMemoryManager().getAvailableMemory();
        int size = cachePtr.getLength() + txState.length;
        if (size > available) {
            evictInactiveEntries(size - available + EVICTION_MARGIN_BYTES);
        }
        MemoryChunk chunk = this.getMemoryManager().allocate(size);
        boolean success = false;
        try {
            CacheEntry entry;
            synchronized(this) {
                entry = getIfPresent(cacheId);
                if (entry == null) {
                    entry = new CacheEntry(
                        cacheId, cachePtr, cacheFactory, txState, chunk,
                        usePersistentCache, useProtoForIndexMaintainer,
                        clientVersion);
                    getServerCaches().put(cacheId, entry);
                    if (usePersistentCache) {
                        getPersistentServerCaches().put(cacheId, entry);
                    }
                }
                entry.incrementLiveQueryCount();
            }
            success = true;
            return entry;
        } finally {
            if (!success) {
                Closeables.closeAllQuietly(Collections.singletonList(chunk));
            }
        }
    }

    @Override
    synchronized public void removeServerCache(ImmutableBytesPtr cacheId) {
        CacheEntry entry = getServerCaches().getIfPresent(cacheId);
        if (entry == null) {
            return;
        }
        entry.decrementLiveQueryCount();
        if (!entry.isLive()) {
            LOGGER.debug("Cache ID " + Bytes.toLong(cacheId.get())
                    + " is no longer live, invalidate it");
            getServerCaches().invalidate(cacheId);
        }
    }

    @Override
    public void removeAllServerCache() {
        getServerCaches().invalidateAll();
        getPersistentServerCaches().invalidateAll();
    }
}
