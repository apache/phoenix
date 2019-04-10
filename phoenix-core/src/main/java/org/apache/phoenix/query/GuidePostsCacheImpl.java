/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import static org.apache.phoenix.query.QueryServices.STATS_COLLECTION_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_STATS_COLLECTION_ENABLED;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

/**
 * "Client-side" cache for storing {@link GuidePostsInfo} for a column family. Intended to decouple
 * Phoenix from a specific version of Guava's cache.
 */
public class GuidePostsCacheImpl implements GuidePostsCache {
    private static final Logger logger = LoggerFactory.getLogger(GuidePostsCacheImpl.class);

    private final ConnectionQueryServices queryServices;
    private final LoadingCache<GuidePostsKey, GuidePostsInfo> cache;

    public GuidePostsCacheImpl(ConnectionQueryServices queryServices, Configuration config) {
        this.queryServices = Objects.requireNonNull(queryServices);

        // Number of millis to expire cache values after write
        final long statsUpdateFrequency = config.getLong(
                QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS);

        // Maximum total weight (size in bytes) of stats entries
        final long maxTableStatsCacheSize = config.getLong(
                QueryServices.STATS_MAX_CACHE_SIZE,
                QueryServicesOptions.DEFAULT_STATS_MAX_CACHE_SIZE);

        final boolean isStatsEnabled = config.getBoolean(STATS_COLLECTION_ENABLED, DEFAULT_STATS_COLLECTION_ENABLED);

        PhoenixStatsCacheLoader cacheLoader = new PhoenixStatsCacheLoader(
                isStatsEnabled ? new StatsLoaderImpl() : new EmptyStatsLoader(), config);

        cache = CacheBuilder.newBuilder()
                // Refresh entries a given amount of time after they were written
                .refreshAfterWrite(statsUpdateFrequency, TimeUnit.MILLISECONDS)
                // Maximum total weight (size in bytes) of stats entries
                .maximumWeight(maxTableStatsCacheSize)
                // Defer actual size to the PTableStats.getEstimatedSize()
                .weigher(new Weigher<GuidePostsKey, GuidePostsInfo>() {
                    @Override public int weigh(GuidePostsKey key, GuidePostsInfo info) {
                        return info.getEstimatedSize();
                    }
                })
                // Log removals at TRACE for debugging
                .removalListener(new PhoenixStatsCacheRemovalListener())
                // Automatically load the cache when entries need to be refreshed
                .build(cacheLoader);
    }

    /**
     * {@link PhoenixStatsLoader} implementation for the Stats Loader.
     * Empty stats loader if stats are disabled
     */
    protected class EmptyStatsLoader implements PhoenixStatsLoader {
        @Override
        public boolean needsLoad() {
            return false;
        }

        @Override
        public GuidePostsInfo loadStats(GuidePostsKey statsKey) throws Exception {
            return GuidePostsInfo.NO_GUIDEPOST;
        }

        @Override
        public GuidePostsInfo loadStats(GuidePostsKey statsKey, GuidePostsInfo prevGuidepostInfo) throws Exception {
            return GuidePostsInfo.NO_GUIDEPOST;
        }
    }

    /**
     * Returns the underlying cache. Try to use the provided methods instead of accessing the cache
     * directly.
     */
    LoadingCache<GuidePostsKey, GuidePostsInfo> getCache() {
        return cache;
    }

    /**
     * Returns the PTableStats for the given <code>tableName</code, using the provided
     * <code>valueLoader</code> if no such mapping exists.
     *
     * @see com.google.common.cache.LoadingCache#get(Object)
     */
    @Override
    public GuidePostsInfo get(GuidePostsKey key) throws ExecutionException {
        return getCache().get(key);
    }

    /**
     * Cache the given <code>stats</code> to the cache for the given <code>tableName</code>.
     *
     * @see com.google.common.cache.Cache#put(Object, Object)
     */
    @Override
    public void put(GuidePostsKey key, GuidePostsInfo info) {
        getCache().put(Objects.requireNonNull(key), Objects.requireNonNull(info));
    }

    /**
     * Removes the mapping for <code>tableName</code> if it exists.
     *
     * @see com.google.common.cache.Cache#invalidate(Object)
     */
    @Override
    public void invalidate(GuidePostsKey key) {
        getCache().invalidate(Objects.requireNonNull(key));
    }

    /**
     * Removes all mappings from the cache.
     *
     * @see com.google.common.cache.Cache#invalidateAll()
     */
    @Override
    public void invalidateAll() {
        getCache().invalidateAll();
    }

    /**
     * Removes all mappings where the {@link org.apache.phoenix.schema.stats.GuidePostsKey#getPhysicalName()}
     * equals physicalName. Because all keys in the map must be iterated, this method should be avoided.
     * @param physicalName
     */
    @Override
    public void invalidateAll(byte[] physicalName) {
        for (GuidePostsKey key : getCache().asMap().keySet()) {
            if (Bytes.compareTo(key.getPhysicalName(), physicalName) == 0) {
                invalidate(key);
            }
        }
    }

    @Override
    public void invalidateAll(TableDescriptor htableDesc) {
        byte[] tableName = htableDesc.getTableName().getName();
        for (byte[] fam : htableDesc.getColumnFamilyNames()) {
            invalidate(new GuidePostsKey(tableName, fam));
        }
    }

    @Override
    public void invalidateAll(PTable table) {
        byte[] physicalName = table.getPhysicalName().getBytes();
        List<PColumnFamily> families = table.getColumnFamilies();
        if (families.isEmpty()) {
            invalidate(new GuidePostsKey(physicalName, SchemaUtil.getEmptyColumnFamily(table)));
        } else {
            for (PColumnFamily family : families) {
                invalidate(new GuidePostsKey(physicalName, family.getName().getBytes()));
            }
        }
    }

    /**
     * A {@link RemovalListener} implementation to track evictions from the table stats cache.
     */
    static class PhoenixStatsCacheRemovalListener implements
            RemovalListener<GuidePostsKey, GuidePostsInfo> {
        @Override
        public void onRemoval(RemovalNotification<GuidePostsKey, GuidePostsInfo> notification) {
            if (logger.isTraceEnabled()) {
                final RemovalCause cause = notification.getCause();
                if (wasEvicted(cause)) {
                    GuidePostsKey key = notification.getKey();
                    logger.trace("Cached stats for {} with size={}bytes was evicted due to cause={}",
                            new Object[] {key, notification.getValue().getEstimatedSize(),
                                    cause});
                }
            }
        }

        boolean wasEvicted(RemovalCause cause) {
            // This is actually a method on RemovalCause but isn't exposed
            return RemovalCause.EXPLICIT != cause && RemovalCause.REPLACED != cause;
        }
    }
}