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

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalCause;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalNotification;
import org.apache.phoenix.thirdparty.com.google.common.cache.Weigher;

/**
 * "Client-side" cache for storing {@link GuidePostsInfo} for a column family. Intended to decouple
 * Phoenix from a specific version of Guava's cache.
 */
public class GuidePostsCacheImpl implements GuidePostsCache {
    private static final Logger logger = LoggerFactory.getLogger(GuidePostsCacheImpl.class);

    private final LoadingCache<GuidePostsKey, GuidePostsInfo> cache;

    public GuidePostsCacheImpl(PhoenixStatsCacheLoader cacheLoader, Configuration config) {
        Preconditions.checkNotNull(cacheLoader);

        // Number of millis to expire cache values after write
        final long statsUpdateFrequency = config.getLong(
                QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS);

        // Maximum total weight (size in bytes) of stats entries
        final long maxTableStatsCacheSize = config.getLong(
                QueryServices.STATS_MAX_CACHE_SIZE,
                QueryServicesOptions.DEFAULT_STATS_MAX_CACHE_SIZE);

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
     * Returns the underlying cache. Try to use the provided methods instead of accessing the cache
     * directly.
     */
    LoadingCache<GuidePostsKey, GuidePostsInfo> getCache() {
        return cache;
    }

    /**
     * Returns the PTableStats for the given <code>tableName</code>, using the provided
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