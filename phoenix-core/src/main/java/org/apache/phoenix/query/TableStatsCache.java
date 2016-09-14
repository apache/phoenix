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

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

/**
 * "Client-side" cache for storing {@link PTableStats} for Phoenix tables. Intended to decouple
 * Phoenix from a specific version of Guava's cache.
 */
public class TableStatsCache {
    private static final Logger logger = LoggerFactory.getLogger(TableStatsCache.class);

    private final ConnectionQueryServices queryServices;
    private final LoadingCache<ImmutableBytesPtr, PTableStats> cache;

    public TableStatsCache(ConnectionQueryServices queryServices, Configuration config) {
        this.queryServices = Objects.requireNonNull(queryServices);
        // Number of millis to expire cache values after write
        final long statsUpdateFrequency = config.getLong(
                QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS);
        // Maximum number of entries (tables) to store in the cache at one time
        final long maxTableStatsCacheSize = config.getLong(
                QueryServices.STATS_MAX_CACHE_SIZE,
                QueryServicesOptions.DEFAULT_STATS_MAX_CACHE_SIZE);
        cache = CacheBuilder.newBuilder()
                // Expire entries a given amount of time after they were written
                .expireAfterWrite(statsUpdateFrequency, TimeUnit.MILLISECONDS)
                // Maximum total weight (size in bytes) of stats entries
                .maximumWeight(maxTableStatsCacheSize)
                // Defer actual size to the PTableStats.getEstimatedSize()
                .weigher(new Weigher<ImmutableBytesPtr, PTableStats>() {
                    @Override public int weigh(ImmutableBytesPtr key, PTableStats stats) {
                        return stats.getEstimatedSize();
                    }
                })
                // Log removals at TRACE for debugging
                .removalListener(new PhoenixStatsCacheRemovalListener())
                // Automatically load the cache when entries are missing
                .build(new StatsLoader());
    }

    /**
     * {@link CacheLoader} implementation for the Phoenix Table Stats cache.
     */
    protected class StatsLoader extends CacheLoader<ImmutableBytesPtr, PTableStats> {
        @Override
        public PTableStats load(ImmutableBytesPtr tableName) throws Exception {
            @SuppressWarnings("deprecation")
            HTableInterface statsHTable = queryServices.getTable(SchemaUtil.getPhysicalName(
                    PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,
                            queryServices.getProps()).getName());
            final byte[] tableNameBytes = tableName.copyBytesIfNecessary();
            try {
                PTableStats stats = StatisticsUtil.readStatistics(statsHTable, tableNameBytes,
                        Long.MAX_VALUE);
                traceStatsUpdate(tableNameBytes, stats);
                return stats;
            } catch (TableNotFoundException e) {
                // On a fresh install, stats might not yet be created, don't warn about this.
                logger.debug("Unable to locate Phoenix stats table", e);
                return PTableStats.EMPTY_STATS;
            } catch (IOException e) {
                logger.warn("Unable to read from stats table", e);
                // Just cache empty stats. We'll try again after some time anyway.
                return PTableStats.EMPTY_STATS;
            } finally {
                try {
                    statsHTable.close();
                } catch (IOException e) {
                    // Log, but continue. We have our stats anyway now.
                    logger.warn("Unable to close stats table", e);
                }
            }
        }

        /**
         * Logs a trace message for newly inserted entries to the stats cache.
         */
        void traceStatsUpdate(byte[] tableName, PTableStats stats) {
            logger.trace("Updating local TableStats cache (id={}) for {}, size={}bytes",
                  new Object[] {Objects.hashCode(TableStatsCache.this), Bytes.toString(tableName),
                  stats.getEstimatedSize()});
        }
    }

    /**
     * Returns the underlying cache. Try to use the provided methods instead of accessing the cache
     * directly.
     */
    LoadingCache<ImmutableBytesPtr, PTableStats> getCache() {
        return cache;
    }

    /**
     * Returns the PTableStats for the given <code>tableName</code, using the provided
     * <code>valueLoader</code> if no such mapping exists.
     *
     * @see com.google.common.cache.LoadingCache#get(Object)
     */
    public PTableStats get(ImmutableBytesPtr tableName) throws ExecutionException {
        return getCache().get(tableName);
    }

    /**
     * Cache the given <code>stats</code> to the cache for the given <code>tableName</code>.
     *
     * @see com.google.common.cache.Cache#put(Object, Object)
     */
    public void put(ImmutableBytesPtr tableName, PTableStats stats) {
        getCache().put(Objects.requireNonNull(tableName), Objects.requireNonNull(stats));
    }

    /**
     * Removes the mapping for <code>tableName</code> if it exists.
     *
     * @see com.google.common.cache.Cache#invalidate(Object)
     */
    public void invalidate(ImmutableBytesPtr tableName) {
        getCache().invalidate(Objects.requireNonNull(tableName));
    }

    /**
     * Removes all mappings from the cache.
     *
     * @see com.google.common.cache.Cache#invalidateAll()
     */
    public void invalidateAll() {
        getCache().invalidateAll();
    }

    /**
     * A {@link RemovalListener} implementation to track evictions from the table stats cache.
     */
    static class PhoenixStatsCacheRemovalListener implements
            RemovalListener<ImmutableBytesPtr, PTableStats> {
        @Override
        public void onRemoval(RemovalNotification<ImmutableBytesPtr, PTableStats> notification) {
            final RemovalCause cause = notification.getCause();
            if (wasEvicted(cause)) {
                ImmutableBytesPtr ptr = notification.getKey();
                String tableName = new String(ptr.get(), ptr.getOffset(), ptr.getLength());
                logger.trace("Cached stats for {} with size={}bytes was evicted due to cause={}",
                        new Object[] {tableName, notification.getValue().getEstimatedSize(),
                                cause});
            }
        }

        boolean wasEvicted(RemovalCause cause) {
            // This is actually a method on RemovalCause but isn't exposed
            return RemovalCause.EXPLICIT != cause && RemovalCause.REPLACED != cause;
        }
    }
}
