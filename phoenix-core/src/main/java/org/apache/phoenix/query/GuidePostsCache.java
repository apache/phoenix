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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
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
 * "Client-side" cache for storing {@link GuidePostsInfo} for a column family. Intended to decouple
 * Phoenix from a specific version of Guava's cache.
 */
public class GuidePostsCache {
    private static final Logger logger = LoggerFactory.getLogger(GuidePostsCache.class);

    private final ConnectionQueryServices queryServices;
    private final LoadingCache<GuidePostsKey, GuidePostsInfo> cache;

    public GuidePostsCache(ConnectionQueryServices queryServices, Configuration config) {
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
                .weigher(new Weigher<GuidePostsKey, GuidePostsInfo>() {
                    @Override public int weigh(GuidePostsKey key, GuidePostsInfo info) {
                        return info.getEstimatedSize();
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
    protected class StatsLoader extends CacheLoader<GuidePostsKey, GuidePostsInfo> {
        @Override
        public GuidePostsInfo load(GuidePostsKey statsKey) throws Exception {
            @SuppressWarnings("deprecation")
            HTableInterface statsHTable = queryServices.getTable(SchemaUtil.getPhysicalName(
                    PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,
                            queryServices.getProps()).getName());
            try {
                GuidePostsInfo guidePostsInfo = StatisticsUtil.readStatistics(statsHTable, statsKey,
                        HConstants.LATEST_TIMESTAMP);
                traceStatsUpdate(statsKey, guidePostsInfo);
                return guidePostsInfo;
            } catch (TableNotFoundException e) {
                // On a fresh install, stats might not yet be created, don't warn about this.
                logger.debug("Unable to locate Phoenix stats table", e);
                return GuidePostsInfo.NO_GUIDEPOST;
            } catch (IOException e) {
                logger.warn("Unable to read from stats table", e);
                // Just cache empty stats. We'll try again after some time anyway.
                return GuidePostsInfo.NO_GUIDEPOST;
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
        void traceStatsUpdate(GuidePostsKey key, GuidePostsInfo info) {
            if (logger.isTraceEnabled()) {
                logger.trace("Updating local TableStats cache (id={}) for {}, size={}bytes",
                      new Object[] {Objects.hashCode(GuidePostsCache.this), key,
                      info.getEstimatedSize()});
            }
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
    public GuidePostsInfo get(GuidePostsKey key) throws ExecutionException {
        return getCache().get(key);
    }

    /**
     * Cache the given <code>stats</code> to the cache for the given <code>tableName</code>.
     *
     * @see com.google.common.cache.Cache#put(Object, Object)
     */
    public void put(GuidePostsKey key, GuidePostsInfo info) {
        getCache().put(Objects.requireNonNull(key), Objects.requireNonNull(info));
    }

    /**
     * Removes the mapping for <code>tableName</code> if it exists.
     *
     * @see com.google.common.cache.Cache#invalidate(Object)
     */
    public void invalidate(GuidePostsKey key) {
        getCache().invalidate(Objects.requireNonNull(key));
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
     * Removes all mappings where the {@link org.apache.phoenix.schema.stats.GuidePostsKey#getPhysicalName()}
     * equals physicalName. Because all keys in the map must be iterated, this method should be avoided.
     * @param physicalName
     */
    public void invalidateAll(byte[] physicalName) {
        for (GuidePostsKey key : getCache().asMap().keySet()) {
            if (Bytes.compareTo(key.getPhysicalName(), physicalName) == 0) {
                invalidate(key);
            }
        }
    }
    
    public void invalidateAll(HTableDescriptor htableDesc) {
        byte[] tableName = htableDesc.getTableName().getName();
        for (byte[] fam : htableDesc.getFamiliesKeys()) {
            invalidate(new GuidePostsKey(tableName, fam));
        }
    }
    
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
