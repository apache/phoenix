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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

import java.lang.Thread;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;

/**
 * Test class around the PhoenixStatsCacheLoader.
 */
public class PhoenixStatsCacheLoaderTest {
    /**
     * {@link PhoenixStatsLoader} test implementation for the Stats Loader.
     */
    protected class TestStatsLoaderImpl implements PhoenixStatsLoader {
        private int maxLength = 1;

        @Override
        public boolean needsLoad() {
            // Whenever it's called, we try to load stats from stats table
            // no matter it has been updated or not.
            return true;
        }

        @Override
        public GuidePostsInfo loadStats(GuidePostsKey statsKey, GuidePostsInfo prevGuidepostInfo) throws Exception {
            try {
                Thread.sleep(2000);
            }
            catch (InterruptedException e) {
                assertFalse(true);
            }

            return new GuidePostsInfo(Collections.<Long> emptyList(),
                    new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY),
                    Collections.<Long> emptyList(), maxLength++, 0, Collections.<Long> emptyList());
        }
    }

    GuidePostsInfo getStats(LoadingCache<GuidePostsKey, GuidePostsInfo> cache, GuidePostsKey guidePostsKey) {
        GuidePostsInfo guidePostsInfo;
        try {
            guidePostsInfo = cache.get(guidePostsKey);
        } catch (ExecutionException e) {
            assertFalse(true);
            return GuidePostsInfo.NO_GUIDEPOST;
        }

        return guidePostsInfo;
    }

    void sleep(int x) {
        try {
            Thread.sleep(x);
        }
        catch (InterruptedException e) {
            assertFalse(true);
        }
    }

    @Test
    public void testStatsBeingAutomaticallyRefreshed() {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        LoadingCache<GuidePostsKey, GuidePostsInfo> cache = CacheBuilder.newBuilder()
                // Refresh entries a given amount of time after they were written
                .refreshAfterWrite(1000, TimeUnit.MILLISECONDS)
                // Maximum total weight (size in bytes) of stats entries
                .maximumWeight(QueryServicesOptions.DEFAULT_STATS_MAX_CACHE_SIZE)
                // Defer actual size to the PTableStats.getEstimatedSize()
                .weigher(new Weigher<GuidePostsKey, GuidePostsInfo>() {
                    @Override public int weigh(GuidePostsKey key, GuidePostsInfo info) {
                        return info.getEstimatedSize();
                    }
                })
                // Log removals at TRACE for debugging
                .removalListener(new GuidePostsCache.PhoenixStatsCacheRemovalListener())
                // Automatically load the cache when entries are missing
                .build(new PhoenixStatsCacheLoader(new TestStatsLoaderImpl(), executor));

        GuidePostsKey guidePostsKey = new GuidePostsKey(new byte[4], new byte[4]);
        GuidePostsInfo guidePostsInfo = getStats(cache, guidePostsKey);
        assertTrue(guidePostsInfo.getMaxLength() == 1);
        sleep(3000);
        guidePostsInfo = getStats(cache, guidePostsKey);
        // Refresh will be triggered, but still get the old value
        assertTrue(guidePostsInfo.getMaxLength() == 1);
        sleep(3000);
        guidePostsInfo = getStats(cache, guidePostsKey);
        // Refresh has completed, so get the new value.
        assertTrue(guidePostsInfo.getMaxLength() == 2);
    }
}