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
package org.apache.phoenix.query;

import org.apache.phoenix.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * {@link CacheLoader} asynchronous implementation for the Phoenix Table Stats cache.
 */
public class PhoenixStatsCacheLoader extends CacheLoader<GuidePostsKey, GuidePostsInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixStatsCacheLoader.class);

    final private PhoenixStatsLoader statsLoader;
    private static volatile ExecutorService executor;

    public PhoenixStatsCacheLoader(PhoenixStatsLoader statsLoader, Configuration config) {
        this.statsLoader = statsLoader;

        if (executor == null) {
            synchronized (PhoenixStatsCacheLoader.class) {
                if (executor == null) {
                    // The size of the thread pool used for refreshing cached table stats
                    final int statsCacheThreadPoolSize = config.getInt(
                            QueryServices.STATS_CACHE_THREAD_POOL_SIZE,
                            QueryServicesOptions.DEFAULT_STATS_CACHE_THREAD_POOL_SIZE);
                    final ThreadFactory threadFactory =
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("PHOENIX-STATS-CACHE-LOADER-thread-%s")
                                    .build();
                    executor =
                            Executors.newFixedThreadPool(statsCacheThreadPoolSize, threadFactory);
                }
            }
        }
    }

    @Override
    public GuidePostsInfo load(GuidePostsKey statsKey) throws Exception {
        return statsLoader.loadStats(statsKey);
    }

    @Override
    public ListenableFuture<GuidePostsInfo> reload(
            final GuidePostsKey key,
            GuidePostsInfo prevGuidepostInfo)
    {
        if (statsLoader.needsLoad()) {
            // schedule asynchronous task
            ListenableFutureTask<GuidePostsInfo> task =
                    ListenableFutureTask.create(new Callable<GuidePostsInfo>() {
                        public GuidePostsInfo call() {
                            try {
                                return statsLoader.loadStats(key, prevGuidepostInfo);
                            } catch (Exception e) {
                                LOGGER.warn("Unable to load stats from table: " + key.toString(), e);
                                return prevGuidepostInfo;
                            }
                        }
                    });
            executor.execute(task);
            return task;
        }
        else {
            return Futures.immediateFuture(prevGuidepostInfo);
        }
    }
}