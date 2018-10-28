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

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * {@link CacheLoader} implementation for the Phoenix Table Stats cache.
 */
public class PhoenixStatsCacheLoader extends CacheLoader<GuidePostsKey, GuidePostsInfo> {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixStatsCacheLoader.class);

    final private PhoenixStatsLoader statsLoader;
    final private ExecutorService executor;

    public PhoenixStatsCacheLoader(PhoenixStatsLoader statsLoader, ExecutorService executor) {
        super();
        this.statsLoader = statsLoader;
        this.executor = executor;
    }

    @Override
    public GuidePostsInfo load(GuidePostsKey statsKey) throws Exception {
        return statsLoader.loadStats(statsKey, GuidePostsInfo.NO_GUIDEPOST);
    }

    @Override
    public ListenableFuture<GuidePostsInfo> reload(
            final GuidePostsKey key,
            GuidePostsInfo prevGuidepostInfo)
    {
        // schedule asynchronous task
        ListenableFutureTask<GuidePostsInfo> task =
                ListenableFutureTask.create(
                        new Callable<GuidePostsInfo>() {
                            public GuidePostsInfo call() {
                                try {
                                    return statsLoader.loadStats(key, prevGuidepostInfo);
                                } catch (Exception e) {
                                    logger.warn("Unable to load stats from table: " + key.toString(), e);
                                    return prevGuidepostInfo;
                                }
                            }
                        }
                );
        executor.execute(task);
        return task;
    }
}