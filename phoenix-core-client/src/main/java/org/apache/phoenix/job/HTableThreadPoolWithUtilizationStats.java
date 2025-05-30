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
package org.apache.phoenix.job;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.monitoring.HTableThreadPoolHistograms;
import org.apache.phoenix.monitoring.HTableThreadPoolMetricsManager;
import org.apache.phoenix.util.PhoenixRuntime;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class HTableThreadPoolWithUtilizationStats extends ThreadPoolExecutor {

    private final String htableThreadPoolHistogramsName;
    private final Supplier<HTableThreadPoolHistograms> hTableThreadPoolHistogramsSupplier;

    /**
     * A wrapper over traditional ThreadPoolExecutor with instrumentation for capturing utilization
     * metrics i.e. active thread count and queue size. When a new task arrives in the thread pool,
     * active thread count and queue size stats at that moment are record in histograms.
     * <br/><br/>
     * For utilization stats to be recorded a {@link HTableThreadPoolHistograms} supplier should be
     * provided and should be non-null. The supplier should be idempotent.
     * <br/><br/>
     * Please refer documentation of {@link HTableThreadPoolHistograms} to understand how stats
     * are recorded.
     * <br/><br/>
     * All parameters are same as the ones accepted by {@link ThreadPoolExecutor} in addition to
     * few extra for the purpose of collecting stats.
     * <br/><br/>
     * To better understand how to use this wrapper ThreadPoolExecutor along with
     * {@link HTableThreadPoolHistograms} please refer ITs:
     * <li>CQSIThreadPoolMetricsIT</li>
     * <li>ExternalHTableThreadPoolMetricsIT</li>
     * @param htableThreadPoolHistogramsName Name of the {@link HTableThreadPoolHistograms}
     *                                       instance. This will be used as key in the map
     *                                       returned by
     *                                       {@link PhoenixRuntime#getHTableThreadPoolHistograms()}.
     * @param supplier Supplier which will return {@link HTableThreadPoolHistograms} instance and
     *                should be idempotent. Passing a null value disables stats collection.
     */
    public HTableThreadPoolWithUtilizationStats(int corePoolSize, int maximumPoolSize,
                                                long keepAliveTime, TimeUnit unit,
                                                BlockingQueue<Runnable> workQueue,
                                                ThreadFactory threadFactory,
                                                String htableThreadPoolHistogramsName,
                                                Supplier<HTableThreadPoolHistograms> supplier) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.htableThreadPoolHistogramsName = htableThreadPoolHistogramsName;
        this.hTableThreadPoolHistogramsSupplier = supplier;
    }

    public void execute(Runnable runnable) {
        Preconditions.checkNotNull(runnable);
        if (hTableThreadPoolHistogramsSupplier != null) {
            HTableThreadPoolMetricsManager.updateActiveThreads(htableThreadPoolHistogramsName,
                    this.getActiveCount(), hTableThreadPoolHistogramsSupplier);
            // Should we offset queue size by available threads if CorePoolSize == MaxPoolSize?
            // Tasks will first be put into thread pool's queue and will be consumed by a worker
            // thread waiting for tasks to arrive in queue. But while a task is in queue, queue
            // size > 0 though active no. of threads might be less than MaxPoolSize.
            HTableThreadPoolMetricsManager.updateQueueSize(htableThreadPoolHistogramsName,
                    this.getQueue().size(), hTableThreadPoolHistogramsSupplier);
        }
        super.execute(runnable);
    }
}
