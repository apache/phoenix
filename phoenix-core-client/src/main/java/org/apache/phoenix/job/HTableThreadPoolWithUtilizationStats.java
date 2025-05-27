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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class HTableThreadPoolWithUtilizationStats extends ThreadPoolExecutor {

    private final String threadPoolName;
    private final Supplier<HTableThreadPoolHistograms> hTableThreadPoolHistogramsSupplier;

    public HTableThreadPoolWithUtilizationStats(int corePoolSize, int maximumPoolSize,
                                                long keepAliveTime, TimeUnit unit,
                                                BlockingQueue<Runnable> workQueue,
                                                ThreadFactory threadFactory,
                                                String threadPoolName,
                                                Supplier<HTableThreadPoolHistograms> supplier) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.threadPoolName = threadPoolName;
        this.hTableThreadPoolHistogramsSupplier = supplier;
    }

    public void execute(Runnable runnable) {
        Preconditions.checkNotNull(runnable);
        if (hTableThreadPoolHistogramsSupplier != null) {
            HTableThreadPoolMetricsManager.getInstance().updateActiveThreads(threadPoolName,
                    this.getActiveCount(), hTableThreadPoolHistogramsSupplier);
            HTableThreadPoolMetricsManager.getInstance().updateQueueSize(threadPoolName,
                    this.getQueue().size(), hTableThreadPoolHistogramsSupplier);
        }
        super.execute(runnable);
    }
}
