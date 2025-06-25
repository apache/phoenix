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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.monitoring.HTableThreadPoolHistograms;
import org.apache.phoenix.monitoring.HTableThreadPoolMetricsManager;
import org.apache.phoenix.util.PhoenixRuntime;


/**
 * <b>External User-Facing API</b>
 * <p>
 * A specialized ThreadPoolExecutor designed specifically for capturing HTable thread pool
 * utilization statistics. This class extends the standard ThreadPoolExecutor with built-in
 * instrumentation to automatically collect key utilization metrics including active thread count
 * and queue size.
 * </p>
 * <h3>Purpose</h3>
 * <p>
 * Use this ThreadPoolExecutor when you need to monitor and analyze the performance characteristics
 * of HTable thread pool. The collected metrics help in understanding thread pool behavior,
 * identifying bottlenecks, and optimizing thread pool configurations.
 * </p>
 * <h3>Setup and Configuration</h3>
 * <p>
 * When instantiating this thread pool executor, you must provide an idempotent supplier that
 * returns an instance of {@link HTableThreadPoolHistograms}. This supplier enables the collection
 * of utilization statistics. Within the supplier, you can also attach custom tags to the
 * {@link HTableThreadPoolHistograms} instance for enhanced monitoring and filtering capabilities.
 * </p>
 * <p>
 * <b>Important:</b> If you pass a null supplier, metrics collection will be completely disabled.
 * This can be useful in scenarios where you want to use the thread pool without the overhead of
 * collecting utilization statistics.
 * </p>
 * <h3>Consuming Metrics</h3>
 * <p>
 * To retrieve the collected metrics as percentile distributions:
 * </p>
 * <ol>
 * <li>Call {@link PhoenixRuntime#getHTableThreadPoolHistograms()}</li>
 * <li>Use the htableThreadPoolHistogramsName as the key to retrieve the list of
 * {@link org.apache.phoenix.monitoring.PercentileHistogramDistribution} instances</li>
 * <li>Each metric type will have its own distribution instance in the returned list</li>
 * </ol>
 * <p>
 * Refer to the {@link org.apache.phoenix.monitoring.PercentileHistogramDistribution} documentation
 * to understand how to extract percentile values from the recorded data.
 * </p>
 * <h3>Usage Examples</h3>
 * <p>
 * For comprehensive usage examples and best practices, refer to the following integration tests:
 * </p>
 * <ul>
 * <li>CQSIThreadPoolMetricsIT</li>
 * <li>ExternalHTableThreadPoolMetricsIT</li>
 * </ul>
 * @see HTableThreadPoolHistograms
 * @see PhoenixRuntime#getHTableThreadPoolHistograms()
 * @see org.apache.phoenix.monitoring.PercentileHistogramDistribution
 */
public class HTableThreadPoolWithUtilizationStats extends ThreadPoolExecutor {

    private final String htableThreadPoolHistogramsName;
    private final Supplier<HTableThreadPoolHistograms> hTableThreadPoolHistogramsSupplier;

    /**
     * Creates a new HTable thread pool executor with built-in utilization statistics collection.
     * <p>
     * This constructor accepts all the standard {@link ThreadPoolExecutor} parameters plus
     * additional parameters specific to HTable thread pool monitoring. The thread pool will
     * automatically collect utilization metrics (active thread count and queue size) during task
     * execution. To retrieve the collected metrics, use
     * {@link PhoenixRuntime#getHTableThreadPoolHistograms()}.
     * </p>
     * @param htableThreadPoolHistogramsName Unique identifier for this thread pool's metrics. This
     *                                       name serves as the key in the metrics map returned by
     *                                       {@link PhoenixRuntime#getHTableThreadPoolHistograms()}.
     *                                       Choose a descriptive name that identifies the purpose
     *                                       of this thread pool.
     * @param supplier                       Idempotent supplier that provides the
     *                                       {@link HTableThreadPoolHistograms} instance for metrics
     *                                       collection. This supplier will be called only the first
     *                                       time a metric is recorded for the given
     *                                       htableThreadPoolHistogramsName. Subsequent metric
     *                                       recordings will reuse the same histogram instance.
     *                                       <b>Pass null to disable metrics collection
     *                                       entirely</b>, which eliminates monitoring overhead but
     *                                       provides no utilization statistics.
     * @see ThreadPoolExecutor#ThreadPoolExecutor(int, int, long, TimeUnit, BlockingQueue,
     *      ThreadFactory)
     * @see HTableThreadPoolHistograms
     * @see PhoenixRuntime#getHTableThreadPoolHistograms()
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
