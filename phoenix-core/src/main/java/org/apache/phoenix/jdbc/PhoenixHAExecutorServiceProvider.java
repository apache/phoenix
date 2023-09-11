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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_REJECTED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_REJECTED_COUNTER;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

/**
 * Provides a bounded and configurable executor service for {@link ParallelPhoenixConnection} and
 * related infra. Provides a lazily initialized singleton executor service to be used for all
 * ParallelPhoenixConnections Also provides visibility into the capacity of ExecutorService
 * {@link PhoenixHAExecutorServiceProvider#hasCapacity(Properties)}
 */
public class PhoenixHAExecutorServiceProvider {

    public static final String HA_MAX_POOL_SIZE = "phoenix.ha.max.pool.size";
    public static final String DEFAULT_HA_MAX_POOL_SIZE = "30";
    public static final String HA_MAX_QUEUE_SIZE = "phoenix.ha.max.queue.size";
    public static final String DEFAULT_HA_MAX_QUEUE_SIZE = "300";
    public static final String HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD =
            "phoenix.ha.threadpool.queue.backoff.threshold";
    public static final String DEFAULT_HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD = "0.9";

    public static final String HA_CLOSE_MAX_POOL_SIZE = "phoenix.ha.close.max.pool.size";
    public static final String DEFAULT_HA_CLOSE_MAX_POOL_SIZE = "15";
    public static final String HA_CLOSE_MAX_QUEUE_SIZE = "phoenix.ha.close.max.queue.size";
    public static final String DEFAULT_HA_CLOSE_MAX_QUEUE_SIZE = "150";

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixHAExecutorServiceProvider.class);

    // Can make configurable if needed
    private static final int KEEP_ALIVE_TIME_SECONDS = 120;

    private static volatile List<PhoenixHAClusterExecutorServices> INSTANCE = null;

    /**
     * pojo for holding an execution and a close executorService
     */
    public static class PhoenixHAClusterExecutorServices {
        private final ExecutorService executorService;
        private final ExecutorService closeExecutorService;

        PhoenixHAClusterExecutorServices(ExecutorService executorService, ExecutorService closeExecutorService) {
            this.executorService = executorService;
            this.closeExecutorService = closeExecutorService;
        }

        public ExecutorService getExecutorService() {
            return executorService;
        }

        public ExecutorService getCloseExecutorService() {
            return closeExecutorService;
        }
    }

    private PhoenixHAExecutorServiceProvider() {
    }

    public static List<PhoenixHAClusterExecutorServices> get(Properties properties) {
        if (INSTANCE == null) {
            synchronized (PhoenixHAExecutorServiceProvider.class) {
                if (INSTANCE == null) {
                    INSTANCE = initThreadPool(properties);
                }
            }
        }
        return INSTANCE;
    }

    @VisibleForTesting
    static synchronized void resetExecutor() {
        INSTANCE = null;
    }

    /**
     * Checks if the underlying executorServices have sufficient available capacity based on
     * {@link #HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD}. Monitors the capacity of the blockingqueues
     * linked with the executor services.
     *
     * @param properties phoenix properties
     * @return true if queue is less than {@link #HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD} full
     */
    public static List<Boolean> hasCapacity(Properties properties) {
        if (INSTANCE == null) {
            return ImmutableList.of(Boolean.TRUE, Boolean.TRUE);
        }
        double backoffThreshold =
                Double.parseDouble(properties.getProperty(HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD,
                        DEFAULT_HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD));
        int i = 0;
        List<Boolean> executorCapacities = new ArrayList<>();
        List<ExecutorService> executorServicesList = ImmutableList.of(INSTANCE.get(0).getExecutorService(), INSTANCE.get(1).getExecutorService());
        for (ExecutorService executor : executorServicesList) {
            double queueSize = ((ThreadPoolExecutor) executor).getQueue().size();
            double queueRemainingCapacity =
                    ((ThreadPoolExecutor) executor).getQueue().remainingCapacity();
            double queueCapacity = queueSize + queueRemainingCapacity;
            boolean hasCapacity = ((queueSize / queueCapacity) < backoffThreshold);
            if (!hasCapacity) {
                LOGGER.warn(
                        "PhoenixHAExecutorServiceProvider ThreadPoolExecutor[" + i + "] hasCapacity: false queueSize:" + queueSize + " queueCapacity:"
                                + queueCapacity + " backoffThreshold:" + backoffThreshold);
            }
            i++;
            executorCapacities.add(hasCapacity);
        }
        return executorCapacities;
    }

    // We need a threadPool that increases the number of threads for incoming tasks first rather
    // than the default
    // behavior of filling the queue first, hence we have the corePoolSize and maxPoolSize as same
    private static List<PhoenixHAClusterExecutorServices> initThreadPool(Properties properties) {
        int maxPoolSize =
                Integer.parseInt(
                        properties.getProperty(HA_MAX_POOL_SIZE, DEFAULT_HA_MAX_POOL_SIZE));
        int maxQueueSize =
                Integer.parseInt(
                        properties.getProperty(HA_MAX_QUEUE_SIZE, DEFAULT_HA_MAX_QUEUE_SIZE));
        ThreadPoolExecutor pool1 =
                createThreadPool(maxPoolSize, maxQueueSize, "phoenixha1", getGlobalExecutorMetricsForPool1()
                );
        ThreadPoolExecutor pool2 =
                createThreadPool(maxPoolSize, maxQueueSize, "phoenixha2", getGlobalExecutorMetricsForPool2()
                );

        //Make the close executor services
        maxPoolSize =
                Integer.parseInt(
                        properties.getProperty(HA_CLOSE_MAX_POOL_SIZE, DEFAULT_HA_CLOSE_MAX_POOL_SIZE));
        maxQueueSize =
                Integer.parseInt(
                        properties.getProperty(HA_CLOSE_MAX_QUEUE_SIZE, DEFAULT_HA_CLOSE_MAX_QUEUE_SIZE));

        ThreadPoolExecutor closePool1 =
                createThreadPool(maxPoolSize, maxQueueSize, "phoenixha1close");
        ThreadPoolExecutor closePool2 =
                createThreadPool(maxPoolSize, maxQueueSize, "phoenixha2close");
        closePool1.allowCoreThreadTimeOut(true);
        closePool2.allowCoreThreadTimeOut(true);

        return ImmutableList.of(new PhoenixHAClusterExecutorServices(pool1, closePool1), new PhoenixHAClusterExecutorServices(pool2, closePool2));
    }

    private static ThreadPoolExecutor createThreadPool(int maxPoolSize, int maxQueueSize, String threadPoolNamePrefix) {
        return createThreadPool(maxPoolSize, maxQueueSize, threadPoolNamePrefix, null);
    }

    private static ThreadPoolExecutor createThreadPool(int maxPoolSize, int maxQueueSize, String threadPoolNamePrefix,
                                                       @Nullable GlobalExecutorMetrics metrics) {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(maxQueueSize);
        RejectedExecutionHandler handler;
        if (metrics != null) {
            handler = new MonitoredCallerRunsPolicy(threadPoolNamePrefix, metrics);
        } else {
            handler = new ThreadPoolExecutor.CallerRunsPolicy();
        }
        ThreadPoolExecutor pool =
                new PhoenixHAThreadPoolExecutor(maxPoolSize, maxPoolSize, KEEP_ALIVE_TIME_SECONDS,
                        TimeUnit.SECONDS, queue,
                        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadPoolNamePrefix + "-%d").build(),
                        handler, metrics);
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private static GlobalExecutorMetrics getGlobalExecutorMetricsForPool1() {
        return new GlobalExecutorMetrics(GLOBAL_HA_PARALLEL_POOL1_TASK_REJECTED_COUNTER,
                GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTED_COUNTER,
                GLOBAL_HA_PARALLEL_POOL1_TASK_QUEUE_WAIT_TIME,
                GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTION_TIME,
                GLOBAL_HA_PARALLEL_POOL1_TASK_END_TO_END_TIME);
    }

    private static GlobalExecutorMetrics getGlobalExecutorMetricsForPool2() {
        return new GlobalExecutorMetrics(GLOBAL_HA_PARALLEL_POOL2_TASK_REJECTED_COUNTER,
                GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTED_COUNTER,
                GLOBAL_HA_PARALLEL_POOL2_TASK_QUEUE_WAIT_TIME,
                GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTION_TIME,
                GLOBAL_HA_PARALLEL_POOL2_TASK_END_TO_END_TIME);
    }

    private static class MonitoredCallerRunsPolicy extends ThreadPoolExecutor.CallerRunsPolicy {
        private final String threadPoolName;
        private final GlobalExecutorMetrics metrics;

        public MonitoredCallerRunsPolicy(String threadPoolName, GlobalExecutorMetrics metrics) {
            this.threadPoolName = threadPoolName;
            this.metrics = metrics;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            super.rejectedExecution(r, e);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        "Task was rejected by " + threadPoolName + " and executed in caller's thread");
            }
            metrics.getTaskRejectedCounter().increment();
        }
    }

    // Executor with monitoring
    private static class PhoenixHAThreadPoolExecutor extends ThreadPoolExecutor {

        private final GlobalExecutorMetrics metrics;

        public PhoenixHAThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                           long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue,
                                           ThreadFactory threadFactory, RejectedExecutionHandler handler,
                                           GlobalExecutorMetrics metrics) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory,
                    handler);
            this.metrics = metrics;
        }

        @Override
        public void execute(Runnable r) {
            if (metrics != null) {
                super.execute(new MonitoredRunnable<>(r, null));
                metrics.getTaskExecutedCounter().increment();
            } else {
                super.execute(r);
            }
        }

        @Override
        protected void beforeExecute(Thread t, Runnable r) {
            if (r instanceof MonitoredRunnable && metrics != null) {
                MonitoredRunnable<?> mr = (MonitoredRunnable<?>) r;
                mr.taskBeginTime = EnvironmentEdgeManager.currentTime();
                long taskQueueWaitTime = mr.taskBeginTime - mr.taskSubmitTime;
                metrics.getTaskQueueWaitTime().update(taskQueueWaitTime);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                            String.format("%s waited %d ms", mr.toString(), taskQueueWaitTime));
                }
            }
            super.beforeExecute(t, r);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            try {
                super.afterExecute(r, t);
            } finally {
                if (r instanceof MonitoredRunnable && metrics != null) {
                    MonitoredRunnable<?> mr = (MonitoredRunnable<?>) r;
                    mr.taskEndTime = EnvironmentEdgeManager.currentTime();
                    long taskExecutionTime = mr.taskEndTime - mr.taskBeginTime;
                    long taskEndToEndTime = mr.taskEndTime - mr.taskSubmitTime;
                    metrics.getTaskExecutionTime().update(taskExecutionTime);
                    metrics.getTaskEndToEndCounter().update(taskEndToEndTime);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(
                                String.format("%s executed in %d ms with end to end time of %d",
                                        mr.toString(), taskExecutionTime, taskEndToEndTime));
                    }
                }
            }
        }
    }

    private static class MonitoredRunnable<V> extends FutureTask<V> {
        private final long taskSubmitTime;
        private long taskBeginTime;
        private long taskEndTime;

        public MonitoredRunnable(Runnable runnable, V result) {
            super(runnable, result);
            this.taskSubmitTime = EnvironmentEdgeManager.currentTime();
        }
    }

    private static class GlobalExecutorMetrics {

        private final GlobalClientMetrics taskRejectedCounter;
        private final GlobalClientMetrics taskExecutedCounter;
        private final GlobalClientMetrics taskQueueWaitTime;
        private final GlobalClientMetrics taskExecutionTime;
        private final GlobalClientMetrics taskEndToEndCounter;

        public GlobalExecutorMetrics(GlobalClientMetrics taskRejectedCounter,
                                     GlobalClientMetrics taskExecutedCounter, GlobalClientMetrics taskQueueWaitTime,
                                     GlobalClientMetrics taskExecutionTime, GlobalClientMetrics taskEndToEndCounter) {
            this.taskRejectedCounter = taskRejectedCounter;
            this.taskExecutedCounter = taskExecutedCounter;
            this.taskQueueWaitTime = taskQueueWaitTime;
            this.taskExecutionTime = taskExecutionTime;
            this.taskEndToEndCounter = taskEndToEndCounter;
        }

        GlobalClientMetrics getTaskRejectedCounter() {
            return taskRejectedCounter;
        }

        GlobalClientMetrics getTaskExecutedCounter() {
            return taskExecutedCounter;
        }

        GlobalClientMetrics getTaskQueueWaitTime() {
            return taskQueueWaitTime;
        }

        GlobalClientMetrics getTaskExecutionTime() {
            return taskExecutionTime;
        }

        GlobalClientMetrics getTaskEndToEndCounter() {
            return taskEndToEndCounter;
        }
    }
}
