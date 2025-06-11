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
package org.apache.phoenix.monitoring;

import java.util.List;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * A collection of histograms that monitor HTable thread pool performance metrics including thread
 * utilization and queue contention. This class tracks two key metrics to help identify thread pool
 * bottlenecks and performance issues. <br/>
 * <br/>
 * <b>External User-Facing Class:</b><br/>
 * This is an external user-facing class that should only be used in conjunction with
 * {@link HTableThreadPoolUtilizationStats}. When creating an instance of
 * {@link HTableThreadPoolUtilizationStats}, a {@link java.util.function.Supplier} of this class
 * must be provided. <br/>
 * <br/>
 * <b>Monitored Metrics:</b><br/>
 * <ul>
 * <li><b>Active Threads Count</b> - Number of threads currently executing tasks</li>
 * <li><b>Queue Size</b> - Number of tasks waiting in the thread pool queue</li>
 * </ul>
 * <br/>
 * Each metric is captured using a {@link UtilizationHistogram} that provides percentile
 * distributions, min/max values, and operation counts for comprehensive analysis. <br/>
 * <br/>
 * <b>Tags:</b><br/>
 * Supports metadata tags for dimensional monitoring:
 * <ul>
 * <li><b>servers</b> - Connection quorum string (ZK quorum, master quorum, etc.)</li>
 * <li><b>cqsiName</b> - Principal identifier for CQSI instance differentiation</li>
 * <li>Custom tags via {@link #addTag(String, String)}</li>
 * </ul>
 * The instance created by a supplier can also add tags to provide additional context. <br/>
 * <br/>
 * <b>Instance Management:</b><br/>
 * For instances created internally by the CQSI class: One instance per unique connection info is
 * created at the CQSI level, and multiple CQSI instances having the same connection info will share
 * the same instance of this class. <br/>
 * For instances created externally by users: This instance management detail does not apply, and
 * users have full control over instance creation. <br/>
 * <br/>
 * Use {@link #getThreadPoolHistogramsDistribution()} to retrieve immutable snapshots of the
 * collected metrics for monitoring and analysis.
 */
public class HTableThreadPoolHistograms {
    /**
     * Predefined tag keys for dimensional monitoring and contextual categorization of histogram
     * instances. These tags provide context about the connection and CQSI instance associated with
     * the metrics.
     */
    public enum Tag {
        servers,
        cqsiName,
    }

    /**
     * Enum that captures the name of each of the monitored metrics. These names correspond to the
     * specific thread pool performance metrics being tracked.
     */
    public enum HistogramName {
        ActiveThreadsCount,
        QueueSize,
    }

    private final UtilizationHistogram activeThreadsHisto;
    private final UtilizationHistogram queuedSizeHisto;

    /**
     * Creates a new instance of HTableThreadPoolHistograms with the specified maximum values for
     * thread pool and queue size.
     * @param maxThreadPoolSize the maximum number of threads in the thread pool, used to configure
     *                          the active threads histogram
     * @param maxQueueSize      the maximum size of the thread pool queue, used to configure the
     *                          queue size histogram
     */
    public HTableThreadPoolHistograms(long maxThreadPoolSize, long maxQueueSize) {
        activeThreadsHisto = new UtilizationHistogram(maxThreadPoolSize,
                HistogramName.ActiveThreadsCount.name());
        queuedSizeHisto = new UtilizationHistogram(maxQueueSize, HistogramName.QueueSize.name());
    }

    /**
     * Updates the histogram that tracks active threads count with the current number of active
     * threads. <br/>
     * <br/>
     * This method is to be called from HTableThreadPoolUtilizationStats class only and should not
     * be used from outside Phoenix.
     * @param activeThreads the current number of threads actively executing tasks
     */
    public void updateActiveThreads(long activeThreads) {
        activeThreadsHisto.addValue(activeThreads);
    }

    /**
     * Updates the histogram that tracks queue size with the current number of queued tasks. <br/>
     * <br/>
     * This method is to be called from HTableThreadPoolUtilizationStats class only and should not
     * be used from outside Phoenix.
     * @param queuedSize the current number of tasks waiting in the thread pool queue
     */
    public void updateQueuedSize(long queuedSize) {
        queuedSizeHisto.addValue(queuedSize);
    }

    /**
     * Adds a server tag for dimensional monitoring that identifies the connection quorum string
     * such as ZK quorum, master quorum, etc. This corresponds to the "servers" tag key. <br/>
     * <br/>
     * This is an external user-facing method which can be called when creating an instance of the
     * class.
     * @param value the connection quorum string value
     */
    public void addServerTag(String value) {
        addTag(Tag.servers.name(), value);
    }

    /**
     * Adds a CQSI name tag that captures the principal of the CQSI instance. This corresponds to
     * the "cqsiName" tag key. <br/>
     * <br/>
     * This is an external user-facing method which can be called while creating instance of the
     * class.
     * @param value the principal identifier for the CQSI instance
     */
    public void addCqsiNameTag(String value) {
        addTag(Tag.cqsiName.name(), value);
    }

    /**
     * Adds a custom tag with the specified key-value pair for dimensional monitoring. This method
     * allows adding arbitrary tags beyond the predefined servers and CQSI name tags. <br/>
     * <br/>
     * This is an external user-facing method which can be called while creating instance of the
     * class.
     * @param key   the tag key
     * @param value the tag value
     */
    public void addTag(String key, String value) {
        activeThreadsHisto.addTag(key, value);
        queuedSizeHisto.addTag(key, value);
    }

    /**
     * Returns a list of HistogramDistribution which are immutable snapshots containing percentile
     * distribution, min/max values, and count of values for the monitored metrics (active threads
     * count and queue size). <br/>
     * <br/>
     * This is an external user-facing method that is called for publishing the metrics from
     * PhoenixRuntime via the
     * {@link org.apache.phoenix.util.PhoenixRuntime#getHTableThreadPoolHistograms()} method.
     * @return list of HistogramDistribution instances representing comprehensive snapshots of the
     *         metrics
     */
    public List<HistogramDistribution> getThreadPoolHistogramsDistribution() {
        return ImmutableList.of(activeThreadsHisto.getPercentileHistogramDistribution(),
                queuedSizeHisto.getPercentileHistogramDistribution());
    }
}
