/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Central registry and manager for HTable thread pool utilization and contention metrics.
 * <p>
 * <b>Internal Use Only:</b> This class is designed for internal use only and should not be used
 * directly from outside Phoenix. External consumers should access thread pool metrics through
 * {@link org.apache.phoenix.util.PhoenixRuntime#getHTableThreadPoolHistograms()}.
 * </p>
 * <p>
 * This class serves as a singleton registry that maintains {@link HTableThreadPoolHistograms}
 * instances across the entire application lifecycle. Each {@link HTableThreadPoolHistograms}
 * instance contains utilization metrics (active thread count and queue size histograms) for a
 * specific HTable thread pool identified by a unique histogram key.
 * </p>
 * <h3>Storage and Instance Management</h3>
 * <p>
 * The manager stores {@link HTableThreadPoolHistograms} instances in a thread-safe
 * {@link ConcurrentHashMap} with the following characteristics:
 * </p>
 * <ul>
 * <li><b>Key-based storage:</b> Each histogram instance is identified by a unique string key</li>
 * <li><b>Lazy initialization:</b> Instances are created on-demand when first accessed</li>
 * <li><b>Shared instances:</b> Multiple threads/components can share the same histogram instance
 * using the same key</li>
 * <li><b>Thread-safe operations:</b> All storage operations are atomic and thread-safe</li>
 * </ul>
 * <h3>Integration with HTable Thread Pool Utilization</h3>
 * <p>
 * This manager is exclusively used by
 * {@link org.apache.phoenix.job.HTableThreadPoolWithUtilizationStats}, a specialized
 * ThreadPoolExecutor that automatically captures HTable thread pool utilization statistics. The
 * integration workflow is:
 * </p>
 * <ol>
 * <li>The thread pool calls {@link #updateActiveThreads} and {@link #updateQueueSize} to record
 * metrics</li>
 * <li>Metrics are stored in {@link HTableThreadPoolHistograms} instances managed by this class</li>
 * <li>External consumers access immutable metric snapshots through
 * {@link org.apache.phoenix.util.PhoenixRuntime#getHTableThreadPoolHistograms()}, which internally
 * calls {@link #getHistogramsForAllThreadPools()} to return {@link HistogramDistribution} instances
 * containing percentile distributions, min/max values, and operation counts for both active thread
 * count and queue size metrics</li>
 * </ol>
 * <h3>Usage Patterns</h3>
 * <p>
 * <b>CQSI Level:</b> For ConnectionQueryServicesImpl thread pools, the histogram key is always the
 * connection URL, allowing multiple CQSI instances with the same connection info to share the same
 * {@link HTableThreadPoolHistograms} instance.
 * </p>
 * <p>
 * <b>External Thread Pools:</b> For user-defined thread pools, the histogram key can be the thread
 * pool name or any unique identifier chosen by the application.
 * </p>
 * @see HTableThreadPoolHistograms
 * @see org.apache.phoenix.job.HTableThreadPoolWithUtilizationStats
 * @see org.apache.phoenix.util.PhoenixRuntime#getHTableThreadPoolHistograms()
 */
public class HTableThreadPoolMetricsManager {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(HTableThreadPoolMetricsManager.class);

  private static final ConcurrentHashMap<String,
    HTableThreadPoolHistograms> THREAD_POOL_HISTOGRAMS_MAP = new ConcurrentHashMap<>();

  private HTableThreadPoolMetricsManager() {
  }

  public static Map<String, List<HistogramDistribution>> getHistogramsForAllThreadPools() {
    Map<String, List<HistogramDistribution>> map = new HashMap<>();
    for (Map.Entry<String, HTableThreadPoolHistograms> entry : THREAD_POOL_HISTOGRAMS_MAP
      .entrySet()) {
      HTableThreadPoolHistograms hTableThreadPoolHistograms = entry.getValue();
      map.put(entry.getKey(), hTableThreadPoolHistograms.getThreadPoolHistogramsDistribution());
    }
    return map;
  }

  private static HTableThreadPoolHistograms getThreadPoolHistograms(String histogramKey,
    Supplier<HTableThreadPoolHistograms> supplier) {
    if (supplier == null) {
      return null;
    }
    return THREAD_POOL_HISTOGRAMS_MAP.computeIfAbsent(histogramKey, k -> supplier.get());
  }

  /**
   * Records the current number of active threads in the thread pool in the histogram.
   * @param histogramKey  Key to uniquely identify {@link HTableThreadPoolHistograms} instance.
   * @param activeThreads Number of active threads in the thread pool.
   * @param supplier      An idempotent supplier of {@link HTableThreadPoolHistograms}.
   */
  public static void updateActiveThreads(String histogramKey, int activeThreads,
    Supplier<HTableThreadPoolHistograms> supplier) {
    HTableThreadPoolHistograms hTableThreadPoolHistograms =
      getThreadPoolHistograms(histogramKey, supplier);
    if (hTableThreadPoolHistograms != null) {
      hTableThreadPoolHistograms.updateActiveThreads(activeThreads);
    } else {
      logWarningForNullSupplier(histogramKey);
    }
  }

  /**
   * Records the current number of tasks in the thread pool's queue in the histogram.
   * @param histogramKey Key to uniquely identify {@link HTableThreadPoolHistograms} instance.
   * @param queueSize    Number of tasks in the HTable thread pool's queue.
   * @param supplier     An idempotent supplier of {@link HTableThreadPoolHistograms}.
   */
  public static void updateQueueSize(String histogramKey, int queueSize,
    Supplier<HTableThreadPoolHistograms> supplier) {
    HTableThreadPoolHistograms hTableThreadPoolHistograms =
      getThreadPoolHistograms(histogramKey, supplier);
    if (hTableThreadPoolHistograms != null) {
      hTableThreadPoolHistograms.updateQueuedSize(queueSize);
    } else {
      logWarningForNullSupplier(histogramKey);
    }
  }

  private static void logWarningForNullSupplier(String threadPoolName) {
    LOGGER.warn("No HTable thread pool histograms created for thread pool {}", threadPoolName);
  }

  @VisibleForTesting
  public static void clearHTableThreadPoolHistograms() {
    THREAD_POOL_HISTOGRAMS_MAP.clear();
  }
}
