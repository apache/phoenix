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

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Central place where we keep track of HTable thread pool utilization and contention
 * level metrics for all the HTable thread pools.
 */
public class HTableThreadPoolMetricsManager {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(HTableThreadPoolMetricsManager.class);

    private static final ConcurrentHashMap<String, HTableThreadPoolHistograms>
            threadPoolHistogramsMap = new ConcurrentHashMap<>();

   public static Map<String, List<HistogramDistribution>> getHistogramsForAllThreadPools() {
        Map<String, List<HistogramDistribution>> map = new HashMap<>();
        for (Map.Entry<String, HTableThreadPoolHistograms> entry :
                threadPoolHistogramsMap.entrySet()) {
            HTableThreadPoolHistograms hTableThreadPoolHistograms = entry.getValue();
            map.put(entry.getKey(),
                    hTableThreadPoolHistograms.getThreadPoolHistogramsDistribution());
        }
        return map;
   }

   private static HTableThreadPoolHistograms getThreadPoolHistograms(
           String histogramKey, Supplier<HTableThreadPoolHistograms> supplier) {
        HTableThreadPoolHistograms hTableThreadPoolHistograms =
                threadPoolHistogramsMap.get(histogramKey);
        if (hTableThreadPoolHistograms == null) {
            synchronized (HTableThreadPoolMetricsManager.class) {
                hTableThreadPoolHistograms = threadPoolHistogramsMap.get(histogramKey);
                if (hTableThreadPoolHistograms == null) {
                    hTableThreadPoolHistograms = supplier.get();
                    if (hTableThreadPoolHistograms != null) {
                        threadPoolHistogramsMap.put(histogramKey, hTableThreadPoolHistograms);
                    }
                }
            }
        }
        return hTableThreadPoolHistograms;
   }

    /**
     * Records the value of no. of active threads in HTable thread pool in
     * {@link HTableThreadPoolHistograms}.
     * <br/><br/>
     * HistogramKey is used to uniquely identify {@link HTableThreadPoolHistograms} instance for
     * recording metrics. This can be same as HTable thread pool name but not necessary. Ex- for
     * CQSI level HTable thread pool, histogramKey is the URL returned by ConnectionInfo. By
     * setting histogramKey same as Connection URL, even if multiple CQSI instances are there
     * per connection info (one just evicted from cache vs other newly created) they will share
     * same {@link HTableThreadPoolHistograms} instance.
     * <br/>
     * Ex- if one client connects to multiple HBase clusters such that for each HBase cluster a
     * separate HTable thread pool is used (like HA scenario), then histogramKey can be same as
     * thread pool name.
     * @param histogramKey Key to uniquely identify {@link HTableThreadPoolHistograms} instance.
     * @param activeThreads Number of active threads in the thread pool.
     * @param supplier An idempotent supplier of {@link HTableThreadPoolHistograms}.
     */
   public static void updateActiveThreads(String histogramKey, int activeThreads,
                                   Supplier<HTableThreadPoolHistograms> supplier) {
        HTableThreadPoolHistograms hTableThreadPoolHistograms =
                getThreadPoolHistograms(histogramKey, supplier);
        if (hTableThreadPoolHistograms != null) {
            hTableThreadPoolHistograms.updateActiveThreads(activeThreads);
        }
        else {
           logWarningForNullSupplier(histogramKey);
        }
   }

    /**
     * Records the value of no. of tasks in the HTable thread pool's queue in
     * {@link HTableThreadPoolHistograms}.
     * <br/><br/>
     * HistogramKey is used to uniquely identify {@link HTableThreadPoolHistograms} instance for
     * recording metrics. This can be same as HTable thread pool name but not necessary. Ex- for
     * CQSI level HTable thread pool, histogramKey is the URL returned by ConnectionInfo. By
     * setting histogramKey same as Connection URL, even if multiple CQSI instances are there
     * per connection info (one just evicted from cache vs other newly created) they will share
     * same {@link HTableThreadPoolHistograms} instance.
     * <br/>
     * Ex- if one client connects to multiple HBase clusters such that for each HBase cluster a
     * separate HTable thread pool is used (like HA scenario), then histogramKey can be same as
     * thread pool name.
     * @param histogramKey Key to uniquely identify {@link HTableThreadPoolHistograms} instance.
     * @param queueSize Number of tasks in the HTable thread pool's queue.
     * @param supplier An idempotent supplier of {@link HTableThreadPoolHistograms}.
     */
   public static void updateQueueSize(String histogramKey, int queueSize,
                                      Supplier<HTableThreadPoolHistograms> supplier) {
        HTableThreadPoolHistograms hTableThreadPoolHistograms =
                getThreadPoolHistograms(histogramKey, supplier);
        if (hTableThreadPoolHistograms != null) {
            hTableThreadPoolHistograms.updateQueuedSize(queueSize);
        }
        else {
            logWarningForNullSupplier(histogramKey);
        }
   }

   private static void logWarningForNullSupplier(String threadPoolName) {
       LOGGER.warn("No HTable thread pool histograms created for thread pool {}", threadPoolName);
   }

   @VisibleForTesting
   public static void clearHTableThreadPoolHistograms() {
       threadPoolHistogramsMap.clear();
   }
}
