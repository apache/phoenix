package org.apache.phoenix.monitoring;

import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class HTableThreadPoolMetricsManager {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(HTableThreadPoolMetricsManager.class);

    volatile private static ConcurrentHashMap<String, HTableThreadPoolHistograms>
            threadPoolHistogramsMap = null;

    private static HTableThreadPoolMetricsManager hTableThreadPoolMetricsManager = null;

    protected HTableThreadPoolMetricsManager() {
        threadPoolHistogramsMap = new ConcurrentHashMap<>();
    }

    public static HTableThreadPoolMetricsManager getInstance() {
        if (hTableThreadPoolMetricsManager == null) {
            synchronized (HTableThreadPoolMetricsManager.class) {
                if (hTableThreadPoolMetricsManager == null) {
                    QueryServicesOptions options = QueryServicesOptions.withDefaults();
                    if (!options.isHTableThreadPoolMetricsEnabled()) {
                        hTableThreadPoolMetricsManager =
                                NoOpHTableThreadPoolMetricsManager.noOpHTableThreadPoolMetricManager;
                    }
                    hTableThreadPoolMetricsManager = new HTableThreadPoolMetricsManager();
                }
            }
        }
        return hTableThreadPoolMetricsManager;
    }

   public static Map<String, List<HistogramDistribution>> getHistogramsForAllThreadPools() {
        Map<String, List<HistogramDistribution>> map = new HashMap<>();
        if (threadPoolHistogramsMap == null) {
            return map;
        }
        for (Map.Entry<String, HTableThreadPoolHistograms> entry :
                threadPoolHistogramsMap.entrySet()) {
            HTableThreadPoolHistograms hTableThreadPoolHistograms = entry.getValue();
            map.put(entry.getKey(),
                    hTableThreadPoolHistograms.getThreadPoolHistogramsDistribution());
        }
        return map;
   }

   private HTableThreadPoolHistograms getThreadPoolHistograms(
           String threadPoolName, Supplier<HTableThreadPoolHistograms> supplier) {
        HTableThreadPoolHistograms hTableThreadPoolHistograms =
                threadPoolHistogramsMap.get(threadPoolName);
        if (hTableThreadPoolHistograms == null) {
            synchronized (HTableThreadPoolMetricsManager.class) {
                hTableThreadPoolHistograms = threadPoolHistogramsMap.get(threadPoolName);
                if (hTableThreadPoolHistograms == null) {
                    hTableThreadPoolHistograms = supplier.get();
                    if (hTableThreadPoolHistograms != null) {
                        threadPoolHistogramsMap.put(threadPoolName, hTableThreadPoolHistograms);
                    }
                }
            }
        }
        return hTableThreadPoolHistograms;
   }

   public void updateActiveThreads(String threadPoolName, int activeThreads,
                                   Supplier<HTableThreadPoolHistograms> supplier) {
        HTableThreadPoolHistograms hTableThreadPoolHistograms =
                getThreadPoolHistograms(threadPoolName, supplier);
        if (hTableThreadPoolHistograms != null) {
            hTableThreadPoolHistograms.updateActiveThreads(activeThreads);
        }
        else {
           logWarningForNullSupplier(threadPoolName);
        }
   }

   public void updateQueueSize(String threadPoolName, int queueSize,
                               Supplier<HTableThreadPoolHistograms> supplier) {
        HTableThreadPoolHistograms hTableThreadPoolHistograms =
                getThreadPoolHistograms(threadPoolName, supplier);
        if (hTableThreadPoolHistograms != null) {
            hTableThreadPoolHistograms.updateQueuedSize(queueSize);
        }
        else {
            logWarningForNullSupplier(threadPoolName);
        }
   }

   private void logWarningForNullSupplier(String threadPoolName) {
       LOGGER.warn("No HTable thread pool histograms created for thread pool {}", threadPoolName);
   }
}
