package org.apache.phoenix.monitoring;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolMetricsManager {

    volatile private static ConcurrentHashMap<ThreadPoolExecutor, ThreadPoolHistograms>
            threadPoolMetricsMap = null;

   public static void collectUtilizationHistograms(ThreadPoolExecutor threadPoolExecutor,
                                            ThreadPoolHistograms threadPoolHistogram) {
       ConcurrentHashMap<ThreadPoolExecutor, ThreadPoolHistograms> localRef = threadPoolMetricsMap;
       if (localRef == null) {
           synchronized (ThreadPoolMetricsManager.class) {
               localRef = threadPoolMetricsMap;
               if (localRef == null) {
                   threadPoolMetricsMap = new ConcurrentHashMap<>();
               }
           }
       }
       threadPoolMetricsMap.put(threadPoolExecutor, threadPoolHistogram);
   }

   public static Map<String, List<HistogramDistribution>> getHistogramsForAllThreadPools() {
        Map<String, List<HistogramDistribution>> map = new HashMap<>();
        if (threadPoolMetricsMap == null) {
            return map;
        }
        Iterator<Map.Entry<ThreadPoolExecutor, ThreadPoolHistograms>> iterator =
                threadPoolMetricsMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ThreadPoolExecutor, ThreadPoolHistograms> entry = iterator.next();
            ThreadPoolExecutor threadPoolExecutor = entry.getKey();
            if (threadPoolExecutor.isShutdown()) {
                iterator.remove();
                continue;
            }
            ThreadPoolHistograms threadPoolHistograms = entry.getValue();
            map.put(threadPoolExecutor.toString(),
                    threadPoolHistograms.getThreadPoolHistogramsDistribution());
        }
        return map;
   }
}
