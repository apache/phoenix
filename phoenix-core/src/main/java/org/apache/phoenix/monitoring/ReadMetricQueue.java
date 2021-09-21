/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.monitoring;

import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nonnull;

import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.monitoring.CombinableMetric.NoOpRequestMetric;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Queue of all metrics associated with performing reads from the cluster.
 */
public class ReadMetricQueue {

    private static final int MAX_QUEUE_SIZE = 20000; // TODO: should this be configurable?

    private final ConcurrentMap<MetricKey, Queue<CombinableMetric>> metricsMap = new ConcurrentHashMap<>();

    private final List<ScanMetricsHolder> scanMetricsHolderList = new ArrayList<ScanMetricsHolder>();


    private LogLevel connectionLogLevel;

    private boolean isRequestMetricsEnabled;

    public ReadMetricQueue(boolean isRequestMetricsEnabled, LogLevel connectionLogLevel) {
        this.isRequestMetricsEnabled = isRequestMetricsEnabled;
        this.connectionLogLevel = connectionLogLevel;
    }

    public CombinableMetric allotMetric(MetricType type, String tableName) {
        if (type.isLoggingEnabled(connectionLogLevel) || isRequestMetricsEnabled) {
            MetricKey key = new MetricKey(type, tableName);
            Queue<CombinableMetric> q = getMetricQueue(key);
            CombinableMetric metric = getMetric(type);
            q.offer(metric);
            return metric;
        } else {
            return NoOpRequestMetric.INSTANCE;
        }
    }

    @VisibleForTesting
    public CombinableMetric getMetric(MetricType type) {
        CombinableMetric metric = new CombinableMetricImpl(type);
        return metric;
    }

    /**
     * @return map of table {@code name -> list } of pair of (metric name, metric value)
     */
    public Map<String, Map<MetricType, Long>> aggregate() {
        Map<String, Map<MetricType, Long>> publishedMetrics = new HashMap<>();
        for (Entry<MetricKey, Queue<CombinableMetric>> entry : metricsMap.entrySet()) {
            String tableNameToPublish = entry.getKey().tableName;
            Collection<CombinableMetric> metrics = entry.getValue();
            if (metrics.size() > 0) {
                CombinableMetric m = combine(metrics);
                Map<MetricType, Long> map = publishedMetrics.get(tableNameToPublish);
                if (map == null) {
                    map = new HashMap<>();
                    publishedMetrics.put(tableNameToPublish, map);
                }
                map.put(m.getMetricType(), m.getValue());
            }
        }
        return publishedMetrics;
    }

    public void clearMetrics() {
        metricsMap.clear(); // help gc
    }

    private static CombinableMetric combine(Collection<CombinableMetric> metrics) {
        int size = metrics.size();
        if (size == 0) { throw new IllegalArgumentException("Metrics collection needs to have at least one element"); }
        Iterator<CombinableMetric> itr = metrics.iterator();
        //Clone first metric for combining so that aggregate always give consistent result
        CombinableMetric combinedMetric = itr.next().clone();
        while (itr.hasNext()) {
            combinedMetric = combinedMetric.combine(itr.next());
        }
        return combinedMetric;
    }

    /**
     * Combine the metrics. This method should only be called in a single threaded manner when the two metric holders
     * are not getting modified.
     */
    public ReadMetricQueue combineReadMetrics(ReadMetricQueue other) {
        ConcurrentMap<MetricKey, Queue<CombinableMetric>> otherMetricsMap = other.metricsMap;
        for (Entry<MetricKey, Queue<CombinableMetric>> entry : otherMetricsMap.entrySet()) {
            MetricKey key = entry.getKey();
            Queue<CombinableMetric> otherQueue = entry.getValue();
            CombinableMetric combinedMetric = null;
            // combine the metrics corresponding to this metric key before putting it in the queue.
            for (CombinableMetric m : otherQueue) {
                if (combinedMetric == null) {
                    combinedMetric = m;
                } else {
                    combinedMetric.combine(m);
                }
            }
            if (combinedMetric != null) {
                Queue<CombinableMetric> thisQueue = getMetricQueue(key);
                thisQueue.offer(combinedMetric);
            }
        }
        return this;
    }

    /**
     * Inner class whose instances are used as keys in the metrics map.
     */
    private static class MetricKey {
        @Nonnull
        private final MetricType type;

        @Nonnull
        private final String tableName;

        MetricKey(MetricType type, String tableName) {
            checkNotNull(type);
            checkNotNull(tableName);
            this.type = type;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + tableName.hashCode();
            result = prime * result + type.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            MetricKey other = (MetricKey)obj;
            if (tableName.equals(other.tableName) && type == other.type) return true;
            return false;
        }

    }

    private Queue<CombinableMetric> getMetricQueue(MetricKey key) {
        Queue<CombinableMetric> q = metricsMap.get(key);
        if (q == null) {
            q = new LinkedBlockingQueue<CombinableMetric>(MAX_QUEUE_SIZE);
            Queue<CombinableMetric> curQ = metricsMap.putIfAbsent(key, q);
            if (curQ != null) {
                q = curQ;
            }
        }
        return q;
    }

    public void addScanHolder(ScanMetricsHolder holder){
        scanMetricsHolderList.add(holder);
    }

    public List<ScanMetricsHolder> getScanMetricsHolderList() {
        return scanMetricsHolderList;
    }
    
    public boolean isRequestMetricsEnabled() {
        return isRequestMetricsEnabled;
    }    

}
