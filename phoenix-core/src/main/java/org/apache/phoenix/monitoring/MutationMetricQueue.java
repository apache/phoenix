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

import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.phoenix.log.LogLevel;

/**
 * Queue that tracks various writes/mutations related phoenix request metrics.
 */
public class MutationMetricQueue {
    
    // Map of table name -> mutation metric
    private Map<String, MutationMetric> tableMutationMetric = new HashMap<>();
    
    public void addMetricsForTable(String tableName, MutationMetric metric) {
        MutationMetric tableMetric = tableMutationMetric.get(tableName);
        if (tableMetric == null) {
            tableMutationMetric.put(tableName, metric);
        } else {
            tableMetric.combineMetric(metric);
        }
    }

    public void combineMetricQueues(MutationMetricQueue other) {
        Map<String, MutationMetric> tableMetricMap = other.tableMutationMetric;
        for (Entry<String, MutationMetric> entry : tableMetricMap.entrySet()) {
            addMetricsForTable(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Publish the metrics to wherever you want them published. The internal state is cleared out after every publish.
     * @return map of table name -> list of pair of (metric name, metric value)
     */
    public Map<String, Map<MetricType, Long>> aggregate() {
        Map<String, Map<MetricType, Long>> publishedMetrics = new HashMap<>();
        for (Entry<String, MutationMetric> entry : tableMutationMetric.entrySet()) {
            String tableName = entry.getKey();
            MutationMetric metric = entry.getValue();
            Map<MetricType, Long> publishedMetricsForTable = publishedMetrics.get(tableName);
            if (publishedMetricsForTable == null) {
                publishedMetricsForTable = new HashMap<>();
                publishedMetrics.put(tableName, publishedMetricsForTable);
            }
            publishedMetricsForTable.put(metric.getNumMutations().getMetricType(), metric.getNumMutations().getValue());
            publishedMetricsForTable.put(metric.getMutationsSizeBytes().getMetricType(), metric.getMutationsSizeBytes().getValue());
            publishedMetricsForTable.put(metric.getCommitTimeForMutations().getMetricType(), metric.getCommitTimeForMutations().getValue());
            publishedMetricsForTable.put(metric.getNumFailedMutations().getMetricType(), metric.getNumFailedMutations().getValue());
        }
        return publishedMetrics;
    }
    
    public void clearMetrics() {
        tableMutationMetric.clear(); // help gc
    }
    
    /**
     * Class that holds together the various metrics associated with mutations.
     */
    public static class MutationMetric {
        private final CombinableMetric numMutations;;
        private final CombinableMetric mutationsSizeBytes;
        private final CombinableMetric totalCommitTimeForMutations;
        private final CombinableMetric numFailedMutations;

        public MutationMetric(LogLevel connectionLogLevel, long numMutations, long mutationsSizeBytes, long commitTimeForMutations, long numFailedMutations) {
            this.numMutations = MetricUtil.getCombinableMetric(connectionLogLevel,MUTATION_BATCH_SIZE);
            this.mutationsSizeBytes =MetricUtil.getCombinableMetric(connectionLogLevel,MUTATION_BYTES);
            this.totalCommitTimeForMutations =MetricUtil.getCombinableMetric(connectionLogLevel,MUTATION_COMMIT_TIME);
            this.numFailedMutations = MetricUtil.getCombinableMetric(connectionLogLevel,MUTATION_BATCH_FAILED_SIZE);
            this.numMutations.change(numMutations);
            this.mutationsSizeBytes.change(mutationsSizeBytes);
            this.totalCommitTimeForMutations.change(commitTimeForMutations);
            this.numFailedMutations.change(numFailedMutations);
        }

        public CombinableMetric getCommitTimeForMutations() {
            return totalCommitTimeForMutations;
        }

        public CombinableMetric getNumMutations() {
            return numMutations;
        }

        public CombinableMetric getMutationsSizeBytes() {
            return mutationsSizeBytes;
        }
        
        public CombinableMetric getNumFailedMutations() {
            return numFailedMutations;
        }

        public void combineMetric(MutationMetric other) {
            this.numMutations.combine(other.numMutations);
            this.mutationsSizeBytes.combine(other.mutationsSizeBytes);
            this.totalCommitTimeForMutations.combine(other.totalCommitTimeForMutations);
            this.numFailedMutations.combine(other.numFailedMutations);
        }

    }

    /**
     * Class to represent a no-op mutation metric. Used in places where request level metric tracking for mutations is not
     * needed or desired.
     */
    public static class NoOpMutationMetricsQueue extends MutationMetricQueue {

        public static final NoOpMutationMetricsQueue NO_OP_MUTATION_METRICS_QUEUE = new NoOpMutationMetricsQueue();

        private NoOpMutationMetricsQueue() {}

        @Override
        public void addMetricsForTable(String tableName, MutationMetric metric) {}

        @Override
        public Map<String, Map<MetricType, Long>> aggregate() { return Collections.emptyMap(); }
        
        
    }

}
