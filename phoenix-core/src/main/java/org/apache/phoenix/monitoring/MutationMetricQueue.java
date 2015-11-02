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

import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
    public Map<String, Map<String, Long>> aggregate() {
        Map<String, Map<String, Long>> publishedMetrics = new HashMap<>();
        for (Entry<String, MutationMetric> entry : tableMutationMetric.entrySet()) {
            String tableName = entry.getKey();
            MutationMetric metric = entry.getValue();
            Map<String, Long> publishedMetricsForTable = publishedMetrics.get(tableName);
            if (publishedMetricsForTable == null) {
                publishedMetricsForTable = new HashMap<>();
                publishedMetrics.put(tableName, publishedMetricsForTable);
            }
            publishedMetricsForTable.put(metric.getNumMutations().getName(), metric.getNumMutations().getValue());
            publishedMetricsForTable.put(metric.getMutationsSizeBytes().getName(), metric.getMutationsSizeBytes().getValue());
            publishedMetricsForTable.put(metric.getCommitTimeForMutations().getName(), metric.getCommitTimeForMutations().getValue());
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
        private final CombinableMetric numMutations = new CombinableMetricImpl(MUTATION_BATCH_SIZE);
        private final CombinableMetric mutationsSizeBytes = new CombinableMetricImpl(MUTATION_BYTES);
        private final CombinableMetric totalCommitTimeForMutations = new CombinableMetricImpl(MUTATION_COMMIT_TIME);

        public MutationMetric(long numMutations, long mutationsSizeBytes, long commitTimeForMutations) {
            this.numMutations.change(numMutations);
            this.mutationsSizeBytes.change(mutationsSizeBytes);
            this.totalCommitTimeForMutations.change(commitTimeForMutations);
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

        public void combineMetric(MutationMetric other) {
            this.numMutations.combine(other.numMutations);
            this.mutationsSizeBytes.combine(other.mutationsSizeBytes);
            this.totalCommitTimeForMutations.combine(other.totalCommitTimeForMutations);
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
        public Map<String, Map<String, Long>> aggregate() { return Collections.emptyMap(); }
        
        
    }

}
