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

import static org.apache.phoenix.monitoring.MetricType.ATOMIC_UPSERT_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.DELETE_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.INDEX_COMMIT_FAILURE_SIZE;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_BATCH_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_SQL_COUNTER;

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
     * @return map of table {@code name -> list } of pair of (metric name, metric value)
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
            publishedMetricsForTable.put(metric.getUpsertMutationsSizeBytes().getMetricType(), metric.getUpsertMutationsSizeBytes().getValue());
            publishedMetricsForTable.put(metric.getDeleteMutationsSizeBytes().getMetricType(), metric.getDeleteMutationsSizeBytes().getValue());
            publishedMetricsForTable.put(metric.getCommitTimeForMutations().getMetricType(), metric.getCommitTimeForMutations().getValue());
            publishedMetricsForTable.put(metric.getTotalCommitTimeForUpserts().getMetricType(), metric.getTotalCommitTimeForUpserts().getValue());
            publishedMetricsForTable.put(metric.getTotalCommitTimeForAtomicUpserts().getMetricType(), metric.getTotalCommitTimeForAtomicUpserts().getValue());
            publishedMetricsForTable.put(metric.getTotalCommitTimeForDeletes().getMetricType(), metric.getTotalCommitTimeForDeletes().getValue());
            publishedMetricsForTable.put(metric.getNumFailedMutations().getMetricType(), metric.getNumFailedMutations().getValue());
            publishedMetricsForTable.put(metric.getNumOfIndexCommitFailedMutations().getMetricType(), metric.getNumOfIndexCommitFailedMutations().getValue());
            publishedMetricsForTable.put(metric.getUpsertMutationSqlCounterSuccess().getMetricType(), metric.getUpsertMutationSqlCounterSuccess().getValue());
            publishedMetricsForTable.put(metric.getDeleteMutationSqlCounterSuccess().getMetricType(), metric.getDeleteMutationSqlCounterSuccess().getValue());
            publishedMetricsForTable.put(metric.getTotalMutationsSizeBytes().getMetricType(), metric.getTotalMutationsSizeBytes().getValue());
            publishedMetricsForTable.put(metric.getUpsertBatchFailedSize().getMetricType(), metric.getUpsertBatchFailedSize().getValue());
            publishedMetricsForTable.put(metric.getUpsertBatchFailedCounter().getMetricType(), metric.getUpsertBatchFailedCounter().getValue());
            publishedMetricsForTable.put(metric.getDeleteBatchFailedSize().getMetricType(), metric.getDeleteBatchFailedSize().getValue());
            publishedMetricsForTable.put(metric.getDeleteBatchFailedCounter().getMetricType(), metric.getDeleteBatchFailedCounter().getValue());

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
        private final CombinableMetric totalMutationsSizeBytes = new CombinableMetricImpl(MUTATION_BYTES);
        private final CombinableMetric totalCommitTimeForMutations = new CombinableMetricImpl(MUTATION_COMMIT_TIME);
        private final CombinableMetric numFailedMutations = new CombinableMetricImpl(MUTATION_BATCH_FAILED_SIZE);
        private final CombinableMetric totalCommitTimeForUpserts = new CombinableMetricImpl(UPSERT_COMMIT_TIME);
        private final CombinableMetric totalCommitTimeForAtomicUpserts = new CombinableMetricImpl(ATOMIC_UPSERT_COMMIT_TIME);
        private final CombinableMetric totalCommitTimeForDeletes = new CombinableMetricImpl(DELETE_COMMIT_TIME);
        private final CombinableMetric upsertMutationsSizeBytes = new CombinableMetricImpl(UPSERT_MUTATION_BYTES);
        private final CombinableMetric deleteMutationsSizeBytes = new CombinableMetricImpl(DELETE_MUTATION_BYTES);
        private final CombinableMetric upsertMutationSqlCounterSuccess = new CombinableMetricImpl(UPSERT_MUTATION_SQL_COUNTER);
        private final CombinableMetric deleteMutationSqlCounterSuccess = new CombinableMetricImpl(DELETE_MUTATION_SQL_COUNTER);
        private final CombinableMetric upsertBatchFailedSize = new CombinableMetricImpl(UPSERT_BATCH_FAILED_SIZE);
        private final CombinableMetric upsertBatchFailedCounter = new CombinableMetricImpl(UPSERT_BATCH_FAILED_COUNTER);
        private final CombinableMetric deleteBatchFailedSize = new CombinableMetricImpl(DELETE_BATCH_FAILED_SIZE);
        private final CombinableMetric deleteBatchFailedCounter = new CombinableMetricImpl(DELETE_BATCH_FAILED_COUNTER);

        private final CombinableMetric numOfIndexCommitFailMutations = new CombinableMetricImpl(
                INDEX_COMMIT_FAILURE_SIZE);

        public static final MutationMetric EMPTY_METRIC =
                new MutationMetric(0,0,0,0, 0, 0,0,0,0,0,0,0,0,0,0);

        public MutationMetric(long numMutations, long upsertMutationsSizeBytes,
                long deleteMutationsSizeBytes, long commitTimeForUpserts, long commitTimeForAtomicUpserts,
                long commitTimeForDeletes, long numFailedMutations, long upsertMutationSqlCounterSuccess,
                long deleteMutationSqlCounterSuccess, long totalMutationBytes,
                long numOfPhase3Failed, long upsertBatchFailedSize,
                long upsertBatchFailedCounter, long deleteBatchFailedSize,
                long deleteBatchFailedCounter) {
            this.numMutations.change(numMutations);
            this.totalCommitTimeForUpserts.change(commitTimeForUpserts);
            this.totalCommitTimeForAtomicUpserts.change(commitTimeForAtomicUpserts);
            this.totalCommitTimeForDeletes.change(commitTimeForDeletes);
            this.totalCommitTimeForMutations.change(commitTimeForUpserts + commitTimeForDeletes);
            this.numFailedMutations.change(numFailedMutations);
            this.numOfIndexCommitFailMutations.change(numOfPhase3Failed);
            this.upsertMutationsSizeBytes.change(upsertMutationsSizeBytes);
            this.deleteMutationsSizeBytes.change(deleteMutationsSizeBytes);
            this.totalMutationsSizeBytes.change(totalMutationBytes);
            this.upsertMutationSqlCounterSuccess.change(upsertMutationSqlCounterSuccess);
            this.deleteMutationSqlCounterSuccess.change(deleteMutationSqlCounterSuccess);
            this.upsertBatchFailedSize.change(upsertBatchFailedSize);
            this.upsertBatchFailedCounter.change(upsertBatchFailedCounter);
            this.deleteBatchFailedSize.change(deleteBatchFailedSize);
            this.deleteBatchFailedCounter.change(deleteBatchFailedCounter);
        }

        public CombinableMetric getTotalCommitTimeForUpserts() {
            return totalCommitTimeForUpserts;
        }

        public CombinableMetric getTotalCommitTimeForAtomicUpserts() { return totalCommitTimeForAtomicUpserts; }

        public CombinableMetric getTotalCommitTimeForDeletes() {
            return totalCommitTimeForDeletes;
        }

        public CombinableMetric getCommitTimeForMutations() {
            return totalCommitTimeForMutations;
        }

        public CombinableMetric getNumMutations() {
            return numMutations;
        }

        public CombinableMetric getTotalMutationsSizeBytes() {
            return totalMutationsSizeBytes;
        }

        public CombinableMetric getNumFailedMutations() {
            return numFailedMutations;
        }

        public CombinableMetric getNumOfIndexCommitFailedMutations() {
            return numOfIndexCommitFailMutations;
        }

        public CombinableMetric getUpsertMutationsSizeBytes() {
            return upsertMutationsSizeBytes;
        }

        public CombinableMetric getDeleteMutationsSizeBytes() {
            return deleteMutationsSizeBytes;
        }

        public CombinableMetric getUpsertMutationSqlCounterSuccess() {
            return upsertMutationSqlCounterSuccess;
        }

        public CombinableMetric getDeleteMutationSqlCounterSuccess() {
            return deleteMutationSqlCounterSuccess;
        }

        public CombinableMetric getUpsertBatchFailedSize() {
            return upsertBatchFailedSize;
        }

        public CombinableMetric getUpsertBatchFailedCounter() {
            return upsertBatchFailedCounter;
        }

        public CombinableMetric getDeleteBatchFailedSize() {
            return deleteBatchFailedSize;
        }

        public CombinableMetric getDeleteBatchFailedCounter() {
            return deleteBatchFailedCounter;
        }

        public void combineMetric(MutationMetric other) {
            this.numMutations.combine(other.numMutations);
            this.totalCommitTimeForUpserts.combine(other.totalCommitTimeForUpserts);
            this.totalCommitTimeForAtomicUpserts.combine(other.totalCommitTimeForAtomicUpserts);
            this.totalCommitTimeForDeletes.combine(other.totalCommitTimeForDeletes);
            this.totalCommitTimeForMutations.combine(other.totalCommitTimeForMutations);
            this.numFailedMutations.combine(other.numFailedMutations);
            this.numOfIndexCommitFailMutations.combine(other.numOfIndexCommitFailMutations);
            this.upsertMutationsSizeBytes.combine(other.upsertMutationsSizeBytes);
            this.deleteMutationsSizeBytes.combine(other.deleteMutationsSizeBytes);
            this.totalMutationsSizeBytes.combine(other.totalMutationsSizeBytes);
            this.upsertMutationSqlCounterSuccess.combine(other.upsertMutationSqlCounterSuccess);
            this.deleteMutationSqlCounterSuccess.combine(other.deleteMutationSqlCounterSuccess);
            this.upsertBatchFailedSize.combine(other.upsertBatchFailedSize);
            this.upsertBatchFailedCounter.combine(other.upsertBatchFailedCounter);
            this.deleteBatchFailedSize.combine(other.deleteBatchFailedSize);
            this.deleteBatchFailedCounter.combine(other.deleteBatchFailedCounter);
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
