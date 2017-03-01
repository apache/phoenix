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

import static org.apache.phoenix.monitoring.MetricType.HCONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.MEMORY_CHUNK_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MEMORY_WAIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.NUM_PARALLEL_SCANS;
import static org.apache.phoenix.monitoring.MetricType.QUERY_SERVICES_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TASK_REJECTED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SPOOL_FILE_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SPOOL_FILE_SIZE;
import static org.apache.phoenix.monitoring.MetricType.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.phoenix.query.QueryServicesOptions;

import com.google.common.annotations.VisibleForTesting;

/**
 * Central place where we keep track of all the global client phoenix metrics. These metrics are different from
 * {@link ReadMetricQueue} or {@link MutationMetricQueue} as they are collected at the client JVM level as opposed
 * to the above two which are collected for every phoenix request.
 */

public enum GlobalClientMetrics {
    
    GLOBAL_MUTATION_BATCH_SIZE(MUTATION_BATCH_SIZE),
    GLOBAL_MUTATION_BYTES(MUTATION_BYTES),
    GLOBAL_MUTATION_COMMIT_TIME(MUTATION_COMMIT_TIME),
    GLOBAL_QUERY_TIME(QUERY_TIME),
    GLOBAL_NUM_PARALLEL_SCANS(NUM_PARALLEL_SCANS),
    GLOBAL_SCAN_BYTES(SCAN_BYTES),
    GLOBAL_SPOOL_FILE_SIZE(SPOOL_FILE_SIZE),
    GLOBAL_MEMORY_CHUNK_BYTES(MEMORY_CHUNK_BYTES),
    GLOBAL_MEMORY_WAIT_TIME(MEMORY_WAIT_TIME),
    GLOBAL_TASK_QUEUE_WAIT_TIME(TASK_QUEUE_WAIT_TIME),
    GLOBAL_TASK_END_TO_END_TIME(TASK_END_TO_END_TIME),
    GLOBAL_TASK_EXECUTION_TIME(TASK_EXECUTION_TIME),
    GLOBAL_MUTATION_SQL_COUNTER(MUTATION_SQL_COUNTER),
    GLOBAL_SELECT_SQL_COUNTER(SELECT_SQL_COUNTER),
    GLOBAL_TASK_EXECUTED_COUNTER(TASK_EXECUTED_COUNTER),
    GLOBAL_REJECTED_TASK_COUNTER(TASK_REJECTED_COUNTER),
    GLOBAL_QUERY_TIMEOUT_COUNTER(QUERY_TIMEOUT_COUNTER),
    GLOBAL_FAILED_QUERY_COUNTER(QUERY_FAILED_COUNTER),
    GLOBAL_SPOOL_FILE_COUNTER(SPOOL_FILE_COUNTER),
    GLOBAL_OPEN_PHOENIX_CONNECTIONS(OPEN_PHOENIX_CONNECTIONS_COUNTER),
    GLOBAL_QUERY_SERVICES_COUNTER(QUERY_SERVICES_COUNTER),
    GLOBAL_HCONNECTIONS_COUNTER(HCONNECTIONS_COUNTER),
    GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER(PHOENIX_CONNECTIONS_THROTTLED_COUNTER),
    GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER(PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER);

    
    private static final boolean isGlobalMetricsEnabled = QueryServicesOptions.withDefaults().isGlobalMetricsEnabled();
    private GlobalMetric metric;

    public void update(long value) {
        if (isGlobalMetricsEnabled) {
            metric.change(value);
        }
    }

    @VisibleForTesting
    public GlobalMetric getMetric() {
        return metric;
    }

    @Override
    public String toString() {
        return metric.toString();
    }

    private GlobalClientMetrics(MetricType metricType) {
        this.metric = new GlobalMetricImpl(metricType);
    }

    public void increment() {
        if (isGlobalMetricsEnabled) {
            metric.increment();
        }
    }
    
    public void decrement() {
        if (isGlobalMetricsEnabled) {
            metric.decrement();
        }
    }

    public static Collection<GlobalMetric> getMetrics() {
        List<GlobalMetric> metrics = new ArrayList<>();
        for (GlobalClientMetrics m : GlobalClientMetrics.values()) {
            metrics.add(m.metric);
        }
        return metrics;
    }

    public static boolean isMetricsEnabled() {
        return isGlobalMetricsEnabled;
    }

}
