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
import static org.apache.phoenix.monitoring.MetricType.MEMORY_CHUNK_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MEMORY_WAIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.NUM_PARALLEL_SCANS;
import static org.apache.phoenix.monitoring.MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.INDEX_COMMIT_FAILURE_SIZE;
import static org.apache.phoenix.monitoring.MetricType.QUERY_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_SERVICES_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
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
import static org.apache.phoenix.monitoring.MetricType.TASK_REJECTED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.CLIENT_METADATA_CACHE_HIT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.CLIENT_METADATA_CACHE_MISS_COUNTER;

import static org.apache.phoenix.monitoring.MetricType.COUNT_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_REMOTE_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_MILLS_BETWEEN_NEXTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_NOT_SERVING_REGION_EXCEPTION;
import static org.apache.phoenix.monitoring.MetricType.COUNT_BYTES_REGION_SERVER_RESULTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_BYTES_IN_REMOTE_RESULTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_SCANNED_REGIONS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_RPC_RETRIES;
import static org.apache.phoenix.monitoring.MetricType.COUNT_REMOTE_RPC_RETRIES;
import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_SCANNED;
import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_FILTERED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.phoenix.query.QueryServicesOptions;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central place where we keep track of all the global client phoenix metrics. These metrics are different from
 * {@link ReadMetricQueue} or {@link MutationMetricQueue} as they are collected at the client JVM level as opposed
 * to the above two which are collected for every phoenix request.
 */

public enum GlobalClientMetrics {
    GLOBAL_MUTATION_BATCH_SIZE(MUTATION_BATCH_SIZE),
    GLOBAL_MUTATION_BYTES(MUTATION_BYTES),
    GLOBAL_MUTATION_COMMIT_TIME(MUTATION_COMMIT_TIME),
    GLOBAL_MUTATION_BATCH_FAILED_COUNT(MUTATION_BATCH_FAILED_SIZE),
    GLOBAL_MUTATION_INDEX_COMMIT_FAILURE_COUNT(INDEX_COMMIT_FAILURE_SIZE),
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
    GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS(OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER),
    GLOBAL_QUERY_SERVICES_COUNTER(QUERY_SERVICES_COUNTER),
    GLOBAL_HCONNECTIONS_COUNTER(HCONNECTIONS_COUNTER),
    GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER(PHOENIX_CONNECTIONS_THROTTLED_COUNTER),
    GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER(PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER),

    GLOBAL_HBASE_COUNT_RPC_CALLS(COUNT_RPC_CALLS),
    GLOBAL_HBASE_COUNT_REMOTE_RPC_CALLS(COUNT_REMOTE_RPC_CALLS),
    GLOBAL_HBASE_COUNT_MILLS_BETWEEN_NEXTS(COUNT_MILLS_BETWEEN_NEXTS),
    GLOBAL_HBASE_COUNT_NOT_SERVING_REGION_EXCEPTION(COUNT_NOT_SERVING_REGION_EXCEPTION),
    GLOBAL_HBASE_COUNT_BYTES_REGION_SERVER_RESULTS(COUNT_BYTES_REGION_SERVER_RESULTS),
    GLOBAL_HBASE_COUNT_BYTES_IN_REMOTE_RESULTS(COUNT_BYTES_IN_REMOTE_RESULTS),
    GLOBAL_HBASE_COUNT_SCANNED_REGIONS(COUNT_SCANNED_REGIONS),
    GLOBAL_HBASE_COUNT_RPC_RETRIES(COUNT_RPC_RETRIES),
    GLOBAL_HBASE_COUNT_REMOTE_RPC_RETRIES(COUNT_REMOTE_RPC_RETRIES),
    GLOBAL_HBASE_COUNT_ROWS_SCANNED(COUNT_ROWS_SCANNED),
    GLOBAL_HBASE_COUNT_ROWS_FILTERED(COUNT_ROWS_FILTERED),
    GLOBAL_CLIENT_METADATA_CACHE_MISS_COUNTER(CLIENT_METADATA_CACHE_MISS_COUNTER),
    GLOBAL_CLIENT_METADATA_CACHE_HIT_COUNTER(CLIENT_METADATA_CACHE_HIT_COUNTER);


    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalClientMetrics.class);
    private static final boolean isGlobalMetricsEnabled = QueryServicesOptions.withDefaults().isGlobalMetricsEnabled();
    private MetricType metricType;
    private GlobalMetric metric;

    static {
        initPhoenixGlobalClientMetrics();
        if (isGlobalMetricsEnabled) {
            MetricRegistry metricRegistry = createMetricRegistry();
            registerPhoenixMetricsToRegistry(metricRegistry);
            GlobalMetricRegistriesAdapter.getInstance().registerMetricRegistry(metricRegistry);
        }
    }

    private static void initPhoenixGlobalClientMetrics() {
        for (GlobalClientMetrics globalMetric : GlobalClientMetrics.values()) {
            globalMetric.metric = isGlobalMetricsEnabled ?
                    new GlobalMetricImpl(globalMetric.metricType) : new NoOpGlobalMetricImpl();
        }
    }

    private static void registerPhoenixMetricsToRegistry(MetricRegistry metricRegistry) {
        for (GlobalClientMetrics globalMetric : GlobalClientMetrics.values()) {
            metricRegistry.register(globalMetric.metricType.columnName(),
                    new PhoenixGlobalMetricGauge(globalMetric.metric));
        }
    }

    private static MetricRegistry createMetricRegistry() {
        LOGGER.info("Creating Metric Registry for Phoenix Global Metrics");
        MetricRegistryInfo registryInfo = new MetricRegistryInfo("PHOENIX", "Phoenix Client Metrics",
                "phoenix", "Phoenix,sub=CLIENT", true);
        return MetricRegistries.global().create(registryInfo);
    }

    /**
     * Class to convert Phoenix Metric objects into HBase Metric objects (Gauge)
     */
    private static class PhoenixGlobalMetricGauge implements Gauge<Long> {

        private final GlobalMetric metric;

        public PhoenixGlobalMetricGauge(GlobalMetric metric) {
            this.metric = metric;
        }

        @Override
        public Long getValue() {
            return metric.getValue();
        }
    }

    public void update(long value) {
        metric.change(value);
    }

    @VisibleForTesting
    public GlobalMetric getMetric() {
        return metric;
    }

    @VisibleForTesting
    public MetricType getMetricType() {
        return metricType;
    }


    @Override
    public String toString() {
        return metric.toString();
    }

    private GlobalClientMetrics(MetricType metricType) {
        this.metricType = metricType;
    }

    public void increment() {
        metric.increment();
    }
    
    public void decrement() {
        metric.decrement();
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
