/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.monitoring;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.NUM_METADATA_LOOKUP_FAILURES;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_FAILURES;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_SUCCESS;
import static org.apache.phoenix.monitoring.MetricType.QUERY_POINTLOOKUP_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_POINTLOOKUP_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_SCAN_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_SCAN_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.RESULT_SET_TIME_MS;
import static org.apache.phoenix.monitoring.MetricType.SELECT_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.DELETE_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_SCANNED;
import static org.apache.phoenix.monitoring.MetricType.QUERY_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.DELETE_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.DELETE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_BATCH_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_AGGREGATE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_AGGREGATE_FAILURE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_AGGREGATE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_AGGREGATE_FAILURE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_AGGREGATE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_AGGREGATE_FAILURE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_SUCCESS;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_FAILURES;
import static org.apache.phoenix.monitoring.MetricType.TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.ATOMIC_UPSERT_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.ATOMIC_UPSERT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.ATOMIC_UPSERT_SQL_QUERY_TIME;

/**
 * This is used by TableMetricsManager class to store instance of
 * object associated with a tableName.
 */
public class TableClientMetrics {

    public enum TableMetrics {
        TABLE_MUTATION_BATCH_FAILED_SIZE(MUTATION_BATCH_FAILED_SIZE), TABLE_MUTATION_BATCH_SIZE(
                MUTATION_BATCH_SIZE), TABLE_MUTATION_BYTES(
                MUTATION_BYTES), TABLE_UPSERT_MUTATION_BYTES(
                UPSERT_MUTATION_BYTES), TABLE_UPSERT_MUTATION_SQL_COUNTER(
                UPSERT_MUTATION_SQL_COUNTER), TABLE_DELETE_MUTATION_BYTES(
                DELETE_MUTATION_BYTES), TABLE_DELETE_MUTATION_SQL_COUNTER(
                DELETE_MUTATION_SQL_COUNTER), TABLE_MUTATION_SQL_COUNTER(
                MUTATION_SQL_COUNTER), TABLE_MUTATION_COMMIT_TIME(
                MUTATION_COMMIT_TIME), TABLE_UPSERT_SQL_COUNTER(
                UPSERT_SQL_COUNTER), TABLE_UPSERT_SQL_QUERY_TIME(
                UPSERT_SQL_QUERY_TIME), TABLE_SUCCESS_UPSERT_SQL_COUNTER(
                UPSERT_SUCCESS_SQL_COUNTER), TABLE_FAILED_UPSERT_SQL_COUNTER(
                UPSERT_FAILED_SQL_COUNTER), TABLE_UPSERT_BATCH_FAILED_SIZE(
                UPSERT_BATCH_FAILED_SIZE), TABLE_UPSERT_BATCH_FAILED_COUNTER(
                UPSERT_BATCH_FAILED_COUNTER), TABLE_DELETE_SQL_COUNTER(
                DELETE_SQL_COUNTER), TABLE_DELETE_SQL_QUERY_TIME(
                DELETE_SQL_QUERY_TIME), TABLE_SUCCESS_DELETE_SQL_COUNTER(
                DELETE_SUCCESS_SQL_COUNTER), TABLE_FAILED_DELETE_SQL_COUNTER(
                DELETE_FAILED_SQL_COUNTER), TABLE_DELETE_BATCH_FAILED_SIZE(
                DELETE_BATCH_FAILED_SIZE), TABLE_DELETE_BATCH_FAILED_COUNTER(
                DELETE_BATCH_FAILED_COUNTER), TABLE_UPSERT_COMMIT_TIME(
                UPSERT_COMMIT_TIME), TABLE_DELETE_COMMIT_TIME(
                DELETE_COMMIT_TIME), TABLE_TASK_END_TO_END_TIME(
                TASK_END_TO_END_TIME), TABLE_COUNT_ROWS_SCANNED(
                COUNT_ROWS_SCANNED), TABLE_QUERY_FAILED_COUNTER(
                QUERY_FAILED_COUNTER), TABLE_QUERY_POINTLOOKUP_FAILED_COUNTER(
                QUERY_POINTLOOKUP_FAILED_COUNTER), TABLE_QUERY_SCAN_FAILED_COUNTER(
                QUERY_SCAN_FAILED_COUNTER), TABLE_QUERY_TIMEOUT_COUNTER(
                QUERY_TIMEOUT_COUNTER), TABLE_QUERY_POINTLOOKUP_TIMEOUT_COUNTER(
                QUERY_POINTLOOKUP_TIMEOUT_COUNTER), TABLE_QUERY_SCAN_TIMEOUT_COUNTER(
                QUERY_SCAN_TIMEOUT_COUNTER), TABLE_SELECT_QUERY_RESULT_SET_MS(
                RESULT_SET_TIME_MS), TABLE_SCANBYTES(SCAN_BYTES), TABLE_SELECT_SQL_COUNTER(
                SELECT_SQL_COUNTER), TABLE_SELECT_SQL_QUERY_TIME(
                SELECT_SQL_QUERY_TIME), TABLE_SUCCESS_SELECT_SQL_COUNTER(
                SELECT_SUCCESS_SQL_COUNTER), TABLE_FAILED_SELECT_SQL_COUNTER(
                SELECT_FAILED_SQL_COUNTER), TABLE_SELECT_POINTLOOKUP_COUNTER_SUCCESS(
                SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER), TABLE_SELECT_POINTLOOKUP_COUNTER_FAILED(
                SELECT_POINTLOOKUP_FAILED_SQL_COUNTER), TABLE_SELECT_SCAN_COUNTER_SUCCESS(
                SELECT_SCAN_SUCCESS_SQL_COUNTER), TABLE_SELECT_SCAN_COUNTER_FAILED(
                SELECT_SCAN_FAILED_SQL_COUNTER), TABLE_UPSERT_AGGREGATE_SUCCESS_SQL_COUNTER(
                UPSERT_AGGREGATE_SUCCESS_SQL_COUNTER), TABLE_UPSERT_AGGREGATE_FAILURE_SQL_COUNTER(
                UPSERT_AGGREGATE_FAILURE_SQL_COUNTER), TABLE_DELETE_AGGREGATE_SUCCESS_SQL_COUNTER(
                DELETE_AGGREGATE_SUCCESS_SQL_COUNTER), TABLE_DELETE_AGGREGATE_FAILURE_SQL_COUNTER(
                DELETE_AGGREGATE_FAILURE_SQL_COUNTER), TABLE_SELECT_AGGREGATE_SUCCESS_SQL_COUNTER(
                SELECT_AGGREGATE_SUCCESS_SQL_COUNTER), TABLE_SELECT_AGGREGATE_FAILURE_SQL_COUNTER(
                SELECT_AGGREGATE_FAILURE_SQL_COUNTER),
                TABLE_ATOMIC_UPSERT_SQL_COUNTER(ATOMIC_UPSERT_SQL_COUNTER),
                TABLE_ATOMIC_UPSERT_COMMIT_TIME(ATOMIC_UPSERT_COMMIT_TIME),
                TABLE_ATOMIC_UPSERT_SQL_QUERY_TIME(ATOMIC_UPSERT_SQL_QUERY_TIME),
                TABLE_NUM_SYSTEM_TABLE_RPC_SUCCESS(NUM_SYSTEM_TABLE_RPC_SUCCESS),
                TABLE_NUM_SYSTEM_TABLE_RPC_FAILURES(NUM_SYSTEM_TABLE_RPC_FAILURES),
                TABLE_NUM_METADATA_LOOKUP_FAILURES(NUM_METADATA_LOOKUP_FAILURES),
                TABLE_TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS(TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS);

        private MetricType metricType;
        private PhoenixTableMetric metric;

        TableMetrics(MetricType metricType) {
            this.metricType = metricType;
        }
    }

    private final String tableName;
    private Map<MetricType, PhoenixTableMetric> metricRegister;
    private TableHistograms tableHistograms;

    public TableClientMetrics(final String tableName, Configuration conf) {
        this.tableName = tableName;
        metricRegister = new HashMap<>();
        for (TableMetrics tableMetric : TableMetrics.values()) {
            tableMetric.metric = new PhoenixTableMetricImpl(tableMetric.metricType);
            metricRegister.put(tableMetric.metricType, tableMetric.metric);
        }
        tableHistograms = new TableHistograms(tableName, conf);
    }

    /**
     * This function is used to update the value of Metric
     * Incase of counter val will passed as 1.
     *
     * @param type metric type
     * @param val update value. In case of counters, this will be 1
     */
    public void changeMetricValue(MetricType type, long val) {
        if (!metricRegister.containsKey(type)) {
            return;
        }
        PhoenixTableMetric metric = metricRegister.get(type);
        metric.change(val);
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * This method is called to aggregate all the Metrics across all Tables in Phoenix.
     *
     * @return map of table name -> list of TableMetric.
     */
    public List<PhoenixTableMetric> getMetricMap() {
        List<PhoenixTableMetric> metricsList = new ArrayList<>();
        for (PhoenixTableMetric value : metricRegister.values()) {
            metricsList.add(value);
        }
        return metricsList;
    }

    public Map<MetricType, PhoenixTableMetric> getMetricRegistry() {
        return metricRegister;
    }

    public TableHistograms getTableHistograms() {
        return tableHistograms;
    }

}