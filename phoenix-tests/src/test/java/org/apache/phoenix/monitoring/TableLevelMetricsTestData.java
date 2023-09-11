/**
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_SCANNED;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_FAILURES;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_SUCCESS;
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class is used primarily to populate data and
 * verification methods
 */

public class TableLevelMetricsTestData {

    public static final String[] tableNames = { "T0001", "T0002", "T0003" };
    public static Map<String, Map<MetricType, Long>>[] tableMetricsMap = new Map[tableNames.length];
    public static final long[] mutationBatchSizeCounter = { 90, 100, 150 };
    public static final long[] upsertMutationBytesCounter = { 100, 200, 250 };
    public static final long[] upsertMutationSqlCounter = { 100, 200, 300 };
    public static final long[] deleteMutationByesCounter = { 100, 200, 150 };
    public static final long[] deleteMutationSqlCounter = { 100, 200, 140 };
    public static final long[] mutationSqlCounter = { 200, 400, 600 };
    public static final long[] mutationSqlCommitTimeCounter = { 150, 300, 100 };
    public static final long[] taskEndToEndTimeCounter = { 10, 20, 30 };
    public static final long[] countRowsScannedCounter = { 500, 600, 400 };
    public static final long[] queryFailedCounter = { 10, 20, 30 };
    public static final long[] queryTimeOutCounter = { 30, 40, 40 };
    public static final long[] scanBytesCounter = { 500, 600, 400 };
    public static final long[] selectPointLookUpFailedCounter = { 10, 29, 49 };
    public static final long[] selectSqlQueryTimeCounter = { 30, 40, 55 };
    public static final long[] selectPointLookUpSuccessCounter = { 10, 20, 55 };
    public static final long[] selectScanSuccessCounter = { 200000, 300000, 4444 };
    public static final long[] selectScanFailedCounter = { 1000000, 20000000, 3455 };
    public static final long[] numRpcSuccessCallsSystemCatalog = {200, 100, 300};
    public static final long[] numRpcFailureCallsSystemCatalog = {100, 200, 300};
    public static final long[] timeTakenForRpcCallsSystemCatalog = {500, 600, 370};

    public static void populateMetrics() {
        for (int i = 0; i < tableMetricsMap.length; i++) {
            tableMetricsMap[i] = new HashMap<>();
        }
        for (int i = 0; i < tableNames.length; i++) {
            Map<MetricType, Long> metrics = new HashMap<>();
            metrics.put(MUTATION_BATCH_SIZE, mutationBatchSizeCounter[i]);
            metrics.put(UPSERT_MUTATION_BYTES, upsertMutationBytesCounter[i]);
            metrics.put(UPSERT_MUTATION_SQL_COUNTER, upsertMutationSqlCounter[i]);
            metrics.put(DELETE_MUTATION_BYTES, deleteMutationByesCounter[i]);
            metrics.put(DELETE_MUTATION_SQL_COUNTER, deleteMutationSqlCounter[i]);
            metrics.put(MUTATION_SQL_COUNTER, mutationSqlCounter[i]);
            metrics.put(MUTATION_COMMIT_TIME, mutationSqlCommitTimeCounter[i]);
            metrics.put(TASK_END_TO_END_TIME, taskEndToEndTimeCounter[i]);
            metrics.put(COUNT_ROWS_SCANNED, countRowsScannedCounter[i]);
            metrics.put(QUERY_FAILED_COUNTER, queryFailedCounter[i]);
            metrics.put(QUERY_TIMEOUT_COUNTER, queryTimeOutCounter[i]);
            metrics.put(SCAN_BYTES, scanBytesCounter[i]);
            metrics.put(MetricType.SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER,
                    selectPointLookUpSuccessCounter[i]);
            metrics.put(SELECT_POINTLOOKUP_FAILED_SQL_COUNTER,
                    selectPointLookUpFailedCounter[i]);
            metrics.put(MetricType.SELECT_SQL_QUERY_TIME, selectSqlQueryTimeCounter[i]);
            metrics.put(SELECT_SCAN_SUCCESS_SQL_COUNTER, selectScanSuccessCounter[i]);
            metrics.put(MetricType.SELECT_SCAN_FAILED_SQL_COUNTER, selectScanFailedCounter[i]);
            tableMetricsMap[i].put(tableNames[i], metrics);
            metrics.put(NUM_SYSTEM_TABLE_RPC_SUCCESS, numRpcSuccessCallsSystemCatalog[i]);
            metrics.put(NUM_SYSTEM_TABLE_RPC_FAILURES, numRpcFailureCallsSystemCatalog[i]);
            metrics.put(TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS, timeTakenForRpcCallsSystemCatalog[i]);
        }
    }

    public void verifyMetricsInjection(int noOfTables) {
        Map<String, List<PhoenixTableMetric>> map = TableMetricsManager.getTableMetricsMethod();
        assertFalse(map == null || map.isEmpty());
        for (int i = 0; i < noOfTables; i++) {
            System.out.println("CURRENTLY ON: " + tableNames[i]);
            assertTrue(map.containsKey(tableNames[i]));
            List<PhoenixTableMetric> tableMetric = map.get(tableNames[i]);
            for (PhoenixTableMetric metric : tableMetric) {
                if (metric.getMetricType().equals(MUTATION_BATCH_SIZE)) {
                    assertEquals(mutationBatchSizeCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(UPSERT_MUTATION_BYTES)) {
                    assertEquals(upsertMutationBytesCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(UPSERT_MUTATION_SQL_COUNTER)) {
                    assertEquals(upsertMutationSqlCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(DELETE_MUTATION_BYTES)) {
                    assertEquals(deleteMutationByesCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(DELETE_MUTATION_SQL_COUNTER)) {
                    assertEquals(deleteMutationSqlCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(MUTATION_SQL_COUNTER)) {
                    assertEquals(mutationSqlCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(MUTATION_COMMIT_TIME)) {
                    assertEquals(mutationSqlCommitTimeCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(TASK_END_TO_END_TIME)) {
                    assertEquals(taskEndToEndTimeCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(COUNT_ROWS_SCANNED)) {
                    assertEquals(countRowsScannedCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(QUERY_FAILED_COUNTER)) {
                    assertEquals(queryFailedCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(QUERY_TIMEOUT_COUNTER)) {
                    assertEquals(queryTimeOutCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(SCAN_BYTES)) {
                    assertEquals(scanBytesCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER)) {
                    assertEquals(selectPointLookUpSuccessCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(SELECT_POINTLOOKUP_FAILED_SQL_COUNTER)) {
                    assertEquals(selectPointLookUpFailedCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(SELECT_SQL_QUERY_TIME)) {
                    assertEquals(selectSqlQueryTimeCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(SELECT_SCAN_FAILED_SQL_COUNTER)) {
                    assertEquals(selectScanFailedCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(SELECT_SCAN_SUCCESS_SQL_COUNTER)) {
                    assertEquals(selectScanSuccessCounter[i], metric.getValue());
                }
            }
        }
    }

}
