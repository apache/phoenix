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

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.util.HashMap;
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
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.countRowsScannedCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.deleteMutationByesCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.deleteMutationSqlCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.mutationBatchSizeCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.mutationSqlCommitTimeCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.mutationSqlCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.queryFailedCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.queryTimeOutCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.scanBytesCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectPointLookUpFailedCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectPointLookUpSuccessCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectScanFailedCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectScanSuccessCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectSqlQueryTimeCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.taskEndToEndTimeCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.upsertMutationBytesCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.upsertMutationSqlCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.tableNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * This test does UT for TableClientMetrics class
 * This class has following API
 * 1. changeMetricValue
 * 2.getTableName
 * 3.getMetricMap
 */
public class TableClientMetricsTest {

    private static Map<String, TableClientMetrics> tableMetricsSet = new HashMap<>();

    public void verifyMetricsFromTableClientMetrics() {
        assertFalse(tableMetricsSet.isEmpty());
        for (int i = 0; i < tableNames.length; i++) {
            TableClientMetrics instance = tableMetricsSet.get(tableNames[i]);
            assertEquals(instance.getTableName(), tableNames[i]);
            List<PhoenixTableMetric> metricList = instance.getMetricMap();
            for (PhoenixTableMetric metric : metricList) {

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

    public boolean verifyTableName() {

        if (tableMetricsSet.isEmpty()) {
            return false;
        }
        for (String tableName : tableNames) {
            TableClientMetrics instance = tableMetricsSet.get(tableName);
            if (!instance.getTableName().equals(tableName)) {
                return false;
            }
        }
        return true;
    }


    /**
     * This test is for changeMetricValue() Method and getMetricMap()
     */
    @Test
    public void testTableClientMetrics() {
        Configuration conf = new Configuration();
        for (int i = 0; i < tableNames.length; i++) {
            TableClientMetrics tableClientMetrics = new TableClientMetrics(tableNames[i], conf);
            tableMetricsSet.put(tableNames[i], tableClientMetrics);

            tableClientMetrics.changeMetricValue(MUTATION_BATCH_SIZE,
                    mutationBatchSizeCounter[i]);
            tableClientMetrics.changeMetricValue(UPSERT_MUTATION_BYTES,
                    upsertMutationBytesCounter[i]);
            tableClientMetrics.changeMetricValue(UPSERT_MUTATION_SQL_COUNTER,
                    upsertMutationSqlCounter[i]);
            tableClientMetrics.changeMetricValue(DELETE_MUTATION_BYTES,
                    deleteMutationByesCounter[i]);
            tableClientMetrics.changeMetricValue(DELETE_MUTATION_SQL_COUNTER,
                    deleteMutationSqlCounter[i]);
            tableClientMetrics.changeMetricValue(MUTATION_SQL_COUNTER,
                    mutationSqlCounter[i]);
            tableClientMetrics.changeMetricValue(MUTATION_COMMIT_TIME,
                    mutationSqlCommitTimeCounter[i]);
            tableClientMetrics.changeMetricValue(TASK_END_TO_END_TIME,
                    taskEndToEndTimeCounter[i]);
            tableClientMetrics.changeMetricValue(COUNT_ROWS_SCANNED,
                    countRowsScannedCounter[i]);
            tableClientMetrics.changeMetricValue(QUERY_FAILED_COUNTER,
                    queryFailedCounter[i]);
            tableClientMetrics.changeMetricValue(QUERY_TIMEOUT_COUNTER,
                    queryTimeOutCounter[i]);
            tableClientMetrics.changeMetricValue(SCAN_BYTES, scanBytesCounter[i]);
            tableClientMetrics.changeMetricValue(MetricType.SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER,
                    selectPointLookUpSuccessCounter[i]);
            tableClientMetrics.changeMetricValue(SELECT_POINTLOOKUP_FAILED_SQL_COUNTER,
                    selectPointLookUpFailedCounter[i]);
            tableClientMetrics.changeMetricValue(SELECT_SQL_QUERY_TIME,
                    selectSqlQueryTimeCounter[i]);
            tableClientMetrics.changeMetricValue(SELECT_SCAN_SUCCESS_SQL_COUNTER,
                    selectScanSuccessCounter[i]);
            tableClientMetrics.changeMetricValue(SELECT_SCAN_FAILED_SQL_COUNTER,
                    selectScanFailedCounter[i]);
        }
        verifyMetricsFromTableClientMetrics();
        tableMetricsSet.clear();
    }

    /**
     * This test is for getTableName()
     */
    @Test
    public void testTableClientMetricsforTableName() {
        Configuration conf = new Configuration();
        for (int i = 0; i < tableNames.length; i++) {
            TableClientMetrics tableClientMetrics = new TableClientMetrics(tableNames[i], conf);
            tableMetricsSet.put(tableNames[i], tableClientMetrics);
        }
        assertTrue(verifyTableName());
    }

}
