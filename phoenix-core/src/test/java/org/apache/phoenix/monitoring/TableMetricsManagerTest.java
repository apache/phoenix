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

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.tableNames;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.tableMetricsMap;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.populateMetrics;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.taskEndToEndTimeCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.upsertMutationBytesCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.upsertMutationSqlCounter;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This method is for UT TableMetricManager class.
 * This class exposes 4 static functions
 * 1.pushMetricsFromConnInstanceMethod
 * 2.getTableMetricsMethod
 * 3.clearTableLevelMetricsMethod
 * 4.updateMetricsMethod
 */
public class TableMetricsManagerTest {

    public boolean verifyMetricsReset(){
        Map<String, List<PhoenixTableMetric>>map = TableMetricsManager.getTableMetricsMethod();
        return map != null && map.isEmpty();
    }

    private static class PushMetrics implements Runnable {
        private final Map<String, Map<MetricType, Long>> map;

        public PushMetrics(Map<String, Map<MetricType, Long>> map) {
            this.map = map;
        }

        @Override public void run() {
            TableMetricsManager.pushMetricsFromConnInstanceMethod(map);
        }
    }

    public boolean verifyTableNamesExists(String tableName){
        Map<String,List<PhoenixTableMetric>>map = TableMetricsManager.getTableMetricsMethod();
        return map != null && map.containsKey(tableName);
    }

    /**
     * Injecting data parallely to TableMetricsManager using pushMetricsFromConnInstanceMethod()
     */
    @Test
    public void testVerifyTableLevelMetricsMutilpleThreads() throws Exception {
        QueryServicesOptions options = QueryServicesOptions.withDefaults();
        options.setTableLevelMetricsEnabled();
        String tableNamesList = tableNames[0] + "," + tableNames[1] + "," + tableNames[2];
        options.setAllowedListForTableLevelMetrics(tableNamesList);
        TableMetricsManager tableMetricsManager = new TableMetricsManager(options);
        TableMetricsManager.setInstance(tableMetricsManager);
        TableLevelMetricsTestData testData = new TableLevelMetricsTestData();
        populateMetrics();

        ExecutorService executorService =
                Executors.newFixedThreadPool(tableNames.length, new ThreadFactory() {
                    @Override public Thread newThread(Runnable r) {
                        Thread t = Executors.defaultThreadFactory().newThread(r);
                        t.setDaemon(true);
                        t.setPriority(Thread.MIN_PRIORITY);
                        return t;
                    }
                });
        List<Future<?>> futureList = Lists.newArrayListWithExpectedSize(tableNames.length);
        for (int i = 0; i < tableNames.length; ++i) {
            futureList.add(executorService.submit(new PushMetrics(tableMetricsMap[i])));
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        testData.verifyMetricsInjection(tableNames.length);
        TableMetricsManager.clearTableLevelMetricsMethod();
        assertTrue(verifyMetricsReset());
    }

    /**
     * test for pushMetricsFromConnInstanceMethod() , getTableMetricsMethod()
     * and clearTableLevelMetrics();
     */
    @Test
    public void testTableMetricsForPushMetricsFromConnInstanceMethod() {
        QueryServicesOptions options = QueryServicesOptions.withDefaults();
        options.setTableLevelMetricsEnabled();
        String tableNamesList = tableNames[0] + "," + tableNames[1] + "," + tableNames[2];
        options.setAllowedListForTableLevelMetrics(tableNamesList);
        TableMetricsManager tableMetricsManager = new TableMetricsManager(options);
        TableMetricsManager.setInstance(tableMetricsManager);

        TableLevelMetricsTestData testData = new TableLevelMetricsTestData();
        populateMetrics();
        for(int i = 0; i < tableNames.length ; i++){
            TableMetricsManager.pushMetricsFromConnInstanceMethod(tableMetricsMap[i]);
        }
        testData.verifyMetricsInjection(3);
        TableMetricsManager.clearTableLevelMetricsMethod();
        assertTrue(verifyMetricsReset());
    }

    /**
     * test for updateMetricsMethod() , getTableMetricsMethod()
     * and clearTableLevelMetrics();
     */
    @Test
    public void testTableMetricsForUpdateMetricsMethod() {

        QueryServicesOptions options = QueryServicesOptions.withDefaults();
        options.setTableLevelMetricsEnabled();
        TableMetricsManager tableMetricsManager = new TableMetricsManager(options);
        TableMetricsManager.setInstance(tableMetricsManager);

        TableLevelMetricsTestData testData = new TableLevelMetricsTestData();
        for(int i = 0; i < tableNames.length; i++) {
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.MUTATION_BATCH_SIZE, mutationBatchSizeCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.UPSERT_MUTATION_SQL_COUNTER, upsertMutationSqlCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.UPSERT_MUTATION_BYTES, upsertMutationBytesCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.DELETE_MUTATION_SQL_COUNTER, deleteMutationSqlCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.DELETE_MUTATION_BYTES, deleteMutationByesCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.MUTATION_SQL_COUNTER, mutationSqlCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.MUTATION_COMMIT_TIME, mutationSqlCommitTimeCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.TASK_END_TO_END_TIME, taskEndToEndTimeCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.COUNT_ROWS_SCANNED, countRowsScannedCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.QUERY_FAILED_COUNTER, queryFailedCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.QUERY_TIMEOUT_COUNTER, queryTimeOutCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.SCAN_BYTES, scanBytesCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER, selectPointLookUpSuccessCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.SELECT_POINTLOOKUP_FAILED_SQL_COUNTER, selectPointLookUpFailedCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.SELECT_SQL_QUERY_TIME, selectSqlQueryTimeCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.SELECT_SCAN_FAILED_SQL_COUNTER, selectScanFailedCounter[i]);
            TableMetricsManager.updateMetricsMethod(tableNames[i],MetricType.SELECT_SCAN_SUCCESS_SQL_COUNTER, selectScanSuccessCounter[i]);

        }
        testData.verifyMetricsInjection(3);
        TableMetricsManager.clearTableLevelMetricsMethod();
        assertTrue(verifyMetricsReset());
    }



    //This test puts T0001, T0002 in allowed list and verifies the existence of metrics
    //and blocking the T0003.
    @Test
    public void testTableMetricsForPushMetricsFromConnInstanceMethodWithAllowedTables() {
        QueryServicesOptions options = QueryServicesOptions.withDefaults();
        options.setTableLevelMetricsEnabled();
        String tableNamesList = tableNames[0] + "," + tableNames[1];
        options.setAllowedListForTableLevelMetrics(tableNamesList);

        TableMetricsManager tableMetricsManager = new TableMetricsManager(options);
        TableMetricsManager.setInstance(tableMetricsManager);

        TableLevelMetricsTestData testData = new TableLevelMetricsTestData();
        populateMetrics();
        for(int i = 0; i < tableNames.length ; i++){
            TableMetricsManager.pushMetricsFromConnInstanceMethod(tableMetricsMap[i]);
        }

        testData.verifyMetricsInjection(2);
        assertFalse(verifyTableNamesExists(tableNames[2]));
    }

}