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

import org.apache.hadoop.conf.Configuration;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

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
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.numRpcFailureCallsSystemCatalog;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.numRpcSuccessCallsSystemCatalog;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.scanBytesCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectPointLookUpFailedCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectPointLookUpSuccessCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectScanFailedCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectScanSuccessCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.selectSqlQueryTimeCounter;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.tableNames;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.tableMetricsMap;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.populateMetrics;
import static org.apache.phoenix.monitoring.TableLevelMetricsTestData.timeTakenForRpcCallsSystemCatalog;
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
            TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableNames[i],
                    MetricType.NUM_SYSTEM_TABLE_RPC_SUCCESS, numRpcSuccessCallsSystemCatalog[i]);
            TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableNames[i],
                    MetricType.NUM_SYSTEM_TABLE_RPC_FAILURES, numRpcFailureCallsSystemCatalog[i]);
            TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableNames[i],
                    MetricType.TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS, timeTakenForRpcCallsSystemCatalog[i]);
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

    /*
      Tests histogram metrics for upsert mutations.
   */
    @Test
    public void testHistogramMetricsForUpsertMutations() {
        String tableName = "TEST-TABLE";
        Configuration conf = new Configuration();
        conf.set(QueryServices.PHOENIX_HISTOGRAM_LATENCY_RANGES, "2,5,8");
        conf.set(QueryServices.PHOENIX_HISTOGRAM_SIZE_RANGES, "10, 100, 1000");

        QueryServicesOptions mockOptions = Mockito.mock(QueryServicesOptions.class);
        Mockito.doReturn(true).when(mockOptions).isTableLevelMetricsEnabled();
        Mockito.doReturn(tableName).when(mockOptions).getAllowedListTableNames();
        Mockito.doReturn(conf).when(mockOptions).getConfiguration();
        TableMetricsManager tableMetricsManager = new TableMetricsManager(mockOptions);
        TableMetricsManager.setInstance(tableMetricsManager);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 1, true);
        MutationMetricQueue.MutationMetric metric = new MutationMetricQueue.MutationMetric(
                0L, 5L, 0L, 0L, 0L,0L,
                0L, 1L, 0L, 5L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), true);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 2, true);
        metric = new MutationMetricQueue.MutationMetric(0L, 10L, 0L, 0L, 0L,0L,
                0L, 1L, 0L, 10L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), true);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 4, true);
        metric = new MutationMetricQueue.MutationMetric(0L, 50L, 0L, 0L, 0L,0L,
                0L, 1L, 0L, 50L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), true);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 5, true);
        metric = new MutationMetricQueue.MutationMetric(0L, 100L, 0L, 0L, 0L,0L,
                0L, 1L, 0L, 100L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), true);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 6, true);
        metric = new MutationMetricQueue.MutationMetric(0L, 500L, 0L, 0L, 0L,0L,
                0L, 1L, 0L, 500L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), true);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 8, true);
        metric = new MutationMetricQueue.MutationMetric(0L, 1000L, 0L, 0L, 0L,0L,
                0L, 1L, 0L, 1000L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), true);


        // Generate distribution map from histogram snapshots.
        LatencyHistogram latencyHistogram =
                TableMetricsManager.getUpsertLatencyHistogramForTable(tableName);
        SizeHistogram sizeHistogram = TableMetricsManager.getUpsertSizeHistogramForTable(tableName);

        Map<String, Long> latencyMap = latencyHistogram.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> sizeMap = sizeHistogram.getRangeHistogramDistribution().getRangeDistributionMap();
        for (Long count: latencyMap.values()) {
            Assert.assertEquals(new Long(2), count);
        }
        for (Long count: sizeMap.values()) {
            Assert.assertEquals(new Long(2), count);
        }
    }

    /*
        Tests histogram metrics for delete mutations.
     */
    @Test
    public void testHistogramMetricsForDeleteMutations() {
        String tableName = "TEST-TABLE";
        Configuration conf = new Configuration();
        conf.set(QueryServices.PHOENIX_HISTOGRAM_LATENCY_RANGES, "2,5,8");
        conf.set(QueryServices.PHOENIX_HISTOGRAM_SIZE_RANGES, "10, 100, 1000");

        QueryServicesOptions mockOptions = Mockito.mock(QueryServicesOptions.class);
        Mockito.doReturn(true).when(mockOptions).isTableLevelMetricsEnabled();
        Mockito.doReturn(tableName).when(mockOptions).getAllowedListTableNames();
        Mockito.doReturn(conf).when(mockOptions).getConfiguration();
        TableMetricsManager tableMetricsManager = new TableMetricsManager(mockOptions);
        TableMetricsManager.setInstance(tableMetricsManager);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 1, false);
        MutationMetricQueue.MutationMetric metric = new MutationMetricQueue.MutationMetric(
                0L, 0L, 5L, 0L, 0L, 0L,
                0L, 0L, 1L, 5L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), false);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 2, false);
        metric = new MutationMetricQueue.MutationMetric(0L, 0L, 10L, 0L, 0L, 0L,
                0L, 0L, 1L, 10L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), false);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 4, false);
        metric = new MutationMetricQueue.MutationMetric(0L, 0L, 50L, 0L, 0L, 0L,
                0L, 0L, 1L, 50L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), false);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 5,false);
        metric = new MutationMetricQueue.MutationMetric(0L, 0L, 100L, 0L, 0L, 0L,
                0L, 0L, 1L, 100L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), false);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 6,false);
        metric = new MutationMetricQueue.MutationMetric(0L, 0L, 500L, 0L, 0L, 0L,
                0L, 0L, 1L, 500L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), false);

        TableMetricsManager.updateLatencyHistogramForMutations(tableName, 8, false);
        metric = new MutationMetricQueue.MutationMetric(0L, 0L, 1000L, 0L, 0L, 0L,
                0L, 0L, 1L, 1000L, 0L, 0L, 0L, 0L, 0L);
        TableMetricsManager.updateSizeHistogramMetricsForMutations(tableName, metric.getTotalMutationsSizeBytes().getValue(), false);


        // Generate distribution map from histogram snapshots.
        LatencyHistogram latencyHistogram =
                TableMetricsManager.getDeleteLatencyHistogramForTable(tableName);
        SizeHistogram sizeHistogram = TableMetricsManager.getDeleteSizeHistogramForTable(tableName);

        Map<String, Long> latencyMap = latencyHistogram.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> sizeMap = sizeHistogram.getRangeHistogramDistribution().getRangeDistributionMap();
        for (Long count: latencyMap.values()) {
            Assert.assertEquals(new Long(2), count);
        }
        for (Long count: sizeMap.values()) {
            Assert.assertEquals(new Long(2), count);
        }
    }

    /*
        Tests histogram metrics for select query, point lookup query and range scan query.
     */
    @Test
    public void testHistogramMetricsForQuery() {
        String tableName = "TEST-TABLE";
        Configuration conf = new Configuration();
        conf.set(QueryServices.PHOENIX_HISTOGRAM_LATENCY_RANGES, "2,5,8");
        conf.set(QueryServices.PHOENIX_HISTOGRAM_SIZE_RANGES, "10, 100, 1000");

        QueryServicesOptions mockOptions = Mockito.mock(QueryServicesOptions.class);
        Mockito.doReturn(true).when(mockOptions).isTableLevelMetricsEnabled();
        Mockito.doReturn(tableName).when(mockOptions).getAllowedListTableNames();
        Mockito.doReturn(conf).when(mockOptions).getConfiguration();
        TableMetricsManager tableMetricsManager = new TableMetricsManager(mockOptions);
        TableMetricsManager.setInstance(tableMetricsManager);

        //Generate 2 read metrics in each bucket, one with point lookup and other with range scan.
        TableMetricsManager.updateHistogramMetricsForQueryLatency(tableName, 1, true);
        TableMetricsManager.updateHistogramMetricsForQueryScanBytes(5l, tableName, true);

        TableMetricsManager.updateHistogramMetricsForQueryLatency(tableName, 2, false);
        TableMetricsManager.updateHistogramMetricsForQueryScanBytes(10l, tableName, false);

        TableMetricsManager.updateHistogramMetricsForQueryLatency(tableName, 4, true);
        TableMetricsManager.updateHistogramMetricsForQueryScanBytes(50l, tableName, true);

        TableMetricsManager.updateHistogramMetricsForQueryLatency(tableName, 5, false);
        TableMetricsManager.updateHistogramMetricsForQueryScanBytes(100l, tableName, false);

        TableMetricsManager.updateHistogramMetricsForQueryLatency(tableName, 7, true);
        TableMetricsManager.updateHistogramMetricsForQueryScanBytes(500l, tableName, true);

        TableMetricsManager.updateHistogramMetricsForQueryLatency(tableName, 8, false);
        TableMetricsManager.updateHistogramMetricsForQueryScanBytes(1000l, tableName, false);

        // Generate distribution map from histogram snapshots.
        LatencyHistogram latencyHistogram =
                TableMetricsManager.getQueryLatencyHistogramForTable(tableName);
        SizeHistogram sizeHistogram = TableMetricsManager.getQuerySizeHistogramForTable(tableName);

        Map<String, Long> latencyMap = latencyHistogram.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> sizeMap = sizeHistogram.getRangeHistogramDistribution().getRangeDistributionMap();
        for (Long count: latencyMap.values()) {
            Assert.assertEquals(new Long(2), count);
        }
        for (Long count: sizeMap.values()) {
            Assert.assertEquals(new Long(2), count);
        }

        // Verify there is 1 entry in each bucket for point lookup query.
        LatencyHistogram pointLookupLtHisto =
                TableMetricsManager.getPointLookupLatencyHistogramForTable(tableName);
        SizeHistogram pointLookupSizeHisto =
                TableMetricsManager.getPointLookupSizeHistogramForTable(tableName);

        Map<String, Long> pointLookupLtMap = pointLookupLtHisto.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> pointLookupSizeMap = pointLookupSizeHisto.getRangeHistogramDistribution().getRangeDistributionMap();
        for (Long count: pointLookupLtMap.values()) {
            Assert.assertEquals(new Long(1), count);
        }
        for (Long count: pointLookupSizeMap.values()) {
            Assert.assertEquals(new Long(1), count);
        }

        // Verify there is 1 entry in each bucket for range scan query.
        LatencyHistogram rangeScanLtHisto =
                TableMetricsManager.getRangeScanLatencyHistogramForTable(tableName);
        SizeHistogram rangeScanSizeHisto =
                TableMetricsManager.getRangeScanSizeHistogramForTable(tableName);

        Map<String, Long> rangeScanLtMap = rangeScanLtHisto.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> rangeScanSizeMap = rangeScanSizeHisto.getRangeHistogramDistribution().getRangeDistributionMap();
        for (Long count: rangeScanLtMap.values()) {
            Assert.assertEquals(new Long(1), count);
        }
        for (Long count: rangeScanSizeMap.values()) {
            Assert.assertEquals(new Long(1), count);
        }
    }

    @Test
    public void testTableMetricsNull() {
        String tableName = "TEST-TABLE";
        String badTableName = "NOT-ALLOWED-TABLE";

        QueryServicesOptions mockOptions = Mockito.mock(QueryServicesOptions.class);
        Mockito.doReturn(true).when(mockOptions).isTableLevelMetricsEnabled();
        Mockito.doReturn(tableName).when(mockOptions).getAllowedListTableNames();

        TableMetricsManager tableMetricsManager = new TableMetricsManager(mockOptions);
        TableMetricsManager.setInstance(tableMetricsManager);
        Assert.assertNull(TableMetricsManager.getQueryLatencyHistogramForTable(badTableName));
    }

}