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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central place where we keep track of all the Table Level metrics. Register each tableMetrics and
 * store the instance of it associated with TableName in a map
 * This class exposes following functions as static methods to help catch all execptions
 * 1.clearTableLevelMetricsMethod
 * 2.getTableMetricsMethod
 * 3.pushMetricsFromConnInstanceMethod
 * 4.updateMetricsMethod
 */

public class TableMetricsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetricsManager.class);
    private static boolean isTableLevelMetricsEnabled;
    private static boolean isMetricPublisherEnabled;
    private static final Set<String> allowedListOfTableNames = new HashSet<>();
    private static ConcurrentMap<String, TableClientMetrics> tableClientMetricsMapping;
    // Singleton object
    public static TableMetricsManager tableMetricsManager = null;
    private static QueryServicesOptions options;

    @VisibleForTesting
    public TableMetricsManager(QueryServicesOptions opts) {
        options = opts;
        isTableLevelMetricsEnabled = options.isTableLevelMetricsEnabled();
        LOGGER.info(String.format("Phoenix Table metrics enabled status: %s", isTableLevelMetricsEnabled));
        // Return simply if table level metrics are not enabled
        if (!isTableLevelMetricsEnabled) {
            tableMetricsManager = new NoOpTableMetricsManager(options);
            return;
        }
        tableClientMetricsMapping = new ConcurrentHashMap<>();

        String tableNamesList = options.getAllowedListTableNames();
        if (tableNamesList != null && !tableNamesList.isEmpty()) {
            for (String tableName : tableNamesList.split(",")) {
                allowedListOfTableNames.add(tableName);
            }
        }

        isMetricPublisherEnabled = options.isMetricPublisherEnabled();
        LOGGER.info(String.format("Phoenix table level metrics publisher enabled status %s",
                isMetricPublisherEnabled));
    }

    @VisibleForTesting
    public static void setInstance(TableMetricsManager metricsManager) {
        tableMetricsManager = metricsManager;
    }

    /**
     * Method to provide instance of TableMetricsManager(Create if needed in thread safe manner)
     *
     * @return
     */
    private static TableMetricsManager getInstance() {

        if (tableMetricsManager == null) {
            synchronized (TableMetricsManager.class) {
                if (tableMetricsManager == null) {
                    QueryServicesOptions options = QueryServicesOptions.withDefaults();
                    tableMetricsManager = new TableMetricsManager(options);
                    LOGGER.info("Phoenix Table metrics created object for metrics manager");
                    if (isMetricPublisherEnabled) {
                        String className = options.getMetricPublisherClass();
                        if (className != null) {
                            MetricServiceResolver mResolver = new MetricServiceResolver();

                            LOGGER.info(String.format("Phoenix table level metrics publisher className %s",
                                    className));
                            try {
                                MetricPublisherSupplierFactory mPublisher = mResolver.instantiate(className);
                                mPublisher.registerMetricProvider();
                            } catch (Throwable e) {
                                LOGGER.error("The exception from metric publish Function", e);
                            }

                        } else {
                            LOGGER.error("Phoenix table level metrics publisher className cant be null");
                        }

                    }
                }
            }
        }
        return tableMetricsManager;
    }

    /**
     * This function is provided as hook to publish the tableLevel Metrics to
     * LocalStore(tablePhoenixMapping).
     *
     * @param map of tableName to pair of (MetricType, Metric Value)
     */
    private void pushMetricsFromConnInstance(Map<String, Map<MetricType, Long>> map) {

        if (map == null) {
            LOGGER.debug("Phoenix table level metrics input map cant be null");
            return;
        }

        long startTime = EnvironmentEdgeManager.currentTime();
        for (Map.Entry<String, Map<MetricType,Long>> tableEntry : map.entrySet()) {
            for(Map.Entry<MetricType,Long>metricEntry  : tableEntry.getValue().entrySet()){
                updateMetrics(tableEntry.getKey(), metricEntry.getKey(), metricEntry.getValue());
            }
        }

        LOGGER.debug(String.format("Phoenix table level metrics completed updating metrics from conn instance, timetaken:\t%d",
                + EnvironmentEdgeManager.currentTime() - startTime));
    }

    /**
     * This function will be used to add individual MetricType to LocalStore.
     *
     * @param tableName
     * @param type
     * @param value
     */
    private void updateMetrics(String tableName, MetricType type, long value) {

        long startTime = EnvironmentEdgeManager.currentTime();

        TableClientMetrics tInstance = getTableClientMetrics(tableName);
        if (tInstance == null) {
            LOGGER.debug("Table level client metrics are disabled for table: " + tableName);
            return;
        }
        tInstance.changeMetricValue(type, value);

        LOGGER.debug(String.format("Phoenix table level metrics completed updating metric"
                        + " %s to value %s, timetaken = %s", type, value,
                EnvironmentEdgeManager.currentTime() - startTime));
    }

    /**
     * Get Table specific metrics object and create if not initialized(thread safe)
     *
     * @param tableName
     * @return TableClientMetrics object
     */
    private TableClientMetrics getTableClientMetrics(String tableName) {

        if (Strings.isNullOrEmpty(tableName)) {
            LOGGER.debug("Phoenix Table metrics TableName cant be null or empty");
            return null;
        }

        if (!allowedListOfTableNames.isEmpty() && !allowedListOfTableNames.contains(tableName)) {
            return null;
        }

        TableClientMetrics tInstance;
        tInstance = tableClientMetricsMapping.get(tableName);
        if (tInstance == null) {
            synchronized (TableMetricsManager.class) {
                tInstance = tableClientMetricsMapping.get(tableName);
                if (tInstance == null) {

                    LOGGER.info(String.format("Phoenix Table metrics creating object for table: %s", tableName));
                    tInstance = new TableClientMetrics(tableName);
                    tableClientMetricsMapping.put(tableName, tInstance);
                }
            }
        }
        return tInstance;
    }

    /**
     * Publish the metrics to wherever you want them published.
     *
     * @return map of table name ->TableMetric
     */
    private Map<String, List<PhoenixTableMetric>> getTableLevelMetrics() {

        long startTime = EnvironmentEdgeManager.currentTime();
        Map<String, List<PhoenixTableMetric>> map = new HashMap<>();
        for (Map.Entry<String, TableClientMetrics> entry : tableClientMetricsMapping.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getMetricMap());
        }
        long timeTakenForMetricConversion = EnvironmentEdgeManager.currentTime() - startTime;
        LOGGER.info(String.format("Phoenix Table metrics fetching complete, timeTaken: \t%d",
                + timeTakenForMetricConversion));
        return map;
    }

    /**
     * Helps reset the localstore(tableClientMetricsMapping)
     */
    private void clearTableLevelMetrics() {
        if (tableClientMetricsMapping != null) {
            tableClientMetricsMapping.clear();
        }
        LOGGER.info("Phoenix Table metrics clearing complete");
    }

    // static methods to push, update or retrieve TableLevel Metrics.

    public static void updateMetricsMethod(String tableName, MetricType type, long value) {
        try {
            TableMetricsManager.getInstance().updateMetrics(tableName, type, value);
        }
        catch(Exception e){
            LOGGER.error("Failed updating Phoenix table level metrics",e);
        }
    }

    public static void pushMetricsFromConnInstanceMethod(Map<String, Map<MetricType, Long>> map) {
        try {
            TableMetricsManager.getInstance().pushMetricsFromConnInstance(map);
        }
        catch(Exception e){
            LOGGER.error("Failed pushing Phoenix table level metrics",e);
        }
    }

    public static Map<String, List<PhoenixTableMetric>> getTableMetricsMethod() {
        try {
            return TableMetricsManager.getInstance().getTableLevelMetrics();
        } catch (Exception e) {
            LOGGER.error("Failed retrieving table level Metrics", e);
        }
        return null;
    }

    public static void clearTableLevelMetricsMethod() {
        try {
            TableMetricsManager.getInstance().clearTableLevelMetrics();
        } catch (Exception e) {
            LOGGER.error("Failed resetting table level Metrics", e);
        }
    }

    public void clear() {
        TableMetricsManager.clearTableLevelMetricsMethod();
    }

    @VisibleForTesting
    public static Long getMetricValue(String tableName, MetricType type) {
        TableClientMetrics tableMetrics = getInstance().getTableClientMetrics(tableName);
        if (tableMetrics == null) {
            return null;
        }
        for (PhoenixTableMetric metric:  tableMetrics.getMetricMap()) {
            if (metric.getMetricType() == type) {
                return metric.getValue();
            }
        }
        return null;
    }
}
