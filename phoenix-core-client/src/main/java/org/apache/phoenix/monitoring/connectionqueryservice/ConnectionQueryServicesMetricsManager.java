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
package org.apache.phoenix.monitoring.connectionqueryservice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.monitoring.ConnectionQueryServicesMetric;
import org.apache.phoenix.monitoring.HistogramDistribution;
import org.apache.phoenix.monitoring.MetricPublisherSupplierFactory;
import org.apache.phoenix.monitoring.MetricServiceResolver;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central place where we keep track of all the Connection Query Service metrics. Register each
 * Connection Query Service and store the instance of it associated with ConnectionServiceName in a
 * map This class exposes following functions as static functions to help catch all exception
 * 1.clearAllConnectionQueryServiceMetrics
 * 2.getConnectionQueryServicesMetrics
 * 3.updateMetrics
 */
public class ConnectionQueryServicesMetricsManager {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConnectionQueryServicesMetricsManager.class);
    private static volatile boolean isConnectionQueryServiceMetricsEnabled;
    private static volatile boolean isConnectionQueryServiceMetricPublisherEnabled;
    private static ConcurrentMap<String, ConnectionQueryServicesMetrics>
            connectionQueryServiceMetricsMapping;
    // Singleton object
    private static volatile ConnectionQueryServicesMetricsManager
            connectionQueryServicesMetricsManager = null;
    private static volatile MetricPublisherSupplierFactory mPublisher = null;
    private static volatile QueryServicesOptions options;

    @SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "This " +
            "Object is only created once for the JVM")
    public ConnectionQueryServicesMetricsManager(QueryServicesOptions opts) {
        options = opts;
        connectionQueryServiceMetricsMapping = new ConcurrentHashMap<>();
        isConnectionQueryServiceMetricsEnabled = options.isConnectionQueryServiceMetricsEnabled();
        isConnectionQueryServiceMetricPublisherEnabled =
                options.isConnectionQueryServiceMetricsPublisherEnabled();
        LOGGER.info("Connection query service metrics enabled : "
                + isConnectionQueryServiceMetricsEnabled + " publisher enabled : "
                + isConnectionQueryServiceMetricPublisherEnabled);
    }

    @SuppressWarnings(value = "EI_EXPOSE_STATIC_REP2", justification = "Only used for testing")
    public static void setInstance(ConnectionQueryServicesMetricsManager metricsManager) {
        connectionQueryServicesMetricsManager = metricsManager;
    }

    /**
     * Function to provide instance of ConnectionQueryServiceMetricsManager(Create if needed in
     * thread safe manner)
     * @return returns instance of ConnectionQueryServicesMetricsManager
     */
    @SuppressWarnings(value = "MS_EXPOSE_REP", justification = "Only used internally, not exposed" +
            " to external client")
    public static ConnectionQueryServicesMetricsManager getInstance() {
        if (connectionQueryServicesMetricsManager == null) {
            synchronized (ConnectionQueryServicesMetricsManager.class) {
                if (connectionQueryServicesMetricsManager == null) {
                    QueryServicesOptions options = QueryServicesOptions.withDefaults();
                    if (options.isConnectionQueryServiceMetricsEnabled()) {
                        connectionQueryServicesMetricsManager =
                                new ConnectionQueryServicesMetricsManager(options);
                        LOGGER.info("Created object for Connection query service metrics manager");
                    } else {
                        connectionQueryServicesMetricsManager =
                                NoOpConnectionQueryServicesMetricsManager.NO_OP_CONN_QUERY_SERVICES_METRICS_MANAGER;
                        LOGGER.info("Created object for NoOp Connection query service metrics manager");
                        return connectionQueryServicesMetricsManager;
                    }
                    registerMetricsPublisher();
                }
            }
        }
        return connectionQueryServicesMetricsManager;
    }

    ConnectionQueryServicesMetricsManager() {

    }

    public static void registerMetricsPublisher() {
        if (isConnectionQueryServiceMetricPublisherEnabled) {
            String className = options.getConnectionQueryServiceMetricsPublisherClass();
            if (className != null) {
                MetricServiceResolver mResolver = new MetricServiceResolver();
                LOGGER.info("Connection query service metrics publisher className "
                        + className);
                try {
                    mPublisher = mResolver.instantiate(className);
                    mPublisher.registerMetricProvider();
                } catch (Throwable e) {
                    LOGGER.error("The exception from metric publish Function", e);
                }

            } else {
                LOGGER.warn("Connection query service metrics publisher className"
                        + " can't be null");
            }
        }
    }

    /**
     * Function to provide Object of ConnectionQueryServicesMetrics (Create if needed in
     * thread safe manner) for connectionQueryServiceName
     * @param connectionQueryServiceName Connection Query Service Name
     * @return returns instance of ConnectionQueryServicesMetrics for connectionQueryServiceName
     */
    ConnectionQueryServicesMetrics getConnectionQueryServiceMetricsInstance(
            String connectionQueryServiceName) {
        if (Strings.isNullOrEmpty(connectionQueryServiceName)) {
            LOGGER.warn("Connection query service Name can't be null or empty");
            return null;
        }

        ConnectionQueryServicesMetrics cqsInstance =
                connectionQueryServiceMetricsMapping.get(connectionQueryServiceName);
        if (cqsInstance == null) {
            synchronized (ConnectionQueryServicesMetricsManager.class) {
                cqsInstance = connectionQueryServiceMetricsMapping.get(connectionQueryServiceName);
                if (cqsInstance == null) {

                    LOGGER.info("Creating connection query service metrics object for : "
                            + connectionQueryServiceName);
                    cqsInstance = new ConnectionQueryServicesMetrics(connectionQueryServiceName,
                            options.getConfiguration());
                    connectionQueryServiceMetricsMapping
                            .put(connectionQueryServiceName, cqsInstance);
                }
            }
        }
        return cqsInstance;
    }

    /**
     * This function will be used to add individual MetricType to LocalStore. Also this will serve
     * as LocalStore to store connection query service metrics before their current value is added
     * to histogram.
     * This func is only used for metrics which are counter based, where values increases or
     * decreases frequently. Like Open Conn Counter. This function will first retrieve it's current
     * value and increment or decrement (by +/-1) it as required then update the new values.
     * <br>
     * Example :- OPEN_PHOENIX_CONNECTIONS_COUNTER, OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER
     * <br>
     * <br>
     * histogram will update with each increment/decrement.
     * @param connectionQueryServiceName
     * @param type
     * @param value
     */
    void updateMetricsValue(String connectionQueryServiceName, MetricType type,
            long value) {

        long startTime = EnvironmentEdgeManager.currentTime();

        ConnectionQueryServicesMetrics cqsInstance =
                getConnectionQueryServiceMetricsInstance(connectionQueryServiceName);
        if (cqsInstance == null) {
            return;
        }
        cqsInstance.setMetricValue(type, value);

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Connection query service metrics completed updating metric "
                    + type + " to value " + value + ", timetaken = "
                    + (EnvironmentEdgeManager.currentTime() - startTime));
        }
    }

    /**
     * static functions to push, update or retrieve ConnectionQueryService Metrics.
     * @param connectionQueryServiceName name of the connection query service
     * @param type                       type of metric
     * @param value                      metric value
     */
    public static void updateMetrics(String connectionQueryServiceName, MetricType type,
            long value) {
        try {
            ConnectionQueryServicesMetricsManager.getInstance()
                    .updateMetricsValue(connectionQueryServiceName, type, value);
        } catch (Exception e) {
            LOGGER.error("Failed updating connection query service metrics", e);
        }
    }

    public static Map<String, List<ConnectionQueryServicesMetric>> getAllConnectionQueryServicesMetrics() {
        return ConnectionQueryServicesMetricsManager.getInstance()
                .getConnectionQueryServicesMetrics();
    }

    /**
     * This function will return all the counters for Phoenix connection query service.
     * @return Map of all ConnectionQueryService Metrics.
     */
    Map<String, List<ConnectionQueryServicesMetric>> getConnectionQueryServicesMetrics() {
        try {
            long startTime = EnvironmentEdgeManager.currentTime();
            Map<String, List<ConnectionQueryServicesMetric>> map = new HashMap<>();
            for (Map.Entry<String, ConnectionQueryServicesMetrics> entry
                    : connectionQueryServiceMetricsMapping.entrySet()) {
                map.put(entry.getKey(), entry.getValue().getAllMetrics());
            }
            long timeTakenForMetricConversion = EnvironmentEdgeManager.currentTime() - startTime;
            LOGGER.info("Connection query service metrics fetching complete, timeTaken: "
                    + timeTakenForMetricConversion);
            return map;
        } catch (Exception e) {
            LOGGER.error("Failed retrieving connection query service Metrics", e);
        }
        return null;
    }

    public static Map<String, List<HistogramDistribution>> getHistogramsForAllConnectionQueryServices() {
        return ConnectionQueryServicesMetricsManager.getInstance()
                .getHistogramsForConnectionQueryServices();
    }

    /**
     * This function will return histogram for all the Phoenix connection query service metrics.
     * @return Map of all ConnectionServiceMetrics Histogram
     */
    Map<String, List<HistogramDistribution>> getHistogramsForConnectionQueryServices() {
        Map<String, List<HistogramDistribution>> map = new HashMap<>();
        for (Map.Entry<String, ConnectionQueryServicesMetrics> entry
                : connectionQueryServiceMetricsMapping.entrySet()) {
            ConnectionQueryServicesMetricsHistograms connectionQueryServiceHistogramsHistograms =
                    entry.getValue().getConnectionQueryServiceHistograms();
            map.put(entry.getKey(), connectionQueryServiceHistogramsHistograms
                    .getConnectionQueryServicesHistogramsDistribution());
        }
        return map;
    }

    /**
     * Function to update {@link MetricType#OPEN_PHOENIX_CONNECTIONS_COUNTER} counter value in
     * Histogram
     * @param connCount                  current count of
     *                                   {@link MetricType#OPEN_PHOENIX_CONNECTIONS_COUNTER}
     * @param connectionQueryServiceName ConnectionQueryService name
     */
    public static void updateConnectionQueryServiceOpenConnectionHistogram(long connCount,
            String connectionQueryServiceName) {
        ConnectionQueryServicesMetrics metrics =
                getInstance().getConnectionQueryServiceMetricsInstance(connectionQueryServiceName);
        if (metrics == null) {
            return;
        }
        metrics.getConnectionQueryServiceHistograms().getConnectionQueryServicesOpenConnHisto()
                .add(connCount);
    }

    /**
     * Function to update {@link MetricType#OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER} counter value
     * in Histogram
     * @param connCount                  current count of
     *                                   {@link
     *                                   MetricType#OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER}
     * @param connectionQueryServiceName ConnectionQueryService name
     */
    public static void updateConnectionQueryServiceOpenInternalConnectionHistogram(long connCount,
            String connectionQueryServiceName) {
        ConnectionQueryServicesMetrics metrics =
                getInstance().getConnectionQueryServiceMetricsInstance(connectionQueryServiceName);
        if (metrics == null) {
            return;
        }
        metrics.getConnectionQueryServiceHistograms()
                .getConnectionQueryServicesInternalOpenConnHisto().add(connCount);
    }

    /////////////////////////////////////////////////////////
    ////// Below Functions are majorly used in testing //////
    /////////////////////////////////////////////////////////

    public static ConnectionQueryServicesHistogram getConnectionQueryServiceOpenInternalConnectionHistogram(
            String connectionQueryServiceName) {
        ConnectionQueryServicesMetrics metrics =
                getInstance().getConnectionQueryServiceMetricsInstance(connectionQueryServiceName);
        if (metrics == null) {
            return null;
        }
        return metrics.getConnectionQueryServiceHistograms()
                .getConnectionQueryServicesInternalOpenConnHisto();
    }

    public static ConnectionQueryServicesHistogram
        getConnectionQueryServiceOpenConnectionHistogram(String connectionQueryServiceName) {
        ConnectionQueryServicesMetrics metrics =
                getInstance().getConnectionQueryServiceMetricsInstance(connectionQueryServiceName);
        if (metrics == null) {
            return null;
        }
        return metrics.getConnectionQueryServiceHistograms()
                .getConnectionQueryServicesOpenConnHisto();
    }

    /**
     * Helps reset the localstore(connectionQueryServiceMetricsMapping)
     */
    void clearConnectionQueryServiceMetrics() {
        if (connectionQueryServiceMetricsMapping != null) {
            connectionQueryServiceMetricsMapping.clear();
        }
        LOGGER.info("Connection query service metrics clearing complete");
    }

    public static void clearAllConnectionQueryServiceMetrics() {
        try {
            ConnectionQueryServicesMetricsManager.getInstance()
                    .clearConnectionQueryServiceMetrics();
        } catch (Exception e) {
            LOGGER.error("Failed resetting connection query service Metrics", e);
        }
    }
}
