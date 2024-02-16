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
package org.apache.phoenix.monitoring.connectionqueryservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.monitoring.ConnectionQueryServicesMetric;
import org.apache.phoenix.monitoring.HistogramDistribution;
import org.apache.phoenix.monitoring.Metric;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.monitoring.MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.apache.phoenix.query.QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS;
import static org.apache.phoenix.query.QueryServices.CONNECTION_QUERY_SERVICE_METRICS_ENABLED;
import static org.apache.phoenix.query.QueryServices.INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS;
import static org.apache.phoenix.query.QueryServices.QUERY_SERVICES_NAME;
import static org.apache.phoenix.util.PhoenixRuntime.clearAllConnectionQueryServiceMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class ConnectionQueryServicesMetricsIT extends BaseTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConnectionQueryServicesMetricsIT.class);
    private AtomicInteger counter = new AtomicInteger();
    private static HBaseTestingUtility hbaseTestUtil;
    private String tableName;
    private static final String CONN_QUERY_SERVICE_1 = "CONN_QUERY_SERVICE_1";
    private static final String
            CONN_QUERY_SERVICE_CHECK_CONN_THROTTLE = "CONN_QUERY_SERVICE_CHECK_CONN_THROTTLE";
    private static final String CONN_QUERY_SERVICE_2 = "CONN_QUERY_SERVICE_2";
    private static final String CONN_QUERY_SERVICE_NULL = null;
    private enum CompareOp {
        LT, EQ, GT, LTEQ, GTEQ
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        InstanceResolver.clearSingletons();
        // Override to get required config for static fields loaded that require HBase config
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {

            @Override public Configuration getConfiguration() {
                Configuration conf = HBaseConfiguration.create();
                conf.set(CONNECTION_QUERY_SERVICE_METRICS_ENABLED, String.valueOf(true));
                // Without this config, unlimited connections are allowed from client and connection
                // counter won't be increased at all. So we need to set max allowed connection count
                conf.set(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, "2");
                conf.set(INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS, "1");
                return conf;
            }

            @Override public Configuration getConfiguration(Configuration confToClone) {
                Configuration conf = HBaseConfiguration.create();
                conf.set(CONNECTION_QUERY_SERVICE_METRICS_ENABLED, String.valueOf(true));
                // Without this config, unlimited connections are allowed from client and connection
                // counter won't be increased at all. So we need to set max allowed connection count
                conf.set(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, "2");
                conf.set(INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS, "1");
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
        Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        hbaseTestUtil.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @AfterClass
    public static void tearDownMiniCluster() {
        try {
            if (hbaseTestUtil != null) {
                hbaseTestUtil.shutdownMiniCluster();
            }
        } catch (Exception e) {
            // ignore
        }
    }

    @Before
    public void resetTableLevelMetrics() {
        clearAllConnectionQueryServiceMetrics();
        tableName = generateUniqueName();
    }

    @After
    public void cleanUp() {
        clearAllConnectionQueryServiceMetrics();
    }

    private String connUrlWithPrincipal(String principalName) throws SQLException {
        return ConnectionInfo.create(url, null, null).withPrincipal(principalName).toUrl();
    }

    @Test
    public void testMultipleCQSIMetricsInParallel() throws Exception {
        Thread csqi1 = new Thread(() -> {
            try {
                checkConnectionQueryServiceMetricsValues(CONN_QUERY_SERVICE_1);
                // Increment counter for successful check
                counter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread csqi2 = new Thread(() -> {
            try {
                // We have set limit of 2 for phoenix connection counter in doSetup() function.
                // For this one, we will create more than 2 connections and
                // test that Connection Throttle Count Metric is also working as expected.
                checkConnectionQueryServiceMetricsValues(CONN_QUERY_SERVICE_CHECK_CONN_THROTTLE);
            } catch (Exception e) {
                e.printStackTrace();
                if(!e.getMessage().equals("This should not be thrown for "
                        + CONN_QUERY_SERVICE_CHECK_CONN_THROTTLE)) {
                    // Increment counter for successful check. For this Connection Query Service,
                    // code will throw error since it will try to create more than 2 connections.
                    // So we would count exception as success here and increment the counter.
                    counter.incrementAndGet();
                }
            }
        });
        Thread csqi3 = new Thread(() -> {
            try {
                checkConnectionQueryServiceMetricsValues(CONN_QUERY_SERVICE_2);
                // Increment counter for successful check
                counter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread csqi4 = new Thread(() -> {
            try {
                // Test default CQS name
                checkConnectionQueryServiceMetricsValues(CONN_QUERY_SERVICE_NULL);
                // Increment counter for successful check
                counter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Start Single Query Service Test
        csqi1.start();
        csqi1.join();

        // Start 3 Query Service Test in parallel
        csqi2.start();
        csqi3.start();
        csqi4.start();
        csqi2.join();
        csqi3.join();
        csqi4.join();

        // Check If all CSQI Metric check passed or not
        assertEquals("Number of passing CSQI Metrics check should be : ",4, counter.get());
    }

    private void checkConnectionQueryServiceMetricsValues(
            String queryServiceName) throws Exception {
        String CREATE_TABLE_DDL = "CREATE TABLE IF NOT EXISTS %s (K VARCHAR(10) NOT NULL"
                + " PRIMARY KEY, V VARCHAR)";
        String princURL = connUrlWithPrincipal(queryServiceName);
        LOGGER.info("Connection Query Service : " + queryServiceName + " URL : " + princURL);

        String connQueryServiceName;
        try (Connection conn = DriverManager.getConnection(princURL);
             Statement stmt = conn.createStatement()) {
            connQueryServiceName = conn.unwrap(PhoenixConnection.class).getQueryServices()
                    .getConfiguration().get(QUERY_SERVICES_NAME);
            // When queryServiceName is passed as null, Phoenix will change query service name
            // to DEFAULT_CQSN. That's why we are re-assigning the query service name here to check
            // metric in finally block.
            queryServiceName = connQueryServiceName;
            stmt.execute(String.format(CREATE_TABLE_DDL, tableName + "_" + connQueryServiceName));
            if (connQueryServiceName.equals(CONN_QUERY_SERVICE_CHECK_CONN_THROTTLE)) {
                try(Connection conn1 = DriverManager.getConnection(princURL)) {
                    assertMetricValues(connQueryServiceName, 2, 0, 0);
                    assertHistogramMetricsForMutations(connQueryServiceName, 2, 0, 0,
                            0);
                    try(Connection conn2 = DriverManager.getConnection(princURL)) {
                        // This should never execute in this test.
                        throw new RuntimeException("This should not be thrown for "
                                + CONN_QUERY_SERVICE_CHECK_CONN_THROTTLE);
                    }
                }
            } else {
                // We only create one connection so,
                // Open Connection Count : 1
                // Open Internal Connection Count : 0
                // Connection Throttled Count : 0
                assertMetricValues(connQueryServiceName, 1, 0, 0);
                assertHistogramMetricsForMutations(connQueryServiceName, 1, 0, 0, 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (queryServiceName.equals(CONN_QUERY_SERVICE_CHECK_CONN_THROTTLE)) {
                // We have closed the connection in try-resource-catch block so,
                // Open Connection Count : 0
                // Connection Throttled Count : 1
                // Open Internal Connection Count : 0
                assertMetricValues(queryServiceName, 0, 1, 0);
                // In histogram, we will still have max open connection count as 2
                // while rest of the values will be 0.
                assertHistogramMetricsForMutations(queryServiceName, 2, 0, 0, 0);
            } else {
                // We have closed the connection in try-resource-catch block so,
                // Open Connection Count : 0
                // Connection Throttled Count : 0
                // Open Internal Connection Count : 0
                assertMetricValues(queryServiceName, 0, 0, 0);
                // In histogram, we will still have max open connection count as 1 while rest of the values will be 0.
                assertHistogramMetricsForMutations(queryServiceName, 1, 0, 0, 0);
            }
        }
    }

    /**
     * check min/max value in histogram
     * @param queryServiceName Connection Query Service Name
     * @param oMaxValue Max value of {@link MetricType#OPEN_PHOENIX_CONNECTIONS_COUNTER}
     * @param oMinValue Min value of {@link MetricType#OPEN_PHOENIX_CONNECTIONS_COUNTER}
     * @param ioMaxValue Max value of {@link MetricType#OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER}
     * @param ioMinValue Min value of {@link MetricType#OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER}
     */
    private void assertHistogramMetricsForMutations(
            String queryServiceName, int oMaxValue, int oMinValue, int ioMaxValue, int ioMinValue) {
        Map<String, List<HistogramDistribution>> listOfHistoDistribution =
                PhoenixRuntime.getAllConnectionQueryServicesHistograms();
        for(HistogramDistribution histo : listOfHistoDistribution.get(queryServiceName)) {
            assertHistogram(histo, "PhoenixInternalOpenConn", ioMaxValue, ioMinValue,
                    CompareOp.EQ);
            assertHistogram(histo, "PhoenixOpenConn", oMaxValue, oMinValue, CompareOp.EQ);
        }
    }

    public void assertHistogram(HistogramDistribution histo, String histoName, long maxValue,
            long minValue, CompareOp op) {
        if (histo.getHistoName().equals(histoName)) {
            switch (op) {
                case EQ:
                    assertEquals(maxValue, histo.getMax());
                    assertEquals(minValue, histo.getMin());
                    break;
            }
        }
    }

    /**
     * check metric value for connection query service
     * @param queryServiceName Connection Query Service Name
     * @param o {@link MetricType#OPEN_PHOENIX_CONNECTIONS_COUNTER}
     * @param ct {@link MetricType#PHOENIX_CONNECTIONS_THROTTLED_COUNTER}
     * @param io {@link MetricType#OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER}
     */
    public void assertMetricValues(String queryServiceName, int o, int ct, int io) {
        Map<String, List<ConnectionQueryServicesMetric>> listOfMetrics =
                PhoenixRuntime.getAllConnectionQueryServicesCounters();
        /*
          There are 3 metrics which are tracked as part of Phoenix Connection Query Service Metrics.
          Defined here : {@link ConnectionQueryServicesMetrics.QueryServiceMetrics}
          OPEN_PHOENIX_CONNECTIONS_COUNTER
          OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER
          PHOENIX_CONNECTIONS_THROTTLED_COUNTER
         */
        assertEquals(3, listOfMetrics.get(queryServiceName).size());
        for (ConnectionQueryServicesMetric metric : listOfMetrics.get(queryServiceName)) {
            assertMetricValue(metric, OPEN_PHOENIX_CONNECTIONS_COUNTER, o, CompareOp.EQ);
            assertMetricValue(metric, PHOENIX_CONNECTIONS_THROTTLED_COUNTER, ct, CompareOp.EQ);
            assertMetricValue(metric, OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER, io, CompareOp.EQ);
        }
    }

    /**
     * Check if metrics collection is empty.
     */
    public void assertMetricListIsEmpty() {
        Map<String, List<ConnectionQueryServicesMetric>> listOfMetrics =
                PhoenixRuntime.getAllConnectionQueryServicesCounters();
        assertTrue(listOfMetrics.isEmpty());
    }

    /**
     * Checks that if the metric is of the passed in type, it has the expected value
     * (based on the CompareOp). If the metric type is different than checkType, ignore
     * @param m metric to check
     * @param checkType type to check for
     * @param compareValue value to compare against
     * @param op CompareOp
     */
    private static void assertMetricValue(Metric m, MetricType checkType, long compareValue,
                                          CompareOp op) {
        if (m.getMetricType().equals(checkType)) {
            switch (op) {
                case EQ:
                    assertEquals(compareValue, m.getValue());
                    break;
                case LT:
                    assertTrue(m.getValue() < compareValue);
                    break;
                case LTEQ:
                    assertTrue(m.getValue() <= compareValue);
                    break;
                case GT:
                    assertTrue(m.getValue() > compareValue);
                    break;
                case GTEQ:
                    assertTrue(m.getValue() >= compareValue);
                    break;
            }
        }
    }
}
