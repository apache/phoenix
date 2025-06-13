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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.AbstractRPCConnectionInfo;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.ZKConnectionInfo;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.jdbc.ConnectionInfo.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_ENABLED;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_METRICS_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_QUERY_SERVICES_NAME;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class CQSIThreadPoolMetricsIT extends BaseHTableThreadPoolMetricsIT {

    private final String registryClassName;
    private final Properties props = new Properties();

    public CQSIThreadPoolMetricsIT(String registryClassName) {
        this.registryClassName = registryClassName;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        conf.setBoolean(CQSI_THREAD_POOL_METRICS_ENABLED, true);
        conf.setBoolean(CQSI_THREAD_POOL_ENABLED, true);

        InstanceResolver.clearSingletons();
        // Override to get required config for static fields loaded that require HBase config
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {

            @Override public Configuration getConfiguration() {
                return conf;
            }

            @Override public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });

        Map<String, String> props = new HashMap<>();
        props.put(QueryServices.TESTS_MINI_CLUSTER_NUM_MASTERS, "2");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    @Before
    public void testCaseSetup() {
        props.setProperty(CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, registryClassName);
    }

    @After
    public void cleanup() throws Exception {
        driver.cleanUpCQSICache();
        HTableThreadPoolMetricsManager.clearHTableThreadPoolHistograms();
        props.clear();
    }

    @Parameterized.Parameters(name = "ExternalHTableThreadPoolMetricsIT_registryClassName={0}")
    public synchronized static Collection<String> data() {
        return Arrays.asList(ZKConnectionInfo.ZK_REGISTRY_NAME,
                "org.apache.hadoop.hbase.client.RpcConnectionRegistry",
                "org.apache.hadoop.hbase.client.MasterRegistry");
    }

    @Test
    public void testHistogramsPerConnInfo() throws Exception {
        String tableName = generateUniqueName();
        String histogramKey;
        String cqsiNameService1 = "service1";
        String cqsiNameService2 = "service2";

        // Create a connection for "service1" connection profile
        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration(),
                cqsiNameService1);
        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;
        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);

            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);

            histogramKey = getHistogramKey(url);
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, histogramKey);
            Map<String, String> expectedTagKeyValues = getExpectedTagKeyValues(url,
                    cqsiNameService1);
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, histogramKey);
        }

        // Create a connection for "service2" connection profile
        url = QueryUtil.getConnectionUrl(props, utility.getConfiguration(), cqsiNameService2);
        htableThreadPoolHistograms = PhoenixRuntime.getHTableThreadPoolHistograms();
        // Assert that HTableThreadPoolHistograms for service2 is not there yet
        Assert.assertNull(htableThreadPoolHistograms.get(getHistogramKey(url)));
        try (Connection conn = driver.connect(url, props)) {
            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);

            assertHTableThreadPoolNotUsed(htableThreadPoolHistograms, histogramKey);
            Map<String, String> expectedTagKeyValues = getExpectedTagKeyValues(url,
                    cqsiNameService1);
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, histogramKey);

            histogramKey = getHistogramKey(url);
            // We have HTableThreadPoolHistograms for service1 and service2 CQSI instances
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, histogramKey);
            expectedTagKeyValues = getExpectedTagKeyValues(url,
                    cqsiNameService2);
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, histogramKey);
        }
    }

    @Test
    public void testCQSIThreadPoolHistogramsDisabled() throws Exception {
        String tableName = generateUniqueName();
        String cqsiName = "service1";
        props.setProperty(CQSI_THREAD_POOL_METRICS_ENABLED, "false");
        props.setProperty(CQSI_THREAD_POOL_ENABLED, "true");
        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration(), cqsiName);
        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);

            Map<String, List<HistogramDistribution>> htableThreadPoolHistograms =
                    runQueryAndGetHistograms(conn, tableName);
            String histogramKey = getHistogramKey(url);
            Assert.assertNull(htableThreadPoolHistograms.get(histogramKey));
        }
    }

    @Test
    public void testDefaultCQSIHistograms() throws Exception {
        String tableName = generateUniqueName();

        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration());
        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;
        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);

            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);

            String histogramKey = getHistogramKey(url);
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, histogramKey);
            Map<String, String> expectedTagKeyValues = getExpectedTagKeyValues(url,
                    DEFAULT_QUERY_SERVICES_NAME);
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, histogramKey);
        }
    }

    @Test
    public void testThreadPoolHistogramsSharedAcrossCQSIWithSameConnInfo() throws Exception {
        String tableName = generateUniqueName();
        String histogramKey;
        String cqsiName = "service1";

        // Create a connection for "service1" connection profile
        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration(), cqsiName);
        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;
        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);

            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);

            histogramKey = getHistogramKey(url);
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, histogramKey);
            Map<String, String> expectedTagKeyValues = getExpectedTagKeyValues(url, cqsiName);
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, histogramKey);
        }

        driver.cleanUpCQSICache();
        try (Connection conn = driver.connect(url, props)) {
            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);
            // Assert that no new HTableThreadPoolHistograms instance was created for a new CQSI
            // instance
            String histogramKeyForDefaultCQSI = getHistogramKey(QueryUtil.getConnectionUrl(
                    new Properties(), utility.getConfiguration()));
            Set<String> histogramKeySet =
                    new HashSet<>(Arrays.asList(histogramKeyForDefaultCQSI, histogramKey));
            Assert.assertTrue(histogramKeySet.containsAll(htableThreadPoolHistograms.keySet()));
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, histogramKey);
            Map<String, String> expectedTagKeyValues = getExpectedTagKeyValues(url, cqsiName);
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, histogramKey);
        }
    }

    private String getHistogramKey(String url) throws SQLException {
        return ConnectionInfo.createNoLogin(url, null, null).toUrl();
    }

    private Map<String, String> getExpectedTagKeyValues(String url, String cqsiName)
            throws SQLException {
        Map<String, String> expectedTagKeyValues = new HashMap<>();
        ConnectionInfo connInfo = ConnectionInfo.createNoLogin(url, null, null);
        if (registryClassName.equals(ZKConnectionInfo.ZK_REGISTRY_NAME)) {
            expectedTagKeyValues.put(HTableThreadPoolHistograms.Tag.servers.name(),
                    ((ZKConnectionInfo) connInfo).getZkHosts());
        }
        else {
            expectedTagKeyValues.put(HTableThreadPoolHistograms.Tag.servers.name(),
                    ((AbstractRPCConnectionInfo) connInfo).getBoostrapServers());
        }
        expectedTagKeyValues.put(HTableThreadPoolHistograms.Tag.cqsiName.name(), cqsiName);
        return expectedTagKeyValues;
    }
}
