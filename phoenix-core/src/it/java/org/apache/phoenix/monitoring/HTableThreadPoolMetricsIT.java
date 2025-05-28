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

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ZKConnectionInfo;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.phoenix.jdbc.ConnectionInfo.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_CORE_POOL_SIZE;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_ENABLED;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_MAX_QUEUE;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_MAX_THREADS;
import static org.apache.phoenix.query.QueryServices.HTABLE_THREAD_POOL_METRICS_ENABLED;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class HTableThreadPoolMetricsIT extends BaseTest {
    private static final int ROWS_TO_LOAD_INITIALLY = 10000;
    private static final int CONCURRENT_QUERIES = 100;

    private final String registryClassName;

    public HTableThreadPoolMetricsIT(String registryClassName) {
        this.registryClassName = registryClassName;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(QueryServices.TESTS_MINI_CLUSTER_NUM_MASTERS, "2");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    @Parameterized.Parameters(name = "HTableThreadPoolMetricsIT_registryClassName={0}")
    public static Collection<String> data() {
        return Arrays.asList(ZKConnectionInfo.ZK_REGISTRY_NAME,
                "org.apache.hadoop.hbase.client.RpcConnectionRegistry",
                "org.apache.hadoop.hbase.client.MasterRegistry");
    }

    @Test
    public void testCSQIThreadPoolMetrics() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(HTABLE_THREAD_POOL_METRICS_ENABLED, "true");
        props.setProperty(CQSI_THREAD_POOL_ENABLED, "true");
        props.setProperty(CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
                "org.apache.hadoop.hbase.client.RpcConnectionRegistry");
        props.setProperty(CQSI_THREAD_POOL_CORE_POOL_SIZE, "1024");
        props.setProperty(CQSI_THREAD_POOL_MAX_THREADS, "1024");
        props.setProperty(CQSI_THREAD_POOL_MAX_QUEUE, "16384");
        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration(), "service1");
        System.out.println("URL=" + url);
        try (Connection conn = DriverManager.getConnection(url, props)) {
            createTableAndUpsertData(conn, tableName);
            System.out.println("Checking thread pool statistics after upserts");
            Map<String, List<HistogramDistribution>> htableThreadPoolHistograms =
                    PhoenixRuntime.getHTableThreadPoolHistograms();
            for (List<HistogramDistribution> histograms : htableThreadPoolHistograms.values()) {
                for (HistogramDistribution histogram : histograms) {
                    System.out.println(histogram.getHistoName());
                    System.out.println(histogram.getPercentileDistributionMap());
                    System.out.println(histogram.getTags());
                }
            }

            try (Statement stmt = conn.createStatement()) {
                ThreadPoolExecutor threadPoolExecutor =
                        (ThreadPoolExecutor) Executors.newFixedThreadPool(CONCURRENT_QUERIES);
                System.gc();
//                Thread.sleep(240000);
                System.out.println("Starting concurrent queries");
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // Set fetch size to 1
                            stmt.setFetchSize(1);
                            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                                int rowCount = 0;
                                while (rs.next()) {
                                    rowCount++;
                                }
                                Assert.assertEquals(ROWS_TO_LOAD_INITIALLY, rowCount);
                                System.out.println("Finished loading " + rowCount + " rows");
                            }
                        }
                        catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                for (int i = 0; i < CONCURRENT_QUERIES; i++) {
                    threadPoolExecutor.execute(runnable);
                }
                Thread.sleep(60 * 1000);
            }
            System.out.println("All queries completed");
        }
        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms =
                PhoenixRuntime.getHTableThreadPoolHistograms();
        for (List<HistogramDistribution> histograms : htableThreadPoolHistograms.values()) {
            for (HistogramDistribution histogram : histograms) {
                System.out.println(histogram.getHistoName());
                System.out.println(histogram.getPercentileDistributionMap());
                System.out.println(histogram.getTags());
            }
        }
        System.out.println("Recheck the thread pool statistics after reads");
        htableThreadPoolHistograms = PhoenixRuntime.getHTableThreadPoolHistograms();
        for (List<HistogramDistribution> histograms : htableThreadPoolHistograms.values()) {
            for (HistogramDistribution histogram : histograms) {
                System.out.println(histogram.getHistoName());
                System.out.println(histogram.getPercentileDistributionMap());
                System.out.println(histogram.getTags());
            }
        }
    }

    private void createTableAndUpsertData(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v "
                    + "VARCHAR)");
        }
        try (PreparedStatement stmt =conn.prepareStatement("UPSERT INTO " + tableName
                + " VALUES (?, ?)")) {
            for (int i = 1; i <= ROWS_TO_LOAD_INITIALLY; i++) {
                stmt.setString(1, "k" + i);
                stmt.setString(2, "v" + i);
                stmt.executeUpdate();
                if (i % 10000 == 0) {
                    conn.commit();
                }
            }
            conn.commit();
        }
    }
}
