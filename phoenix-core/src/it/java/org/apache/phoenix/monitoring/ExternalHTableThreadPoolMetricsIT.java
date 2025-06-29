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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ZKConnectionInfo;
import org.apache.phoenix.job.HTableThreadPoolWithUtilizationStats;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.HTableFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.util.InstanceResolver;
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

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.phoenix.jdbc.ConnectionInfo.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ExternalHTableThreadPoolMetricsIT extends BaseHTableThreadPoolMetricsIT{
    private static final int MAX_THREADS_IN_EXTERNAL_THREAD_POOL = 10;
    private static final int QUEUE_CAPACITY_OF_EXTERNAL_THREAD_POOL = 100;
    private static final String TAG_NAME = "cluster";
    private static final Map<String, String> tagValues = new HashMap<>();
    private static final String THREAD_POOL_1A = "external_thread_pool_1".toUpperCase();
    private static final String THREAD_POOL_2A = "external_thread_pool_2".toUpperCase();
    private static final String HISTOGRAM_DISABLED_THREAD_POOL =
            "histogram_disabled_thread_pool".toUpperCase();
    private static final String NULL_SUPPLIER_THREAD_POOL = "null_supplier_thread_pool".toUpperCase();
    private static final String NO_TAGS_THREAD_POOL = "no_tags_thread_pool".toUpperCase();

    private final String registryClassName;
    private final Properties props = new Properties();

    public ExternalHTableThreadPoolMetricsIT(String registryClassName) {
        this.registryClassName = registryClassName;
    }

    private static ThreadPoolExecutor createThreadPoolExecutor(String threadPoolName) {
        Supplier<HTableThreadPoolHistograms> supplier =
                getHTableThreadPoolHistogramsSupplier(threadPoolName);
        BlockingQueue<Runnable> workQueue =
                new LinkedBlockingQueue<>(QUEUE_CAPACITY_OF_EXTERNAL_THREAD_POOL);
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(threadPoolName + "-shared-pool-%d")
                .setUncaughtExceptionHandler(
                        Threads.LOGGING_EXCEPTION_HANDLER)
                .build();
        return new HTableThreadPoolWithUtilizationStats(
                MAX_THREADS_IN_EXTERNAL_THREAD_POOL, MAX_THREADS_IN_EXTERNAL_THREAD_POOL,
                30, TimeUnit.SECONDS, workQueue, threadFactory,
                threadPoolName, supplier) {
                    @Override
                    public void execute(Runnable command) {
                        super.execute(command);
                    }
                };
    }

    private static Supplier<HTableThreadPoolHistograms> getHTableThreadPoolHistogramsSupplier(
            String threadPoolName) {
        Supplier<HTableThreadPoolHistograms> supplier;
        if (threadPoolName.equals(HISTOGRAM_DISABLED_THREAD_POOL)) {
            supplier = null;
        }
        else if (threadPoolName.equals(NULL_SUPPLIER_THREAD_POOL)) {
            supplier = new Supplier<HTableThreadPoolHistograms>() {
                @Override
                public HTableThreadPoolHistograms get() {
                    return null;
                }
            };
        }
        else if (threadPoolName.equals(NO_TAGS_THREAD_POOL)) {
            supplier = new Supplier<HTableThreadPoolHistograms>() {
                @Override
                public HTableThreadPoolHistograms get() {
                    return new HTableThreadPoolHistograms(MAX_THREADS_IN_EXTERNAL_THREAD_POOL,
                            QUEUE_CAPACITY_OF_EXTERNAL_THREAD_POOL);
                }
            };
        }
        else {
            supplier = new Supplier<HTableThreadPoolHistograms>() {
                @Override
                public HTableThreadPoolHistograms get() {
                    HTableThreadPoolHistograms hTableThreadPoolHistograms =
                            new HTableThreadPoolHistograms(MAX_THREADS_IN_EXTERNAL_THREAD_POOL,
                                    QUEUE_CAPACITY_OF_EXTERNAL_THREAD_POOL);
                    hTableThreadPoolHistograms.addTag(TAG_NAME, tagValues.get(threadPoolName));
                    return hTableThreadPoolHistograms;
                }
            };
        }
        return supplier;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        tagValues.put(THREAD_POOL_1A, "hbase1a");
        tagValues.put(THREAD_POOL_2A, "hbase2a");

        final Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);

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
        ThreadPoolExecutor executorFor1a = createThreadPoolExecutor(THREAD_POOL_1A);
        ThreadPoolExecutor executorFor2a = createThreadPoolExecutor(THREAD_POOL_2A);
        ThreadPoolExecutor histogramDisabledExecutor =
                createThreadPoolExecutor(HISTOGRAM_DISABLED_THREAD_POOL);
        ThreadPoolExecutor nullSupplierExecutor =
                createThreadPoolExecutor(NULL_SUPPLIER_THREAD_POOL);
        ThreadPoolExecutor defaultExecutor = createThreadPoolExecutor(NO_TAGS_THREAD_POOL);
        InstanceResolver.getSingleton(HTableFactory.class, new HTableFactory.HTableFactoryImpl() {
            @Override
            public Table getTable(byte[] tableName,
                                  org.apache.hadoop.hbase.client.Connection connection,
                                  ExecutorService pool) throws IOException {
                if (Bytes.toString(tableName).startsWith(HISTOGRAM_DISABLED_THREAD_POOL)) {
                    return super.getTable(tableName, connection, histogramDisabledExecutor);
                }
                else if (Bytes.toString(tableName).startsWith(NULL_SUPPLIER_THREAD_POOL)) {
                    return super.getTable(tableName, connection, nullSupplierExecutor);
                }
                else if (Bytes.toString(tableName).startsWith(THREAD_POOL_1A)) {
                    return super.getTable(tableName, connection, executorFor1a);
                }
                else if (Bytes.toString(tableName).startsWith(THREAD_POOL_2A)) {
                    return super.getTable(tableName, connection, executorFor2a);
                }
                else {
                    return super.getTable(tableName, connection, defaultExecutor);
                }
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
        HTableThreadPoolMetricsManager.clearHTableThreadPoolHistograms();
        props.clear();
    }

    @Parameterized.Parameters(name = "ExternalHTableThreadPoolMetricsIT_registryClassName={0}")
    public synchronized static Collection<String> data() {
        List<String> list = new ArrayList<>();
        list.add(ZKConnectionInfo.ZK_REGISTRY_NAME);
        if (VersionInfo.compareVersion(VersionInfo.getVersion(), "2.3.0") >= 0) {
            list.add("org.apache.hadoop.hbase.client.MasterRegistry");
        }
        if (VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0") >= 0) {
            list.add("org.apache.hadoop.hbase.client.RpcConnectionRegistry");
        }
        return list;
    }

    @Test
    public void testHistogramsPerHTableThreadPool() throws Exception {
        String tableName = THREAD_POOL_1A + "." + generateUniqueName();

        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;
        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration());

        // Send traffic to HTable thread pool for hbase1a
        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);

            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, THREAD_POOL_1A);
            Assert.assertNull(htableThreadPoolHistograms.get(THREAD_POOL_2A));

            Map<String, String> expectedTagKeyValues = new HashMap<>();
            expectedTagKeyValues.put(TAG_NAME, tagValues.get(THREAD_POOL_1A));
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, THREAD_POOL_1A);
        }

        // Send traffic to HTable thread pool for hbase2a
        tableName = THREAD_POOL_2A + "." + generateUniqueName();
        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);

            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);

            // We will have a HTable thread pool for hbase1a also
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, THREAD_POOL_2A);
            assertHTableThreadPoolNotUsed(htableThreadPoolHistograms, THREAD_POOL_1A);

            Map<String, String> expectedTagKeyValues = new HashMap<>();
            expectedTagKeyValues.put(TAG_NAME, tagValues.get(THREAD_POOL_1A));
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, THREAD_POOL_1A);

            expectedTagKeyValues.put(TAG_NAME, tagValues.get(THREAD_POOL_2A));
            assertHistogramTags(htableThreadPoolHistograms, expectedTagKeyValues, THREAD_POOL_2A);
        }
    }

    @Test
    public void testHistogramDisabled() throws Exception {
        String tableName = HISTOGRAM_DISABLED_THREAD_POOL + "." + generateUniqueName();

        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;
        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration());

        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);
            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);
            Assert.assertNull(htableThreadPoolHistograms.get(HISTOGRAM_DISABLED_THREAD_POOL));
        }
    }

    @Test
    public void testNullHistogramSupplier() throws Exception {
        String tableName = NULL_SUPPLIER_THREAD_POOL + "." + generateUniqueName();

        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;
        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration());

        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);
            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);
            Assert.assertNull(htableThreadPoolHistograms.get(NULL_SUPPLIER_THREAD_POOL));
        }
    }

    @Test
    public void testHistogramsWithoutTags() throws Exception {
        String tableName = generateUniqueName();
        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;

        String url = QueryUtil.getConnectionUrl(props, utility.getConfiguration());
        try (Connection conn = driver.connect(url, props)) {
            createTableAndUpsertData(conn, tableName);
            htableThreadPoolHistograms = runQueryAndGetHistograms(conn, tableName);
            assertHTableThreadPoolUsed(htableThreadPoolHistograms, NO_TAGS_THREAD_POOL);
            assertHistogramTags(htableThreadPoolHistograms, new HashMap<>(), NO_TAGS_THREAD_POOL);
        }
    }
}
