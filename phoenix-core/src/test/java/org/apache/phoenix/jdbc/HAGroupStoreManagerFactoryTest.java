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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.query.QueryServicesOptions.CONSISTENT_HA_IMPLEMENTATION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit tests for {@link HAGroupStoreManagerFactory}.
 */
public class HAGroupStoreManagerFactoryTest {

    @Rule
    public TestName testName = new TestName();

    private Configuration testConfig;
    private String zkUrl;
    private static final String TEST_ZK_HOST_1 = "test-zk1,test-zk2";
    private static final String TEST_ZK_HOST_2 = "test-zk3,test-zk4";
    private static final String TEST_ZK_PORT = "2181";
    private static final String TEST_ZK_ZNODE = "/hbase";

    @Before
    public void setUp() throws Exception {
        testConfig = createTestConfiguration(TEST_ZK_HOST_1);
        zkUrl = testConfig.get(HConstants.ZOOKEEPER_QUORUM);
        // Clear the static instances cache before each test
        clearStaticInstances();
    }

    @After
    public void tearDown() throws Exception {
        // Clear the static instances cache after each test
        clearStaticInstances();
    }

    private Configuration createTestConfiguration(String zkHosts) {
        Configuration config = new Configuration();
        config.set(HConstants.ZOOKEEPER_QUORUM, zkHosts);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, HAGroupStoreManagerFactoryTest.TEST_ZK_PORT);
        config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, HAGroupStoreManagerFactoryTest.TEST_ZK_ZNODE);
        return config;
    }

    @SuppressWarnings("unchecked")
    private void clearStaticInstances() throws Exception {
        Field instancesField = HAGroupStoreManagerFactory.class.getDeclaredField("INSTANCES");
        instancesField.setAccessible(true);
        Map<String, HAGroupStoreManager> instances = (Map<String, HAGroupStoreManager>) instancesField.get(null);
        instances.clear();
    }

    @Test
    public void testGetInstanceWithDefaultImplementation() {
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);

        assertTrue(managerOpt.isPresent());
        assertTrue(managerOpt.get() instanceof HAGroupStoreManagerV1Impl);
    }

    @Test
    public void testGetInstanceWithExplicitV1Implementation() {
        testConfig.set(QueryServices.HA_IMPLEMENTATION, QueryServicesOptions.DEFAULT_HA_IMPLEMENTATION);

        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);

        assertTrue(managerOpt.isPresent());
        assertTrue(managerOpt.get() instanceof HAGroupStoreManagerV1Impl);
    }

    @Test
    public void testGetInstanceWithMainImplementation() {
        testConfig.set(QueryServices.HA_IMPLEMENTATION, CONSISTENT_HA_IMPLEMENTATION);

        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);

        assertTrue(managerOpt.isPresent());
        assertTrue(managerOpt.get() instanceof HAGroupStoreManagerImpl);
    }

    @Test
    public void testGetInstanceWitInvalidImplementation() {
        testConfig.set(QueryServices.HA_IMPLEMENTATION, "random");

        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);

        assertFalse(managerOpt.isPresent());
    }

    @Test
    public void testSingletonBehaviorSameZkUrl() {
        Optional<HAGroupStoreManager> manager1Opt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);
        Optional<HAGroupStoreManager> manager2Opt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);

        assertTrue(manager1Opt.isPresent());
        assertTrue(manager2Opt.isPresent());
        assertSame(manager1Opt.get(), manager2Opt.get());
    }

    @Test
    public void testDifferentInstancesForDifferentZkUrls() {
        Configuration testConfig2 = createTestConfiguration(TEST_ZK_HOST_2);

        Optional<HAGroupStoreManager> manager1Opt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);
        Optional<HAGroupStoreManager> manager2Opt
                = HAGroupStoreManagerFactory.getInstance(testConfig2, testConfig2.get(HConstants.ZOOKEEPER_QUORUM));

        assertTrue(manager1Opt.isPresent());
        assertTrue(manager2Opt.isPresent());
        assertNotSame(manager1Opt.get(), manager2Opt.get()); // Different instances for different ZK URLs
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicReference<HAGroupStoreManager> managerReference = new AtomicReference<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(testConfig, zkUrl);

                    assertTrue(managerOpt.isPresent());
                    HAGroupStoreManager manager = managerOpt.get();

                    // All threads should get the same instance
                    if (managerReference.get() == null) {
                        managerReference.set(manager);
                    } else {
                        assertSame(managerReference.get(), manager);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertNotNull(managerReference.get());
    }

    @Test
    public void testCreateInstanceWithNullConfiguration() {
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(null, zkUrl);

        assertFalse(managerOpt.isPresent());
    }

    @Test
    public void testNullZkUrlHandling() {
        Configuration nullZkConfig = new Configuration();

        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(nullZkConfig, zkUrl);

        assertFalse(managerOpt.isPresent());
    }

    @Test
    public void testEmptyStringZkUrlHandling() {
        Configuration emptyZkConfig = createTestConfiguration("");

        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(emptyZkConfig, zkUrl);

        assertFalse(managerOpt.isPresent());
    }
}