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

import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/**
 * Integration tests for {@link HAGroupStoreClient}
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStoreClientIT extends BaseTest {

    private final PhoenixHAAdmin haAdmin = new PhoenixHAAdmin(config);
    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 5000L;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void before() throws Exception {
        // Clean up all the existing HAGroupStores
        List<HAGroupStore> haGroupStores = haAdmin.listAllHAGroupStoresOnZookeeper();
        for (HAGroupStore haGroupStore : haGroupStores) {
            haAdmin.getCurator().delete().forPath(toPath(haGroupStore.getHaGroupName()));
        }
    }

    @Test
    public void testHAGroupStoreClientWithSingleHAGroupStore() throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(config);
        HAGroupStore haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();

        // Now Update HAGroupStore so that current cluster has state ACTIVE_TO_STANDBY
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 2L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Check that now the cluster should be in ActiveToStandby
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).isEmpty();
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;


        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 3L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();


        // Change it again to ACTIVE_TO_STANDBY so that we can validate watcher works repeatedly
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 4L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).isEmpty();
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;


        // Change peer cluster to ACTIVE_TO_STANDBY so that we can still process mutation on this cluster
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY, 5L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();
    }


    @Test
    public void testHAGroupStoreClientWithMultipleHAGroupStores() throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(config);
        // Setup initial HAGroupStores
        HAGroupStore haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        HAGroupStore haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 2;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();

        // Now Update HAGroupStore so that current cluster has state ACTIVE_TO_STANDBY for only 1 HAGroupStore
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 2L);
        haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 2L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Check that now the cluster should be in ActiveToStandby
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;


        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 3L);
        haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 3L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 2;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();


        // Change other HAGroupStore to ACTIVE_TO_STANDBY and one in ACTIVE state so that we can validate watcher works repeatedly
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 4L);
        haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 4L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;


        // Change peer cluster to ACTIVE_TO_STANDBY so that we can still process mutation on this cluster
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY, 5L);
        haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY, 5L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 2;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();
    }

    @Test
    public void testMultiThreadedAccessToHACache() throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(config);
        // Setup initial HAGroupStores
        HAGroupStore haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        HAGroupStore haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        int threadCount = 10;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 2;
                    assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();
                    latch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        assert latch.await(10, TimeUnit.SECONDS);

        // Update HAGroupStores
        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 2L);
        haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 2L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        final CountDownLatch latch2 = new CountDownLatch(threadCount);
        executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
                    assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;
                    latch2.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        assert latch2.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void testHAGroupStoreClientWithRootPathDeletion() throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(config);
        HAGroupStore haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        HAGroupStore haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;

        haAdmin.getCurator().delete().deletingChildrenIfNeeded().forPath(ZKPaths.PATH_SEPARATOR);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).isEmpty();
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();


        haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 2L);
        haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 2L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;
    }

    @Test
    public void testThrowsExceptionWithZKDisconnectionAndThenConnection() throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(config);
        // Setup initial HAGroupStores
        HAGroupStore haGroupStore = new HAGroupStore("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        HAGroupStore haGroupStore1 = new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 2;

        // Shutdown the ZK Cluster to simulate CONNECTION_SUSPENDED event
        utility.shutdownMiniZKCluster();

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        //Check that HAGroupStoreClient instance is not healthy and throws IOException
        assertThrows(IOException.class, () -> haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE));

        // Start ZK on the same port to simulate CONNECTION_RECONNECTED event
        utility.startMiniZKCluster(1,Integer.parseInt(getZKClientPort(config)));

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        //Check that HAGroupStoreClient instance is back to healthy and provides correct response
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 2;
    }


    @Test
    public void testHAGroupStoreClientWithDifferentZKURLFormats() throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(config);
        final String zkClientPort = getZKClientPort(config);
        // Setup initial HAGroupStores
        final String format1 = "127.0.0.1\\:"+zkClientPort+"::/hbase"; // 127.0.0.1\:53228::/hbase
        final String format2 = "127.0.0.1:"+zkClientPort+"::/hbase"; //   127.0.0.1:53228::/hbase
        final String format3 = "127.0.0.1\\:"+zkClientPort+":/hbase";   // 127.0.0.1\:53228:/hbase

        HAGroupStore haGroupStore = new HAGroupStore("parallel1",
                HighAvailabilityPolicy.PARALLEL, format1, HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore);

        HAGroupStore haGroupStore1 = new HAGroupStore("parallel2",
                HighAvailabilityPolicy.PARALLEL, format2, HAGroupStore.ClusterRole.STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore1);

        HAGroupStore haGroupStore2 = new HAGroupStore("parallel3",
                HighAvailabilityPolicy.PARALLEL, format3, HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(haGroupStore2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.ACTIVE_TO_STANDBY).size() == 1;
        assert haGroupStoreClient.getGroupStoresByClusterRole(HAGroupStore.ClusterRole.STANDBY).size() == 1;
    }


    /**
     * This test verifies that the updates coming via PathChildrenCacheListener are in order in which updates are sent to ZK
     * @throws Exception
     */
    @Test
    public void testHAGroupStoreClientWithMultiThreadedUpdates() throws Exception {
        // Number of threads to execute
        int threadCount = 5;

        // Capture versions of HAGroupStore in a list(haGroupStoreEventVersions)  in order they are received.
        List<Integer> haGroupStoreEventVersions = new ArrayList<>();
        CountDownLatch eventsLatch = new CountDownLatch(threadCount);
        PathChildrenCacheListener pathChildrenCacheListener = (client, event) -> {
            if(event.getData() != null && event.getData().getData() != null && HAGroupStore.fromJson(event.getData().getData()).isPresent()) {
                    HAGroupStore haGroupStore = HAGroupStore.fromJson(event.getData().getData()).get();
                    haGroupStoreEventVersions.add((int)haGroupStore.getVersion());
                    eventsLatch.countDown();
            }
        };

        // Start a new HAGroupStoreClient.
        new HAGroupStoreClient(config, pathChildrenCacheListener);

        // Create multiple threads for update to ZK.
        final CountDownLatch updateLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // List captures the order of events that are sent.
        List<Integer> updateList = new ArrayList<>();

        // Create a queue which can be polled to send updates to ZK.
        ConcurrentLinkedQueue<HAGroupStore> updateQueue = new ConcurrentLinkedQueue<>();
        for(int i = 0; i < threadCount; i++) {
            updateQueue.add(createHAGroupStore(i+1));
            updateList.add(i+1);
        }

        // Submit updates to ZK.
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    synchronized (HAGroupStoreClientIT.class) {
                        haAdmin.createOrUpdateDataOnZookeeper(Objects.requireNonNull(updateQueue.poll()));
                    }
                    updateLatch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Check if updates are sent and updates are received.
        assert eventsLatch.await(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS*threadCount, TimeUnit.MILLISECONDS);
        assert updateLatch.await(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS*threadCount, TimeUnit.MILLISECONDS);

        // Assert that the order of updates is same as order of events.
        assert updateList.equals(haGroupStoreEventVersions);
    }

    private HAGroupStore createHAGroupStore(Integer version) {
        return new HAGroupStore("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), HAGroupStore.ClusterRole.ACTIVE,
                "random-zk-url", HAGroupStore.ClusterRole.STANDBY, version);
    }


}
