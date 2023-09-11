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

import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityPolicy.PARALLEL;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class ParallelPhoenixConnectionFallbackIT {

    private static final Logger LOG =
            LoggerFactory.getLogger(ParallelPhoenixConnectionFallbackIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
    private static final Properties PROPERTIES = new Properties();

    private static String jdbcUrl;
    private static HighAvailabilityGroup haGroup;
    private static String tableName = ParallelPhoenixConnectionFallbackIT.class.getSimpleName();
    private static String haGroupName;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        haGroupName = ParallelPhoenixConnectionFallbackIT.class.getSimpleName();
        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        PROPERTIES.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
        PROPERTIES.setProperty(PhoenixHAExecutorServiceProvider.HA_MAX_POOL_SIZE, "1");
        PROPERTIES.setProperty(PhoenixHAExecutorServiceProvider.HA_MAX_QUEUE_SIZE, "2");
        PROPERTIES.setProperty(
            PhoenixHAExecutorServiceProvider.HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD, "0.5");

        // Make first cluster ACTIVE
        CLUSTERS.initClusterRole(haGroupName, PARALLEL);

        jdbcUrl = CLUSTERS.getJdbcHAUrl();
        haGroup = HighAvailabilityTestingUtility.getHighAvailibilityGroup(jdbcUrl, PROPERTIES);
        LOG.info("Initialized haGroup {} with URL {}", haGroup.getGroupInfo().getName(), jdbcUrl);
        CLUSTERS.createTableOnClusterPair(tableName);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        CLUSTERS.close();
    }

    @Test
    public void testParallelConnectionBackoff() throws Exception {
        Connection connA = DriverManager.getConnection(jdbcUrl, PROPERTIES);
        assertTrue(connA instanceof ParallelPhoenixConnection);
        doTestBasicOperationsWithConnection(connA, tableName, haGroupName);
        // Block threads of the pool
        CountDownLatch cdl1 = new CountDownLatch(1);
        CountDownLatch cdl2 = new CountDownLatch(1);
        ParallelPhoenixContext contextA = ((ParallelPhoenixConnection) connA).getContext();
        waitFor(() -> contextA.getChainOnConn1().isDone(), 100, 5000);
        waitFor(() -> contextA.getChainOnConn2().isDone(), 100, 5000);
        contextA.chainOnConn1(getSuplierWithLatch(cdl1));
        contextA.chainOnConn2(getSuplierWithLatch(cdl2));
        waitFor(() -> PhoenixHAExecutorServiceProvider.hasCapacity(PROPERTIES).get(0) &&
            PhoenixHAExecutorServiceProvider.hasCapacity(PROPERTIES).get(1), 100, 5000);
        ExecutorService executor = Executors.newFixedThreadPool(1);
        // Since both the cluster executors are busy, the new connection will be
        // put in the executor queue, and a connection won't be returned unless
        // this is picked. At this point of time, the capacity is available, so
        // this should be a ParallelPhoenixConnection.
        Future<Connection> futureConnB = executor.submit(() -> DriverManager.getConnection(jdbcUrl, PROPERTIES));

        // The previous call of connection creation should fill the queue by half.
        waitFor(() -> !PhoenixHAExecutorServiceProvider.hasCapacity(PROPERTIES).get(0) &&
                !PhoenixHAExecutorServiceProvider.hasCapacity(PROPERTIES).get(1), 100, 5000);

        // This should be backed off now, as the capacity is not available.
        Connection connC = DriverManager.getConnection(jdbcUrl, PROPERTIES);
        // We should've backed off now since creating the prev connection should fill
        // the queue half
        assertTrue(connC instanceof PhoenixConnection);
        // Let the threads go
        cdl1.countDown();
        cdl2.countDown();

        // Once the previous tasks are done, we expect the futureConnB to be picked and be done.
        waitFor(() -> futureConnB.isDone(), 1000, 5000);
        Connection connB = futureConnB.get();
        assertTrue(connB instanceof ParallelPhoenixConnection);

        doTestBasicOperationsWithConnection(connB, tableName, haGroupName);
        ParallelPhoenixContext contextB = ((ParallelPhoenixConnection) connB).getContext();
        waitFor(() -> contextB.getChainOnConn1().isDone(), 100, 5000);
        waitFor(() -> contextB.getChainOnConn2().isDone(), 100, 5000);

        // Now that the queue has capacity, this should be ParallelLPhoenixConnection.
        Connection connD = DriverManager.getConnection(jdbcUrl, PROPERTIES);
        assertTrue(connD instanceof ParallelPhoenixConnection);
        closeConnections(connA, connB, connC, connD);
    }

    private Supplier<?> getSuplierWithLatch(CountDownLatch cdl) {
        Supplier<?> s = () -> {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        };
        return s;
    }

    private void closeConnections(Connection... connections) {
        for (Connection conn : connections) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
