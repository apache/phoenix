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

import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityPolicy.PARALLEL;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneHBaseDown;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.jdbc.ParallelPhoenixResultSetFactory.ParallelPhoenixResultSetType;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class ParallelPhoenixNullComparingResultSetIT {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelPhoenixNullComparingResultSetIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
    private static final Properties PROPERTIES = new Properties();
    private static final String tableName =
            ParallelPhoenixNullComparingResultSetIT.class.getSimpleName();
    private static final AtomicInteger intCounter = new AtomicInteger();
    private static final String selectFormat = "SELECT v FROM %s WHERE id = %d";

    private static String jdbcUrl;
    private static HighAvailabilityGroup haGroup;

    @Rule
    public final Timeout globalTimeout= new Timeout(300, TimeUnit.SECONDS);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String haGroupName = ParallelPhoenixNullComparingResultSetIT.class.getSimpleName();
        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        PROPERTIES.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
        PROPERTIES.setProperty(ParallelPhoenixResultSetFactory.PHOENIX_PARALLEL_RESULTSET_TYPE,
            ParallelPhoenixResultSetType.PARALLEL_PHOENIX_NULL_COMPARING_RESULT_SET.getName());
        PROPERTIES.setProperty(ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB, "3000");
        // Make first cluster ACTIVE
        CLUSTERS.initClusterRole(haGroupName, PARALLEL);

        jdbcUrl = CLUSTERS.getJdbcHAUrl();
        haGroup = HighAvailabilityTestingUtility.getHighAvailibilityGroup(jdbcUrl, PROPERTIES);
        LOG.info("Initialized haGroup {} with URL {}", haGroup.getGroupInfo().getName(), jdbcUrl);
        CLUSTERS.createTableOnClusterPair(tableName, false);
        // Disable replication from cluster 1 to cluster 2
        ReplicationAdmin admin =
                new ReplicationAdmin(CLUSTERS.getHBaseCluster1().getConfiguration());
        admin.removePeer(PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT);
        ReplicationAdmin admin2 =
                new ReplicationAdmin(CLUSTERS.getHBaseCluster2().getConfiguration());
        admin2.removePeer(PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        haGroup.close();
        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        CLUSTERS.close();
    }

    @Before
    public void init() {
        GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER.getMetric().reset();
    }

    @Test
    public void testRowOnCluster1() throws SQLException {
        testRowOnCluster(CLUSTERS.getUrl1());
    }

    @Test
    public void testRowOnCluster2() throws SQLException {
        testRowOnCluster(CLUSTERS.getUrl2());
    }

    private void testRowOnCluster(String clusterUrl) throws SQLException {
        int id = intCounter.incrementAndGet();
        int v = 1000 + id;
        addRowToCluster(clusterUrl, tableName, id, v);
        readIdAndVerifyValue(tableName, id, v);
    }

    @Test
    public void testReadCluster1Down() throws Exception {
        int id = intCounter.incrementAndGet();
        int v = 1000 + id;
        addRowToCluster2(tableName, id, v);
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
            CLUSTERS.logClustersStates();
            readIdAndVerifyValue(tableName, id, v);
            // Read a non existent row
            readNonExistentRowAndVerify(tableName, intCounter.incrementAndGet());
            readNonExistentRowAndVerifyErrorOnSingleNull(tableName, intCounter.incrementAndGet());
        });
    }

    @Test
    public void testReadCluster2Down() throws Exception {
        int id = intCounter.incrementAndGet();
        int v = 1000 + id;
        addRowToCluster1(tableName, id, v);
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
            CLUSTERS.logClustersStates();
            readIdAndVerifyValue(tableName, id, v);
            readNonExistentRowAndVerify(tableName, intCounter.incrementAndGet());
            readNonExistentRowAndVerifyErrorOnSingleNull(tableName, intCounter.incrementAndGet());
        });
    }

    @Test
    public void testReadNonExistentRow() throws SQLException {
        readNonExistentRowAndVerify(tableName, intCounter.incrementAndGet());
    }

    private void addRowToCluster1(String tableName, int id, int v) throws SQLException {
        addRowToCluster(CLUSTERS.getUrl1(), tableName, id, v);
    }

    private void addRowToCluster2(String tableName, int id, int v) throws SQLException {
        addRowToCluster(CLUSTERS.getUrl2(), tableName, id, v);
    }

    private void addRowToCluster(String url, String tableName, int id, int v) throws SQLException {
        String jdbcUrl = CLUSTERS.getJdbcUrl(url);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, PROPERTIES)) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(String.format("UPSERT INTO %s VALUES(%d, %d)", tableName, id, v));
            conn.commit();
        }
    }

    private void readIdAndVerifyValue(String tableName, int id, int v) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, PROPERTIES)) {
            try (ResultSet rs =
                    conn.createStatement()
                            .executeQuery(String.format(selectFormat, tableName, id))) {
                assertTrue(rs.next());
                assertEquals(v, rs.getInt(1));
                assertTrue(rs instanceof ParallelPhoenixNullComparingResultSet);
            }
        }
    }

    private void readNonExistentRowAndVerify(String tableName, int id) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, PROPERTIES)) {
            try (ResultSet rs =
                    conn.createStatement()
                            .executeQuery(String.format(selectFormat, tableName, id))) {
                assertFalse(rs.next());
                assertTrue(rs instanceof ParallelPhoenixNullComparingResultSet);
            }
        }
    }

    private void readNonExistentRowAndVerifyErrorOnSingleNull(String tableName, int id)
            throws SQLException {
        Properties props = PropertiesUtil.deepCopy(PROPERTIES);
        props.setProperty(ParallelPhoenixNullComparingResultSet.ERROR_ON_SINGLE_NULL_ATTRIB,
            "true");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (ResultSet rs =
                    conn.createStatement()
                            .executeQuery(String.format(selectFormat, tableName, id))) {
                assertTrue(rs instanceof ParallelPhoenixNullComparingResultSet);
                try {
                    rs.next();
                    fail("RS should've errored on single null");
                } catch (SQLException e) {
                    LOG.debug("Exception", e);
                    assertEquals(e.getErrorCode(),
                        SQLExceptionCode.HA_READ_FROM_CLUSTER_FAILED_ON_NULL.getErrorCode());
                }
            }
        }
        assertEquals(1,GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER.getMetric().getValue());
    }
}
