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

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneHBaseDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneZKDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test failover basics for {@link HighAvailabilityTestingUtility}.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class HighAvailabilityTestingUtilityIT {
    private static final Logger LOG = LoggerFactory.getLogger(
            HighAvailabilityTestingUtilityIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();

    @Rule
    public TestName testName = new TestName();

    /** Client properties to create a connection per test. */
    private Properties clientProperties;
    /** JDBC connection string for this test HA group. */
    private String jdbcHAUrl;
    /** Failover HA group for to test. */
    private HighAvailabilityGroup haGroup;
    private String haGroupName;
    private final ClusterRoleRecord.RegistryType registryType;

    /** Table name per test case. */
    private String tableName;

    public HighAvailabilityTestingUtilityIT(ClusterRoleRecord.RegistryType registryType) {
        this.registryType = registryType;
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        CLUSTERS.close();
    }

    @Before
    public void setup() throws Exception {
        haGroupName = testName.getMethodName();
        clientProperties = HighAvailabilityTestingUtility.getHATestProperties();
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);

        // Make first cluster ACTIVE
        if (registryType == null) {
            CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
        } else {
            CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER, registryType);
        }

        tableName = RandomStringUtils.randomAlphabetic(10);
        jdbcHAUrl = CLUSTERS.getJdbcHAUrl();
        haGroup = getHighAvailibilityGroup(jdbcHAUrl,clientProperties);
        CLUSTERS.createTableOnClusterPair(haGroup, tableName);
    }

    @Parameterized.Parameters(name="ClusterRoleRecord_registryType={0}")
    public static Collection<Object> data() {
        return Arrays.asList(new Object[] {
                ClusterRoleRecord.RegistryType.ZK,
                ClusterRoleRecord.RegistryType.MASTER,
                ClusterRoleRecord.RegistryType.RPC,
                null //For Backward Compatibility
        });
    }

    /**
     * Test Phoenix connection creation and basic operations with HBase cluster(s) unavail.
     */
    @Test
    public void testClusterUnavailableNormalConnection() throws Exception {
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
            CLUSTERS.logClustersStates();
            try (Connection conn = CLUSTERS.getCluster1Connection(haGroup)) {
                doTestBasicOperationsWithConnection(conn, tableName, null);
            }
        });
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
            CLUSTERS.logClustersStates();
            try (Connection conn = CLUSTERS.getCluster2Connection(haGroup)) {
                doTestBasicOperationsWithConnection(conn, tableName, null);
            }
        });
    }

    /**
     * Test that replication works between HBase cluster(s).
     */
    @Test
    public void testClusterReplication() throws Exception {
        try (Connection conn = CLUSTERS.getClusterConnection(0, haGroup)) {
            doTestBasicOperationsWithConnection(conn, tableName, null);
        }

        CLUSTERS.checkReplicationComplete();

        try (Connection conn = CLUSTERS.getClusterConnection(1, haGroup);
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s",tableName))) {

            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    /**
     * Test that getting a new CQSI should fail when target cluster is failing.
     */
    @Test
    public void testGetNewCQSShouldFail() throws Exception {
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            try {
                Properties properties = HighAvailabilityTestingUtility.getHATestProperties();
                properties.setProperty(PHOENIX_HA_GROUP_ATTR, testName.getMethodName());
                ConnectionQueryServices cqs = PhoenixDriver.INSTANCE.getConnectionQueryServices(
                        CLUSTERS.getJdbcUrl1(haGroup), properties);
                fail("Should have failed since the target cluster is down, but got a CQS: " + cqs);
            } catch (Exception e) {
                LOG.info("Got expected exception since target cluster is down:", e);
            }
        });
    }

}
