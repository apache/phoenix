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

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.RET_SUCCESS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ZK_URL_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ZK_URL_2;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_URL_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_URL_2;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_ROLE_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_ROLE_2;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.POLICY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VERSION;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;
import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.replication.reader.ReplicationLogReplay;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for {@link PhoenixHAAdminTool}.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixHAAdminToolIT extends BaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixHAAdminToolIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
    private static final PrintStream STDOUT = System.out;
    private static final ByteArrayOutputStream STDOUT_CAPTURE = new ByteArrayOutputStream();
    private static final Long BUFFER_TIME_IN_MS = 100L;

    private String haGroupName;
    private PhoenixHAAdminTool adminTool;

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final TestWatcher testWatcher = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            // Print captured stdout if test failed and there's content
            String capturedOutput = STDOUT_CAPTURE.toString();
            if (!capturedOutput.isEmpty()) {
                System.err.println("\n=============== CAPTURED STDOUT (Test Failed) ===============");
                System.err.println(capturedOutput);
                System.err.println("=============================================================\n");
            }
        }
    };

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        CLUSTERS.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        CLUSTERS.close();
    }

    @Before
    public void setup() throws Exception {
        // Clean up all HA group records from both clusters before starting test
        try {
            String zkUrl1 = CLUSTERS.getZkUrl1();
            String zkUrl2 = CLUSTERS.getZkUrl2();

            // Clean up cluster 1
            try (PhoenixHAAdmin haAdmin1 = new PhoenixHAAdmin(
                    CLUSTERS.getHBaseCluster1().getConfiguration(),
                    ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

                List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl1);
                for (String name : haGroupNames) {
                    try {
                        // Delete from ZK
                        haAdmin1.deleteHAGroupStoreRecordInZooKeeper(name);
                        // Delete from system table
                        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(name, zkUrl1);
                    } catch (Exception e) {
                        LOG.warn("Failed to cleanup HA group: " + name, e);
                    }
                }
            }

            // Clean up cluster 2
            try (PhoenixHAAdmin haAdmin2 = new PhoenixHAAdmin(
                    CLUSTERS.getHBaseCluster2().getConfiguration(),
                    ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

                List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl2);
                for (String name : haGroupNames) {
                    try {
                        // Delete from ZK
                        haAdmin2.deleteHAGroupStoreRecordInZooKeeper(name);
                        // Delete from system table
                        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(name, zkUrl2);
                    } catch (Exception e) {
                        LOG.warn("Failed to cleanup HA group from cluster 2: " + name, e);
                    }
                }
            }

        } catch (Exception e) {
            LOG.warn("Error during cleanup", e);
            // Don't fail the test due to cleanup errors
        }
        
        standbyUri = testFolder.getRoot().toURI();
        // Set the required configuration for ReplicationLogReplay
        CLUSTERS.getHBaseCluster2().getConfiguration().set(ReplicationLogReplay.REPLICATION_LOG_REPLAY_HDFS_URL_KEY,
                standbyUri.toString());

        adminTool = new PhoenixHAAdminTool();
        adminTool.setConf(CLUSTERS.getHBaseCluster1().getConfiguration());

        haGroupName = testName.getMethodName();

        // Insert multiple HAGroupStoreRecords into the SYSTEM.HA_GROUP table with completely unique names
        // Using distinct names without common prefixes to avoid validation issues

        // Group 1 - Active cluster group
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(
                "prod_cluster_alpha",
                CLUSTERS.getZkUrl1(),
                CLUSTERS.getZkUrl2(),
                CLUSTERS.getMasterAddress1(),
                CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE,
                ClusterRoleRecord.ClusterRole.STANDBY,
                null);

        // Group 2 - Standby cluster group
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(
                "disaster_recovery_beta",
                CLUSTERS.getZkUrl1(),
                CLUSTERS.getZkUrl2(),
                CLUSTERS.getMasterAddress1(),
                CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.STANDBY,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                null);

        // Group 3 - Test-specific group (uses test method name for uniqueness across test runs)
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(
                haGroupName,
                CLUSTERS.getZkUrl1(),
                CLUSTERS.getZkUrl2(),
                CLUSTERS.getMasterAddress1(),
                CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE,
                ClusterRoleRecord.ClusterRole.STANDBY,
                null);

        // Clear stdout capture
        STDOUT_CAPTURE.reset();
    }

    @After
    public void after() throws Exception {
        // reset STDOUT in case it was captured for testing
        System.setOut(STDOUT);

        // Clean up all HA group records from both clusters after each test
        try {
            String zkUrl1 = CLUSTERS.getZkUrl1();
            String zkUrl2 = CLUSTERS.getZkUrl2();

            // Clean up cluster 1
            try (PhoenixHAAdmin haAdmin1 = new PhoenixHAAdmin(
                    CLUSTERS.getHBaseCluster1().getConfiguration(),
                    ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

                // Delete all ZNodes in the namespace
                haAdmin1.getCurator().delete().quietly().deletingChildrenIfNeeded()
                        .forPath(toPath(""));
                LOG.info("Cleaned up all ZK records from cluster 1");

                // Delete all records from System Table
                HAGroupStoreTestUtil.deleteAllHAGroupRecordsInSystemTable(zkUrl1);
                LOG.info("Cleaned up all system table records from cluster 1");
            } catch (Exception e) {
                LOG.warn("Failed to cleanup cluster 1", e);
            }

            // Clean up cluster 2
            try (PhoenixHAAdmin haAdmin2 = new PhoenixHAAdmin(
                    CLUSTERS.getHBaseCluster2().getConfiguration(),
                    ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

                // Delete all ZNodes in the namespace
                haAdmin2.getCurator().delete().quietly().deletingChildrenIfNeeded()
                        .forPath(toPath(""));
                LOG.info("Cleaned up all ZK records from cluster 2");

                // Delete all records from System Table
                HAGroupStoreTestUtil.deleteAllHAGroupRecordsInSystemTable(zkUrl2);
                LOG.info("Cleaned up all system table records from cluster 2");
            } catch (Exception e) {
                LOG.warn("Failed to cleanup cluster 2", e);
            }

        } catch (Exception e) {
            LOG.warn("Error during @After cleanup", e);
            // Don't fail the test due to cleanup errors
        }
    }

    /**
     * Test that the list command shows HA groups correctly.
     */
    @Test(timeout = 180000)
    public void testListCommand() throws Exception {
        System.setOut(new PrintStream(STDOUT_CAPTURE));

        int ret = ToolRunner.run(adminTool, new String[]{"list"});

        assertEquals(RET_SUCCESS, ret);

        String output = STDOUT_CAPTURE.toString();
        LOG.info("Got stdout from list command: \n++++++++\n{}++++++++\n", output);

        // Verify the output contains all three HA group names with unique names
        assertOutputContainsHAGroupNames(output,
                "prod_cluster_alpha",
                "disaster_recovery_beta",
                haGroupName);

        // Verify the output contains all expected fields
        assertOutputContainsHAGroupFields(output);

        // Verify total count is shown
        assertTrue("Output should show total count",
                output.contains("Total:"));
    }

    /**
     * Test that the get command shows a single HA group correctly.
     */
    @Test(timeout = 180000)
    public void testGetCommand() throws Exception {
        System.setOut(new PrintStream(STDOUT_CAPTURE));

        // Test getting a specific group with a unique name
        int ret = ToolRunner.run(adminTool, new String[]{"get", "-g", "prod_cluster_alpha"});

        assertEquals(RET_SUCCESS, ret);

        String output = STDOUT_CAPTURE.toString();
        LOG.info("Got stdout from get command: \n++++++++\n{}++++++++\n", output);

        // Verify the output contains the requested HA group name
        assertTrue("Output should contain the requested HA group name",
                output.contains("prod_cluster_alpha"));

        // Verify the output contains all expected fields
        assertOutputContainsHAGroupFields(output);

        // Verify it does NOT contain other HA groups (since it's a get, not list)
        assertTrue("Output should not contain disaster_recovery_beta",
                !output.contains("disaster_recovery_beta"));

        // Only check for test method name if it's different from the requested group
        if (!haGroupName.equals("prod_cluster_alpha")) {
            assertTrue("Output should not contain test method name group",
                    !output.contains(haGroupName));
        }
    }

    /**
     * Helper method to query system table and get HA group record details.
     *
     * @param haGroupName the HA group name to query
     * @param zkUrl the ZooKeeper URL to connect to
     * @return SystemTableRecord containing the record details, or null if not found
     */
    private SystemTableRecord querySystemTable(String haGroupName, String zkUrl) throws Exception {
        String queryString = String.format("SELECT * FROM %s WHERE %s = '%s'",
                SYSTEM_HA_GROUP_NAME, HA_GROUP_NAME, haGroupName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(queryString)) {
            if (rs.next()) {
                return new SystemTableRecord(
                        rs.getString(HA_GROUP_NAME),
                        rs.getString(ZK_URL_1),
                        rs.getString(ZK_URL_2),
                        rs.getString(CLUSTER_URL_1),
                        rs.getString(CLUSTER_URL_2),
                        rs.getString(CLUSTER_ROLE_1),
                        rs.getString(CLUSTER_ROLE_2),
                        rs.getString(POLICY),
                        rs.getLong(VERSION)
                );
            }
        }
        return null;
    }

    /**
     * Helper class to hold system table record data for verification.
     */
    private static class SystemTableRecord {
        final String zkUrl1;
        final String zkUrl2;
        final String clusterUrl1;
        final String clusterUrl2;
        final String policy;
        final long version;

        SystemTableRecord(String haGroupName, String zkUrl1, String zkUrl2,
                         String clusterUrl1, String clusterUrl2,
                         String clusterRole1, String clusterRole2,
                         String policy, long version) {
            // Note: haGroupName, clusterRole1, and clusterRole2 are intentionally not stored
            // as they are not currently needed for verification
            this.zkUrl1 = zkUrl1;
            this.zkUrl2 = zkUrl2;
            this.clusterUrl1 = clusterUrl1;
            this.clusterUrl2 = clusterUrl2;
            this.policy = policy;
            this.version = version;
        }
    }

    /**
     * Helper method to assert output contains all expected HA group names.
     */
    private void assertOutputContainsHAGroupNames(String output, String... groupNames) {
        for (String groupName : groupNames) {
            assertTrue("Output should contain HA group name: " + groupName,
                    output.contains(groupName));
        }
    }

    /**
     * Helper method to wait for a cluster to reach a specific HA group state.
     * Uses polling with timeout to handle eventual consistency.
     *
     * @param manager the HAGroupStoreManager to query
     * @param haGroupName the HA group name
     * @param expectedState the expected HAGroupState
     * @param clusterDescription description for logging (e.g., "Cluster1", "Cluster2")
     * @throws Exception if timeout occurs or other errors
     */
    private void waitForHAGroupState(HAGroupStoreManager manager, String haGroupName,
            HAGroupStoreRecord.HAGroupState expectedState, String clusterDescription) throws Exception {
        waitFor(() -> {
            Optional<HAGroupStoreRecord> record;
            try {
                record = manager.getHAGroupStoreRecord(haGroupName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return record.isPresent() && record.get().getHAGroupState() == expectedState;
        }, 100, 30000);
        LOG.debug("{} reached expected state: {}", clusterDescription, expectedState);
    }

    /**
     * Helper method to assert output contains all expected HAGroupStoreRecord fields.
     */
    private void assertOutputContainsHAGroupFields(String output) {
        // Verify state information
        assertTrue("Output should contain state information",
                output.contains("ACTIVE_IN_SYNC") || output.contains("State") ||
                output.contains("STANDBY"));

        // Verify policy information
        assertTrue("Output should contain policy information",
                output.contains("FAILOVER") || output.contains("Policy"));

        // Verify cluster role information
        assertTrue("Output should contain cluster role information",
                output.contains("ACTIVE") || output.contains("STANDBY") ||
                output.contains("Cluster Role"));

        // Verify protocol version
        assertTrue("Output should contain protocol version",
                output.contains("1.0") || output.contains("Protocol Version"));

        // Verify admin version
        assertTrue("Output should contain admin version",
                output.contains("Admin Version") || output.contains("adminCRRVersion"));
    }

    /**
     * Test that the get-cluster-role-record command shows cluster role information correctly.
     * This test sets up both local cluster data (via System table) and peer cluster data (via ZK).
     */
    @Test(timeout = 180000)
    public void testGetClusterRoleRecordCommand() throws Exception {
        System.setOut(new PrintStream(STDOUT_CAPTURE));

        // Create peer ZK record using PhoenixHAAdmin
        try (PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(
                CLUSTERS.getHBaseCluster2().getConfiguration(),
                ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

            // Create a HAGroupStoreRecord in the peer cluster with STANDBY state
            HAGroupStoreRecord peerRecord = new HAGroupStoreRecord(
                    HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
                    haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY,
                    0L,
                    HighAvailabilityPolicy.FAILOVER.toString(),
                    CLUSTERS.getZkUrl1(),
                    CLUSTERS.getMasterAddress2(),
                    CLUSTERS.getMasterAddress1(),
                    1L);

            peerHaAdmin.createHAGroupStoreRecordInZooKeeper(peerRecord);

            // Wait for ZK event propagation
            Thread.sleep(5000);

            // Now execute the get-cluster-role-record command
            int ret = ToolRunner.run(adminTool, new String[]{"get-cluster-role-record", "-g", haGroupName});

            assertEquals(RET_SUCCESS, ret);

            String output = STDOUT_CAPTURE.toString();
            LOG.info("Got stdout from get-cluster-role-record command: \n++++++++\n{}++++++++\n", output);

            // Verify the output contains cluster role record information
            assertTrue("Output should contain HA group name",
                    output.contains(haGroupName));

            // Verify policy is shown
            assertTrue("Output should contain policy (FAILOVER)",
                    output.contains("FAILOVER"));

            // Verify cluster 1 and cluster 2 information is present
            assertTrue("Output should contain Cluster 1 URL",
                    output.contains("Cluster 1 URL"));
            assertTrue("Output should contain Cluster 1 Role",
                    output.contains("Cluster 1 Role"));
            assertTrue("Output should contain Cluster 2 URL",
                    output.contains("Cluster 2 URL"));
            assertTrue("Output should contain Cluster 2 Role",
                    output.contains("Cluster 2 Role"));

            // CRITICAL: Verify roles are NOT swapped by checking via ClusterRoleRecord.getRole(url)
            // The ClusterRoleRecord may have url1/url2 in any order, so we need to query by URL
            HAGroupStoreManager manager = new HAGroupStoreManager(adminTool.getConf());
            ClusterRoleRecord clusterRoleRecord = manager.getClusterRoleRecord(haGroupName);

            // Local cluster (zkUrl1) should be ACTIVE
            ClusterRoleRecord.ClusterRole localRole = clusterRoleRecord.getRole(CLUSTERS.getMasterAddress1());
            assertEquals("Local cluster (zkUrl1) should have ACTIVE role",
                    ClusterRoleRecord.ClusterRole.ACTIVE, localRole);

            // Peer cluster (zkUrl2) should be STANDBY
            ClusterRoleRecord.ClusterRole peerRole = clusterRoleRecord.getRole(CLUSTERS.getMasterAddress2());
            assertEquals("Peer cluster (zkUrl2) should have STANDBY role",
                    ClusterRoleRecord.ClusterRole.STANDBY, peerRole);

            // Verify the output text shows both URLs (regardless of order)
            assertTrue("Output should contain local masterAddress1",
                    output.contains(CLUSTERS.getMasterAddress1()));
            assertTrue("Output should contain peer masterAddress2",
                    output.contains(CLUSTERS.getMasterAddress2()));

            // Verify output contains both role types
            assertTrue("Output should contain ACTIVE role",
                    output.contains("ACTIVE"));
            assertTrue("Output should contain STANDBY role",
                    output.contains("STANDBY"));

            // Verify version information
            assertTrue("Output should contain Version",
                    output.contains("Version"));

            // Clean up: delete the peer ZK record
            peerHaAdmin.deleteHAGroupStoreRecordInZooKeeper(haGroupName);
        }
    }

    /**
     * Test that the initiate-failover command triggers failover correctly.
     * This test verifies the complete failover flow with automatic state transitions.
     */
    @Test(timeout = 180000)
    public void testInitiateFailoverCommand() throws Exception {
        // Use a specific HA group name for this test
        String failoverHaGroupName = "testInitiateFailover_" + System.currentTimeMillis();

        String zkUrl1 = CLUSTERS.getZkUrl1();
        String zkUrl2 = CLUSTERS.getZkUrl2();

        // Set up system tables for both clusters
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(failoverHaGroupName, zkUrl1, zkUrl2,
                CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(failoverHaGroupName, zkUrl1, zkUrl2,
                CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, zkUrl2);

        // Create HAGroupStoreManager instances for both clusters
        // This will automatically setup failover management and create ZNodes from system table
        HAGroupStoreManager cluster1HAManager = new HAGroupStoreManager(CLUSTERS.getHBaseCluster1().getConfiguration());
        HAGroupStoreManager cluster2HAManager = new HAGroupStoreManager(CLUSTERS.getHBaseCluster2().getConfiguration());

        // Initialize HAGroupStoreClient and move to ACTIVE_IN_SYNC state
        cluster1HAManager.getHAGroupStoreRecord(failoverHaGroupName);
        cluster2HAManager.getHAGroupStoreRecord(failoverHaGroupName);

        // Wait for ZK session timeout to allow transition from ACTIVE_NOT_IN_SYNC to ACTIVE_IN_SYNC
        long waitTime = cluster1HAManager.setHAGroupStatusToSync(failoverHaGroupName);
        Thread.sleep(waitTime + BUFFER_TIME_IN_MS);
        assertEquals("Wait time should be 0",
                0,
                cluster1HAManager.setHAGroupStatusToSync(failoverHaGroupName));
        
        // Start the ReplicationLogReplay
        ReplicationLogReplay replicationLogReplay = ReplicationLogReplay.get(CLUSTERS.getHBaseCluster2().getConfiguration(),
                failoverHaGroupName);
        replicationLogReplay.startReplay();

        // === INITIAL STATE VERIFICATION ===
        waitForHAGroupState(cluster1HAManager, failoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, "Cluster1");
        waitForHAGroupState(cluster2HAManager, failoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY, "Cluster2");
        LOG.info("Initial state verified: Cluster1=ACTIVE_IN_SYNC, Cluster2=STANDBY");

        // === EXECUTE INITIATE-FAILOVER COMMAND ===
        System.setOut(new PrintStream(STDOUT_CAPTURE));

        PhoenixHAAdminTool cluster1AdminTool = new PhoenixHAAdminTool();
        cluster1AdminTool.setConf(CLUSTERS.getHBaseCluster1().getConfiguration());

        // Use a timeout of 60 seconds for test (shorter than default 120)
        int ret = ToolRunner.run(cluster1AdminTool,
                new String[]{"initiate-failover", "-g", failoverHaGroupName, "-t", "180"});

        assertEquals("initiate-failover command should succeed", RET_SUCCESS, ret);

        String output = STDOUT_CAPTURE.toString();
        LOG.info("Got stdout from initiate-failover command: \n++++++++\n{}++++++++\n", output);

        // Verify command output indicates completion
        assertTrue("Output should indicate failover completed successfully",
                output.contains("Failover completed successfully") ||
                output.contains("Transition completed"));

        // === FINAL STATE VERIFICATION ===
        waitForHAGroupState(cluster1HAManager, failoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY, "Cluster1");
        waitForHAGroupState(cluster2HAManager, failoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, "Cluster2");
        LOG.info("Final state verified: Cluster1=STANDBY, Cluster2=ACTIVE_IN_SYNC");

        // Verify cluster role records show correct role swap
        // Note: url1/url2 in ClusterRoleRecord can be in any order, so we query by specific URL
        ClusterRoleRecord cluster1Role = cluster1HAManager.getClusterRoleRecord(failoverHaGroupName);
        ClusterRoleRecord cluster2Role = cluster2HAManager.getClusterRoleRecord(failoverHaGroupName);

        // After failover, zkUrl1 (originally ACTIVE) should now be STANDBY
        ClusterRoleRecord.ClusterRole cluster1RoleForZkUrl1 = cluster1Role.getRole(CLUSTERS.getMasterAddress1());
        assertEquals("Cluster1 view: zkUrl1 should now have STANDBY role after failover",
                ClusterRoleRecord.ClusterRole.STANDBY, cluster1RoleForZkUrl1);

        // After failover, zkUrl2 (originally STANDBY) should now be ACTIVE
        ClusterRoleRecord.ClusterRole cluster2RoleForZkUrl2 = cluster2Role.getRole(CLUSTERS.getMasterAddress2());
        assertEquals("Cluster2 view: zkUrl2 should now have ACTIVE role after failover",
                ClusterRoleRecord.ClusterRole.ACTIVE, cluster2RoleForZkUrl2);

        LOG.info("✓ Failover completed successfully: roles swapped from Cluster1=ACTIVE to Cluster2=ACTIVE");

        // Clean up
        replicationLogReplay.stopReplay();
//        cluster2HAManager.unsubscribeFromTargetState(failoverHaGroupName,
//                HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE, ClusterType.LOCAL, listener);
    }

    /**
     * Test that the abort-failover command aborts an ongoing failover correctly.
     * This test verifies the abort failover flow with automatic state transitions.
     */
    @Test(timeout = 180000)
    public void testAbortFailoverCommand() throws Exception {
        // Use a specific HA group name for this test
        String abortFailoverHaGroupName = "testAbortFailover_" + System.currentTimeMillis();

        String zkUrl1 = CLUSTERS.getZkUrl1();
        String zkUrl2 = CLUSTERS.getZkUrl2();

        // Set up system tables for both clusters
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(abortFailoverHaGroupName, zkUrl1, zkUrl2,
                CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(abortFailoverHaGroupName, zkUrl1, zkUrl2,
                CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, zkUrl2);

        // Create HAGroupStoreManager instances for both clusters
        HAGroupStoreManager cluster1HAManager = new HAGroupStoreManager(CLUSTERS.getHBaseCluster1().getConfiguration());
        HAGroupStoreManager cluster2HAManager = new HAGroupStoreManager(CLUSTERS.getHBaseCluster2().getConfiguration());

        // Initialize HAGroupStoreClient and move to ACTIVE_IN_SYNC state
        cluster1HAManager.getHAGroupStoreRecord(abortFailoverHaGroupName);
        cluster2HAManager.getHAGroupStoreRecord(abortFailoverHaGroupName);

        // Wait for ZK session timeout to allow transition from ACTIVE_NOT_IN_SYNC to ACTIVE_IN_SYNC
        long waitTime = cluster1HAManager.setHAGroupStatusToSync(abortFailoverHaGroupName);
        Thread.sleep(waitTime + BUFFER_TIME_IN_MS);
        assertEquals("Wait time should be 0",
                0,
                cluster1HAManager.setHAGroupStatusToSync(abortFailoverHaGroupName));

        // === INITIAL STATE VERIFICATION ===
        waitForHAGroupState(cluster1HAManager, abortFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, "Cluster1");
        waitForHAGroupState(cluster2HAManager, abortFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY, "Cluster2");
        LOG.info("Initial state verified: Cluster1=ACTIVE_IN_SYNC, Cluster2=STANDBY");

        // === STEP 1: INITIATE FAILOVER ON CLUSTER1 (ACTIVE) ===
        cluster1HAManager.initiateFailoverOnActiveCluster(abortFailoverHaGroupName);

        // Wait for cluster1 to transition to ACTIVE_IN_SYNC_TO_STANDBY state
        waitForHAGroupState(cluster1HAManager, abortFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, "Cluster1");
        LOG.info("Failover initiated: Cluster1=ACTIVE_IN_SYNC_TO_STANDBY");

        // === STEP 2: VERIFY AUTOMATIC PEER REACTION ===
        // Cluster2 (standby) should automatically move to STANDBY_TO_ACTIVE
        waitForHAGroupState(cluster2HAManager, abortFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE, "Cluster2");
        LOG.info("Peer reaction verified: Cluster2=STANDBY_TO_ACTIVE");

        // === STEP 3: EXECUTE ABORT-FAILOVER COMMAND ON CLUSTER2 ===
        System.setOut(new PrintStream(STDOUT_CAPTURE));

        PhoenixHAAdminTool cluster2AdminTool = new PhoenixHAAdminTool();
        cluster2AdminTool.setConf(CLUSTERS.getHBaseCluster2().getConfiguration());

        // Use a timeout of 60 seconds for test (shorter than default 120)
        int ret = ToolRunner.run(cluster2AdminTool,
                new String[]{"abort-failover", "-g", abortFailoverHaGroupName, "-t", "60"});

        assertEquals("abort-failover command should succeed", RET_SUCCESS, ret);

        String output = STDOUT_CAPTURE.toString();
        LOG.info("Got stdout from abort-failover command: \n++++++++\n{}++++++++\n", output);

        // Verify command output indicates completion
        assertTrue("Output should indicate abort completed successfully",
                output.contains("Failover abort completed successfully") ||
                output.contains("Transition completed"));

        // === FINAL VERIFICATION ===
        // Wait for both clusters to reach final state
        waitForHAGroupState(cluster1HAManager, abortFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, "Cluster1");
        waitForHAGroupState(cluster2HAManager, abortFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY, "Cluster2");

        // Verify cluster role records are back to original - use getRole(url) to handle any order
        ClusterRoleRecord cluster1Role = cluster1HAManager.getClusterRoleRecord(abortFailoverHaGroupName);
        ClusterRoleRecord cluster2Role = cluster2HAManager.getClusterRoleRecord(abortFailoverHaGroupName);

        // Cluster1 view: zkUrl1 should have ACTIVE role
        ClusterRoleRecord.ClusterRole cluster1RoleForZkUrl1 = cluster1Role.getRole(CLUSTERS.getMasterAddress1());
        assertEquals("Cluster1 view: zkUrl1 should have ACTIVE role (back to original)",
                ClusterRoleRecord.ClusterRole.ACTIVE, cluster1RoleForZkUrl1);

        // Cluster2 view: zkUrl2 should have STANDBY role
        ClusterRoleRecord.ClusterRole cluster2RoleForZkUrl2 = cluster2Role.getRole(CLUSTERS.getMasterAddress2());
        assertEquals("Cluster2 view: zkUrl2 should have STANDBY role (back to original)",
                ClusterRoleRecord.ClusterRole.STANDBY, cluster2RoleForZkUrl2);

        LOG.info("✓ Abort failover completed successfully: roles remain Cluster1=ACTIVE, Cluster2=STANDBY");
    }

    /**
     * Test that the initiate-failover command times out when the standby cluster doesn't
     * complete the transition. This simulates a stuck failover scenario.
     */
    @Test(timeout = 180000)
    public void testInitiateFailoverCommandTimeout() throws Exception {
        // Use a specific HA group name for this test
        String timeoutFailoverHaGroupName = "testInitiateFailoverTimeout_" + System.currentTimeMillis();

        String zkUrl1 = CLUSTERS.getZkUrl1();
        String zkUrl2 = CLUSTERS.getZkUrl2();

        // Set up system tables for both clusters
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(timeoutFailoverHaGroupName, zkUrl1, zkUrl2,
                CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(timeoutFailoverHaGroupName, zkUrl1, zkUrl2,
                CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, zkUrl2);

        // Create HAGroupStoreManager instances for both clusters
        HAGroupStoreManager cluster1HAManager = new HAGroupStoreManager(CLUSTERS.getHBaseCluster1().getConfiguration());
        HAGroupStoreManager cluster2HAManager = new HAGroupStoreManager(CLUSTERS.getHBaseCluster2().getConfiguration());

        // Initialize HAGroupStoreClient and move to ACTIVE_IN_SYNC state
        cluster1HAManager.getHAGroupStoreRecord(timeoutFailoverHaGroupName);
        cluster2HAManager.getHAGroupStoreRecord(timeoutFailoverHaGroupName);

        // Wait for ZK session timeout to allow transition from ACTIVE_NOT_IN_SYNC to ACTIVE_IN_SYNC
        long waitTime = cluster1HAManager.setHAGroupStatusToSync(timeoutFailoverHaGroupName);
        Thread.sleep(waitTime);
        assertEquals("Wait time should be 0",
                0,
                cluster1HAManager.setHAGroupStatusToSync(timeoutFailoverHaGroupName));

        // === INITIAL STATE VERIFICATION ===
        waitForHAGroupState(cluster1HAManager, timeoutFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, "Cluster1");
        waitForHAGroupState(cluster2HAManager, timeoutFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY, "Cluster2");
        LOG.info("Initial state verified: Cluster1=ACTIVE_IN_SYNC, Cluster2=STANDBY");

        // === KEY DIFFERENCE: DO NOT SET UP LISTENER ===
        // Unlike testInitiateFailoverCommand, we intentionally don't set up the listener
        // that would transition cluster2 from STANDBY_TO_ACTIVE to ACTIVE_IN_SYNC
        // This simulates a stuck failover where the standby doesn't complete the transition
        LOG.info("NOT setting up listener - simulating stuck failover where reader doesn't react");

        // === EXECUTE INITIATE-FAILOVER COMMAND WITH SHORT TIMEOUT ===
        System.setOut(new PrintStream(STDOUT_CAPTURE));

        PhoenixHAAdminTool cluster1AdminTool = new PhoenixHAAdminTool();
        cluster1AdminTool.setConf(CLUSTERS.getHBaseCluster1().getConfiguration());

        // Use a short timeout (15 seconds) to avoid long test runtime
        int ret = ToolRunner.run(cluster1AdminTool,
                new String[]{"initiate-failover", "-g", timeoutFailoverHaGroupName, "-t", "15"});

        // Command should fail due to timeout
        assertEquals("initiate-failover command should return error due to timeout",
                PhoenixHAAdminTool.RET_UPDATE_ERROR, ret);

        String output = STDOUT_CAPTURE.toString();
        LOG.info("Got stdout from initiate-failover command (timeout expected): \n++++++++\n{}++++++++\n", output);

        // Verify command output indicates timeout
        assertTrue("Output should indicate transition incomplete",
                output.contains("Failover transition incomplete") ||
                output.contains("Timeout"));

        // === VERIFY INTERMEDIATE STATE ===
        // Cluster1 should be stuck in ACTIVE_IN_SYNC_TO_STANDBY (not reached STANDBY)
        waitForHAGroupState(cluster1HAManager, timeoutFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, "Cluster1");

        // Cluster2 should have reacted and moved to STANDBY_TO_ACTIVE but NOT to ACTIVE_IN_SYNC
        waitForHAGroupState(cluster2HAManager, timeoutFailoverHaGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE, "Cluster2");

        LOG.info("Timeout behavior verified: Cluster1=ACTIVE_IN_SYNC_TO_STANDBY (stuck), Cluster2=STANDBY_TO_ACTIVE (stuck)");

        // Verify cluster role records show transition states (not final states)
        ClusterRoleRecord cluster1Role = cluster1HAManager.getClusterRoleRecord(timeoutFailoverHaGroupName);
        ClusterRoleRecord cluster2Role = cluster2HAManager.getClusterRoleRecord(timeoutFailoverHaGroupName);

        // Cluster1 should show ACTIVE_TO_STANDBY (transitioning)
        ClusterRoleRecord.ClusterRole cluster1RoleForZkUrl1 = cluster1Role.getRole(CLUSTERS.getMasterAddress1());
        assertEquals("Cluster1 view: zkUrl1 should have ACTIVE_TO_STANDBY role (transitioning)",
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, cluster1RoleForZkUrl1);

        // Cluster2 should show STANDBY_TO_ACTIVE (transitioning)
        ClusterRoleRecord.ClusterRole cluster2RoleForZkUrl2 = cluster2Role.getRole(CLUSTERS.getMasterAddress2());
        assertEquals("Cluster2 view: zkUrl2 should have STANDBY_TO_ACTIVE role (transitioning)",
                ClusterRoleRecord.ClusterRole.STANDBY_TO_ACTIVE, cluster2RoleForZkUrl2);

        LOG.info("✓ Timeout test completed successfully: both clusters stuck in transition states as expected");
    }

    /**
     * Test that the update command successfully updates HA group configuration.
     * This test updates multiple attributes using auto-increment version.
     */
    @Test(timeout = 180000)
    public void testUpdateCommand() throws Exception {
        // Use the test-specific HA group created in @Before
        String updateHaGroupName = haGroupName;

        // The system table record was already created in @Before
        // Use HAGroupStoreManager to initialize the ZK record from system table
        HAGroupStoreManager manager = new HAGroupStoreManager(CLUSTERS.getHBaseCluster1().getConfiguration());

        // Get the record - this will automatically create ZK record from system table
        Optional<HAGroupStoreRecord> initialRecordOpt = manager.getHAGroupStoreRecord(updateHaGroupName);
        assertTrue("Initial record should exist", initialRecordOpt.isPresent());

        HAGroupStoreRecord initialRecord = initialRecordOpt.get();
        long initialVersion = initialRecord.getAdminCRRVersion();

        LOG.info("Initial record loaded with version {}", initialVersion);

        // === EXECUTE UPDATE COMMAND - UPDATE MULTIPLE FIELDS ===
        System.setOut(new PrintStream(STDOUT_CAPTURE));

        // Update peer ZK URL, cluster URL, and peer cluster URL using auto-increment version
        String newPeerZkUrl = "localhost:9999:/test";
        String newClusterUrl = "localhost:16020,localhost:16021,localhost:16022";
        String newPeerClusterUrl = "localhost:16030,localhost:16031,localhost:16032";

        int ret = ToolRunner.run(adminTool,
                new String[]{"update", "-g", updateHaGroupName,
                    "-pz", newPeerZkUrl,
                    "-c", newClusterUrl,
                    "-pc", newPeerClusterUrl,
                    "-av"});

        assertEquals("update command should succeed", RET_SUCCESS, ret);

        String output = STDOUT_CAPTURE.toString();
        LOG.info("Got stdout from update command: \n++++++++\n{}++++++++\n", output);

        // Verify command output
        assertTrue("Output should indicate update completed",
                output.contains("Update completed successfully") ||
                output.contains("✓"));
        assertTrue("Output should show version increment",
                output.contains("Admin Version") || output.contains("->"));
        assertTrue("Output should show peer ZK URL change",
                output.contains(newPeerZkUrl));
        assertTrue("Output should show cluster URL change",
                output.contains(newClusterUrl));
        assertTrue("Output should show peer cluster URL change",
                output.contains(newPeerClusterUrl));

        // === VERIFY UPDATE IN ZK ===
        Thread.sleep(2000);

        // Read updated record from ZK
        Optional<HAGroupStoreRecord> updatedRecordOpt = manager.getHAGroupStoreRecord(updateHaGroupName);
        assertTrue("Record should exist after update", updatedRecordOpt.isPresent());
        HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();

        // Verify all updates were applied in ZK
        assertEquals("Version should be incremented",
            initialVersion + 1, updatedRecord.getAdminCRRVersion());
        assertEquals("Peer ZK URL should be updated",
            newPeerZkUrl, updatedRecord.getPeerZKUrl());
        assertEquals("Cluster URL should be updated",
            newClusterUrl, updatedRecord.getClusterUrl());
        assertEquals("Peer cluster URL should be updated",
            newPeerClusterUrl, updatedRecord.getPeerClusterUrl());

        // Verify fields that should remain unchanged in ZK
        assertEquals("State should remain unchanged",
            initialRecord.getHAGroupState(), updatedRecord.getHAGroupState());
        assertEquals("Policy should remain unchanged",
            initialRecord.getPolicy(), updatedRecord.getPolicy());
        assertEquals("Protocol version should remain unchanged",
            initialRecord.getProtocolVersion(), updatedRecord.getProtocolVersion());

        LOG.info("✓ ZK verification passed: multiple fields updated, version incremented");

        // === VERIFY UPDATE IN SYSTEM TABLE ===
        SystemTableRecord systemRecord = querySystemTable(updateHaGroupName, CLUSTERS.getZkUrl1());
        assertNotNull("System table record should exist", systemRecord);

        // Verify version was incremented in system table
        assertEquals("System table version should be incremented",
            initialVersion + 1, systemRecord.version);

        // Verify updated URLs are persisted in system table
        // Note: The system table stores zkUrl1/zkUrl2 and clusterUrl1/clusterUrl2 in a specific order
        // We need to check which position corresponds to local cluster vs peer cluster
        String localZkUrl = JDBCUtil.formatUrl(CLUSTERS.getZkUrl1());
        boolean localIsZkUrl1 = localZkUrl.equals(JDBCUtil.formatUrl(systemRecord.zkUrl1));
        if (localIsZkUrl1) {
            // Local cluster is in position 1, peer is in position 2
            assertEquals("System table ZK_URL_1 should remain unchanged (local cluster)",
                JDBCUtil.formatUrl(localZkUrl), JDBCUtil.formatUrl(systemRecord.zkUrl1));
            assertEquals("System table ZK_URL_2 should be updated (peer cluster)",
                newPeerZkUrl, systemRecord.zkUrl2);
            assertEquals("System table CLUSTER_URL_1 should be updated (local cluster)",
                newClusterUrl, systemRecord.clusterUrl1);
            assertEquals("System table CLUSTER_URL_2 should be updated (peer cluster)",
                newPeerClusterUrl, systemRecord.clusterUrl2);
        } else {
            // Local cluster is in position 2, peer is in position 1
            assertEquals("System table ZK_URL_2 should remain unchanged (local cluster)",
                    JDBCUtil.formatUrl(localZkUrl), JDBCUtil.formatUrl(systemRecord.zkUrl2));
            assertEquals("System table ZK_URL_1 should be updated (peer cluster)",
                newPeerZkUrl, systemRecord.zkUrl1);
            assertEquals("System table CLUSTER_URL_2 should be updated (local cluster)",
                newClusterUrl, systemRecord.clusterUrl2);
            assertEquals("System table CLUSTER_URL_1 should be updated (peer cluster)",
                newPeerClusterUrl, systemRecord.clusterUrl1);
        }

        // Verify policy and roles remain unchanged in system table
        assertEquals("System table policy should remain unchanged",
            initialRecord.getPolicy(), systemRecord.policy);

        LOG.info("✓ System table verification passed: all fields updated correctly, version incremented");
        LOG.info("✓ Update test completed successfully: ZK and System Table both updated correctly");
    }
}

