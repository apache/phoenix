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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.util.Map;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Integration test for mutation blocking functionality in IndexRegionObserver.preBatchMutate()
 * Tests that MutationBlockedIOException is properly thrown when cluster role-based mutation
 * blocking is enabled and CRRs are in ACTIVE_TO_STANDBY state.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexRegionObserverMutationBlockingIT extends BaseTest {

    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 1000L;
    private PhoenixHAAdmin haAdmin;
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // Enable cluster role-based mutation blocking
        props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "true");
        // No retries so that tests fail faster.
        props.put("hbase.client.retries.number", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        CLUSTERS.start();
    }

    @Before
    public void setUp() throws Exception {
        haAdmin = new PhoenixHAAdmin(config, ZK_CONSISTENT_HA_NAMESPACE);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        String zkUrl = getLocalZkUrl(config);
        String peerZkUrl = CLUSTERS.getZkUrl2();
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), zkUrl, peerZkUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
    }

    @Test
    public void testMutationBlockedOnDataTableWithIndex() throws Exception {
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();
        String haGroupName = testName.getMethodName();

        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            conn.setHAGroupName(haGroupName);
            // Create data table and index
            conn.createStatement().execute("CREATE TABLE " + dataTableName +
                    " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
            conn.createStatement().execute("CREATE INDEX " + indexName +
                    " ON " + dataTableName + "(name)");

            // Initially, mutations should work - verify baseline
            conn.createStatement().execute("UPSERT INTO " + dataTableName +
                    " VALUES ('1', 'John', 25)");
            conn.commit();

            // Set up HAGroupStoreRecord that will block mutations (ACTIVE_TO_STANDBY state)
            HAGroupStoreRecord haGroupStoreRecord
                    = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
                    haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);
            haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);

            // Wait for the event to propagate
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

            // Test that UPSERT throws MutationBlockedIOException
            try {
                conn.createStatement().execute("UPSERT INTO " + dataTableName +
                        " VALUES ('2', 'Jane', 30)");
                conn.commit();
                fail("Expected MutationBlockedIOException to be thrown");
            } catch (CommitException e) {
                // Verify the exception chain contains MutationBlockedIOException
                assertTrue("Expected MutationBlockedIOException in exception chain",
                        containsMutationBlockedException(e));
            }
        }
    }


    @Test
    public void testMutationAllowedWhenNotBlocked() throws Exception {
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();

        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            conn.setHAGroupName(testName.getMethodName());
            // Create data table and index
            conn.createStatement().execute("CREATE TABLE " + dataTableName +
                    " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
            conn.createStatement().execute("CREATE INDEX " + indexName +
                    " ON " + dataTableName + "(name)");

            // Mutations should work fine
            conn.createStatement().execute("UPSERT INTO " + dataTableName +
                    " VALUES ('1', 'Bob', 35)");
            conn.createStatement().execute("UPSERT INTO " + dataTableName +
                    " VALUES ('2', 'Carol', 27)");
            conn.commit();

            // Verify data was inserted successfully
            try (java.sql.ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT COUNT(*) FROM " + dataTableName)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    public void testMutationBlockingTransition() throws Exception {
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();
        String haGroupName = testName.getMethodName();

        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            conn.setHAGroupName(testName.getMethodName());
            // Create data table and index
            conn.createStatement().execute("CREATE TABLE " + dataTableName +
                    " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
            conn.createStatement().execute("CREATE INDEX " + indexName +
                    " ON " + dataTableName + "(name)");

            // Mutation should work
            conn.createStatement().execute("UPSERT INTO " + dataTableName +
                    " VALUES ('1', 'David', 40)");
            conn.commit();
            // Set up HAGroupStoreRecord that will block mutations (ACTIVE_TO_STANDBY state)
            HAGroupStoreRecord haGroupStoreRecord
                    = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
                    haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);
            haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

            // Now mutations should be blocked
            try {
                conn.createStatement().execute("UPSERT INTO " + dataTableName +
                        " VALUES ('2', 'Eve', 33)");
                conn.commit();
                fail("Expected MutationBlockedIOException after transition to ACTIVE_TO_STANDBY");
            } catch (CommitException e) {
                assertTrue("Expected mutation blocked exception after transition",
                        containsMutationBlockedException(e));
            }

            // Transition back to ACTIVE (non-blocking state) and peer cluster is in ATS state
            // Set up HAGroupStoreRecord that will block mutations (ACTIVE_TO_STANDBY state)
            haGroupStoreRecord
                    = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
                    haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
            haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

            // Mutations should work again
            conn.createStatement().execute("UPSERT INTO " + dataTableName +
                    " VALUES ('3', 'Frank', 45)");
            conn.commit();
        }
    }

    private boolean containsMutationBlockedException(CommitException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RetriesExhaustedWithDetailsException) {
            RetriesExhaustedWithDetailsException re = (RetriesExhaustedWithDetailsException) cause;
            return re.getCause(0) instanceof MutationBlockedIOException;
        }
        return false;
    }
}