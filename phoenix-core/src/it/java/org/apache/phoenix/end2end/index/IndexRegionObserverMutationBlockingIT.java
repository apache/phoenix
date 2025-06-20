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

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;    

/**
 * Integration test for mutation blocking functionality in IndexRegionObserver.preBatchMutate()
 * Tests that MutationBlockedIOException is properly thrown when cluster role-based mutation 
 * blocking is enabled and CRRs are in ACTIVE_TO_STANDBY state.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexRegionObserverMutationBlockingIT extends BaseTest {
    
    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 1000L;
    private PhoenixHAAdmin haAdmin;
    private HAGroupStoreManager haGroupStoreManager;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // Enable cluster role-based mutation blocking
        props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "true");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void setUp() throws Exception {
        haAdmin = new PhoenixHAAdmin(config);
        haGroupStoreManager = HAGroupStoreManager.getInstance(config);
        
        // Clean up all existing CRRs before each test
        List<ClusterRoleRecord> crrs = haAdmin.listAllClusterRoleRecordsOnZookeeper();
        for (ClusterRoleRecord crr : crrs) {
            haAdmin.getCurator().delete().forPath(toPath(crr.getHaGroupName()));
        }
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    }

    @After
    public void tearDown() throws Exception {
        // Clean up CRRs after each test
        if (haAdmin != null) {
            List<ClusterRoleRecord> crrs = haAdmin.listAllClusterRoleRecordsOnZookeeper();
            for (ClusterRoleRecord crr : crrs) {
                haAdmin.getCurator().delete().forPath(toPath(crr.getHaGroupName()));
            }
        }
    }

    @Test
    public void testMutationBlockedOnDataTableWithIndex() throws Exception {
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();
        
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create data table and index
            conn.createStatement().execute("CREATE TABLE " + dataTableName + 
                " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
            conn.createStatement().execute("CREATE INDEX " + indexName + 
                " ON " + dataTableName + "(name)");
            
            // Initially, mutations should work - verify baseline
            conn.createStatement().execute("UPSERT INTO " + dataTableName + 
                " VALUES ('1', 'John', 25)");
            conn.commit();

            // Set up CRR that will block mutations (ACTIVE_TO_STANDBY state)
            ClusterRoleRecord blockingCrr = new ClusterRoleRecord("failover_test",
                    HighAvailabilityPolicy.FAILOVER, 
                    haAdmin.getZkUrl(), 
                    ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                    "standby-zk-url", 
                    ClusterRoleRecord.ClusterRole.STANDBY, 
                    1L);
            haAdmin.createOrUpdateDataOnZookeeper(blockingCrr);
            
            // Wait for the event to propagate
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

            // Verify that mutations are now blocked
            assertTrue("Mutations should be blocked", haGroupStoreManager.isMutationBlocked());
            
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
    public void testMutationBlockedOnLocalIndexTable() throws Exception {
        String dataTableName = generateUniqueName();
        String localIndexName = generateUniqueName();
        
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create data table and local index
            conn.createStatement().execute("CREATE TABLE " + dataTableName + 
                " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + localIndexName + 
                " ON " + dataTableName + "(name)");
            
            // Set up CRR that will block mutations
            ClusterRoleRecord blockingCrr = new ClusterRoleRecord("local_index_test",
                    HighAvailabilityPolicy.PARALLEL, 
                    haAdmin.getZkUrl(), 
                    ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                    "standby-zk-url", 
                    ClusterRoleRecord.ClusterRole.STANDBY, 
                    1L);
            haAdmin.createOrUpdateDataOnZookeeper(blockingCrr);
            
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
            
            // Test that UPSERT with local index also throws MutationBlockedIOException
            try {
                conn.createStatement().execute("UPSERT INTO " + dataTableName + 
                    " VALUES ('1', 'Alice', 28)");
                conn.commit();
                fail("Expected MutationBlockedIOException to be thrown for local index");
            } catch (CommitException e) {
                assertTrue("Expected mutation blocked exception", 
                    containsMutationBlockedException(e));
            }
        }
    }

    @Test
    public void testMutationAllowedWhenNotBlocked() throws Exception {
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();
        
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create data table and index
            conn.createStatement().execute("CREATE TABLE " + dataTableName + 
                " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
            conn.createStatement().execute("CREATE INDEX " + indexName + 
                " ON " + dataTableName + "(name)");
            
            // Set up CRR in ACTIVE state (should not block)
            ClusterRoleRecord nonBlockingCrr = new ClusterRoleRecord("active_test",
                    HighAvailabilityPolicy.FAILOVER, 
                    haAdmin.getZkUrl(), 
                    ClusterRoleRecord.ClusterRole.ACTIVE,
                    "standby-zk-url", 
                    ClusterRoleRecord.ClusterRole.STANDBY, 
                    1L);
            haAdmin.createOrUpdateDataOnZookeeper(nonBlockingCrr);
            
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
            
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
        
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create data table and index
            conn.createStatement().execute("CREATE TABLE " + dataTableName + 
                " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
            conn.createStatement().execute("CREATE INDEX " + indexName + 
                " ON " + dataTableName + "(name)");
            
            // Initially set up non-blocking CRR
            ClusterRoleRecord crr = new ClusterRoleRecord("transition_test",
                    HighAvailabilityPolicy.FAILOVER, 
                    haAdmin.getZkUrl(), 
                    ClusterRoleRecord.ClusterRole.ACTIVE,
                    "standby-zk-url", 
                    ClusterRoleRecord.ClusterRole.STANDBY, 
                    1L);
            haAdmin.createOrUpdateDataOnZookeeper(crr);
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
            
            // Mutation should work
            conn.createStatement().execute("UPSERT INTO " + dataTableName + 
                " VALUES ('1', 'David', 40)");
            conn.commit();
            
            // Transition to ACTIVE_TO_STANDBY (blocking state)
            crr = new ClusterRoleRecord("transition_test",
                    HighAvailabilityPolicy.FAILOVER, 
                    haAdmin.getZkUrl(), 
                    ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                    "standby-zk-url", 
                    ClusterRoleRecord.ClusterRole.STANDBY, 
                    2L);
            haAdmin.createOrUpdateDataOnZookeeper(crr);
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
            
            // Transition back to STANDBY (non-blocking state)
            crr = new ClusterRoleRecord("transition_test",
                    HighAvailabilityPolicy.FAILOVER, 
                    haAdmin.getZkUrl(), 
                    ClusterRoleRecord.ClusterRole.STANDBY,
                    "standby-zk-url", 
                    ClusterRoleRecord.ClusterRole.ACTIVE, 
                    3L);
            haAdmin.createOrUpdateDataOnZookeeper(crr);
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