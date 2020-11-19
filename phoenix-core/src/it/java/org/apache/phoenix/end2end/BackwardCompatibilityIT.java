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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.ADD_DATA;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.ADD_DELETE;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.CREATE_ADD;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.CREATE_DIVERGED_VIEW;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.INDEX_REBUILD_ASYNC;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.QUERY_ADD_DATA;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.QUERY_ADD_DELETE;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.QUERY_CREATE_ADD;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.QUERY_CREATE_DIVERGED_VIEW;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.QUERY_INDEX_REBUILD_ASYNC;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.assertExpectedOutput;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.checkForPreConditions;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.computeClientVersions;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.executeQueriesWithCurrentVersion;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.executeQueryWithClientVersion;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.UpgradeProps.NONE;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.UpgradeProps.SET_MAX_LOOK_BACK_AGE;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.coprocessor.TaskMetaDataEndpoint;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SystemTaskSplitPolicy;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This class is meant for testing all compatible client versions 
 * against the current server version. It runs SQL queries with given 
 * client versions and compares the output against gold files
 */

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class BackwardCompatibilityIT {

    private final String compatibleClientVersion;
    private static Configuration conf;
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static String url;

    public BackwardCompatibilityIT(String compatibleClientVersion) {
        this.compatibleClientVersion = compatibleClientVersion;
    }

    @Parameters(name = "BackwardCompatibilityIT_compatibleClientVersion={0}")
    public static synchronized Collection<String> data() throws Exception {
        return computeClientVersions();
    }

    @Before
    public synchronized void doSetup() throws Exception {
        conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        hbaseTestUtil.startMiniCluster();
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        checkForPreConditions(compatibleClientVersion, conf);
    }
    
    @After
    public void cleanUpAfterTest() throws Exception {
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            hbaseTestUtil.shutdownMiniCluster();
        }
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testUpsertWithOldClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_CREATE_ADD, url, NONE);
        assertExpectedOutput(QUERY_CREATE_ADD);
    }

    @Test
    public void testCreateDivergedViewWithOldClientReadFromNewClient() throws Exception {
        // Create a base table, view and make it diverge from an old client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_DIVERGED_VIEW, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_CREATE_DIVERGED_VIEW,url, NONE);
        assertExpectedOutput(QUERY_CREATE_DIVERGED_VIEW);
    }

    @Test
    public void testCreateDivergedViewWithOldClientReadWithMaxLookBackAge()
            throws Exception {
        // Create a base table, view and make it diverge from an old client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_DIVERGED_VIEW, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_CREATE_DIVERGED_VIEW, url, SET_MAX_LOOK_BACK_AGE);
        assertExpectedOutput(QUERY_CREATE_DIVERGED_VIEW);
    }

    @Test
    public void testCreateDivergedViewWithOldClientReadFromOldClient() throws Exception {
        // Create a base table, view and make it diverge from an old client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_DIVERGED_VIEW, zkQuorum);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_CREATE_DIVERGED_VIEW, zkQuorum);
        assertExpectedOutput(QUERY_CREATE_DIVERGED_VIEW);
    }

    @Test
    public void testCreateDivergedViewWithOldClientReadFromOldClientAfterUpgrade()
            throws Exception {
        // Create a base table, view and make it diverge from an old client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_DIVERGED_VIEW, zkQuorum);
        try (Connection conn = DriverManager.getConnection(url)) {
            // Just connect with a new client to cause a metadata upgrade
        }
        // Query with an old client again
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_CREATE_DIVERGED_VIEW, zkQuorum);
        assertExpectedOutput(QUERY_CREATE_DIVERGED_VIEW);
    }

    @Test
    public void testCreateDivergedViewWithNewClientReadFromOldClient() throws Exception {
        executeQueriesWithCurrentVersion(CREATE_DIVERGED_VIEW, url, NONE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_CREATE_DIVERGED_VIEW, zkQuorum);
        assertExpectedOutput(QUERY_CREATE_DIVERGED_VIEW);
    }

    @Test
    public void testCreateDivergedViewWithNewClientReadFromNewClient() throws Exception {
        executeQueriesWithCurrentVersion(CREATE_DIVERGED_VIEW, url, NONE);
        executeQueriesWithCurrentVersion(QUERY_CREATE_DIVERGED_VIEW, url, NONE);
        assertExpectedOutput(QUERY_CREATE_DIVERGED_VIEW);
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the new client
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testSelectWithOldClient() throws Exception {
        // Insert data with new client and read with old client
        executeQueriesWithCurrentVersion(CREATE_ADD, url, NONE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_CREATE_ADD, zkQuorum);
        assertExpectedOutput(QUERY_CREATE_ADD);
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client 
     * 4. New Client inserts more data into the tables created by old client 
     * 5. Old Client reads the data inserted by new client
     * Use phoenix.max.lookback.age.seconds config and ensure that upgrade
     * is not impacted by the config.
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testSelectUpsertWithNewClientWithMaxLookBackAge() throws Exception {
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_CREATE_ADD, url, SET_MAX_LOOK_BACK_AGE);
        assertExpectedOutput(QUERY_CREATE_ADD);

        // Insert more data with new client and read with old client
        executeQueriesWithCurrentVersion(ADD_DATA, url, SET_MAX_LOOK_BACK_AGE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_ADD_DATA, zkQuorum);
        assertExpectedOutput(QUERY_ADD_DATA);
    }

    /**
     * Scenario:
     * 1. Old Client connects to the updated server
     * 2. Old Client creates tables and inserts data
     * 3. New Client reads the data inserted by the old client
     * 4. New Client inserts more data into the tables created by old client
     * 5. Old Client reads the data inserted by new client
     *
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testSelectUpsertWithNewClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_CREATE_ADD, url, NONE);
        assertExpectedOutput(QUERY_CREATE_ADD);

        // Insert more data with new client and read with old client
        executeQueriesWithCurrentVersion(ADD_DATA, url, NONE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_ADD_DATA, zkQuorum);
        assertExpectedOutput(QUERY_ADD_DATA);
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client 
     * 4. Old Client inserts more data into the tables created by old client 
     * 5. New Client reads the data inserted by new client
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testSelectUpsertWithOldClient() throws Exception {
        // Insert data with new client and read with old client
        executeQueriesWithCurrentVersion(CREATE_ADD, url, NONE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_CREATE_ADD, zkQuorum);
        assertExpectedOutput(QUERY_CREATE_ADD);

        // Insert more data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, ADD_DATA, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_ADD_DATA, url, NONE);
        assertExpectedOutput(QUERY_ADD_DATA);
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client 
     * 4. Old Client creates and deletes the data
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testUpsertDeleteWithOldClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_CREATE_ADD, url, NONE);
        assertExpectedOutput(QUERY_CREATE_ADD);

        // Deletes with the old client
        executeQueryWithClientVersion(compatibleClientVersion, ADD_DELETE, zkQuorum);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_ADD_DELETE, zkQuorum);
        assertExpectedOutput(QUERY_ADD_DELETE);
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client 
     * 4. New Client creates and deletes the data
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testUpsertDeleteWithNewClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueriesWithCurrentVersion(CREATE_ADD, url, NONE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_CREATE_ADD, zkQuorum);
        assertExpectedOutput(QUERY_CREATE_ADD);

        // Deletes with the new client
        executeQueriesWithCurrentVersion(ADD_DELETE, url, NONE);
        executeQueriesWithCurrentVersion(QUERY_ADD_DELETE, url,NONE);
        assertExpectedOutput(QUERY_ADD_DELETE);
    }

    @Test
    public void testSplitPolicyAndCoprocessorForSysTask() throws Exception {
        executeQueryWithClientVersion(compatibleClientVersion,
            CREATE_DIVERGED_VIEW, zkQuorum);

        String[] versionArr = compatibleClientVersion.split("\\.");
        int majorVersion = Integer.parseInt(versionArr[0]);
        int minorVersion = Integer.parseInt(versionArr[1]);
        org.apache.hadoop.hbase.client.Connection conn = null;
        Admin admin = null;
        // if connected with client < 4.15, SYSTEM.TASK does not exist
        // if connected with client 4.15, SYSTEM.TASK exists without any
        // split policy and also TaskMetaDataEndpoint coprocessor would not
        // exist
        if (majorVersion == 4 && minorVersion == 15) {
            conn = hbaseTestUtil.getConnection();
            admin = conn.getAdmin();
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(
                TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME));
            assertNull("split policy should be null with compatible client version: "
                + compatibleClientVersion, tableDescriptor.getRegionSplitPolicyClassName());
            assertFalse("Coprocessor " + TaskMetaDataEndpoint.class.getName()
                + " should not have been added with compatible client version: "
                + compatibleClientVersion,
                tableDescriptor.hasCoprocessor(TaskMetaDataEndpoint.class.getName()));
        }

        executeQueriesWithCurrentVersion(QUERY_CREATE_DIVERGED_VIEW, url, NONE);

        if (conn == null) {
            conn = hbaseTestUtil.getConnection();
            admin = conn.getAdmin();
        }
        // connect with client > 4.15, and we have new split policy and new
        // coprocessor loaded
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(
            TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME));
        assertEquals("split policy not updated with compatible client version: "
            + compatibleClientVersion,
            tableDescriptor.getRegionSplitPolicyClassName(),
            SystemTaskSplitPolicy.class.getName());
        assertTrue("Coprocessor " + TaskMetaDataEndpoint.class.getName()
            + " has not been added with compatible client version: "
            + compatibleClientVersion, tableDescriptor.hasCoprocessor(
            TaskMetaDataEndpoint.class.getName()));
        assertExpectedOutput(QUERY_CREATE_DIVERGED_VIEW);
        admin.close();
        conn.close();
    }

    @Test
    public void testSystemTaskCreationWithIndexAsyncRebuild() throws Exception {
        String[] versionArr = compatibleClientVersion.split("\\.");
        int majorVersion = Integer.parseInt(versionArr[0]);
        int minorVersion = Integer.parseInt(versionArr[1]);
        // index async rebuild support min version check
        if (majorVersion > 4 || (majorVersion == 4 && minorVersion >= 15)) {
            executeQueryWithClientVersion(compatibleClientVersion,
                INDEX_REBUILD_ASYNC, zkQuorum);
            executeQueriesWithCurrentVersion(QUERY_INDEX_REBUILD_ASYNC, url, NONE);
            assertExpectedOutput(QUERY_INDEX_REBUILD_ASYNC);
        }
    }

}
