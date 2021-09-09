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
package org.apache.phoenix.rpc;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixServerRpcIT extends BaseTest {

    private String schemaName;
    private String indexName;
    private String dataTableFullName;
    private String indexTableFullName;
    
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
    	Map<String, String> serverProps = Collections.singletonMap(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS, 
        		TestPhoenixIndexRpcSchedulerFactory.class.getName());
        Map<String, String> clientProps = Collections.emptyMap();
        NUM_SLAVES_BASE = 2;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
    
    @After
    public void cleanUpAfterTest() throws Exception {
        TestPhoenixIndexRpcSchedulerFactory.reset();
    }
    
    @Before
    public void generateTableNames() throws SQLException {
        schemaName = generateUniqueName();
        indexName = generateUniqueName();
        indexTableFullName = SchemaUtil.getTableName(schemaName, indexName);
        dataTableFullName = SchemaUtil.getTableName(schemaName, generateUniqueName());
    }
    
    @Test
    public void testIndexQos() throws Exception { 
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(getUrl(), props);
        try {
            // create the table
            createTable(conn, dataTableFullName);
    
            // create the index
            createIndex(conn, indexName);

            ensureTablesOnDifferentRegionServers(dataTableFullName, indexTableFullName);
            TestPhoenixIndexRpcSchedulerFactory.reset();
            upsertRow(conn, dataTableFullName);
            // An index row is updated twice, once before the data table row, and once after it. Thus, the factory is invoked twice
            Mockito.verify(TestPhoenixIndexRpcSchedulerFactory.getIndexRpcExecutor(), Mockito.times(2))
                    .dispatch(Mockito.any(CallRunner.class));
            TestPhoenixIndexRpcSchedulerFactory.reset();
            // run select query that should use the index
            String selectSql = "SELECT k, v2 from " + dataTableFullName + " WHERE v1=?";
            PreparedStatement stmt = conn.prepareStatement(selectSql);
            stmt.setString(1, "v1");
    
            // verify that the query does a range scan on the index table
            ResultSet rs = stmt.executeQuery("EXPLAIN " + selectSql);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + indexTableFullName + " ['v1']", QueryUtil.getExplainPlan(rs));
    
            // verify that the correct results are returned
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("k1", rs.getString(1));
            assertEquals("v2", rs.getString(2));
            assertFalse(rs.next());
            
            // drop index table 
            conn.createStatement().execute(
                    "DROP INDEX " + indexName + " ON " + dataTableFullName );
            // create a data table with the same name as the index table
            createTable(conn, indexTableFullName);
            
            TestPhoenixIndexRpcSchedulerFactory.reset();
            // upsert one row to the table (which has the same table name as the previous index table)
            upsertRow(conn, indexTableFullName);
            Mockito.verify(TestPhoenixIndexRpcSchedulerFactory.getIndexRpcExecutor(), never()).dispatch(Mockito.any(CallRunner.class));
            
            // run select query on the new table
            selectSql = "SELECT k, v2 from " + indexTableFullName + " WHERE v1=?";
            stmt = conn.prepareStatement(selectSql);
            stmt.setString(1, "v1");
    
            // verify that the correct results are returned
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("k1", rs.getString(1));
            assertEquals("v2", rs.getString(2));
            assertFalse(rs.next());
        }
        finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectServerDisabled() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // disable server side upsert select
        props.setProperty(QueryServices.ENABLE_SERVER_UPSERT_SELECT, "false");
        try (Connection conn = driver.connect(getUrl(), props)) {
            // create two tables with identical schemas
            createTable(conn, dataTableFullName);
            upsertRow(conn, dataTableFullName);
            String tableName2 = dataTableFullName + "_2";
            createTable(conn, tableName2);
            ensureTablesOnDifferentRegionServers(dataTableFullName, tableName2);
            // copy the row from the first table using upsert select
            upsertSelectRows(conn, dataTableFullName, tableName2);
            Mockito.verify(TestPhoenixIndexRpcSchedulerFactory.getIndexRpcExecutor(),
                    Mockito.never()).dispatch(Mockito.any(CallRunner.class));

        }
    }

    private void createTable(Connection conn, String tableName) throws SQLException {
        conn.createStatement().execute(
                "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
    }

    private void createIndex(Connection conn, String indexName) throws SQLException {
        conn.createStatement().execute(
                "CREATE INDEX " + indexName + " ON " + dataTableFullName + " (v1) INCLUDE (v2)");
    }

    private void upsertRow(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        stmt.setString(1, "k1");
        stmt.setString(2, "v1");
        stmt.setString(3, "v2");
        stmt.execute();
        conn.commit();
    }

    private void upsertSelectRows(Connection conn, String tableName1, String tableName2) throws SQLException {
        PreparedStatement stmt =
                conn.prepareStatement(
                        "UPSERT INTO " + tableName2 + " (k, v1, v2) SELECT k, v1, v2 FROM "
                                + tableName1);
        stmt.execute();
        conn.commit();
    }

	/**
	 * Verifies that the given tables each have a single region and are on
	 * different region servers. If they are on the same server moves tableName2
	 * to the other region server.
	 */
	private void ensureTablesOnDifferentRegionServers(String tableName1, String tableName2) throws Exception  {
		byte[] table1 = Bytes.toBytes(tableName1);
		byte[] table2 = Bytes.toBytes(tableName2);
		Admin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
		HBaseTestingUtility util = getUtility();
		MiniHBaseCluster cluster = util.getHBaseCluster();
		HMaster master = cluster.getMaster();
		AssignmentManager am = master.getAssignmentManager();
   
		// verify there is only a single region for data table
		List<RegionInfo> tableRegions = admin.getRegions(TableName.valueOf(table1));
		assertEquals("Expected single region for " + table1, tableRegions.size(), 1);
		RegionInfo hri1 = tableRegions.get(0);
   
		// verify there is only a single region for index table
		tableRegions = admin.getRegions(TableName.valueOf(table2));
		RegionInfo hri2 = tableRegions.get(0);
		assertEquals("Expected single region for " + table2, tableRegions.size(), 1);
   
		ServerName serverName1 = am.getRegionStates().getRegionServerOfRegion(hri1);
		ServerName serverName2 = am.getRegionStates().getRegionServerOfRegion(hri2);
   
		// if data table and index table are on same region server, move the index table to the other region server
		if (serverName1.equals(serverName2)) {
		    HRegionServer server1 = util.getHBaseCluster().getRegionServer(0);
		    HRegionServer server2 = util.getHBaseCluster().getRegionServer(1);
		    HRegionServer dstServer = null;
		    HRegionServer srcServer = null;
		    if (server1.getServerName().equals(serverName2)) {
		        dstServer = server2;
		        srcServer = server1;
		    } else {
		        dstServer = server1;
		        srcServer = server2;
		    }
		    byte[] encodedRegionNameInBytes = hri2.getEncodedNameAsBytes();
		    admin.move(encodedRegionNameInBytes, Bytes.toBytes(dstServer.getServerName().getServerName()));
		    while (dstServer.getOnlineRegion(hri2.getRegionName()) == null
		            || dstServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)
		            || srcServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)
		            || master.getAssignmentManager().getRegionStates().isRegionInTransition(hri2)) {
		        // wait for the move to be finished
		        Thread.sleep(1);
		    }
		}
   
		hri1 = admin.getRegions(TableName.valueOf(table1)).get(0);
		serverName1 = am.getRegionStates().getRegionServerOfRegion(hri1);
		hri2 = admin.getRegions(TableName.valueOf(table2)).get(0);
		serverName2 = am.getRegionStates().getRegionServerOfRegion(hri2);

		// verify index and data tables are on different servers
		assertNotEquals("Tables " + tableName1 + " and " + tableName2 + " should be on different region servers", serverName1, serverName2);
	}
}
