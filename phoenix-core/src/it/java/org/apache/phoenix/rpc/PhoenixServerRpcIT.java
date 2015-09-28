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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class PhoenixServerRpcIT extends BaseOwnClusterHBaseManagedTimeIT {

    private static final String SCHEMA_NAME = "S";
    private static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");
    
    @BeforeClass
    public static void doSetup() throws Exception {
    	Map<String, String> serverProps = Collections.singletonMap(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS, 
        		TestPhoenixIndexRpcSchedulerFactory.class.getName());
        // use the standard rpc controller for client rpc, so that we can isolate server rpc and ensure they use the correct queue  
    	Map<String, String> clientProps = Collections.singletonMap(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY, 
    			RpcControllerFactory.class.getName());      
        NUM_SLAVES_BASE = 2;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
    
    @AfterClass
    public static void cleanUpAfterTestSuite() throws Exception {
        TestPhoenixIndexRpcSchedulerFactory.reset();
    }
    
    @Test
    public void testIndexQos() throws Exception { 
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(getUrl(), props);
        try {
            // create the table 
            conn.createStatement().execute(
                    "CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
    
            // create the index 
            conn.createStatement().execute(
                    "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");

            ensureTablesOnDifferentRegionServers(DATA_TABLE_FULL_NAME, INDEX_TABLE_FULL_NAME);
    
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
            stmt.setString(1, "k1");
            stmt.setString(2, "v1");
            stmt.setString(3, "v2");
            stmt.execute();
            conn.commit();
    
            // run select query that should use the index
            String selectSql = "SELECT k, v2 from " + DATA_TABLE_FULL_NAME + " WHERE v1=?";
            stmt = conn.prepareStatement(selectSql);
            stmt.setString(1, "v1");
    
            // verify that the query does a range scan on the index table
            ResultSet rs = stmt.executeQuery("EXPLAIN " + selectSql);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER S.I ['v1']", QueryUtil.getExplainPlan(rs));
    
            // verify that the correct results are returned
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("k1", rs.getString(1));
            assertEquals("v2", rs.getString(2));
            assertFalse(rs.next());
            
            // drop index table 
            conn.createStatement().execute(
                    "DROP INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME );
            // create a data table with the same name as the index table 
            conn.createStatement().execute(
                    "CREATE TABLE " + INDEX_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            
            // upsert one row to the table (which has the same table name as the previous index table)
            stmt = conn.prepareStatement("UPSERT INTO " + INDEX_TABLE_FULL_NAME + " VALUES(?,?,?)");
            stmt.setString(1, "k1");
            stmt.setString(2, "v1");
            stmt.setString(3, "v2");
            stmt.execute();
            conn.commit();
            
            // run select query on the new table
            selectSql = "SELECT k, v2 from " + INDEX_TABLE_FULL_NAME + " WHERE v1=?";
            stmt = conn.prepareStatement(selectSql);
            stmt.setString(1, "v1");
    
            // verify that the correct results are returned
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("k1", rs.getString(1));
            assertEquals("v2", rs.getString(2));
            assertFalse(rs.next());
            
            // verify that that index queue is used only once (for the first upsert)
            Mockito.verify(TestPhoenixIndexRpcSchedulerFactory.getIndexRpcExecutor()).dispatch(Mockito.any(CallRunner.class));
        }
        finally {
            conn.close();
        }
    }

	/**
	 * Verifies that the given tables each have a single region and are on
	 * different region servers. If they are on the same server moves tableName2
	 * to the other region server.
	 */
	private void ensureTablesOnDifferentRegionServers(String tableName1, String tableName2) throws Exception  {
		byte[] table1 = Bytes.toBytes(tableName1);
		byte[] table2 = Bytes.toBytes(tableName2);
		HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
		HBaseTestingUtility util = getUtility();
		MiniHBaseCluster cluster = util.getHBaseCluster();
		HMaster master = cluster.getMaster();
		AssignmentManager am = master.getAssignmentManager();
   
		// verify there is only a single region for data table
		List<HRegionInfo> tableRegions = admin.getTableRegions(table1);
		assertEquals("Expected single region for " + table1, tableRegions.size(), 1);
		HRegionInfo hri1 = tableRegions.get(0);
   
		// verify there is only a single region for index table
		tableRegions = admin.getTableRegions(table2);
		HRegionInfo hri2 = tableRegions.get(0);
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
		            || master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
		        // wait for the move to be finished
		        Thread.sleep(1);
		    }
		}
   
		hri1 = admin.getTableRegions(table1).get(0);
		serverName1 = am.getRegionStates().getRegionServerOfRegion(hri1);
		hri2 = admin.getTableRegions(table2).get(0);
		serverName2 = am.getRegionStates().getRegionServerOfRegion(hri2);

		// verify index and data tables are on different servers
		assertNotEquals("Tables " + tableName1 + " and " + tableName2 + " should be on different region servers", serverName1, serverName2);
	}
    
    @Test
    public void testMetadataQos() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(getUrl(), props);
        try {
        	ensureTablesOnDifferentRegionServers(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, PhoenixDatabaseMetaData.SYSTEM_STATS_NAME);
            // create the table 
            conn.createStatement().execute(
                    "CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)");
            // query the table from another connection, so that SYSTEM.STATS will be used 
            conn.createStatement().execute("SELECT * FROM "+DATA_TABLE_FULL_NAME);
            // verify that that metadata queue is at least once 
            Mockito.verify(TestPhoenixIndexRpcSchedulerFactory.getMetadataRpcExecutor(), Mockito.atLeastOnce()).dispatch(Mockito.any(CallRunner.class));
        }
        finally {
            conn.close();
        }
    }

}
