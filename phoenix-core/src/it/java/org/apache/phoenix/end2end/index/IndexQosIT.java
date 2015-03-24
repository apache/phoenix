/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ipc.BalancedQueueRpcExecutor;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.PhoenixIndexRpcScheduler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.RpcExecutor;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexQosRpcControllerFactory;
import org.apache.phoenix.hbase.index.ipc.PhoenixIndexRpcSchedulerFactory;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;


@Category(NeedsOwnMiniClusterTest.class)
public class IndexQosIT extends BaseTest {

    private static final String SCHEMA_NAME = "S";
    private static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");
    private static final int NUM_SLAVES = 2;

    private static String url;
    private static PhoenixTestDriver driver;
    private HBaseTestingUtility util;
    private HBaseAdmin admin;
    private Configuration conf;
    private static RpcExecutor spyRpcExecutor = Mockito.spy(new BalancedQueueRpcExecutor("test-queue", 30, 1, 300));

    /**
     * Factory that uses a spyed RpcExecutor
     */
    public static class TestPhoenixIndexRpcSchedulerFactory extends PhoenixIndexRpcSchedulerFactory {
        @Override
        public RpcScheduler create(Configuration conf, PriorityFunction priorityFunction, Abortable abortable) {
            PhoenixIndexRpcScheduler phoenixIndexRpcScheduler = (PhoenixIndexRpcScheduler)super.create(conf, priorityFunction, abortable);
            phoenixIndexRpcScheduler.setExecutorForTesting(spyRpcExecutor);
            return phoenixIndexRpcScheduler;
        }
    }

    @Before
    public void doSetup() throws Exception {
        conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        conf.set(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
                TestPhoenixIndexRpcSchedulerFactory.class.getName());
        conf.set(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY, IndexQosRpcControllerFactory.class.getName());
        util = new HBaseTestingUtility(conf);
        // start cluster with 2 region servers
        util.startMiniCluster(NUM_SLAVES);
        admin = util.getHBaseAdmin();
        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
                + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        driver = initAndRegisterDriver(url, ReadOnlyProps.EMPTY_PROPS);
    }

    @After
    public void tearDown() throws Exception {
        try {
            destroyDriver(driver);
            if (admin!=null) {
            	admin.close();
            }
        } finally {
            util.shutdownMiniCluster();
        }
    }
    
    @Test
    public void testIndexWriteQos() throws Exception { 
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(url, props);

        // create the table 
        conn.createStatement().execute(
                "CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");

        // create the index 
        conn.createStatement().execute(
                "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");

        byte[] dataTableName = Bytes.toBytes(DATA_TABLE_FULL_NAME);
        byte[] indexTableName = Bytes.toBytes(INDEX_TABLE_FULL_NAME);
        MiniHBaseCluster cluster = util.getHBaseCluster();
        HMaster master = cluster.getMaster();
        AssignmentManager am = master.getAssignmentManager();

        // verify there is only a single region for data table
        List<HRegionInfo> tableRegions = admin.getTableRegions(dataTableName);
        assertEquals("Expected single region for " + dataTableName, tableRegions.size(), 1);
        HRegionInfo dataHri = tableRegions.get(0);

        // verify there is only a single region for index table
        tableRegions = admin.getTableRegions(indexTableName);
        HRegionInfo indexHri = tableRegions.get(0);
        assertEquals("Expected single region for " + indexTableName, tableRegions.size(), 1);

        ServerName dataServerName = am.getRegionStates().getRegionServerOfRegion(dataHri);
        ServerName indexServerName = am.getRegionStates().getRegionServerOfRegion(indexHri);

        // if data table and index table are on same region server, move the index table to the other region server
        if (dataServerName.equals(indexServerName)) {
            HRegionServer server1 = util.getHBaseCluster().getRegionServer(0);
            HRegionServer server2 = util.getHBaseCluster().getRegionServer(1);
            HRegionServer dstServer = null;
            HRegionServer srcServer = null;
            if (server1.getServerName().equals(indexServerName)) {
                dstServer = server2;
                srcServer = server1;
            } else {
                dstServer = server1;
                srcServer = server2;
            }
            byte[] encodedRegionNameInBytes = indexHri.getEncodedNameAsBytes();
            admin.move(encodedRegionNameInBytes, Bytes.toBytes(dstServer.getServerName().getServerName()));
            while (dstServer.getOnlineRegion(indexHri.getRegionName()) == null
                    || dstServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)
                    || srcServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)
                    || master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
                // wait for the move to be finished
                Thread.sleep(1);
            }
        }

        dataHri = admin.getTableRegions(dataTableName).get(0);
        dataServerName = am.getRegionStates().getRegionServerOfRegion(dataHri);
        indexHri = admin.getTableRegions(indexTableName).get(0);
        indexServerName = am.getRegionStates().getRegionServerOfRegion(indexHri);

        // verify index and data tables are on different servers
        assertNotEquals("Index and Data table should be on different region servers dataServer " + dataServerName
                + " indexServer " + indexServerName, dataServerName, indexServerName);

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
        Mockito.verify(spyRpcExecutor).dispatch(Mockito.any(CallRunner.class));
    }

}
