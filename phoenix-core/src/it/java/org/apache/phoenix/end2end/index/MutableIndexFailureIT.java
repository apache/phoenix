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

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.balancer.IndexLoadBalancer;
import org.apache.phoenix.hbase.index.master.IndexMasterObserver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;
/**
 * 
 * Test for failure of region server to write to index table.
 * For some reason dropping tables after running this test
 * fails unless it runs its own mini cluster. 
 * 
 * 
 * @since 2.1
 */

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class MutableIndexFailureIT extends BaseTest {
    private static final int NUM_SLAVES = 4;
    private static PhoenixTestDriver driver;
    private static HBaseTestingUtility util;
    private Timer scheduleTimer;

    private String tableName;
    private String indexName;
    private String fullTableName;
    private String fullIndexName;

    private boolean transactional;
    private final String tableDDLOptions;

    public MutableIndexFailureIT(boolean transactional) {
        this.transactional = transactional;
        this.tableDDLOptions = transactional ? " TRANSACTIONAL=true " : "";
        this.tableName = TestUtil.DEFAULT_DATA_TABLE_NAME;
        this.indexName = "IDX";
        this.fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        this.fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    }
    
    @Before
    public void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        conf.setInt("hbase.client.retries.number", 2);
        conf.setInt("hbase.client.pause", 5000);
        conf.setInt("hbase.balancer.period", Integer.MAX_VALUE);
        conf.setLong(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB, 0);
        conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
        conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, IndexLoadBalancer.class,
            LoadBalancer.class);
        util = new HBaseTestingUtility(conf);
        util.startMiniCluster(NUM_SLAVES);
        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
                + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        driver = initAndRegisterDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
        clusterInitialized = true;
        setupTxManager();
    }
    
    @Parameters(name = "transactional = {0}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] { { false }, { true } });
    }
    
    @After
    public void tearDown() throws Exception {
        try {
            destroyDriver(driver);
        } finally {
            try {
                if(scheduleTimer != null){
                    scheduleTimer.cancel();
                    scheduleTimer = null;
                }
            } finally {
                util.shutdownMiniCluster();
            }
        }
    }

    @Ignore("See PHOENIX-2331")
    @Test(timeout=300000)
    public void testWriteFailureDisablesLocalIndex() throws Exception {
        helpTestWriteFailureDisablesIndex(true);
    }
 
    @Ignore("See PHOENIX-2332")
    @Test(timeout=300000)
    public void testWriteFailureDisablesIndex() throws Exception {
        helpTestWriteFailureDisablesIndex(false);
    }
    
    public void helpTestWriteFailureDisablesIndex(boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = driver.connect(url, props)) {
            String query;
            ResultSet rs;
            conn.setAutoCommit(false);
            conn.createStatement().execute(
                    "CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) "+tableDDLOptions);
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
    
            if(localIndex) {
                conn.createStatement().execute(
                    "CREATE LOCAL INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
                conn.createStatement().execute(
                    "CREATE LOCAL INDEX " + indexName+ "_2" + " ON " + fullTableName + " (v2) INCLUDE (v1)");
            } else {
                conn.createStatement().execute(
                    "CREATE INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            }
                
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
    
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(TestUtil.DEFAULT_SCHEMA_NAME), indexName,
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            conn.commit();
    
            TableName indexTable =
                    TableName.valueOf(localIndex ? MetaDataUtil
                            .getLocalIndexTableName(fullTableName) : fullIndexName);
            HBaseAdmin admin = this.util.getHBaseAdmin();
            HTableDescriptor indexTableDesc = admin.getTableDescriptor(indexTable);
            try{
              admin.disableTable(indexTable);
              admin.deleteTable(indexTable);
            } catch (TableNotFoundException ignore) {}
    
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1, "a2");
            stmt.setString(2, "x2");
            stmt.setString(3, "2");
            stmt.execute();
            if (transactional) {
                try {
                    conn.commit();
                    fail();
                } catch (SQLException e) {
                    conn.rollback();
                }
            }
            else {
                conn.commit();
            }
    
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(TestUtil.DEFAULT_SCHEMA_NAME), indexName,
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            // the index is only disabled for non-txn tables upon index table write failure
            PIndexState indexState =  transactional ? PIndexState.ACTIVE : PIndexState.DISABLE;
            assertEquals(indexState.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            if(localIndex) {
                rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(TestUtil.DEFAULT_SCHEMA_NAME), indexName + "_2",
                    new String[] { PTableType.INDEX.toString() });
                assertTrue(rs.next());
                assertEquals(indexName + "_2", rs.getString(3));
                assertEquals(indexState.toString(), rs.getString("INDEX_STATE"));
                assertFalse(rs.next());
            }
    
            // if the table is transactional the write to the index table will fail because the
            // index has not been disabled
            if (!transactional) {
                // Verify UPSERT on data table still work after index is disabled
                stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
                stmt.setString(1, "a3");
                stmt.setString(2, "x3");
                stmt.setString(3, "3");
                stmt.execute();
                conn.commit();
            }
            
            if (transactional) {
                // if the table was transactional there should be 1 row (written before the index
                // was disabled)
                query = "SELECT /*+ NO_INDEX */ v2 FROM " + fullTableName;
                rs = conn.createStatement().executeQuery("EXPLAIN " + query);
                String expectedPlan =
                        "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullTableName;
                assertEquals(expectedPlan, QueryUtil.getExplainPlan(rs));
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1));
                assertFalse(rs.next());
            } else {
                // if the table was not transactional there should be three rows (all writes to data
                // table should succeed)
                query = "SELECT v2 FROM " + fullTableName;
                rs = conn.createStatement().executeQuery("EXPLAIN " + query);
                String expectedPlan =
                        "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullTableName;
                assertEquals(expectedPlan, QueryUtil.getExplainPlan(rs));
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("2", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("3", rs.getString(1));
                assertFalse(rs.next());
            }
            
            // recreate index table
            admin.createTable(indexTableDesc);
            do {
              Thread.sleep(15 * 1000); // sleep 15 secs
              rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(TestUtil.DEFAULT_SCHEMA_NAME), indexName,
                  new String[] { PTableType.INDEX.toString() });
              assertTrue(rs.next());
              if(PIndexState.ACTIVE.toString().equals(rs.getString("INDEX_STATE"))){
                  break;
              }
              if(localIndex) {
                  rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(TestUtil.DEFAULT_SCHEMA_NAME), indexName + "_2",
                      new String[] { PTableType.INDEX.toString() });
                  assertTrue(rs.next());
                  if(PIndexState.ACTIVE.toString().equals(rs.getString("INDEX_STATE"))){
                      break;
                  }
              }
            } while(true);
            
            // Verify UPSERT on data table still work after index table is recreated
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1, "a4");
            stmt.setString(2, "x4");
            stmt.setString(3, "4");
            stmt.execute();
            conn.commit();

            // verify index table has data
            query = "SELECT count(1) FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            // for txn tables there will be only one row in the index (a4)
            // for non txn tables there will be three rows because we only partially build index
            // from where we failed and the oldest
            // index row has been deleted when we dropped the index table during test
            assertEquals(transactional ? 1 : 3, rs.getInt(1));
            }
        }
        
        @Test(timeout=300000)
        public void testWriteFailureWithRegionServerDown() throws Exception {
            String query;
            ResultSet rs;
    
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            try (Connection conn = driver.connect(url, props);) {
                conn.setAutoCommit(false);
                conn.createStatement().execute(
                        "CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) "+tableDDLOptions);
                query = "SELECT * FROM " + fullTableName;
                rs = conn.createStatement().executeQuery(query);
                assertFalse(rs.next());
        
                conn.createStatement().execute(
                        "CREATE INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
                query = "SELECT * FROM " + fullIndexName;
                rs = conn.createStatement().executeQuery(query);
                assertFalse(rs.next());
        
                // Verify the metadata for index is correct.
                rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(TestUtil.DEFAULT_SCHEMA_NAME), indexName,
                        new String[] { PTableType.INDEX.toString() });
                assertTrue(rs.next());
                assertEquals(indexName, rs.getString(3));
                assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
                assertFalse(rs.next());
                
                PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
                stmt.setString(1, "a");
                stmt.setString(2, "x");
                stmt.setString(3, "1");
                stmt.execute();
                conn.commit();
                
                // find a RS which doesn't has CATALOG table
                TableName catalogTable = TableName.valueOf("SYSTEM.CATALOG");
                TableName indexTable = TableName.valueOf(fullIndexName);
                final HBaseCluster cluster = this.util.getHBaseCluster();
                Collection<ServerName> rss = cluster.getClusterStatus().getServers();
                HBaseAdmin admin = this.util.getHBaseAdmin();
                List<HRegionInfo> regions = admin.getTableRegions(catalogTable);
                ServerName catalogRS = cluster.getServerHoldingRegion(regions.get(0).getTable(),
                        regions.get(0).getRegionName());
                ServerName metaRS = cluster.getServerHoldingMeta();
                ServerName rsToBeKilled = null;
                
                // find first RS isn't holding META or CATALOG table
                for(ServerName curRS : rss) {
                    if(!curRS.equals(catalogRS) && !metaRS.equals(curRS)) {
                        rsToBeKilled = curRS;
                        break;
                    }
                }
                assertTrue(rsToBeKilled != null);
                
                regions = admin.getTableRegions(indexTable);
                final HRegionInfo indexRegion = regions.get(0);
                final ServerName dstRS = rsToBeKilled;
                admin.move(indexRegion.getEncodedNameAsBytes(), Bytes.toBytes(rsToBeKilled.getServerName()));
                this.util.waitFor(30000, 200, new Waiter.Predicate<Exception>() {
                    @Override
                    public boolean evaluate() throws Exception {
                      ServerName sn = cluster.getServerHoldingRegion(indexRegion.getTable(),
                              indexRegion.getRegionName());
                      return (sn != null && sn.equals(dstRS));
                    }
                  });
                
                // use timer sending updates in every 10ms
                this.scheduleTimer = new Timer(true);
                this.scheduleTimer.schedule(new SendingUpdatesScheduleTask(conn, fullTableName), 0, 10);
                // let timer sending some updates
                Thread.sleep(100);
                
                // kill RS hosting index table
                this.util.getHBaseCluster().killRegionServer(rsToBeKilled);
                
                // wait for index table completes recovery
                this.util.waitUntilAllRegionsAssigned(indexTable);
                
                // Verify the metadata for index is correct.       
                do {
                  Thread.sleep(15 * 1000); // sleep 15 secs
                  rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(TestUtil.DEFAULT_SCHEMA_NAME), indexName,
                      new String[] { PTableType.INDEX.toString() });
                  assertTrue(rs.next());
                  if(PIndexState.ACTIVE.toString().equals(rs.getString("INDEX_STATE"))){
                      break;
                  }
                } while(true);
                this.scheduleTimer.cancel();
                
                assertEquals(cluster.getClusterStatus().getDeadServers(), 1);
            }
    }
    
    static class SendingUpdatesScheduleTask extends TimerTask {
        private static final Log LOG = LogFactory.getLog(SendingUpdatesScheduleTask.class);
        
        // inProgress is to prevent timer from invoking a new task while previous one is still
        // running
        private final static AtomicInteger inProgress = new AtomicInteger(0);
        private final Connection conn;
        private final String fullTableName;
        private int inserts = 0;

        public SendingUpdatesScheduleTask(Connection conn, String fullTableName) {
            this.conn = conn;
            this.fullTableName = fullTableName;
        }

        public void run() {
            if(inProgress.get() > 0){
                return;
            }
            
            try {
                inProgress.incrementAndGet();
                inserts++;
                PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
                stmt.setString(1, "a" + inserts);
                stmt.setString(2, "x" + inserts);
                stmt.setString(3, String.valueOf(inserts));
                stmt.execute();
                conn.commit();
            } catch (Throwable t) {
                LOG.warn("ScheduledBuildIndexTask failed!", t);
            } finally {
                inProgress.decrementAndGet();
            }
        }
    }
    
}
