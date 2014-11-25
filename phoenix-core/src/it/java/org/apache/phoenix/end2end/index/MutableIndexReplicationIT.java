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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

/**
 * Test that we correctly replicate indexes over replication
 * <p>
 * Code for setUp/teardown copied from org.apache.hadoop.hbase.replication.TestReplicationBase in
 * HBase 0.98.5
 * </p>
 */

@Category(NeedsOwnMiniClusterTest.class)
public class MutableIndexReplicationIT extends BaseTest {

    private static final Log LOG = LogFactory.getLog(MutableIndexReplicationIT.class);

    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    public static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    public static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");
    private static final long REPLICATION_WAIT_TIME_MILLIS = 10000;

    protected static PhoenixTestDriver driver;
    private static String URL;

    protected static Configuration conf1 = HBaseConfiguration.create();
    protected static Configuration conf2;

    protected static ZooKeeperWatcher zkw1;
    protected static ZooKeeperWatcher zkw2;

    protected static ReplicationAdmin admin;

    protected static HBaseTestingUtility utility1;
    protected static HBaseTestingUtility utility2;
    protected static final int REPLICATION_RETRIES = 100;

    protected static final byte[] tableName = Bytes.toBytes("test");
    protected static final byte[] row = Bytes.toBytes("row");

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setupConfigsAndStartCluster();
        setupDriver();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            destroyDriver(driver);
        } finally {
            try {
                utility2.shutdownMiniCluster();
            } finally {
                utility1.shutdownMiniCluster();
            }
        }
    }

    private static void setupConfigsAndStartCluster() throws Exception {
        // cluster-1 lives at regular HBase home, so we don't need to change how phoenix handles
        // lookups
//        conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
        // smaller log roll size to trigger more events
        setUpConfigForMiniCluster(conf1);
        conf1.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
        conf1.setInt("replication.source.size.capacity", 10240);
        conf1.setLong("replication.source.sleepforretries", 100);
        conf1.setInt("hbase.regionserver.maxlogs", 10);
        conf1.setLong("hbase.master.logcleaner.ttl", 10);
        conf1.setInt("zookeeper.recovery.retry", 1);
        conf1.setInt("zookeeper.recovery.retry.intervalmill", 10);
        conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
        conf1.setBoolean("dfs.support.append", true);
        conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
        conf1.setInt("replication.stats.thread.period.seconds", 5);
        conf1.setBoolean("hbase.tests.use.shortcircuit.reads", false);

        utility1 = new HBaseTestingUtility(conf1);
        utility1.startMiniZKCluster();
        MiniZooKeeperCluster miniZK = utility1.getZkCluster();
        // Have to reset conf1 in case zk cluster location different
        // than default
        conf1 = utility1.getConfiguration();
        zkw1 = new ZooKeeperWatcher(conf1, "cluster1", null, true);
        admin = new ReplicationAdmin(conf1);
        LOG.info("Setup first Zk");

        // Base conf2 on conf1 so it gets the right zk cluster, and general cluster configs
        conf2 = HBaseConfiguration.create(conf1);
        conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
        conf2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
        conf2.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
        conf2.setBoolean("dfs.support.append", true);
        conf2.setBoolean("hbase.tests.use.shortcircuit.reads", false);

        utility2 = new HBaseTestingUtility(conf2);
        utility2.setZkCluster(miniZK);
        zkw2 = new ZooKeeperWatcher(conf2, "cluster2", null, true);

        //replicate from cluster 1 -> cluster 2, but not back again
        admin.addPeer("1", utility2.getClusterKey());

        LOG.info("Setup second Zk");
        utility1.startMiniCluster(2);
        utility2.startMiniCluster(2);
    }

    private static void setupDriver() throws Exception {
        LOG.info("Setting up phoenix driver");
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        // Forces server cache to be used
        props.put(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(2));
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        URL = getLocalClusterUrl(utility1);
        LOG.info("Connecting driver to "+URL);
        driver = initAndRegisterDriver(URL, new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testReplicationWithMutableIndexes() throws Exception {
        Connection conn = getConnection();

        //create the primary and index tables
        conn.createStatement().execute(
                "CREATE TABLE " + DATA_TABLE_FULL_NAME
                        + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        conn.createStatement().execute(
                "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                        + " (v1)");

        // make sure that the tables are empty, but reachable
        String query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        //make sure there is no data in the table
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // make sure the data tables are created on the remote cluster
        HBaseAdmin admin = utility1.getHBaseAdmin();
        HBaseAdmin admin2 = utility2.getHBaseAdmin();

        List<String> dataTables = new ArrayList<String>();
        dataTables.add(DATA_TABLE_FULL_NAME);
        dataTables.add(INDEX_TABLE_FULL_NAME);
        for (String tableName : dataTables) {
            HTableDescriptor desc = admin.getTableDescriptor(TableName.valueOf(tableName));

            //create it as-is on the remote cluster
            admin2.createTable(desc);

            LOG.info("Enabling replication on source table: "+tableName);
            HColumnDescriptor[] cols = desc.getColumnFamilies();
            assertEquals(1, cols.length);
            // add the replication scope to the column
            HColumnDescriptor col = desc.removeFamily(cols[0].getName());
            col.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
            desc.addFamily(col);
            //disable/modify/enable table so it has replication enabled
            admin.disableTable(desc.getTableName());
            admin.modifyTable(tableName, desc);
            admin.enableTable(desc.getTableName());
            LOG.info("Replication enabled on source table: "+tableName);
        }


        // load some data into the source cluster table
        PreparedStatement stmt =
                conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a"); // k
        stmt.setString(2, "x"); // v1 <- has index
        stmt.setString(3, "1"); // v2
        stmt.execute();
        conn.commit();

        // make sure the index is working as expected
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertFalse(rs.next());
        conn.close();

        /*
         Validate that we have replicated the rows to the remote cluster
        */

        // other table can't be reached through Phoenix right now - would need to change how we
        // lookup tables. For right now, we just go through an HTable
        LOG.info("Looking up tables in replication target");
        TableName[] tables = admin2.listTableNames();
        HTable remoteTable = new HTable(utility2.getConfiguration(), tables[0]);
        for (int i = 0; i < REPLICATION_RETRIES; i++) {
            if (i >= REPLICATION_RETRIES - 1) {
                fail("Waited too much time for put replication on table " + remoteTable
                        .getTableDescriptor().getNameAsString());
            }
            if (ensureAnyRows(remoteTable)) {
                break;
            }
            LOG.info("Sleeping for " + REPLICATION_WAIT_TIME_MILLIS
                    + " for edits to get replicated");
            Thread.sleep(REPLICATION_WAIT_TIME_MILLIS);
        }
        remoteTable.close();
    }

    private boolean ensureAnyRows(HTable remoteTable) throws IOException {
        Scan scan = new Scan();
        scan.setRaw(true);
        ResultScanner scanner = remoteTable.getScanner(scan);
        boolean found = false;
        for (Result r : scanner) {
            LOG.info("got row: " + r);
            found = true;
        }
        scanner.close();
        return found;
    }

    private static Connection getConnection() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return DriverManager.getConnection(URL, props);
    }
}