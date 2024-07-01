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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.phoenix.coprocessor.ReplicationSinkEndpoint;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests to replicate Phoenix metadata attributes like tenant id, schema, logical table name,
 * table type etc over to the Sink cluster. For this, we need to set two important attributes:
 * 1. hbase.coprocessor.regionserver.classes
 * 2. phoenix.append.metadata.to.wal
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationWithWALAnnotationIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationWithWALAnnotationIT.class);

    private static final String SCHEMA_NAME = generateUniqueName();
    private static final String DATA_TABLE_NAME = generateUniqueName();
    private static final String INDEX_TABLE_NAME = "IDX_" + DATA_TABLE_NAME;
    private static final String TENANT_VIEW_NAME = generateUniqueName();
    private static final String TENANT_VIEW_INDEX_NAME = "IDX_" + TENANT_VIEW_NAME;
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, DATA_TABLE_NAME);
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, INDEX_TABLE_NAME);

    private static final long REPLICATION_WAIT_TIME_MILLIS = 10000;

    private static String url1;
    private static String url2;

    protected static Configuration conf1 = HBaseConfiguration.create();
    protected static Configuration conf2;

    protected static ZKWatcher zkw1;
    protected static ZKWatcher zkw2;

    protected static HBaseTestingUtility utility1;
    protected static HBaseTestingUtility utility2;
    protected static final int REPLICATION_RETRIES = 10;

    protected static final byte[] tableName = Bytes.toBytes("test");
    protected static final byte[] row = Bytes.toBytes("row");

    private static Map<String, String> props;

    @BeforeClass
    public static synchronized void setUpBeforeClass() throws Exception {
        setupConfigsAndStartCluster();
        props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(2));
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        url1 = getLocalClusterUrl(utility1);
        url2 = getLocalClusterUrl(utility2);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        utility1.shutdownMiniCluster();
        utility2.shutdownMiniCluster();
    }

    private static void setupConfigsAndStartCluster() throws Exception {
        conf1.setInt("zookeeper.recovery.retry", 1);
        conf1.setInt("zookeeper.recovery.retry.intervalmill", 10);
        conf1.setBoolean("dfs.support.append", true);
        conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
        conf1.setInt("replication.stats.thread.period.seconds", 5);
        conf1.setBoolean("hbase.tests.use.shortcircuit.reads", false);
        // The most important configs:
        // 1. hbase.coprocessor.regionserver.classes
        // 2. phoenix.append.metadata.to.wal
        conf1.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
                ReplicationSinkEndpoint.class.getName());
        conf1.setBoolean(IndexRegionObserver.PHOENIX_APPEND_METADATA_TO_WAL, true);
        conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
        setUpConfigForMiniCluster(conf1);

        utility1 = new HBaseTestingUtility(conf1);
        utility1.startMiniZKCluster();

        conf1 = utility1.getConfiguration();
        zkw1 = new ZKWatcher(conf1, "cluster1", null, true);
        Admin admin = ConnectionFactory.createConnection(conf1).getAdmin();

        // Base conf2 on conf1 so it gets the right zk cluster, and general cluster configs
        conf2 = HBaseConfiguration.create(conf1);
        conf2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
        conf2.setBoolean("dfs.support.append", true);
        conf2.setBoolean("hbase.tests.use.shortcircuit.reads", false);

        utility2 = new HBaseTestingUtility(conf2);
        utility2.startMiniZKCluster();
        zkw2 = new ZKWatcher(conf2, "cluster2", null, true);

        utility1.startMiniCluster(2);
        utility2.startMiniCluster(2);

        admin.addReplicationPeer("1",
                ReplicationPeerConfig.newBuilder().setClusterKey(utility2.getClusterKey()).build());
    }

    private boolean isReplicationSinkEndpointEnabled() {
        // true for 2.4.16+ or 2.5.3+ versions, false otherwise
        String hbaseVersion = VersionInfo.getVersion();
        String[] versionArr = hbaseVersion.split("\\.");
        int majorVersion = Integer.parseInt(versionArr[0]);
        int minorVersion = Integer.parseInt(versionArr[1]);
        int patchVersion = Integer.parseInt(versionArr[2].split("-")[0]);
        if (majorVersion > 2) {
            return true;
        }
        if (majorVersion < 2) {
            return false;
        }
        if (minorVersion > 5) {
            return true;
        }
        if (minorVersion < 4) {
            return false;
        }
        if (minorVersion == 4) {
            return patchVersion >= 16;
        }
        return patchVersion >= 3;
    }

    @Test
    public void testReplicationWithWALExtendedAttributes() throws Exception {
        Assume.assumeTrue("Replication sink endpoint on hbase versions 2.4.16+ or 2.5.3+",
                isReplicationSinkEndpointEnabled());
        // register driver for url1
        driver = initAndRegisterTestDriver(url1, new ReadOnlyProps(props.entrySet().iterator()));
        Connection primaryConnection = getConnection(url1);

        primaryConnection.createStatement()
                .execute(String.format("DROP TABLE IF EXISTS %s CASCADE", DATA_TABLE_FULL_NAME));

        //create the primary and index tables
        primaryConnection.createStatement().execute(
                String.format("CREATE TABLE %s (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) "
                        + "REPLICATION_SCOPE = 1", DATA_TABLE_FULL_NAME));
        primaryConnection.createStatement()
                .execute(String.format("CREATE INDEX %s ON %s (v1)", INDEX_TABLE_NAME, DATA_TABLE_FULL_NAME));

        String query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        ResultSet rs = primaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = primaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // de-register drive
        destroyDriver(driver);

        // register driver for url2
        driver = initAndRegisterTestDriver(url2, new ReadOnlyProps(props.entrySet().iterator()));
        Connection secondaryConnection = getConnection(url2);

        secondaryConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s CASCADE",
                DATA_TABLE_FULL_NAME));

        //create the primary and index tables
        secondaryConnection.createStatement().execute(
                String.format("CREATE TABLE %s (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) "
                        + "REPLICATION_SCOPE = 1", DATA_TABLE_FULL_NAME));
        secondaryConnection.createStatement()
                .execute(String.format("CREATE INDEX %s ON %s (v1)", INDEX_TABLE_NAME, DATA_TABLE_FULL_NAME));

        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = secondaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = secondaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // add coproc to verify whether required attributes are getting replicated to the Sink side
        TestUtil.addCoprocessor(secondaryConnection, DATA_TABLE_FULL_NAME,
                TestCoprocessorForWALAnnotationAtSink.class);

        // de-register drive
        destroyDriver(driver);

        // register driver for url1
        driver = initAndRegisterTestDriver(url1, new ReadOnlyProps(props.entrySet().iterator()));

        primaryConnection = getConnection(url1);

        // load some data into the source cluster table
        PreparedStatement stmt =
                primaryConnection.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a1");
        stmt.setString(2, "x1");
        stmt.setString(3, "11");
        stmt.execute();

        stmt.setString(1, "a2");
        stmt.setString(2, "x2");
        stmt.setString(3, "12");
        stmt.execute();

        primaryConnection.commit();

        // make sure the index is working as expected
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = primaryConnection.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("x2", rs.getString(1));
        assertFalse(rs.next());
        primaryConnection.close();

        assertReplicatedData(DATA_TABLE_FULL_NAME);

        // de-register drive
        destroyDriver(driver);

        // register driver for url1
        driver = initAndRegisterTestDriver(url1, new ReadOnlyProps(props.entrySet().iterator()));
        primaryConnection = getConnection(url1);
        primaryConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s CASCADE",
                DATA_TABLE_FULL_NAME));

        // de-register drive
        destroyDriver(driver);

        // register driver for url2
        driver = initAndRegisterTestDriver(url2, new ReadOnlyProps(props.entrySet().iterator()));
        secondaryConnection = getConnection(url2);
        secondaryConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s CASCADE",
                DATA_TABLE_FULL_NAME));

        // de-register drive
        destroyDriver(driver);
    }

    @Test
    public void testReplicationWithWALExtendedAttributesWithTenants() throws Exception {
        Assume.assumeTrue("Replication sink endpoint on hbase versions 2.4.16+ or 2.5.3+",
                isReplicationSinkEndpointEnabled());
        // register driver for url1
        driver = initAndRegisterTestDriver(url1, new ReadOnlyProps(props.entrySet().iterator()));
        Connection primaryConnection = getConnection(url1);

        primaryConnection.createStatement()
                .execute(String.format("DROP TABLE IF EXISTS %s CASCADE", DATA_TABLE_FULL_NAME));

        //create the primary and index tables
        primaryConnection.createStatement().execute(
                String.format("CREATE TABLE %s (TENANT_ID VARCHAR(15) NOT NULL, k VARCHAR NOT NULL, v1 "
                        + "VARCHAR, v2 VARCHAR CONSTRAINT pk PRIMARY KEY (TENANT_ID, k)) "
                        + "REPLICATION_SCOPE = 1, MULTI_TENANT = true", DATA_TABLE_FULL_NAME));
        primaryConnection.createStatement()
                .execute(String.format("CREATE INDEX %s ON %s (v1)", INDEX_TABLE_NAME, DATA_TABLE_FULL_NAME));

        Connection primaryTenantConnection = getTenantConnection(url1, "tenant01");

        primaryTenantConnection.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s ",
                TENANT_VIEW_NAME, DATA_TABLE_FULL_NAME));
        primaryTenantConnection.createStatement()
                .execute(String.format("CREATE INDEX %s ON %s (v1)", TENANT_VIEW_INDEX_NAME, TENANT_VIEW_NAME));

        String query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        ResultSet rs = primaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = primaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + TENANT_VIEW_NAME;
        rs = primaryTenantConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + TENANT_VIEW_INDEX_NAME;
        rs = primaryTenantConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // de-register drive
        destroyDriver(driver);

        // register driver for url2
        driver = initAndRegisterTestDriver(url2, new ReadOnlyProps(props.entrySet().iterator()));
        Connection secondaryConnection = getConnection(url2);

        secondaryConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s CASCADE",
                DATA_TABLE_FULL_NAME));

        //create the primary and index tables
        secondaryConnection.createStatement().execute(
                String.format("CREATE TABLE %s (TENANT_ID VARCHAR(15) NOT NULL, k VARCHAR NOT NULL, v1 "
                        + "VARCHAR, v2 VARCHAR CONSTRAINT pk PRIMARY KEY (TENANT_ID, k)) "
                        + "REPLICATION_SCOPE = 1, MULTI_TENANT = true", DATA_TABLE_FULL_NAME));
        secondaryConnection.createStatement()
                .execute(String.format("CREATE INDEX %s ON %s (v1)", INDEX_TABLE_NAME, DATA_TABLE_FULL_NAME));

        Connection secondaryTenantConnection = getTenantConnection(url2, "tenant01");

        secondaryTenantConnection.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s ",
                TENANT_VIEW_NAME, DATA_TABLE_FULL_NAME));
        secondaryTenantConnection.createStatement()
                .execute(String.format("CREATE INDEX %s ON %s (v1)", TENANT_VIEW_INDEX_NAME, TENANT_VIEW_NAME));

        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = secondaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = secondaryConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + TENANT_VIEW_NAME;
        rs = secondaryTenantConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        query = "SELECT * FROM " + TENANT_VIEW_INDEX_NAME;
        rs = secondaryTenantConnection.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // add coproc to verify whether required attributes are getting replicated to the Sink side
        TestUtil.addCoprocessor(secondaryConnection, DATA_TABLE_FULL_NAME,
                TestTenantCoprocessorForWALAnnotationAtSink.class);

        // de-register drive
        destroyDriver(driver);

        // register driver for url1
        driver = initAndRegisterTestDriver(url1, new ReadOnlyProps(props.entrySet().iterator()));

        primaryTenantConnection = getTenantConnection(url1, "tenant01");

        // load some data into the source cluster table
        PreparedStatement stmt =
                primaryTenantConnection.prepareStatement("UPSERT INTO " + TENANT_VIEW_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a1");
        stmt.setString(2, "x1");
        stmt.setString(3, "11");
        stmt.execute();

        stmt.setString(1, "a2");
        stmt.setString(2, "x2");
        stmt.setString(3, "12");
        stmt.execute();

        primaryTenantConnection.commit();

        // make sure the index is working as expected
        query = "SELECT * FROM " + TENANT_VIEW_INDEX_NAME;
        rs = primaryTenantConnection.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("x2", rs.getString(1));
        assertFalse(rs.next());
        primaryTenantConnection.close();

        assertReplicatedData(DATA_TABLE_FULL_NAME);

        // de-register drive
        destroyDriver(driver);

        // register driver for url1
        driver = initAndRegisterTestDriver(url1, new ReadOnlyProps(props.entrySet().iterator()));
        primaryConnection = getConnection(url1);
        primaryConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s CASCADE",
                DATA_TABLE_FULL_NAME));

        // de-register drive
        destroyDriver(driver);

        // register driver for url2
        driver = initAndRegisterTestDriver(url2, new ReadOnlyProps(props.entrySet().iterator()));
        secondaryConnection = getConnection(url2);
        secondaryConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s CASCADE",
                DATA_TABLE_FULL_NAME));

        // de-register drive
        destroyDriver(driver);
    }

    private void assertReplicatedData(String tableName) throws IOException, InterruptedException {
        LOGGER.info("Looking up tables in replication target");
        org.apache.hadoop.hbase.client.Connection hbaseConn =
                ConnectionFactory.createConnection(utility2.getConfiguration());
        Table remoteTable = hbaseConn.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < REPLICATION_RETRIES; i++) {
            if (i >= REPLICATION_RETRIES - 1) {
                fail("Waited too much time for put replication on table " + remoteTable
                        .getDescriptor().getTableName());
            }
            if (ensureRows(remoteTable, 2)) {
                break;
            }
            LOGGER.info("Sleeping for " + REPLICATION_WAIT_TIME_MILLIS
                    + " for edits to get replicated");
            Thread.sleep(REPLICATION_WAIT_TIME_MILLIS);
        }
        remoteTable.close();
    }

    private boolean ensureRows(Table remoteTable, int numRows) throws IOException {
        Scan scan = new Scan();
        scan.setRaw(true);
        ResultScanner scanner = remoteTable.getScanner(scan);
        int rows = 0;
        for (Result r : scanner) {
            LOGGER.info("got row: {}", r);
            rows++;
        }
        scanner.close();
        return rows == numRows;
    }

    private static Connection getConnection(String url) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return DriverManager.getConnection(url, props);
    }

    private static Connection getTenantConnection(String url, String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    public static class TestCoprocessorForWALAnnotationAtSink implements RegionCoprocessor, RegionObserver {

        @Override
        public Optional<RegionObserver> getRegionObserver() {
            return Optional.of(this);
        }

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                                   MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            String tenantId = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.TENANT_ID.toString()));
            String schemaName = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.SCHEMA_NAME.toString()));
            String logicalTableName = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.LOGICAL_TABLE_NAME.toString()));
            String tableType = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.TABLE_TYPE.toString()));

            LOGGER.info("TestCoprocessorForWALAnnotationAtSink preBatchMutate: tenantId: {}, schemaName: {}, "
                    + "logicalTableName: {}, tableType: {}", tenantId, schemaName, logicalTableName, tableType);

            if (tenantId != null || !SCHEMA_NAME.equals(schemaName) || !DATA_TABLE_NAME.equals(logicalTableName) ||
                    !PTableType.TABLE.getValue().toString().equals(tableType)) {
                throw new IOException("Replication Sink mutation attributes are not matching. Abort the mutation.");
            }
        }
    }


    public static class TestTenantCoprocessorForWALAnnotationAtSink implements RegionCoprocessor, RegionObserver {

        @Override
        public Optional<RegionObserver> getRegionObserver() {
            return Optional.of(this);
        }

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                                   MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            String tenantId = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.TENANT_ID.toString()));
            String schemaName = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.SCHEMA_NAME.toString()));
            String logicalTableName = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.LOGICAL_TABLE_NAME.toString()));
            String tableType = Bytes.toString(miniBatchOp.getOperation(0)
                    .getAttribute(MutationState.MutationMetadataType.TABLE_TYPE.toString()));

            LOGGER.info("TestCoprocessorForWALAnnotationAtSink preBatchMutate: tenantId: {}, schemaName: {}, "
                    + "logicalTableName: {}, tableType: {}", tenantId, schemaName, logicalTableName, tableType);

            if (!"tenant01".equals(tenantId) || !"".equals(schemaName) || !TENANT_VIEW_NAME.equals(logicalTableName) ||
                    !PTableType.VIEW.getValue().toString().equals(tableType)) {
                throw new IOException("Replication Sink mutation attributes are not matching. Abort the mutation.");
            }
        }
    }

}