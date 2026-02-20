/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication;

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.replication.ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.FailoverPhoenixConnection;
import org.apache.phoenix.jdbc.HABaseIT;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HighAvailabilityGroup;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.replication.tool.LogFileAnalyzer;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogGroupIT extends HABaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroupIT.class);

  private static String zkUrl;
  private static String peerZkUrl;

  @Rule
  public TestName name = new TestName();

  private Properties clientProps = new Properties();
  private String haGroupName;
  private PhoenixHAAdmin haAdmin1;
  private HighAvailabilityGroup haGroup;
  private ReplicationLogGroup logGroup;

  @BeforeClass
  public static void doSetup() throws Exception {
    conf1.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 20);
    CLUSTERS.start();
    zkUrl = CLUSTERS.getZkUrl1();
    peerZkUrl = CLUSTERS.getZkUrl2();
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    CLUSTERS.close();
  }

  @Before
  public void beforeTest() throws Exception {
    LOG.info("Starting test {}", name.getMethodName());
    haGroupName = name.getMethodName();
    haAdmin1 = CLUSTERS.getHaAdmin1();
    clientProps = HighAvailabilityTestingUtility.getHATestProperties();
    clientProps.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
    CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
    haGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), clientProps);
    LOG.info("Initialized haGroup {} with URL {}", haGroup, CLUSTERS.getJdbcHAUrl());

    // Update the state to ACTIVE_IN_SYNC
    HAGroupStoreRecord haGroupStoreRecord =
      new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
        HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), peerZkUrl, CLUSTERS.getMasterAddress1(),
        CLUSTERS.getMasterAddress2(), fallbackUri.toString(), standbyUri.toString(), 0L);
    haAdmin1.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);
    logGroup = getReplicationLogGroup();
  }

  @After
  public void afterTest() throws Exception {
    LOG.info("Starting cleanup for test {}", name.getMethodName());
    logGroup.close();
    LOG.info("Ending cleanup for test {}", name.getMethodName());
  }

  private ReplicationLogGroup getReplicationLogGroup() throws IOException {
    HRegionServer rs = CLUSTERS.getHBaseCluster1().getHBaseCluster().getRegionServer(0);
    return ReplicationLogGroup.get(conf1, rs.getServerName(), haGroupName);
  }

  private Map<String, List<Mutation>> groupLogsByTable() throws Exception {
    LogFileAnalyzer analyzer = new LogFileAnalyzer();
    analyzer.setConf(conf1);
    Path standByLogDir = logGroup.getStandbyShardManager().getRootDirectoryPath();
    LOG.info("Analyzing log files at {}", standByLogDir);
    String[] args = { "--check", standByLogDir.toString() };
    assertEquals(0, analyzer.run(args));
    return analyzer.groupLogsByTable(standByLogDir.toString());
  }

  private int getCountForTable(Map<String, List<Mutation>> logsByTable, String tableName)
    throws Exception {
    List<Mutation> mutations = logsByTable.get(tableName);
    return mutations != null ? mutations.size() : 0;
  }

  private void verifyReplication(Map<String, Integer> expected) throws Exception {
    // first close the logGroup
    logGroup.close();
    Map<String, List<Mutation>> mutationsByTable = groupLogsByTable();
    dumpTableLogCount(mutationsByTable);
    for (Map.Entry<String, Integer> entry : expected.entrySet()) {
      String tableName = entry.getKey();
      int expectedMutationCount = entry.getValue();
      List<Mutation> mutations = mutationsByTable.get(tableName);
      int actualMutationCount = mutations != null ? mutations.size() : 0;
      try {
        if (!tableName.equals(SYSTEM_CATALOG_NAME)) {
          assertEquals(String.format("For table %s", tableName), expectedMutationCount,
            actualMutationCount);
        } else {
          // special handling for syscat
          assertTrue("For SYSCAT", actualMutationCount >= expectedMutationCount);
        }
      } catch (AssertionError e) {
        // create a regular connection
        try (Connection conn = DriverManager.getConnection(CLUSTERS.getJdbcUrl1(haGroup))) {
          TestUtil.dumpTable(conn, TableName.valueOf(tableName));
          throw e;
        }
      }
    }
  }

  private void dumpTableLogCount(Map<String, List<Mutation>> mutationsByTable) {
    LOG.info("Dump table log count for test {}", name.getMethodName());
    for (Map.Entry<String, List<Mutation>> table : mutationsByTable.entrySet()) {
      LOG.info("#Log entries for {} = {}", table.getKey(), table.getValue().size());
    }
  }

  private void moveRegionToServer(TableName tableName, ServerName sn) throws Exception {
    HBaseTestingUtility util = CLUSTERS.getHBaseCluster1();
    try (RegionLocator locator = util.getConnection().getRegionLocator(tableName)) {
      String regEN = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      while (!sn.equals(locator.getAllRegionLocations().get(0).getServerName())) {
        LOG.info("Moving region {} of table {} to server {}", regEN, tableName, sn);
        util.getAdmin().move(Bytes.toBytes(regEN), sn);
        Thread.sleep(100);
      }
      LOG.info("Moved region {} of table {} to server {}", regEN, tableName, sn);
    }
  }

  private PhoenixTestBuilder.SchemaBuilder createViewHierarchy() throws Exception {
    // Define the test schema.
    // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
    // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
    // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
    final PhoenixTestBuilder.SchemaBuilder schemaBuilder =
      new PhoenixTestBuilder.SchemaBuilder(CLUSTERS.getJdbcHAUrl());
    PhoenixTestBuilder.SchemaBuilder.ConnectOptions connectOptions =
      new PhoenixTestBuilder.SchemaBuilder.ConnectOptions();
    connectOptions.setConnectProps(clientProps);
    PhoenixTestBuilder.SchemaBuilder.TableOptions tableOptions =
      PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
    PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions globalViewOptions =
      PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions.withDefaults();
    PhoenixTestBuilder.SchemaBuilder.TenantViewOptions tenantViewWithOverrideOptions =
      PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
    PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions tenantViewIndexOverrideOptions =
      PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions.withDefaults();
    schemaBuilder.withConnectOptions(connectOptions).withTableOptions(tableOptions)
      .withGlobalViewOptions(globalViewOptions).withTenantViewOptions(tenantViewWithOverrideOptions)
      .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildWithNewTenant();
    return schemaBuilder;
  }

  @Test
  public void testAppendAndSync() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName1 = "I_" + generateUniqueName();
    final String indexName2 = "I_" + generateUniqueName();
    final String indexName3 = "L_" + generateUniqueName();
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      String ddl = String.format("create table %s (id1 integer not null, "
        + "id2 integer not null, val1 varchar, val2 varchar "
        + "constraint pk primary key (id1, id2))", tableName);
      conn.createStatement().execute(ddl);
      ddl = String.format("create index %s on %s (val1) include (val2)", indexName1, tableName);
      conn.createStatement().execute(ddl);
      ddl = String.format("create index %s on %s (val2) include (val1)", indexName2, tableName);
      conn.createStatement().execute(ddl);
      ddl = String.format("create local index %s on %s (id2,val1) include (val2)", indexName3,
        tableName);
      conn.createStatement().execute(ddl);
      conn.commit();
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?)");
      // upsert 50 rows
      int rowCount = 50;
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
          stmt.setString(4, null);
          stmt.executeUpdate();
        }
        conn.commit();
      }
      // do some atomic upserts which will be ignored and therefore not replicated
      stmt = conn.prepareStatement(
        "upsert into " + tableName + " VALUES(?, ?, ?) " + "ON DUPLICATE KEY IGNORE");
      conn.setAutoCommit(true);
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 2; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, null);
          assertEquals(0, stmt.executeUpdate());
        }
      }
      // verify the correctness of the index
      // TODO Index tool test API doesn't work with Failover connection
      // IndexToolIT.verifyIndexTable(conf1, tableName, indexName1, conn);
      // verify replication
      Map<String, Integer> expected = Maps.newHashMap();
      // mutation count will be equal to row count since the atomic upsert mutations will be
      // ignored and therefore not replicated
      expected.put(tableName, rowCount * 3); // Put + Delete + local index update
      // for index1 unverified + verified + delete (Delete column)
      expected.put(indexName1, rowCount * 3);
      // for index2 unverified + verified since the null column is part of row key
      expected.put(indexName2, rowCount * 2);
      // we didn't create any tenant views so no change in the syscat entries
      expected.put(SYSTEM_CATALOG_NAME, 0);
      expected.put(SYSTEM_CHILD_LINK_NAME, 0);
      verifyReplication(expected);
    }
  }

  @Test
  public void testAppendAndSyncNoIndex() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      String ddl = String.format("create table %s (id1 integer not null, "
        + "id2 integer not null, val1 varchar, val2 varchar "
        + "constraint pk primary key (id1, id2))", tableName);
      conn.createStatement().execute(ddl);
      conn.commit();
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?)");
      // upsert 50 rows
      int rowCount = 50;
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
          stmt.setString(4, null);
          stmt.executeUpdate();
        }
        conn.commit();
      }
      // verify replication
      Map<String, Integer> expected = Maps.newHashMap();
      // mutation count will be equal to row count since the atomic upsert mutations will be
      // ignored and therefore not replicated
      expected.put(tableName, rowCount * 2); // Put + Delete
      verifyReplication(expected);
    }
  }

  /**
   * This test simulates RS crashes in the middle of write transactions after the edits have been
   * written to the WAL but before they have been replicated to the standby cluster. Those edits
   * will be replicated when the WAL is replayed.
   */
  @Test
  public void testWALRestore() throws Exception {
    HBaseTestingUtility util = CLUSTERS.getHBaseCluster1();
    MiniHBaseCluster cluster = util.getHBaseCluster();
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    TableName table = TableName.valueOf(tableName);
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      String ddl = String.format("create table %s (id1 integer not null, "
        + "id2 integer not null, val1 varchar, val2 varchar "
        + "constraint pk primary key (id1, id2))", tableName);
      conn.createStatement().execute(ddl);
      ddl = String.format("create index %s on %s (val1) include (val2)", indexName, tableName);
      conn.createStatement().execute(ddl);
      conn.commit();
    }
    // Mini cluster by default comes with only 1 RS. Starting a second RS so that
    // we can kill the RS
    JVMClusterUtil.RegionServerThread rs2 = cluster.startRegionServer();
    ServerName sn2 = rs2.getRegionServer().getServerName();
    // Assign some table regions to the new RS we started above
    moveRegionToServer(table, sn2);
    moveRegionToServer(TableName.valueOf(SYSTEM_CATALOG_NAME), sn2);
    moveRegionToServer(TableName.valueOf(SYSTEM_CHILD_LINK_NAME), sn2);
    int rowCount = 50;
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?)");
      // upsert 50 rows
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
          stmt.setString(4, null); // Generate a DeleteColumn cell
          stmt.executeUpdate();
        }
        // we want to simulate RS crash after updating memstore and WAL
        IndexRegionObserver.setIgnoreSyncReplicationForTesting(true);
        conn.commit();
      }
      // Create tenant views for syscat and child link replication
      // Mutations on SYSTEM.CATALOG and SYSTEM.CHILD_LINK are generated on the server side
      // and don't have the HAGroup attribute set
      // createViewHierarchy();
    } finally {
      IndexRegionObserver.setIgnoreSyncReplicationForTesting(false);
    }
    // Kill the RS
    cluster.killRegionServer(rs2.getRegionServer().getServerName());
    Threads.sleep(20000); // just to be sure that the kill has fully started.
    // Regions will be re-opened and the WAL will be replayed
    util.waitUntilAllRegionsAssigned(table);
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      Map<String, Integer> expected = Maps.newHashMap();
      // For each row 1 Put + 1 Delete (DeleteColumn)
      expected.put(tableName, rowCount * 2);
      // unverified + verified + delete (Delete column)
      expected.put(indexName, rowCount * 3);
      // 1 tenant view was created
      // expected.put(SYSTEM_CHILD_LINK_NAME, 1);
      // atleast 1 log entry for syscat
      // expected.put(SYSTEM_CATALOG_NAME, 1);
      verifyReplication(expected);
    }
  }

  @Ignore("Mutations on SYSTEM.CATALOG and SYSTEM.CHILD_LINK are generated on the server side and don't have the HAGroup attribute set")
  public void testSystemTables() throws Exception {
    createViewHierarchy();
    Map<String, List<Mutation>> logsByTable = groupLogsByTable();
    dumpTableLogCount(logsByTable);
    // find all the log entries for system tables
    Map<String,
      List<Mutation>> systemTables = logsByTable.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(QueryConstants.SYSTEM_SCHEMA_NAME))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    // there should be only 2 entries CATALOG, CHILD_LINK
    assertEquals(2, systemTables.size());
    assertEquals(1, getCountForTable(systemTables, SYSTEM_CHILD_LINK_NAME));
    assertTrue(getCountForTable(systemTables, SYSTEM_CATALOG_NAME) > 0);
  }
}
