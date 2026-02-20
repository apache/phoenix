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
package org.apache.phoenix.mapreduce;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.PhoenixSyncTableMapper.SyncCounters;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixPhoenixSyncTableToolIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixPhoenixSyncTableToolIT.class);

  private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
  private static final String TEST_TABLE_NAME = "TEST_SYNC_TABLE";
  private static final int REPLICATION_WAIT_TIMEOUT_MS = 100000;
  private static final int REPLICATION_POLL_INTERVAL_MS = 500;

  private Connection sourceConnection;
  private Connection targetConnection;
  private String targetZkQuorum;

  @Rule
  public final TestName testName = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CLUSTERS.start(); // Starts both clusters and sets up replication
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    CLUSTERS.close();
  }

  @Before
  public void setUp() throws Exception {
    // Create Phoenix connections to both clusters
    String sourceJdbcUrl = "jdbc:phoenix:" + CLUSTERS.getZkUrl1();
    String targetJdbcUrl = "jdbc:phoenix:" + CLUSTERS.getZkUrl2();

    sourceConnection = DriverManager.getConnection(sourceJdbcUrl);
    targetConnection = DriverManager.getConnection(targetJdbcUrl);

    // Extract target ZK quorum for PhoenixSyncTableTool (format: host:port:znode)
    // Input format: "127.0.0.1\:52638::/hbase" → Output: "127.0.0.1:52638:/hbase"
    // Note: The backslash is a single character, not escaped in the actual string
    targetZkQuorum = CLUSTERS.getZkUrl2().replace("\\", "").replace("::", ":");
  }

  @After
  public void tearDown() throws Exception {
    dropTableIfExists(sourceConnection, TEST_TABLE_NAME);
    dropTableIfExists(targetConnection, TEST_TABLE_NAME);

    // Close connections
    if (sourceConnection != null) {
      sourceConnection.close();
    }
    if (targetConnection != null) {
      targetConnection.close();
    }
  }

  @Test
  public void testSyncTableWithDataDifference() throws Exception {
    createTableOnBothClusters(sourceConnection, targetConnection, TEST_TABLE_NAME);

    insertTestData(sourceConnection, 1, 1000);

    waitForReplication(targetConnection, TEST_TABLE_NAME, 1000, REPLICATION_WAIT_TIMEOUT_MS);

    verifyDataIdentical(sourceConnection, targetConnection, TEST_TABLE_NAME);

    introduceTargetDifferences();

    List<TestRow> sourceRowsBefore = queryAllRows(sourceConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + TEST_TABLE_NAME + " ORDER BY ID");
    List<TestRow> targetRowsBefore = queryAllRows(targetConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + TEST_TABLE_NAME + " ORDER BY ID");

    assertEquals(sourceRowsBefore, targetRowsBefore);

    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] args = new String[] { "--table-name", TEST_TABLE_NAME, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--chunk-size", "10240" };
    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);
    int exitCode = tool.run(args);
    Job job = tool.getJob();
    assertNotNull("Job should not be null", job);
    assertEquals(0, exitCode);
    Counters counters = job.getCounters();
    long chunksMismatched = counters.findCounter(SyncCounters.CHUNKS_MISMATCHED).getValue();
    assertEquals("Should have detected mismatched chunks", 4, chunksMismatched);
  }

  private void createTableOnBothClusters(Connection sourceConn, Connection targetConn,
    String tableName) throws SQLException {
    String ddl = "CREATE TABLE " + tableName + " (\n" + "    ID INTEGER NOT NULL PRIMARY KEY,\n"
      + "    NAME VARCHAR(50),\n" + "    NAME_VALUE BIGINT,\n" + "    UPDATED_DATE TIMESTAMP\n"
      + ") REPLICATION_SCOPE=1,UPDATE_CACHE_FREQUENCY = 0\n" + "SPLIT ON (500, 650, 800)";

    sourceConn.createStatement().execute(ddl);
    sourceConn.commit();
    // Clear cache to prevent it from affecting target cluster table creation.
    // Both region servers share the same JVM
    ((PhoenixConnection) sourceConn).getQueryServices().clearCache();

    ddl = "CREATE TABLE " + tableName + " (\n" + "    ID INTEGER NOT NULL PRIMARY KEY,\n"
      + "    NAME VARCHAR(50),\n" + "    NAME_VALUE BIGINT,\n" + "    UPDATED_DATE TIMESTAMP\n"
      + ") UPDATE_CACHE_FREQUENCY = 0\n" + "SPLIT ON (60, 100, 300, 525, 600, 900)";

    targetConn.createStatement().execute(ddl);
    targetConn.commit();
    ((PhoenixConnection) targetConn).getQueryServices().clearCache();
  }

  private void insertTestData(Connection conn, int startId, int endId) throws SQLException {
    String upsert = "UPSERT INTO " + TEST_TABLE_NAME
      + " (ID, NAME, NAME_VALUE, UPDATED_DATE) VALUES (?, ?, ?, ?)";
    PreparedStatement stmt = conn.prepareStatement(upsert);
    for (int i = startId; i <= endId; i++) {
      stmt.setInt(1, i);
      stmt.setString(2, "NAME_" + i);
      stmt.setLong(3, (long) i);
      stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
      stmt.executeUpdate();
      conn.commit();
    }
  }

  /**
   * Waits for HBase replication to complete by polling target cluster.
   */
  private void waitForReplication(Connection targetConn, String tableName, int expectedRows,
    long timeoutMs) throws Exception {
    long startTime = System.currentTimeMillis();
    String countQuery = "SELECT COUNT(*) FROM " + tableName;

    while (System.currentTimeMillis() - startTime < timeoutMs) {
      ResultSet rs = targetConn.createStatement().executeQuery(countQuery);
      rs.next();
      int count = rs.getInt(1);
      rs.close();

      if (count == expectedRows) {
        return;
      }

      Thread.sleep(REPLICATION_POLL_INTERVAL_MS);
    }

    fail("Replication timeout: expected " + expectedRows + " rows on target");
  }

  /**
   * Verifies that source and target have identical data.
   */
  private void verifyDataIdentical(Connection sourceConn, Connection targetConn, String tableName)
    throws SQLException {
    String query = "SELECT ID, NAME, NAME_VALUE FROM " + tableName + " ORDER BY ID";
    List<TestRow> sourceRows = queryAllRows(sourceConn, query);
    List<TestRow> targetRows = queryAllRows(targetConn, query);

    assertEquals("Row counts should match", sourceRows.size(), targetRows.size());

    for (int i = 0; i < sourceRows.size(); i++) {
      assertEquals("Row " + i + " should be identical", sourceRows.get(i), targetRows.get(i));
    }
  }

  private void introduceTargetDifferences() throws SQLException {
    String updateValue = "UPSERT INTO " + TEST_TABLE_NAME + " (ID, NAME) VALUES (65, 'NAME_65')";
    PreparedStatement ps1 = targetConnection.prepareStatement(updateValue);
    ps1.executeUpdate();

    String updateValue2 = "UPSERT INTO " + TEST_TABLE_NAME + " (ID, NAME) VALUES (300, 'NAME_300')";
    PreparedStatement ps2 = targetConnection.prepareStatement(updateValue2);
    ps2.executeUpdate();

    String updateValue3 = "UPSERT INTO " + TEST_TABLE_NAME + " (ID, NAME) VALUES (500, 'NAME_500')";
    PreparedStatement ps3 = targetConnection.prepareStatement(updateValue3);
    ps3.executeUpdate();

    String updateValue4 = "UPSERT INTO " + TEST_TABLE_NAME + " (ID, NAME) VALUES (650, 'NAME_650')";
    PreparedStatement ps4 = targetConnection.prepareStatement(updateValue4);
    ps4.executeUpdate();

    targetConnection.commit();
  }

  /**
   * Queries all rows from a table.
   */
  private List<TestRow> queryAllRows(Connection conn, String query) throws SQLException {
    List<TestRow> rows = new ArrayList<>();

    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {

      while (rs.next()) {
        TestRow row = new TestRow();
        row.id = rs.getInt("ID");
        row.name = rs.getString("NAME");
        row.name_value = rs.getLong("NAME_VALUE");
        rows.add(row);
      }
    }

    return rows;
  }

  /**
   * Drops a table if it exists.
   */
  private void dropTableIfExists(Connection conn, String tableName) {
    try {
      conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      conn.commit();
    } catch (SQLException e) {
      LOGGER.warn("Failed to drop table {}: {}", tableName, e.getMessage());
    }
  }

  private static class TestRow {
    int id;
    String name;
    long name_value;

    public boolean equals(Object o) {
      if (!(o instanceof TestRow)) return false;
      TestRow other = (TestRow) o;
      return id == other.id && Objects.equals(name, other.name) && name_value == other.name_value;
    }
  }
}
