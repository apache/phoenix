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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.PhoenixSyncTableMapper.SyncCounters;
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRow;
import org.apache.phoenix.mapreduce.PhoenixSyncTableTool;
import org.apache.phoenix.query.BaseTest;
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
public class PhoenixSyncTableToolIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableToolIT.class);

  @Rule
  public final TestName testName = new TestName();

  private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
  private static final int REPLICATION_WAIT_TIMEOUT_MS = 10000;

  private Connection sourceConnection;
  private Connection targetConnection;
  private String targetZkQuorum;
  private String uniqueTableName;

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
    uniqueTableName = BaseTest.generateUniqueName();

    targetZkQuorum = String.format("%s:%d:/hbase",
      CLUSTERS.getHBaseCluster2().getConfiguration().get("hbase.zookeeper.quorum"),
      CLUSTERS.getHBaseCluster2().getZkCluster().getClientPort());
  }

  @After
  public void tearDown() throws Exception {
    if (sourceConnection != null && uniqueTableName != null) {
      try {
        dropTableIfExists(sourceConnection, uniqueTableName);
        dropTableIfExists(sourceConnection, uniqueTableName + "_IDX"); // For index test
        cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
        cleanupCheckpointTable(sourceConnection, uniqueTableName + "_IDX", targetZkQuorum);
      } catch (Exception e) {
        LOGGER.warn("Failed to cleanup tables for {}: {}", uniqueTableName, e.getMessage());
      }
    }

    if (targetConnection != null && uniqueTableName != null) {
      try {
        dropTableIfExists(targetConnection, uniqueTableName);
        dropTableIfExists(targetConnection, uniqueTableName + "_IDX"); // For index test
      } catch (Exception e) {
        LOGGER.warn("Failed to cleanup tables on target for {}: {}", uniqueTableName,
          e.getMessage());
      }
    }

    if (sourceConnection != null) {
      sourceConnection.close();
    }
    if (targetConnection != null) {
      targetConnection.close();
    }
  }

  @Test
  public void testSyncTableWithDataDifference() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    Job job = runSyncToolWithLargeChunks(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(job);

    validateSyncCounters(counters, 10, 10, 1, 3);

    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    validateCheckpointEntries(checkpointEntries, uniqueTableName, targetZkQuorum, 10, 10, 1, 3, 4,
      3);
  }

  @Test
  public void testSyncTableWithDeletedRowsOnTarget() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    deleteRowsOnTarget(targetConnection, uniqueTableName, 1, 4, 9);

    // Verify row counts differ between source and target
    int sourceCount = getRowCount(sourceConnection, uniqueTableName);
    int targetCount = getRowCount(targetConnection, uniqueTableName);
    assertEquals("Source should have 10 rows", 10, sourceCount);
    assertEquals("Target should have 7 rows (3 deleted)", 7, targetCount);

    Job job = runSyncTool(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(job);

    validateSyncCounters(counters, 10, 10, 7, 3);
  }

  @Test
  public void testSyncMultiTenantSaltedTableWithTenantSpecificDifferences() throws Exception {
    String[] tenantIds = new String[] { "TENANT_001", "TENANT_002", "TENANT_003" };
    int rowsPerTenant = 10;

    createMultiTenantSaltedTableOnBothClusters(sourceConnection, targetConnection, uniqueTableName);

    for (String tenantId : tenantIds) {
      Connection tenantSourceConn = getTenantConnection(sourceConnection, tenantId);
      insertMultiTenantTestData(tenantSourceConn, uniqueTableName, 1, rowsPerTenant);
      tenantSourceConn.close();
    }

    waitForReplication(targetConnection, uniqueTableName, 30);

    for (String tenantId : tenantIds) {
      withTenantConnections(tenantId,
        (sourceConn, targetConn) -> verifyDataIdentical(sourceConn, targetConn, uniqueTableName));
    }

    // Introduce differences specific to TENANT_002 on target cluster
    Connection tenant002TargetConnForUpdate = getTenantConnection(targetConnection, tenantIds[1]);
    introduceMultiTenantTargetDifferences(tenant002TargetConnForUpdate, uniqueTableName);
    tenant002TargetConnForUpdate.close();

    // Verify TENANT_001 and TENANT_003 still have identical data
    for (int i = 0; i < tenantIds.length; i++) {
      if (i == 1) continue; // Skip TENANT_002 as we introduced differences

      final String tenantId = tenantIds[i];
      withTenantConnections(tenantId, (sourceConn, targetConn) -> {
        List<TestRow> sourceRows = queryAllRows(sourceConn,
          "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
        List<TestRow> targetRows = queryAllRows(targetConn,
          "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
        assertEquals("Tenant " + tenantId + " should still have identical data", sourceRows,
          targetRows);
      });
    }

    // TENANT_001 has no differences, expect all rows verified
    Job job1 = runSyncTool(uniqueTableName, "--tenant-id", tenantIds[0]);
    SyncCountersResult counters1 = getSyncCounters(job1);
    validateSyncCounters(counters1, 10, 10, 10, 0);

    // TENANT_002 has 3 modified rows
    Job job2 = runSyncTool(uniqueTableName, "--tenant-id", tenantIds[1]);
    SyncCountersResult counters2 = getSyncCounters(job2);
    validateSyncCounters(counters2, 10, 10, 7, 3);
  }

  @Test
  public void testSyncTableWithTimeRangeFilter() throws Exception {
    createTableOnBothClusters(sourceConnection, targetConnection, uniqueTableName);

    // Insert data BEFORE the time range window
    insertTestData(sourceConnection, uniqueTableName, 1, 10);

    long startTime = System.currentTimeMillis();

    // Insert data WITHIN the time range window
    insertTestData(sourceConnection, uniqueTableName, 11, 20);

    long endTime = System.currentTimeMillis();

    // Insert data AFTER the time range window
    insertTestData(sourceConnection, uniqueTableName, 21, 30);

    // Wait for replication to complete
    waitForReplication(targetConnection, uniqueTableName, 30);

    // Verify all data replicated correctly
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    // Modify rows BEFORE startTime time
    upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { 3, 5, 8 },
      new String[] { "MODIFIED_NAME_3", "MODIFIED_NAME_5", "MODIFIED_NAME_8" });

    // Modify rows AFTER end time
    upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { 23, 25, 28 },
      new String[] { "MODIFIED_NAME_23", "MODIFIED_NAME_25", "MODIFIED_NAME_28" });

    // Run sync tool with time range covering ONLY the middle window
    Job job = runSyncTool(uniqueTableName, "--from-time", String.valueOf(startTime), "--to-time",
      String.valueOf(endTime));
    SyncCountersResult counters = getSyncCounters(job);

    validateSyncCounters(counters, 10, 10, 10, 0);
  }

  @Test
  public void testSyncTableCheckpointWithPartialRun() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Capture consistent time range for both runs (ensures checkpoint lookup will match)
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run sync tool for the FIRST time with explicit time range
    Job job1 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters1 = getSyncCounters(job1);

    validateSyncCounters(counters1, 10, 10, 7, 3);

    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    assertEquals("Should have checkpoint entries after first run", 14, checkpointEntries.size());

    // To mimic the partial run scenario, we do a full run and then delete some entry from
    // checkpoint table
    PhoenixSyncTableOutputRow mapperToDelete = null;
    PhoenixSyncTableOutputRow chunkToDelete = null;
    for (PhoenixSyncTableOutputRow entry : checkpointEntries) {
      if (
        PhoenixSyncTableOutputRow.Type.MAPPER_REGION.equals(entry.getType())
          && mapperToDelete == null
      ) {
        mapperToDelete = entry;
      } else
        if (PhoenixSyncTableOutputRow.Type.CHUNK.equals(entry.getType()) && chunkToDelete == null) {
          chunkToDelete = entry;
        } else if (mapperToDelete != null && chunkToDelete != null) {
          break;
        }
    }

    assertNotNull("Should have at least one MAPPER_REGION entry", mapperToDelete);
    assertNotNull("Should have at least one CHUNK entry", chunkToDelete);

    // Get counters from the chunk entry to be deleted
    long expectedSourceRows = chunkToDelete.getSourceRowsProcessed();
    long expectedTargetRows = chunkToDelete.getTargetRowsProcessed();

    // Delete the mapper entry and chunk entry
    int deletedCount = deleteCheckpointEntry(sourceConnection, uniqueTableName, targetZkQuorum,
      mapperToDelete.getStartRowKey(), chunkToDelete.getStartRowKey());
    assertEquals("Should have deleted one mapper and one chunk entry", 2, deletedCount);

    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterDelete =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    assertEquals("Should have fewer checkpoint entries after deletion", 2,
      checkpointEntries.size() - checkpointEntriesAfterDelete.size());

    // Run sync tool again with SAME time range - it should reprocess only the deleted chunk's data
    Job job2 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    // Second run should process exactly the same number of rows as the deleted chunk
    assertEquals("Second run should process same source rows as deleted chunk", expectedSourceRows,
      counters2.sourceRowsProcessed);
    assertEquals("Second run should process same target rows as deleted chunk", expectedTargetRows,
      counters2.targetRowsProcessed);

    // Verify checkpoint table now has entries for the reprocessed region
    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterRerun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    assertEquals("Should have same number of checkpoint entries after rerun",
      checkpointEntries.size(), checkpointEntriesAfterRerun.size());

    // Verify checkpoint entries match
    verifyCheckpointEntriesMatch(checkpointEntries, checkpointEntriesAfterRerun);
  }

  @Test
  public void testSyncTableIdempotentOnReRun() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Introduce differences on target to create mismatches
    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Capture consistent time range for both runs (ensures checkpoint lookup will match)
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run sync tool for the FIRST time
    Job job1 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters1 = getSyncCounters(job1);

    // Validate first run counters
    validateSyncCounters(counters1, 10, 10, 7, 3);

    // Query checkpoint table to verify entries were created
    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterFirstRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    assertEquals("Should have 14 checkpoint entries after first run", 14,
      checkpointEntriesAfterFirstRun.size());

    // Run sync tool for the SECOND time WITHOUT deleting any checkpoints (idempotent behavior)
    Job job2 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    // KEY VALIDATION: Second run should process ZERO rows (idempotent behavior)
    assertEquals("Second run should process ZERO source rows (all check pointed)", 0,
      counters2.sourceRowsProcessed);
    assertEquals("Second run should process ZERO target rows (all check pointed)", 0,
      counters2.targetRowsProcessed);
    assertEquals("Second run should detect ZERO mismatched chunks (already processed)", 0,
      counters2.chunksMismatched);
    assertEquals("Second run should verify ZERO chunks (already check pointed)", 0,
      counters2.chunksVerified);

    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterSecondRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    assertEquals("Checkpoint entries should be identical after idempotent run",
      checkpointEntriesAfterFirstRun, checkpointEntriesAfterSecondRun);
  }

  @Test
  public void testSyncTableWithSchemaAndTableNameOptions() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Introduce differences on target
    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Run sync tool with both --schema and --table-name options
    Job job = runSyncTool(uniqueTableName, "--schema", "");
    SyncCountersResult counters = getSyncCounters(job);

    // Validate counters
    validateSyncCounters(counters, 10, 10, 7, 3);
  }

  @Test
  public void testSyncTableInBackgroundMode() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] args = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--chunk-size", "1" };

    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);
    int exitCode = tool.run(args);

    Job job = tool.getJob();
    assertNotNull("Job should not be null", job);
    assertEquals("Tool should submit job successfully", 0, exitCode);

    boolean jobCompleted = job.waitForCompletion(true);
    assertTrue("Background job should complete successfully", jobCompleted);

    SyncCountersResult counters = new SyncCountersResult(job.getCounters());
    counters.logCounters(testName.getMethodName());

    validateSyncCounters(counters, 10, 10, 7, 3);
  }

  private void createTableOnBothClusters(Connection sourceConn, Connection targetConn,
    String tableName) throws SQLException {
    // For 10 rows: split source at 3, 5, 7 creating 4 regions
    String sourceDdl = buildStandardTableDdl(tableName, true, "3, 5, 7");
    executeTableCreation(sourceConn, sourceDdl);

    // For target: different split points (2, 4, 6, 8) creating 5 regions
    String targetDdl = buildStandardTableDdl(tableName, false, "2, 4, 6, 8");
    executeTableCreation(targetConn, targetDdl);
  }

  /**
   * Builds DDL for standard test table with common schema.
   */
  private String buildStandardTableDdl(String tableName, boolean withReplication,
    String splitPoints) {
    return String.format(
      "CREATE TABLE IF NOT EXISTS %s (\n" + "    ID INTEGER NOT NULL PRIMARY KEY,\n"
        + "    NAME VARCHAR(50),\n" + "    NAME_VALUE BIGINT,\n" + "    UPDATED_DATE TIMESTAMP\n"
        + ") %sUPDATE_CACHE_FREQUENCY=0\n" + "SPLIT ON (%s)",
      tableName, withReplication ? "REPLICATION_SCOPE=1, " : "REPLICATION_SCOPE=0,", splitPoints);
  }

  /**
   * Executes table creation DDL.
   */
  private void executeTableCreation(Connection conn, String ddl) throws SQLException {
    conn.createStatement().execute(ddl);
    conn.commit();
  }

  private void insertTestData(Connection conn, String tableName, int startId, int endId)
    throws SQLException {
    insertTestData(conn, tableName, startId, endId, System.currentTimeMillis());
  }

  /**
   * Waits for HBase replication to complete by polling target cluster.
   */
  private void waitForReplication(Connection targetConn, String tableName, int expectedRows)
    throws Exception {
    long startTime = System.currentTimeMillis();
    String countQuery = "SELECT COUNT(*) FROM " + tableName;

    while (
      System.currentTimeMillis() - startTime
          < (long) PhoenixSyncTableToolIT.REPLICATION_WAIT_TIMEOUT_MS
    ) {
      ResultSet rs = targetConn.createStatement().executeQuery(countQuery);
      rs.next();
      int count = rs.getInt(1);
      rs.close();

      if (count == expectedRows) {
        return;
      }
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

  private void introduceAndVerifyTargetDifferences(String tableName) throws SQLException {
    upsertRowsOnTarget(targetConnection, tableName, new int[] { 2, 5, 8 },
      new String[] { "MODIFIED_NAME_2", "MODIFIED_NAME_5", "MODIFIED_NAME_8" });

    List<TestRow> sourceRows = queryAllRows(sourceConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + tableName + " ORDER BY ID");
    List<TestRow> targetRows = queryAllRows(targetConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + tableName + " ORDER BY ID");
    assertEquals("Row count should be the same", sourceRows.size(), targetRows.size());
    assertNotEquals("Row values should differ after introducing differences", sourceRows,
      targetRows);
  }

  /**
   * Upserts multiple rows on target cluster with specified names.
   */
  private void upsertRowsOnTarget(Connection conn, String tableName, int[] ids, String[] names)
    throws SQLException {
    String upsert = "UPSERT INTO " + tableName + " (ID, NAME) VALUES (?, ?)";
    PreparedStatement stmt = conn.prepareStatement(upsert);

    for (int i = 0; i < ids.length; i++) {
      stmt.setInt(1, ids[i]);
      stmt.setString(2, names[i]);
      stmt.executeUpdate();
    }

    conn.commit();
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

  /**
   * Creates a multi-tenant salted table with 4 salt buckets on both clusters.
   */
  private void createMultiTenantSaltedTableOnBothClusters(Connection sourceConn,
    Connection targetConn, String tableName) throws SQLException {
    String sourceDdl = buildMultiTenantTableDdl(tableName, true);
    executeTableCreation(sourceConn, sourceDdl);

    String targetDdl = buildMultiTenantTableDdl(tableName, false);
    executeTableCreation(targetConn, targetDdl);
  }

  /**
   * Builds DDL for multi-tenant salted table.
   */
  private String buildMultiTenantTableDdl(String tableName, boolean withReplication) {
    return String.format(
      "CREATE TABLE IF NOT EXISTS %s (\n" + "    TENANT_ID VARCHAR(15) NOT NULL,\n"
        + "    ID INTEGER NOT NULL,\n" + "    NAME VARCHAR(50),\n" + "    NAME_VALUE BIGINT,\n"
        + "    UPDATED_DATE TIMESTAMP,\n" + "    CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)\n"
        + ") MULTI_TENANT=true, SALT_BUCKETS=4, %sUPDATE_CACHE_FREQUENCY=0",
      tableName, withReplication ? "REPLICATION_SCOPE=1, " : "");
  }

  /**
   * Gets a tenant-specific connection.
   */
  private Connection getTenantConnection(Connection baseConnection, String tenantId)
    throws SQLException {
    String jdbcUrl = baseConnection.unwrap(PhoenixConnection.class).getURL();
    String tenantJdbcUrl = jdbcUrl + ";TenantId=" + tenantId;
    return DriverManager.getConnection(tenantJdbcUrl);
  }

  /**
   * Executes an operation with tenant connections and ensures they are closed.
   */
  @FunctionalInterface
  private interface TenantConnectionConsumer {
    void accept(Connection sourceConn, Connection targetConn) throws SQLException;
  }

  private void withTenantConnections(String tenantId, TenantConnectionConsumer consumer)
    throws SQLException {
    try (Connection tenantSourceConn = getTenantConnection(sourceConnection, tenantId);
      Connection tenantTargetConn = getTenantConnection(targetConnection, tenantId)) {
      consumer.accept(tenantSourceConn, tenantTargetConn);
    }
  }

  /**
   * Inserts test data for a multi-tenant table using tenant-specific connection.
   */
  private void insertMultiTenantTestData(Connection tenantConn, String tableName, int startId,
    int endId) throws SQLException {
    insertTestData(tenantConn, tableName, startId, endId, System.currentTimeMillis());
  }

  /**
   * Introduces differences in the target cluster for multi-tenant table.
   */
  private void introduceMultiTenantTargetDifferences(Connection tenantConn, String tableName)
    throws SQLException {
    upsertRowsOnTarget(tenantConn, tableName, new int[] { 3, 7, 9 },
      new String[] { "MODIFIED_NAME_3", "MODIFIED_NAME_7", "MODIFIED_NAME_9" });
  }

  /**
   * Inserts test data with a specific timestamp for time-range testing.
   */
  private void insertTestData(Connection conn, String tableName, int startId, int endId,
    long timestamp) throws SQLException {
    String upsert =
      "UPSERT INTO " + tableName + " (ID, NAME, NAME_VALUE, UPDATED_DATE) VALUES (?, ?, ?, ?)";
    PreparedStatement stmt = conn.prepareStatement(upsert);
    Timestamp ts = new Timestamp(timestamp);
    for (int i = startId; i <= endId; i++) {
      stmt.setInt(1, i);
      stmt.setString(2, "NAME_" + i);
      stmt.setLong(3, (long) i);
      stmt.setTimestamp(4, ts);
      stmt.executeUpdate();
    }
    conn.commit();
  }

  /**
   * Deletes multiple rows from target cluster to create mismatches. This method accepts variable
   * number of row IDs to delete.
   */
  private void deleteRowsOnTarget(Connection conn, String tableName, int... rowIds)
    throws SQLException {
    String delete = "DELETE FROM " + tableName + " WHERE ID = ?";
    PreparedStatement stmt = conn.prepareStatement(delete);

    for (int id : rowIds) {
      stmt.setInt(1, id);
      stmt.executeUpdate();
    }
    conn.commit();
  }

  /**
   * Gets the row count for a table.
   */
  private int getRowCount(Connection conn, String tableName) throws SQLException {
    String countQuery = "SELECT COUNT(*) FROM " + tableName;
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(countQuery);
    rs.next();
    int count = rs.getInt(1);
    rs.close();
    stmt.close();
    return count;
  }

  /**
   * Queries the checkpoint table for entries matching the given table and target cluster. Retrieves
   * all columns for comprehensive validation.
   */
  private List<PhoenixSyncTableOutputRow> queryCheckpointTable(Connection conn, String tableName,
    String targetCluster) throws SQLException {
    List<PhoenixSyncTableOutputRow> entries = new ArrayList<>();
    String query = "SELECT TABLE_NAME, TARGET_CLUSTER, TYPE, FROM_TIME, TO_TIME, IS_DRY_RUN, "
      + "START_ROW_KEY, END_ROW_KEY, IS_FIRST_REGION, EXECUTION_START_TIME, EXECUTION_END_TIME, "
      + "STATUS, COUNTERS FROM PHOENIX_SYNC_TABLE_CHECKPOINT "
      + "WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ?";

    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, tableName);
    stmt.setString(2, targetCluster);
    ResultSet rs = stmt.executeQuery();

    while (rs.next()) {
      String typeStr = rs.getString("TYPE");
      String statusStr = rs.getString("STATUS");

      PhoenixSyncTableOutputRow entry = new PhoenixSyncTableOutputRow.Builder()
        .setTableName(rs.getString("TABLE_NAME")).setTargetCluster(rs.getString("TARGET_CLUSTER"))
        .setType(typeStr != null ? PhoenixSyncTableOutputRow.Type.valueOf(typeStr) : null)
        .setFromTime(rs.getLong("FROM_TIME")).setToTime(rs.getLong("TO_TIME"))
        .setIsDryRun(rs.getBoolean("IS_DRY_RUN")).setStartRowKey(rs.getBytes("START_ROW_KEY"))
        .setEndRowKey(rs.getBytes("END_ROW_KEY")).setIsFirstRegion(rs.getBoolean("IS_FIRST_REGION"))
        .setExecutionStartTime(rs.getTimestamp("EXECUTION_START_TIME"))
        .setExecutionEndTime(rs.getTimestamp("EXECUTION_END_TIME"))
        .setStatus(statusStr != null ? PhoenixSyncTableOutputRow.Status.valueOf(statusStr) : null)
        .setCounters(rs.getString("COUNTERS")).build();
      entries.add(entry);
    }

    rs.close();
    return entries;
  }

  /**
   * Deletes checkpoint entries for specific mapper and chunk row keys.
   */
  private int deleteCheckpointEntry(Connection conn, String tableName, String targetCluster,
    byte[] mapperStartRowKey, byte[] chunkStartRowKey) throws SQLException {
    String delete = "DELETE FROM PHOENIX_SYNC_TABLE_CHECKPOINT "
      + "WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ? AND START_ROW_KEY = ?";

    PreparedStatement stmt = conn.prepareStatement(delete);
    int totalDeleted = 0;

    // Delete mapper entry
    stmt.setString(1, tableName);
    stmt.setString(2, targetCluster);
    stmt.setBytes(3, mapperStartRowKey);
    totalDeleted += stmt.executeUpdate();

    // Delete chunk entry
    stmt.setString(1, tableName);
    stmt.setString(2, targetCluster);
    stmt.setBytes(3, chunkStartRowKey);
    totalDeleted += stmt.executeUpdate();

    conn.commit();
    return totalDeleted;
  }

  /**
   * Cleans up checkpoint table entries for a specific table and target cluster.
   */
  private void cleanupCheckpointTable(Connection conn, String tableName, String targetCluster) {
    try {
      String delete = "DELETE FROM PHOENIX_SYNC_TABLE_CHECKPOINT "
        + "WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ?";
      PreparedStatement stmt = conn.prepareStatement(delete);
      stmt.setString(1, tableName);
      stmt.setString(2, targetCluster);
      stmt.executeUpdate();
      conn.commit();
    } catch (SQLException e) {
      LOGGER.warn("Failed to cleanup checkpoint table: {}", e.getMessage());
    }
  }

  /**
   * Standard test setup: creates table, inserts data, waits for replication, and verifies. This
   * consolidates the repetitive setup pattern used in most tests.
   */
  private void setupStandardTestWithReplication(String tableName, int startId, int endId)
    throws Exception {
    createTableOnBothClusters(sourceConnection, targetConnection, tableName);
    insertTestData(sourceConnection, tableName, startId, endId);
    int expectedRows = endId - startId + 1;
    waitForReplication(targetConnection, tableName, expectedRows);
    verifyDataIdentical(sourceConnection, targetConnection, tableName);
  }

  /**
   * Runs the PhoenixSyncTableTool with standard configuration. Uses chunk size of 1 byte by default
   * to create chunks of ~1 row each. Returns the completed Job for counter verification.
   */
  private Job runSyncTool(String tableName, String... additionalArgs) throws Exception {
    return runSyncToolWithChunkSize(tableName, 1, additionalArgs);
  }

  /**
   * Runs the PhoenixSyncTableTool with 1KB chunk size for testing multiple rows per chunk. Returns
   * the completed Job for counter verification.
   */
  private Job runSyncToolWithLargeChunks(String tableName, String... additionalArgs)
    throws Exception {
    return runSyncToolWithChunkSize(tableName, 1024, additionalArgs);
  }

  /**
   * Runs the PhoenixSyncTableTool with specified chunk size. Returns the completed Job for counter
   * verification.
   */
  private Job runSyncToolWithChunkSize(String tableName, int chunkSize, String... additionalArgs)
    throws Exception {
    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());

    // Build args list: start with common args, then add additional ones
    List<String> argsList = new ArrayList<>();
    argsList.add("--table-name");
    argsList.add(tableName);
    argsList.add("--target-cluster");
    argsList.add(targetZkQuorum);
    argsList.add("--run-foreground");
    argsList.add("--chunk-size");
    argsList.add(String.valueOf(chunkSize));

    // Add any additional args (like --tenant-id, --from-time, etc.)
    argsList.addAll(Arrays.asList(additionalArgs));

    String[] args = argsList.toArray(new String[0]);

    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);
    int exitCode = tool.run(args);
    Job job = tool.getJob();

    assertNotNull("Job should not be null", job);
    assertEquals("Tool should complete successfully", 0, exitCode);

    return job;
  }

  /**
   * Extracts and returns sync counters from a completed job.
   */
  private static class SyncCountersResult {
    public final long sourceRowsProcessed;
    public final long targetRowsProcessed;
    public final long chunksMismatched;
    public final long chunksVerified;

    SyncCountersResult(Counters counters) {
      this.sourceRowsProcessed =
        counters.findCounter(SyncCounters.SOURCE_ROWS_PROCESSED).getValue();
      this.targetRowsProcessed =
        counters.findCounter(SyncCounters.TARGET_ROWS_PROCESSED).getValue();
      this.chunksMismatched = counters.findCounter(SyncCounters.CHUNKS_MISMATCHED).getValue();
      this.chunksVerified = counters.findCounter(SyncCounters.CHUNKS_VERIFIED).getValue();
    }

    public void logCounters(String testName) {
      LOGGER.info("{}: source rows={}, target rows={}, chunks mismatched={}, chunks verified={}",
        testName, sourceRowsProcessed, targetRowsProcessed, chunksMismatched, chunksVerified);
    }
  }

  /**
   * Gets sync counters from job and logs them.
   */
  private SyncCountersResult getSyncCounters(Job job) throws IOException {
    Counters counters = job.getCounters();
    SyncCountersResult result = new SyncCountersResult(counters);
    result.logCounters(testName.getMethodName());
    return result;
  }

  private void validateSyncCounters(SyncCountersResult counters, long expectedSourceRows,
    long expectedTargetRows, long expectedChunksVerified, long expectedChunksMismatched) {
    assertEquals("Should process expected source rows", expectedSourceRows,
      counters.sourceRowsProcessed);
    assertEquals("Should process expected target rows", expectedTargetRows,
      counters.targetRowsProcessed);
    assertEquals("Should have expected verified chunks", expectedChunksVerified,
      counters.chunksVerified);
    assertEquals("Should have expected mismatched chunks", expectedChunksMismatched,
      counters.chunksMismatched);
  }

  /**
   * Validates that a checkpoint table has entries with proper structure.
   */
  private void validateCheckpointEntries(List<PhoenixSyncTableOutputRow> entries,
    String expectedTableName, String expectedTargetCluster, int expectedSourceRows,
    int expectedTargetRows, int expectedChunkVerified, int expectedChunkMismatched,
    int expectedMapperRegion, int expectedMapperMismatched) {
    int mapperRegionCount = 0;
    int chunkCount = 0;
    int mismatchedEntry = 0;
    int sourceRowsProcessed = 0;
    int targetRowsProcessed = 0;
    for (PhoenixSyncTableOutputRow entry : entries) {
      // Validate primary key columns
      assertEquals("TABLE_NAME should match", expectedTableName, entry.getTableName());
      assertEquals("TARGET_CLUSTER should match", expectedTargetCluster, entry.getTargetCluster());
      assertNotNull("TYPE should not be null", entry.getType());
      assertTrue("TYPE should be MAPPER_REGION or CHUNK",
        PhoenixSyncTableOutputRow.Type.MAPPER_REGION.equals(entry.getType())
          || PhoenixSyncTableOutputRow.Type.CHUNK.equals(entry.getType()));

      // Validate time range
      assertTrue("FROM_TIME should be >= 0", entry.getFromTime() >= 0);
      assertTrue("TO_TIME should be > FROM_TIME", entry.getToTime() > entry.getFromTime());

      // Validate row keys
      assertNotNull("START_ROW_KEY should not be null", entry.getStartRowKey());

      // Validate execution timestamps
      assertNotNull("EXECUTION_START_TIME should not be null", entry.getExecutionStartTime());
      assertNotNull("EXECUTION_END_TIME should not be null", entry.getExecutionEndTime());
      assertTrue("EXECUTION_END_TIME should be >= EXECUTION_START_TIME",
        entry.getExecutionEndTime().getTime() >= entry.getExecutionStartTime().getTime());

      // Validate status
      assertNotNull("STATUS should not be null", entry.getStatus());
      assertTrue("STATUS should be VERIFIED or MISMATCHED",
        PhoenixSyncTableOutputRow.Status.VERIFIED.equals(entry.getStatus())
          || PhoenixSyncTableOutputRow.Status.MISMATCHED.equals(entry.getStatus()));

      if (PhoenixSyncTableOutputRow.Status.MISMATCHED.equals(entry.getStatus())) {
        mismatchedEntry++;
      }

      // Count entry types
      if (PhoenixSyncTableOutputRow.Type.MAPPER_REGION.equals(entry.getType())) {
        mapperRegionCount++;
        sourceRowsProcessed += (int) entry.getSourceRowsProcessed();
        targetRowsProcessed += (int) entry.getTargetRowsProcessed();
      } else if (PhoenixSyncTableOutputRow.Type.CHUNK.equals(entry.getType())) {
        chunkCount++;
        assertNotNull("COUNTERS should not be null for CHUNK entries", entry.getCounters());
      }
    }

    assertEquals(String.format("Should have %d MAPPER_REGION entry", expectedMapperRegion),
      expectedMapperMismatched, expectedMapperRegion, mapperRegionCount);
    assertEquals(
      String.format("Should have %d CHUNK entry", expectedChunkVerified + expectedChunkMismatched),
      expectedChunkVerified + expectedChunkMismatched, chunkCount);
    assertEquals(
      String.format("Should have %d MISMATCHED entry",
        expectedMapperMismatched + expectedChunkMismatched),
      expectedMapperMismatched + expectedChunkMismatched, mismatchedEntry);
    assertEquals(String.format("Should have %d Source rows processed", expectedSourceRows),
      expectedSourceRows, sourceRowsProcessed);
    assertEquals(String.format("Should have %d Target rows processed", expectedTargetRows),
      expectedTargetRows, targetRowsProcessed);
  }

  /**
   * Verifies that two lists of checkpoint entries match structurally (same regions and chunks with
   * identical row key boundaries). Status, counters, and execution times are excluded
   */
  private void verifyCheckpointEntriesMatch(List<PhoenixSyncTableOutputRow> expected,
    List<PhoenixSyncTableOutputRow> actual) {
    assertEquals("Should have same number of checkpoint entries", expected.size(), actual.size());

    for (int i = 0; i < expected.size(); i++) {
      PhoenixSyncTableOutputRow expectedEntry = expected.get(i);
      PhoenixSyncTableOutputRow actualEntry = actual.get(i);
      assertEquals("Entry " + i + " should have same table name", expectedEntry.getTableName(),
        actualEntry.getTableName());
      assertEquals("Entry " + i + " should have same target cluster",
        expectedEntry.getTargetCluster(), actualEntry.getTargetCluster());
      assertEquals("Entry " + i + " should have same type", expectedEntry.getType(),
        actualEntry.getType());
      assertArrayEquals("Entry " + i + " should have same start row key",
        expectedEntry.getStartRowKey(), actualEntry.getStartRowKey());
      assertArrayEquals("Entry " + i + " should have same end row key",
        expectedEntry.getEndRowKey(), actualEntry.getEndRowKey());
    }
  }

  /**
   * Data class to hold test table data
   */
  private static class TestRow {
    int id;
    String name;
    long name_value;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestRow)) return false;
      TestRow other = (TestRow) o;
      return id == other.id && Objects.equals(name, other.name) && name_value == other.name_value;
    }
  }

}
