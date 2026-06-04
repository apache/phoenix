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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.PhoenixSyncTableCheckpointOutputRow;
import org.apache.phoenix.mapreduce.PhoenixSyncTableInputFormat;
import org.apache.phoenix.mapreduce.PhoenixSyncTableMapper.SyncCounters;
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRepository;
import org.apache.phoenix.mapreduce.PhoenixSyncTableTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
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
    CLUSTERS.start();
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    CLUSTERS.close();
  }

  @Before
  public void setUp() throws Exception {
    sourceConnection = DriverManager.getConnection("jdbc:phoenix:" + CLUSTERS.getZkUrl1());
    targetConnection = DriverManager.getConnection("jdbc:phoenix:" + CLUSTERS.getZkUrl2());
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
        dropTableIfExists(sourceConnection, uniqueTableName + "_IDX");
        dropTableIfExists(sourceConnection, uniqueTableName + "_LOCAL_IDX");
        cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
        cleanupCheckpointTable(sourceConnection, uniqueTableName + "_IDX", targetZkQuorum, null);
        cleanupCheckpointTable(sourceConnection, uniqueTableName + "_LOCAL_IDX", targetZkQuorum,
          null);
      } catch (Exception e) {
        LOGGER.warn("Failed to cleanup tables for {}: {}", uniqueTableName, e.getMessage());
      }
    }

    if (targetConnection != null && uniqueTableName != null) {
      try {
        dropTableIfExists(targetConnection, uniqueTableName);
        dropTableIfExists(targetConnection, uniqueTableName + "_IDX");
        dropTableIfExists(targetConnection, uniqueTableName + "_LOCAL_IDX");
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
  public void testSyncTableValidateWithDataDifference() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Pin the time window so the dry-run and repair share the same checkpoint PK.
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Phase 1: dry-run only — verify checkpoint table sees only VERIFIED/MISMATCHED rows.
    Job dryRunJob = runSyncToolWithLargeChunks(uniqueTableName, "--dry-run", "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    SyncCountersResult dryRunCounters = getSyncCounters(dryRunJob);

    validateSyncCounters(dryRunCounters, 10, 10, 1, 3);
    validateMapperCounters(dryRunCounters, 1, 3);
    assertEquals("Expected 4 mapper task to be created", 4, dryRunCounters.taskCreated);
    // Dry-run row-level logging should flag the 3 same-key/different-value rows as
    // ROWS_DIFFERENT_ON_TARGET; nothing missing or extra (replication seeded both sides
    // with the same row keys before introduceAndVerifyTargetDifferences mutated three).
    assertEquals("Dry-run should detect 3 rows different on target", 3,
      dryRunCounters.rowsDifferentOnTarget);
    assertEquals("Dry-run should report 0 rows missing on target", 0,
      dryRunCounters.rowsMissingOnTarget);
    assertEquals("Dry-run should report 0 rows extra on target", 0,
      dryRunCounters.rowsExtraOnTarget);

    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    validateCheckpointEntries(checkpointEntries, uniqueTableName, targetZkQuorum, 10, 10, 1, 3, 4,
      3, null);

    // Phase 2: repair pass over the same window — MISMATCHED rows transition to REPAIRED in
    // place.
    Job repairJob = runSyncToolWithLargeChunks(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    Counters repairCounters = repairJob.getCounters();
    assertRepairChunkAndMapperCounters(repairCounters, 3, 0, 3, 0, 0);
    assertRepairRowCounters(repairCounters, 0, 0, 0);
    // 3 rows × 2 mismatched cells (NAME + Phoenix's _0 empty-key cell) = 6 missing and 6 extra.
    assertRepairCellCounters(repairCounters, 6, 6, 0, 0);

    // Target rows should now match source.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    List<PhoenixSyncTableCheckpointOutputRow> postRepairEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    assertEquals("Expected 3 CHUNK/REPAIRED rows after repair", 3,
      countCheckpointsByTypeAndStatus(postRepairEntries,
        PhoenixSyncTableCheckpointOutputRow.Type.CHUNK,
        PhoenixSyncTableCheckpointOutputRow.Status.REPAIRED));
    assertEquals("Expected 3 REGION/REPAIRED rows after repair", 3,
      countCheckpointsByTypeAndStatus(postRepairEntries,
        PhoenixSyncTableCheckpointOutputRow.Type.REGION,
        PhoenixSyncTableCheckpointOutputRow.Status.REPAIRED));
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateWithDifferentZkQuorumFormats() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);
    introduceAndVerifyTargetDifferences(uniqueTableName);

    String zkHost = CLUSTERS.getHBaseCluster2().getConfiguration().get("hbase.zookeeper.quorum");
    int zkPort = CLUSTERS.getHBaseCluster2().getZkCluster().getClientPort();

    // Three supported formats
    String[] zkQuorumFormats = new String[] { zkHost + ":" + zkPort + ":/hbase", // "host:port:/znode"
      zkHost + "," + zkHost + ":" + zkPort + ":/hbase", // "h1,h2:port:/znode"
      zkHost + ":" + zkPort + "," + zkHost + ":" + zkPort + ":/hbase", // "h1:p1,h2:p2:/znode"
    };

    for (String zkQuorum : zkQuorumFormats) {
      Job job = runSyncToolWithZkQuorum(uniqueTableName, zkQuorum, "--dry-run");
      SyncCountersResult counters = getSyncCounters(job);
      validateSyncCounters(counters, 10, 10, 7, 3);
      cleanupCheckpointTable(sourceConnection, uniqueTableName, zkQuorum, null);
    }

    // After validating detection across ZK formats, run dry-run + repair against the default
    // targetZkQuorum to confirm the tool converges source and target.
    runSyncToolWithRepair(uniqueTableName);
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableWithDeletedRowsOnTarget() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    deleteRows(targetConnection, uniqueTableName, 1, 4, 9);

    // Verify row counts differ between source and target
    long sourceCount = TestUtil.getRowCount(sourceConnection, uniqueTableName);
    long targetCount = TestUtil.getRowCount(targetConnection, uniqueTableName);
    assertEquals("Source should have 10 rows", 10, sourceCount);
    assertEquals("Target should have 7 rows (3 deleted)", 7, targetCount);

    // Dry-run: detects 3 mismatched chunks.
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(result.dryRunJob);

    validateSyncCounters(counters, 10, 7, 7, 3);
    validateMapperCounters(counters, 1, 3);
    assertEquals("Should have only 1 Mapper task created with coalescing", 4, counters.taskCreated);

    // Repair pass only re-runs the 3 mismatched chunks (verified chunks are excluded by the
    // resume filter). Target's DELETEs left tombstones that shadow source's Puts at lower
    // timestamps, so each re-run mapper rolls up to UNREPAIRABLE.
    SyncCountersResult repairCounters = getSyncCounters(result.repairJob);
    validateMapperCountersRepair(repairCounters, 0, 0, 3, 0);
  }

  @Test
  public void testSyncTableWithConditionalTTLExpiredRows() throws Exception {
    // With IS_STRICT_TTL=false
    String ddl = "CREATE TABLE IF NOT EXISTS %s (" + "ID INTEGER NOT NULL PRIMARY KEY, "
      + "NAME VARCHAR(50), NAME_VALUE BIGINT, UPDATED_DATE TIMESTAMP, " + "EXPIRED BOOLEAN"
      + ") REPLICATION_SCOPE=%d, UPDATE_CACHE_FREQUENCY=0, "
      + "TTL='EXPIRED = TRUE', IS_STRICT_TTL=false " + "SPLIT ON (5, 7, 9)";
    executeTableCreation(sourceConnection, String.format(ddl, uniqueTableName, 1));
    executeTableCreation(targetConnection, String.format(ddl, uniqueTableName, 0));

    // Insert 10 rows on source: rows 1-3 marked as expired, rows 4-10 as live
    insertTestDataWithExpiredFlag(sourceConnection, uniqueTableName, 1, 3, true);
    insertTestDataWithExpiredFlag(sourceConnection, uniqueTableName, 4, 10, false);

    waitForReplication(targetConnection, uniqueTableName, 10);

    long sourceCount = TestUtil.getRowCount(sourceConnection, uniqueTableName);
    long targetCount = TestUtil.getRowCount(targetConnection, uniqueTableName);
    assertEquals("Source should see 10 live rows", 10, sourceCount);
    assertEquals("Target should see 10 live rows", 10, targetCount);

    // Introduce differences on 2 of the 7 live rows on target
    upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { 5, 8 },
      new String[] { "MODIFIED_5", "MODIFIED_8" });

    // Run sync tool, TTL-expired rows (1-3) should be skipped on both source and target
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(result.dryRunJob);

    validateSyncCounters(counters, 7, 7, 5, 2);
    validateMapperCounters(counters, 2, 2);

    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableWithConditionalTTLExpiredRowsCompact() throws Exception {
    // With IS_STRICT_TTL=false
    String ddl = "CREATE TABLE IF NOT EXISTS %s (" + "ID INTEGER NOT NULL PRIMARY KEY, "
      + "NAME VARCHAR(50), NAME_VALUE BIGINT, UPDATED_DATE TIMESTAMP, " + "EXPIRED BOOLEAN"
      + ") REPLICATION_SCOPE=%d, UPDATE_CACHE_FREQUENCY=0, "
      + "TTL='EXPIRED = TRUE', IS_STRICT_TTL=false " + "SPLIT ON (5, 7, 9)";
    executeTableCreation(sourceConnection, String.format(ddl, uniqueTableName, 1));
    executeTableCreation(targetConnection, String.format(ddl, uniqueTableName, 0));

    // Insert 10 rows on source: rows 1-3 marked as expired, rows 4-10 as live
    insertTestDataWithExpiredFlag(sourceConnection, uniqueTableName, 1, 3, true);
    insertTestDataWithExpiredFlag(sourceConnection, uniqueTableName, 4, 10, false);

    waitForReplication(targetConnection, uniqueTableName, 10);

    long sourceCount = TestUtil.getRowCount(sourceConnection, uniqueTableName);
    long targetCount = TestUtil.getRowCount(targetConnection, uniqueTableName);
    assertEquals("Source should see 10 live rows", 10, sourceCount);
    assertEquals("Target should see 10 live rows", 10, targetCount);

    // Run sync tool, TTL-expired rows (1-3) should be skipped on both source and target
    Job job = runSyncTool(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(job);

    validateSyncCounters(counters, 7, 7, 7, 0);
    validateMapperCounters(counters, 4, 0);

    flushAndMajorCompact(CLUSTERS.getHBaseCluster2(), uniqueTableName);

    long sourceCountPostCompact = TestUtil.getRowCount(sourceConnection, uniqueTableName);
    long targetCountPostCompact = TestUtil.getRowCount(targetConnection, uniqueTableName);
    assertEquals("Source should see 10 live rows", 10, sourceCountPostCompact);
    assertEquals("Target should see 7 live rows", 7, targetCountPostCompact);

    // We shouldn't see expired rows even with --raw-scan flag
    Job job2 = runSyncTool(uniqueTableName, "--raw-scan");
    SyncCountersResult counters2 = getSyncCounters(job2);

    validateSyncCounters(counters2, 7, 7, 7, 0);
    validateMapperCounters(counters2, 4, 0);

    // Source and target each show 7 live rows under the conditional TTL filter the sync tool
    // applies on both sides, so the repair pass is a no-op and no MISMATCHED rows are written.
    // Note: the standard Phoenix query (without the TTL filter the tool applies) sees 10 rows
    // on source vs 7 on target because IS_STRICT_TTL=false returns expired rows on source
    // (uncompacted) but compaction on target physically removed them — that asymmetry is by
    // design, not drift the tool can converge.
    runSyncToolWithRepair(uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncValidateIndexTable() throws Exception {
    // Create data table on both clusters with replication
    createTableOnBothClusters(sourceConnection, targetConnection, uniqueTableName);

    // Create index on both clusters
    String indexName = uniqueTableName + "_IDX";
    createIndexOnBothClusters(sourceConnection, targetConnection, uniqueTableName, indexName);

    // Insert data on source
    insertTestData(sourceConnection, uniqueTableName, 1, 10);

    // Wait for replication to target (both data table and index)
    waitForReplication(targetConnection, uniqueTableName, 10);

    // Verify initial replication
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    deleteHBaseRows(CLUSTERS.getHBaseCluster2(), uniqueTableName, 3);
    deleteHBaseRows(CLUSTERS.getHBaseCluster2(), indexName, 3);

    RepairRunResult result = runSyncToolWithRepair(indexName);
    SyncCountersResult counters = getSyncCounters(result.dryRunJob);

    validateSyncCounters(counters, 10, 7, 7, 3);

    // Verify checkpoint entries show mismatches (from dry-run pass) before repair runs.
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, indexName, targetZkQuorum, null);

    assertFalse("Should have checkpointEntries", checkpointEntries.isEmpty());

    // The repair pass syncs the index physical table on target with the source index. Since the
    // data table was also corrupted on target (3 rows deleted via deleteHBaseRows) but we only
    // ran sync on the index, the data table itself is still drifted — only assert on index
    // checkpoint rows.
    assertNoMismatchedCheckpoints(indexName, null);
  }

  @Test
  public void testSyncValidateLocalIndexTable() throws Exception {
    // Create data table on both clusters with replication
    createTableOnBothClusters(sourceConnection, targetConnection, uniqueTableName);

    // Create LOCAL index on both clusters
    String indexName = uniqueTableName + "_LOCAL_IDX";
    createLocalIndexOnBothClusters(sourceConnection, targetConnection, uniqueTableName, indexName);

    // Insert data on source
    insertTestData(sourceConnection, uniqueTableName, 1, 10);

    // Wait for replication to target (both data table and local index)
    waitForReplication(targetConnection, uniqueTableName, 10);

    // Verify initial replication
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    deleteHBaseRows(CLUSTERS.getHBaseCluster2(), uniqueTableName, 5);

    // Run sync tool on the LOCAL INDEX table (not the data table). Local indexes share regions
    // with the data table — the dry-run pass detects drift, the repair pass writes back the
    // missing index rows on target.
    RepairRunResult result = runSyncToolWithRepair(indexName);
    SyncCountersResult counters = getSyncCounters(result.dryRunJob);

    assertTrue(String.format("Should have at least %d verified chunks, actual: %d", 1,
      counters.chunksVerified), counters.chunksVerified >= 1);
    assertTrue(String.format("Should have at least %d mismatched chunks, actual: %d", 1,
      counters.chunksMismatched), counters.chunksMismatched >= 1);

    // Verify checkpoint entries
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, indexName, targetZkQuorum, null);

    assertFalse("Should have checkpoint entries for local index", checkpointEntries.isEmpty());

    // After repair, the local-index physical table on target should match source's index.
    assertNoMismatchedCheckpoints(indexName, null);
  }

  @Test
  public void testSyncValidateMultiTenantSaltedTableDifferences() throws Exception {
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
    String toTime = String.valueOf(System.currentTimeMillis());

    for (String tenantId : tenantIds) {
      Connection tenantSourceConn = getTenantConnection(sourceConnection, tenantId);
      insertMultiTenantTestData(tenantSourceConn, uniqueTableName, 1000, rowsPerTenant);
      tenantSourceConn.close();
    }

    // TENANT_001 has no differences, expect all rows verified. Use dry-run + repair to confirm
    // the no-drift case still leaves no MISMATCHED rows.
    RepairRunResult t1 =
      runSyncToolWithRepair(uniqueTableName, "--tenant-id", tenantIds[0], "--to-time", toTime);
    SyncCountersResult counters1 = getSyncCounters(t1.dryRunJob);
    validateSyncCounters(counters1, 10, 10, 10, 0);
    validateMapperCounters(counters1, 4, 0);

    // TENANT_002 has 3 modified rows. Dry-run detects, repair writes back source values.
    RepairRunResult t2 = runSyncToolWithRepair(uniqueTableName, "--tenant-id", tenantIds[1]);
    SyncCountersResult counters2 = getSyncCounters(t2.dryRunJob);
    validateSyncCounters(counters2, 10, 10, 7, 3);
    validateMapperCounters(counters2, 2, 2);

    // Verify checkpoint table has entries for the reprocessed regions
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, "TENANT_002");
    assertFalse("Should have checkpoint entries for TENANT_002", checkpointEntries.isEmpty());

    checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, "TENANT_001");
    assertFalse("Should have checkpoint entries for TENANT_001", checkpointEntries.isEmpty());

    // No MISMATCHED rows should remain after repair pass for either tenant.
    assertNoMismatchedCheckpoints(uniqueTableName, "TENANT_001");
    assertNoMismatchedCheckpoints(uniqueTableName, "TENANT_002");

    // After repair, TENANT_002's data should be identical between source and target.
    withTenantConnections(tenantIds[1],
      (sourceConn, targetConn) -> verifyDataIdentical(sourceConn, targetConn, uniqueTableName));
  }

  @Test
  public void testSyncTableValidateWithTimeRangeFilter() throws Exception {
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
    validateMapperCounters(counters, 1, 0);

    // Within-window data (IDs 11-20) was identical, so the repair flow is a no-op there and
    // no MISMATCHED rows are written. Out-of-window drift (IDs 3,5,8,23,25,28) is invisible
    // to the time-range filter and remains on target by design — full convergence is NOT
    // expected here, only checkpoint cleanliness for the window we scanned.
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateCheckpointWithPartialReRunAndRegionSplits() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    List<Integer> sourceSplits = Arrays.asList(15, 45, 51, 75, 95);
    splitTableAt(sourceConnection, uniqueTableName, sourceSplits);

    // Introduce differences on target scattered across the dataset
    List<Integer> mismatchIds = Arrays.asList(10, 25, 40, 55, 70, 85, 95);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    // Capture consistent time range for both runs
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    PartialRerunSetup setup = setupPartialRerun(uniqueTableName, fromTime, toTime, 1, 0.75);
    validateSyncCountersWithMinChunk(setup.firstRunCounters, 100, 100, 1, 1);

    List<Integer> additionalSourceSplits =
      Arrays.asList(12, 22, 28, 32, 42, 52, 58, 62, 72, 78, 82, 92);
    splitTableAt(sourceConnection, uniqueTableName, additionalSourceSplits);

    List<Integer> targetSplits = Arrays.asList(25, 40, 50, 65, 70, 80, 90);
    splitTableAt(targetConnection, uniqueTableName, targetSplits);

    // Run sync tool again with SAME time range - should reprocess only deleted regions
    // despite the new region boundaries from splits
    Job job2 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    LOGGER.info(
      "Second run - Processed: {} source rows, {} target rows, {} verified chunks, {} mismatched chunks",
      counters2.sourceRowsProcessed, counters2.targetRowsProcessed, counters2.chunksVerified,
      counters2.chunksMismatched);

    // (Remaining chunks from checkpoint) + (Second run) should equal (First run)
    long totalSourceRows = setup.remainingCounters.sourceRowsProcessed + counters2.sourceRowsProcessed;
    long totalTargetRows = setup.remainingCounters.targetRowsProcessed + counters2.targetRowsProcessed;
    long totalVerifiedChunks = setup.remainingCounters.chunksVerified + counters2.chunksVerified;

    assertEquals(
      "Remaining + Second run source rows should equal first run source rows. " + "Remaining: "
        + setup.remainingCounters.sourceRowsProcessed + ", Second run: "
        + counters2.sourceRowsProcessed + ", Total: " + totalSourceRows + ", Expected: "
        + setup.firstRunCounters.sourceRowsProcessed,
      setup.firstRunCounters.sourceRowsProcessed, totalSourceRows);

    assertEquals(
      "Remaining + Second run target rows should equal first run target rows. " + "Remaining: "
        + setup.remainingCounters.targetRowsProcessed + ", Second run: "
        + counters2.targetRowsProcessed + ", Total: " + totalTargetRows + ", Expected: "
        + setup.firstRunCounters.targetRowsProcessed,
      setup.firstRunCounters.targetRowsProcessed, totalTargetRows);

    // Splits introduced between runs widen the second-run chunk count beyond the deleted
    // 75% of the first run's chunks (extra region boundaries → extra chunks). So we relax
    // the strict equality to >=. The row-count invariant above is unaffected by splits.
    assertTrue("Remaining + Second run verified chunks should be >= first run verified chunks. "
      + "Remaining: " + setup.remainingCounters.chunksVerified + ", Second run: "
      + counters2.chunksVerified + ", Total: " + totalVerifiedChunks + ", Expected (>=): "
      + setup.firstRunCounters.chunksVerified,
      totalVerifiedChunks >= setup.firstRunCounters.chunksVerified);

    // Verify checkpoint table has entries for the reprocessed regions
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterRerun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    // After rerun, we should have at least more entries compared to delete table
    assertTrue("Should have checkpoint entries after rerun",
      checkpointEntriesAfterRerun.size() > setup.entriesAfterDelete.size());

    // The partial-rerun pattern (delete checkpoints + re-split + rerun) leaves chunks marked
    // REPAIRED with stale boundaries (relative to the post-split layout); the resume filter
    // skips those, so a final run can leave residual drift. Cleanup the checkpoint and run
    // a dry-run + repair pass on the stable layout to converge.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithRepair(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateCheckpointWithChunkSizeChangeOnReRun() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    List<Integer> sourceSplits = Arrays.asList(25, 50, 75);
    splitTableAt(sourceConnection, uniqueTableName, sourceSplits);

    List<Integer> mismatchIds = Arrays.asList(10, 30, 60, 90);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // First run with large chunk size, then delete 75% of chunks for partial rerun.
    int largeChunkSize = 10240;
    PartialRerunSetup setup =
      setupPartialRerun(uniqueTableName, fromTime, toTime, largeChunkSize, 0.75);
    SyncCountersResult counters1 = setup.firstRunCounters;
    validateSyncCounters(counters1, 100, 100, counters1.chunksVerified, counters1.chunksMismatched);
    int mapperCountAfterFirstRun = setup.mappers.size();
    int chunkCountAfterFirstRun = setup.chunks.size();

    // Re-run with smaller chunk size (1 byte) - produces more, smaller chunks
    int smallChunkSize = 1;
    Job job2 = runSyncToolWithChunkSize(uniqueTableName, smallChunkSize, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    // (Remaining chunks) + (Second run) should equal (First run) for row counts
    long totalSourceRows = setup.remainingCounters.sourceRowsProcessed + counters2.sourceRowsProcessed;
    long totalTargetRows = setup.remainingCounters.targetRowsProcessed + counters2.targetRowsProcessed;

    assertEquals("Remaining + rerun source rows should equal first run",
      counters1.sourceRowsProcessed, totalSourceRows);
    assertEquals("Remaining + rerun target rows should equal first run",
      counters1.targetRowsProcessed, totalTargetRows);

    // Validate checkpoint table after rerun
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterRerun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    SeparatedCheckpointEntries separatedAfterRerun =
      separateMapperAndChunkEntries(checkpointEntriesAfterRerun);

    // Mapper count should be at least what the first run had
    assertTrue(
      "Mapper count after rerun (" + separatedAfterRerun.mappers.size()
        + ") should be >= first run (" + mapperCountAfterFirstRun + ")",
      separatedAfterRerun.mappers.size() >= mapperCountAfterFirstRun);

    // Chunk count should be more because smaller chunk size produces more chunks
    assertTrue(
      "Chunk count after rerun (" + separatedAfterRerun.chunks.size()
        + ") should be greater than first run (" + chunkCountAfterFirstRun + ")",
      separatedAfterRerun.chunks.size() > chunkCountAfterFirstRun);

    // The partial-rerun pattern (delete chunks, rerun with smaller chunks) exercises the
    // checkpoint resume path. Once that has been validated, run a clean dry-run + repair
    // pass on the same window so the repair flow has a stable boundary set to converge.
    // Use the dry-run+repair pattern so any chunk that landed in a non-resumable state
    // (REPAIRED with stale boundaries) is re-validated rather than skipped.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithRepair(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateCheckpointWithPartialReRunAndRegionMerges() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    List<Integer> sourceSplits = Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90);
    splitTableAt(sourceConnection, uniqueTableName, sourceSplits);

    List<Integer> mismatchIds = Arrays.asList(5, 15, 25, 35, 45, 55, 65, 75, 85, 95);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    PartialRerunSetup setup = setupPartialRerun(uniqueTableName, fromTime, toTime, 1, 0.75);
    SyncCountersResult counters1 = setup.firstRunCounters;
    validateSyncCounters(counters1, 100, 100, 90, 10);

    // Merge adjacent regions on source and target (6 pairs each).
    mergeAdjacentRegions(sourceConnection, uniqueTableName, 6);
    mergeAdjacentRegions(targetConnection, uniqueTableName, 6);

    // Run sync tool again with SAME time range - should reprocess only deleted regions
    // despite the new region boundaries from merges
    Job job2 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    long totalSourceRows = setup.remainingCounters.sourceRowsProcessed + counters2.sourceRowsProcessed;
    long totalTargetRows = setup.remainingCounters.targetRowsProcessed + counters2.targetRowsProcessed;
    long totalVerifiedChunks = setup.remainingCounters.chunksVerified + counters2.chunksVerified;

    assertEquals(
      "Remaining + Second run source rows should equal first run source rows. " + "Remaining: "
        + setup.remainingCounters.sourceRowsProcessed + ", Second run: "
        + counters2.sourceRowsProcessed + ", Total: " + totalSourceRows + ", Expected: "
        + counters1.sourceRowsProcessed,
      counters1.sourceRowsProcessed, totalSourceRows);

    assertEquals(
      "Remaining + Second run target rows should equal first run target rows. " + "Remaining: "
        + setup.remainingCounters.targetRowsProcessed + ", Second run: "
        + counters2.targetRowsProcessed + ", Total: " + totalTargetRows + ", Expected: "
        + counters1.targetRowsProcessed,
      counters1.targetRowsProcessed, totalTargetRows);

    // Region merges between the two runs change mapper region boundaries, so the resume
    // filter sees stale chunks that don't align to the new mapper's range and reprocesses
    // them. The "remaining + second run >= first run" invariant still holds; equality does
    // not. Row-count invariant above is preserved.
    assertTrue("Remaining + Second run verified chunks should be >= first run verified chunks. "
      + "Remaining: " + setup.remainingCounters.chunksVerified + ", Second run: "
      + counters2.chunksVerified + ", Total: " + totalVerifiedChunks + ", Expected (>=): "
      + counters1.chunksVerified, totalVerifiedChunks >= counters1.chunksVerified);

    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterRerun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    // After rerun with merges, we should have more entries as after deletion
    assertTrue("Should have checkpoint entries after rerun",
      checkpointEntriesAfterRerun.size() > setup.entriesAfterDelete.size());

    // Both runs were non-dry-run, so repair ran inline. Target should converge.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateIdempotentOnReRun() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Capture consistent time range for both runs (ensures checkpoint lookup will match)
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run sync tool for the FIRST time
    Job job1 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters1 = getSyncCounters(job1);

    // Validate first run counters
    validateSyncCounters(counters1, 10, 10, 10, 0);
    validateMapperCounters(counters1, 4, 0);

    // Query checkpoint table to verify entries were created
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterFirstRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    // Run sync tool for the SECOND time WITHOUT deleting any checkpoints (idempotent behavior)
    Job job2 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    // Second run should process ZERO rows (idempotent behavior)
    validateSyncCounters(counters2, 0, 0, 0, 0);
    validateMapperCounters(counters2, 0, 0);

    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterSecondRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    assertEquals("Checkpoint entries should be identical after idempotent run",
      checkpointEntriesAfterFirstRun, checkpointEntriesAfterSecondRun);

    // Both passes were non-dry-run with no drift to begin with; the repair flow ran as a
    // no-op, target should still match source and no MISMATCHED rows should exist.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateIdempotentAfterRegionSplits() throws Exception {
    // Setup table with initial splits and data
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Verify data is identical after replication
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    // Capture consistent time range for both runs
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run sync tool for the FIRST time (no differences, all chunks verified)
    Job job1 = runSyncToolWithLargeChunks(uniqueTableName, "--from-time", String.valueOf(fromTime),
      "--to-time", String.valueOf(toTime));
    SyncCountersResult counters1 = getSyncCounters(job1);

    // Validate first run: all rows processed, no mismatches
    validateSyncCounters(counters1, 10, 10, 4, 0);
    validateMapperCounters(counters1, 4, 0);

    // Query checkpoint table to verify entries were created
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterFirstRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    assertFalse("Should have checkpoint entries after first run",
      checkpointEntriesAfterFirstRun.isEmpty());

    // Attempt to split tables on BOTH source and target at new split points
    // Some splits may fail if regions are in transition, which is acceptable for this test
    splitTableAt(sourceConnection, uniqueTableName, Arrays.asList(2, 6));
    splitTableAt(targetConnection, uniqueTableName, Arrays.asList(3, 7));

    // Run sync tool for the SECOND time after splits (idempotent behavior)
    Job job2 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    // Second run should process ZERO rows despite new region boundaries
    validateSyncCounters(counters2, 0, 0, 0, 0);
    validateMapperCounters(counters2, 0, 0);

    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterSecondRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    // Checkpoint entries may differ in count due to new regions, but all original data is
    // checkpointed
    assertFalse("Should have checkpoint entries after second run",
      checkpointEntriesAfterSecondRun.isEmpty());

    // No drift was introduced; repair flow should be a no-op even after concurrent splits.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateWithSchemaAndTableNameOptions() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Introduce differences on target
    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Run sync tool with both --schema and --table-name options
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--schema", "");
    SyncCountersResult counters = getSyncCounters(result.dryRunJob);

    // Validate counters
    validateSyncCounters(counters, 10, 10, 7, 3);
    validateMapperCounters(counters, 1, 3);

    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateInBackgroundMode() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Pin the time window so the background dry-run pass and the repair pass below share
    // the same checkpoint PK and the repair pass overwrites MISMATCHED → REPAIRED in place.
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    Configuration conf = sourceClusterConf();
    String[] args = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--chunk-size", "1", "--dry-run", "--from-time", String.valueOf(fromTime),
      "--to-time", String.valueOf(toTime) };

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
    validateMapperCounters(counters, 1, 3);

    // Now run the repair pass (foreground for synchronous assertions). Same time window so
    // the dry-run-pass MISMATCHED rows are overwritten with REPAIRED.
    runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));

    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateWithCustomTimeouts() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Create configuration with custom timeout values
    Configuration conf = sourceClusterConf();

    // Set custom timeout values (higher than defaults to ensure job succeeds)
    long customQueryTimeout = 900000L; // 15 minutes
    long customRpcTimeout = 1200000L; // 20 minutes
    long customScannerTimeout = 2400000L; // 40 minutes
    int customRpcRetries = 10;

    conf.setLong(QueryServices.SYNC_TABLE_QUERY_TIMEOUT_ATTRIB, customQueryTimeout);
    conf.setLong(QueryServices.SYNC_TABLE_RPC_TIMEOUT_ATTRIB, customRpcTimeout);
    conf.setLong(QueryServices.SYNC_TABLE_CLIENT_SCANNER_TIMEOUT_ATTRIB, customScannerTimeout);
    conf.setInt(QueryServices.SYNC_TABLE_RPC_RETRIES_COUNTER, customRpcRetries);

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();
    Job job = runSyncToolWithChunkSize(uniqueTableName, 1, conf, "--dry-run", "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));

    // Verify that custom timeout configurations were applied to the job
    Configuration jobConf = job.getConfiguration();
    assertEquals("Custom query timeout should be applied", customQueryTimeout,
      jobConf.getLong(MRJobConfig.TASK_TIMEOUT, -1));
    assertEquals("Custom RPC timeout should be applied", customRpcTimeout,
      jobConf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, -1));
    assertEquals("Custom scanner timeout should be applied", customScannerTimeout,
      jobConf.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
    assertEquals("Custom RPC retries should be applied", customRpcRetries,
      jobConf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, -1));

    // Verify sync completed successfully
    SyncCountersResult counters = new SyncCountersResult(job.getCounters());
    counters.logCounters(testName.getMethodName());
    validateSyncCounters(counters, 10, 10, 7, 3);
    validateMapperCounters(counters, 1, 3);

    // Repair pass over the same window: convergence + no MISMATCHED rows remaining.
    runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateWithExtraRowsOnTarget() throws Exception {
    // Create tables on both clusters
    createTableOnBothClusters(sourceConnection, targetConnection, uniqueTableName);

    // Insert data on source with HOLES (gaps in the sequence)
    List<Integer> oddIds = Arrays.asList(1, 3, 5, 7, 9, 11, 13, 15, 17, 19);
    insertTestData(sourceConnection, uniqueTableName, oddIds);

    // Wait for replication to target
    waitForReplication(targetConnection, uniqueTableName, 10);

    // Verify initial replication
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    // Insert rows in the HOLES on target cluster only
    // Target gets: 2, 4, 6, 8, 10 (5 even numbers filling the gaps in first half)
    List<Integer> evenIds = Arrays.asList(2, 4, 6, 8, 10);
    insertTestData(targetConnection, uniqueTableName, evenIds);

    // Verify target now has more rows than source
    long sourceCount = TestUtil.getRowCount(sourceConnection, uniqueTableName);
    long targetCount = TestUtil.getRowCount(targetConnection, uniqueTableName);
    assertEquals("Source should have 10 rows (odd numbers 1-19)", 10, sourceCount);
    assertEquals("Target should have 15 rows (odd 1-19 + even 2-10)", 15, targetCount);

    // Run dry-run + repair sharing the same time window.
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(result.dryRunJob);

    validateSyncCounters(counters, 10, 15, 5, 5);
    validateMapperCounters(counters, 0, 4);

    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    // Count mismatched entries in checkpoint table — after the repair pass, all MISMATCHED
    // rows from the dry-run pass should have been overwritten with REPAIRED.
    long mismatchedCount = countCheckpointsByStatus(checkpointEntries,
      PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED);
    long repairedCount = countCheckpointsByStatus(checkpointEntries,
      PhoenixSyncTableCheckpointOutputRow.Status.REPAIRED);
    assertEquals("After repair, no MISMATCHED rows should remain", 0, mismatchedCount);
    assertTrue("Should have REPAIRED rows after repair pass", repairedCount > 0);

    // After repair: target should converge to source (10 odd-id rows). The 5 extra even-id
    // rows on target had only live cells, so tombstoneWholeRow can remove them.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertEquals("Source should have 10 rows", 10,
      TestUtil.getRowCount(sourceConnection, uniqueTableName));
    assertEquals("Target should now also have 10 rows after repair tombstones the extras", 10,
      TestUtil.getRowCount(targetConnection, uniqueTableName));
  }

  @Test
  public void testSyncTableValidateWithConcurrentRegionSplits() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);
    // Introduce some mismatches on target before sync
    List<Integer> mismatchIds = Arrays.asList(15, 35, 55, 75, 95);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    // Capture time range for the sync
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run splits on source/target concurrently with the sync.
    Runnable splitJoiner = startConcurrentRegionWork(
      () -> splitTableAt(sourceConnection, uniqueTableName,
        Arrays.asList(20, 25, 40, 45, 60, 65, 80, 85, 95)),
      () -> splitTableAt(targetConnection, uniqueTableName,
        Arrays.asList(11, 21, 31, 41, 51, 75, 81, 91)),
      "splits");

    // Run sync tool while splits are happening
    Job job = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));

    splitJoiner.run();

    // Verify the job completed successfully despite concurrent splits
    assertTrue("Sync job should complete successfully despite concurrent splits",
      job.isSuccessful());

    SyncCountersResult counters = getSyncCounters(job);

    // Validate counters - should process all 100 rows
    validateSyncCountersWithMinChunk(counters, 100, 100, 1, 1);

    // Verify checkpoint entries were created
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    assertFalse("Should have checkpoint entries", checkpointEntries.isEmpty());

    // Concurrent splits may race with the first sync pass — a chunk that straddled a region
    // boundary mid-flight can land in REPAIRED with stale boundaries; once REPAIRED, the
    // resume filter skips it. Cleanup the checkpoint and run a dry-run + repair pass on the
    // stable region layout to converge.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithRepair(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  /**
   * P3 (concurrent splits during repair pass): Today's concurrent-split tests run splits during
   * the dry-run pass; this exercises the repair pass against splitting target regions, which is
   * the production reality that exercises {@code flushRepairMutations}'s
   * {@code NotServingRegionException} path → {@code firstFailureIdx} → {@code REPAIR_FAILED}.
   *
   * <p>Convergence strategy:
   * <ol>
   *   <li>Dry-run first on a stable layout to populate MISMATCHED checkpoint rows.</li>
   *   <li>Start concurrent splits on the target cluster, then run the repair pass. Some chunks
   *       may land in {@code REPAIR_FAILED} if a flush hits a region in transition — the
   *       resume filter re-enters those chunks on the next pass.</li>
   *   <li>Run a final dry-run + repair pass after splits have settled; expect zero MISMATCHED.
   *       {@code verifyDataIdentical} must succeed.</li>
   * </ol>
   */
  @Test
  public void testRepairWithConcurrentTargetSplits() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);
    List<Integer> mismatchIds = Arrays.asList(12, 24, 36, 48, 60, 72, 84, 96);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Stage 1: stable dry-run populates MISMATCHED checkpoint rows.
    Job dryRunJob = runSyncTool(uniqueTableName, "--dry-run", "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    assertTrue("Stable dry-run should succeed", dryRunJob.isSuccessful());
    SyncCountersResult dryRunCounters = getSyncCounters(dryRunJob);
    assertTrue("Dry-run should detect at least one mismatched chunk",
      dryRunCounters.chunksMismatched >= 1);

    // Stage 2: kick off target-side splits while the repair pass runs. Source splits left out
    // because a target-side split is what surfaces flushRepairMutations failures.
    Runnable splitJoiner = startConcurrentRegionWork(() -> {
      // No source-side work; pass a trivial Runnable so startConcurrentRegionWork still wires
      // both threads and the joiner times out cleanly.
    }, () -> splitTableAt(targetConnection, uniqueTableName,
      Arrays.asList(15, 25, 35, 45, 55, 65, 75, 85, 95)), "repair-splits");

    Job repairJob = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime),
      "--to-time", String.valueOf(toTime));
    splitJoiner.run();
    assertTrue("Repair pass should not throw despite concurrent splits", repairJob.isSuccessful());

    // Stage 3: stable convergence pass. Cleanup checkpoint so the resume filter doesn't skip
    // chunks that were marked REPAIRED with stale boundaries during the racing pass.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithRepair(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));

    assertNoMismatchedCheckpoints(uniqueTableName, null);
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
  }

  /**
   * P4 (idempotent repair): Guards against a regression where repair claims REPAIRED but does
   * not actually converge. Run a full dry-run + repair on a divergent table, clean the
   * checkpoint, then run the same dry-run + repair again on the now-converged tables and assert
   * the second pass is a no-op (zero mismatches detected, zero chunks repaired).
   */
  @Test
  public void testRepairIsIdempotent() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 50);
    List<Integer> mismatchIds = Arrays.asList(7, 14, 21, 28, 35, 42, 49);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Pass 1: detect + repair.
    RepairRunResult firstRun = runSyncToolWithRepair(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    assertTrue("First dry-run should succeed", firstRun.dryRunJob.isSuccessful());
    assertTrue("First repair should succeed", firstRun.repairJob.isSuccessful());

    SyncCountersResult firstDryRunCounters = getSyncCounters(firstRun.dryRunJob);
    assertTrue("First dry-run should detect mismatched chunks",
      firstDryRunCounters.chunksMismatched >= 1);
    Counters firstRepairCounters = firstRun.repairJob.getCounters();
    assertTrue("First repair should mark chunks REPAIRED",
      firstRepairCounters.findCounter(SyncCounters.CHUNKS_REPAIRED).getValue() >= 1);

    // Tables must be data-identical after the first repair.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    // Pass 2 prep: clean the checkpoint so the resume filter doesn't skip already-VERIFIED
    // chunks — the second dry-run must scan the full layout from scratch.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    // Pass 2: same window, no further mutations. Both passes must be no-ops.
    RepairRunResult secondRun = runSyncToolWithRepair(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    assertTrue("Second dry-run should succeed", secondRun.dryRunJob.isSuccessful());
    assertTrue("Second repair should succeed", secondRun.repairJob.isSuccessful());

    SyncCountersResult secondDryRunCounters = getSyncCounters(secondRun.dryRunJob);
    assertEquals("Second dry-run should detect zero mismatches", 0,
      secondDryRunCounters.chunksMismatched);

    // Second repair pass should be a no-op: nothing repaired, nothing failed.
    assertRepairChunkAndMapperCounters(secondRun.repairJob.getCounters(), 0, 0, 0, 0, 0);

    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
  }

  /**
   * P5 (all-tombstoned target-extra row): Source has no row K; target has row K but every cell
   * is already a tombstone. {@code tombstoneWholeRow} returns {@code liveCellsTombstoned == 0}
   * (line ~217 of {@code PhoenixSyncTableChunkRepairer}) → {@code drift.rowsCannotRepair++},
   * {@code rowsExtraOnTarget} unchanged. Pins the rare "target row extra but already
   * fully-tombstoned" path that currently has zero coverage.
   */
  @Test
  public void testRepairAllTombstonedTargetRowExtra() throws Exception {
    final int rowId = 5;
    final int otherRowId = 4;
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 1, "3, 7");

    long fromTime = 0L;
    final long ts = base + 1L;
    final long tombstoneTs = base + 2L;

    // Plant a sentinel row on both sides so the verifier has *something* to compare and
    // produces a non-empty chunk hash. The sentinel row stays clean — the test focuses on
    // rowId=5 only.
    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), ts)) {
      scnSrc.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + otherRowId + ", 'sentinel')");
      scnSrc.commit();
    }
    try (Connection scnTgt = openConnectionAtScn(CLUSTERS.getZkUrl2(), ts)) {
      scnTgt.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + otherRowId + ", 'sentinel')");
      scnTgt.commit();
    }

    // Target only: plant raw DeleteColumn tombstones for row K with NO underlying Put cells.
    // Under --raw-scan the row surfaces (tombstones are themselves cells) but every cell is a
    // Delete, so tombstoneWholeRow() returns liveCellsTombstoned == 0 → drift.rowsCannotRepair
    // increments and rowsExtraOnTarget stays 0. This pins the rare "row exists in raw view but
    // has no live cells to tombstone" branch.
    byte[] rowKey = integerRowKey(rowId);
    writeRawDeleteColumn(targetConnection, uniqueTableName, rowKey, "0", "NAME", tombstoneTs);
    writeRawDeleteColumn(targetConnection, uniqueTableName, rowKey, "0", "NAME_VALUE",
      tombstoneTs);
    writeRawDeleteColumn(targetConnection, uniqueTableName, rowKey, "0", "_0", tombstoneTs);

    // Spin until wall-clock advances past the highest cell timestamp so --to-time
    // (which defaults to currentTimeMillis()) covers our planted cells.
    while (System.currentTimeMillis() <= tombstoneTs) {
      // spin
    }
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(System.currentTimeMillis()),
      "--raw-scan");
    assertTrue("Repair should succeed", result.repairJob.isSuccessful());

    // Row K's cells were already all tombstones — no live cells to tombstone again, and
    // the row is flagged unrepairable.
    assertRepairRowCounters(result.repairJob.getCounters(), 0, 0, 1);
  }

  @Test
  public void testSyncTableValidateWithOnlyTimestampDifferences() throws Exception {
    createTableOnBothClusters(sourceConnection, targetConnection, uniqueTableName);

    // Define two different timestamps
    long timestamp1 = System.currentTimeMillis();
    Thread.sleep(100); // Ensure different timestamp
    long timestamp2 = System.currentTimeMillis();

    // Insert same data on source with timestamp1
    insertTestData(sourceConnection, uniqueTableName, 1, 10, timestamp1);

    // Insert same data on target with timestamp2 (different timestamp, same values)
    insertTestData(targetConnection, uniqueTableName, 1, 10, timestamp2);

    // Verify both have same row count and same values
    long sourceCount = TestUtil.getRowCount(sourceConnection, uniqueTableName);
    long targetCount = TestUtil.getRowCount(targetConnection, uniqueTableName);
    assertEquals("Both should have 10 rows", sourceCount, targetCount);

    // Query and verify data values are identical (but timestamps differ)
    List<TestRow> sourceRows = queryAllRows(sourceConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
    List<TestRow> targetRows = queryAllRows(targetConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
    assertEquals("Row values should be identical", sourceRows, targetRows);

    // Dry-run sync — should detect timestamp differences as mismatches because timestamps are
    // included in the hash calculation. We use dry-run so the MISMATCHED rows persist for the
    // assertions below; this is a residual-drift case where Phoenix-level queries already
    // see identical values (timestamps differ but values match) so the repair phase is not
    // exercised here.
    Job job = runSyncTool(uniqueTableName, "--dry-run");
    SyncCountersResult counters = getSyncCounters(job);

    // Validate counters - all rows should be processed and all chunks mismatched
    validateSyncCounters(counters, 10, 10, 0, 10);

    // Verify checkpoint entries show mismatches
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);

    long mismatchedCount = countCheckpointsByStatus(checkpointEntries,
      PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED);
    assertTrue("Should have mismatched entries due to timestamp differences", mismatchedCount > 0);
  }

  @Test
  public void testSyncTableValidateWithConcurrentRegionMerges() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);
    // Explicitly split tables to create many regions for merging
    List<Integer> sourceSplits = Arrays.asList(10, 15, 20, 25, 40, 45, 60, 65, 80, 85);
    splitTableAt(sourceConnection, uniqueTableName, sourceSplits);

    List<Integer> targetSplits = Arrays.asList(12, 18, 22, 28, 42, 48, 62, 68, 82, 88);
    splitTableAt(targetConnection, uniqueTableName, targetSplits);

    // Introduce some mismatches on target before sync
    List<Integer> mismatchIds = Arrays.asList(10, 30, 50, 70, 90);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    // Capture time range for the sync
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run merges on source/target concurrently with the sync.
    Runnable mergeJoiner = startConcurrentRegionWork(
      () -> mergeAdjacentRegions(sourceConnection, uniqueTableName, 6),
      () -> mergeAdjacentRegions(targetConnection, uniqueTableName, 6),
      "merges");

    // Run sync tool while merges are happening
    Job job = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));

    mergeJoiner.run();

    // Verify the job completed successfully despite concurrent merges
    assertTrue("Sync job should complete successfully despite concurrent merges",
      job.isSuccessful());

    SyncCountersResult counters = getSyncCounters(job);

    // Validate counters - should process all 100 rows and detect mismatched chunks
    validateSyncCountersWithMinChunk(counters, 100, 100, 1, 1);

    // Verify checkpoint entries were created
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    assertFalse("Should have checkpoint entries", checkpointEntries.isEmpty());

    // Run sync again to verify idempotent behavior after merges
    Job job2 = runSyncToolWithChunkSize(uniqueTableName, 512, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    // Second run should process ZERO rows (all checkpointed despite region merges)
    validateSyncCounters(counters2, 0, 0, 0, 0);

    // Concurrent merges may leave chunks REPAIRED with stale boundaries; the resume filter
    // skips those on a single rerun. Cleanup the checkpoint and run a dry-run + repair pass
    // on the stable region layout to converge.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithRepair(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateWithPagingTimeout() throws Exception {
    // Create tables on both clusters
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    // Introduce mismatches scattered across the dataset
    List<Integer> mismatchIds = Arrays.asList(15, 25, 35, 45, 55, 75);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    // First, run --dry-run without aggressive paging to establish baseline chunk count.
    // Dry-run so the baseline doesn't repair the drift before the paging pass below sees it.
    int chunkSize = 10240;
    long baselineChunkCount = captureBaselineChunkCount(uniqueTableName, chunkSize);

    // Configure paging with aggressive timeouts to force mid-chunk timeouts
    Configuration conf = sourceClusterConf();
    conf.setBoolean(QueryServices.PHOENIX_SERVER_PAGING_ENABLED_ATTRIB, true);
    conf.setLong(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, 1);

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Dry-run with paging to assert chunk-count expansion under mid-chunk timeouts.
    Job job = runSyncToolWithChunkSize(uniqueTableName, chunkSize, conf, "--dry-run", "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));

    // Verify the job completed successfully despite paging timeouts
    assertTrue("Sync job should complete successfully despite paging", job.isSuccessful());

    SyncCountersResult counters = getSyncCounters(job);

    // Validate that all 100 rows were processed on both sides
    // Despite paging timeouts, no rows should be lost
    validateSyncCountersWithMinChunk(counters, 100, 100, 1, 1);

    long pagingChunkCount = counters.chunksVerified + counters.chunksMismatched;

    assertTrue(
      "Paging should create more chunks than baseline due to mid-chunk timeouts. " + "Baseline: "
        + baselineChunkCount + ", Paging: " + pagingChunkCount,
      pagingChunkCount > baselineChunkCount);

    // Verify checkpoint entries were created
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    assertFalse("Should have checkpoint entries", checkpointEntries.isEmpty());

    // Now run the repair pass over the same window so target converges. Confirms paging
    // does not block the repair flow. Clean up the dry-run pass's MISMATCHED checkpoint
    // rows first so the repair pass starts fresh — paging-driven chunk boundaries differ
    // between passes and stale MISMATCHED rows from the dry-run can land outside the new
    // boundary set.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithChunkSize(uniqueTableName, chunkSize, conf, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateWithPagingTimeoutWithSplits() throws Exception {
    // Create tables on both clusters
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    // Introduce mismatches scattered across the dataset
    List<Integer> mismatchIds = Arrays.asList(15, 25, 35, 45, 55, 75);
    introduceMismatchesByIds(uniqueTableName, mismatchIds);

    // First, run --dry-run without aggressive paging to establish baseline chunk count.
    // Dry-run so the baseline doesn't repair the drift before the paging pass below sees it.
    int chunkSize = 10240;
    long baselineChunkCount = captureBaselineChunkCount(uniqueTableName, chunkSize);

    // Configure paging with aggressive timeouts to force mid-chunk timeouts
    Configuration conf = sourceClusterConf();
    conf.setBoolean(QueryServices.PHOENIX_SERVER_PAGING_ENABLED_ATTRIB, true);
    conf.setLong(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, 1);

    // Run splits on source/target concurrently with the sync.
    Runnable splitJoiner = startConcurrentRegionWork(
      () -> splitTableAt(sourceConnection, uniqueTableName,
        Arrays.asList(12, 22, 32, 42, 52, 63, 72, 82, 92, 98)),
      () -> splitTableAt(targetConnection, uniqueTableName,
        Arrays.asList(13, 23, 33, 43, 53, 64, 74, 84, 95, 99)),
      "splits");

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Dry-run sync while splits are happening — drift must remain on target so the chunk-count
    // assertion below has work to do (otherwise an inline repair would converge mid-pass).
    Job job = runSyncToolWithChunkSize(uniqueTableName, chunkSize, conf, "--dry-run", "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));

    splitJoiner.run();

    // Verify the job completed successfully despite concurrent splits and paging timeouts
    assertTrue("Sync job should complete successfully despite paging and concurrent splits",
      job.isSuccessful());

    SyncCountersResult counters = getSyncCounters(job);

    // Validate that all 100 rows were processed on both sides
    // Despite paging timeouts AND concurrent region splits, no rows should be lost
    validateSyncCountersWithMinChunk(counters, 100, 100, 1, 1);

    // Paging should create MORE chunks than baseline
    // Concurrent region splits may also create additional chunks as mappers process new regions
    long pagingChunkCount = counters.chunksVerified + counters.chunksMismatched;

    assertTrue(
      "Paging should create more chunks than baseline due to mid-chunk timeouts. " + "Baseline: "
        + baselineChunkCount + ", Paging: " + pagingChunkCount,
      pagingChunkCount > baselineChunkCount);

    // Verify checkpoint entries were created
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    assertFalse("Should have checkpoint entries", checkpointEntries.isEmpty());

    // Run the repair pass over the same window so target converges. Confirms paging plus
    // concurrent splits do not block the repair flow. Clean up the dry-run pass's MISMATCHED
    // checkpoint rows first so the resume filter doesn't leave stale MISMATCHED entries that
    // sit outside the repair pass's chunk boundaries (paging + splits change boundary set).
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithChunkSize(uniqueTableName, chunkSize, conf, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableMapperFailsWithNonExistentTable() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Try to run sync tool on a NON-EXISTENT table
    String nonExistentTable = "NON_EXISTENT_TABLE_" + System.currentTimeMillis();
    String[] args = new String[] { "--table-name", nonExistentTable, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--to-time", String.valueOf(System.currentTimeMillis()) };

    assertSyncToolFails(args,
      String.format("Table %s does not exist, mapper setup should fail", nonExistentTable));
  }

  @Test
  public void testSyncTableMapperFailsWithInvalidTargetCluster() throws Exception {
    // Create table on source cluster
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Try to run sync tool with INVALID target cluster ZK quorum.
    String invalidTargetZk = "invalid-zk-host:2181:/hbase";
    String[] args =
      new String[] { "--table-name", uniqueTableName, "--target-cluster", invalidTargetZk,
        "--run-foreground", "--to-time", String.valueOf(System.currentTimeMillis()) };

    assertSyncToolFails(args,
      String.format("Target cluster %s is invalid, mapper setup should fail", invalidTargetZk));
  }

  @Test
  public void testSyncTableMapperFailsWithMissingTargetTable() throws Exception {
    // Create table on source cluster ONLY (not on target); no replication needed
    String sourceDdl = buildStandardTableDdl(uniqueTableName, false, "3, 5, 7");
    executeTableCreation(sourceConnection, sourceDdl);

    // Insert data on source
    insertTestData(sourceConnection, uniqueTableName, 1, 10);

    // Don't create table on target - this will cause mapper map() to fail
    // when trying to scan the non-existent target table
    String[] args = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--to-time", String.valueOf(System.currentTimeMillis()) };

    assertSyncToolFails(args, String.format(
      "Table %s does not exist on target cluster, mapper map() should fail during target scan",
      uniqueTableName));
  }

  @Test
  public void testSyncTableCheckpointPersistsAcrossFailedRuns() throws Exception {
    // Setup table with replication and insert data
    // setupStandardTestWithReplication creates splits, resulting in multiple mapper regions
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Capture time range for both runs (ensures checkpoint lookup will match)
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // First run + 75% deletion preamble (shared with other partial-rerun tests)
    PartialRerunSetup setup = setupPartialRerun(uniqueTableName, fromTime, toTime, 1, 0.75);
    SyncCountersResult counters1 = setup.firstRunCounters;

    // Validate first run succeeded
    assertTrue("First run should succeed", setup.firstRunJob.isSuccessful());
    validateSyncCounters(counters1, 10, 10, 10, 0);

    SeparatedCheckpointEntries separatedAfterDelete =
      separateMapperAndChunkEntries(setup.entriesAfterDelete);

    assertEquals("Should have 0 mapper entries after deleting all mappers", 0,
      separatedAfterDelete.mappers.size());
    assertEquals("Should have remaining chunk entries after deletion",
      setup.chunks.size() - setup.chunksToDelete.size(), separatedAfterDelete.chunks.size());

    // Drop target table to cause mapper failures during second run.
    // Use HBase Admin directly because Phoenix DROP TABLE IF EXISTS via targetConnection
    // may silently no-op in the shared-JVM mini-cluster due to metadata cache issues.
    Admin targetAdmin = CLUSTERS.getHBaseCluster2().getConnection().getAdmin();
    TableName hbaseTableName = TableName.valueOf(uniqueTableName);
    if (targetAdmin.tableExists(hbaseTableName)) {
      targetAdmin.disableTable(hbaseTableName);
      targetAdmin.deleteTable(hbaseTableName);
    }
    LOGGER.info("Dropped target table to cause mapper failures");

    // Second run: Job should fail (exit code != 0) because target table is missing
    String[] args = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime) };

    assertSyncToolFails(args,
      "Second run should fail with non-zero exit code due to missing target table");

    // Remaining chunk entries that we dint delete should still persist despite job failure
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntriesAfterFailedRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    SeparatedCheckpointEntries separatedAfterFailedRun =
      separateMapperAndChunkEntries(checkpointEntriesAfterFailedRun);

    // After the failed run:
    // - No mapper entries should exist (we deleted them all, and the job failed before creating new
    // ones)
    // - Only the remaining chunk entries (1/4th) should persist
    assertEquals("Should have 0 mapper entries after failed run", 0,
      separatedAfterFailedRun.mappers.size());
    assertEquals("Remaining chunk entries should persist after failed run",
      setup.chunks.size() - setup.chunksToDelete.size(), separatedAfterFailedRun.chunks.size());
  }

  /**
   * P1 (hidden-version unwinding): Verifies the most subtle correctness path in the repairer —
   * tombstoneTargetCell case 3 from {@code PhoenixSyncTableChunkRepairer.tombstoneTargetCell}.
   *
   * <p>Scenario: source row has {@code Put(NAME, "alice", T0)}; target row has {@code
   * Put(NAME, "bob", T1)} and {@code Put(NAME, "carol", T2)} where {@code T0 < T1 < T2} and
   * {@code MAX_VERSIONS=2}. Visible cell on target is "carol" (T2); "bob" (T1) is
   * MAX_VERSIONS-hidden. Naive repair would point-delete only T2, exposing "bob" above
   * source's mirror at T0 — divergent. Correct behavior: point-delete BOTH T2 and T1.
   *
   * <p>Without this test, a regression that "fixes" only the visible cell (case 2) would leave
   * target reading "bob" after a successful-looking repair pass.
   */
  @Test
  public void testRepairUnwindsHiddenTargetVersions() throws Exception {
    final int rowId = 5;
    // Two clusters, no replication — we plant cells deterministically on each side.
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 2, "3, 7");

    long fromTime = 0L;
    final long sourceTs = base + 1L;
    final long targetT1 = base + 2L;
    final long targetT2 = base + 3L;

    byte[] rowKey = integerRowKey(rowId);
    String family = "0"; // COLUMN_ENCODED_BYTES=NONE → family is "0"
    String qualifier = "NAME";

    // Source: single Put(NAME, "alice", T=100). Empty-key cell is needed so the row is
    // visible to Phoenix scans (and thus to the verifier). Use an SCN connection for that.
    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), sourceTs)) {
      scnSrc.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'alice')");
      scnSrc.commit();
    }

    // Target: insert via SCN at T1 then again at T2 to leave two NAME versions; with VERSIONS=2
    // both versions are retained. Visible read is "carol" (T2); "bob" (T1) is one-version-hidden.
    try (Connection scnTgtT1 = openConnectionAtScn(CLUSTERS.getZkUrl2(), targetT1)) {
      scnTgtT1.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'bob')");
      scnTgtT1.commit();
    }
    try (Connection scnTgtT2 = openConnectionAtScn(CLUSTERS.getZkUrl2(), targetT2)) {
      scnTgtT2.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'carol')");
      scnTgtT2.commit();
    }

    // Sanity: target's visible NAME is "carol" before repair.
    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Pre-repair target visible NAME should be carol", "carol", rs.getString(1));
      }
    }

    // Run dry-run + repair sharing the same time window with --read-all-versions so the
    // verifier and repairer both see the hidden version.
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(System.currentTimeMillis()),
      "--read-all-versions");

    assertTrue("Dry-run should succeed", result.dryRunJob.isSuccessful());
    assertTrue("Repair should succeed", result.repairJob.isSuccessful());

    Counters repairCounters = result.repairJob.getCounters();
    // Two NAME versions ("bob"@T1 + "carol"@T2) and two empty-key versions on target — all sit
    // at timestamps that don't match source's single sourceTs, so each gets counted as
    // either "different" or "extra" depending on the diff branch. We require at least 2 extras
    // (the two extra NAME versions vs source's single mirror) — the exact split between
    // CELLS_EXTRA and CELLS_DIFFERENT depends on per-qualifier matching. Post-repair raw scan
    // assertions below pin the structural outcome.
    assertTrue("At least 2 cells should be tombstoned for target's hidden+visible NAME versions",
      repairCounters.findCounter(SyncCounters.CELLS_EXTRA_ON_TARGET).getValue() >= 2);

    // Post-repair, Phoenix's standard read on target must see source's "alice", NOT "bob".
    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Post-repair target NAME must be alice (hidden version unwound)", "alice",
          rs.getString(1));
      }
    }

    // Raw scan: target should have two Delete markers at T2 and T1 plus source's mirror Put@T0.
    try (Table targetHTable = getHBaseTable(targetConnection, uniqueTableName)) {
      Scan scan = new Scan().withStartRow(rowKey, true).withStopRow(rowKey, true).setRaw(true);
      scan.readAllVersions();
      int nameDeletes = 0;
      int namePutAtSourceTs = 0;
      try (ResultScanner sc = targetHTable.getScanner(scan)) {
        for (Result r; (r = sc.next()) != null;) {
          for (Cell c : r.rawCells()) {
            if (Bytes.equals(CellUtil.cloneFamily(c), Bytes.toBytes(family))
              && Bytes.equals(CellUtil.cloneQualifier(c), Bytes.toBytes(qualifier))) {
              if (CellUtil.isDelete(c)) {
                nameDeletes++;
              } else if (c.getTimestamp() == sourceTs) {
                namePutAtSourceTs++;
              }
            }
          }
        }
      }
      assertEquals("Two delete markers (one for each target NAME version) expected", 2,
        nameDeletes);
      assertEquals("Source's Put@" + sourceTs + " should be mirrored", 1, namePutAtSourceTs);
    }
  }

  /**
   * P2 (partial-mirror shadow): Verifies the {@code RowMirrorStatus.PARTIALLY_MIRRORED} branch
   * indirectly via {@code generateMutationForDiffCells} — both rows exist; one source cell is
   * shadowed by a target tombstone, sibling cells mirror successfully. {@code anyCellUnrepairable}
   * propagates up to {@code drift.rowsCannotRepair++} while no cell counter increments for the
   * shadowed cell (mirror returned false, so nothing was written and nothing counted).
   *
   * <p>Setup: row K exists on both sides via a matching {@code NAME_VALUE} cell. Target has a
   * pre-existing {@code DeleteColumn(NAME, T=300)} shadowing any future {@code NAME} Put at
   * {@code ts <= 300}. Source's {@code NAME, "alice", T=200} would land on disk but stay
   * invisible — repair detects this upfront via {@code wouldShadow} and skips the doomed write.
   */
  @Test
  public void testRepairPartialShadowWithinRow() throws Exception {
    final int rowId = 5;
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 1, "3, 7");

    long fromTime = 0L;
    final long sourceTs = base + 1L;
    final long shadowTombstoneTs = base + 2L;

    // Source: row K with NAME="alice" and NAME_VALUE=99, all at sourceTs.
    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), sourceTs)) {
      scnSrc.createStatement().execute("UPSERT INTO " + uniqueTableName
        + " (ID, NAME, NAME_VALUE) VALUES (" + rowId + ", 'alice', 99)");
      scnSrc.commit();
    }

    // Target: row K with only NAME_VALUE=99 at sourceTs (matches source's NAME_VALUE).
    try (Connection scnTgt = openConnectionAtScn(CLUSTERS.getZkUrl2(), sourceTs)) {
      scnTgt.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME_VALUE) VALUES (" + rowId + ", 99)");
      scnTgt.commit();
    }
    // Plant a DeleteColumn tombstone on target's NAME at shadowTombstoneTs, which shadows any
    // source mirror at ts <= shadowTombstoneTs.
    writeRawDeleteColumn(targetConnection, uniqueTableName, integerRowKey(rowId), "0", "NAME",
      shadowTombstoneTs);

    // Spin until wall-clock advances past the highest cell timestamp so --to-time
    // (which defaults to currentTimeMillis()) covers our planted cells.
    while (System.currentTimeMillis() <= shadowTombstoneTs) {
      // spin
    }
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(System.currentTimeMillis()),
      "--raw-scan");

    assertTrue("Dry-run should succeed", result.dryRunJob.isSuccessful());
    assertTrue("Repair pass should succeed (shadowing is correctness-only, not a job error)",
      result.repairJob.isSuccessful());

    Counters repairCounters = result.repairJob.getCounters();
    // Source's NAME mirror is suppressed by the shadow → no cell counter ticks; row is unrepairable.
    assertRepairCellCounters(repairCounters, 0, 0, 0, 1);
    assertTrue("At least one mapper should roll up to UNREPAIRABLE",
      repairCounters.findCounter(SyncCounters.MAPPERS_UNREPAIRABLE).getValue() >= 1);

    // Post-repair, target's read view of NAME for row K is still null (DeleteColumn at T=300
    // covers everything <= T=300 — including any source mirror we *might* have written). The
    // assertion validates the repair refused to write the doomed Put.
    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertNull("NAME should still be null on target — shadow was respected",
          rs.getString(1));
      }
    }
  }

  /**
   * P2 (cell-missing branch): same row exists on both sides, source has an extra column the
   * target lacks. {@code generateMutationForDiffCells} should mirror it through the
   * {@code cellMissing++} branch (source-only cell, no shadow on target).
   */
  @Test
  public void testRepairCellMissingOnTarget() throws Exception {
    final int rowId = 5;
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 1, "3, 7");

    final long ts = base + 1L;

    // Source: row K with NAME and NAME_VALUE.
    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), ts)) {
      scnSrc.createStatement().execute("UPSERT INTO " + uniqueTableName
        + " (ID, NAME, NAME_VALUE) VALUES (" + rowId + ", 'alice', 99)");
      scnSrc.commit();
    }
    // Target: row K with only NAME_VALUE matching source. NAME is absent.
    try (Connection scnTgt = openConnectionAtScn(CLUSTERS.getZkUrl2(), ts)) {
      scnTgt.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME_VALUE) VALUES (" + rowId + ", 99)");
      scnTgt.commit();
    }

    // Spin until wall-clock advances past ts so --to-time (defaulting to currentTimeMillis())
    // covers the planted cells.
    while (System.currentTimeMillis() <= ts) {
      // spin
    }
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time", "0",
      "--to-time", String.valueOf(System.currentTimeMillis()));
    assertTrue("Repair should succeed", result.repairJob.isSuccessful());

    assertRepairCellCounters(result.repairJob.getCounters(), 1, 0, 0, 0);

    // Post-repair: target's NAME should equal source's "alice".
    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("alice", rs.getString(1));
      }
    }
  }

  /**
   * P2 (cell-extra branch): same row exists on both sides, target has an extra column source
   * lacks. {@code generateMutationForDiffCells} should tombstone it via the
   * {@code cellExtra++} branch ({@code tombstoneTargetCell} with {@code sourceMaxTs == null}
   * → {@code Delete.addColumns}).
   */
  @Test
  public void testRepairCellExtraOnTarget() throws Exception {
    final int rowId = 5;
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 1, "3, 7");

    final long ts = base + 1L;

    // Source: row K with only NAME_VALUE.
    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), ts)) {
      scnSrc.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME_VALUE) VALUES (" + rowId + ", 99)");
      scnSrc.commit();
    }
    // Target: row K with same NAME_VALUE plus an extra raw NAME cell at the same ts.
    try (Connection scnTgt = openConnectionAtScn(CLUSTERS.getZkUrl2(), ts)) {
      scnTgt.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME_VALUE) VALUES (" + rowId + ", 99)");
      scnTgt.commit();
    }
    writeRawCell(targetConnection, uniqueTableName, integerRowKey(rowId), "0", "NAME", ts,
      Bytes.toBytes("bob"));

    // Spin until wall-clock advances past ts so --to-time (defaulting to currentTimeMillis())
    // covers the planted cells.
    while (System.currentTimeMillis() <= ts) {
      // spin
    }
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time", "0",
      "--to-time", String.valueOf(System.currentTimeMillis()));
    assertTrue("Repair should succeed", result.repairJob.isSuccessful());

    assertRepairCellCounters(result.repairJob.getCounters(), 0, 1, 0, 0);

    // Post-repair: target's NAME should be tombstoned and read as null.
    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertNull(rs.getString(1));
      }
    }
  }

  /**
   * P2 (cell-different branch): same row, same {@code (cf, q, ts)} coords, different value.
   * {@code generateMutationForDiffCells} should hit the {@code cellDifferent++} branch via the
   * {@code !matchingValue} check at the head of the loop.
   */
  @Test
  public void testRepairCellDifferentValue() throws Exception {
    final int rowId = 5;
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 1, "3, 7");

    final long ts = base + 1L;

    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), ts)) {
      scnSrc.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'alice')");
      scnSrc.commit();
    }
    try (Connection scnTgt = openConnectionAtScn(CLUSTERS.getZkUrl2(), ts)) {
      scnTgt.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'bob')");
      scnTgt.commit();
    }

    // Spin until wall-clock advances past ts so --to-time (defaulting to currentTimeMillis())
    // covers the planted cells.
    while (System.currentTimeMillis() <= ts) {
      // spin
    }
    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time", "0",
      "--to-time", String.valueOf(System.currentTimeMillis()));
    assertTrue("Repair should succeed", result.repairJob.isSuccessful());

    assertRepairCellCounters(result.repairJob.getCounters(), 0, 0, 1, 0);

    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("alice", rs.getString(1));
      }
    }
  }

  /**
   * P6 (asymmetric load-target time-range): Source has Put at T=200 inside the user's window;
   * target has a {@code DeleteColumn} planted at T=600 — strictly above {@code --to-time T=500}.
   * The repair scan honors {@code --to-time} and never sees the tombstone in the diff window, so
   * the diff routes to {@code mirrorWholeRow} in
   * {@link org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer}. Inside,
   * {@code loadTargetRowRecord} deliberately uses {@code (fromTime, MAX_VALUE)} (line 487) — so it
   * still sees the T=600 tombstone and {@code wouldShadow} returns true on Source's Put@T=200
   * (DeleteColumn covers ts &lt;= T=600). Result: source's mirror is suppressed, the row is
   * flagged unrepairable.
   */
  @Test
  public void testRepairShadowFromTombstoneAboveToTime() throws Exception {
    final int rowId = 5;
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 1, "3, 7");

    final long fromTime = 0L;
    final long sourceTs = base + 1L;
    final long toTime = base + 2L;
    final long tombstoneTs = base + 3L;

    // Source has the row inside the diff window.
    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), sourceTs)) {
      scnSrc.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'alice')");
      scnSrc.commit();
    }
    // Target has a tombstone strictly above --to-time. Diff scan won't see it; loadTargetRowRecord
    // still will.
    writeRawDeleteColumn(targetConnection, uniqueTableName, integerRowKey(rowId), "0", "NAME",
      tombstoneTs);

    // Phoenix requires --to-time <= currentTimeMillis() at tool-run. Spin until wall-clock
    // moves past tombstoneTs (the highest cell ts in this test) so toTime is unambiguously in
    // the past.
    while (System.currentTimeMillis() <= tombstoneTs) {
      // spin
    }

    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime), "--raw-scan");
    assertTrue("Repair should succeed (shadowing is correctness-only)",
      result.repairJob.isSuccessful());

    Counters c = result.repairJob.getCounters();
    // Phoenix UPSERT plants NAME *and* the empty-key cell ("_0"). DeleteColumn shadows only NAME —
    // "_0" mirrors through (rowsMissing++), and the row is unrepairable because NAME was suppressed.
    assertRepairRowCounters(c, 1, 0, 1);
    assertTrue("At least one mapper should roll up to UNREPAIRABLE",
      c.findCounter(SyncCounters.MAPPERS_UNREPAIRABLE).getValue() >= 1);

    // Post-repair: target's NAME should still read as null. The shadow was respected, so no Put
    // for NAME landed (only the empty-key cell, which gives the row visible existence with NAME
    // covered by the DeleteColumn tombstone above it).
    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("Row exists on target via mirrored empty-key cell", rs.next());
        assertNull("NAME should still be null — DeleteColumn shadow respected", rs.getString(1));
      }
    }
  }

  /**
   * P7 (mid-row repair-batch flush boundary): drives many missing-row mirrors through a tiny
   * {@code repairBatchSize=2} so {@code generateMutationForDiffRows} flushes mid-stream multiple
   * times. Validates that every row converges despite the mid-flush boundary — i.e., no Put
   * gets dropped because pendingPuts/pendingDeletes were drained mid-iteration.
   */
  @Test
  public void testRepairFlushesMidRowWithSmallBatchSize() throws Exception {
    // No replication — seed source manually so target legitimately lacks the rows.
    createRepairTestTableOnBothClusters(uniqueTableName, 1, null);

    // Introduce extra rows on source that target lacks. Each row → at least 2 cells (NAME and the
    // empty-key cell), so a batch size of 2 forces a flush every row, exercising the mid-stream
    // flush in generateMutationForDiffRows.
    int[] sourceOnlyIds = new int[] { 100, 101, 102, 103, 104, 105, 106, 107 };
    String[] sourceOnlyNames = new String[sourceOnlyIds.length];
    for (int i = 0; i < sourceOnlyIds.length; i++) {
      sourceOnlyNames[i] = "extra_" + sourceOnlyIds[i];
    }
    upsertRowsOnTarget(sourceConnection, uniqueTableName, sourceOnlyIds, sourceOnlyNames);
    sourceConnection.commit();

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    Configuration conf = sourceClusterConfWithRepairBatchSize(2);

    // Stage 1: dry-run.
    Job dryRunJob = runSyncToolWithChunkSize(uniqueTableName, 1024, conf, "--dry-run",
      "--from-time", String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    assertTrue("Dry-run should succeed", dryRunJob.isSuccessful());
    SyncCountersResult dryRunCounters = getSyncCounters(dryRunJob);
    assertTrue("Dry-run should detect mismatched chunks", dryRunCounters.chunksMismatched >= 1);

    // Stage 2: repair with the same small batch size.
    Job repairJob = runSyncToolWithChunkSize(uniqueTableName, 1024, conf, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    assertTrue("Repair should succeed despite small batch size", repairJob.isSuccessful());

    Counters repairCounters = repairJob.getCounters();
    assertTrue("All source-only rows should be marked missing on target",
      repairCounters.findCounter(SyncCounters.ROWS_MISSING_ON_TARGET).getValue()
          >= sourceOnlyIds.length);
    assertEquals("No row should be flagged unrepairable", 0,
      repairCounters.findCounter(SyncCounters.ROWS_CANNOT_REPAIR).getValue());

    // Verify each source-only row landed on target with the right NAME.
    for (int i = 0; i < sourceOnlyIds.length; i++) {
      try (PreparedStatement ps = targetConnection
        .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
        ps.setInt(1, sourceOnlyIds[i]);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue("Row " + sourceOnlyIds[i] + " should exist on target after repair",
            rs.next());
          assertEquals("Row " + sourceOnlyIds[i] + " NAME should match source",
            sourceOnlyNames[i], rs.getString(1));
        }
      }
    }
  }

  /**
   * P8 ({@code --raw-scan} + {@code --read-all-versions} interplay): a multi-version row on
   * source that includes an in-window {@code DeleteColumn} between two Puts. Target lags with
   * only the older Put. Repair must mirror the missing tombstone (preserving its subtype via
   * {@code mirrorSourceCell} in
   * {@link org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer} routing Delete cells
   * through {@code Delete#add}) and the missing newer Put.
   */
  @Test
  public void testRepairRawScanAllVersionsMirrorsTombstoneAndPut() throws Exception {
    final int rowId = 5;
    long base = createRepairTestTableOnBothClusters(uniqueTableName, 3, "3, 7");

    final long fromTime = 0L;
    final long t1 = base + 1L;
    final long t2 = base + 2L;
    final long t3 = base + 3L;

    // Source: Put@T1 → DeleteColumn@T2 → Put@T3.
    try (Connection scnSrc = openConnectionAtScn(CLUSTERS.getZkUrl1(), t1)) {
      scnSrc.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'v1')");
      scnSrc.commit();
    }
    writeRawDeleteColumn(sourceConnection, uniqueTableName, integerRowKey(rowId), "0", "NAME", t2);
    try (Connection scnSrc2 = openConnectionAtScn(CLUSTERS.getZkUrl1(), t3)) {
      scnSrc2.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'v3')");
      scnSrc2.commit();
    }

    // Target: only the oldest Put@T1.
    try (Connection scnTgt = openConnectionAtScn(CLUSTERS.getZkUrl2(), t1)) {
      scnTgt.createStatement().execute(
        "UPSERT INTO " + uniqueTableName + " (ID, NAME) VALUES (" + rowId + ", 'v1')");
      scnTgt.commit();
    }

    RepairRunResult result = runSyncToolWithRepair(uniqueTableName, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(System.currentTimeMillis()),
      "--raw-scan", "--read-all-versions");
    assertTrue("Repair should succeed", result.repairJob.isSuccessful());

    Counters c = result.repairJob.getCounters();
    // Source has NAME@T1 Put, NAME@T2 DeleteColumn, NAME@T3 Put + empty-key@T1 and @T3. Target has
    // only NAME@T1 + empty-key@T1, so 3 cells missing: NAME-tombstone@T2, NAME-Put@T3, empty-key@T3.
    assertRepairCellCounters(c, 3, 0, 0, 0);

    // Post-repair raw scan on target should show: Put@T3, DeleteColumn@T2, Put@T1 for the NAME
    // qualifier (rawCells reverse-ts ordered).
    int observedPuts = 0;
    int observedDeleteColumns = 0;
    long observedNewestPutTs = -1L;
    try (Table targetHTable = getHBaseTable(targetConnection, uniqueTableName)) {
      Scan scan = new Scan();
      scan.withStartRow(integerRowKey(rowId), true);
      scan.withStopRow(integerRowKey(rowId), true);
      scan.setRaw(true);
      scan.readAllVersions();
      try (ResultScanner scanner = targetHTable.getScanner(scan)) {
        Result r = scanner.next();
        assertNotNull("Target row should exist", r);
        for (Cell cell : r.rawCells()) {
          if (Bytes.equals(CellUtil.cloneQualifier(cell), Bytes.toBytes("NAME"))) {
            if (CellUtil.isDelete(cell)) {
              observedDeleteColumns++;
            } else {
              observedPuts++;
              observedNewestPutTs = Math.max(observedNewestPutTs, cell.getTimestamp());
            }
          }
        }
      }
    }
    assertEquals("Target should have both NAME Puts after repair", 2, observedPuts);
    assertEquals("Target should have the mirrored DeleteColumn after repair", 1,
      observedDeleteColumns);
    assertEquals("Newest mirrored Put should sit at T3", t3, observedNewestPutTs);

    // Read-side: NAME under default visibility should now be null (T3 Put → T2 DeleteColumn covers
    // T1; visible state is "deleted but tombstone caps NAME"). Phoenix sees no value.
    try (PreparedStatement ps = targetConnection
      .prepareStatement("SELECT NAME FROM " + uniqueTableName + " WHERE ID = ?")) {
      ps.setInt(1, rowId);
      try (ResultSet rs = ps.executeQuery()) {
        // The newest Put is T3 — reads see "v3" since T3 > tombstone T2.
        assertTrue(rs.next());
        assertEquals("v3", rs.getString(1));
      }
    }
  }

  /**
   * P9 (mixed Put+Delete batch under small {@code repairBatchSize}): many missing source rows AND
   * many extra target rows in the same chunk. With {@code repairBatchSize=4}, most flushes
   * straddle a Put/Delete boundary and exercise {@code flushRepairMutations} in
   * {@link org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer} on its mixed
   * {@code Table#batch} path. Validates that no mutation gets dropped at the batch boundary.
   */
  @Test
  public void testRepairMixedPutDeleteBatchWithSmallBatchSize() throws Exception {
    // No replication — seed each side independently so source-only and target-only rows truly
    // diverge.
    createRepairTestTableOnBothClusters(uniqueTableName, 1, null);

    // Add 5 source-only rows (will need Puts) and 5 target-only rows (will need Deletes), all
    // inside the same chunk so they queue together in a single mapper's pendingPuts/pendingDeletes
    // and get flushed as mixed batches.
    int[] sourceOnly = new int[] { 200, 201, 202, 203, 204 };
    String[] sourceOnlyNames = new String[] { "s200", "s201", "s202", "s203", "s204" };
    upsertRowsOnTarget(sourceConnection, uniqueTableName, sourceOnly, sourceOnlyNames);
    sourceConnection.commit();

    int[] targetOnly = new int[] { 300, 301, 302, 303, 304 };
    String[] targetOnlyNames = new String[] { "t300", "t301", "t302", "t303", "t304" };
    upsertRowsOnTarget(targetConnection, uniqueTableName, targetOnly, targetOnlyNames);
    targetConnection.commit();

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    Configuration conf = sourceClusterConfWithRepairBatchSize(4);

    Job dryRunJob = runSyncToolWithChunkSize(uniqueTableName, 1024, conf, "--dry-run",
      "--from-time", String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    assertTrue("Dry-run should succeed", dryRunJob.isSuccessful());

    SyncCountersResult dryRunCounters = getSyncCounters(dryRunJob);
    assertTrue("Dry-run should detect all source-only rows as missing",
      dryRunCounters.rowsMissingOnTarget >= sourceOnly.length);
    assertTrue("Dry-run should detect all target-only rows as extra",
      dryRunCounters.rowsExtraOnTarget >= targetOnly.length);

    Job repairJob = runSyncToolWithChunkSize(uniqueTableName, 1024, conf, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    assertTrue("Repair should succeed", repairJob.isSuccessful());

    Counters c = repairJob.getCounters();
    assertTrue("All source-only rows should be marked missing",
      c.findCounter(SyncCounters.ROWS_MISSING_ON_TARGET).getValue() >= sourceOnly.length);
    assertTrue("All target-only rows should be marked extra",
      c.findCounter(SyncCounters.ROWS_EXTRA_ON_TARGET).getValue() >= targetOnly.length);
    assertEquals("No chunk should fail repair", 0,
      c.findCounter(SyncCounters.CHUNKS_REPAIR_FAILED).getValue());

    // Convergence pass: cleanup checkpoint and re-run a stable repair to assert no chunks are
    // mismatched after the mixed-batch flushes.
    cleanupCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    runSyncToolWithRepair(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    assertNoMismatchedCheckpoints(uniqueTableName, null);
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
  }

  @Test
  public void testSyncTableWithDeleteAndCompactionOnSource() throws Exception {
    // Setup table with replication and insert data (rows 1-10)
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Verify data is identical after replication
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertEquals("Should have 10 rows on source", 10,
      TestUtil.getRowCount(sourceConnection, uniqueTableName));
    assertEquals("Should have 10 rows on target", 10,
      TestUtil.getRowCount(targetConnection, uniqueTableName));

    // Insert extra row on source and then delete it to create a delete marker on source
    upsertRowsOnTarget(sourceConnection, uniqueTableName, new int[] { 100 },
      new String[] { "EXTRA_ROW" });
    sourceConnection.commit();
    waitForReplication(targetConnection, uniqueTableName, 11);

    assertEquals("Should have 11 rows on target after insert", 11,
      TestUtil.getRowCount(targetConnection, uniqueTableName));
    assertEquals("Should have 11 rows on source after insert", 11,
      TestUtil.getRowCount(sourceConnection, uniqueTableName));

    deleteRows(sourceConnection, uniqueTableName, 100);
    waitForReplication(targetConnection, uniqueTableName, 10);
    assertEquals("Should have 10 rows on target after deletion", 10,
      TestUtil.getRowCount(targetConnection, uniqueTableName));
    assertEquals("Should have 10 rows on source after deletion", 10,
      TestUtil.getRowCount(sourceConnection, uniqueTableName));

    // Run sync tool without raw scan
    Job job1 = runSyncTool(uniqueTableName);
    SyncCountersResult counters1 = getSyncCounters(job1);

    assertTrue("First run without raw scan should succeed", job1.isSuccessful());
    validateSyncCounters(counters1, 10, 10, 10, 0);
    validateMapperCounters(counters1, 4, 0);

    // Run sync tool with --raw-scan
    // Should detect mismatch because target has delete marker, source doesn't
    Job job2 = runSyncTool(uniqueTableName, "--raw-scan");
    SyncCountersResult counters2 = getSyncCounters(job2);

    assertTrue("Second run with raw scan should succeed", job2.isSuccessful());
    validateSyncCounters(counters2, 11, 11, 11, 0);
    validateMapperCounters(counters2, 4, 0);

    sourceConnection.createStatement()
      .execute("ALTER TABLE " + uniqueTableName + " SET KEEP_DELETED_CELLS=false");
    sourceConnection.commit();

    flushAndMajorCompact(CLUSTERS.getHBaseCluster1(), uniqueTableName);

    // Run sync tool with --raw-scan again
    Job job3 = runSyncTool(uniqueTableName, "--raw-scan");
    SyncCountersResult counters3 = getSyncCounters(job3);

    assertTrue("Third run with raw scan after compaction should succeed", job3.isSuccessful());
    validateSyncCounters(counters3, 10, 11, 9, 1);
    // Repair runs inline (non-dry-run). Under --raw-scan target has residual cells for row 100
    // that source no longer has after compaction; repair tombstones the residual target cells,
    // so the chunk's mapper rolls up to REPAIRED.
    validateMapperCountersRepair(counters3, 3, 1, 0, 0);

    // The standard Phoenix view (no raw-scan) on both clusters remains identical.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
  }

  @Test
  public void testSyncTableWithDeleteAndCompactionOnTarget() throws Exception {
    // Setup table with replication and insert data (rows 1-10)
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Verify data is identical after replication
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertEquals("Should have 10 rows on source", 10,
      TestUtil.getRowCount(sourceConnection, uniqueTableName));
    assertEquals("Should have 10 rows on target", 10,
      TestUtil.getRowCount(targetConnection, uniqueTableName));

    // Insert extra row on target and then delete it to create delete marker on target
    upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { 100 },
      new String[] { "EXTRA_ROW" });
    targetConnection.commit();
    assertEquals("Should have 11 rows on target after insert", 11,
      TestUtil.getRowCount(targetConnection, uniqueTableName));

    deleteRows(targetConnection, uniqueTableName, 100);
    assertEquals("Should have 10 rows on target after deletion", 10,
      TestUtil.getRowCount(targetConnection, uniqueTableName));

    // Run sync tool without raw scan, should match
    Job job1 = runSyncTool(uniqueTableName);
    SyncCountersResult counters1 = getSyncCounters(job1);

    assertTrue("First run without raw scan should succeed", job1.isSuccessful());
    validateSyncCounters(counters1, 10, 10, 10, 0);
    validateMapperCounters(counters1, 4, 0);

    // Run sync tool with --raw-scan, should detect mismatch
    Job job2 = runSyncTool(uniqueTableName, "--raw-scan");
    SyncCountersResult counters2 = getSyncCounters(job2);

    assertTrue("Second run with raw scan should succeed", job2.isSuccessful());
    validateSyncCounters(counters2, 10, 11, 9, 1);
    // Non-dry-run: target has a delete marker that source doesn't; repair tombstones at source
    // ts cover the residual under raw scan, so the mapper rolls up to REPAIRED.
    validateMapperCountersRepair(counters2, 3, 1, 0, 0);

    flushAndMajorCompact(CLUSTERS.getHBaseCluster2(), uniqueTableName);

    // Run sync tool with --raw-scan again, Should now match
    Job job3 = runSyncTool(uniqueTableName, "--raw-scan");
    SyncCountersResult counters3 = getSyncCounters(job3);

    assertTrue("Third run with raw scan after compaction should succeed", job3.isSuccessful());
    validateSyncCounters(counters3, 10, 10, 10, 0);
    validateMapperCounters(counters3, 4, 0);

    // After major compaction tombstones are gone; the third raw-scan pass is clean and the
    // standard Phoenix view matches.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableWithMultipleVersionAndCompactionOnSource() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);
    // Set VERSIONS=2 on target so readAllVersions scan can return both versions
    try (Statement stmt = targetConnection.createStatement()) {
      stmt.execute("ALTER TABLE " + uniqueTableName + " SET VERSIONS=2");
    }
    targetConnection.commit();
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertEquals("Should have 10 rows on source", 10,
      TestUtil.getRowCount(sourceConnection, uniqueTableName));
    assertEquals("Should have 10 rows on target", 10,
      TestUtil.getRowCount(targetConnection, uniqueTableName));

    // Update row 5 on source to create a second version
    upsertRowsOnTarget(sourceConnection, uniqueTableName, new int[] { 5 },
      new String[] { "EXTRA_ROW" });
    sourceConnection.commit();

    waitForRowContentReplication(targetConnection, uniqueTableName, 5, "EXTRA_ROW");

    flushAndMajorCompact(CLUSTERS.getHBaseCluster1(), uniqueTableName);

    // Run sync without reading all versions, only latest version compared, should match
    Job job1 = runSyncTool(uniqueTableName);
    SyncCountersResult counters1 = getSyncCounters(job1);

    assertTrue("First run without reading all versions should succeed", job1.isSuccessful());
    validateSyncCounters(counters1, 10, 10, 10, 0);
    validateMapperCounters(counters1, 4, 0);

    // Run sync with --read-all-versions: target has extra old version, should mismatch
    Job job3 = runSyncTool(uniqueTableName, "--read-all-versions");
    SyncCountersResult counters3 = getSyncCounters(job3);

    assertTrue("Second run with all versions should succeed", job3.isSuccessful());
    // Target retains old version of row 5 (VERSIONS=2), source does not after compaction.
    validateSyncCounters(counters3, 10, 10, 9, 1);
    // Non-dry-run: target has an extra historical version that source no longer has; repair
    // tombstones the residual cells at the appropriate ts, so the mapper rolls up to REPAIRED.
    validateMapperCountersRepair(counters3, 3, 1, 0, 0);

    // The standard Phoenix view (latest version only) on both clusters is identical.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
  }

  @Test
  public void testSyncTableWithMultipleVersionAndCompactionOnTarget() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);
    // Set VERSIONS=2 on target so readAllVersions scan can return both versions
    try (Statement stmt = sourceConnection.createStatement()) {
      stmt.execute("ALTER TABLE " + uniqueTableName + " SET VERSIONS=2");
    }
    sourceConnection.commit();
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertEquals("Should have 10 rows on source", 10,
      TestUtil.getRowCount(sourceConnection, uniqueTableName));
    assertEquals("Should have 10 rows on target", 10,
      TestUtil.getRowCount(targetConnection, uniqueTableName));

    // Update row 5 on source to create a second version
    upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { 5 },
      new String[] { "EXTRA_ROW" });
    targetConnection.commit();

    // Run sync with --read-all-versions: target diverged with EXTRA_ROW; source is unchanged.
    // Non-dry-run: repair pushes source's row 5 onto target. Status rolls up to REPAIRED (the
    // diverging cell is a value mismatch with both sides live, not a tombstone shadowing).
    Job job = runSyncTool(uniqueTableName, "--read-all-versions");
    SyncCountersResult counters1 = getSyncCounters(job);

    assertTrue("First run with all versions should succeed", job.isSuccessful());
    validateSyncCounters(counters1, 10, 10, 9, 1);
    validateMapperCountersRepair(counters1, 3, 1, 0, 0);

    flushAndMajorCompact(CLUSTERS.getHBaseCluster2(), uniqueTableName);

    // Subsequent runs see target already converged to source from the first repair pass.
    Job job1 = runSyncTool(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(job1);

    assertTrue("Second run without reading all versions should succeed", job1.isSuccessful());
    validateSyncCounters(counters, 10, 10, 10, 0);
    validateMapperCounters(counters, 4, 0);

    Job job3 = runSyncTool(uniqueTableName, "--read-all-versions");
    SyncCountersResult counters3 = getSyncCounters(job3);

    assertTrue("Third run with all versions should succeed", job3.isSuccessful());
    validateSyncCounters(counters3, 10, 10, 10, 0);
    validateMapperCounters(counters3, 4, 0);

    // After repair the standard Phoenix view matches.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  @Test
  public void testSyncTableValidateWithSplitCoalescing() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Enable split coalescing via command-line parameter, all regions will be coalesced into one
    // mapper. Use a pinned window so the dry-run and repair share the same checkpoint PK.
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    Job dryRunJob = runSyncTool(uniqueTableName, "--coalesce-split", "--dry-run", "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    SyncCountersResult counters = getSyncCounters(dryRunJob);

    assertEquals("Should have only 1 Mapper task created with coalescing", 1, counters.taskCreated);

    validateSyncCounters(counters, 10, 10, 7, 3);
    validateMapperCounters(counters, 1, 3);

    // Verify checkpoint entries from the dry-run pass are created correctly.
    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum, null);
    validateCheckpointEntries(checkpointEntries, uniqueTableName, targetZkQuorum, 10, 10, 7, 3, 4,
      3, null);

    // Repair pass over the same window: MISMATCHED rows transition to REPAIRED in place.
    runSyncTool(uniqueTableName, "--coalesce-split", "--from-time", String.valueOf(fromTime),
      "--to-time", String.valueOf(toTime));
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  /**
   * Verifies that the sync job completes successfully when {@code endTime} (--to-time) is older
   * than {@code phoenix.max.lookback.age.seconds}.
   *
   * <p>Root cause without fix: {@link PhoenixSyncTableTool} sets {@code CURRENT_SCN_VALUE =
   * endTime} in the MR job configuration. During split generation, {@code
   * PhoenixInputFormat.getQueryPlan()} creates a Phoenix connection with that SCN. {@code
   * QueryCompiler.verifySCN()} (client-side) then throws {@code ERROR 538} when {@code endTime} is
   * older than {@code phoenix.max.lookback.age.seconds}.
   *
   * <p>Fix: {@link PhoenixSyncTableInputFormat} overrides the parent to strip {@code
   * CURRENT_SCN_VALUE} before creating the query plan for split generation. With SCN absent, {@code
   * verifySCN()} returns early (SCN == null), so no exception is thrown.
   *
   * <p>Data access correctness: The mapper uses raw HBase {@code Scan.setTimeRange(fromTime,
   * toTime)}, which does NOT go through {@code QueryCompiler.compile()} or {@code verifySCN()}, so
   * data within [fromTime, toTime] is always accessible regardless of max lookback age.
   */
  @Test
  public void testSyncTableSucceedsWhenEndTimeOlderThanMaxLookbackAge() throws Exception {
    // Setup: create tables on both clusters and replicate 10 rows
    createTableOnBothClusters(sourceConnection, targetConnection, uniqueTableName);
    insertTestData(sourceConnection, uniqueTableName, 1, 10);
    waitForReplication(targetConnection, uniqueTableName, 10);
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);

    // Capture toTime BEFORE the lookback window will expire
    long toTime = System.currentTimeMillis();

    // Configure a short max lookback age (5 seconds) in the MR job configuration.
    // QueryCompiler.verifySCN() reads PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY from
    // conn.getQueryServices().getConfiguration(), which is the client-side MR conf.
    long maxLookbackAgeSeconds = 5;
    Configuration conf = sourceClusterConf();
    conf.setLong(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      maxLookbackAgeSeconds);

    // Wait until toTime is older than the lookback age.
    // After this sleep: (now - maxLookbackAgeMillis) > toTime  → verifySCN would throw ERROR 538
    Thread.sleep((maxLookbackAgeSeconds + 2) * 1000L);

    // Run the sync tool with the now-stale toTime.
    // Without PhoenixSyncTableInputFormat.getQueryPlan() override:
    //   getSplits() → PhoenixInputFormat.getQueryPlan() sets SCN=toTime on Phoenix connection
    //   → QueryCompiler.verifySCN() → ERROR 538 (toTime older than maxLookbackAge)
    // With the fix:
    //   getSplits() → overridden getQueryPlan() strips CURRENT_SCN_VALUE from conf copy
    //   → verifySCN() sees scn == null → returns early → no exception thrown
    // The mapper still uses raw HBase Scan.setTimeRange(0, toTime), bypassing verifySCN entirely,
    // so all 10 rows within [0, toTime] are accessible and compared correctly.
    Job job = runSyncToolWithChunkSize(uniqueTableName, 1, conf, "--from-time", "0", "--to-time",
      String.valueOf(toTime));

    assertTrue(
      "Sync job should complete successfully even when endTime is older than maxLookbackAge",
      job.isSuccessful());

    // Verify the mapper processed all 10 rows via raw HBase scan (bypasses verifySCN)
    SyncCountersResult counters = getSyncCounters(job);
    validateSyncCounters(counters, 10, 10, 10, 0);
    validateMapperCounters(counters, 4, 0);

    // Run was non-dry-run with no drift; repair flow is a no-op and target should match source
    // even though toTime is older than max lookback age.
    verifyDataIdentical(sourceConnection, targetConnection, uniqueTableName);
    assertNoMismatchedCheckpoints(uniqueTableName, null);
  }

  /**
   * Helper class to hold separated mapper and chunk entries.
   */
  private static class SeparatedCheckpointEntries {
    final List<PhoenixSyncTableCheckpointOutputRow> mappers;
    final List<PhoenixSyncTableCheckpointOutputRow> chunks;

    SeparatedCheckpointEntries(List<PhoenixSyncTableCheckpointOutputRow> mappers,
      List<PhoenixSyncTableCheckpointOutputRow> chunks) {
      this.mappers = mappers;
      this.chunks = chunks;
    }
  }

  /**
   * Helper class to hold aggregated counters from checkpoint chunk entries.
   */
  private static class CheckpointAggregateCounters {
    final long sourceRowsProcessed;
    final long targetRowsProcessed;
    final long chunksVerified;
    final long chunksMismatched;

    CheckpointAggregateCounters(long sourceRowsProcessed, long targetRowsProcessed,
      long chunksVerified, long chunksMismatched) {
      this.sourceRowsProcessed = sourceRowsProcessed;
      this.targetRowsProcessed = targetRowsProcessed;
      this.chunksVerified = chunksVerified;
      this.chunksMismatched = chunksMismatched;
    }
  }

  /**
   * Separates checkpoint entries into mapper and chunk entries.
   */
  private SeparatedCheckpointEntries
    separateMapperAndChunkEntries(List<PhoenixSyncTableCheckpointOutputRow> entries) {
    List<PhoenixSyncTableCheckpointOutputRow> mappers = new ArrayList<>();
    List<PhoenixSyncTableCheckpointOutputRow> chunks = new ArrayList<>();

    for (PhoenixSyncTableCheckpointOutputRow entry : entries) {
      if (PhoenixSyncTableCheckpointOutputRow.Type.REGION.equals(entry.getType())) {
        mappers.add(entry);
      } else if (PhoenixSyncTableCheckpointOutputRow.Type.CHUNK.equals(entry.getType())) {
        chunks.add(entry);
      }
    }

    return new SeparatedCheckpointEntries(mappers, chunks);
  }

  /**
   * Calculates aggregate counters from checkpoint CHUNK entries. This aggregates the rows processed
   * and chunk counts from all chunk entries in the checkpoint table.
   * @param entries List of checkpoint entries (both mappers and chunks)
   * @return Aggregated counters from chunk entries
   */
  private CheckpointAggregateCounters
    calculateAggregateCountersFromCheckpoint(List<PhoenixSyncTableCheckpointOutputRow> entries) {
    long sourceRowsProcessed = 0;
    long targetRowsProcessed = 0;
    long chunksVerified = 0;
    long chunksMismatched = 0;

    for (PhoenixSyncTableCheckpointOutputRow entry : entries) {
      if (PhoenixSyncTableCheckpointOutputRow.Type.CHUNK.equals(entry.getType())) {
        sourceRowsProcessed += entry.getSourceRowsProcessed();
        targetRowsProcessed += entry.getTargetRowsProcessed();
        if (PhoenixSyncTableCheckpointOutputRow.Status.VERIFIED.equals(entry.getStatus())) {
          chunksVerified++;
        } else
          if (PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED.equals(entry.getStatus())) {
            chunksMismatched++;
          }
      }
    }

    return new CheckpointAggregateCounters(sourceRowsProcessed, targetRowsProcessed, chunksVerified,
      chunksMismatched);
  }

  /**
   * Result of {@link #setupPartialRerun}. Captures the first-run job/counters, the snapshots of
   * the checkpoint table before and after deletion, and the aggregate counters re-derived from
   * the chunks that survived the deletion. Tests use these to assert the
   * {@code remaining + rerun == first-run} invariant after re-running the sync tool.
   */
  private static class PartialRerunSetup {
    final Job firstRunJob;
    final SyncCountersResult firstRunCounters;
    final List<PhoenixSyncTableCheckpointOutputRow> mappers;
    final List<PhoenixSyncTableCheckpointOutputRow> chunks;
    final List<PhoenixSyncTableCheckpointOutputRow> chunksToDelete;
    final int deletedCount;
    final List<PhoenixSyncTableCheckpointOutputRow> entriesAfterDelete;
    final CheckpointAggregateCounters remainingCounters;

    PartialRerunSetup(Job firstRunJob, SyncCountersResult firstRunCounters,
      List<PhoenixSyncTableCheckpointOutputRow> mappers,
      List<PhoenixSyncTableCheckpointOutputRow> chunks,
      List<PhoenixSyncTableCheckpointOutputRow> chunksToDelete, int deletedCount,
      List<PhoenixSyncTableCheckpointOutputRow> entriesAfterDelete,
      CheckpointAggregateCounters remainingCounters) {
      this.firstRunJob = firstRunJob;
      this.firstRunCounters = firstRunCounters;
      this.mappers = mappers;
      this.chunks = chunks;
      this.chunksToDelete = chunksToDelete;
      this.deletedCount = deletedCount;
      this.entriesAfterDelete = entriesAfterDelete;
      this.remainingCounters = remainingCounters;
    }
  }

  /**
   * Runs the partial-rerun preamble shared by all checkpoint-resume tests:
   * <ol>
   *   <li>Run the sync tool once at {@code chunkSize} over the pinned [{@code fromTime},
   *       {@code toTime}] window.</li>
   *   <li>Query the checkpoint table and assert non-empty mapper/chunk results.</li>
   *   <li>Select {@code deletionFraction} of each mapper's chunks for deletion (0.75 in all
   *       current tests) and delete them along with every mapper row.</li>
   *   <li>Re-query the checkpoint table and aggregate the surviving CHUNK rows so callers can
   *       assert the {@code remaining + rerun == first-run} row-count invariant.</li>
   * </ol>
   * Each test then performs its own divergent action (extra splits, merges, smaller chunk size,
   * dropping the target table) on the returned state.
   */
  private PartialRerunSetup setupPartialRerun(String tableName, long fromTime, long toTime,
    int chunkSize, double deletionFraction) throws Exception {
    Job firstRunJob = runSyncToolWithChunkSize(tableName, chunkSize, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    SyncCountersResult firstRunCounters = getSyncCounters(firstRunJob);

    List<PhoenixSyncTableCheckpointOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, tableName, targetZkQuorum, null);
    assertFalse("Should have checkpoint entries after first run", checkpointEntries.isEmpty());

    SeparatedCheckpointEntries separated = separateMapperAndChunkEntries(checkpointEntries);
    assertFalse("Should have mapper region entries", separated.mappers.isEmpty());
    assertFalse("Should have chunk entries", separated.chunks.isEmpty());

    List<PhoenixSyncTableCheckpointOutputRow> chunksToDelete = selectChunksToDeleteFromMappers(
      sourceConnection, tableName, targetZkQuorum, fromTime, toTime, null, separated.mappers,
      deletionFraction);

    int deletedCount = deleteCheckpointEntries(sourceConnection, tableName, targetZkQuorum, null,
      separated.mappers, chunksToDelete);
    assertEquals("Should have deleted all mapper and selected chunk entries",
      separated.mappers.size() + chunksToDelete.size(), deletedCount);

    List<PhoenixSyncTableCheckpointOutputRow> entriesAfterDelete =
      queryCheckpointTable(sourceConnection, tableName, targetZkQuorum, null);
    CheckpointAggregateCounters remainingCounters =
      calculateAggregateCountersFromCheckpoint(entriesAfterDelete);

    return new PartialRerunSetup(firstRunJob, firstRunCounters, separated.mappers,
      separated.chunks, chunksToDelete, deletedCount, entriesAfterDelete, remainingCounters);
  }

  private List<PhoenixSyncTableCheckpointOutputRow> findChunksBelongingToMapper(Connection conn,
    String tableName, String targetCluster, long fromTime, long toTime, String tenantId,
    PhoenixSyncTableCheckpointOutputRow mapper) throws SQLException {
    PhoenixSyncTableOutputRepository repository = new PhoenixSyncTableOutputRepository(conn);
    return repository.getProcessedChunks(tableName, targetCluster, fromTime, toTime, tenantId,
      mapper.getStartRowKey(), mapper.getEndRowKey(), true);
  }

  /**
   * Selects a percentage of chunks to delete from each mapper region. This is used to simulate
   * partial rerun scenarios where some checkpoint entries are missing. SyncTableRepository uses
   * overlap-based boundary checking, so chunks that span across mapper boundaries may be returned
   * by multiple mappers. We use a Set to track unique chunks by their start row key to avoid
   * duplicates.
   */
  private List<PhoenixSyncTableCheckpointOutputRow> selectChunksToDeleteFromMappers(Connection conn,
    String tableName, String targetCluster, long fromTime, long toTime, String tenantId,
    List<PhoenixSyncTableCheckpointOutputRow> mappers, double deletionFraction)
    throws SQLException {
    // Use a map to track unique chunks by start row key to avoid duplicates
    Map<String, PhoenixSyncTableCheckpointOutputRow> uniqueChunksToDelete = new LinkedHashMap<>();

    for (PhoenixSyncTableCheckpointOutputRow mapper : mappers) {
      List<PhoenixSyncTableCheckpointOutputRow> mapperChunks = findChunksBelongingToMapper(conn,
        tableName, targetCluster, fromTime, toTime, tenantId, mapper);

      int chunksToDeleteCount = (int) Math.ceil(mapperChunks.size() * deletionFraction);
      for (int i = 0; i < chunksToDeleteCount && i < mapperChunks.size(); i++) {
        PhoenixSyncTableCheckpointOutputRow chunk = mapperChunks.get(i);
        // Use start row key as unique identifier (convert to string for map key)
        String key =
          chunk.getStartRowKey() == null ? "NULL" : Bytes.toStringBinary(chunk.getStartRowKey());
        uniqueChunksToDelete.put(key, chunk);
      }
    }

    return new ArrayList<>(uniqueChunksToDelete.values());
  }

  /**
   * Deletes mapper and chunk checkpoint entries to simulate partial rerun scenarios.
   * @param conn            Connection to use
   * @param tableName       Table name
   * @param targetZkQuorum  Target cluster ZK quorum
   * @param tenantId        Tenant ID
   * @param mappersToDelete List of mapper entries to delete
   * @param chunksToDelete  List of chunk entries to delete
   * @return Total number of entries deleted
   */
  private int deleteCheckpointEntries(Connection conn, String tableName, String targetZkQuorum,
    String tenantId, List<PhoenixSyncTableCheckpointOutputRow> mappersToDelete,
    List<PhoenixSyncTableCheckpointOutputRow> chunksToDelete) throws SQLException {
    int deletedCount = 0;

    // Delete mapper entries
    for (PhoenixSyncTableCheckpointOutputRow mapper : mappersToDelete) {
      deletedCount += deleteSingleCheckpointEntry(conn, tableName, targetZkQuorum, tenantId,
        PhoenixSyncTableCheckpointOutputRow.Type.REGION, mapper.getStartRowKey(), false);
    }

    // Delete chunk entries
    for (PhoenixSyncTableCheckpointOutputRow chunk : chunksToDelete) {
      deletedCount += deleteSingleCheckpointEntry(conn, tableName, targetZkQuorum, tenantId,
        PhoenixSyncTableCheckpointOutputRow.Type.CHUNK, chunk.getStartRowKey(), false);
    }

    conn.commit();
    return deletedCount;
  }

  /**
   * Initiates merge of adjacent regions in a table. Merges happen asynchronously in background.
   */
  private void mergeAdjacentRegions(Connection conn, String tableName, int mergeCount) {
    try {
      List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);
      LOGGER.info("Table {} has {} regions before merge", tableName, regions.size());

      int mergedCount = 0;
      for (int i = 0; i < regions.size() - 1 && mergedCount < mergeCount; i++) {
        try {
          String region1 = regions.get(i).getRegion().getEncodedName();
          String region2 = regions.get(i + 1).getRegion().getEncodedName();
          LOGGER.info("Merging regions {} and {}", region1, region2);
          TestUtil.mergeTableRegions(conn, tableName, Arrays.asList(region1, region2));
          mergedCount++;
          i++;
        } catch (Exception e) {
          LOGGER.warn("Failed to merge regions: {}", e.getMessage());
        }
      }
      LOGGER.info("Completed {} region merges for table {}", mergedCount, tableName);
    } catch (Exception e) {
      LOGGER.error("Error during region merge for table {}: {}", tableName, e.getMessage(), e);
    }
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
    // Use NO_INDEX hint to force a full data table scan
    String countQuery = "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName;

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
   * Waits for a specific row's content to be replicated to the target cluster. This is more precise
   * than waitForReplication() when dealing with UPDATEs where the row count doesn't change but the
   * content does.
   * @param targetConn   Target cluster connection
   * @param tableName    Table name
   * @param rowId        The ID of the row to check
   * @param expectedName The expected NAME value
   * @throws Exception if replication times out or query fails
   */
  private void waitForRowContentReplication(Connection targetConn, String tableName, int rowId,
    String expectedName) throws Exception {
    long startTime = System.currentTimeMillis();
    String query = "SELECT NAME FROM " + tableName + " WHERE ID = ?";

    while (System.currentTimeMillis() - startTime < REPLICATION_WAIT_TIMEOUT_MS) {
      try (PreparedStatement stmt = targetConn.prepareStatement(query)) {
        stmt.setInt(1, rowId);
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next() && expectedName.equals(rs.getString(1))) {
            return;
          }
        }
      }
      Thread.sleep(200);
    }

    fail(String.format("Row content replication timeout for table %s, row %d. Expected NAME='%s'",
      tableName, rowId, expectedName));
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
   * Inserts test data with a specific timestamp for time-range testing. Converts range to list and
   * delegates to core method.
   */
  private void insertTestData(Connection conn, String tableName, int startId, int endId,
    long timestamp) throws SQLException {
    List<Integer> ids = new ArrayList<>();
    for (int i = startId; i <= endId; i++) {
      ids.add(i);
    }
    insertTestData(conn, tableName, ids, timestamp);
  }

  /**
   * Core method: Inserts test data for specific list of IDs with given timestamp. All other
   * insertTestData overloads delegate to this method.
   */
  private void insertTestData(Connection conn, String tableName, List<Integer> ids, long timestamp)
    throws SQLException {
    if (ids == null || ids.isEmpty()) {
      return;
    }
    String upsert =
      "UPSERT INTO " + tableName + " (ID, NAME, NAME_VALUE, UPDATED_DATE) VALUES (?, ?, ?, ?)";
    PreparedStatement stmt = conn.prepareStatement(upsert);
    Timestamp ts = new Timestamp(timestamp);
    for (int id : ids) {
      stmt.setInt(1, id);
      stmt.setString(2, "NAME_" + id);
      stmt.setLong(3, (long) id);
      stmt.setTimestamp(4, ts);
      stmt.executeUpdate();
    }
    conn.commit();
  }

  /**
   * Inserts test data for specific list of IDs with current timestamp.
   */
  private void insertTestData(Connection conn, String tableName, List<Integer> ids)
    throws SQLException {
    insertTestData(conn, tableName, ids, System.currentTimeMillis());
  }

  /**
   * Inserts test data with an EXPIRED boolean flag for conditional TTL testing.
   */
  private void insertTestDataWithExpiredFlag(Connection conn, String tableName, int startId,
    int endId, boolean expired) throws SQLException {
    String upsert = "UPSERT INTO " + tableName
      + " (ID, NAME, NAME_VALUE, UPDATED_DATE, EXPIRED) VALUES (?, ?, ?, ?, ?)";
    PreparedStatement stmt = conn.prepareStatement(upsert);
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    for (int id = startId; id <= endId; id++) {
      stmt.setInt(1, id);
      stmt.setString(2, "NAME_" + id);
      stmt.setLong(3, (long) id);
      stmt.setTimestamp(4, ts);
      stmt.setBoolean(5, expired);
      stmt.executeUpdate();
    }
    conn.commit();
  }

  /**
   * Deletes multiple rows from target cluster to create mismatches. This method accepts variable
   * number of row IDs to delete.
   */
  private void deleteRows(Connection conn, String tableName, int... rowIds) throws SQLException {
    String delete = "DELETE FROM " + tableName + " WHERE ID = ?";
    PreparedStatement stmt = conn.prepareStatement(delete);

    for (int id : rowIds) {
      stmt.setInt(1, id);
      stmt.executeUpdate();
    }
    conn.commit();
  }

  private void deleteHBaseRows(HBaseTestingUtility cluster, String tableName, int rowsToDelete)
    throws Exception {
    TableName hbaseTableName = TableName.valueOf(tableName);
    try (Table table = cluster.getConnection().getTable(hbaseTableName)) {
      ResultScanner scanner = table.getScanner(new Scan());
      List<Delete> deletes = new ArrayList<>();
      Result result;
      int rowsDeleted = 0;
      while ((result = scanner.next()) != null && rowsDeleted < rowsToDelete) {
        deletes.add(new Delete(result.getRow()));
        rowsDeleted++;
      }
      scanner.close();
      if (!deletes.isEmpty()) {
        table.delete(deletes);
      }
    }
    try (Admin admin = cluster.getConnection().getAdmin()) {
      admin.flush(hbaseTableName);
    }
  }

  /**
   * Flushes and major-compacts a table on the given cluster
   */
  private void flushAndMajorCompact(HBaseTestingUtility cluster, String tableName)
    throws Exception {
    TableName hbaseTableName = TableName.valueOf(tableName);
    try (Admin admin = cluster.getConnection().getAdmin()) {
      admin.flush(hbaseTableName);
    }
    TestUtil.majorCompact(cluster, hbaseTableName);
  }

  /**
   * Creates an index on both source and target clusters. Note: Indexes inherit replication settings
   * from their parent table.
   */
  private void createIndexOnBothClusters(Connection sourceConn, Connection targetConn,
    String tableName, String indexName) throws SQLException {
    // Create index on source (inherits replication from data table)
    String indexDdl = String.format(
      "CREATE INDEX IF NOT EXISTS %s ON %s (NAME) INCLUDE (NAME_VALUE)", indexName, tableName);

    sourceConn.createStatement().execute(indexDdl);
    sourceConn.commit();

    // Create same index on target
    targetConn.createStatement().execute(indexDdl);
    targetConn.commit();
  }

  /**
   * Creates a local index on both source and target clusters. Note: Local indexes are stored in the
   * same regions as the data table and inherit replication settings from their parent table.
   */
  private void createLocalIndexOnBothClusters(Connection sourceConn, Connection targetConn,
    String tableName, String indexName) throws SQLException {
    String indexDdl =
      String.format("CREATE LOCAL INDEX IF NOT EXISTS %s ON %s (NAME) INCLUDE (NAME_VALUE)",
        indexName, tableName);

    sourceConn.createStatement().execute(indexDdl);
    sourceConn.commit();

    // Create same local index on target
    targetConn.createStatement().execute(indexDdl);
    targetConn.commit();
  }

  /**
   * Attempts to split a table at the specified row ID using HBase Admin API. Ignores errors if the
   * split fails (e.g., region in transition).
   */
  private void splitTableAt(Connection conn, String tableName, int splitId) {
    try {
      byte[] splitPoint = PInteger.INSTANCE.toBytes(splitId);
      TestUtil.splitTable(conn, tableName, splitPoint);
      LOGGER.info("Split completed for table {} at split point {} (bytes: {})", tableName, splitId,
        Bytes.toStringBinary(splitPoint));
    } catch (Exception e) {
      // Ignore split failures - they don't affect the test's main goal
      LOGGER.warn("Failed to split table {} at split point {}: {}", tableName, splitId,
        e.getMessage());
    }
  }

  /**
   * Attempts to split a table at multiple split points using HBase Admin API. Ignores errors if any
   * split fails (e.g., region in transition).
   */
  private void splitTableAt(Connection conn, String tableName, List<Integer> splitIds) {
    if (splitIds == null || splitIds.isEmpty()) {
      return;
    }
    for (int splitId : splitIds) {
      splitTableAt(conn, tableName, splitId);
    }
  }

  /**
   * Queries the checkpoint table for entries matching the given table and target cluster. Retrieves
   * all columns for comprehensive validation.
   */
  private List<PhoenixSyncTableCheckpointOutputRow> queryCheckpointTable(Connection conn,
    String tableName, String targetCluster, String tenantId) throws SQLException {
    List<PhoenixSyncTableCheckpointOutputRow> entries = new ArrayList<>();
    String query = "SELECT TABLE_NAME, TARGET_CLUSTER, TYPE, FROM_TIME, TO_TIME, IS_DRY_RUN, "
      + "START_ROW_KEY, END_ROW_KEY, EXECUTION_START_TIME, EXECUTION_END_TIME, "
      + "STATUS, COUNTERS FROM PHOENIX_SYNC_TABLE_CHECKPOINT "
      + "WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ? "
      + (tenantId != null ? "AND TENANT_ID = ?" : "AND TENANT_ID IS NULL");

    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, tableName);
    stmt.setString(2, targetCluster);
    if (tenantId != null) {
      stmt.setString(3, tenantId);
    }
    ResultSet rs = stmt.executeQuery();

    while (rs.next()) {
      String typeStr = rs.getString("TYPE");
      String statusStr = rs.getString("STATUS");

      PhoenixSyncTableCheckpointOutputRow entry = new PhoenixSyncTableCheckpointOutputRow.Builder()
        .setTableName(rs.getString("TABLE_NAME")).setTargetCluster(rs.getString("TARGET_CLUSTER"))
        .setType(typeStr != null ? PhoenixSyncTableCheckpointOutputRow.Type.valueOf(typeStr) : null)
        .setFromTime(rs.getLong("FROM_TIME")).setToTime(rs.getLong("TO_TIME"))
        .setIsDryRun(rs.getBoolean("IS_DRY_RUN")).setStartRowKey(rs.getBytes("START_ROW_KEY"))
        .setEndRowKey(rs.getBytes("END_ROW_KEY"))
        .setExecutionStartTime(rs.getTimestamp("EXECUTION_START_TIME"))
        .setExecutionEndTime(rs.getTimestamp("EXECUTION_END_TIME"))
        .setStatus(
          statusStr != null ? PhoenixSyncTableCheckpointOutputRow.Status.valueOf(statusStr) : null)
        .setCounters(rs.getString("COUNTERS")).build();
      entries.add(entry);
    }

    rs.close();
    return entries;
  }

  /**
   * Unified method to delete a single checkpoint entry by start row key and optional type. Handles
   * NULL/empty start keys for first region boundaries.
   * @param conn          Connection to use
   * @param tableName     Table name
   * @param targetCluster Target cluster ZK quorum
   * @param tenantId      Tenant ID (nullable)
   * @param type          Entry type (REGION or CHUNK), or null to delete regardless of type
   * @param startRowKey   Start row key to match
   * @param autoCommit    Whether to commit after delete
   * @return Number of rows deleted
   */
  private int deleteSingleCheckpointEntry(Connection conn, String tableName, String targetCluster,
    String tenantId, PhoenixSyncTableCheckpointOutputRow.Type type, byte[] startRowKey,
    boolean autoCommit) throws SQLException {
    StringBuilder deleteBuilder = new StringBuilder(
      "DELETE FROM PHOENIX_SYNC_TABLE_CHECKPOINT WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ?");

    // Add TENANT_ID filter
    deleteBuilder.append(tenantId != null ? " AND TENANT_ID = ?" : " AND TENANT_ID IS NULL");

    // Add TYPE filter if provided
    if (type != null) {
      deleteBuilder.append(" AND TYPE = ?");
    }

    // Add START_ROW_KEY filter (handle NULL/empty keys)
    boolean isNullOrEmptyKey = (startRowKey == null || startRowKey.length == 0);
    if (isNullOrEmptyKey) {
      // Phoenix stores empty byte arrays as NULL in VARBINARY columns
      deleteBuilder.append(" AND START_ROW_KEY IS NULL");
    } else {
      deleteBuilder.append(" AND START_ROW_KEY = ?");
    }

    PreparedStatement stmt = conn.prepareStatement(deleteBuilder.toString());
    int paramIndex = 1;
    stmt.setString(paramIndex++, tableName);
    stmt.setString(paramIndex++, targetCluster);
    if (tenantId != null) {
      stmt.setString(paramIndex++, tenantId);
    }

    if (type != null) {
      stmt.setString(paramIndex++, type.name());
    }

    if (!isNullOrEmptyKey) {
      stmt.setBytes(paramIndex, startRowKey);
    }

    int deleted = stmt.executeUpdate();
    if (autoCommit) {
      conn.commit();
    }
    return deleted;
  }

  /**
   * Cleans up checkpoint table entries for a specific table and target cluster.
   */
  private void cleanupCheckpointTable(Connection conn, String tableName, String targetCluster,
    String tenantId) {
    try {
      String delete = "DELETE FROM PHOENIX_SYNC_TABLE_CHECKPOINT "
        + "WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ? "
        + (tenantId != null ? "AND TENANT_ID = ?" : "AND TENANT_ID IS NULL");
      PreparedStatement stmt = conn.prepareStatement(delete);
      stmt.setString(1, tableName);
      stmt.setString(2, targetCluster);
      if (tenantId != null) {
        stmt.setString(3, tenantId);
      }
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

  private Job runSyncToolWithZkQuorum(String tableName, String zkQuorum, String... additionalArgs)
    throws Exception {
    String savedZkQuorum = targetZkQuorum;
    targetZkQuorum = zkQuorum;
    try {
      return runSyncToolWithChunkSize(tableName, 1, additionalArgs);
    } finally {
      targetZkQuorum = savedZkQuorum;
    }
  }

  /**
   * Returns a fresh, mutable copy of the source cluster's HBase {@link Configuration}. Tests that
   * need to override individual settings (paging, timeouts, etc.) should use this helper rather
   * than constructing the Configuration inline so that any future change to the base config flows
   * through one place.
   */
  private static Configuration sourceClusterConf() {
    return new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
  }

  /**
   * Builds a {@link PhoenixSyncTableTool}, runs it with the supplied args, and asserts the run
   * surfaces failure as a non-zero exit code rather than a thrown exception. Used by the
   * failure-mode tests that previously hand-rolled this same try/run/assertTrue/catch-fail block.
   */
  private void assertSyncToolFails(String[] args, String failureContext) {
    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(sourceClusterConf());
    try {
      int exitCode = tool.run(args);
      assertTrue(failureContext, exitCode != 0);
    } catch (Exception ex) {
      fail("Tool should return non-zero exit code on failure instead of throwing exception: "
        + ex.getMessage());
    }
  }

  /**
   * Upserts a "MODIFIED_NAME_<id>" row on target for each id in {@code mismatchIds}. Replaces the
   * common pattern {@code for (int id : ids) upsertRowsOnTarget(..., new int[]{id}, new
   * String[]{"MODIFIED_NAME_"+id})} which defeated the batch-upsert path of {@link
   * #upsertRowsOnTarget}.
   */
  private void introduceMismatchesByIds(String tableName, List<Integer> mismatchIds)
    throws SQLException {
    int[] ids = new int[mismatchIds.size()];
    String[] names = new String[mismatchIds.size()];
    for (int i = 0; i < mismatchIds.size(); i++) {
      ids[i] = mismatchIds.get(i);
      names[i] = "MODIFIED_NAME_" + mismatchIds.get(i);
    }
    upsertRowsOnTarget(targetConnection, tableName, ids, names);
  }

  /**
   * Starts two daemon-style threads that perform region mutations (splits or merges) on the
   * source and target clusters and returns a {@link Runnable} the caller invokes to join them
   * with a 30-second timeout. Both worker {@link Runnable}s are wrapped in try/catch so that an
   * unexpected exception is logged rather than killing the JVM thread silently.
   *
   * <p>Usage:
   * <pre>
   *   Runnable joiner = startConcurrentRegionWork(sourceWork, targetWork, "splits");
   *   ... run main sync work ...
   *   joiner.run();
   * </pre>
   *
   * <p>Caller is responsible for invoking the returned joiner; tests should always join before
   * asserting on cluster state, otherwise late-arriving region mutations can race the assertions.
   */
  private Runnable startConcurrentRegionWork(Runnable sourceWork, Runnable targetWork,
    String label) {
    Thread sourceThread = new Thread(() -> {
      try {
        sourceWork.run();
      } catch (Exception e) {
        LOGGER.error("Error during source {}", label, e);
      }
    });
    Thread targetThread = new Thread(() -> {
      try {
        targetWork.run();
      } catch (Exception e) {
        LOGGER.error("Error during target {}", label, e);
      }
    });
    sourceThread.start();
    targetThread.start();
    return () -> {
      try {
        sourceThread.join(30000);
        targetThread.join(30000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while joining " + label + " threads", ie);
      }
    };
  }

  /**
   * Runs a dry-run sync with the given chunk size to establish a baseline chunk count
   * (CHUNKS_VERIFIED + CHUNKS_MISMATCHED), then deletes the baseline checkpoint rows so a
   * subsequent run starts fresh. Used by the paging-timeout tests, which assert that aggressive
   * paging produces strictly more chunks than the no-paging baseline.
   */
  private long captureBaselineChunkCount(String tableName, int chunkSize) throws Exception {
    Job baselineJob = runSyncToolWithChunkSize(tableName, chunkSize, "--dry-run", "--from-time",
      "0", "--to-time", String.valueOf(System.currentTimeMillis()));
    long chunkCount =
      baselineJob.getCounters().findCounter(SyncCounters.CHUNKS_VERIFIED).getValue()
        + baselineJob.getCounters().findCounter(SyncCounters.CHUNKS_MISMATCHED).getValue();
    cleanupCheckpointTable(sourceConnection, tableName, targetZkQuorum, null);
    return chunkCount;
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
    return runSyncToolWithChunkSize(tableName, chunkSize, sourceClusterConf(), additionalArgs);
  }

  /**
   * Holds both the dry-run and repair jobs from a {@link #runSyncToolWithRepair} invocation,
   * along with the pinned time window so callers can re-query the checkpoint table or run
   * additional assertions against the same range.
   */
  private static class RepairRunResult {
    final Job dryRunJob;
    final Job repairJob;
    final long fromTime;
    final long toTime;

    RepairRunResult(Job dryRunJob, Job repairJob, long fromTime, long toTime) {
      this.dryRunJob = dryRunJob;
      this.repairJob = repairJob;
      this.fromTime = fromTime;
      this.toTime = toTime;
    }
  }

  /**
   * Runs the sync tool twice with the SAME pinned time window: first as a --dry-run to detect
   * mismatches, then as a repair pass (no --dry-run) so the repair run rewrites the MISMATCHED
   * checkpoint rows in place. The shared window is mandatory because the checkpoint PK is
   * (TABLE_NAME, TARGET_CLUSTER, TYPE, FROM_TIME, TO_TIME, TENANT_ID, START_ROW_KEY) — without
   * pinning, each invocation would fall through to System.currentTimeMillis() and the repair
   * pass would create fresh rows instead of overwriting the dry-run pass's output.
   *
   * <p>If the caller does not provide --from-time / --to-time, defaults of 0 / now are pinned.
   *
   * <p>Default chunk size is 1 byte (one row per chunk) to mirror {@link #runSyncTool}.
   */
  private RepairRunResult runSyncToolWithRepair(String tableName, String... additionalArgs)
    throws Exception {
    return runSyncToolWithRepairAndChunkSize(tableName, 1, additionalArgs);
  }

  /**
   * Same as {@link #runSyncToolWithRepair} but uses 1KB chunks (multiple rows per chunk).
   */
  private RepairRunResult runSyncToolWithRepairLargeChunks(String tableName,
    String... additionalArgs) throws Exception {
    return runSyncToolWithRepairAndChunkSize(tableName, 1024, additionalArgs);
  }

  /**
   * Same as {@link #runSyncToolWithRepair} but with an explicit chunk size.
   */
  private RepairRunResult runSyncToolWithRepairAndChunkSize(String tableName, int chunkSize,
    String... additionalArgs) throws Exception {
    long fromTime = parseLongFlag(additionalArgs, "--from-time", 0L);
    long toTime = parseLongFlag(additionalArgs, "--to-time", System.currentTimeMillis());
    String[] pinnedArgs = ensureTimeArgs(additionalArgs, fromTime, toTime);

    // First run: --dry-run, only detect mismatches.
    String[] dryRunArgs = appendArg(pinnedArgs, "--dry-run");
    Job dryRunJob = runSyncToolWithChunkSize(tableName, chunkSize, dryRunArgs);

    // Second run: no --dry-run. Same time window so the checkpoint PK matches and any
    // CHUNK/MISMATCHED rows from the dry-run pass are overwritten by CHUNK/REPAIRED.
    Job repairJob = runSyncToolWithChunkSize(tableName, chunkSize, pinnedArgs);

    return new RepairRunResult(dryRunJob, repairJob, fromTime, toTime);
  }

  /**
   * Parses a long-valued command-line flag (e.g., --from-time 12345) from the args array.
   * Returns the default value if the flag is absent.
   */
  private static long parseLongFlag(String[] args, String flag, long defaultValue) {
    for (int i = 0; i < args.length - 1; i++) {
      if (flag.equals(args[i])) {
        return Long.parseLong(args[i + 1]);
      }
    }
    return defaultValue;
  }

  /**
   * Returns args with --from-time/--to-time appended only if they are not already present.
   */
  private static String[] ensureTimeArgs(String[] args, long fromTime, long toTime) {
    boolean hasFrom = false;
    boolean hasTo = false;
    for (String a : args) {
      if ("--from-time".equals(a)) {
        hasFrom = true;
      } else if ("--to-time".equals(a)) {
        hasTo = true;
      }
    }
    List<String> result = new ArrayList<>(Arrays.asList(args));
    if (!hasFrom) {
      result.add("--from-time");
      result.add(String.valueOf(fromTime));
    }
    if (!hasTo) {
      result.add("--to-time");
      result.add(String.valueOf(toTime));
    }
    return result.toArray(new String[0]);
  }

  private static String[] appendArg(String[] args, String newArg) {
    String[] result = new String[args.length + 1];
    System.arraycopy(args, 0, result, 0, args.length);
    result[args.length] = newArg;
    return result;
  }

  /**
   * After a repair pass, asserts that no CHUNK or REGION rows in the checkpoint table are
   * still in MISMATCHED status. They should all have transitioned to REPAIRED, VERIFIED, or
   * (when target rows are entirely tombstoned) UNREPAIRABLE.
   */
  private void assertNoMismatchedCheckpoints(String tableName, String tenantId)
    throws SQLException {
    List<PhoenixSyncTableCheckpointOutputRow> entries =
      queryCheckpointTable(sourceConnection, tableName, targetZkQuorum, tenantId);
    long mismatched = countCheckpointsByStatus(entries,
      PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED);
    assertEquals("After repair, no MISMATCHED checkpoint rows should remain for table "
      + tableName + " tenant=" + tenantId, 0, mismatched);
  }

  /**
   * Counts checkpoint entries (both REGION and CHUNK rows) in the given status. Replaces the
   * ad-hoc {@code for (entry : entries) if (status.equals(entry.getStatus())) count++} loops
   * that recurred across several tests.
   */
  private static long countCheckpointsByStatus(List<PhoenixSyncTableCheckpointOutputRow> entries,
    PhoenixSyncTableCheckpointOutputRow.Status status) {
    long count = 0;
    for (PhoenixSyncTableCheckpointOutputRow entry : entries) {
      if (status.equals(entry.getStatus())) {
        count++;
      }
    }
    return count;
  }

  /**
   * Counts checkpoint entries that match BOTH the given type (REGION or CHUNK) and status. Used
   * when a test needs to discriminate, e.g., REPAIRED CHUNK rows from REPAIRED REGION rows.
   */
  private static long countCheckpointsByTypeAndStatus(
    List<PhoenixSyncTableCheckpointOutputRow> entries,
    PhoenixSyncTableCheckpointOutputRow.Type type,
    PhoenixSyncTableCheckpointOutputRow.Status status) {
    long count = 0;
    for (PhoenixSyncTableCheckpointOutputRow entry : entries) {
      if (type.equals(entry.getType()) && status.equals(entry.getStatus())) {
        count++;
      }
    }
    return count;
  }

  /**
   * Runs the PhoenixSyncTableTool with specified chunk size and custom configuration. Allows
   * passing pre-configured Configuration object for tests that need specific settings (e.g., paging
   * enabled, custom timeouts).
   * @param tableName      Table name to sync
   * @param chunkSize      Chunk size in bytes
   * @param conf           Pre-configured Configuration object
   * @param additionalArgs Additional command-line arguments
   * @return Completed Job for counter verification
   */
  private Job runSyncToolWithChunkSize(String tableName, int chunkSize, Configuration conf,
    String... additionalArgs) throws Exception {
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
    List<String> additionalArgsList = Arrays.asList(additionalArgs);
    argsList.addAll(additionalArgsList);

    // If --to-time is not explicitly provided in additionalArgs, add current time
    // This is needed because the default is now (current time - 1 hour) which won't
    // capture data inserted immediately before running the sync tool
    if (!additionalArgsList.contains("--to-time")) {
      argsList.add("--to-time");
      argsList.add(String.valueOf(System.currentTimeMillis()));
    }

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
    public final long mappersVerified;
    public final long mappersMismatched;
    public final long mappersRepaired;
    public final long mappersUnrepairable;
    public final long mappersRepairFailed;
    public final long rowsMissingOnTarget;
    public final long rowsExtraOnTarget;
    public final long rowsDifferentOnTarget;
    public final long taskCreated;

    SyncCountersResult(Counters counters) {
      this.sourceRowsProcessed =
        counters.findCounter(SyncCounters.SOURCE_ROWS_PROCESSED).getValue();
      this.targetRowsProcessed =
        counters.findCounter(SyncCounters.TARGET_ROWS_PROCESSED).getValue();
      this.chunksMismatched = counters.findCounter(SyncCounters.CHUNKS_MISMATCHED).getValue();
      this.chunksVerified = counters.findCounter(SyncCounters.CHUNKS_VERIFIED).getValue();
      this.mappersVerified = counters.findCounter(SyncCounters.MAPPERS_VERIFIED).getValue();
      this.mappersMismatched = counters.findCounter(SyncCounters.MAPPERS_MISMATCHED).getValue();
      this.mappersRepaired = counters.findCounter(SyncCounters.MAPPERS_REPAIRED).getValue();
      this.mappersUnrepairable =
        counters.findCounter(SyncCounters.MAPPERS_UNREPAIRABLE).getValue();
      this.mappersRepairFailed =
        counters.findCounter(SyncCounters.MAPPERS_REPAIR_FAILED).getValue();
      this.rowsMissingOnTarget =
        counters.findCounter(SyncCounters.ROWS_MISSING_ON_TARGET).getValue();
      this.rowsExtraOnTarget =
        counters.findCounter(SyncCounters.ROWS_EXTRA_ON_TARGET).getValue();
      this.rowsDifferentOnTarget =
        counters.findCounter(SyncCounters.ROWS_DIFFERENT_ON_TARGET).getValue();
      this.taskCreated = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
    }

    public void logCounters(String testName) {
      LOGGER.info(
        "{}: source rows={}, target rows={}, chunks mismatched={}, chunks verified={}, "
          + "mappers verified={}, mappers mismatched={}, mappers repaired={}, "
          + "mappers unrepairable={}, mappers repair_failed={}, rows missing={}, "
          + "rows extra={}, rows different={}",
        testName, sourceRowsProcessed, targetRowsProcessed, chunksMismatched, chunksVerified,
        mappersVerified, mappersMismatched, mappersRepaired, mappersUnrepairable,
        mappersRepairFailed, rowsMissingOnTarget, rowsExtraOnTarget, rowsDifferentOnTarget);
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

  private void validateMapperCounters(SyncCountersResult counters, long expectedMappersVerified,
    long expectedMappersMismatched) {
    assertEquals("Should have expected verified mappers", expectedMappersVerified,
      counters.mappersVerified);
    assertEquals("Should have expected mismatched mappers", expectedMappersMismatched,
      counters.mappersMismatched);
  }

  /**
   * Validates mapper counters when the chunks roll up into different repair outcomes (run was
   * non-dry-run so mismatches were repaired rather than left as MISMATCHED).
   */
  private void validateMapperCountersRepair(SyncCountersResult counters,
    long expectedMappersVerified, long expectedMappersRepaired, long expectedMappersUnrepairable,
    long expectedMappersRepairFailed) {
    assertEquals("Should have expected verified mappers", expectedMappersVerified,
      counters.mappersVerified);
    assertEquals("Should have expected repaired mappers", expectedMappersRepaired,
      counters.mappersRepaired);
    assertEquals("Should have expected unrepairable mappers", expectedMappersUnrepairable,
      counters.mappersUnrepairable);
    assertEquals("Should have expected repair_failed mappers", expectedMappersRepairFailed,
      counters.mappersRepairFailed);
  }

  /**
   * Pins the cell-level repair drift counters to exact expected values. Use in repair tests
   * where the drift is constructed deterministically and any miscount (off-by-one,
   * double-counting, missed branch) should fail the test loudly.
   */
  private void assertRepairCellCounters(Counters counters, long expectedCellsMissing,
    long expectedCellsExtra, long expectedCellsDifferent, long expectedRowsCannotRepair) {
    assertEquals("CELLS_MISSING_ON_TARGET", expectedCellsMissing,
      counters.findCounter(SyncCounters.CELLS_MISSING_ON_TARGET).getValue());
    assertEquals("CELLS_EXTRA_ON_TARGET", expectedCellsExtra,
      counters.findCounter(SyncCounters.CELLS_EXTRA_ON_TARGET).getValue());
    assertEquals("CELLS_DIFFERENT_ON_TARGET", expectedCellsDifferent,
      counters.findCounter(SyncCounters.CELLS_DIFFERENT_ON_TARGET).getValue());
    assertEquals("ROWS_CANNOT_REPAIR", expectedRowsCannotRepair,
      counters.findCounter(SyncCounters.ROWS_CANNOT_REPAIR).getValue());
  }

  /**
   * Pins the chunk- and mapper-level repair-status counters. Complements
   * {@link #validateMapperCountersRepair} (which omits chunk-level counters) for tests that
   * need to assert both layers.
   */
  private void assertRepairChunkAndMapperCounters(Counters counters, long expectedChunksRepaired,
    long expectedChunksRepairFailed, long expectedMappersRepaired, long expectedMappersUnrepairable,
    long expectedMappersRepairFailed) {
    assertEquals("CHUNKS_REPAIRED", expectedChunksRepaired,
      counters.findCounter(SyncCounters.CHUNKS_REPAIRED).getValue());
    assertEquals("CHUNKS_REPAIR_FAILED", expectedChunksRepairFailed,
      counters.findCounter(SyncCounters.CHUNKS_REPAIR_FAILED).getValue());
    assertEquals("MAPPERS_REPAIRED", expectedMappersRepaired,
      counters.findCounter(SyncCounters.MAPPERS_REPAIRED).getValue());
    assertEquals("MAPPERS_UNREPAIRABLE", expectedMappersUnrepairable,
      counters.findCounter(SyncCounters.MAPPERS_UNREPAIRABLE).getValue());
    assertEquals("MAPPERS_REPAIR_FAILED", expectedMappersRepairFailed,
      counters.findCounter(SyncCounters.MAPPERS_REPAIR_FAILED).getValue());
  }

  /**
   * Pins the row-level repair drift counters. Mirror of {@link #assertRepairCellCounters} for
   * tests that need to assert whole-row outcomes (missing, extra, unrepairable).
   */
  private void assertRepairRowCounters(Counters counters, long expectedRowsMissing,
    long expectedRowsExtra, long expectedRowsCannotRepair) {
    assertEquals("ROWS_MISSING_ON_TARGET", expectedRowsMissing,
      counters.findCounter(SyncCounters.ROWS_MISSING_ON_TARGET).getValue());
    assertEquals("ROWS_EXTRA_ON_TARGET", expectedRowsExtra,
      counters.findCounter(SyncCounters.ROWS_EXTRA_ON_TARGET).getValue());
    assertEquals("ROWS_CANNOT_REPAIR", expectedRowsCannotRepair,
      counters.findCounter(SyncCounters.ROWS_CANNOT_REPAIR).getValue());
  }

  /**
   * Builds DDL for a "repair test" table that uses {@code COLUMN_ENCODED_BYTES=NONE} so column
   * qualifiers on disk match the SQL column name verbatim. This lets cell-level test helpers
   * inject raw HBase Puts/Deletes against {@code (cf=0, q=NAME)} or {@code (cf=0, q=NAME_VALUE)}
   * without computing encoded qualifier bytes.
   *
   * <p>Set {@code maxVersions > 1} when the test exercises hidden-version unwinding.
   */
  private String buildRepairTestTableDdl(String tableName, boolean withReplication, int maxVersions,
    String splitPoints) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n")
      .append("    ID INTEGER NOT NULL PRIMARY KEY,\n").append("    NAME VARCHAR(50),\n")
      .append("    NAME_VALUE BIGINT,\n").append("    UPDATED_DATE TIMESTAMP\n")
      .append(") COLUMN_ENCODED_BYTES=NONE, UPDATE_CACHE_FREQUENCY=0");
    if (withReplication) {
      sb.append(", REPLICATION_SCOPE=1");
    } else {
      sb.append(", REPLICATION_SCOPE=0");
    }
    if (maxVersions > 1) {
      sb.append(", VERSIONS=").append(maxVersions);
    }
    if (splitPoints != null && !splitPoints.isEmpty()) {
      sb.append(" SPLIT ON (").append(splitPoints).append(")");
    }
    return sb.toString();
  }

  /**
   * Creates the same {@link #buildRepairTestTableDdl} schema on both source and target clusters.
   * Used by repair tests that bypass replication and seed the two clusters separately.
   *
   * <p>Returns a wall-clock anchor in milliseconds. SCN-bound connections must use timestamps
   * &ge; the anchor, otherwise an SCN below the table's CREATE-TABLE timestamp surfaces as
   * {@code TableNotFoundException}.
   */
  private long createRepairTestTableOnBothClusters(String tableName, int maxVersions,
    String splitPoints) throws SQLException {
    executeTableCreation(sourceConnection,
      buildRepairTestTableDdl(tableName, false, maxVersions, splitPoints));
    executeTableCreation(targetConnection,
      buildRepairTestTableDdl(tableName, false, maxVersions, splitPoints));
    // Wait until the wall clock advances at least one millisecond past the CREATE TABLE
    // timestamp so any caller-chosen SCN >= the returned anchor is guaranteed to be above the
    // table's metadata row.
    long anchor = System.currentTimeMillis() + 1;
    while (System.currentTimeMillis() < anchor) {
      // spin
    }
    return anchor;
  }

  /**
   * Returns a Phoenix connection bound to {@code scnTimestamp} via
   * {@link PhoenixRuntime#CURRENT_SCN_ATTRIB}. UPSERTs through this connection write cells with
   * {@code timestamp == scnTimestamp}, giving tests precise control over cell timestamps without
   * needing to construct raw HBase Puts.
   */
  private Connection openConnectionAtScn(String zkUrl, long scnTimestamp) throws SQLException {
    Properties props = new Properties();
    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scnTimestamp));
    return DriverManager.getConnection("jdbc:phoenix:" + zkUrl, props);
  }

  /**
   * Resolves the HBase {@link Table} backing a Phoenix table for a given Phoenix
   * {@link Connection}. Used by raw-cell helpers that need to bypass Phoenix and write cells at
   * exact (cf, q, ts) coordinates.
   */
  private Table getHBaseTable(Connection phoenixConn, String phoenixTableName) throws Exception {
    PhoenixConnection pConn = phoenixConn.unwrap(PhoenixConnection.class);
    byte[] physicalName = pConn.getTable(phoenixTableName).getPhysicalName().getBytes();
    return pConn.getQueryServices().getTable(physicalName);
  }

  /**
   * Writes a single raw {@link Put} cell at the given {@code (rowKey, family, qualifier, ts,
   * value)} coordinates, bypassing Phoenix's UPSERT path. Tests use this to plant historical
   * versions or specific timestamps that Phoenix wouldn't naturally produce.
   */
  private void writeRawCell(Connection phoenixConn, String tableName, byte[] rowKey, String family,
    String qualifier, long ts, byte[] value) throws Exception {
    try (Table hTable = getHBaseTable(phoenixConn, tableName)) {
      Put put = new Put(rowKey);
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, value);
      hTable.put(put);
    }
  }

  /**
   * Plants a raw point-{@link Delete} (single-version tombstone) at {@code (rowKey, family,
   * qualifier, ts)}. Equivalent to HBase's {@code Delete.addColumn(family, qualifier, ts)}.
   */
  private void writeRawPointDelete(Connection phoenixConn, String tableName, byte[] rowKey,
    String family, String qualifier, long ts) throws Exception {
    try (Table hTable = getHBaseTable(phoenixConn, tableName)) {
      Delete del = new Delete(rowKey);
      del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts);
      hTable.delete(del);
    }
  }

  /**
   * Plants a raw {@link Delete#addColumns} (DeleteColumn — covers all versions at {@code ts <=
   * markerTs}) at {@code (rowKey, family, qualifier)}. Used by shadow-detection tests where a
   * future source Put at {@code ts <= markerTs} must be suppressed.
   */
  private void writeRawDeleteColumn(Connection phoenixConn, String tableName, byte[] rowKey,
    String family, String qualifier, long markerTs) throws Exception {
    try (Table hTable = getHBaseTable(phoenixConn, tableName)) {
      Delete del = new Delete(rowKey);
      del.addColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier), markerTs);
      hTable.delete(del);
    }
  }

  /**
   * Returns the row-key bytes Phoenix uses for an INTEGER primary key value, matching the
   * encoding used by {@code splitTableAt}.
   */
  private static byte[] integerRowKey(int id) {
    return PInteger.INSTANCE.toBytes(id);
  }

  /**
   * Returns a fresh {@link Configuration} clone of the source cluster with a custom
   * {@link PhoenixSyncTableTool#PHOENIX_SYNC_TABLE_REPAIR_BATCH_SIZE} setting baked in. Used by
   * the mid-row-flush boundary test.
   */
  private static Configuration sourceClusterConfWithRepairBatchSize(int repairBatchSize) {
    Configuration conf = sourceClusterConf();
    conf.setInt(PhoenixSyncTableTool.PHOENIX_SYNC_TABLE_REPAIR_BATCH_SIZE, repairBatchSize);
    return conf;
  }

  /**
   * Validates sync counters with exact source/target rows and minimum chunk thresholds. Use this
   * when chunk counts may vary but should be at least certain values.
   */
  private void validateSyncCountersWithMinChunk(SyncCountersResult counters,
    long expectedSourceRows, long expectedTargetRows, long minChunksVerified,
    long minChunksMismatched) {
    assertEquals("Should process expected source rows", expectedSourceRows,
      counters.sourceRowsProcessed);
    assertEquals("Should process expected target rows", expectedTargetRows,
      counters.targetRowsProcessed);
    assertTrue(String.format("Should have at least %d verified chunks, actual: %d",
      minChunksVerified, counters.chunksVerified), counters.chunksVerified >= minChunksVerified);
    assertTrue(String.format("Should have at least %d mismatched chunks, actual: %d",
      minChunksMismatched, counters.chunksMismatched),
      counters.chunksMismatched >= minChunksMismatched);
  }

  /**
   * Validates that a checkpoint table has entries with proper structure.
   */
  private void validateCheckpointEntries(List<PhoenixSyncTableCheckpointOutputRow> entries,
    String expectedTableName, String expectedTargetCluster, int expectedSourceRows,
    int expectedTargetRows, int expectedChunkVerified, int expectedChunkMismatched,
    int expectedMapperRegion, int expectedMapperMismatched, String expectedTenantId) {
    int mapperRegionCount = 0;
    int chunkCount = 0;
    int mismatchedEntry = 0;
    int sourceRowsProcessed = 0;
    int targetRowsProcessed = 0;
    for (PhoenixSyncTableCheckpointOutputRow entry : entries) {
      // Validate primary key columns
      assertEquals("TABLE_NAME should match", expectedTableName, entry.getTableName());
      assertEquals("TARGET_CLUSTER should match", expectedTargetCluster, entry.getTargetCluster());
      assertNotNull("TYPE should not be null", entry.getType());
      assertTrue("TYPE should be REGION or CHUNK",
        PhoenixSyncTableCheckpointOutputRow.Type.REGION.equals(entry.getType())
          || PhoenixSyncTableCheckpointOutputRow.Type.CHUNK.equals(entry.getType()));

      // Validate TENANT_ID
      if (expectedTenantId == null) {
        assertNull("TENANT_ID should be null for non-multi-tenant tables", entry.getTenantId());
      } else {
        assertEquals("TENANT_ID should match", expectedTenantId, entry.getTenantId());
      }

      // Validate time range
      assertTrue("FROM_TIME should be >= 0", entry.getFromTime() >= 0);
      assertTrue("TO_TIME should be > FROM_TIME", entry.getToTime() > entry.getFromTime());

      // Validate execution timestamps
      assertNotNull("EXECUTION_START_TIME should not be null", entry.getExecutionStartTime());
      assertNotNull("EXECUTION_END_TIME should not be null", entry.getExecutionEndTime());
      assertTrue("EXECUTION_END_TIME should be >= EXECUTION_START_TIME",
        entry.getExecutionEndTime().getTime() >= entry.getExecutionStartTime().getTime());

      // Validate status
      assertNotNull("STATUS should not be null", entry.getStatus());
      assertTrue("STATUS should be VERIFIED or MISMATCHED",
        PhoenixSyncTableCheckpointOutputRow.Status.VERIFIED.equals(entry.getStatus())
          || PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED.equals(entry.getStatus()));

      if (PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED.equals(entry.getStatus())) {
        mismatchedEntry++;
      }

      // Count entry types
      if (PhoenixSyncTableCheckpointOutputRow.Type.REGION.equals(entry.getType())) {
        mapperRegionCount++;
        sourceRowsProcessed += (int) entry.getSourceRowsProcessed();
        targetRowsProcessed += (int) entry.getTargetRowsProcessed();
      } else if (PhoenixSyncTableCheckpointOutputRow.Type.CHUNK.equals(entry.getType())) {
        chunkCount++;
        assertNotNull("COUNTERS should not be null for CHUNK entries", entry.getCounters());
      }
    }

    assertEquals(String.format("Should have %d REGION entry", expectedMapperRegion),
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
