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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.PhoenixSyncTableMapper.SyncCounters;
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRepository;
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRow;
import org.apache.phoenix.mapreduce.PhoenixSyncTableTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PInteger;
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
  public void testSyncTableValidateWithDataDifference() throws Exception {
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

    // Run sync tool on the INDEX table (not the data table)
    Job job = runSyncTool(indexName);
    SyncCountersResult counters = getSyncCounters(job);

    validateSyncCounters(counters, 10, 10, 10, 0);

    // Verify checkpoint entries show mismatches
    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, indexName, targetZkQuorum);

    assertFalse("Should have checkpointEntries", checkpointEntries.isEmpty());
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
  }

  @Test
  public void testSyncTableValidateCheckpointWithPartialReRunAndRegionSplits() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    List<Integer> sourceSplits = Arrays.asList(15, 45, 51, 75, 95);
    splitTableAt(sourceConnection, uniqueTableName, sourceSplits);

    // Introduce differences on target scattered across the dataset
    List<Integer> mismatchIds = Arrays.asList(10, 25, 40, 55, 70, 85, 95);
    for (int id : mismatchIds) {
      upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { id },
        new String[] { "MODIFIED_NAME_" + id });
    }

    // Capture consistent time range for both runs
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run sync tool for the FIRST time with explicit time range
    Job job1 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters1 = getSyncCounters(job1);

    // Validate first run counters - should process all 100 rows
    assertEquals("Should process 100 source rows", 100, counters1.sourceRowsProcessed);
    assertEquals("Should process 100 target rows", 100, counters1.targetRowsProcessed);
    assertTrue("Should have at least 1 mismatched chunks", counters1.chunksMismatched > 0);

    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    assertTrue("Should have checkpoint entries after first run", checkpointEntries.size() > 0);

    // Separate mapper and chunk entries using utility method
    SeparatedCheckpointEntries separated = separateMapperAndChunkEntries(checkpointEntries);
    List<PhoenixSyncTableOutputRow> allMappers = separated.mappers;
    List<PhoenixSyncTableOutputRow> allChunks = separated.chunks;

    assertFalse("Should have mapper region entries", allMappers.isEmpty());
    assertFalse("Should have chunk entries", allChunks.isEmpty());

    // Select 3/4th of chunks from each mapper to delete (simulating partial rerun)
    // We repro the partial run via deleting some entries from checkpoint table and re-running the
    // tool
    List<PhoenixSyncTableOutputRow> chunksToDelete = selectChunksToDeleteFromMappers(
      sourceConnection, uniqueTableName, targetZkQuorum, fromTime, toTime, allMappers, 0.75);

    // Delete all mappers and selected chunks
    int deletedCount = deleteCheckpointEntries(sourceConnection, uniqueTableName, targetZkQuorum,
      allMappers, chunksToDelete);

    assertEquals("Should have deleted all mapper and selected chunk entries",
      allMappers.size() + chunksToDelete.size(), deletedCount);

    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterDelete =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    assertEquals("Should have fewer checkpoint entries after deletion",
      allMappers.size() + chunksToDelete.size(),
      checkpointEntries.size() - checkpointEntriesAfterDelete.size());

    // Calculate totals from REMAINING CHUNK entries in checkpoint table using utility method
    CheckpointAggregateCounters remainingCounters =
      calculateAggregateCountersFromCheckpoint(checkpointEntriesAfterDelete);

    List<Integer> additionalSourceSplits =
      Arrays.asList(12, 22, 28, 32, 42, 52, 58, 62, 72, 78, 82, 92);
    splitTableAt(sourceConnection, uniqueTableName, additionalSourceSplits);

    List<Integer> targetSplits = Arrays.asList(25, 40, 50, 65, 70, 80, 90);
    splitTableAt(targetConnection, uniqueTableName, targetSplits);

    // Wait for splits to complete
    Thread.sleep(3000);

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
    long totalSourceRows = remainingCounters.sourceRowsProcessed + counters2.sourceRowsProcessed;
    long totalTargetRows = remainingCounters.targetRowsProcessed + counters2.targetRowsProcessed;
    long totalVerifiedChunks = remainingCounters.chunksVerified + counters2.chunksVerified;
    long totalMismatchedChunks = remainingCounters.chunksMismatched + counters2.chunksMismatched;

    assertEquals(
      "Remaining + Second run source rows should equal first run source rows. " + "Remaining: "
        + remainingCounters.sourceRowsProcessed + ", Second run: " + counters2.sourceRowsProcessed
        + ", Total: " + totalSourceRows + ", Expected: " + counters1.sourceRowsProcessed,
      counters1.sourceRowsProcessed, totalSourceRows);

    assertEquals(
      "Remaining + Second run target rows should equal first run target rows. " + "Remaining: "
        + remainingCounters.targetRowsProcessed + ", Second run: " + counters2.targetRowsProcessed
        + ", Total: " + totalTargetRows + ", Expected: " + counters1.targetRowsProcessed,
      counters1.targetRowsProcessed, totalTargetRows);

    assertEquals("Remaining + Second run verified chunks should equal first run verified chunks. "
      + "Remaining: " + remainingCounters.chunksVerified + ", Second run: "
      + counters2.chunksVerified + ", Total: " + totalVerifiedChunks + ", Expected: "
      + counters1.chunksVerified, counters1.chunksVerified, totalVerifiedChunks);

    assertEquals(
      "Remaining + Second run mismatched chunks should equal first run mismatched chunks. "
        + "Remaining: " + remainingCounters.chunksMismatched + ", Second run: "
        + counters2.chunksMismatched + ", Total: " + totalMismatchedChunks + ", Expected: "
        + counters1.chunksMismatched,
      counters1.chunksMismatched, totalMismatchedChunks);

    // Verify checkpoint table has entries for the reprocessed regions
    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterRerun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    // After rerun, we should have at least more entries compared to delete table
    assertTrue("Should have checkpoint entries after rerun",
      checkpointEntriesAfterRerun.size() > checkpointEntriesAfterDelete.size());
  }

  @Test
  public void testSyncTableValidateCheckpointWithPartialReRunAndRegionMerges() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    List<Integer> sourceSplits = Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90);
    splitTableAt(sourceConnection, uniqueTableName, sourceSplits);

    List<Integer> mismatchIds = Arrays.asList(5, 15, 25, 35, 45, 55, 65, 75, 85, 95);
    for (int id : mismatchIds) {
      upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { id },
        new String[] { "MODIFIED_NAME_" + id });
    }

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    Job job1 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters1 = getSyncCounters(job1);

    assertEquals("Should process 100 source rows", 100, counters1.sourceRowsProcessed);
    assertEquals("Should process 100 target rows", 100, counters1.targetRowsProcessed);
    assertTrue("Should have mismatched chunks", counters1.chunksMismatched > 0);

    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    assertTrue("Should have checkpoint entries after first run", checkpointEntries.size() > 0);

    // Separate mapper and chunk entries using utility method
    SeparatedCheckpointEntries separated = separateMapperAndChunkEntries(checkpointEntries);
    List<PhoenixSyncTableOutputRow> allMappers = separated.mappers;
    List<PhoenixSyncTableOutputRow> allChunks = separated.chunks;

    assertTrue("Should have mapper region entries", allMappers.size() > 0);
    assertTrue("Should have chunk entries", allChunks.size() > 0);

    // Select 3/4th of chunks from each mapper to delete (simulating partial rerun)
    // We repro the partial run via deleting some entries from checkpoint table and re-running the
    // tool. Use production repository to query chunks within mapper boundaries.
    List<PhoenixSyncTableOutputRow> chunksToDelete = selectChunksToDeleteFromMappers(
      sourceConnection, uniqueTableName, targetZkQuorum, fromTime, toTime, allMappers, 0.75);

    // Delete all mappers and selected chunks
    int deletedCount = deleteCheckpointEntries(sourceConnection, uniqueTableName, targetZkQuorum,
      allMappers, chunksToDelete);

    assertEquals("Should have deleted all mapper and selected chunk entries",
      allMappers.size() + chunksToDelete.size(), deletedCount);

    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterDelete =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    assertEquals("Should have fewer checkpoint entries after deletion",
      allMappers.size() + chunksToDelete.size(),
      checkpointEntries.size() - checkpointEntriesAfterDelete.size());

    // Calculate totals from REMAINING CHUNK entries in checkpoint table using utility method
    CheckpointAggregateCounters remainingCounters =
      calculateAggregateCountersFromCheckpoint(checkpointEntriesAfterDelete);

    // Merge adjacent regions on source (merge 6 pairs of regions)
    mergeAdjacentRegions(sourceConnection, uniqueTableName, 6);

    // Merge adjacent regions on target (merge 6 pairs of regions)
    mergeAdjacentRegions(targetConnection, uniqueTableName, 6);

    // Wait for merges to complete
    Thread.sleep(2000);

    // Run sync tool again with SAME time range - should reprocess only deleted regions
    // despite the new region boundaries from merges
    Job job2 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    long totalSourceRows = remainingCounters.sourceRowsProcessed + counters2.sourceRowsProcessed;
    long totalTargetRows = remainingCounters.targetRowsProcessed + counters2.targetRowsProcessed;
    long totalVerifiedChunks = remainingCounters.chunksVerified + counters2.chunksVerified;
    long totalMismatchedChunks = remainingCounters.chunksMismatched + counters2.chunksMismatched;

    assertEquals(
      "Remaining + Second run source rows should equal first run source rows. " + "Remaining: "
        + remainingCounters.sourceRowsProcessed + ", Second run: " + counters2.sourceRowsProcessed
        + ", Total: " + totalSourceRows + ", Expected: " + counters1.sourceRowsProcessed,
      counters1.sourceRowsProcessed, totalSourceRows);

    assertEquals(
      "Remaining + Second run target rows should equal first run target rows. " + "Remaining: "
        + remainingCounters.targetRowsProcessed + ", Second run: " + counters2.targetRowsProcessed
        + ", Total: " + totalTargetRows + ", Expected: " + counters1.targetRowsProcessed,
      counters1.targetRowsProcessed, totalTargetRows);

    assertEquals("Remaining + Second run verified chunks should equal first run verified chunks. "
      + "Remaining: " + remainingCounters.chunksVerified + ", Second run: "
      + counters2.chunksVerified + ", Total: " + totalVerifiedChunks + ", Expected: "
      + counters1.chunksVerified, counters1.chunksVerified, totalVerifiedChunks);

    assertEquals(
      "Remaining + Second run mismatched chunks should equal first run mismatched chunks. "
        + "Remaining: " + remainingCounters.chunksMismatched + ", Second run: "
        + counters2.chunksMismatched + ", Total: " + totalMismatchedChunks + ", Expected: "
        + counters1.chunksMismatched,
      counters1.chunksMismatched, totalMismatchedChunks);

    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterRerun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    // After rerun with merges, we should have more entries as after deletion
    assertTrue("Should have checkpoint entries after rerun",
      checkpointEntriesAfterRerun.size() > checkpointEntriesAfterDelete.size());
  }

  @Test
  public void testSyncTableValidateIdempotentOnReRun() throws Exception {
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

    // Second run should process ZERO rows (idempotent behavior)
    validateSyncCounters(counters2, 0, 0, 0, 0);

    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterSecondRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    assertEquals("Checkpoint entries should be identical after idempotent run",
      checkpointEntriesAfterFirstRun, checkpointEntriesAfterSecondRun);
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

    // Query checkpoint table to verify entries were created
    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterFirstRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

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

    // KEY VALIDATION: Second run should process ZERO rows despite new region boundaries
    validateSyncCounters(counters2, 0, 0, 0, 0);

    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterSecondRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    // Checkpoint entries may differ in count due to new regions, but all original data is
    // checkpointed
    assertTrue("Should have checkpoint entries after second run",
      checkpointEntriesAfterSecondRun.size() > 0);
  }

  @Test
  public void testSyncTableValidateWithSchemaAndTableNameOptions() throws Exception {
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
  public void testSyncTableValidateInBackgroundMode() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] args =
      new String[] { "--table-name", uniqueTableName, "--target-cluster", targetZkQuorum,
        "--chunk-size", "1", "--to-time", String.valueOf(System.currentTimeMillis()) };

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

  @Test
  public void testSyncTableValidateWithCustomTimeouts() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    introduceAndVerifyTargetDifferences(uniqueTableName);

    // Create configuration with custom timeout values
    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());

    // Set custom timeout values (higher than defaults to ensure job succeeds)
    long customQueryTimeout = 900000L; // 15 minutes
    long customRpcTimeout = 1200000L; // 20 minutes
    long customScannerTimeout = 2400000L; // 40 minutes
    int customRpcRetries = 10;

    conf.setLong(QueryServices.SYNC_TABLE_QUERY_TIMEOUT_ATTRIB, customQueryTimeout);
    conf.setLong(QueryServices.SYNC_TABLE_RPC_TIMEOUT_ATTRIB, customRpcTimeout);
    conf.setLong(QueryServices.SYNC_TABLE_CLIENT_SCANNER_TIMEOUT_ATTRIB, customScannerTimeout);
    conf.setInt(QueryServices.SYNC_TABLE_RPC_RETRIES_COUNTER, customRpcRetries);

    String[] args = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--chunk-size", "1", "--run-foreground", "--to-time",
      String.valueOf(System.currentTimeMillis()) };

    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);
    int exitCode = tool.run(args);

    Job job = tool.getJob();
    assertNotNull("Job should not be null", job);
    assertEquals("Tool should complete successfully with custom timeouts", 0, exitCode);

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
    int sourceCount = getRowCount(sourceConnection, uniqueTableName);
    int targetCount = getRowCount(targetConnection, uniqueTableName);
    assertEquals("Source should have 10 rows (odd numbers 1-19)", 10, sourceCount);
    assertEquals("Target should have 15 rows (odd 1-19 + even 2-10)", 15, targetCount);

    // Run sync tool to detect the extra rows interspersed on target
    Job job = runSyncTool(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(job);

    validateSyncCounters(counters, 10, 15, 5, 5);

    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    // Count mismatched entries in checkpoint table
    int mismatchedCount = 0;
    for (PhoenixSyncTableOutputRow entry : checkpointEntries) {
      if (PhoenixSyncTableOutputRow.Status.MISMATCHED.equals(entry.getStatus())) {
        mismatchedCount++;
      }
    }
    assertTrue("Should have mismatched entries for chunks with extra rows", mismatchedCount > 0);

    // Verify source and target are still different
    List<TestRow> sourceRows = queryAllRows(sourceConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
    List<TestRow> targetRows = queryAllRows(targetConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
    assertEquals("Source should still have 10 rows", 10, sourceRows.size());
    assertEquals("Target should still have 15 rows", 15, targetRows.size());
    assertNotEquals("Source and target should have different data", sourceRows, targetRows);

    // Verify that source has only odd numbers
    for (TestRow row : sourceRows) {
      assertTrue("Source should only have odd IDs", row.id % 2 == 1);
    }

    // Verify that target has all numbers 1-11 (with gaps filled) and 13,15,17,19
    assertEquals("Target should have ID=1", 1, targetRows.get(0).id);
    assertEquals("Target should have ID=2", 2, targetRows.get(1).id);
    assertEquals("Target should have ID=10", 10, targetRows.get(9).id);
    assertEquals("Target should have ID=11", 11, targetRows.get(10).id);
    assertEquals("Target should have ID=19", 19, targetRows.get(14).id);
  }

  @Test
  public void testSyncTableValidateWithConcurrentRegionSplits() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 100);
    // Introduce some mismatches on target before sync
    List<Integer> mismatchIds = Arrays.asList(15, 35, 55, 75, 95);
    for (int id : mismatchIds) {
      upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { id },
        new String[] { "MODIFIED_NAME_" + id });
    }

    // Capture time range for the sync
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Create a thread that will perform splits on source cluster during sync
    Thread sourceSplitThread = new Thread(() -> {
      try {
        // Split source at multiple points (creating more regions during sync)
        List<Integer> sourceSplits = Arrays.asList(20, 25, 40, 45, 60, 65, 80, 85, 95);
        splitTableAt(sourceConnection, uniqueTableName, sourceSplits);
      } catch (Exception e) {
        LOGGER.error("Error during source splits", e);
      }
    });

    // Create a thread that will perform splits on target cluster during sync
    Thread targetSplitThread = new Thread(() -> {
      try {
        // Split target at different points than source (asymmetric region boundaries)
        List<Integer> targetSplits = Arrays.asList(11, 21, 31, 41, 51, 75, 81, 91);
        splitTableAt(targetConnection, uniqueTableName, targetSplits);
      } catch (Exception e) {
        LOGGER.error("Error during target splits", e);
      }
    });

    // Start split threads
    sourceSplitThread.start();
    targetSplitThread.start();

    // Run sync tool while splits are happening
    // Use smaller chunk size to increase chances of hitting split boundaries
    Job job = runSyncToolWithChunkSize(uniqueTableName, 512, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));

    // Wait for split threads to complete
    sourceSplitThread.join(30000); // 30 second timeout
    targetSplitThread.join(30000);

    // Verify the job completed successfully despite concurrent splits
    assertTrue("Sync job should complete successfully despite concurrent splits",
      job.isSuccessful());

    SyncCountersResult counters = getSyncCounters(job);

    // Validate counters - should process all 100 rows and detect the 5 mismatched rows
    validateSyncCountersExactSourceTarget(counters, 100, 100, 1, 1);

    // Verify checkpoint entries were created
    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    assertFalse("Should have checkpoint entries", checkpointEntries.isEmpty());

    // Count mismatched entries
    int mismatchedCount = 0;
    for (PhoenixSyncTableOutputRow entry : checkpointEntries) {
      if (PhoenixSyncTableOutputRow.Status.MISMATCHED.equals(entry.getStatus())) {
        mismatchedCount++;
      }
    }
    assertTrue("Should have mismatched entries for modified rows", mismatchedCount >= 5);
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
    int sourceCount = getRowCount(sourceConnection, uniqueTableName);
    int targetCount = getRowCount(targetConnection, uniqueTableName);
    assertEquals("Both should have 10 rows", sourceCount, targetCount);

    // Query and verify data values are identical (but timestamps differ)
    List<TestRow> sourceRows = queryAllRows(sourceConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
    List<TestRow> targetRows = queryAllRows(targetConnection,
      "SELECT ID, NAME, NAME_VALUE FROM " + uniqueTableName + " ORDER BY ID");
    assertEquals("Row values should be identical", sourceRows, targetRows);

    // Run sync tool - should detect timestamp differences as mismatches
    Job job = runSyncTool(uniqueTableName);
    SyncCountersResult counters = getSyncCounters(job);

    // Validate counters - all rows should be processed and all chunks mismatched
    // because timestamps are included in the hash calculation
    validateSyncCounters(counters, 10, 10, 0, 10);

    // Verify checkpoint entries show mismatches
    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    int mismatchedCount = 0;
    for (PhoenixSyncTableOutputRow entry : checkpointEntries) {
      if (PhoenixSyncTableOutputRow.Status.MISMATCHED.equals(entry.getStatus())) {
        mismatchedCount++;
      }
    }
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
    for (int id : mismatchIds) {
      upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { id },
        new String[] { "MODIFIED_NAME_" + id });
    }

    // Capture time range for the sync
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Create a thread that will perform merges on source cluster during sync
    Thread sourceMergeThread = new Thread(() -> {
      try {
        // Merge adjacent regions on source
        mergeAdjacentRegions(sourceConnection, uniqueTableName, 6);
      } catch (Exception e) {
        LOGGER.error("Error during source merges", e);
      }
    });

    // Create a thread that will perform merges on target cluster during sync
    Thread targetMergeThread = new Thread(() -> {
      try {
        mergeAdjacentRegions(targetConnection, uniqueTableName, 6);
      } catch (Exception e) {
        LOGGER.error("Error during target merges", e);
      }
    });

    // Start merge threads
    sourceMergeThread.start();
    targetMergeThread.start();

    // Run sync tool while merges are happening
    Job job = runSyncToolWithChunkSize(uniqueTableName, 512, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));

    // Wait for merge threads to complete
    sourceMergeThread.join(30000); // 30 second timeout
    targetMergeThread.join(30000);

    // Verify the job completed successfully despite concurrent merges
    assertTrue("Sync job should complete successfully despite concurrent merges",
      job.isSuccessful());

    SyncCountersResult counters = getSyncCounters(job);

    // Validate counters - should process all 100 rows and detect mismatched chunks
    validateSyncCountersExactSourceTarget(counters, 100, 100, 1, 1);

    // Verify checkpoint entries were created
    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    assertFalse("Should have checkpoint entries", checkpointEntries.isEmpty());

    // Run sync again to verify idempotent behavior after merges
    Job job2 = runSyncToolWithChunkSize(uniqueTableName, 512, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));
    SyncCountersResult counters2 = getSyncCounters(job2);

    // Second run should process ZERO rows (all checkpointed despite region merges)
    validateSyncCounters(counters2, 0, 0, 0, 0);
  }

  @Test
  public void testSyncTableValidateWithPagingTimeout() throws Exception {
    // Create tables on both clusters
    setupStandardTestWithReplication(uniqueTableName, 1, 100);

    // Introduce mismatches scattered across the dataset
    List<Integer> mismatchIds = Arrays.asList(15, 25, 35, 45, 55, 75);
    for (int id : mismatchIds) {
      upsertRowsOnTarget(targetConnection, uniqueTableName, new int[] { id },
        new String[] { "MODIFIED_NAME_" + id });
    }

    // First, run without aggressive paging to establish baseline chunk count
    Configuration baselineConf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] baselineArgs = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--chunk-size", "10240", "--to-time",
      String.valueOf(System.currentTimeMillis()) };

    PhoenixSyncTableTool baselineTool = new PhoenixSyncTableTool();
    baselineTool.setConf(baselineConf);
    baselineTool.run(baselineArgs);
    Job baselineJob = baselineTool.getJob();
    long baselineChunkCount =
      baselineJob.getCounters().findCounter(SyncCounters.CHUNKS_VERIFIED).getValue();

    // Configure paging with aggressive timeouts to force mid-chunk timeouts
    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());

    // Enable server-side paging
    conf.setBoolean(QueryServices.PHOENIX_SERVER_PAGING_ENABLED_ATTRIB, true);
    // Set extremely short paging timeout to force frequent paging
    long aggressiveRpcTimeout = 1000L; // 1 second RPC timeout
    conf.setLong(QueryServices.SYNC_TABLE_RPC_TIMEOUT_ATTRIB, aggressiveRpcTimeout);
    conf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, aggressiveRpcTimeout);
    // Force server-side paging to occur by setting page size to 1ms
    conf.setLong(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, 1);

    int chunkSize = 10240; // 100KB

    // Create a thread that will perform splits on source cluster during sync
    Thread sourceSplitThread = new Thread(() -> {
      try {
        List<Integer> sourceSplits = Arrays.asList(12, 22, 32, 42, 52, 63, 72, 82, 92, 98);
        splitTableAt(sourceConnection, uniqueTableName, sourceSplits);
      } catch (Exception e) {
        LOGGER.error("Error during source splits", e);
      }
    });

    // Create a thread that will perform splits on target cluster during sync
    Thread targetSplitThread = new Thread(() -> {
      try {
        List<Integer> targetSplits = Arrays.asList(13, 23, 33, 43, 53, 64, 74, 84, 95, 99);
        splitTableAt(targetConnection, uniqueTableName, targetSplits);
      } catch (Exception e) {
        LOGGER.error("Error during target splits", e);
      }
    });

    // Start split threads
    sourceSplitThread.start();
    targetSplitThread.start();

    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // Run sync tool while splits are happening
    Job job = runSyncToolWithChunkSize(uniqueTableName, chunkSize, conf, "--from-time",
      String.valueOf(fromTime), "--to-time", String.valueOf(toTime));

    // Wait for split threads to complete
    sourceSplitThread.join(30000); // 30 second timeout
    targetSplitThread.join(30000);

    // Verify the job completed successfully despite concurrent splits and paging timeouts
    assertTrue("Sync job should complete successfully despite paging and concurrent splits",
      job.isSuccessful());

    SyncCountersResult counters = getSyncCounters(job);

    // Validate that all 100 rows were processed on both sides
    // Despite paging timeouts AND concurrent region splits, no rows should be lost
    validateSyncCountersExactSourceTarget(counters, 100, 100, 1, 1);

    // Paging should create MORE chunks than baseline
    // Concurrent region splits may also create additional chunks as mappers process new regions
    long pagingChunkCount = counters.chunksVerified;

    assertTrue(
      "Paging should create more chunks than baseline due to mid-chunk timeouts. " + "Baseline: "
        + baselineChunkCount + ", Paging: " + pagingChunkCount,
      pagingChunkCount > baselineChunkCount);

    // Verify checkpoint entries were created
    List<PhoenixSyncTableOutputRow> checkpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    assertFalse("Should have checkpoint entries", checkpointEntries.isEmpty());
  }

  @Test
  public void testSyncTableMapperFailsWithNonExistentTable() throws Exception {
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Try to run sync tool on a NON-EXISTENT table
    String nonExistentTable = "NON_EXISTENT_TABLE_" + System.currentTimeMillis();
    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] args = new String[] { "--table-name", nonExistentTable, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--to-time", String.valueOf(System.currentTimeMillis()) };

    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);

    try {
      int exitCode = tool.run(args);
      assertTrue(
        String.format("Table %s does not exist, mapper setup should fail", nonExistentTable),
        exitCode != 0);
    } catch (Exception ex) {
      fail("Tool should return non-zero exit code on failure instead of throwing exception: "
        + ex.getMessage());
    }
  }

  @Test
  public void testSyncTableMapperFailsWithInvalidTargetCluster() throws Exception {
    // Create table on source cluster
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Try to run sync tool with INVALID target cluster ZK quorum
    String invalidTargetZk = "invalid-zk-host:2181:/hbase";
    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] args =
      new String[] { "--table-name", uniqueTableName, "--target-cluster", invalidTargetZk,
        "--run-foreground", "--to-time", String.valueOf(System.currentTimeMillis()) };

    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);

    try {
      int exitCode = tool.run(args);
      assertTrue(
        String.format("Target cluster %s is invalid, mapper setup should fail", invalidTargetZk),
        exitCode != 0);
    } catch (Exception ex) {
      fail("Tool should return non-zero exit code on failure instead of throwing exception: "
        + ex.getMessage());
    }
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
    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] args = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--to-time", String.valueOf(System.currentTimeMillis()) };

    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);

    try {
      int exitCode = tool.run(args);
      assertTrue(String.format(
        "Table %s does not exist on target cluster, mapper map() should fail during target scan",
        uniqueTableName), exitCode != 0);
    } catch (Exception ex) {
      fail("Tool should return non-zero exit code on failure instead of throwing exception: "
        + ex.getMessage());
    }
  }

  @Test
  public void testSyncTableCheckpointPersistsAcrossFailedRuns() throws Exception {
    // Setup table with replication and insert data
    // setupStandardTestWithReplication creates splits, resulting in multiple mapper regions
    setupStandardTestWithReplication(uniqueTableName, 1, 10);

    // Capture time range for both runs (ensures checkpoint lookup will match)
    long fromTime = 0L;
    long toTime = System.currentTimeMillis();

    // First run: Sync should succeed and create checkpoint entries for all mappers
    Job job1 = runSyncTool(uniqueTableName, "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime));
    SyncCountersResult counters1 = getSyncCounters(job1);

    // Validate first run succeeded
    assertTrue("First run should succeed", job1.isSuccessful());
    assertEquals("Should process 10 source rows", 10, counters1.sourceRowsProcessed);
    assertEquals("Should process 10 target rows", 10, counters1.targetRowsProcessed);

    // Query checkpoint table to get all mapper entries
    List<PhoenixSyncTableOutputRow> allCheckpointEntries =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);

    // Separate mapper and chunk entries using utility method
    SeparatedCheckpointEntries separated = separateMapperAndChunkEntries(allCheckpointEntries);
    List<PhoenixSyncTableOutputRow> mapperEntries = separated.mappers;
    List<PhoenixSyncTableOutputRow> allChunks = separated.chunks;

    assertTrue("Should have at least 3 mapper entries after first run", mapperEntries.size() >= 3);

    // Select 3/4th of chunks from each mapper to delete (simulating partial rerun)
    // We repro the partial run via deleting some entries from checkpoint table and re-running the
    // tool. Use production repository to query chunks within mapper boundaries.
    List<PhoenixSyncTableOutputRow> chunksToDelete = selectChunksToDeleteFromMappers(
      sourceConnection, uniqueTableName, targetZkQuorum, fromTime, toTime, mapperEntries, 0.75);

    // Delete all mappers and selected chunks using utility method
    deleteCheckpointEntries(sourceConnection, uniqueTableName, targetZkQuorum, mapperEntries,
      chunksToDelete);

    // Verify mapper entries were deleted
    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterDelete =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    SeparatedCheckpointEntries separatedAfterDelete =
      separateMapperAndChunkEntries(checkpointEntriesAfterDelete);

    assertEquals("Should have 0 mapper entries after deleting all mappers", 0,
      separatedAfterDelete.mappers.size());
    assertEquals("Should have remaining chunk entries after deletion",
      allChunks.size() - chunksToDelete.size(), separatedAfterDelete.chunks.size());

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
    Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    String[] args = new String[] { "--table-name", uniqueTableName, "--target-cluster",
      targetZkQuorum, "--run-foreground", "--from-time", String.valueOf(fromTime), "--to-time",
      String.valueOf(toTime) };

    PhoenixSyncTableTool tool = new PhoenixSyncTableTool();
    tool.setConf(conf);
    int exitCode = tool.run(args);

    // Job should fail
    assertTrue("Second run should fail with non-zero exit code due to missing target table",
      exitCode != 0);
    LOGGER.info("Second run failed as expected with exit code: {}", exitCode);

    // Remaining chunk entries that we dint delete should still persist despite job failure
    List<PhoenixSyncTableOutputRow> checkpointEntriesAfterFailedRun =
      queryCheckpointTable(sourceConnection, uniqueTableName, targetZkQuorum);
    SeparatedCheckpointEntries separatedAfterFailedRun =
      separateMapperAndChunkEntries(checkpointEntriesAfterFailedRun);

    // After the failed run:
    // - No mapper entries should exist (we deleted them all, and the job failed before creating new
    // ones)
    // - Only the remaining chunk entries (1/4th) should persist
    assertEquals("Should have 0 mapper entries after failed run", 0,
      separatedAfterFailedRun.mappers.size());
    assertEquals("Remaining chunk entries should persist after failed run",
      allChunks.size() - chunksToDelete.size(), separatedAfterFailedRun.chunks.size());
  }

  /**
   * Helper class to hold separated mapper and chunk entries.
   */
  private static class SeparatedCheckpointEntries {
    final List<PhoenixSyncTableOutputRow> mappers;
    final List<PhoenixSyncTableOutputRow> chunks;

    SeparatedCheckpointEntries(List<PhoenixSyncTableOutputRow> mappers,
      List<PhoenixSyncTableOutputRow> chunks) {
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
    separateMapperAndChunkEntries(List<PhoenixSyncTableOutputRow> entries) {
    List<PhoenixSyncTableOutputRow> mappers = new ArrayList<>();
    List<PhoenixSyncTableOutputRow> chunks = new ArrayList<>();

    for (PhoenixSyncTableOutputRow entry : entries) {
      if (PhoenixSyncTableOutputRow.Type.MAPPER_REGION.equals(entry.getType())) {
        mappers.add(entry);
      } else if (PhoenixSyncTableOutputRow.Type.CHUNK.equals(entry.getType())) {
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
    calculateAggregateCountersFromCheckpoint(List<PhoenixSyncTableOutputRow> entries) {
    long sourceRowsProcessed = 0;
    long targetRowsProcessed = 0;
    long chunksVerified = 0;
    long chunksMismatched = 0;

    for (PhoenixSyncTableOutputRow entry : entries) {
      if (PhoenixSyncTableOutputRow.Type.CHUNK.equals(entry.getType())) {
        sourceRowsProcessed += entry.getSourceRowsProcessed();
        targetRowsProcessed += entry.getTargetRowsProcessed();
        if (PhoenixSyncTableOutputRow.Status.VERIFIED.equals(entry.getStatus())) {
          chunksVerified++;
        } else if (PhoenixSyncTableOutputRow.Status.MISMATCHED.equals(entry.getStatus())) {
          chunksMismatched++;
        }
      }
    }

    return new CheckpointAggregateCounters(sourceRowsProcessed, targetRowsProcessed, chunksVerified,
      chunksMismatched);
  }

  /**
   * Finds all chunks that belong to a specific mapper region using the production repository. This
   * ensures test code uses the same boundary logic as production code.
   * @param conn          Connection to use
   * @param tableName     Table name
   * @param targetCluster Target cluster ZK quorum
   * @param fromTime      From time for checkpoint query
   * @param toTime        To time for checkpoint query
   * @param mapper        Mapper region entry
   * @return List of chunks belonging to this mapper region
   */
  private List<PhoenixSyncTableOutputRow> findChunksBelongingToMapper(Connection conn,
    String tableName, String targetCluster, long fromTime, long toTime,
    PhoenixSyncTableOutputRow mapper) throws SQLException {
    PhoenixSyncTableOutputRepository repository = new PhoenixSyncTableOutputRepository(conn);
    return repository.getProcessedChunks(tableName, targetCluster, fromTime, toTime,
      mapper.getStartRowKey(), mapper.getEndRowKey());
  }

  /**
   * Selects a percentage of chunks to delete from each mapper region. This is used to simulate
   * partial rerun scenarios where some checkpoint entries are missing.
   * Repository uses overlap-based boundary checking, so chunks that span across mapper boundaries
   * may be returned by multiple mappers. We use a Set to track unique chunks by their start row key
   * to avoid duplicates.
   * @param conn             Connection to use
   * @param tableName        Table name
   * @param targetCluster    Target cluster ZK quorum
   * @param fromTime         From time for checkpoint query
   * @param toTime           To time for checkpoint query
   * @param mappers          All mapper entries
   * @param deletionFraction Fraction of chunks to delete per mapper (0.0 to 1.0)
   * @return List of unique chunks selected for deletion
   */
  private List<PhoenixSyncTableOutputRow> selectChunksToDeleteFromMappers(Connection conn,
    String tableName, String targetCluster, long fromTime, long toTime,
    List<PhoenixSyncTableOutputRow> mappers, double deletionFraction) throws SQLException {
    // Use a map to track unique chunks by start row key to avoid duplicates
    Map<String, PhoenixSyncTableOutputRow> uniqueChunksToDelete = new LinkedHashMap<>();

    for (PhoenixSyncTableOutputRow mapper : mappers) {
      List<PhoenixSyncTableOutputRow> mapperChunks =
        findChunksBelongingToMapper(conn, tableName, targetCluster, fromTime, toTime, mapper);

      int chunksToDeleteCount = (int) Math.ceil(mapperChunks.size() * deletionFraction);
      for (int i = 0; i < chunksToDeleteCount && i < mapperChunks.size(); i++) {
        PhoenixSyncTableOutputRow chunk = mapperChunks.get(i);
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
   * @param mappersToDelete List of mapper entries to delete
   * @param chunksToDelete  List of chunk entries to delete
   * @return Total number of entries deleted
   */
  private int deleteCheckpointEntries(Connection conn, String tableName, String targetZkQuorum,
    List<PhoenixSyncTableOutputRow> mappersToDelete, List<PhoenixSyncTableOutputRow> chunksToDelete)
    throws SQLException {
    int deletedCount = 0;

    // Delete mapper entries
    for (PhoenixSyncTableOutputRow mapper : mappersToDelete) {
      deletedCount += deleteSingleCheckpointEntry(conn, tableName, targetZkQuorum,
        PhoenixSyncTableOutputRow.Type.MAPPER_REGION, mapper.getStartRowKey(), false);
    }

    // Delete chunk entries
    for (PhoenixSyncTableOutputRow chunk : chunksToDelete) {
      deletedCount += deleteSingleCheckpointEntry(conn, tableName, targetZkQuorum,
        PhoenixSyncTableOutputRow.Type.CHUNK, chunk.getStartRowKey(), false);
    }

    conn.commit();
    return deletedCount;
  }

  /**
   * Initiates merge of adjacent regions in a table. Merges happen asynchronously in background.
   */
  private void mergeAdjacentRegions(Connection conn, String tableName, int mergeCount) {
    try {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTable(tableName);
      TableName hbaseTableName = TableName.valueOf(table.getPhysicalName().getBytes());
      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        // Get current regions
        List<RegionInfo> regions = admin.getRegions(hbaseTableName);
        LOGGER.info("Table {} has {} regions before merge", tableName, regions.size());

        // Merge adjacent region pairs
        int mergedCount = 0;
        for (int i = 0; i < regions.size() - 1 && mergedCount < mergeCount; i++) {
          try {
            RegionInfo region1 = regions.get(i);
            RegionInfo region2 = regions.get(i + 1);

            LOGGER.info("Initiating merge of regions {} and {}", region1.getEncodedName(),
              region2.getEncodedName());
            // Merge regions asynchronously
            admin.mergeRegionsAsync(region1.getEncodedNameAsBytes(),
              region2.getEncodedNameAsBytes(), false);
            mergedCount++;
            i++; // Skip next region since it's being merged
          } catch (Exception e) {
            LOGGER.warn("Failed to merge regions: {}", e.getMessage());
          }
        }

        LOGGER.info("Initiated {} region merges for table {}", mergedCount, tableName);
        // Wait a bit for merges to start processing
        Thread.sleep(1000);
        // Get updated region count
        List<RegionInfo> regionsAfter = admin.getRegions(hbaseTableName);
        LOGGER.info("Table {} has {} regions after merge attempts", tableName, regionsAfter.size());
      }
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
   * Converts range to list and delegates to core method.
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
   * Core method: Inserts test data for specific list of IDs with given timestamp.
   * All other insertTestData overloads delegate to this method.
   */
  private void insertTestData(Connection conn, String tableName, List<Integer> ids,
    long timestamp) throws SQLException {
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
   * Attempts to split a table at the specified row ID using HBase Admin API. Ignores errors if the
   * split fails (e.g., region in transition).
   */
  private void splitTableAt(Connection conn, String tableName, int splitId) {
    try {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTable(tableName);
      TableName hbaseTableName = TableName.valueOf(table.getPhysicalName().getBytes());

      byte[] splitPoint = PInteger.INSTANCE.toBytes(splitId);

      // Attempt to split the region at the specified row key
      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        admin.split(hbaseTableName, splitPoint);
        LOGGER.info("Split initiated for table {} at split point {} (bytes: {})", tableName,
          splitId, Bytes.toStringBinary(splitPoint));
      }
      Thread.sleep(1500);
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
  private List<PhoenixSyncTableOutputRow> queryCheckpointTable(Connection conn, String tableName,
    String targetCluster) throws SQLException {
    List<PhoenixSyncTableOutputRow> entries = new ArrayList<>();
    String query = "SELECT TABLE_NAME, TARGET_CLUSTER, TYPE, FROM_TIME, TO_TIME, IS_DRY_RUN, "
      + "START_ROW_KEY, END_ROW_KEY, EXECUTION_START_TIME, EXECUTION_END_TIME, "
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
        .setEndRowKey(rs.getBytes("END_ROW_KEY"))
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
   * Deletes checkpoint entries for specific mapper and chunk row keys. Handles NULL/empty start
   * keys for first region boundaries.
   */
  private int deleteCheckpointEntry(Connection conn, String tableName, String targetCluster,
    byte[] mapperStartRowKey, byte[] chunkStartRowKey) throws SQLException {
    int totalDeleted = 0;

    // Delete mapper entry (without type filter)
    totalDeleted +=
      deleteSingleCheckpointEntry(conn, tableName, targetCluster, null, mapperStartRowKey, false);

    // Delete chunk entry (without type filter)
    totalDeleted +=
      deleteSingleCheckpointEntry(conn, tableName, targetCluster, null, chunkStartRowKey, false);

    conn.commit();
    return totalDeleted;
  }

  /**
   * Unified method to delete a single checkpoint entry by start row key and optional type. Handles
   * NULL/empty start keys for first region boundaries.
   * @param conn          Connection to use
   * @param tableName     Table name
   * @param targetCluster Target cluster ZK quorum
   * @param type          Entry type (MAPPER_REGION or CHUNK), or null to delete regardless of type
   * @param startRowKey   Start row key to match
   * @param autoCommit    Whether to commit after delete
   * @return Number of rows deleted
   */
  private int deleteSingleCheckpointEntry(Connection conn, String tableName, String targetCluster,
    PhoenixSyncTableOutputRow.Type type, byte[] startRowKey, boolean autoCommit)
    throws SQLException {
    StringBuilder deleteBuilder = new StringBuilder(
      "DELETE FROM PHOENIX_SYNC_TABLE_CHECKPOINT WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ?");

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
    return runSyncToolWithChunkSize(tableName, chunkSize, conf, additionalArgs);
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
   * Validates sync counters with exact source/target rows and minimum chunk thresholds.
   * Use this when chunk counts may vary but should be at least certain values.
   */
  private void validateSyncCountersExactSourceTarget(SyncCountersResult counters,
    long expectedSourceRows, long expectedTargetRows, long minChunksVerified,
    long minChunksMismatched) {
    assertEquals("Should process expected source rows", expectedSourceRows,
      counters.sourceRowsProcessed);
    assertEquals("Should process expected target rows", expectedTargetRows,
      counters.targetRowsProcessed);
    assertTrue(
      String.format("Should have at least %d verified chunks, actual: %d", minChunksVerified,
        counters.chunksVerified),
      counters.chunksVerified >= minChunksVerified);
    assertTrue(
      String.format("Should have at least %d mismatched chunks, actual: %d", minChunksMismatched,
        counters.chunksMismatched),
      counters.chunksMismatched >= minChunksMismatched);
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
