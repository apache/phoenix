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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.mapreduce.PhoenixSyncTableCheckpointOutputRow.Status;
import org.apache.phoenix.mapreduce.PhoenixSyncTableCheckpointOutputRow.Type;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Unit tests for PhoenixSyncTableOutputRepository and PhoenixSyncTableCheckpointOutputRow. Tests
 * checkpoint table operations and data model functionality.
 */
public class PhoenixSyncTableOutputRepositoryTest extends BaseTest {

  private Connection connection;
  private PhoenixSyncTableOutputRepository repository;
  private String targetCluster;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @AfterClass
  public static synchronized void freeResources() throws Exception {
    BaseTest.freeResourcesIfBeyondThreshold();
  }

  @Before
  public void setup() throws Exception {
    Properties props = new Properties();
    connection = DriverManager.getConnection(getUrl(), props);
    repository = new PhoenixSyncTableOutputRepository(connection);
    repository.createSyncCheckpointTableIfNotExists();
    targetCluster = "target-zk1,target-zk2:2181:/hbase";
  }

  @After
  public void tearDown() throws Exception {
    if (connection != null) {
      try {
        connection.createStatement().execute("DROP TABLE IF EXISTS "
          + PhoenixSyncTableOutputRepository.SYNC_TABLE_CHECKPOINT_TABLE_NAME);
        connection.commit();
        connection.close();
      } catch (SQLException e) {
        // Ignore cleanup errors
      }
    }
  }

  @Test
  public void testCreateSyncCheckpointTableIfNotExists() throws Exception {
    // Table was already created in @Before, verify it exists
    String query =
      "SELECT COUNT(*) FROM " + PhoenixSyncTableOutputRepository.SYNC_TABLE_CHECKPOINT_TABLE_NAME;
    try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      assertTrue("Table should exist and be queryable", rs.next());
    }
  }

  @Test
  public void testCreateSyncCheckpointTableIdempotent() throws Exception {
    // Create again - should not throw exception
    repository.createSyncCheckpointTableIfNotExists();

    // Verify table still exists
    String query =
      "SELECT COUNT(*) FROM " + PhoenixSyncTableOutputRepository.SYNC_TABLE_CHECKPOINT_TABLE_NAME;
    try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      assertTrue("Table should still exist after second create", rs.next());
    }
  }

  @Test
  public void testCheckpointMapperRegionVerified() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .setCounters("SOURCE_ROWS_PROCESSED=10,TARGET_ROWS_PROCESSED=10").build());

    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);

    assertEquals(1, results.size());
    assertArrayEquals(startKey, results.get(0).getStartRowKey());
    assertArrayEquals(endKey, results.get(0).getEndRowKey());
  }

  @Test
  public void testCheckpointChunkVerified() throws Exception {
    String tableName = generateUniqueName();
    byte[] chunkStart = Bytes.toBytes("row10");
    byte[] chunkEnd = Bytes.toBytes("row20");
    byte[] mapperStart = Bytes.toBytes("row1");
    byte[] mapperEnd = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunkStart).setEndRowKey(chunkEnd)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    List<PhoenixSyncTableCheckpointOutputRow> results = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, null, mapperStart, mapperEnd);

    assertFalse("Should find chunk within mapper region", results.isEmpty());
  }

  @Test
  public void testCheckpointWithEmptyStartKey() throws Exception {
    String tableName = generateUniqueName();
    byte[] emptyStartKey = new byte[0];
    byte[] endKey1 = Bytes.toBytes("row50");
    byte[] startKey2 = Bytes.toBytes("row50");
    byte[] endKey2 = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert second region first, then first region (empty start key)
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey2).setEndRowKey(endKey2)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(emptyStartKey).setEndRowKey(endKey1)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);

    assertEquals(2, results.size());
    // Phoenix returns null for empty byte arrays in primary key columns
    // Verify ORDER BY: empty/null key should come first
    byte[] retrievedStartKey = results.get(0).getStartRowKey();
    assertTrue("First result should have empty/null start key for first region",
      retrievedStartKey == null || retrievedStartKey.length == 0);
    assertArrayEquals("Second result should be row50", startKey2, results.get(1).getStartRowKey());
  }

  @Test
  public void testCheckpointWithNullEndKey() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setStatus(Status.VERIFIED)
      .setExecutionStartTime(timestamp).setExecutionEndTime(timestamp).build());

    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);

    assertEquals(1, results.size());
  }

  @Test
  public void testCheckpointWithCounters() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String counters = "SOURCE_ROWS_PROCESSED=100,TARGET_ROWS_PROCESSED=95";

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey)
      .setStatus(Status.MISMATCHED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .setCounters(counters).build());

    // Verify by querying directly
    String query = "SELECT COUNTERS FROM "
      + PhoenixSyncTableOutputRepository.SYNC_TABLE_CHECKPOINT_TABLE_NAME + " WHERE TABLE_NAME = ?";
    try (java.sql.PreparedStatement ps = connection.prepareStatement(query)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(counters, rs.getString("COUNTERS"));
      }
    }
  }

  @Test
  public void testCheckpointValidationNullTableName() throws Exception {
    byte[] startKey = Bytes.toBytes("row1");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    try {
      repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
        .setTableName(null).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
        .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(startKey)
        .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
        .build());
      fail("Should throw IllegalArgumentException for null tableName");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("TableName cannot be null"));
    }
  }

  @Test
  public void testCheckpointValidationNullTargetCluster() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    try {
      repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
        .setTableName(tableName).setTargetCluster(null).setType(Type.REGION).setFromTime(0L)
        .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(startKey)
        .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
        .build());
      fail("Should throw IllegalArgumentException for null targetCluster");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("TargetCluster cannot be null"));
    }
  }

  @Test
  public void testCheckpointValidationNullType() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    try {
      repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
        .setTableName(tableName).setTargetCluster(targetCluster).setType(null).setFromTime(0L)
        .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(startKey)
        .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
        .build());
      fail("Should throw IllegalArgumentException for null type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Type cannot be null"));
    }
  }

  @Test
  public void testCheckpointValidationNullTimeRange() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    try {
      repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
        .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION)
        .setFromTime(null).setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey)
        .setEndRowKey(startKey).setStatus(Status.VERIFIED).setExecutionStartTime(timestamp)
        .setExecutionEndTime(timestamp).build());
      fail("Should throw IllegalArgumentException for null fromTime");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("FromTime and ToTime cannot be null"));
    }
  }

  @Test
  public void testCheckpointUpsertBehavior() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp1 = new Timestamp(System.currentTimeMillis());
    Timestamp timestamp2 = new Timestamp(System.currentTimeMillis() + 1000);

    // Insert first time
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp1).setExecutionEndTime(timestamp1)
      .setCounters("counter=1").build());

    // Upsert with same PK but different values
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey)
      .setStatus(Status.MISMATCHED).setExecutionStartTime(timestamp2)
      .setExecutionEndTime(timestamp2).setCounters("counter=2").build());

    // Verify only one row exists with updated values
    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);

    assertEquals("Should have only one row after upsert", 1, results.size());
  }

  @Test
  public void testGetProcessedMapperRegionsEmpty() throws Exception {
    String tableName = generateUniqueName();

    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);

    assertEquals(0, results.size());
  }

  @Test
  public void testGetProcessedMapperRegionsBoth() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey1 = Bytes.toBytes("row1");
    byte[] endKey1 = Bytes.toBytes("row50");
    byte[] startKey2 = Bytes.toBytes("row50");
    byte[] endKey2 = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert in reverse order to verify ORDER BY clause
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey2).setEndRowKey(endKey2)
      .setStatus(Status.MISMATCHED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey1).setEndRowKey(endKey1)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);

    assertEquals(2, results.size());
    // Verify ORDER BY START_ROW_KEY: row1 should come before row50
    assertArrayEquals("First result should be row1", startKey1, results.get(0).getStartRowKey());
    assertArrayEquals("Second result should be row50", startKey2, results.get(1).getStartRowKey());
  }

  @Test
  public void testGetProcessedMapperRegionsFiltersChunks() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert mapper region
    repository.checkpointSyncTableResult(
      new PhoenixSyncTableCheckpointOutputRow.Builder().setTableName(tableName)
        .setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L).setToTime(1000L)
        .setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey).setStatus(Status.VERIFIED)
        .setExecutionStartTime(timestamp).setExecutionEndTime(timestamp).build());

    // Insert chunk
    repository.checkpointSyncTableResult(
      new PhoenixSyncTableCheckpointOutputRow.Builder().setTableName(tableName)
        .setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L).setToTime(1000L)
        .setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey).setStatus(Status.VERIFIED)
        .setExecutionStartTime(timestamp).setExecutionEndTime(timestamp).build());

    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);

    assertEquals("Should only return REGION entries", 1, results.size());
  }

  @Test
  public void testGetProcessedMapperRegionsFiltersTimeRange() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert with time range 0-1000
    repository.checkpointSyncTableResult(
      new PhoenixSyncTableCheckpointOutputRow.Builder().setTableName(tableName)
        .setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L).setToTime(1000L)
        .setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey).setStatus(Status.VERIFIED)
        .setExecutionStartTime(timestamp).setExecutionEndTime(timestamp).build());

    // Query with different time range
    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 2000L, 3000L, null);

    assertEquals("Should not find entry with different time range", 0, results.size());
  }

  @Test
  public void testGetProcessedChunksEmpty() throws Exception {
    String tableName = generateUniqueName();
    byte[] mapperStart = Bytes.toBytes("row1");
    byte[] mapperEnd = Bytes.toBytes("row100");

    List<PhoenixSyncTableCheckpointOutputRow> results = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, null, mapperStart, mapperEnd);

    assertEquals(0, results.size());
  }

  @Test
  public void testGetProcessedChunksBothStatuses() throws Exception {
    String tableName = generateUniqueName();
    byte[] chunk1Start = Bytes.toBytes("row10");
    byte[] chunk1End = Bytes.toBytes("row20");
    byte[] chunk2Start = Bytes.toBytes("row30");
    byte[] chunk2End = Bytes.toBytes("row40");
    byte[] mapperStart = Bytes.toBytes("row1");
    byte[] mapperEnd = Bytes.toBytes("row99");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert in reverse order to verify ORDER BY clause
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk2Start).setEndRowKey(chunk2End)
      .setStatus(Status.MISMATCHED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk1Start).setEndRowKey(chunk1End)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    List<PhoenixSyncTableCheckpointOutputRow> results = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, null, mapperStart, mapperEnd);

    assertEquals(2, results.size());
    // Verify ORDER BY START_ROW_KEY: row10 should come before row30
    assertArrayEquals("First result should be row10", chunk1Start, results.get(0).getStartRowKey());
    assertArrayEquals("Second result should be row30", chunk2Start,
      results.get(1).getStartRowKey());
  }

  @Test
  public void testGetProcessedChunksFiltersMapperRegions() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row10");
    byte[] endKey = Bytes.toBytes("row20");
    byte[] mapperStart = Bytes.toBytes("row1");
    byte[] mapperEnd = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert mapper region
    repository.checkpointSyncTableResult(
      new PhoenixSyncTableCheckpointOutputRow.Builder().setTableName(tableName)
        .setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L).setToTime(1000L)
        .setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey).setStatus(Status.VERIFIED)
        .setExecutionStartTime(timestamp).setExecutionEndTime(timestamp).build());

    // Insert chunk
    repository.checkpointSyncTableResult(
      new PhoenixSyncTableCheckpointOutputRow.Builder().setTableName(tableName)
        .setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L).setToTime(1000L)
        .setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey).setStatus(Status.VERIFIED)
        .setExecutionStartTime(timestamp).setExecutionEndTime(timestamp).build());

    List<PhoenixSyncTableCheckpointOutputRow> results = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, null, mapperStart, mapperEnd);

    assertEquals("Should only return CHUNK entries", 1, results.size());
  }

  @Test
  public void testGetProcessedChunksWithNoBoundaries() throws Exception {
    String tableName = generateUniqueName();
    byte[] chunkStart = Bytes.toBytes("row50");
    byte[] chunkEnd = Bytes.toBytes("row60");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunkStart).setEndRowKey(chunkEnd)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    // Query with no boundaries (entire table)
    List<PhoenixSyncTableCheckpointOutputRow> results =
      repository.getProcessedChunks(tableName, targetCluster, 0L, 1000L, null, null, null);

    assertEquals(1, results.size());
  }

  @Test
  public void testGetProcessedChunksWithOnlyEndBoundary() throws Exception {
    String tableName = generateUniqueName();
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert chunks at different positions
    byte[] chunk1Start = Bytes.toBytes("row10");
    byte[] chunk1End = Bytes.toBytes("row20");
    byte[] chunk2Start = Bytes.toBytes("row30");
    byte[] chunk2End = Bytes.toBytes("row40");
    byte[] chunk3Start = Bytes.toBytes("row60");
    byte[] chunk3End = Bytes.toBytes("row70");

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk1Start).setEndRowKey(chunk1End)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk2Start).setEndRowKey(chunk2End)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk3Start).setEndRowKey(chunk3End)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    // Query with only end boundary (first region of table: empty start, non-empty end)
    // This tests the code path where hasEndBoundary=true, hasStartBoundary=false
    // SQL should add: START_ROW_KEY <= ? (but not END_ROW_KEY >= ?)
    byte[] mapperStart = new byte[0]; // Empty start key (first region)
    byte[] mapperEnd = Bytes.toBytes("row50");

    List<PhoenixSyncTableCheckpointOutputRow> results = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, null, mapperStart, mapperEnd);

    // Should return chunk1 and chunk2 (both have startKey <= row50)
    // Should NOT return chunk3 (startKey row60 > row50)
    assertEquals("Should return chunks with startKey <= mapperEnd", 2, results.size());
    assertArrayEquals("First chunk should be row10", chunk1Start, results.get(0).getStartRowKey());
    assertArrayEquals("Second chunk should be row30", chunk2Start, results.get(1).getStartRowKey());
  }

  @Test
  public void testGetProcessedChunksWithOnlyStartBoundary() throws Exception {
    String tableName = generateUniqueName();
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert chunks at different positions
    byte[] chunk1Start = Bytes.toBytes("row10");
    byte[] chunk1End = Bytes.toBytes("row20");
    byte[] chunk2Start = Bytes.toBytes("row40");
    byte[] chunk2End = Bytes.toBytes("row50");
    byte[] chunk3Start = Bytes.toBytes("row60");
    byte[] chunk3End = Bytes.toBytes("row70");

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk1Start).setEndRowKey(chunk1End)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk2Start).setEndRowKey(chunk2End)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunk3Start).setEndRowKey(chunk3End)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    // Query with only start boundary (last region of table: non-empty start, empty end)
    // This tests the code path where hasEndBoundary=false, hasStartBoundary=true
    // SQL should add: END_ROW_KEY >= ? (but not START_ROW_KEY <= ?)
    byte[] mapperStart = Bytes.toBytes("row30");
    byte[] mapperEnd = new byte[0]; // Empty end key (last region)

    List<PhoenixSyncTableCheckpointOutputRow> results = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, null, mapperStart, mapperEnd);

    // Should return chunk2 and chunk3 (both have endKey >= row30)
    // Should NOT return chunk1 (endKey row20 < row30)
    assertEquals("Should return chunks with endKey >= mapperStart", 2, results.size());
    assertArrayEquals("First chunk should be row40", chunk2Start, results.get(0).getStartRowKey());
    assertArrayEquals("Second chunk should be row60", chunk3Start, results.get(1).getStartRowKey());
  }

  @Test
  public void testBuilderAllFields() {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("start");
    byte[] endKey = Bytes.toBytes("end");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    PhoenixSyncTableCheckpointOutputRow row = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey)
      .setExecutionStartTime(timestamp).setExecutionEndTime(timestamp).setStatus(Status.VERIFIED)
      .setCounters("SOURCE_ROWS_PROCESSED=100").build();

    assertEquals(tableName, row.getTableName());
    assertEquals(targetCluster, row.getTargetCluster());
    assertEquals(Type.CHUNK, row.getType());
    assertEquals(Long.valueOf(0L), row.getFromTime());
    assertEquals(Long.valueOf(1000L), row.getToTime());
    assertArrayEquals(startKey, row.getStartRowKey());
    assertArrayEquals(endKey, row.getEndRowKey());
    assertEquals(Status.VERIFIED, row.getStatus());
  }

  @Test
  public void testBuilderMinimalFields() {
    byte[] startKey = Bytes.toBytes("start");

    PhoenixSyncTableCheckpointOutputRow row =
      new PhoenixSyncTableCheckpointOutputRow.Builder().setStartRowKey(startKey).build();

    assertNotNull(row);
    assertArrayEquals(startKey, row.getStartRowKey());
  }

  @Test
  public void testGetStartRowKeyDefensiveCopy() {
    byte[] startKey = Bytes.toBytes("start");

    PhoenixSyncTableCheckpointOutputRow row =
      new PhoenixSyncTableCheckpointOutputRow.Builder().setStartRowKey(startKey).build();

    byte[] retrieved = row.getStartRowKey();
    assertNotSame("Should return a copy, not the original", startKey, retrieved);

    // Modify retrieved array
    retrieved[0] = (byte) 0xFF;

    // Get again and verify it's unchanged
    byte[] retrievedAgain = row.getStartRowKey();
    assertNotEquals("Internal array should not be modified", (byte) 0xFF, retrievedAgain[0]);
  }

  @Test
  public void testEqualsDifferentObjectSameValues() {
    byte[] startKey = Bytes.toBytes("start");
    byte[] endKey = Bytes.toBytes("end");

    PhoenixSyncTableCheckpointOutputRow row1 = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName("table1").setTargetCluster(targetCluster).setType(Type.CHUNK)
      .setStartRowKey(startKey).setEndRowKey(endKey).build();

    PhoenixSyncTableCheckpointOutputRow row2 = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName("table1").setTargetCluster(targetCluster).setType(Type.CHUNK)
      .setStartRowKey(startKey).setEndRowKey(endKey).build();

    assertEquals(row1, row2);
    assertEquals(row1.hashCode(), row2.hashCode());
  }

  @Test
  public void testEqualsDifferentValues() {
    PhoenixSyncTableCheckpointOutputRow row1 = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName("table1").setStartRowKey(Bytes.toBytes("start1")).build();

    PhoenixSyncTableCheckpointOutputRow row2 = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName("table2").setStartRowKey(Bytes.toBytes("start2")).build();

    assertNotEquals(row1, row2);
  }

  @Test
  public void testEqualsWithByteArrays() {
    byte[] startKey1 = Bytes.toBytes("start");
    byte[] startKey2 = Bytes.toBytes("start"); // Same content, different object

    PhoenixSyncTableCheckpointOutputRow row1 =
      new PhoenixSyncTableCheckpointOutputRow.Builder().setStartRowKey(startKey1).build();

    PhoenixSyncTableCheckpointOutputRow row2 =
      new PhoenixSyncTableCheckpointOutputRow.Builder().setStartRowKey(startKey2).build();

    assertEquals("Byte arrays with same content should be equal", row1, row2);
  }

  @Test
  public void testParseCounterValueSingle() {
    PhoenixSyncTableCheckpointOutputRow row = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setStartRowKey(Bytes.toBytes("start")).setCounters("SOURCE_ROWS_PROCESSED=100").build();

    assertEquals(100L, row.getSourceRowsProcessed());
  }

  @Test
  public void testParseCounterValueMultiple() {
    PhoenixSyncTableCheckpointOutputRow row = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setStartRowKey(Bytes.toBytes("start"))
      .setCounters("SOURCE_ROWS_PROCESSED=100,TARGET_ROWS_PROCESSED=95,CHUNKS_VERIFIED=10").build();

    assertEquals(100L, row.getSourceRowsProcessed());
    assertEquals(95L, row.getTargetRowsProcessed());
  }

  @Test
  public void testParseCounterValueNull() {
    PhoenixSyncTableCheckpointOutputRow row = new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setStartRowKey(Bytes.toBytes("start")).setCounters(null).build();

    assertEquals(0L, row.getSourceRowsProcessed());
    assertEquals(0L, row.getTargetRowsProcessed());
  }

  @Test
  public void testCheckpointMapperRegionWithTenantId() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String tenantId1 = "TENANT_001";
    String tenantId2 = "TENANT_002";

    // Insert checkpoint for TENANT_001
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setTenantId(tenantId1).setIsDryRun(false).setStartRowKey(startKey)
      .setEndRowKey(endKey).setStatus(Status.VERIFIED).setExecutionStartTime(timestamp)
      .setExecutionEndTime(timestamp).setCounters("counter=1").build());

    // Insert checkpoint for TENANT_002 with same table/target/time but different tenant
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setTenantId(tenantId2).setIsDryRun(false).setStartRowKey(startKey)
      .setEndRowKey(endKey).setStatus(Status.VERIFIED).setExecutionStartTime(timestamp)
      .setExecutionEndTime(timestamp).setCounters("counter=2").build());

    // Insert checkpoint for null tenant (non-multi-tenant table)
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.REGION).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .setCounters("counter=3").build());

    // Query for TENANT_001 - should return only TENANT_001's checkpoint
    List<PhoenixSyncTableCheckpointOutputRow> results1 =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, tenantId1);
    assertEquals("TENANT_001 should have 1 checkpoint", 1, results1.size());

    // Query for TENANT_002 - should return only TENANT_002's checkpoint
    List<PhoenixSyncTableCheckpointOutputRow> results2 =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, tenantId2);
    assertEquals("TENANT_002 should have 1 checkpoint", 1, results2.size());

    // Query for null tenant - should return only null-tenant checkpoint (tenant isolation)
    List<PhoenixSyncTableCheckpointOutputRow> results3 =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L, null);
    assertEquals("Null tenant query should return only null-tenant checkpoint", 1, results3.size());
  }

  @Test
  public void testChunkCheckpointChunkWithDifferentTenants() throws Exception {
    String tableName = generateUniqueName();
    byte[] chunkStart = Bytes.toBytes("row10");
    byte[] chunkEnd = Bytes.toBytes("row20");
    byte[] mapperStart = Bytes.toBytes("row1");
    byte[] mapperEnd = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String tenantId1 = "TENANT_001";
    String tenantId2 = "TENANT_002";

    // Insert chunk checkpoint for TENANT_001
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setTenantId(tenantId1).setIsDryRun(false).setStartRowKey(chunkStart)
      .setEndRowKey(chunkEnd).setStatus(Status.VERIFIED).setExecutionStartTime(timestamp)
      .setExecutionEndTime(timestamp).build());

    // Insert chunk checkpoint for TENANT_002
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setTenantId(tenantId2).setIsDryRun(false).setStartRowKey(chunkStart)
      .setEndRowKey(chunkEnd).setStatus(Status.MISMATCHED).setExecutionStartTime(timestamp)
      .setExecutionEndTime(timestamp).build());

    // Insert chunk checkpoint for null tenant (non-multi-tenant table)
    repository.checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L)
      .setToTime(1000L).setIsDryRun(false).setStartRowKey(chunkStart).setEndRowKey(chunkEnd)
      .setStatus(Status.VERIFIED).setExecutionStartTime(timestamp).setExecutionEndTime(timestamp)
      .build());

    // Query for TENANT_001 chunks
    List<PhoenixSyncTableCheckpointOutputRow> results1 = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, tenantId1, mapperStart, mapperEnd);
    assertEquals("TENANT_001 should have 1 chunk", 1, results1.size());

    // Query for TENANT_002 chunks
    List<PhoenixSyncTableCheckpointOutputRow> results2 = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, tenantId2, mapperStart, mapperEnd);
    assertEquals("TENANT_002 should have 1 chunk", 1, results2.size());

    // Query for null tenant - should return only null-tenant chunk (tenant isolation)
    List<PhoenixSyncTableCheckpointOutputRow> results3 = repository.getProcessedChunks(tableName,
      targetCluster, 0L, 1000L, null, mapperStart, mapperEnd);
    assertEquals("Null tenant query should return only null-tenant chunk", 1, results3.size());
  }
}
