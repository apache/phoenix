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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRow.Status;
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRow.Type;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Unit tests for PhoenixSyncTableOutputRepository and PhoenixSyncTableOutputRow. Tests checkpoint
 * table operations and data model functionality.
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

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.VERIFIED, timestamp, timestamp,
      "SOURCE_ROWS_PROCESSED=10,TARGET_ROWS_PROCESSED=10");

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L);

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

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.CHUNK, 0L, 1000L, false,
      chunkStart, chunkEnd, Status.VERIFIED, timestamp, timestamp, null);

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedChunks(tableName, targetCluster, 0L, 1000L, mapperStart, mapperEnd);

    assertTrue("Should find chunk within mapper region", results.size() > 0);
  }

  @Test
  public void testCheckpointWithEmptyStartKey() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = new byte[0];
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Pass empty byte array as start key (first region)
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.VERIFIED, timestamp, timestamp, null);

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L);

    assertEquals(1, results.size());
    // Phoenix returns null for empty byte arrays in primary key columns
    byte[] retrievedStartKey = results.get(0).getStartRowKey();
    assertTrue("Start key should be null or empty for first region",
      retrievedStartKey == null || retrievedStartKey.length == 0);
  }

  @Test
  public void testCheckpointWithNullEndKey() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, null, Status.VERIFIED, timestamp, timestamp, null);

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L);

    assertEquals(1, results.size());
  }

  @Test
  public void testCheckpointWithCounters() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String counters = "SOURCE_ROWS_PROCESSED=100,TARGET_ROWS_PROCESSED=95";

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.MISMATCHED, timestamp, timestamp, counters);

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
      repository.checkpointSyncTableResult(null, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
        false, startKey, startKey, Status.VERIFIED, timestamp, timestamp, null);
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
      repository.checkpointSyncTableResult(tableName, null, Type.MAPPER_REGION, 0L, 1000L, false,
        startKey, startKey, Status.VERIFIED, timestamp, timestamp, null);
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
      repository.checkpointSyncTableResult(tableName, targetCluster, null, 0L, 1000L, false,
        startKey, startKey, Status.VERIFIED, timestamp, timestamp, null);
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
      repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, null,
        1000L, false, startKey, startKey, Status.VERIFIED, timestamp, timestamp, null);
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
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.VERIFIED, timestamp1, timestamp1, "counter=1");

    // Upsert with same PK but different values
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.MISMATCHED, timestamp2, timestamp2, "counter=2");

    // Verify only one row exists with updated values
    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L);

    assertEquals("Should have only one row after upsert", 1, results.size());
  }

  @Test
  public void testGetProcessedMapperRegionsEmpty() throws Exception {
    String tableName = generateUniqueName();

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L);

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

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey1, endKey1, Status.VERIFIED, timestamp, timestamp, null);

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey2, endKey2, Status.MISMATCHED, timestamp, timestamp, null);

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L);

    assertEquals(2, results.size());
  }

  @Test
  public void testGetProcessedMapperRegionsFiltersChunks() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert mapper region
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.VERIFIED, timestamp, timestamp, null);

    // Insert chunk
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.CHUNK, 0L, 1000L, false,
      startKey, endKey, Status.VERIFIED, timestamp, timestamp, null);

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 0L, 1000L);

    assertEquals("Should only return MAPPER_REGION entries", 1, results.size());
  }

  @Test
  public void testGetProcessedMapperRegionsFiltersTimeRange() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("row1");
    byte[] endKey = Bytes.toBytes("row100");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // Insert with time range 0-1000
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.VERIFIED, timestamp, timestamp, null);

    // Query with different time range
    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedMapperRegions(tableName, targetCluster, 2000L, 3000L);

    assertEquals("Should not find entry with different time range", 0, results.size());
  }

  @Test
  public void testGetProcessedChunksEmpty() throws Exception {
    String tableName = generateUniqueName();
    byte[] mapperStart = Bytes.toBytes("row1");
    byte[] mapperEnd = Bytes.toBytes("row100");

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedChunks(tableName, targetCluster, 0L, 1000L, mapperStart, mapperEnd);

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

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.CHUNK, 0L, 1000L, false,
      chunk1Start, chunk1End, Status.VERIFIED, timestamp, timestamp, null);

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.CHUNK, 0L, 1000L, false,
      chunk2Start, chunk2End, Status.MISMATCHED, timestamp, timestamp, null);

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedChunks(tableName, targetCluster, 0L, 1000L, mapperStart, mapperEnd);

    assertEquals(2, results.size());
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
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, 0L, 1000L,
      false, startKey, endKey, Status.VERIFIED, timestamp, timestamp, null);

    // Insert chunk
    repository.checkpointSyncTableResult(tableName, targetCluster, Type.CHUNK, 0L, 1000L, false,
      startKey, endKey, Status.VERIFIED, timestamp, timestamp, null);

    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedChunks(tableName, targetCluster, 0L, 1000L, mapperStart, mapperEnd);

    assertEquals("Should only return CHUNK entries", 1, results.size());
  }

  @Test
  public void testGetProcessedChunksWithNoBoundaries() throws Exception {
    String tableName = generateUniqueName();
    byte[] chunkStart = Bytes.toBytes("row50");
    byte[] chunkEnd = Bytes.toBytes("row60");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    repository.checkpointSyncTableResult(tableName, targetCluster, Type.CHUNK, 0L, 1000L, false,
      chunkStart, chunkEnd, Status.VERIFIED, timestamp, timestamp, null);

    // Query with no boundaries (entire table)
    List<PhoenixSyncTableOutputRow> results =
      repository.getProcessedChunks(tableName, targetCluster, 0L, 1000L, null, null);

    assertEquals(1, results.size());
  }

  @Test
  public void testBuilderAllFields() throws Exception {
    String tableName = generateUniqueName();
    byte[] startKey = Bytes.toBytes("start");
    byte[] endKey = Bytes.toBytes("end");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    PhoenixSyncTableOutputRow row = new PhoenixSyncTableOutputRow.Builder().setTableName(tableName)
      .setTargetCluster(targetCluster).setType(Type.CHUNK).setFromTime(0L).setToTime(1000L)
      .setIsDryRun(false).setStartRowKey(startKey).setEndRowKey(endKey)
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
  public void testBuilderMinimalFields() throws Exception {
    byte[] startKey = Bytes.toBytes("start");

    PhoenixSyncTableOutputRow row =
      new PhoenixSyncTableOutputRow.Builder().setStartRowKey(startKey).build();

    assertNotNull(row);
    assertArrayEquals(startKey, row.getStartRowKey());
  }

  @Test
  public void testGetStartRowKeyDefensiveCopy() throws Exception {
    byte[] startKey = Bytes.toBytes("start");

    PhoenixSyncTableOutputRow row =
      new PhoenixSyncTableOutputRow.Builder().setStartRowKey(startKey).build();

    byte[] retrieved = row.getStartRowKey();
    assertNotSame("Should return a copy, not the original", startKey, retrieved);

    // Modify retrieved array
    retrieved[0] = (byte) 0xFF;

    // Get again and verify it's unchanged
    byte[] retrievedAgain = row.getStartRowKey();
    assertFalse("Internal array should not be modified", retrievedAgain[0] == (byte) 0xFF);
  }

  @Test
  public void testEqualsDifferentObjectSameValues() throws Exception {
    byte[] startKey = Bytes.toBytes("start");
    byte[] endKey = Bytes.toBytes("end");

    PhoenixSyncTableOutputRow row1 =
      new PhoenixSyncTableOutputRow.Builder().setTableName("table1").setTargetCluster(targetCluster)
        .setType(Type.CHUNK).setStartRowKey(startKey).setEndRowKey(endKey).build();

    PhoenixSyncTableOutputRow row2 =
      new PhoenixSyncTableOutputRow.Builder().setTableName("table1").setTargetCluster(targetCluster)
        .setType(Type.CHUNK).setStartRowKey(startKey).setEndRowKey(endKey).build();

    assertTrue(row1.equals(row2));
    assertEquals(row1.hashCode(), row2.hashCode());
  }

  @Test
  public void testEqualsDifferentValues() throws Exception {
    PhoenixSyncTableOutputRow row1 = new PhoenixSyncTableOutputRow.Builder().setTableName("table1")
      .setStartRowKey(Bytes.toBytes("start1")).build();

    PhoenixSyncTableOutputRow row2 = new PhoenixSyncTableOutputRow.Builder().setTableName("table2")
      .setStartRowKey(Bytes.toBytes("start2")).build();

    assertFalse(row1.equals(row2));
  }

  @Test
  public void testEqualsWithByteArrays() throws Exception {
    byte[] startKey1 = Bytes.toBytes("start");
    byte[] startKey2 = Bytes.toBytes("start"); // Same content, different object

    PhoenixSyncTableOutputRow row1 =
      new PhoenixSyncTableOutputRow.Builder().setStartRowKey(startKey1).build();

    PhoenixSyncTableOutputRow row2 =
      new PhoenixSyncTableOutputRow.Builder().setStartRowKey(startKey2).build();

    assertTrue("Byte arrays with same content should be equal", row1.equals(row2));
  }

  @Test
  public void testParseCounterValueSingle() throws Exception {
    PhoenixSyncTableOutputRow row = new PhoenixSyncTableOutputRow.Builder()
      .setStartRowKey(Bytes.toBytes("start")).setCounters("SOURCE_ROWS_PROCESSED=100").build();

    assertEquals(100L, row.getSourceRowsProcessed());
  }

  @Test
  public void testParseCounterValueMultiple() throws Exception {
    PhoenixSyncTableOutputRow row = new PhoenixSyncTableOutputRow.Builder()
      .setStartRowKey(Bytes.toBytes("start"))
      .setCounters("SOURCE_ROWS_PROCESSED=100,TARGET_ROWS_PROCESSED=95,CHUNKS_VERIFIED=10").build();

    assertEquals(100L, row.getSourceRowsProcessed());
    assertEquals(95L, row.getTargetRowsProcessed());
  }

  @Test
  public void testParseCounterValueNull() throws Exception {
    PhoenixSyncTableOutputRow row = new PhoenixSyncTableOutputRow.Builder()
      .setStartRowKey(Bytes.toBytes("start")).setCounters(null).build();

    assertEquals(0L, row.getSourceRowsProcessed());
    assertEquals(0L, row.getTargetRowsProcessed());
  }
}
