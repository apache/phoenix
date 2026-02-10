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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRow.Status;
import org.apache.phoenix.mapreduce.PhoenixSyncTableOutputRow.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for managing the PHOENIX_SYNC_TABLE_OUTPUT table. This table stores checkpoint
 * information for the PhoenixSyncTableTool, enabling: 1. Job-level checkpointing (skip completed
 * mapper regions on restart) 2. Mapper-level checkpointing (skip completed chunks within a region)
 * 3. Audit trail of all sync operations
 */
public class PhoenixSyncTableOutputRepository {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(PhoenixSyncTableOutputRepository.class);
  public static final String SYNC_TABLE_CHECKPOINT_TABLE_NAME = "PHOENIX_SYNC_TABLE_CHECKPOINT";
  private static final int OUTPUT_TABLE_TTL_SECONDS = 30 * 24 * 60 * 60;
  private final Connection connection;
  private static final byte[] EMPTY_START_KEY_SENTINEL = new byte[] { 0x00 };

  /**
   * Constructor
   * @param connection Phoenix connection
   */
  public PhoenixSyncTableOutputRepository(Connection connection) {
    this.connection = connection;
  }

  /**
   * Creates the PHOENIX_SYNC_TABLE_OUTPUT table if it doesn't exist. Table schema: - Primary key:
   * (TABLE_NAME, TARGET_CLUSTER, FROM_TIME, TO_TIME, ENTRY_TYPE, START_ROW_KEY, END_ROW_KEY) - TTL:
   * 30 days - Salt buckets: 4 (for better distribution)
   */
  public void createOutputTableIfNotExists() throws SQLException {
    String ddl = "CREATE TABLE IF NOT EXISTS " + SYNC_TABLE_CHECKPOINT_TABLE_NAME + " (\n"
      + "    TABLE_NAME VARCHAR NOT NULL,\n" + "    TARGET_CLUSTER VARCHAR NOT NULL,\n"
      + "    ENTRY_TYPE VARCHAR(20) NOT NULL,\n" + "    FROM_TIME BIGINT NOT NULL,\n"
      + "    TO_TIME BIGINT NOT NULL,\n" + "    IS_DRY_RUN BOOLEAN NOT NULL,\n"
      + "    START_ROW_KEY VARBINARY NOT NULL,\n" + "    END_ROW_KEY VARBINARY,\n"
      + "    IS_FIRST_REGION BOOLEAN, \n" + "    EXECUTION_START_TIME TIMESTAMP,\n"
      + "    EXECUTION_END_TIME TIMESTAMP,\n" + "    STATUS VARCHAR(20),\n"
      + "    COUNTERS VARCHAR(255), \n" + "    CONSTRAINT PK PRIMARY KEY (\n"
      + "        TABLE_NAME,\n" + "        TARGET_CLUSTER,\n" + "        ENTRY_TYPE ,\n"
      + "        FROM_TIME,\n" + "        TO_TIME,\n" + "        IS_DRY_RUN,\n"
      + "        START_ROW_KEY )" + ") TTL=" + OUTPUT_TABLE_TTL_SECONDS;

    try (Statement stmt = connection.createStatement()) {
      stmt.execute(ddl);
      connection.commit();
      LOGGER.info("Successfully created or verified existence of {} table",
        SYNC_TABLE_CHECKPOINT_TABLE_NAME);
    }
  }

  /**
   * Logs a chunk-level sync result to the output table.
   * @param tableName          Source table name
   * @param targetCluster      Target cluster ZK quorum
   * @param fromTime           Start timestamp for sync (nullable)
   * @param toTime             End timestamp for sync (nullable)
   * @param isDryRun           Whether this is a dry run
   * @param startKey           Chunk start row key
   * @param endKey             Chunk end row key
   * @param status             Sync status (IN_PROGRESS/VERIFIED)
   * @param executionStartTime When chunk processing started
   * @param executionEndTime   When chunk processing completed
   */
  // public void logChunkResult(String tableName, String targetCluster, Long fromTime, Long toTime,
  // boolean isDryRun, byte[] startKey, byte[] endKey, Status status,
  // Timestamp executionStartTime, Timestamp executionEndTime)
  // throws SQLException {
  //
  // checkpointSyncTableResult(tableName, targetCluster, Type.CHUNK, fromTime, toTime, isDryRun,
  // startKey, endKey, status, executionStartTime, executionEndTime);
  // }

  /**
   * Logs a mapper region completion to the output table. This indicates that all chunks within the
   * region have been processed.
   * @param tableName          Source table name
   * @param targetCluster      Target cluster ZK quorum
   * @param fromTime           Start timestamp for sync (nullable)
   * @param toTime             End timestamp for sync (nullable)
   * @param isDryRun           Whether this is a dry run
   * @param regionStart        Mapper region start row key
   * @param regionEnd          Mapper region end row key
   * @param status             Overall status for the region
   * @param executionStartTime When mapper started processing this region
   * @param executionEndTime   When mapper finished processing this region
   */
  // public void logMapperRegionResult(String tableName, String targetCluster, Long fromTime,
  // Long toTime, boolean isDryRun, byte[] regionStart, byte[] regionEnd, Status status,
  // Timestamp executionStartTime, Timestamp executionEndTime)
  // throws SQLException {
  //
  // checkpointSyncTableResult(tableName, targetCluster, Type.MAPPER_REGION, fromTime, toTime,
  // isDryRun,
  // regionStart, regionEnd, status, executionStartTime, executionEndTime);
  // }

  /**
   * Internal method to log sync results to the output table
   */
  public void checkpointSyncTableResult(String tableName, String targetCluster, Type type,
    Long fromTime, Long toTime, boolean isDryRun, byte[] startKey, byte[] endKey, Status status,
    Timestamp executionStartTime, Timestamp executionEndTime, String counters) throws SQLException {

    // Validate required parameters
    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException("TableName cannot be null or empty for checkpoint");
    }
    if (targetCluster == null || targetCluster.isEmpty()) {
      throw new IllegalArgumentException("TargetCluster cannot be null or empty for checkpoint");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null for checkpoint");
    }
    if (fromTime == null || toTime == null) {
      throw new IllegalArgumentException("FromTime and ToTime cannot be null for checkpoint");
    }

    String upsert = "UPSERT INTO " + SYNC_TABLE_CHECKPOINT_TABLE_NAME + " ("
      + "TABLE_NAME, TARGET_CLUSTER, ENTRY_TYPE, FROM_TIME, TO_TIME, IS_DRY_RUN, "
      + "START_ROW_KEY, END_ROW_KEY, IS_FIRST_REGION, EXECUTION_START_TIME, EXECUTION_END_TIME, "
      + "STATUS, COUNTERS) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    byte[] effectiveStartKey =
      (startKey == null || startKey.length == 0) ? EMPTY_START_KEY_SENTINEL : startKey;

    // Determine if this is the first region (empty start key)
    boolean isFirstRegion = (startKey == null || startKey.length == 0);

    try (PreparedStatement ps = connection.prepareStatement(upsert)) {
      ps.setString(1, tableName);
      ps.setString(2, targetCluster);
      ps.setString(3, type.name());
      ps.setObject(4, fromTime);
      ps.setObject(5, toTime);
      ps.setBoolean(6, isDryRun);
      ps.setBytes(7, effectiveStartKey);
      ps.setBytes(8, endKey);
      ps.setBoolean(9, isFirstRegion);
      ps.setTimestamp(10, executionStartTime);
      ps.setTimestamp(11, executionEndTime);
      ps.setString(12, status != null ? status.name() : null);
      ps.setString(13, counters);
      ps.executeUpdate();
      connection.commit();
    }
  }

  /**
   * Helper to check if a key represents an empty boundary
   */
  // private boolean isEmptyBoundary(byte[] key) {
  // return key != null && key.length == 1 && Bytes.equals(key, HConstants.EMPTY_BYTE_ARRAY);
  // }

  /**
   * Converts stored key back to HBase empty key if needed. For first region, converts
   * EMPTY_START_KEY_SENTINEL back to HConstants.EMPTY_BYTE_ARRAY.
   */
  private byte[] toHBaseKey(byte[] storedKey, boolean isFirstRegion) {
    if (isFirstRegion && Arrays.equals(storedKey, EMPTY_START_KEY_SENTINEL)) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }
    return storedKey;
  }

  /**
   * Queries for completed mapper regions. Used by PhoenixSyncTableInputFormat to filter out
   * already-processed regions.
   * @param tableName     Source table name
   * @param targetCluster Target cluster ZK quorum
   * @param fromTime      Start timestamp (nullable)
   * @param toTime        End timestamp (nullable)
   * @return List of completed mapper regions
   */
  public List<PhoenixSyncTableOutputRow> getProcessedMapperRegions(String tableName,
    String targetCluster, Long fromTime, Long toTime) throws SQLException {

    String query = "SELECT START_ROW_KEY, END_ROW_KEY, IS_FIRST_REGION FROM "
      + SYNC_TABLE_CHECKPOINT_TABLE_NAME + " WHERE TABLE_NAME = ?  AND TARGET_CLUSTER = ?"
      + " AND ENTRY_TYPE = ? AND FROM_TIME = ? AND TO_TIME = ? AND STATUS IN ( ?, ?)"
      + " ORDER BY START_ROW_KEY ";
    List<PhoenixSyncTableOutputRow> results = new ArrayList<>();

    try (PreparedStatement ps = connection.prepareStatement(query)) {
      int paramIndex = 1;
      ps.setString(paramIndex++, tableName);
      ps.setString(paramIndex++, targetCluster);
      ps.setString(paramIndex++, Type.MAPPER_REGION.name());
      ps.setLong(paramIndex++, fromTime);
      ps.setLong(paramIndex++, toTime);
      ps.setString(paramIndex++, Status.VERIFIED.name());
      ps.setString(paramIndex, Status.MISMATCHED.name());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          boolean isFirstRegion = rs.getBoolean("IS_FIRST_REGION");
          PhoenixSyncTableOutputRow row = new PhoenixSyncTableOutputRow.Builder()
            .setStartRowKey(this.toHBaseKey(rs.getBytes("START_ROW_KEY"), isFirstRegion))
            .setEndRowKey(rs.getBytes("END_ROW_KEY")).build();
          results.add(row);
        }
      }
    }
    return results;
  }

  /**
   * Queries for processed chunks within a mapper region. Used by PhoenixSyncTableMapper to skip
   * already-processed chunks.
   * @param tableName         Source table name
   * @param targetCluster     Target cluster ZK quorum
   * @param fromTime          Start timestamp (nullable)
   * @param toTime            End timestamp (nullable)
   * @param mapperRegionStart Mapper region start key
   * @param mapperRegionEnd   Mapper region end key
   * @return List of processed chunks in the region
   */
  public List<PhoenixSyncTableOutputRow> getProcessedChunks(String tableName, String targetCluster,
    Long fromTime, Long toTime, byte[] mapperRegionStart, byte[] mapperRegionEnd)
    throws SQLException {
    String query = "SELECT START_ROW_KEY, END_ROW_KEY, IS_FIRST_REGION FROM "
      + SYNC_TABLE_CHECKPOINT_TABLE_NAME + " WHERE TABLE_NAME = ? " + "  AND TARGET_CLUSTER = ? "
      + "  " + " AND ENTRY_TYPE = ? AND FROM_TIME = ? AND TO_TIME = ? AND START_ROW_KEY < ? "
      + " AND END_ROW_KEY > ? AND STATUS IN (?, ?)" + " ORDER BY START_ROW_KEY";

    List<PhoenixSyncTableOutputRow> results = new ArrayList<>();
    try (PreparedStatement ps = connection.prepareStatement(query)) {
      int paramIndex = 1;
      ps.setString(paramIndex++, tableName);
      ps.setString(paramIndex++, targetCluster);
      ps.setString(paramIndex++, Type.CHUNK.name());
      ps.setLong(paramIndex++, fromTime);
      ps.setLong(paramIndex++, toTime);
      ps.setBytes(paramIndex++, mapperRegionEnd);
      ps.setBytes(paramIndex++, mapperRegionStart);
      ps.setString(paramIndex++, Status.VERIFIED.name());
      ps.setString(paramIndex, Status.MISMATCHED.name());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          PhoenixSyncTableOutputRow row = new PhoenixSyncTableOutputRow.Builder()
            .setStartRowKey(
              this.toHBaseKey(rs.getBytes("START_ROW_KEY"), rs.getBoolean("IS_FIRST_REGION")))
            .setEndRowKey(rs.getBytes("END_ROW_KEY")).build();
          results.add(row);
        }
      }
    }
    return results;
  }

  // /**
  // * For testing: clears all entries for a given table
  // */
  // @VisibleForTesting
  // public void deleteEntriesForTable(String tableName) throws SQLException {
  // String delete = "DELETE FROM " + OUTPUT_TABLE_NAME + " WHERE TABLE_NAME = ?";
  // try (PreparedStatement ps = connection.prepareStatement(delete)) {
  // ps.setString(1, tableName);
  // ps.executeUpdate();
  // connection.commit();
  // }
  // }
}
