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
import java.util.ArrayList;
import java.util.List;
import org.apache.phoenix.mapreduce.PhoenixSyncTableCheckpointOutputRow.Type;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Repository for managing the PHOENIX_SYNC_TABLE_CHECKPOINT table. This table stores checkpoint
 * information for the PhoenixSyncTableTool, enabling: 1. Mapper Level checkpointing (skip completed
 * mapper regions on restart) 2. Chunk level checkpointing (skip completed chunks)
 */
public class PhoenixSyncTableOutputRepository {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(PhoenixSyncTableOutputRepository.class);
  public static final String SYNC_TABLE_CHECKPOINT_TABLE_NAME = "PHOENIX_SYNC_TABLE_CHECKPOINT";
  private static final int OUTPUT_TABLE_TTL_SECONDS = 90 * 24 * 60 * 60; // 90 days
  private final Connection connection;
  private static final String UPSERT_CHECKPOINT_SQL = "UPSERT INTO "
    + SYNC_TABLE_CHECKPOINT_TABLE_NAME + " (TABLE_NAME, TARGET_CLUSTER, TYPE, FROM_TIME, TO_TIME,"
    + " TENANT_ID, START_ROW_KEY, END_ROW_KEY, IS_DRY_RUN, EXECUTION_START_TIME, EXECUTION_END_TIME,"
    + " STATUS, COUNTERS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String CREATE_CHECKPOINT_TABLE_DDL = "CREATE TABLE IF NOT EXISTS "
    + SYNC_TABLE_CHECKPOINT_TABLE_NAME + " (\n" + "    TABLE_NAME VARCHAR NOT NULL,\n"
    + "    TARGET_CLUSTER VARCHAR NOT NULL,\n" + "    TYPE VARCHAR(20) NOT NULL,\n"
    + "    FROM_TIME BIGINT NOT NULL,\n" + "    TO_TIME BIGINT NOT NULL,\n"
    + "    TENANT_ID VARCHAR,\n" + "    START_ROW_KEY VARBINARY_ENCODED,\n"
    + "    END_ROW_KEY VARBINARY_ENCODED,\n" + "    IS_DRY_RUN BOOLEAN, \n"
    + "    EXECUTION_START_TIME TIMESTAMP,\n" + "    EXECUTION_END_TIME TIMESTAMP,\n"
    + "    STATUS VARCHAR(20),\n" + "    COUNTERS VARCHAR, \n" + "    CONSTRAINT PK PRIMARY KEY (\n"
    + "        TABLE_NAME,\n" + "        TARGET_CLUSTER,\n" + "        TYPE ,\n"
    + "        FROM_TIME,\n" + "        TO_TIME,\n" + "        TENANT_ID,\n"
    + "        START_ROW_KEY )" + ") TTL=" + OUTPUT_TABLE_TTL_SECONDS + ", COLUMN_ENCODED_BYTES="
    + QualifierEncodingScheme.TWO_BYTE_QUALIFIERS.getSerializedMetadataValue()
    + ", COMPRESSION='SNAPPY'";

  /**
   * Creates a repository for managing sync table checkpoint operations. Note: The connection is
   * stored as-is and shared across operations. The caller retains ownership and is responsible for
   * connection lifecycle.
   * @param connection Phoenix connection (must remain open for repository lifetime)
   */
  public PhoenixSyncTableOutputRepository(Connection connection) {
    this.connection = connection;
  }

  public void createSyncCheckpointTableIfNotExists() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(CREATE_CHECKPOINT_TABLE_DDL);
      connection.commit();
      LOGGER.info("Initialization of checkpoint table {} complete",
        SYNC_TABLE_CHECKPOINT_TABLE_NAME);
    }
  }

  public void checkpointSyncTableResult(PhoenixSyncTableCheckpointOutputRow row)
    throws SQLException {

    // Validate required parameters
    Preconditions.checkArgument(row.getTableName() != null && !row.getTableName().isEmpty(),
      "TableName cannot be null or empty for checkpoint");
    Preconditions.checkArgument(row.getTargetCluster() != null && !row.getTargetCluster().isEmpty(),
      "TargetCluster cannot be null or empty for checkpoint");
    Preconditions.checkNotNull(row.getType(), "Type cannot be null for checkpoint");
    Preconditions.checkNotNull(row.getFromTime(), "FromTime cannot be null for checkpoint");
    Preconditions.checkNotNull(row.getToTime(), "ToTime cannot be null for checkpoint");

    try (PreparedStatement ps = connection.prepareStatement(UPSERT_CHECKPOINT_SQL)) {
      ps.setString(1, row.getTableName());
      ps.setString(2, row.getTargetCluster());
      ps.setString(3, row.getType().name());
      ps.setLong(4, row.getFromTime());
      ps.setLong(5, row.getToTime());
      ps.setString(6, row.getTenantId());
      ps.setBytes(7, row.getStartRowKey());
      ps.setBytes(8, row.getEndRowKey());
      ps.setBoolean(9, row.getIsDryRun());
      ps.setTimestamp(10, row.getExecutionStartTime());
      ps.setTimestamp(11, row.getExecutionEndTime());
      ps.setString(12, row.getStatus() != null ? row.getStatus().name() : null);
      ps.setString(13, row.getCounters());
      ps.executeUpdate();
      connection.commit();
    }
  }

  /**
   * Queries for completed mapper regions. Used by PhoenixSyncTableInputFormat to filter out
   * already-processed regions.
   * @param tableName     Source table name
   * @param targetCluster Target cluster ZK quorum
   * @param fromTime      Start timestamp
   * @param toTime        End timestamp
   * @param tenantId      Tenant ID
   * @return List of completed mapper regions
   */
  public List<PhoenixSyncTableCheckpointOutputRow> getProcessedMapperRegions(String tableName,
    String targetCluster, Long fromTime, Long toTime, String tenantId) throws SQLException {

    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT START_ROW_KEY, END_ROW_KEY FROM ")
      .append(SYNC_TABLE_CHECKPOINT_TABLE_NAME)
      .append(" WHERE TABLE_NAME = ?  AND TARGET_CLUSTER = ?")
      .append(" AND TYPE = ? AND FROM_TIME = ? AND TO_TIME = ? ");

    // Conditionally build TENANT_ID clause based on whether tenantId is null
    if (tenantId == null) {
      queryBuilder.append(" AND TENANT_ID IS NULL");
    } else {
      queryBuilder.append(" AND TENANT_ID = ?");
    }

    queryBuilder.append(
      " ORDER BY TABLE_NAME, TARGET_CLUSTER, TYPE, FROM_TIME, TO_TIME, TENANT_ID, START_ROW_KEY");

    List<PhoenixSyncTableCheckpointOutputRow> results = new ArrayList<>();
    try (PreparedStatement ps = connection.prepareStatement(queryBuilder.toString())) {
      int paramIndex = 1;
      ps.setString(paramIndex++, tableName);
      ps.setString(paramIndex++, targetCluster);
      ps.setString(paramIndex++, Type.REGION.name());
      ps.setLong(paramIndex++, fromTime);
      ps.setLong(paramIndex++, toTime);
      // Only bind tenantId parameter if non-null
      if (tenantId != null) {
        ps.setString(paramIndex, tenantId);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          PhoenixSyncTableCheckpointOutputRow row =
            new PhoenixSyncTableCheckpointOutputRow.Builder()
              .setStartRowKey(rs.getBytes("START_ROW_KEY")).setEndRowKey(rs.getBytes("END_ROW_KEY"))
              .build();
          results.add(row);
        }
      }
    }
    return results;
  }

  /**
   * Queries for processed chunks. Used by PhoenixSyncTableMapper to skip already-processed chunks.
   * @param tableName         Source table name
   * @param targetCluster     Target cluster ZK quorum
   * @param fromTime          Start timestamp
   * @param toTime            End timestamp
   * @param tenantId          Tenant ID
   * @param mapperRegionStart Mapper region start key
   * @param mapperRegionEnd   Mapper region end key
   * @return List of processed chunks in the region
   */
  public List<PhoenixSyncTableCheckpointOutputRow> getProcessedChunks(String tableName,
    String targetCluster, Long fromTime, Long toTime, String tenantId, byte[] mapperRegionStart,
    byte[] mapperRegionEnd) throws SQLException {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT START_ROW_KEY, END_ROW_KEY FROM " + SYNC_TABLE_CHECKPOINT_TABLE_NAME
      + " WHERE TABLE_NAME = ? AND TARGET_CLUSTER = ? "
      + " AND TYPE = ? AND FROM_TIME = ? AND TO_TIME = ?");

    // Conditionally build TENANT_ID clause based on whether tenantId is null
    if (tenantId == null) {
      queryBuilder.append(" AND TENANT_ID IS NULL");
    } else {
      queryBuilder.append(" AND TENANT_ID = ?");
    }

    // Check if mapper region boundaries are non-empty (i.e., NOT first/last regions)
    // Only add boundary conditions for non-empty boundaries
    boolean hasEndBoundary = mapperRegionEnd != null && mapperRegionEnd.length > 0;
    boolean hasStartBoundary = mapperRegionStart != null && mapperRegionStart.length > 0;

    // Filter chunks that overlap with this mapper region:
    // - Chunk overlaps if: chunkStart < mapperRegionEnd (when end boundary exists)
    // - Chunk overlaps if: chunkEnd > mapperRegionStart (when start boundary exists)
    if (hasEndBoundary) {
      queryBuilder.append(" AND START_ROW_KEY <= ?");
    }
    if (hasStartBoundary) {
      queryBuilder.append(" AND END_ROW_KEY >= ?");
    }

    queryBuilder.append(
      " ORDER BY TABLE_NAME, TARGET_CLUSTER, TYPE, FROM_TIME, TO_TIME, TENANT_ID, START_ROW_KEY");

    List<PhoenixSyncTableCheckpointOutputRow> results = new ArrayList<>();
    try (PreparedStatement ps = connection.prepareStatement(queryBuilder.toString())) {
      int paramIndex = 1;
      ps.setString(paramIndex++, tableName);
      ps.setString(paramIndex++, targetCluster);
      ps.setString(paramIndex++, Type.CHUNK.name());
      ps.setLong(paramIndex++, fromTime);
      ps.setLong(paramIndex++, toTime);
      // Only bind tenantId parameter if non-null
      if (tenantId != null) {
        ps.setString(paramIndex++, tenantId);
      }
      if (hasEndBoundary) {
        ps.setBytes(paramIndex++, mapperRegionEnd);
      }
      if (hasStartBoundary) {
        ps.setBytes(paramIndex, mapperRegionStart);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          byte[] rawStartKey = rs.getBytes("START_ROW_KEY");
          byte[] endRowKey = rs.getBytes("END_ROW_KEY");
          PhoenixSyncTableCheckpointOutputRow row =
            new PhoenixSyncTableCheckpointOutputRow.Builder().setStartRowKey(rawStartKey)
              .setEndRowKey(endRowKey).build();
          results.add(row);
        }
      }
    }
    return results;
  }
}
