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
package org.apache.phoenix.index.vector;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_VECTOR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_REBUILD_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_SCORECARD_UPDATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REASSIGN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE_ACTIVE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE_BUILDING;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SENTINEL_CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SKEW_METRICS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRIGGER_REASON;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_CENTROID_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_LISTS;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TaskMetaDataServiceCallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the persistence, retrieval, and generation lifecycle of vector centroids stored in
 * {@code SYSTEM.VECTOR_CENTROID} and index generation metadata stored in {@code SYSTEM.CATALOG}.
 * Supports both instance-based usage (bound to a {@link Connection}) and static utility calls,
 * including thread-local and default connection resolution for caller convenience.
 */
public class CentroidManager {

  private static final Logger LOG = LoggerFactory.getLogger(CentroidManager.class);
  private static final ThreadLocal<Connection> THREAD_LOCAL_CONN = new ThreadLocal<>();
  private static volatile Connection defaultConnection;

  private final Connection connection;

  public CentroidManager(Connection connection) {
    this.connection = Objects.requireNonNull(connection, "connection must not be null");
  }

  public Connection getConnection() {
    return connection;
  }

  public static void setDefaultConnection(Connection conn) {
    defaultConnection = conn;
  }

  public static Connection getDefaultConnection() {
    return defaultConnection;
  }

  public static void setThreadLocalConnection(Connection conn) {
    THREAD_LOCAL_CONN.set(conn);
  }

  public static Connection getThreadLocalConnection() {
    return THREAD_LOCAL_CONN.get();
  }

  public static void clearThreadLocalConnection() {
    THREAD_LOCAL_CONN.remove();
  }

  private static Connection resolveConnection() {
    Connection conn = THREAD_LOCAL_CONN.get();
    if (conn != null) {
      return conn;
    }
    if (defaultConnection != null) {
      return defaultConnection;
    }
    throw new IllegalStateException(
      "No Connection provided. Either pass Connection explicitly or set default/thread-local Connection.");
  }

  /**
   * Persists the given centroid byte vectors to {@code SYSTEM.VECTOR_CENTROID} under the specified
   * generation ID.
   * @param indexName  index table name (qualified or unqualified)
   * @param generation centroid generation identifier
   * @param centroids  list of serialized centroid byte arrays
   * @throws SQLException if a database access error occurs
   */
  public void persistCentroids(String indexName, long generation, List<byte[]> centroids)
    throws SQLException {
    persistCentroids(this.connection, indexName, generation, centroids);
  }

  /**
   * Persists centroids from a {@link KMeansResult} to {@code SYSTEM.VECTOR_CENTROID}.
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @param result     k-means training result containing centroids
   * @throws SQLException if a database access error occurs
   */
  public void persistCentroids(String indexName, long generation, KMeansResult result)
    throws SQLException {
    persistCentroids(this.connection, indexName, generation, result);
  }

  /**
   * Persists centroids provided as float arrays to {@code SYSTEM.VECTOR_CENTROID}.
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @param centroids  list of float vector centroids
   * @throws SQLException if a database access error occurs
   */
  public void persistCentroidsFromFloatList(String indexName, long generation,
    List<float[]> centroids) throws SQLException {
    persistCentroidsFromFloatList(this.connection, indexName, generation, centroids);
  }

  /**
   * Reads all centroid vectors for the given index and generation from
   * {@code SYSTEM.VECTOR_CENTROID}, ordered by {@code CENTROID_ID ASC}.
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return list of serialized centroid byte arrays, or an empty list if none exist
   * @throws SQLException if a database access error occurs
   */
  public List<byte[]> loadCentroids(String indexName, long generation) throws SQLException {
    return loadCentroids(this.connection, indexName, generation);
  }

  /**
   * Reads all centroid vectors for the given index and generation, deserializing each vector into a
   * {@code float[]}.
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return list of float vector centroids, or an empty list if none exist
   * @throws SQLException if a database access error occurs
   */
  public List<float[]> loadCentroidsAsFloatVectors(String indexName, long generation)
    throws SQLException {
    return loadCentroidsAsFloatVectors(this.connection, indexName, generation);
  }

  /**
   * Atomically increments the generation ID stored in {@code SYSTEM.CATALOG} metadata for the
   * index, returning the new generation ID.
   * @param indexName index table name
   * @return the new (incremented) generation ID
   * @throws SQLException if a database access error occurs
   */
  public long incrementGeneration(String indexName) throws SQLException {
    return incrementGeneration(this.connection, indexName);
  }

  /**
   * Sets the active generation ID in {@code SYSTEM.CATALOG} metadata for the index.
   * @param indexName  index table name
   * @param generation new generation identifier
   * @throws SQLException if a database access error occurs
   */
  public void setGeneration(String indexName, long generation) throws SQLException {
    setGeneration(this.connection, indexName, generation);
  }

  /**
   * Sets the active generation ID and effective list count in {@code SYSTEM.CATALOG} metadata for
   * the index.
   * @param indexName      index table name
   * @param generation     new generation identifier
   * @param effectiveLists effective IVF cluster count
   * @throws SQLException if a database access error occurs
   */
  public void setGenerationAndLists(String indexName, long generation, int effectiveLists)
    throws SQLException {
    setGenerationAndLists(this.connection, indexName, generation, effectiveLists);
  }

  /**
   * Reads the current generation ID stored in {@code SYSTEM.CATALOG} for the index.
   * @param indexName index table name
   * @return current generation ID, or 0 if not set or row not found
   * @throws SQLException if a database access error occurs
   */
  public long getGeneration(String indexName) throws SQLException {
    return getGeneration(this.connection, indexName);
  }

  /**
   * Removes centroid rows for a specific generation from {@code SYSTEM.VECTOR_CENTROID}.
   * @param indexName  index table name
   * @param generation centroid generation identifier to delete
   * @throws SQLException if a database access error occurs
   */
  public void deleteGeneration(String indexName, long generation) throws SQLException {
    deleteGeneration(this.connection, indexName, generation);
  }

  /**
   * Removes all centroid rows for the given index from {@code SYSTEM.VECTOR_CENTROID}.
   * @param indexName index table name
   * @throws SQLException if a database access error occurs
   */
  public void deleteAllCentroids(String indexName) throws SQLException {
    deleteAllCentroids(this.connection, indexName);
  }

  /**
   * Enqueues a background rebuild task for the vector index in {@code SYSTEM.TASK}.
   * @param indexName index table name
   * @throws SQLException if a database access error occurs
   */
  public void scheduleRebuildTask(String indexName) throws SQLException {
    scheduleRebuildTask(this.connection, indexName, false);
  }

  /**
   * Enqueues a background rebuild task for the vector index in {@code SYSTEM.TASK}.
   * @param indexName index table name
   * @param isManual  whether the rebuild was manually requested
   * @throws SQLException if a database access error occurs
   */
  public void scheduleRebuildTask(String indexName, boolean isManual) throws SQLException {
    scheduleRebuildTask(this.connection, indexName, isManual);
  }

  /**
   * Persists a scorecard row for a single centroid to {@code SYSTEM.VECTOR_CENTROID}. Null metrics
   * are omitted to preserve existing values.
   * @param row scorecard row to persist
   * @throws SQLException if a database access error occurs
   */
  public void persistScorecardRow(ScorecardRow row) throws SQLException {
    persistScorecardRow(this.connection, row);
  }

  /**
   * Persists a scorecard row for a single centroid to {@code SYSTEM.VECTOR_CENTROID}. Null metrics
   * are omitted to preserve existing values.
   */
  public void persistScorecardRow(String indexName, long generation, int centroidId,
    Long clusterSize, Long reassignCount, Long lastScorecardUpdate) throws SQLException {
    persistScorecardRow(this.connection, indexName, generation, centroidId, clusterSize,
      reassignCount, lastScorecardUpdate);
  }

  /**
   * Persists a batch of scorecard rows to {@code SYSTEM.VECTOR_CENTROID} in a single commit.
   * @param rows scorecard rows to persist; may be empty
   * @throws SQLException if a database access error occurs
   */
  public void persistScorecard(List<ScorecardRow> rows) throws SQLException {
    persistScorecard(this.connection, rows);
  }

  /**
   * Loads all scorecard rows for the given index and generation from
   * {@code SYSTEM.VECTOR_CENTROID}, ordered by {@code CENTROID_ID ASC}.
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return list of scorecard rows
   * @throws SQLException if a database access error occurs
   */
  public List<ScorecardRow> loadScorecard(String indexName, long generation) throws SQLException {
    return loadScorecard(this.connection, indexName, generation);
  }

  /**
   * Persists generation-level summary metadata to the sentinel row ({@code CENTROID_ID = -1}). Null
   * fields are omitted to preserve existing values.
   * @param summary generation summary to persist
   * @throws SQLException if a database access error occurs
   */
  public void persistGenerationSummary(GenerationSummary summary) throws SQLException {
    persistGenerationSummary(this.connection, summary);
  }

  /**
   * Persists generation-level summary metadata to the sentinel row ({@code CENTROID_ID = -1}). Null
   * arguments are omitted to preserve existing values.
   */
  public void persistGenerationSummary(String indexName, long generation,
    ClusterSkewMetrics skewMetrics, String rebuildState, String triggerReason, Long lastRebuildTime,
    Long lastScorecardUpdate) throws SQLException {
    persistGenerationSummary(this.connection, indexName, generation, skewMetrics, rebuildState,
      triggerReason, lastRebuildTime, lastScorecardUpdate);
  }

  /**
   * Loads the generation-level summary metadata from the sentinel row ({@code CENTROID_ID = -1}).
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return generation summary, or {@code null} if no summary row exists
   * @throws SQLException if a database access error occurs
   */
  public GenerationSummary loadGenerationSummary(String indexName, long generation)
    throws SQLException {
    return loadGenerationSummary(this.connection, indexName, generation);
  }

  /**
   * Lists all distinct generation IDs recorded in {@code SYSTEM.VECTOR_CENTROID} for an index.
   * @param indexName index table name
   * @return list of distinct generation IDs ordered ascending
   * @throws SQLException if a database access error occurs
   */
  public List<Long> listGenerations(String indexName) throws SQLException {
    return listGenerations(this.connection, indexName);
  }

  /**
   * Upserts centroid rows into {@code SYSTEM.VECTOR_CENTROID} with the given generation ID.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @param centroids  list of serialized centroid byte arrays
   * @throws SQLException if a database access error occurs
   */
  public static void persistCentroids(Connection conn, String indexName, long generation,
    List<byte[]> centroids) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }
    if (generation < 0) {
      throw new IllegalArgumentException("generation must be non-negative");
    }
    if (centroids == null) {
      throw new IllegalArgumentException("centroids must not be null");
    }
    if (centroids.isEmpty()) {
      return;
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    String upsertSql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ", " + CENTROID_VECTOR + ", " + CLUSTER_SIZE + ", "
      + REASSIGN_COUNT + ") VALUES (?, ?, ?, ?, 0, 0)";

    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      for (int i = 0; i < centroids.size(); i++) {
        byte[] vector = centroids.get(i);
        if (vector == null) {
          throw new IllegalArgumentException("Centroid vector at index " + i + " must not be null");
        }
        ps.setString(1, normalizedIndexName);
        ps.setLong(2, generation);
        ps.setInt(3, i);
        ps.setBytes(4, vector);
        ps.executeUpdate();
      }
    }
    conn.commit();
  }

  /**
   * Equivalent to {@link #persistCentroids(Connection, String, long, KMeansResult, String)} with no
   * trigger reason recorded.
   */
  public static void persistCentroids(Connection conn, String indexName, long generation,
    KMeansResult result) throws SQLException {
    persistCentroids(conn, indexName, generation, result, null);
  }

  /**
   * Persists centroids from a {@link KMeansResult} into {@code SYSTEM.VECTOR_CENTROID}, recording
   * the generation's training skew metrics, {@code REBUILD_STATE = 'A'} and {@code triggerReason}
   * on its sentinel row.
   * @param conn          Phoenix connection
   * @param indexName     index table name
   * @param generation    centroid generation identifier
   * @param result        trained centroids
   * @param triggerReason why this generation was trained, or {@code null} to leave any already
   *                      recorded reason in place
   * @throws SQLException if a database access error occurs
   */
  public static void persistCentroids(Connection conn, String indexName, long generation,
    KMeansResult result, String triggerReason) throws SQLException {
    if (result == null) {
      throw new IllegalArgumentException("result must not be null");
    }
    persistCentroids(conn, indexName, generation, result.getCentroidsAsBytes());
    if (result.getSkewMetrics() != null || triggerReason != null) {
      persistGenerationSummary(conn, indexName, generation, result.getSkewMetrics(),
        REBUILD_STATE_ACTIVE, triggerReason, null, null);
    }
  }

  /**
   * Persists centroids from a list of float arrays into {@code SYSTEM.VECTOR_CENTROID}.
   */
  public static void persistCentroidsFromFloatList(Connection conn, String indexName,
    long generation, List<float[]> centroids) throws SQLException {
    if (centroids == null) {
      throw new IllegalArgumentException("centroids must not be null");
    }
    List<byte[]> byteList = new ArrayList<>(centroids.size());
    for (int i = 0; i < centroids.size(); i++) {
      float[] centroid = centroids.get(i);
      if (centroid == null) {
        throw new IllegalArgumentException("Centroid vector at index " + i + " must not be null");
      }
      byteList.add(PVectorFloat.INSTANCE.toBytes(centroid));
    }
    persistCentroids(conn, indexName, generation, byteList);
  }

  /**
   * Reads all centroid vectors for a given index and generation from
   * {@code SYSTEM.VECTOR_CENTROID}, ordered by centroid ID ascending.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return list of centroid byte vectors, or empty list if no centroids found
   * @throws SQLException if a database access error occurs
   */
  public static List<byte[]> loadCentroids(Connection conn, String indexName, long generation)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    String selectSql = "SELECT " + CENTROID_VECTOR + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID
      + " >= 0 ORDER BY " + CENTROID_ID + " ASC";

    List<byte[]> centroids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
      ps.setString(1, normalizedIndexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          centroids.add(rs.getBytes(1));
        }
      }
    }
    return centroids;
  }

  /**
   * Reads all centroid vectors for the given index and generation, deserializing each vector into a
   * {@code float[]}.
   */
  public static List<float[]> loadCentroidsAsFloatVectors(Connection conn, String indexName,
    long generation) throws SQLException {
    List<byte[]> byteList = loadCentroids(conn, indexName, generation);
    List<float[]> floatList = new ArrayList<>(byteList.size());
    for (byte[] b : byteList) {
      floatList.add(b != null ? (float[]) PVectorFloat.INSTANCE.toObject(b) : null);
    }
    return floatList;
  }

  /**
   * Atomically increments the generation ID stored in the system catalog metadata for the index,
   * returning the new generation.
   * @param conn      Phoenix connection
   * @param indexName index table name
   * @return the new (incremented) generation ID
   * @throws SQLException if a database access error occurs
   */
  public static synchronized long incrementGeneration(Connection conn, String indexName)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String schemaName = SchemaUtil.getSchemaNameFromFullName(indexName);
    String tableName = SchemaUtil.getTableNameFromFullName(indexName);
    if (schemaName != null && schemaName.isEmpty()) {
      schemaName = null;
    }
    String normalizedSchema =
      schemaName != null ? SchemaUtil.normalizeIdentifier(schemaName) : null;
    String normalizedTable = SchemaUtil.normalizeIdentifier(tableName);

    String tenantId = null;
    try {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      if (pconn != null && pconn.getTenantId() != null) {
        tenantId = pconn.getTenantId().getString();
      }
    } catch (Exception ignored) {
    }

    long currentGen = getGenerationInternal(conn, tenantId, normalizedSchema, normalizedTable);
    long newGen = currentGen + 1L;

    String upsertSql = "UPSERT INTO " + SYSTEM_CATALOG_NAME + " (" + TENANT_ID + ", " + TABLE_SCHEM
      + ", " + TABLE_NAME + ", " + VECTOR_CENTROID_GENERATION + ") VALUES (?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      if (tenantId == null) {
        ps.setNull(1, Types.VARCHAR);
      } else {
        ps.setString(1, tenantId);
      }
      if (normalizedSchema == null) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, normalizedSchema);
      }
      ps.setString(3, normalizedTable);
      ps.setLong(4, newGen);
      ps.executeUpdate();
    }
    conn.commit();

    invalidateMetadataCaches(conn, tenantId, normalizedSchema, normalizedTable);

    return newGen;
  }

  /**
   * Sets the active generation ID in {@code SYSTEM.CATALOG} metadata for the index and invalidates
   * metadata caches.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation new generation identifier
   * @throws SQLException if a database access error occurs
   */
  public static void setGeneration(Connection conn, String indexName, long generation)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    String normalizedSchema = SchemaUtil.getSchemaNameFromFullName(normalized);
    String normalizedTable = SchemaUtil.getTableNameFromFullName(normalized);
    if (normalizedSchema != null && normalizedSchema.isEmpty()) {
      normalizedSchema = null;
    }

    String tenantId = null;
    try {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      if (pconn != null && pconn.getTenantId() != null) {
        tenantId = pconn.getTenantId().getString();
      }
    } catch (Exception ignored) {
    }

    String upsertSql = "UPSERT INTO " + SYSTEM_CATALOG_NAME + " (" + TENANT_ID + ", " + TABLE_SCHEM
      + ", " + TABLE_NAME + ", " + VECTOR_CENTROID_GENERATION + ") VALUES (?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      if (tenantId == null) {
        ps.setNull(1, Types.VARCHAR);
      } else {
        ps.setString(1, tenantId);
      }
      if (normalizedSchema == null) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, normalizedSchema);
      }
      ps.setString(3, normalizedTable);
      ps.setLong(4, generation);
      ps.executeUpdate();
    }
    conn.commit();

    invalidateMetadataCaches(conn, tenantId, normalizedSchema, normalizedTable);
  }

  /**
   * Sets the active generation ID and effective list count in {@code SYSTEM.CATALOG} metadata for
   * the index and invalidates metadata caches.
   * @param conn           Phoenix connection
   * @param indexName      index table name
   * @param generation     new generation identifier
   * @param effectiveLists effective IVF cluster count
   * @throws SQLException if a database access error occurs
   */
  public static void setGenerationAndLists(Connection conn, String indexName, long generation,
    int effectiveLists) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    String normalizedSchema = SchemaUtil.getSchemaNameFromFullName(normalized);
    String normalizedTable = SchemaUtil.getTableNameFromFullName(normalized);
    if (normalizedSchema != null && normalizedSchema.isEmpty()) {
      normalizedSchema = null;
    }

    String tenantId = null;
    try {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      if (pconn != null && pconn.getTenantId() != null) {
        tenantId = pconn.getTenantId().getString();
      }
    } catch (Exception ignored) {
    }

    String upsertSql = "UPSERT INTO " + SYSTEM_CATALOG_NAME + " (" + TENANT_ID + ", " + TABLE_SCHEM
      + ", " + TABLE_NAME + ", " + VECTOR_CENTROID_GENERATION + ", " + VECTOR_IVF_LISTS
      + ") VALUES (?, ?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      if (tenantId == null) {
        ps.setNull(1, Types.VARCHAR);
      } else {
        ps.setString(1, tenantId);
      }
      if (normalizedSchema == null) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, normalizedSchema);
      }
      ps.setString(3, normalizedTable);
      ps.setLong(4, generation);
      ps.setInt(5, effectiveLists);
      ps.executeUpdate();
    }
    conn.commit();

    invalidateMetadataCaches(conn, tenantId, normalizedSchema, normalizedTable);
  }

  private static void invalidateMetadataCaches(Connection conn, String tenantId,
    String normalizedSchema, String normalizedTable) {
    try {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      if (pconn != null) {
        String fullTableName = SchemaUtil.getTableName(normalizedSchema, normalizedTable);
        byte[] tenantIdBytes =
          pconn.getTenantId() != null ? pconn.getTenantId().getBytes() : ByteUtil.EMPTY_BYTE_ARRAY;
        byte[] schemaBytes =
          normalizedSchema != null ? Bytes.toBytes(normalizedSchema) : ByteUtil.EMPTY_BYTE_ARRAY;
        byte[] tableBytes = Bytes.toBytes(normalizedTable);

        // Evict table metadata from client and server caches to ensure subsequent operations
        // refresh updated generation metadata from SYSTEM.CATALOG.
        pconn.removeTable(pconn.getTenantId(), fullTableName, null, HConstants.LATEST_TIMESTAMP);

        try {
          pconn.getQueryServices().clearTableFromCache(tenantIdBytes, schemaBytes, tableBytes,
            HConstants.LATEST_TIMESTAMP);
        } catch (Exception e) {
          LOG.debug("clearTableFromCache for {} failed (may be expected in unit test environments)",
            fullTableName, e);
        }

        // Evict the parent data table cache when invalidating an index to refresh index list
        // references.
        try {
          String parentName =
            getParentDataTableName(conn, tenantId, normalizedSchema, normalizedTable);
          if (parentName != null && !parentName.isEmpty()) {
            String parentSchema = SchemaUtil.getSchemaNameFromFullName(parentName);
            String parentTable = SchemaUtil.getTableNameFromFullName(parentName);
            if (parentSchema != null && parentSchema.isEmpty()) {
              parentSchema = null;
            }
            String parentFullName = SchemaUtil.getTableName(parentSchema, parentTable);
            pconn.removeTable(pconn.getTenantId(), parentFullName, null,
              HConstants.LATEST_TIMESTAMP);
            byte[] parentSchemaBytes =
              parentSchema != null ? Bytes.toBytes(parentSchema) : ByteUtil.EMPTY_BYTE_ARRAY;
            byte[] parentTableBytes = Bytes.toBytes(parentTable);
            pconn.getQueryServices().clearTableFromCache(tenantIdBytes, parentSchemaBytes,
              parentTableBytes, HConstants.LATEST_TIMESTAMP);
          }
        } catch (Exception e) {
          LOG.debug("Failed to clear parent table cache for index {}", fullTableName, e);
        }
      }

      // Reset mock server metadata caches if executing within a test harness.
      try {
        Class<?> clazz = Class.forName("org.apache.phoenix.end2end.ServerMetadataCacheTestImpl");
        clazz.getMethod("resetCache").invoke(null);
      } catch (ClassNotFoundException ignored) {
      } catch (Exception e) {
        LOG.debug("ServerMetadataCacheTestImpl.resetCache() invocation failed", e);
      }
    } catch (Exception e) {
      LOG.debug("Cache invalidation failed for {}.{}", normalizedSchema, normalizedTable, e);
    }
  }

  /**
   * Retrieves the current generation ID stored in {@code SYSTEM.CATALOG} for the specified index.
   */
  public static long getGeneration(Connection conn, String indexName) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String schemaName = SchemaUtil.getSchemaNameFromFullName(indexName);
    String tableName = SchemaUtil.getTableNameFromFullName(indexName);
    if (schemaName != null && schemaName.isEmpty()) {
      schemaName = null;
    }
    String normalizedSchema =
      schemaName != null ? SchemaUtil.normalizeIdentifier(schemaName) : null;
    String normalizedTable = SchemaUtil.normalizeIdentifier(tableName);

    String tenantId = null;
    try {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      if (pconn != null && pconn.getTenantId() != null) {
        tenantId = pconn.getTenantId().getString();
      }
    } catch (Exception ignored) {
    }

    return getGenerationInternal(conn, tenantId, normalizedSchema, normalizedTable);
  }

  private static long getGenerationInternal(Connection conn, String tenantId,
    String normalizedSchema, String normalizedTable) throws SQLException {
    StringBuilder query =
      new StringBuilder("SELECT " + VECTOR_CENTROID_GENERATION + " FROM " + SYSTEM_CATALOG_NAME
        + " WHERE TABLE_NAME = ? AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL");
    if (tenantId != null) {
      query.append(" AND TENANT_ID = ?");
    } else {
      query.append(" AND TENANT_ID IS NULL");
    }
    if (normalizedSchema != null) {
      query.append(" AND TABLE_SCHEM = ?");
    } else {
      query.append(" AND TABLE_SCHEM IS NULL");
    }

    try (PreparedStatement ps = conn.prepareStatement(query.toString())) {
      int paramIdx = 1;
      ps.setString(paramIdx++, normalizedTable);
      if (tenantId != null) {
        ps.setString(paramIdx++, tenantId);
      }
      if (normalizedSchema != null) {
        ps.setString(paramIdx++, normalizedSchema);
      }
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          long val = rs.getLong(1);
          if (!rs.wasNull() && val > 0) {
            return val;
          }
        }
      }
    }
    return 0L;
  }

  /**
   * Looks up the parent data table name for an index from {@code SYSTEM.CATALOG}.
   * @return the parent data table name, or {@code null} if not found or not an index
   */
  public static String getParentDataTableName(Connection conn, String tenantId,
    String normalizedSchema, String normalizedTable) throws SQLException {
    StringBuilder query =
      new StringBuilder("SELECT " + DATA_TABLE_NAME + " FROM " + SYSTEM_CATALOG_NAME
        + " WHERE TABLE_NAME = ? AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL");
    if (tenantId != null) {
      query.append(" AND TENANT_ID = ?");
    } else {
      query.append(" AND TENANT_ID IS NULL");
    }
    if (normalizedSchema != null) {
      query.append(" AND TABLE_SCHEM = ?");
    } else {
      query.append(" AND TABLE_SCHEM IS NULL");
    }
    try (PreparedStatement ps = conn.prepareStatement(query.toString())) {
      int paramIdx = 1;
      ps.setString(paramIdx++, normalizedTable);
      if (tenantId != null) {
        ps.setString(paramIdx++, tenantId);
      }
      if (normalizedSchema != null) {
        ps.setString(paramIdx++, normalizedSchema);
      }
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    }
    return null;
  }

  /**
   * Removes centroid rows for a specific generation during cleanup after a rebuild.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier to delete
   * @throws SQLException if a database access error occurs
   */
  public static final String DELETE_GENERATION_SQL = "DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME
    + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ?";
  public static final String DELETE_ALL_CENTROIDS_SQL =
    "DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?";

  /**
   * Deletes centroid records for a specific generation from {@code SYSTEM.VECTOR_CENTROID}.
   * Executes with autocommit enabled to allow server-side delete execution.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier to delete
   * @throws SQLException if a database access error occurs
   */
  public static void deleteGeneration(Connection conn, String indexName, long generation)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    runAutoCommitted(conn, DELETE_GENERATION_SQL, ps -> {
      ps.setString(1, normalizedIndexName);
      ps.setLong(2, generation);
      ps.executeUpdate();
      return null;
    });
  }

  /**
   * Deletes all centroid records for the specified index from {@code SYSTEM.VECTOR_CENTROID}.
   * Executes with autocommit enabled to allow server-side delete execution.
   * @param conn      Phoenix connection
   * @param indexName index table name
   * @throws SQLException if a database access error occurs
   */
  public static void deleteAllCentroids(Connection conn, String indexName) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    runAutoCommitted(conn, DELETE_ALL_CENTROIDS_SQL, ps -> {
      ps.setString(1, normalizedIndexName);
      ps.executeUpdate();
      return null;
    });
  }

  /** Returns the explain plan for the generation deletion statement. */
  public static String getDeleteGenerationExplainPlan(Connection conn, String indexName,
    long generation) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    return runAutoCommitted(conn, "EXPLAIN " + DELETE_GENERATION_SQL, ps -> {
      ps.setString(1, normalizedIndexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
          sb.append(rs.getString(1)).append('\n');
        }
        return sb.toString();
      }
    });
  }

  /** Body of a single statement run on a borrowed connection. */
  private interface StatementAction<T> {
    T run(PreparedStatement ps) throws SQLException;
  }

  /** Body of a multi-statement unit of work run on a borrowed connection. */
  private interface ConnectionAction {
    void run(Connection conn) throws SQLException;
  }

  /**
   * Executes the specified SQL statement with autocommit enabled in an isolated connection context
   * without committing or exposing external uncommitted mutations.
   */
  private static <T> T runAutoCommitted(Connection conn, String sql, StatementAction<T> action)
    throws SQLException {
    if (conn.isWrapperFor(PhoenixConnection.class)) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      try (PhoenixConnection autoCommitConn = new PhoenixConnection(pconn, (MutationState) null)) {
        autoCommitConn.setAutoCommit(true);
        try (PreparedStatement ps = autoCommitConn.prepareStatement(sql)) {
          return action.run(ps);
        }
      }
    }
    boolean wasAutoCommit = conn.getAutoCommit();
    try {
      conn.setAutoCommit(true);
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        return action.run(ps);
      }
    } finally {
      conn.setAutoCommit(wasAutoCommit);
    }
  }

  /**
   * Executes operations within an isolated transaction committed as a single batch, independent of
   * the caller's autocommit settings and mutation state.
   */
  private static void runBatched(Connection conn, ConnectionAction action) throws SQLException {
    if (conn.isWrapperFor(PhoenixConnection.class)) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      try (PhoenixConnection batchConn = new PhoenixConnection(pconn, (MutationState) null)) {
        batchConn.setAutoCommit(false);
        action.run(batchConn);
        batchConn.commit();
      }
      return;
    }
    action.run(conn);
    conn.commit();
  }

  /**
   * Enqueues a background rebuild task for the vector index in {@code SYSTEM.TASK}.
   * @param conn      Phoenix connection
   * @param indexName index table name
   * @throws SQLException if a database access error occurs
   */
  public static void scheduleRebuildTask(Connection conn, String indexName) throws SQLException {
    scheduleRebuildTask(conn, indexName, false);
  }

  /**
   * Enqueues a background rebuild task for the vector index in {@code SYSTEM.TASK}.
   * @param conn      Phoenix connection
   * @param indexName index table name
   * @param isManual  whether the rebuild was manually requested
   * @throws SQLException if a database access error occurs
   */
  public static void scheduleRebuildTask(Connection conn, String indexName, boolean isManual)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    String schemaName = SchemaUtil.getSchemaNameFromFullName(normalized);
    String tableName = SchemaUtil.getTableNameFromFullName(normalized);
    String data = isManual ? "{\"manual\":true}" : "{}";
    try {
      SystemTaskParams params = new SystemTaskParams.SystemTaskParamsBuilder().setConn(pconn)
        .setTaskType(PTable.TaskType.VECTOR_INDEX_REBUILD)
        .setTenantId(pconn.getTenantId() != null ? pconn.getTenantId().getString() : null)
        .setSchemaName(schemaName).setTableName(tableName)
        .setTaskStatus(PTable.TaskStatus.CREATED.toString()).setData(data).build();
      List<Mutation> mutations = Task.getMutationsForAddTask(params);
      byte[] rowKey = mutations.get(0).getRow();
      Task.taskMetaDataCoprocessorExec(pconn, rowKey, new TaskMetaDataServiceCallBack(mutations));
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  /**
   * Accumulates optional non-primary-key columns and bind values for dynamic
   * {@code SYSTEM.VECTOR_CENTROID} upsert statements. Null columns are omitted to preserve existing
   * cell values.
   */
  private static final class UpsertColumns {
    private final List<String> names = new ArrayList<>();
    private final List<Object> values = new ArrayList<>();

    UpsertColumns add(String name, Object value) {
      if (value != null) {
        names.add(name);
        values.add(value);
      }
      return this;
    }

    /**
     * Appends the column names and their placeholders to a {@code (pk..., cols...) VALUES} list.
     */
    void appendTo(StringBuilder columnList, StringBuilder valueList) {
      for (String name : names) {
        columnList.append(", ").append(name);
        valueList.append(", ?");
      }
    }

    /** Binds the accumulated values starting at {@code startIndex} (1-based). */
    void bind(PreparedStatement ps, int startIndex) throws SQLException {
      for (int i = 0; i < values.size(); i++) {
        Object value = values.get(i);
        int idx = startIndex + i;
        if (value instanceof Long) {
          ps.setLong(idx, (Long) value);
        } else if (value instanceof byte[]) {
          ps.setBytes(idx, (byte[]) value);
        } else {
          ps.setString(idx, (String) value);
        }
      }
    }
  }

  private static void validateScorecardKey(String indexName, long generation, int centroidId) {
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }
    if (generation < 0) {
      throw new IllegalArgumentException("generation must be non-negative");
    }
    if (centroidId < 0) {
      throw new IllegalArgumentException("centroidId for scorecard row must be non-negative");
    }
  }

  private static String scorecardUpsertSql(UpsertColumns columns) {
    StringBuilder columnList = new StringBuilder(INDEX_NAME).append(", ").append(GENERATION_ID)
      .append(", ").append(CENTROID_ID);
    StringBuilder valueList = new StringBuilder("?, ?, ?");
    columns.appendTo(columnList, valueList);
    return "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + columnList + ") VALUES ("
      + valueList + ")";
  }

  /**
   * Persists a scorecard row for a single centroid to {@code SYSTEM.VECTOR_CENTROID}. Null metrics
   * are omitted to preserve existing values.
   * @param conn Phoenix connection
   * @param row  scorecard row to persist
   * @throws SQLException if a database access error occurs
   */
  public static void persistScorecardRow(Connection conn, ScorecardRow row) throws SQLException {
    if (row == null) {
      throw new IllegalArgumentException("row must not be null");
    }
    persistScorecard(conn, Collections.singletonList(row));
  }

  /**
   * Persists a scorecard row for a single centroid to {@code SYSTEM.VECTOR_CENTROID}. Null metrics
   * are omitted to preserve existing values.
   */
  public static void persistScorecardRow(Connection conn, String indexName, long generation,
    int centroidId, Long clusterSize, Long reassignCount, Long lastScorecardUpdate)
    throws SQLException {
    persistScorecard(conn, Collections.singletonList(new ScorecardRow(indexName, generation,
      centroidId, clusterSize, reassignCount, lastScorecardUpdate)));
  }

  /**
   * Persists a batch of scorecard rows to {@code SYSTEM.VECTOR_CENTROID} in a single commit.
   * Statements are grouped and batched by written columns. Null metrics are omitted to preserve
   * existing values.
   * @param conn Phoenix connection
   * @param rows scorecard rows to persist; may be empty
   * @throws SQLException if a database access error occurs
   */
  public static void persistScorecard(Connection conn, List<ScorecardRow> rows)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (rows == null) {
      throw new IllegalArgumentException("rows must not be null");
    }
    if (rows.isEmpty()) {
      return;
    }
    // Validate all rows before execution to avoid leaving partial uncommitted mutations.
    for (ScorecardRow row : rows) {
      if (row == null) {
        throw new IllegalArgumentException("scorecard row must not be null");
      }
      validateScorecardKey(row.getIndexName(), row.getGenerationId(), row.getCentroidId());
    }

    runBatched(conn, batchConn -> {
      String preparedSql = null;
      PreparedStatement ps = null;
      try {
        for (ScorecardRow row : rows) {
          UpsertColumns columns = new UpsertColumns().add(CLUSTER_SIZE, row.getClusterSize())
            .add(REASSIGN_COUNT, row.getReassignCount())
            .add(LAST_SCORECARD_UPDATE, row.getLastScorecardUpdate());
          String sql = scorecardUpsertSql(columns);
          if (!sql.equals(preparedSql)) {
            if (ps != null) {
              ps.close();
            }
            ps = batchConn.prepareStatement(sql);
            preparedSql = sql;
          }
          ps.setString(1, SchemaUtil.normalizeFullTableName(row.getIndexName()));
          ps.setLong(2, row.getGenerationId());
          ps.setInt(3, row.getCentroidId());
          columns.bind(ps, 4);
          ps.executeUpdate();
        }
      } finally {
        if (ps != null) {
          ps.close();
        }
      }
    });
  }

  /**
   * Loads all scorecard rows for the given index and generation from
   * {@code SYSTEM.VECTOR_CENTROID}, ordered by {@code CENTROID_ID ASC}.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return list of scorecard rows
   * @throws SQLException if a database access error occurs
   */
  public static List<ScorecardRow> loadScorecard(Connection conn, String indexName, long generation)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }
    if (generation < 0) {
      throw new IllegalArgumentException("generation must be non-negative");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    String selectSql = "SELECT " + CENTROID_ID + ", " + CLUSTER_SIZE + ", " + REASSIGN_COUNT + ", "
      + LAST_SCORECARD_UPDATE + " FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME
      + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID + " >= 0 ORDER BY " + CENTROID_ID
      + " ASC";

    List<ScorecardRow> list = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
      ps.setString(1, normalizedIndexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          int centroidId = rs.getInt(1);
          long cs = rs.getLong(2);
          Long clusterSize = rs.wasNull() ? null : cs;
          long rc = rs.getLong(3);
          Long reassignCount = rs.wasNull() ? null : rc;
          long lu = rs.getLong(4);
          Long lastScorecardUpdate = rs.wasNull() ? null : lu;
          list.add(new ScorecardRow.Builder().setIndexName(normalizedIndexName)
            .setGenerationId(generation).setCentroidId(centroidId).setClusterSize(clusterSize)
            .setReassignCount(reassignCount).setLastScorecardUpdate(lastScorecardUpdate).build());
        }
      }
    }
    return list;
  }

  /**
   * Persists generation-level summary metadata to the sentinel row ({@code CENTROID_ID = -1}). Null
   * fields are omitted to preserve existing values.
   * @param conn    Phoenix connection
   * @param summary generation summary to persist
   * @throws SQLException if a database access error occurs
   */
  public static void persistGenerationSummary(Connection conn, GenerationSummary summary)
    throws SQLException {
    if (summary == null) {
      throw new IllegalArgumentException("summary must not be null");
    }
    persistGenerationSummaryInternal(conn, summary.getIndexName(), summary.getGenerationId(),
      summary.getSkewMetricsBytes(), summary.getRebuildState(), summary.getTriggerReason(),
      summary.getLastRebuildTime(), summary.getLastScorecardUpdate());
  }

  /**
   * Persists generation-level summary metadata to the sentinel row ({@code CENTROID_ID = -1}). Null
   * arguments are omitted to preserve existing values.
   */
  public static void persistGenerationSummary(Connection conn, String indexName, long generation,
    ClusterSkewMetrics skewMetrics, String rebuildState, String triggerReason, Long lastRebuildTime,
    Long lastScorecardUpdate) throws SQLException {
    persistGenerationSummaryInternal(conn, indexName, generation,
      skewMetrics != null ? skewMetrics.toBytes() : null, rebuildState, triggerReason,
      lastRebuildTime, lastScorecardUpdate);
  }

  private static void persistGenerationSummaryInternal(Connection conn, String indexName,
    long generation, byte[] skewMetricsBytes, String rebuildState, String triggerReason,
    Long lastRebuildTime, Long lastScorecardUpdate) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }
    if (generation < 0) {
      throw new IllegalArgumentException("generation must be non-negative");
    }

    UpsertColumns columns = new UpsertColumns().add(SKEW_METRICS, skewMetricsBytes)
      .add(REBUILD_STATE, rebuildState).add(TRIGGER_REASON, triggerReason)
      .add(LAST_REBUILD_TIME, lastRebuildTime).add(LAST_SCORECARD_UPDATE, lastScorecardUpdate);

    StringBuilder columnList = new StringBuilder(INDEX_NAME).append(", ").append(GENERATION_ID)
      .append(", ").append(CENTROID_ID);
    StringBuilder valueList = new StringBuilder("?, ?, ").append(SENTINEL_CENTROID_ID);
    columns.appendTo(columnList, valueList);
    String upsertSql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + columnList
      + ") VALUES (" + valueList + ")";

    runBatched(conn, batchConn -> {
      try (PreparedStatement ps = batchConn.prepareStatement(upsertSql)) {
        ps.setString(1, SchemaUtil.normalizeFullTableName(indexName));
        ps.setLong(2, generation);
        columns.bind(ps, 3);
        ps.executeUpdate();
      }
    });
  }

  /**
   * Loads the generation-level summary metadata from the sentinel row ({@code CENTROID_ID = -1}).
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return generation summary, or {@code null} if no summary row exists
   * @throws SQLException if a database access error occurs
   */
  public static GenerationSummary loadGenerationSummary(Connection conn, String indexName,
    long generation) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }
    if (generation < 0) {
      throw new IllegalArgumentException("generation must be non-negative");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    String selectSql = "SELECT " + SKEW_METRICS + ", " + REBUILD_STATE + ", " + TRIGGER_REASON
      + ", " + LAST_REBUILD_TIME + ", " + LAST_SCORECARD_UPDATE + " FROM "
      + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID
      + " = ? AND " + CENTROID_ID + " = " + SENTINEL_CENTROID_ID;

    try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
      ps.setString(1, normalizedIndexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          byte[] skewBytes = rs.getBytes(1);
          String rebuildState = rs.getString(2);
          String triggerReason = rs.getString(3);
          long rt = rs.getLong(4);
          Long lastRebuildTime = rs.wasNull() ? null : rt;
          long lu = rs.getLong(5);
          Long lastScorecardUpdate = rs.wasNull() ? null : lu;

          GenerationSummary summary = new GenerationSummary.Builder()
            .setIndexName(normalizedIndexName).setGenerationId(generation)
            .setSkewMetricsBytes(skewBytes).setRebuildState(rebuildState)
            .setTriggerReason(triggerReason).setLastRebuildTime(lastRebuildTime)
            .setLastScorecardUpdate(lastScorecardUpdate).build();
          if (summary.getSkewMetricsDecodeError() != null) {
            LOG.warn("Could not deserialize ClusterSkewMetrics for index {} gen {}: {}",
              normalizedIndexName, generation, summary.getSkewMetricsDecodeError());
          }
          return summary;
        }
      }
    }
    return null;
  }

  /**
   * Checks whether any generation of the specified index is in the building state
   * ({@code REBUILD_STATE = 'B'}) via the metadata sentinel row in {@code SYSTEM.VECTOR_CENTROID}.
   * @param conn      Phoenix connection
   * @param indexName index table name
   * @return true if an index rebuild is currently in progress
   * @throws SQLException if a database access error occurs
   */
  public static boolean isRebuildInProgress(Connection conn, String indexName) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String selectSql = "SELECT " + GENERATION_ID + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? AND " + CENTROID_ID + " = " + SENTINEL_CENTROID_ID + " AND "
      + REBUILD_STATE + " = ? LIMIT 1";
    try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
      ps.setString(1, SchemaUtil.normalizeFullTableName(indexName));
      ps.setString(2, REBUILD_STATE_BUILDING);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Lists all distinct generation IDs recorded in {@code SYSTEM.VECTOR_CENTROID} for an index.
   * @param conn      Phoenix connection
   * @param indexName index table name
   * @return list of distinct generation IDs ordered ascending
   * @throws SQLException if a database access error occurs
   */
  public static List<Long> listGenerations(Connection conn, String indexName) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    String selectSql = "SELECT DISTINCT " + GENERATION_ID + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? ORDER BY " + GENERATION_ID + " ASC";

    List<Long> generations = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
      ps.setString(1, normalizedIndexName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          generations.add(rs.getLong(1));
        }
      }
    }
    return generations;
  }

  /** Returns a {@link CentroidManager} instance using the default connection. */
  public static CentroidManager get() {
    return new CentroidManager(resolveConnection());
  }

  /** Returns a {@link CentroidManager} instance wrapping the given connection. */
  public static CentroidManager get(Connection conn) {
    return new CentroidManager(conn);
  }
}
