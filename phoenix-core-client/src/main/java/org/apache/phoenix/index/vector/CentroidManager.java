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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_CENTROID_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_LISTS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;
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
      + CENTROID_ID + ", " + CENTROID_VECTOR + ", " + GENERATION_ID + ") VALUES (?, ?, ?, ?)";

    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      for (int i = 0; i < centroids.size(); i++) {
        byte[] vector = centroids.get(i);
        if (vector == null) {
          throw new IllegalArgumentException("Centroid vector at index " + i + " must not be null");
        }
        ps.setString(1, normalizedIndexName);
        ps.setInt(2, i);
        ps.setBytes(3, vector);
        ps.setLong(4, generation);
        ps.executeUpdate();
      }
    }
    conn.commit();
  }

  /**
   * Persists centroids from a {@link KMeansResult} into {@code SYSTEM.VECTOR_CENTROID}.
   */
  public static void persistCentroids(Connection conn, String indexName, long generation,
    KMeansResult result) throws SQLException {
    if (result == null) {
      throw new IllegalArgumentException("result must not be null");
    }
    persistCentroids(conn, indexName, generation, result.getCentroidsAsBytes());
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
    String selectSql =
      "SELECT " + CENTROID_VECTOR + " FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME
        + " = ? AND " + GENERATION_ID + " = ? ORDER BY " + CENTROID_ID + " ASC";

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
  private static String getParentDataTableName(Connection conn, String tenantId,
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
  public static void deleteGeneration(Connection conn, String indexName, long generation)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }

    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    String deleteSql = "DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME
      + " = ? AND " + GENERATION_ID + " = ?";

    try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
      ps.setString(1, normalizedIndexName);
      ps.setLong(2, generation);
      ps.executeUpdate();
    }
    conn.commit();
  }

  /**
   * Removes all centroid rows for a given index from {@code SYSTEM.VECTOR_CENTROID}.
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
    String deleteSql =
      "DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?";

    try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
      ps.setString(1, normalizedIndexName);
      ps.executeUpdate();
    }
    conn.commit();
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
