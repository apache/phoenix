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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

/**
 * Manages persistence of IVF centroid generations in {@code SYSTEM.VECTOR_CENTROID} and active
 * generation metadata in {@code SYSTEM.CATALOG}.
 * <p>
 * Indexes are keyed by fully qualified physical name. Generation IDs increase monotonically across
 * index lifetimes to ensure cached model isolation. Operations execute using isolated internal
 * connections to prevent unintended commits of caller transaction state.
 */
public final class CentroidManager {

  private CentroidManager() {
  }

  /**
   * Opens an internal connection with tenant and SCN properties cleared to isolate system table
   * mutations from caller connection state.
   */
  public static PhoenixConnection newInternalConnection(PhoenixConnection conn)
    throws SQLException {
    Properties props = new Properties();
    Properties clientInfo = conn.getClientInfo();
    for (String key : clientInfo.stringPropertyNames()) {
      if (
        !PhoenixRuntime.TENANT_ID_ATTRIB.equals(key)
          && !PhoenixRuntime.CURRENT_SCN_ATTRIB.equals(key)
      ) {
        props.setProperty(key, clientInfo.getProperty(key));
      }
    }
    return new PhoenixConnection(conn, conn.getQueryServices(), props);
  }

  /**
   * Generates a monotonically increasing generation ID derived from wall clock time.
   */
  public static long nextGeneration(Long current) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    return current == null ? now : Math.max(current + 1, now);
  }

  /** Persists centroid vectors for the specified generation and commits the transaction. */
  public static void persistCentroids(Connection conn, String indexName, long generation,
    List<float[]> centroids) throws SQLException {
    String sql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ", " + CENTROID_VECTOR + ") VALUES (?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < centroids.size(); i++) {
        ps.setString(1, indexName);
        ps.setLong(2, generation);
        ps.setInt(3, i);
        ps.setBytes(4, PVectorFloat.INSTANCE.toBytes(centroids.get(i)));
        ps.executeUpdate();
      }
    }
    conn.commit();
  }

  /**
   * Loads centroid vectors for the specified generation in ascending centroid ID order.
   */
  public static List<float[]> loadCentroids(Connection conn, String indexName, long generation)
    throws SQLException {
    String sql = "SELECT " + CENTROID_VECTOR + " FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE "
      + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID + " >= 0 AND "
      + CENTROID_VECTOR + " IS NOT NULL ORDER BY " + CENTROID_ID;
    List<float[]> centroids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          byte[] b = rs.getBytes(1);
          centroids.add(PVectorFloat.readElements(b, 0, b.length));
        }
      }
    }
    return centroids;
  }

  /**
   * Updates the active centroid generation and list count in {@code SYSTEM.CATALOG} via the
   * metadata endpoint, preserving current index state and advancing DDL timestamps for cache
   * invalidation.
   */
  public static PTable setGenerationAndLists(PhoenixConnection conn, PTable index, long generation,
    int lists) throws SQLException {
    String schemaName = index.getSchemaName().getString();
    String tableName = index.getTableName().getString();
    String sql = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"("
      + TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + "," + INDEX_STATE + ","
      + VECTOR_CENTROID_GENERATION + "," + VECTOR_IVF_LISTS + ") VALUES (?, ?, ?, ?, ?, ?)";
    boolean autoCommit = conn.getAutoCommit();
    List<Mutation> tableMetadata;
    conn.setAutoCommit(false);
    try {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, index.getTenantId() == null ? null : index.getTenantId().getString());
        ps.setString(2, schemaName.isEmpty() ? null : schemaName);
        ps.setString(3, tableName);
        ps.setString(4, index.getIndexState().getSerializedValue());
        ps.setLong(5, generation);
        ps.setInt(6, lists);
        ps.execute();
      }
      tableMetadata = conn.getMutationState().toMutations().next().getSecond();
    } finally {
      conn.rollback();
      conn.setAutoCommit(autoCommit);
    }
    MetaDataMutationResult result = conn.getQueryServices().updateIndexState(tableMetadata,
      index.getParentName() == null ? null : index.getParentName().getString());
    if (result.getMutationCode() == MutationCode.TABLE_NOT_FOUND) {
      throw new TableNotFoundException(schemaName, tableName);
    }
    if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION)
        .setMessage(
          "Could not record centroid generation " + generation + ": " + result.getMutationCode())
        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
    }
    return result.getTable();
  }

  /** Deletes all centroid rows for a specific generation. */
  public static void deleteGeneration(Connection conn, String indexName, long generation)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ?")) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      executeAutoCommitted(conn, ps);
    }
  }

  /** Deletes all centroid rows across all generations for the specified index. */
  public static void deleteAllCentroids(Connection conn, String indexName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(
      "DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
      ps.setString(1, indexName);
      executeAutoCommitted(conn, ps);
    }
  }

  /**
   * Executes a delete statement with autocommit enabled to allow server side range deletion.
   */
  private static void executeAutoCommitted(Connection conn, PreparedStatement ps)
    throws SQLException {
    boolean autoCommit = conn.getAutoCommit();
    conn.setAutoCommit(true);
    try {
      ps.executeUpdate();
    } finally {
      conn.setAutoCommit(autoCommit);
    }
  }

  /** Returns the escaped full table name for SQL statements. */
  static String escapedName(PTable table) {
    return SchemaUtil.getEscapedFullTableName(table.getName().getString());
  }
}
