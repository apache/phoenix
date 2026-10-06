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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_REBUILD_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_SCORECARD_UPDATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REASSIGN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REQUESTED_LISTS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SKEW_METRICS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRIGGER_REASON;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_BUILDING_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_CENTROID_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_LISTS;

import java.io.IOException;
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
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskStatus;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TaskMetaDataServiceCallBack;

/**
 * Manages persistence of IVF centroid generations in {@code SYSTEM.VECTOR_CENTROID} and active
 * generation metadata in {@code SYSTEM.CATALOG}.
 * <p>
 * Indexes are keyed by fully qualified physical name. Generation IDs increase monotonically across
 * index lifetimes to ensure cached model isolation. Operations execute using isolated internal
 * connections to prevent unintended commits of caller transaction state.
 */
public final class CentroidManager {

  /** Sentinel centroid ID for generation metadata summary rows. */
  public static final int SENTINEL_CENTROID_ID = -1;
  /** Reserved generation ID for atomic index rebuild claim locks. */
  static final long REBUILD_CLAIM_GENERATION = 0L;

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
    persistCentroids(conn, indexName, generation, centroids, 0);
  }

  /**
   * Persists trained centroid vectors for a generation with monotonically increasing centroid IDs
   * starting at {@code firstId}.
   */
  public static void persistCentroids(Connection conn, String indexName, long generation,
    List<float[]> centroids, int firstId) throws SQLException {
    String sql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ", " + CENTROID_VECTOR + ") VALUES (?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < centroids.size(); i++) {
        ps.setString(1, indexName);
        ps.setLong(2, generation);
        ps.setInt(3, firstId + i);
        ps.setBytes(4, PVectorFloat.INSTANCE.toBytes(centroids.get(i)));
        ps.executeUpdate();
      }
    }
    conn.commit();
  }

  /** Encapsulates trained centroid vectors and their starting identifier for a generation. */
  public static final class Model {
    private final int firstId;
    private final List<float[]> centroids;

    Model(int firstId, List<float[]> centroids) {
      this.firstId = firstId;
      this.centroids = centroids;
    }

    public int getFirstId() {
      return firstId;
    }

    public List<float[]> getCentroids() {
      return centroids;
    }
  }

  /**
   * Loads centroid vectors for the specified generation in ascending centroid ID order.
   */
  public static List<float[]> loadCentroids(Connection conn, String indexName, long generation)
    throws SQLException {
    return loadModel(conn, indexName, generation).getCentroids();
  }

  /** Loads the centroid model and starting ID for the specified generation. */
  public static Model loadModel(Connection conn, String indexName, long generation)
    throws SQLException {
    String sql =
      "SELECT " + CENTROID_VECTOR + ", " + CENTROID_ID + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
        + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID
        + " >= 0 AND " + CENTROID_VECTOR + " IS NOT NULL ORDER BY " + CENTROID_ID;
    List<float[]> centroids = new ArrayList<>();
    int firstId = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          if (centroids.isEmpty()) {
            firstId = rs.getInt(2);
          }
          byte[] b = rs.getBytes(1);
          centroids.add(PVectorFloat.readElements(b, 0, b.length));
        }
      }
    }
    return new Model(firstId, centroids);
  }

  /**
   * Updates the active centroid generation and list count in {@code SYSTEM.CATALOG} via the
   * metadata endpoint, preserving current index state and advancing DDL timestamps for cache
   * invalidation.
   */
  public static PTable setGenerationAndLists(PhoenixConnection conn, PTable index, long generation,
    int lists) throws SQLException {
    return recordGenerations(conn, index, generation, lists, null);
  }

  /**
   * Updates {@code SYSTEM.CATALOG} with the target building generation for an active index rebuild.
   */
  public static PTable setBuildingGeneration(PhoenixConnection conn, PTable index, long generation)
    throws SQLException {
    return recordGenerations(conn, index, null, null, generation);
  }

  private static PTable recordGenerations(PhoenixConnection conn, PTable index, Long active,
    Integer lists, Long building) throws SQLException {
    String schemaName = index.getSchemaName().getString();
    String tableName = index.getTableName().getString();
    StringBuilder columns =
      new StringBuilder(TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + "," + INDEX_STATE);
    StringBuilder values = new StringBuilder("?, ?, ?, ?");
    if (active != null) {
      columns.append(",").append(VECTOR_CENTROID_GENERATION).append(",").append(VECTOR_IVF_LISTS);
      values.append(", ?, ?");
    }
    if (building != null) {
      columns.append(",").append(VECTOR_BUILDING_GENERATION);
      values.append(", ?");
    }
    String sql = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"("
      + columns + ") VALUES (" + values + ")";
    boolean autoCommit = conn.getAutoCommit();
    List<Mutation> tableMetadata;
    conn.setAutoCommit(false);
    try {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = 1;
        ps.setString(i++, index.getTenantId() == null ? null : index.getTenantId().getString());
        ps.setString(i++, schemaName.isEmpty() ? null : schemaName);
        ps.setString(i++, tableName);
        ps.setString(i++, index.getIndexState().getSerializedValue());
        if (active != null) {
          ps.setLong(i++, active);
          ps.setInt(i++, lists);
        }
        if (building != null) {
          ps.setLong(i++, building);
        }
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
        .setMessage("Could not record centroid generation " + (active != null ? active : building)
          + ": " + result.getMutationCode())
        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
    }
    return result.getTable();
  }

  /**
   * Upserts non-null generation metadata attributes into the sentinel row.
   */
  public static void persistGenerationSummary(Connection conn, String indexName, long generation,
    GenerationSummary summary) throws SQLException {
    List<String> columns = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    addIfNotNull(columns, values, REBUILD_STATE, summary.getRebuildState());
    addIfNotNull(columns, values, TRIGGER_REASON, summary.getTriggerReason());
    addIfNotNull(columns, values, REQUESTED_LISTS, summary.getRequestedLists());
    addIfNotNull(columns, values, SKEW_METRICS,
      summary.getSkewMetrics() == null ? null : summary.getSkewMetrics().toBytes());
    addIfNotNull(columns, values, LAST_REBUILD_TIME, summary.getLastRebuildTime());
    addIfNotNull(columns, values, LAST_SCORECARD_UPDATE, summary.getLastScorecardUpdate());
    StringBuilder sql = new StringBuilder("UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " ("
      + INDEX_NAME + ", " + GENERATION_ID + ", " + CENTROID_ID);
    for (String column : columns) {
      sql.append(", ").append(column);
    }
    sql.append(") VALUES (?, ?, ?");
    for (int i = 0; i < columns.size(); i++) {
      sql.append(", ?");
    }
    sql.append(")");
    try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      ps.setInt(3, SENTINEL_CENTROID_ID);
      for (int i = 0; i < values.size(); i++) {
        ps.setObject(i + 4, values.get(i));
      }
      ps.executeUpdate();
    }
    conn.commit();
  }

  private static void addIfNotNull(List<String> columns, List<Object> values, String column,
    Object value) {
    if (value != null) {
      columns.add(column);
      values.add(value);
    }
  }

  /** Reads the summary of one generation, or returns null if none was written. */
  public static GenerationSummary loadGenerationSummary(Connection conn, String indexName,
    long generation) throws SQLException {
    String sql = "SELECT " + REBUILD_STATE + ", " + TRIGGER_REASON + ", " + REQUESTED_LISTS + ", "
      + SKEW_METRICS + ", " + LAST_REBUILD_TIME + ", " + LAST_SCORECARD_UPDATE + " FROM "
      + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID
      + " = ? AND " + CENTROID_ID + " = " + SENTINEL_CENTROID_ID;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        byte[] skew = rs.getBytes(4);
        return new GenerationSummary(rs.getString(1), rs.getString(2), (Integer) rs.getObject(3),
          skew == null ? null : ClusterSkewMetrics.fromBytes(skew), (Long) rs.getObject(5),
          (Long) rs.getObject(6));
      }
    }
  }

  /**
   * Loads scorecard statistics for the specified generation ordered by centroid ID.
   */
  public static List<ScorecardRow> loadScorecard(Connection conn, String indexName, long generation)
    throws SQLException {
    String sql = "SELECT " + CENTROID_ID + ", " + CLUSTER_SIZE + ", " + REASSIGN_COUNT + " FROM "
      + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID
      + " = ? AND " + CENTROID_ID + " >= 0 AND " + CENTROID_VECTOR + " IS NOT NULL ORDER BY "
      + CENTROID_ID;
    List<ScorecardRow> rows = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          rows.add(new ScorecardRow(rs.getInt(1), rs.getLong(2), rs.getLong(3)));
        }
      }
    }
    return rows;
  }

  /**
   * Atomically adds the cluster size and reassignment count of each row, read as deltas, to the
   * scorecard of the specified generation and commits. RegionServer flushes and reconciliation both
   * write through this increment, so concurrent writers compose rather than overwrite one another.
   */
  public static void adjustScorecard(Connection conn, String indexName, long generation,
    List<ScorecardRow> deltas) throws SQLException {
    String sql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ", " + CLUSTER_SIZE + ", " + REASSIGN_COUNT
      + ") VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE " + CLUSTER_SIZE + " = COALESCE("
      + CLUSTER_SIZE + ", 0) + ?, " + REASSIGN_COUNT + " = COALESCE(" + REASSIGN_COUNT + ", 0) + ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (ScorecardRow delta : deltas) {
        ps.setString(1, indexName);
        ps.setLong(2, generation);
        ps.setInt(3, delta.getCentroidId());
        ps.setLong(4, delta.getClusterSize());
        ps.setLong(5, delta.getReassignCount());
        ps.setLong(6, delta.getClusterSize());
        ps.setLong(7, delta.getReassignCount());
        ps.executeUpdate();
      }
    }
    conn.commit();
  }

  /** Lists distinct generation IDs present in {@code SYSTEM.VECTOR_CENTROID}. */
  public static List<Long> listGenerations(Connection conn, String indexName) throws SQLException {
    String sql = "SELECT DISTINCT " + GENERATION_ID + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " > " + REBUILD_CLAIM_GENERATION;
    List<Long> generations = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          generations.add(rs.getLong(1));
        }
      }
    }
    return generations;
  }

  /**
   * Atomically acquires an exclusive rebuild lease in {@code SYSTEM.VECTOR_CENTROID}. Stale leases
   * older than {@code expiryMs} may be preempted.
   * @return true if the lease was successfully acquired by {@code token}
   */
  public static boolean claimRebuild(Connection conn, String indexName, String token, long expiryMs)
    throws SQLException {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long expired = now - expiryMs;
    String sql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ", " + TRIGGER_REASON + ", " + LAST_REBUILD_TIME
      + ") VALUES (?, " + REBUILD_CLAIM_GENERATION + ", " + SENTINEL_CENTROID_ID
      + ", ?, ?) ON DUPLICATE KEY UPDATE " + TRIGGER_REASON + " = CASE WHEN " + LAST_REBUILD_TIME
      + " < ? THEN ? ELSE " + TRIGGER_REASON + " END, " + LAST_REBUILD_TIME + " = CASE WHEN "
      + LAST_REBUILD_TIME + " < ? THEN ? ELSE " + LAST_REBUILD_TIME + " END";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      ps.setString(2, token);
      ps.setLong(3, now);
      ps.setLong(4, expired);
      ps.setString(5, token);
      ps.setLong(6, expired);
      ps.setLong(7, now);
      ps.executeUpdate();
    }
    conn.commit();
    try (PreparedStatement ps = conn.prepareStatement("SELECT " + TRIGGER_REASON + " FROM "
      + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = "
      + REBUILD_CLAIM_GENERATION + " AND " + CENTROID_ID + " = " + SENTINEL_CENTROID_ID)) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && token.equals(rs.getString(1));
      }
    }
  }

  /** Releases the rebuild lease held by {@code token}. */
  public static void releaseRebuild(Connection conn, String indexName, String token)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = " + REBUILD_CLAIM_GENERATION
      + " AND " + CENTROID_ID + " = " + SENTINEL_CENTROID_ID + " AND " + TRIGGER_REASON + " = ?")) {
      ps.setString(1, indexName);
      ps.setString(2, token);
      executeAutoCommitted(conn, ps);
    }
  }

  /**
   * Enqueues an index task in {@code SYSTEM.TASK} if no active task of the given type exists.
   */
  public static void enqueueTask(PhoenixConnection conn, PTable index, TaskType taskType,
    String data) throws SQLException {
    String schemaName = index.getSchemaName().getString();
    String tableName = index.getTableName().getString();
    String tenantId = index.getTenantId() == null ? null : index.getTenantId().getString();
    for (Task.TaskRecord task : Task.queryTaskTable(conn, null, schemaName, tableName, taskType,
      tenantId, null)) {
      String status = task.getStatus();
      if (
        !TaskStatus.COMPLETED.toString().equals(status)
          && !TaskStatus.FAILED.toString().equals(status)
      ) {
        return;
      }
    }
    try {
      List<Mutation> mutations = Task.getMutationsForAddTask(
        new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn).setTaskType(taskType)
          .setTenantId(tenantId).setSchemaName(schemaName.isEmpty() ? null : schemaName)
          .setTableName(tableName).setData(data).build());
      MetaDataMutationResult result = Task.taskMetaDataCoprocessorExec(conn,
        mutations.get(0).getRow(), new TaskMetaDataServiceCallBack(mutations));
      if (MutationCode.UNABLE_TO_UPSERT_TASK.equals(result.getMutationCode())) {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNABLE_TO_UPSERT_TASK)
          .setSchemaName(schemaName).setTableName(tableName).build().buildException();
      }
    } catch (IOException e) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNABLE_TO_UPSERT_TASK).setRootCause(e)
        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
    }
  }

  /** Deletes all centroid and scorecard rows for the specified generation. */
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

  /** Removes vector index rebuild and reconciliation tasks from {@code SYSTEM.TASK}. */
  public static void deleteTasks(Connection conn, PTable index) throws SQLException {
    String schemaName = index.getSchemaName().getString();
    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM "
      + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME + " WHERE " + PhoenixDatabaseMetaData.TASK_TYPE
      + " IN (" + TaskType.VECTOR_SCORECARD_RECONCILE.getSerializedValue() + ", "
      + TaskType.VECTOR_INDEX_REBUILD.getSerializedValue() + ") AND " + TABLE_NAME + " = ? AND "
      + (schemaName.isEmpty() ? TABLE_SCHEM + " IS NULL" : TABLE_SCHEM + " = ?"))) {
      ps.setString(1, index.getTableName().getString());
      if (!schemaName.isEmpty()) {
        ps.setString(2, schemaName);
      }
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
