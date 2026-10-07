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
package org.apache.phoenix.mapreduce.index.fsck.ivf;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_VECTOR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_REBUILD_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRIGGER_REASON;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_INDEX_ALGORITHM;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.task.Task.TaskRecord;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Strings;

/**
 * Data access reader for IVF metadata, centroid models, scorecard state, and background task
 * records.
 */
public class IvfIndexReader {
  private final IvfIndexContext context;
  private final PhoenixConnection conn;
  private final String indexName;

  public IvfIndexReader(IvfIndexContext context) {
    this.context = context;
    this.conn = context.getInternalConnection();
    this.indexName = context.getIndexName();
  }

  /** Centroid record containing centroid ID and vector components. */
  public static final class CentroidRow {
    private final int id;
    private final float[] vector;

    CentroidRow(int id, float[] vector) {
      this.id = id;
      this.vector = vector;
    }

    public int getId() {
      return id;
    }

    public float[] getVector() {
      return vector;
    }
  }

  /** Rebuild claim lease token and acquisition timestamp. */
  public static final class Claim {
    private final String token;
    private final long time;

    Claim(String token, long time) {
      this.token = token;
      this.time = time;
    }

    public String getToken() {
      return token;
    }

    public long getTime() {
      return time;
    }
  }

  /** Returns the non-tenant internal connection. */
  public PhoenixConnection getConnection() {
    return conn;
  }

  public List<Long> listGenerations() throws SQLException {
    return CentroidManager.listGenerations(conn, indexName);
  }

  /** Loads all centroid records for the given generation in ascending centroid ID order. */
  public List<CentroidRow> loadCentroidRows(long generation) throws SQLException {
    List<CentroidRow> rows = new ArrayList<>();
    try (
      PreparedStatement ps = conn.prepareStatement("SELECT " + CENTROID_ID + ", " + CENTROID_VECTOR
        + " FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND "
        + GENERATION_ID + " = ? AND " + CENTROID_ID + " >= 0 ORDER BY " + CENTROID_ID)) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          byte[] bytes = rs.getBytes(2);
          rows.add(new CentroidRow(rs.getInt(1),
            bytes == null ? null : PVectorFloat.readElements(bytes, 0, bytes.length)));
        }
      }
    }
    return rows;
  }

  public GenerationSummary loadSummary(long generation) throws SQLException {
    return CentroidManager.loadGenerationSummary(conn, indexName, generation);
  }

  public List<ScorecardRow> loadScorecard(long generation) throws SQLException {
    return CentroidManager.loadScorecard(conn, indexName, generation);
  }

  /** Loads the current rebuild claim lease, or null if unclaimed. */
  public Claim loadClaim() throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT " + TRIGGER_REASON + ", "
      + LAST_REBUILD_TIME + " FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME
      + " = ? AND " + GENERATION_ID + " = 0 AND " + CENTROID_ID + " = -1")) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? new Claim(rs.getString(1), rs.getLong(2)) : null;
      }
    }
  }

  /** Queries active (pending or running) background tasks of the given type for this index. */
  public List<TaskRecord> loadLiveTasks(TaskType taskType) throws SQLException {
    List<TaskRecord> live = new ArrayList<>();
    for (TaskRecord task : Task.queryTaskTable(conn, null,
      context.getIndexTable().getSchemaName().getString(),
      context.getIndexTable().getTableName().getString(), taskType, tenantId(), null)) {
      if (CentroidManager.isLive(task)) {
        live.add(task);
      }
    }
    return live;
  }

  /** Returns physical posting counts grouped by centroid ID across the index table. */
  public Map<Integer, Long> countPostings() throws SQLException {
    return VectorIndexScorecard.countPostings(conn, context.getIndexTable());
  }

  /**
   * Identifies orphaned vector metadata or background tasks belonging to indexes that no longer
   * exist.
   * @return map of index name to description of orphaned state
   */
  public Map<String, String> findOrphans() throws SQLException {
    TreeSet<String> withCentroids = new TreeSet<>();
    try (
      PreparedStatement ps = conn
        .prepareStatement("SELECT DISTINCT " + INDEX_NAME + " FROM " + SYSTEM_VECTOR_CENTROID_NAME);
      ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        withCentroids.add(rs.getString(1));
      }
    }
    TreeSet<String> withTasks = new TreeSet<>();
    try (
      PreparedStatement ps = conn.prepareStatement(
        "SELECT " + TABLE_SCHEM + ", " + TABLE_NAME + " FROM " + SYSTEM_TASK_NAME + " WHERE "
          + TASK_TYPE + " IN (" + TaskType.VECTOR_SCORECARD_RECONCILE.getSerializedValue() + ", "
          + TaskType.VECTOR_INDEX_REBUILD.getSerializedValue() + ")");
      ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        withTasks.add(SchemaUtil.getTableName(rs.getString(1), rs.getString(2)));
      }
    }
    Map<String, String> orphans = new LinkedHashMap<>();
    TreeSet<String> names = new TreeSet<>(withCentroids);
    names.addAll(withTasks);
    for (String name : names) {
      if (!vectorIndexExists(name)) {
        orphans.put(name,
          withCentroids.contains(name)
            ? (withTasks.contains(name) ? "centroids and tasks" : "centroids")
            : "tasks");
      }
    }
    return orphans;
  }

  /**
   * Checks whether a vector index exists in {@code SYSTEM.CATALOG} across all tenants.
   */
  public boolean vectorIndexExists(String fullName) throws SQLException {
    String schema = SchemaUtil.getSchemaNameFromFullName(fullName);
    try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM " + SYSTEM_CATALOG_NAME
      + " WHERE " + (schema.isEmpty() ? TABLE_SCHEM + " IS NULL" : TABLE_SCHEM + " = ?") + " AND "
      + TABLE_NAME + " = ? AND " + COLUMN_NAME + " IS NULL AND " + COLUMN_FAMILY + " IS NULL AND "
      + VECTOR_INDEX_ALGORITHM + " IS NOT NULL LIMIT 1")) {
      int i = 1;
      if (!schema.isEmpty()) {
        ps.setString(i++, schema);
      }
      ps.setString(i, SchemaUtil.getTableNameFromFullName(fullName));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** Purges orphaned vector tasks from {@code SYSTEM.TASK} for a dropped index. */
  public void deleteOrphanTasks(String fullName) throws SQLException {
    String schema = SchemaUtil.getSchemaNameFromFullName(fullName);
    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + SYSTEM_TASK_NAME + " WHERE "
      + TASK_TYPE + " IN (" + TaskType.VECTOR_SCORECARD_RECONCILE.getSerializedValue() + ", "
      + TaskType.VECTOR_INDEX_REBUILD.getSerializedValue() + ") AND "
      + (schema.isEmpty() ? TABLE_SCHEM + " IS NULL" : TABLE_SCHEM + " = ?") + " AND " + TABLE_NAME
      + " = ?")) {
      int i = 1;
      if (!schema.isEmpty()) {
        ps.setString(i++, schema);
      }
      ps.setString(i, SchemaUtil.getTableNameFromFullName(fullName));
      ps.executeUpdate();
      conn.commit();
    }
  }

  /**
   * Exports a centroid generation as a structured map containing centroids, summary metadata, and
   * scorecard statistics.
   */
  public Map<String, Object> exportGeneration(long generation) throws SQLException {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("generation", generation);
    List<Map<String, Object>> centroids = new ArrayList<>();
    for (CentroidRow row : loadCentroidRows(generation)) {
      Map<String, Object> centroid = new LinkedHashMap<>();
      centroid.put("id", row.getId());
      centroid.put("vector", row.getVector());
      centroids.add(centroid);
    }
    export.put("centroids", centroids);
    GenerationSummary summary = loadSummary(generation);
    if (summary != null) {
      Map<String, Object> s = new LinkedHashMap<>();
      s.put("rebuildState", summary.getRebuildState());
      s.put("triggerReason", summary.getTriggerReason());
      s.put("requestedLists", summary.getRequestedLists());
      s.put("lastRebuildTime", summary.getLastRebuildTime());
      s.put("lastScorecardUpdate", summary.getLastScorecardUpdate());
      export.put("summary", s);
    }
    List<Map<String, Object>> scorecard = new ArrayList<>();
    for (ScorecardRow row : loadScorecard(generation)) {
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("centroidId", row.getCentroidId());
      r.put("clusterSize", row.getClusterSize());
      r.put("reassignCount", row.getReassignCount());
      scorecard.add(r);
    }
    export.put("scorecard", scorecard);
    return export;
  }

  private String tenantId() {
    return context.getIndexTable().getTenantId() == null
      ? null
      : Strings.emptyToNull(context.getIndexTable().getTenantId().getString());
  }
}
