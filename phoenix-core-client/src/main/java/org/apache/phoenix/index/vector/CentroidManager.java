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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Stores IVF centroid generations in {@code SYSTEM.VECTOR_CENTROID}, and records the generations of
 * each index in {@code SYSTEM.CATALOG}.
 * <p>
 * Centroid rows are keyed by the full index name, which is case sensitive. Generation IDs of an
 * index strictly increase. A re-created index takes its first generation ID from the wall clock. If
 * the clock does not go back, its IDs are higher than the IDs of the dropped index. Thus no cache
 * can serve the model of an earlier index with the same name.
 * <p>
 * Some methods commit or roll back the connection that they receive. A caller with uncommitted
 * mutations must give them an internal connection from {@link #newInternalConnection}.
 */
public final class CentroidManager {

  /** Centroid ID of the summary row of a generation and of the rebuild claim row. */
  public static final int SENTINEL_CENTROID_ID = -1;
  /** Generation ID of the rebuild claim row of an index. No centroid generation uses it. */
  static final long REBUILD_CLAIM_GENERATION = 0L;
  /** Lease period of a rebuild claim. A {@link RebuildClaim} renews the lease while it is open. */
  public static final long CLAIM_LEASE_MS = 10L * 60 * 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(CentroidManager.class);
  private static final ScheduledExecutorService CLAIM_RENEWAL =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("vector-index-claim-renewal").setDaemon(true).build());

  private CentroidManager() {
  }

  /**
   * Opens an internal connection with the client properties of {@code conn}, but without the tenant
   * ID and the SCN. Writes to system tables on this connection stay separate from the uncommitted
   * mutations of {@code conn}.
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
   * Returns a new generation ID from the wall clock. If {@code current} is not null, the ID is
   * always greater than {@code current}. Thus the IDs of an index strictly increase, even if the
   * clock goes back. If {@code current} is null, the ID is the wall clock time.
   */
  public static long nextGeneration(Long current) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    return current == null ? now : Math.max(current + 1, now);
  }

  /**
   * Returns the index name under which the centroids of a vector index are recorded. A view with a
   * WHERE clause inherits each index of its parent with the name {@code <view>#<index>}. The
   * inherited index uses the centroids of the parent index, so this method removes all view
   * prefixes.
   */
  public static String getCentroidIndexName(String indexName) {
    return indexName
      .substring(indexName.lastIndexOf(QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR) + 1);
  }

  /** Writes the centroid vectors of a generation with IDs from 0, and commits the connection. */
  public static void persistCentroids(Connection conn, String indexName, long generation,
    List<float[]> centroids) throws SQLException {
    persistCentroids(conn, indexName, generation, centroids, 0);
  }

  /**
   * Writes the centroids of a generation and commits. The centroids get consecutive IDs that start
   * at {@code firstId}.
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

  /** The centroids of a generation and the ID of its first centroid. */
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
   * Loads the centroid vectors of a generation in ascending centroid ID order. Returns an empty
   * list if the generation has no recorded centroids.
   */
  public static List<float[]> loadCentroids(Connection conn, String indexName, long generation)
    throws SQLException {
    return loadModel(conn, indexName, generation).getCentroids();
  }

  /**
   * Loads the centroids of a generation in centroid ID order, and the ID of the first centroid. The
   * model is empty if the generation has no centroids.
   */
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
   * Records the active centroid generation and the list count in {@code SYSTEM.CATALOG} through the
   * metadata endpoint, and returns the updated index. The endpoint keeps the current index state.
   * It advances the DDL timestamp, so that clients refresh their cached index metadata. This method
   * rolls back the uncommitted mutations of {@code conn}.
   */
  public static PTable setGenerationAndLists(PhoenixConnection conn, PTable index, long generation,
    int lists) throws SQLException {
    return recordGenerations(conn, index, generation, lists, null);
  }

  /**
   * Records the building generation of a rebuild migration in {@code SYSTEM.CATALOG}, and returns
   * the updated index.
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
   * Writes the fields of a summary that are not null to the summary row of a generation, and
   * commits. A null field does not change the stored column value.
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
   * Loads the scorecard of a generation, one row for each centroid, in centroid ID order.
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
   * Adds the cluster size and reassignment count of each row, as deltas, to the scorecard of a
   * generation, and commits. Each addition is atomic. RegionServer flushes and reconciliation both
   * use this method, so concurrent writers add to the counts and do not overwrite them.
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

  /** Lists the IDs of the centroid generations of an index, without the rebuild claim row. */
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
   * Takes the exclusive rebuild lease of an index for {@code token}, or renews the lease that
   * {@code token} holds. The operation is atomic. A different token can take a lease that is older
   * than {@code expiryMs}.
   * @return true if {@code token} holds the lease
   */
  public static boolean claimRebuild(Connection conn, String indexName, String token, long expiryMs)
    throws SQLException {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long expired = now - expiryMs;
    String sql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ", " + TRIGGER_REASON + ", " + LAST_REBUILD_TIME
      + ") VALUES (?, " + REBUILD_CLAIM_GENERATION + ", " + SENTINEL_CENTROID_ID
      + ", ?, ?) ON DUPLICATE KEY UPDATE " + TRIGGER_REASON + " = CASE WHEN " + LAST_REBUILD_TIME
      + " < ? OR " + TRIGGER_REASON + " = ? THEN ? ELSE " + TRIGGER_REASON + " END, "
      + LAST_REBUILD_TIME + " = CASE WHEN " + LAST_REBUILD_TIME + " < ? OR " + TRIGGER_REASON
      + " = ? THEN ? ELSE " + LAST_REBUILD_TIME + " END";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      ps.setString(2, token);
      ps.setLong(3, now);
      ps.setLong(4, expired);
      ps.setString(5, token);
      ps.setString(6, token);
      ps.setLong(7, expired);
      ps.setString(8, token);
      ps.setLong(9, now);
      ps.executeUpdate();
    }
    conn.commit();
    return holdsRebuild(conn, indexName, token);
  }

  /**
   * Extends the rebuild lease that {@code token} holds, but never takes a lease. This makes sure
   * that a holder whose lease expired and went to a different holder cannot take the index back.
   * This is also true after the different holder releases the lease.
   * @return true if {@code token} still holds the lease
   */
  private static boolean renewRebuild(Connection conn, String indexName, String token)
    throws SQLException {
    String sql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ") VALUES (?, " + REBUILD_CLAIM_GENERATION + ", "
      + SENTINEL_CENTROID_ID + ") ON DUPLICATE KEY UPDATE_ONLY " + LAST_REBUILD_TIME
      + " = CASE WHEN " + TRIGGER_REASON + " = ? THEN ? ELSE " + LAST_REBUILD_TIME + " END";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      ps.setString(2, token);
      ps.setLong(3, EnvironmentEdgeManager.currentTimeMillis());
      ps.executeUpdate();
    }
    conn.commit();
    return holdsRebuild(conn, indexName, token);
  }

  private static boolean holdsRebuild(Connection conn, String indexName, String token)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT " + TRIGGER_REASON + " FROM "
      + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = "
      + REBUILD_CLAIM_GENERATION + " AND " + CENTROID_ID + " = " + SENTINEL_CENTROID_ID)) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && token.equals(rs.getString(1));
      }
    }
  }

  /**
   * Takes the rebuild claim of an index for {@code token}. The returned claim renews its lease in
   * the background until it is closed. If the holder stops, the index becomes free within one
   * {@link #CLAIM_LEASE_MS}, however long the claimed work runs.
   * @return the claim, or null if a different holder has the claim
   */
  public static RebuildClaim claim(PhoenixConnection conn, String indexName, String token)
    throws SQLException {
    // The claim has its own connection for the renewal thread. Only code that holds the claim
    // lock uses this connection.
    PhoenixConnection claimConn = newInternalConnection(conn);
    try {
      if (claimRebuild(claimConn, indexName, token, CLAIM_LEASE_MS)) {
        return new RebuildClaim(claimConn, indexName, token);
      }
    } catch (SQLException | RuntimeException e) {
      claimConn.close();
      throw e;
    }
    claimConn.close();
    return null;
  }

  /**
   * A rebuild claim that renews its lease in the background while it is open. {@link #close()}
   * releases the claim.
   */
  public static final class RebuildClaim implements AutoCloseable {
    private final PhoenixConnection conn;
    private final String indexName;
    private final String token;
    private final ScheduledFuture<?> renewal;
    private boolean closed;

    private RebuildClaim(PhoenixConnection conn, String indexName, String token) {
      this.conn = conn;
      this.indexName = indexName;
      this.token = token;
      this.renewal = CLAIM_RENEWAL.scheduleWithFixedDelay(() -> {
        try {
          // Skip a renewal that runs after close(), which releases the claim and closes its
          // connection
          synchronized (RebuildClaim.this) {
            if (!closed) {
              renew();
            }
          }
        } catch (Throwable t) {
          LOGGER.warn("Could not renew the rebuild claim of vector index {}", indexName, t);
        }
      }, CLAIM_LEASE_MS / 4, CLAIM_LEASE_MS / 4, TimeUnit.MILLISECONDS);
    }

    /**
     * Renews the lease of this claim. Fails if the claim was released, or if its lease expired and
     * a different holder took the index. Work that must not run without the claim calls this first
     * to make sure that the claim is still held. A renewal never takes a lease again, so a lost
     * claim fails all later renewals.
     */
    public synchronized void renew() throws SQLException {
      if (closed || !renewRebuild(conn, indexName, token)) {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION)
          .setMessage("The rebuild claim of vector index " + indexName + " is no longer held")
          .build().buildException();
      }
    }

    @Override
    public void close() throws SQLException {
      renewal.cancel(false);
      try {
        synchronized (this) {
          closed = true;
          releaseRebuild(conn, indexName, token);
        }
      } finally {
        conn.close();
      }
    }
  }

  /** Releases the rebuild lease of an index if {@code token} holds it. */
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

  /** Returns true if the task is live, which means that it is not COMPLETED and not FAILED. */
  public static boolean isLive(Task.TaskRecord task) {
    String status = task.getStatus();
    return !TaskStatus.COMPLETED.toString().equals(status)
      && !TaskStatus.FAILED.toString().equals(status);
  }

  /**
   * Adds a task of the given type for the index to {@code SYSTEM.TASK}, unless a pending or active
   * task of that type exists.
   */
  public static void enqueueTask(PhoenixConnection conn, PTable index, TaskType taskType,
    String data) throws SQLException {
    String schemaName = index.getSchemaName().getString();
    String tableName = index.getTableName().getString();
    String tenantId = index.getTenantId() == null ? null : index.getTenantId().getString();
    for (Task.TaskRecord task : Task.queryTaskTable(conn, null, schemaName, tableName, taskType,
      tenantId, null)) {
      if (isLive(task)) {
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

  /** Deletes all rows of a generation: centroids, scorecard counts, and summary. */
  public static void deleteGeneration(Connection conn, String indexName, long generation)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ?")) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      executeAutoCommitted(conn, ps);
    }
  }

  /** Deletes the centroid rows of all generations of the index. */
  public static void deleteAllCentroids(Connection conn, String indexName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(
      "DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
      ps.setString(1, indexName);
      executeAutoCommitted(conn, ps);
    }
  }

  /**
   * Deletes the rebuild and scorecard reconciliation tasks of the index from {@code SYSTEM.TASK}.
   */
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
   * Runs a delete with autocommit on, so that the server deletes the rows. Then it restores the
   * autocommit setting of the caller.
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

  /** Returns the escaped full name of the table, for use in SQL statements. */
  static String escapedName(PTable table) {
    return SchemaUtil.getEscapedFullTableName(table.getName().getString());
  }
}
