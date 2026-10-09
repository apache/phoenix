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

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.ServerBuildIndexCompiler;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rebuilds IVF vector indexes to new centroid generations, and reconciles drift scorecards.
 * <p>
 * A rebuild trains a new centroid generation and records it as the building generation in
 * {@code SYSTEM.CATALOG}. Then it migrates the index online. During the migration, writers put
 * index rows under the building generation and delete the earlier index rows under both
 * generations. The migration runs a server side index build, then a catch-up build of the rows that
 * changed during the first build. At the end, the rebuild promotes the building generation to
 * active and deletes the centroid rows of the other generations.
 * <p>
 * A lease row in {@code SYSTEM.VECTOR_CENTROID} makes sure that only one worker at a time rebuilds
 * or reconciles an index. An interrupted rebuild keeps its migration state. A later reconciliation
 * queues a task that resumes the migration, and the resume is idempotent.
 */
public final class VectorIndexRebuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexRebuilder.class);

  /** Trigger reason of a rebuild that a reconciliation queues to resume a migration. */
  public static final String RESUME_REASON = "RESUME";
  /** Trigger reason of a rebuild that an operator starts. */
  public static final String MANUAL_REASON = "MANUAL";

  /** Result of a rebuild. */
  public enum Outcome {
    /** The index was built or migrated to a new centroid generation. */
    REBUILT,
    /** Drift was found, but automatic rebuild is disabled. */
    DISABLED,
    /** Drift was found, but the minimum interval after the last rebuild has not passed. */
    TOO_SOON,
    /** A different worker holds the rebuild claim of the index. */
    IN_PROGRESS,
    /** The index has no centroid generation, or too few sample vectors for training. */
    UNTRAINED,
    /** The index table does not exist. */
    NOT_FOUND,
    /** A resume request found no migration in progress. */
    NOT_MIGRATING,
    /** The index was rebuilt after the request. */
    UP_TO_DATE
  }

  /** Result of a scorecard reconciliation. */
  public enum ReconcileOutcome {
    /** The scorecard counts were reconciled. */
    RECONCILED,
    /**
     * No reconciliation ran, because it was not due, the index has no generation, a migration is in
     * progress, or a different worker holds the claim.
     */
    NOT_DUE,
    /** The index was dropped, and the centroid rows that it left were deleted. */
    INDEX_DROPPED
  }

  /**
   * Test hook that a migration calls after the full build and before the catch-up build, before the
   * promotion of the building generation.
   */
  interface MigrationHook {
    void migrating(PTable index, long buildingGeneration) throws Exception;
  }

  private static volatile MigrationHook migrationHookForTesting;

  /** Sets the test hook that a migration calls between its two builds. */
  static void setMigrationHookForTesting(MigrationHook hook) {
    migrationHookForTesting = hook;
  }

  private VectorIndexRebuilder() {
  }

  /**
   * Runs or resumes a rebuild of a vector index.
   * <p>
   * A manual rebuild ignores the automatic rebuild flag and the minimum interval. It trains an
   * index that has no centroid generation. After the rebuild, it activates an index that is not
   * active, unless the index was disabled during the rebuild.
   * <p>
   * {@link #reconcile} queues an automatic rebuild after it finds drift. An automatic rebuild
   * checks only the automatic rebuild flag and the minimum interval again. A rebuild with
   * {@link #RESUME_REASON} only completes a migration in progress. It activates only an index that
   * the interrupted rebuild left in the building state.
   * @param conn      the Phoenix connection
   * @param indexName the full physical name of the index table
   * @param manual    true if a user or an operator started this rebuild
   * @param reason    the reason for diagnostics, or, for an automatic rebuild, the drift assessment
   *                  that queued it
   */
  public static Outcome rebuild(PhoenixConnection conn, String indexName, boolean manual,
    String reason) throws SQLException {
    return rebuild(conn, indexName, manual, reason, Long.MAX_VALUE);
  }

  /**
   * Runs or resumes a rebuild requested at {@code requestedAt}, as the overload without
   * {@code requestedAt} does, unless the index was rebuilt after the request. Generation IDs are
   * timestamps, and a rebuild trains its generation after the request. If an active index has no
   * migration in progress and its generation is newer than the request, a rebuild already served
   * the request. That rebuild can be an earlier run of the same request, on a RegionServer that no
   * longer hosts {@code SYSTEM.TASK}. That RegionServer lost the task before a sweep recorded the
   * result.
   */
  public static Outcome rebuild(PhoenixConnection conn, String indexName, boolean manual,
    String reason, long requestedAt) throws SQLException {
    try (PhoenixConnection internal = CentroidManager.newInternalConnection(conn);
      CentroidManager.RebuildClaim claim =
        CentroidManager.claim(internal, indexName, UUID.randomUUID().toString())) {
      if (claim == null) {
        return Outcome.IN_PROGRESS;
      }
      PTable index;
      PTable dataTable;
      try {
        index = internal.getTableNoCache(indexName);
        dataTable = internal.getTableNoCache(index.getParentName().getString());
      } catch (TableNotFoundException e) {
        return Outcome.NOT_FOUND;
      }
      boolean resume = RESUME_REASON.equals(reason);
      if (resume && !index.isVectorRebuildInProgress()) {
        return Outcome.NOT_MIGRATING;
      }
      if (
        !resume && !index.isVectorRebuildInProgress() && index.getIndexState() == PIndexState.ACTIVE
          && index.getVectorCentroidGeneration() != null
          && index.getVectorCentroidGeneration() > requestedAt
      ) {
        return Outcome.UP_TO_DATE;
      }
      if (
        manual && !resume && index.getIndexState() != PIndexState.ACTIVE
          && index.getIndexState() != PIndexState.BUILDING
      ) {
        // Writers maintain an index in the building state, and the rebuild activates it at the
        // end, as ALTER INDEX ... REBUILD does for other indexes. A resumed migration does not
        // change the state, so it does not enable again an index that an operator disabled.
        IndexUtil.updateIndexState(internal, indexName, PIndexState.BUILDING, 0L);
        index = internal.getTableNoCache(indexName);
      }
      if (index.getVectorCentroidGeneration() == null) {
        return manual ? trainAndBuild(internal, claim, index, dataTable) : Outcome.UNTRAINED;
      }
      return rebuild(internal, claim, index, dataTable, manual, reason);
    }
  }

  /** Trains the first centroid generation of an index that has none, then builds the index. */
  private static Outcome trainAndBuild(PhoenixConnection conn, CentroidManager.RebuildClaim claim,
    PTable index, PTable dataTable) throws SQLException {
    Long generation = VectorIndexTrainer.trainAndRecord(conn, dataTable, index);
    if (generation == null) {
      return Outcome.UNTRAINED;
    }
    // Wait until client metadata caches see the generation. Index maintenance starts then.
    awaitMetadataRefresh(conn);
    index = refresh(conn, index);
    migrate(conn, claim, index, dataTable, generation);
    VectorIndexScorecard.reconcile(conn, index, generation);
    activate(conn, index.getName().getString());
    LOGGER.info("Built vector index {} at generation {}", index.getName().getString(), generation);
    return Outcome.REBUILT;
  }

  private static Outcome rebuild(PhoenixConnection conn, CentroidManager.RebuildClaim claim,
    PTable index, PTable dataTable, boolean manual, String reason) throws SQLException {
    String indexName = index.getName().getString();
    long active = index.getVectorCentroidGeneration();
    ReadOnlyProps props = conn.getQueryServices().getProps();
    GenerationSummary activeSummary =
      CentroidManager.loadGenerationSummary(conn, indexName, active);
    long building;
    if (
      index.isVectorRebuildInProgress() && !CentroidManager
        .loadCentroids(conn, indexName, index.getVectorBuildingGeneration()).isEmpty()
    ) {
      building = index.getVectorBuildingGeneration();
      LOGGER.info("Resuming the migration of vector index {} to generation {}", indexName,
        building);
    } else {
      if (manual) {
        VectorIndexScorecard.Assessment assessment =
          VectorIndexScorecard.assess(VectorIndexScorecard.reconcile(conn, index, active), props);
        CentroidManager.persistGenerationSummary(conn, indexName, active,
          new GenerationSummary(null, assessment.getReason(), null, null, null, null));
      } else {
        // The reconciliation that queued this rebuild assessed drift on fresh counts. A second
        // reconciliation would reset the reassignment counts that the assessment used, so check
        // only the operational gates again.
        if (
          !props.getBoolean(QueryServices.VECTOR_REBUILD_AUTO_ENABLED_ATTRIB,
            QueryServicesOptions.DEFAULT_VECTOR_REBUILD_AUTO_ENABLED)
        ) {
          return Outcome.DISABLED;
        }
        // The generation ID is a timestamp, so it is a lower bound for the last rebuild time
        long lastRebuild = activeSummary != null && activeSummary.getLastRebuildTime() != null
          ? Math.max(activeSummary.getLastRebuildTime(), active)
          : active;
        if (
          EnvironmentEdgeManager.currentTimeMillis() - lastRebuild
              < props.getLong(QueryServices.VECTOR_REBUILD_MIN_INTERVAL_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_VECTOR_REBUILD_MIN_INTERVAL_MS)
        ) {
          return Outcome.TOO_SOON;
        }
      }
      Integer requestedLists = activeSummary != null ? activeSummary.getRequestedLists() : null;
      int lists = requestedLists != null ? requestedLists : index.getVectorIvfLists();
      KMeansResult model = VectorIndexTrainer.train(conn, dataTable, index, lists);
      if (model == null) {
        return Outcome.UNTRAINED;
      }
      building = CentroidManager.nextGeneration(active);
      // Start the new centroid IDs after the outgoing range. Then a new index row key cannot
      // collide with a delete marker on an outgoing index row.
      CachedCentroids outgoing =
        VectorCentroidCache.getInstance(conn.getQueryServices().getConfiguration()).get(conn,
          indexName, active, DistanceMetric.fromString(index.getVectorDistanceMetric()));
      int firstId = outgoing.getFirstId() + outgoing.size();
      CentroidManager.persistCentroids(conn, indexName, building, model.getCentroids(), firstId);
      CentroidManager.persistGenerationSummary(conn, indexName, building, new GenerationSummary(
        GenerationSummary.BUILDING, reason, lists, model.getSkewMetrics(), null, null));
      VectorCentroidCache.getInstance(conn.getQueryServices().getConfiguration()).put(indexName,
        building, new CachedCentroids(model.getCentroids(), model.getDistanceMetric(), firstId));
      claim.renew();
      CentroidManager.setBuildingGeneration(conn, index, building);
      LOGGER.info("Migrating vector index {} from generation {} to generation {}: {}", indexName,
        active, building, reason);
    }
    // Wait until client metadata caches see the building generation
    awaitMetadataRefresh(conn);
    index = refresh(conn, index);
    migrate(conn, claim, index, dataTable, building);

    List<float[]> centroids = CentroidManager.loadCentroids(conn, indexName, building);
    claim.renew();
    CentroidManager.setGenerationAndLists(conn, index, building, centroids.size());
    CentroidManager.persistGenerationSummary(conn, indexName, building,
      new GenerationSummary(GenerationSummary.ACTIVE, null, null, null,
        EnvironmentEdgeManager.currentTimeMillis(), null));
    // Wait until client metadata caches see the promoted generation
    awaitMetadataRefresh(conn);
    index = refresh(conn, index);
    retireInactiveGenerations(conn, index);
    VectorIndexScorecard.reconcile(conn, index, building);
    activate(conn, indexName);
    LOGGER.info("Rebuilt vector index {} to generation {}", indexName, building);
    return Outcome.REBUILT;
  }

  /**
   * Activates an index that the rebuild left in the building state. The state is read again,
   * because an operator can disable the index during the rebuild. An index disabled before the read
   * stays disabled. The metadata endpoint refuses to activate an index disabled after the read. In
   * both cases the completed rebuild stays, because its generation is already committed.
   */
  static void activate(PhoenixConnection conn, String indexName) throws SQLException {
    if (conn.getTableNoCache(indexName).getIndexState() != PIndexState.BUILDING) {
      return;
    }
    try {
      IndexUtil.updateIndexState(conn, indexName, PIndexState.ACTIVE, 0L);
    } catch (SQLException e) {
      if (e.getErrorCode() != SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode()) {
        throw e;
      }
      LOGGER.info("Vector index {} was disabled during its rebuild and stays disabled", indexName);
    }
  }

  /**
   * Migrates the index in two builds: a full server side index build, then a catch-up build of the
   * rows that changed during the first build.
   */
  private static void migrate(PhoenixConnection conn, CentroidManager.RebuildClaim claim,
    PTable index, PTable dataTable, long building) throws SQLException {
    long start = EnvironmentEdgeManager.currentTimeMillis();
    build(conn, index, dataTable, 0, start);
    claim.renew();
    MigrationHook hook = migrationHookForTesting;
    if (hook != null) {
      try {
        hook.migrating(index, building);
      } catch (Exception e) {
        throw new SQLException(e);
      }
    }
    // A build over a time range that starts after zero selects each data row that has a cell or
    // delete marker in the range. It looks only at the column families that the index reads. The
    // build makes the index rows of each such data row again from its full state.
    build(conn, index, dataTable, start, EnvironmentEdgeManager.currentTimeMillis());
    claim.renew();
  }

  /** Runs the server side index build over the data cells written in [min, max). */
  private static void build(PhoenixConnection conn, PTable index, PTable dataTable, long min,
    long max) throws SQLException {
    MutationPlan plan = new ServerBuildIndexCompiler(conn,
      SchemaUtil.getEscapedFullTableName(dataTable.getName().getString())).compile(index);
    setTimeRange(plan.getContext().getScan(), min, max);
    conn.getQueryServices().updateData(plan);
  }

  /**
   * Reconciles the scorecard of the active generation when it is due, assesses drift, and deletes
   * the centroid rows of retired generations. It queues a rebuild task if it finds drift and
   * automatic rebuild is enabled. For a migration in progress, it queues a resume task.
   * @param conn      the Phoenix connection
   * @param indexName the full physical name of the index table
   */
  public static ReconcileOutcome reconcile(PhoenixConnection conn, String indexName)
    throws SQLException {
    try (PhoenixConnection internal = CentroidManager.newInternalConnection(conn)) {
      PTable index;
      try {
        index = internal.getTableNoCache(indexName);
      } catch (TableNotFoundException e) {
        // The index was dropped, so delete the centroid rows that it left
        CentroidManager.deleteAllCentroids(internal, indexName);
        return ReconcileOutcome.INDEX_DROPPED;
      }
      Long active = index.getVectorCentroidGeneration();
      if (active == null) {
        return ReconcileOutcome.NOT_DUE;
      }
      ReadOnlyProps props = internal.getQueryServices().getProps();
      GenerationSummary summary =
        CentroidManager.loadGenerationSummary(internal, indexName, active);
      boolean due = isDue(summary == null ? null : summary.getLastScorecardUpdate(),
        props.getLong(QueryServices.VECTOR_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB,
          QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_RECONCILE_INTERVAL_MS),
        EnvironmentEdgeManager.currentTimeMillis());
      List<Long> generations = CentroidManager.listGenerations(internal, indexName);
      boolean retire = generations.size() > (index.isVectorRebuildInProgress() ? 2 : 1);
      if (!due && !retire && !index.isVectorRebuildInProgress()) {
        return ReconcileOutcome.NOT_DUE;
      }
      try (CentroidManager.RebuildClaim claim =
        CentroidManager.claim(internal, indexName, UUID.randomUUID().toString())) {
        if (claim == null) {
          return ReconcileOutcome.NOT_DUE;
        }
        retireInactiveGenerations(internal, index);
        if (index.isVectorRebuildInProgress()) {
          CentroidManager.enqueueTask(internal, index, TaskType.VECTOR_INDEX_REBUILD,
            rebuildTaskData(true, RESUME_REASON));
          return ReconcileOutcome.NOT_DUE;
        }
        if (!due) {
          return ReconcileOutcome.NOT_DUE;
        }
        VectorIndexScorecard.Assessment assessment = VectorIndexScorecard
          .assess(VectorIndexScorecard.reconcile(internal, index, active), props);
        CentroidManager.persistGenerationSummary(internal, indexName, active,
          new GenerationSummary(null, assessment.getReason(), null, null, null, null));
        if (
          assessment.isDrifted()
            && props.getBoolean(QueryServices.VECTOR_REBUILD_AUTO_ENABLED_ATTRIB,
              QueryServicesOptions.DEFAULT_VECTOR_REBUILD_AUTO_ENABLED)
        ) {
          CentroidManager.enqueueTask(internal, index, TaskType.VECTOR_INDEX_REBUILD,
            rebuildTaskData(false, assessment.getReason()));
        }
        return ReconcileOutcome.RECONCILED;
      }
    }
  }

  /**
   * Returns true if no update was recorded, or if the interval has passed after the last update.
   * Returns false if the last update is in the future.
   */
  static boolean isDue(Long lastUpdate, long intervalMs, long now) {
    if (lastUpdate == null) {
      return true;
    }
    long elapsed = now - lastUpdate;
    return elapsed >= 0 && elapsed >= intervalMs;
  }

  /** Serializes the parameters of a background rebuild task as a JSON payload. */
  public static String rebuildTaskData(boolean manual, String reason) {
    return "{\"manual\":" + manual + ",\"reason\":\""
      + (reason == null ? "" : reason.replace("\"", "'")) + "\"}";
  }

  /** Deletes all rows of the generations that are neither active nor building. */
  private static void retireInactiveGenerations(PhoenixConnection conn, PTable index)
    throws SQLException {
    String indexName = index.getName().getString();
    for (long generation : CentroidManager.listGenerations(conn, indexName)) {
      if (
        !Long.valueOf(generation).equals(index.getVectorCentroidGeneration())
          && !Long.valueOf(generation).equals(index.getVectorBuildingGeneration())
      ) {
        CentroidManager.deleteGeneration(conn, indexName, generation);
      }
    }
  }

  /** Waits for the index population sleep time, so that client metadata caches can expire. */
  private static void awaitMetadataRefresh(PhoenixConnection conn) throws SQLException {
    long sleep =
      conn.getQueryServices().getProps().getLong(QueryServices.INDEX_POPULATION_SLEEP_TIME,
        QueryServicesOptions.DEFAULT_INDEX_POPULATION_SLEEP_TIME);
    if (sleep > 0) {
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e)
          .build().buildException();
      }
    }
  }

  private static PTable refresh(PhoenixConnection conn, PTable index) throws SQLException {
    String indexName = index.getName().getString();
    conn.removeTable(null, indexName, index.getParentName().getString(),
      org.apache.hadoop.hbase.HConstants.LATEST_TIMESTAMP);
    return conn.getTableNoCache(indexName);
  }

  private static void setTimeRange(Scan scan, long min, long max) throws SQLException {
    try {
      scan.setTimeRange(min, max);
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}
