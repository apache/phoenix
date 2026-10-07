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

import static org.apache.phoenix.query.QueryConstants.TRUE;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.ServerBuildIndexCompiler;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generational rebuild orchestration and drift reconciliation for IVF vector indexes.
 * <p>
 * Rebuilding trains a new centroid generation, marks the index in migrating state via
 * {@code SYSTEM.CATALOG}, and performs an online migration across centroid generations. Concurrent
 * writes target the building generation while purging prior index rows under either generation.
 * Online migration executes via a server side index build followed by catch up scan repair of
 * concurrent mutations, atomic generation promotion, and retirement of prior generations.
 * <p>
 * Distributed mutual exclusion is maintained using atomic lease rows in
 * {@code SYSTEM.VECTOR_CENTROID}. Interrupted rebuilds preserve migration state to enable
 * idempotent resumption during periodic reconciliation tasks.
 */
public final class VectorIndexRebuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexRebuilder.class);

  /** Trigger reason identifier for operator initiated index rebuilds. */
  public static final String MANUAL_REASON = "MANUAL";
  /** Maximum lease duration for rebuild locks before allowing preemption. */
  public static final long CLAIM_EXPIRY_MS = 24L * 60 * 60 * 1000;
  /**
   * Maximum threshold of concurrently changed rows for point catch-up repair before falling back to
   * full scan.
   */
  static final int MAX_CATCH_UP_ROWS = 10000;

  /** Status outcome of a rebuild execution. */
  public enum Outcome {
    /** Index successfully migrated to new centroid generation. */
    REBUILT,
    /** Drift detected but automatic background rebuild is disabled. */
    DISABLED,
    /** Drift detected but throttled by minimum rebuild interval. */
    TOO_SOON,
    /** Rebuild or reconciliation lock currently held by another worker. */
    IN_PROGRESS,
    /** Index is uninitialized or has insufficient sample vectors for training. */
    UNTRAINED,
    /** Target index table not found. */
    NOT_FOUND
  }

  /** Status outcome of a scorecard reconciliation execution. */
  public enum ReconcileOutcome {
    /** Scorecard row counts successfully reconciled. */
    RECONCILED,
    /** Reconciliation skipped due to interval throttling, uninitialized state, or active lock. */
    NOT_DUE,
    /** Target index dropped; orphaned centroid metadata purged. */
    INDEX_DROPPED
  }

  /**
   * Test lifecycle hook invoked during migration while the building generation is populated prior
   * to catalog promotion.
   */
  interface MigrationHook {
    void migrating(PTable index, long buildingGeneration) throws Exception;
  }

  private static volatile MigrationHook migrationHookForTesting;

  /** Injects test hook for synchronization during mid-migration state. */
  static void setMigrationHookForTesting(MigrationHook hook) {
    migrationHookForTesting = hook;
  }

  private VectorIndexRebuilder() {
  }

  /**
   * Executes or resumes a vector index rebuild. Manual rebuilds bypass the automatic rebuild flag
   * and throttle interval. Automatic rebuilds are queued by {@link #reconcile} once it has assessed
   * the index as drifted, and recheck only the automatic rebuild flag and minimum interval.
   * @param conn      Phoenix connection providing execution services
   * @param indexName fully qualified physical index table name
   * @param manual    whether this rebuild was explicitly triggered by user or operator action
   * @param reason    diagnostic reason, or for an automatic rebuild the drift assessment that
   *                  queued it
   */
  public static Outcome rebuild(PhoenixConnection conn, String indexName, boolean manual,
    String reason) throws SQLException {
    try (PhoenixConnection internal = CentroidManager.newInternalConnection(conn)) {
      PTable index;
      PTable dataTable;
      try {
        index = internal.getTableNoCache(indexName);
        dataTable = internal.getTableNoCache(index.getParentName().getString());
      } catch (TableNotFoundException e) {
        return Outcome.NOT_FOUND;
      }
      if (index.getVectorCentroidGeneration() == null) {
        return Outcome.UNTRAINED;
      }
      String token = UUID.randomUUID().toString();
      if (!CentroidManager.claimRebuild(internal, indexName, token, CLAIM_EXPIRY_MS)) {
        return Outcome.IN_PROGRESS;
      }
      try {
        return rebuild(internal, index, dataTable, manual, reason);
      } finally {
        CentroidManager.releaseRebuild(internal, indexName, token);
      }
    }
  }

  private static Outcome rebuild(PhoenixConnection conn, PTable index, PTable dataTable,
    boolean manual, String reason) throws SQLException {
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
        // Drift was assessed by the reconciliation that queued this rebuild, over freshly
        // reconciled counts. Reconciling again would consume the reassignment counters that
        // assessment was based on, so only the operational gates are rechecked here.
        if (
          !props.getBoolean(QueryServices.VECTOR_REBUILD_AUTO_ENABLED_ATTRIB,
            QueryServicesOptions.DEFAULT_VECTOR_REBUILD_AUTO_ENABLED)
        ) {
          return Outcome.DISABLED;
        }
        // Generation ID timestamp bounds the minimum elapsed time since previous rebuild
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
      // Assign centroid IDs past outgoing ranges to avoid row key collisions with prior delete
      // markers
      CachedCentroids outgoing =
        VectorCentroidCache.getInstance(conn.getQueryServices().getConfiguration()).get(conn,
          indexName, active, DistanceMetric.fromString(index.getVectorDistanceMetric()));
      int firstId = outgoing.getFirstId() + outgoing.size();
      CentroidManager.persistCentroids(conn, indexName, building, model.getCentroids(), firstId);
      CentroidManager.persistGenerationSummary(conn, indexName, building, new GenerationSummary(
        GenerationSummary.BUILDING, reason, lists, model.getSkewMetrics(), null, null));
      VectorCentroidCache.getInstance(conn.getQueryServices().getConfiguration()).put(indexName,
        building, new CachedCentroids(model.getCentroids(), model.getDistanceMetric(), firstId));
      CentroidManager.setBuildingGeneration(conn, index, building);
      LOGGER.info("Migrating vector index {} from generation {} to generation {}: {}", indexName,
        active, building, reason);
    }
    // Wait for client metadata caches to observe building generation
    awaitMetadataRefresh(conn);
    index = refresh(conn, index);
    migrate(conn, index, dataTable, building);

    List<float[]> centroids = CentroidManager.loadCentroids(conn, indexName, building);
    CentroidManager.setGenerationAndLists(conn, index, building, centroids.size());
    CentroidManager.persistGenerationSummary(conn, indexName, building,
      new GenerationSummary(GenerationSummary.ACTIVE, null, null, null,
        EnvironmentEdgeManager.currentTimeMillis(), null));
    // Wait for client metadata caches to observe promoted active generation
    awaitMetadataRefresh(conn);
    index = refresh(conn, index);
    retireInactiveGenerations(conn, index);
    VectorIndexScorecard.reconcile(conn, index, building);
    LOGGER.info("Rebuilt vector index {} to generation {}", indexName, building);
    return Outcome.REBUILT;
  }

  /**
   * Executes dual phase migration, a full server side index rebuild followed by catch up scan for
   * concurrent modifications.
   */
  private static void migrate(PhoenixConnection conn, PTable index, PTable dataTable, long building)
    throws SQLException {
    String dataTableName = SchemaUtil.getEscapedFullTableName(dataTable.getName().getString());
    long start = EnvironmentEdgeManager.currentTimeMillis();
    MutationPlan plan = new ServerBuildIndexCompiler(conn, dataTableName).compile(index);
    setTimeRange(plan.getContext().getScan(), 0, start);
    conn.getQueryServices().updateData(plan);
    MigrationHook hook = migrationHookForTesting;
    if (hook != null) {
      try {
        hook.migrating(index, building);
      } catch (Exception e) {
        throw new SQLException(e);
      }
    }
    long end = EnvironmentEdgeManager.currentTimeMillis();
    Set<ImmutableBytesPtr> changed = getChangedRows(conn, dataTable, start, end);
    if (changed.size() > MAX_CATCH_UP_ROWS) {
      plan = new ServerBuildIndexCompiler(conn, dataTableName).compile(index);
      setTimeRange(plan.getContext().getScan(), 0, end);
      conn.getQueryServices().updateData(plan);
    } else if (!changed.isEmpty()) {
      rebuildRows(conn, index, dataTableName, dataTable, changed, end);
    }
  }

  /** Identifies primary data row keys modified within the specified timestamp window. */
  private static Set<ImmutableBytesPtr> getChangedRows(PhoenixConnection conn, PTable dataTable,
    long start, long end) throws SQLException {
    Scan scan = new Scan();
    scan.setRaw(true);
    scan.setFilter(new KeyOnlyFilter());
    setTimeRange(scan, start, end);
    Set<ImmutableBytesPtr> rows = new HashSet<>();
    try (Table table = conn.getQueryServices().getTable(dataTable.getPhysicalName().getBytes());
      ResultScanner scanner = table.getScanner(scan)) {
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        rows.add(new ImmutableBytesPtr(result.getRow()));
        if (rows.size() > MAX_CATCH_UP_ROWS) {
          break;
        }
      }
    } catch (IOException e) {
      throw ClientUtil.parseServerException(e);
    }
    return rows;
  }

  /** Rebuilds index rows for specific primary keys via point scans. */
  private static void rebuildRows(PhoenixConnection conn, PTable index, String dataTableName,
    PTable dataTable, Set<ImmutableBytesPtr> rows, long end) throws SQLException {
    Scan template =
      new ServerBuildIndexCompiler(conn, dataTableName).compile(index).getContext().getScan();
    template.setAttribute(BaseScannerRegionObserverConstants.UNGROUPED_AGG, TRUE);
    try (Table table = conn.getQueryServices().getTable(dataTable.getPhysicalName().getBytes())) {
      for (ImmutableBytesPtr row : rows) {
        Scan scan = new Scan(template);
        scan.withStartRow(row.copyBytesIfNecessary(), true);
        scan.withStopRow(row.copyBytesIfNecessary(), true);
        setTimeRange(scan, 0, end);
        try (ResultScanner scanner = table.getScanner(scan)) {
          scanner.next();
        }
      }
    } catch (IOException e) {
      throw ClientUtil.parseServerException(e);
    }
  }

  /**
   * Reconciles scorecard statistics, evaluates drift conditions, purges retired generations, and
   * schedules background rebuild tasks as necessary.
   * @param conn      Phoenix connection providing execution services
   * @param indexName fully qualified physical index table name
   */
  public static ReconcileOutcome reconcile(PhoenixConnection conn, String indexName)
    throws SQLException {
    try (PhoenixConnection internal = CentroidManager.newInternalConnection(conn)) {
      PTable index;
      try {
        index = internal.getTableNoCache(indexName);
      } catch (TableNotFoundException e) {
        // Clean up orphaned centroid records following concurrent table drop
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
      String token = UUID.randomUUID().toString();
      if (!CentroidManager.claimRebuild(internal, indexName, token, CLAIM_EXPIRY_MS)) {
        return ReconcileOutcome.NOT_DUE;
      }
      try {
        retireInactiveGenerations(internal, index);
        if (index.isVectorRebuildInProgress()) {
          CentroidManager.enqueueTask(internal, index, TaskType.VECTOR_INDEX_REBUILD,
            rebuildTaskData(true, "RESUME"));
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
      } finally {
        CentroidManager.releaseRebuild(internal, indexName, token);
      }
    }
  }

  /** Evaluates whether reconciliation is due based on elapsed time since previous update. */
  static boolean isDue(Long lastUpdate, long intervalMs, long now) {
    if (lastUpdate == null) {
      return true;
    }
    long elapsed = now - lastUpdate;
    return elapsed >= 0 && elapsed >= intervalMs;
  }

  /** Serializes task payload parameters for background rebuild tasks. */
  public static String rebuildTaskData(boolean manual, String reason) {
    return "{\"manual\":" + manual + ",\"reason\":\""
      + (reason == null ? "" : reason.replace("\"", "'")) + "\"}";
  }

  /** Purges centroid rows and metadata for retired generations. */
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

  /** Blocks execution to allow distributed client metadata cache invalidation. */
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
