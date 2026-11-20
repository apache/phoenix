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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.ServerBuildIndexCompiler;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ConnectionProperty;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Rebuilds IVF vector indexes to new centroid generations, and reconciles drift scorecards.
 * <p>
 * A rebuild trains a new centroid generation and records it as the building generation in
 * {@code SYSTEM.CATALOG}. Then it waits until all metadata caches see the building generation, and
 * it migrates the index online. During the migration, writers put index rows under the building
 * generation and delete the earlier index rows under both generations. The migration runs a server
 * side index build, then a catch-up build of the rows that changed during the first build. At the
 * end, the rebuild promotes the building generation to active. It waits for the metadata caches
 * again, and then it deletes the centroid rows of the other generations.
 * <p>
 * Clients and region servers keep the metadata of a table for up to its UPDATE_CACHE_FREQUENCY. The
 * index stays active during a rebuild, so the caches do not refresh earlier. A writer with metadata
 * from before the building generation maintains only the outgoing generation. The catch-up build
 * does not repair a write after its time range, so the promotion loses such a write. Thus each wait
 * is the index population wait plus the largest effective UPDATE_CACHE_FREQUENCY. The rebuild uses
 * the frequencies of the data table, the index, and all views of the data table. Views get the
 * vector index from the data table, and a view can have a longer frequency. The rebuild reads the
 * frequencies again after the training and after the promotion, because an ALTER can change them.
 * <p>
 * A table with the frequency zero uses the default frequency. The rebuild assumes that all clients
 * and region servers use the same default. It uses the default of its own connection.
 * <p>
 * The rebuild does not migrate an index if a cache never expires. Writers can then keep the
 * outgoing generation without limit. A migration in progress stays in progress until all
 * frequencies are finite. The first training of an index waits only for the population wait.
 * Clients do not cache the metadata of a table that has an untrained vector index.
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
    UP_TO_DATE,
    /**
     * A metadata cache of the index never expires, so the rebuild does not migrate the index. The
     * effective UPDATE_CACHE_FREQUENCY of the data table, the index, or a view of the data table is
     * NEVER. {@link #CACHE_NEVER_EXPIRES_ADVICE} tells the operator how to correct this.
     */
    CACHE_NEVER_EXPIRES
  }

  /** Tells an operator how to correct the {@link Outcome#CACHE_NEVER_EXPIRES} condition. */
  public static final String CACHE_NEVER_EXPIRES_ADVICE =
    "Set a finite UPDATE_CACHE_FREQUENCY on the data table, its views and the index, or a finite "
      + QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB + " for a table that uses the default";

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

  /** Longest sleep of a metadata wait between two checks of the rebuild claim. */
  private static final long CLAIM_CHECK_INTERVAL_MS = CentroidManager.CLAIM_LEASE_MS / 4;

  /**
   * Indexes that a reconciliation does not migrate, because a metadata cache never expires. The set
   * makes sure that each change to this condition logs one warning, not one on each sweep.
   */
  private static final Set<String> CACHE_NEVER_EXPIRES_INDEXES = ConcurrentHashMap.newKeySet();

  /** Largest number of parents whose child links one scan of {@code SYSTEM.CHILD_LINK} reads. */
  private static final int CHILD_LINK_PARENT_BATCH_SIZE = 1000;

  /** Number of child link rows that one RPC of a scan returns. */
  private static final int CHILD_LINK_SCAN_CACHING = 1000;

  /** Largest number of view header rows in one batch of point reads. */
  private static final int VIEW_HEADER_BATCH_SIZE = 1000;

  private static final byte[] VIEW_TABLE_TYPE = Bytes.toBytes(PTableType.VIEW.getSerializedValue());

  /** Checks during a long wait that the caller still holds its rebuild claim. */
  interface ClaimCheck {
    void renew() throws SQLException;
  }

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
      // Check the caches before any change to the index state
      long cacheLag = 0;
      if (index.getVectorCentroidGeneration() != null) {
        cacheLag = cacheLagMs(internal, dataTable, index);
        if (cacheLag == Long.MAX_VALUE) {
          warnCacheNeverExpires(internal, indexName, dataTable, index);
          return Outcome.CACHE_NEVER_EXPIRES;
        }
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
      return rebuild(internal, claim, index, dataTable, manual, reason, cacheLag);
    }
  }

  /** Trains the first centroid generation of an index that has none, then builds the index. */
  private static Outcome trainAndBuild(PhoenixConnection conn, CentroidManager.RebuildClaim claim,
    PTable index, PTable dataTable) throws SQLException {
    Long generation = VectorIndexTrainer.trainAndRecord(conn, claim, dataTable, index);
    if (generation == null) {
      return Outcome.UNTRAINED;
    }
    // Wait until client metadata caches see the generation. Index maintenance starts then.
    awaitMetadataRefresh(conn, claim, index.getName().getString(), "generation " + generation, 0);
    index = refresh(conn, index);
    migrate(conn, claim, index, dataTable, generation);
    VectorIndexScorecard.reconcile(conn, index, generation);
    activate(conn, index.getName().getString());
    LOGGER.info("Built vector index {} at generation {}", index.getName().getString(), generation);
    return Outcome.REBUILT;
  }

  private static Outcome rebuild(PhoenixConnection conn, CentroidManager.RebuildClaim claim,
    PTable index, PTable dataTable, boolean manual, String reason, long cacheLag)
    throws SQLException {
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
      // An ALTER during the training can make a frequency longer. A cache entry from before the
      // building generation can then live longer than the first computation allows.
      PTable currentIndex = conn.getTableNoCache(indexName);
      PTable currentData = readDataTable(conn, currentIndex);
      long current = cacheLagMs(conn, currentData, currentIndex);
      if (current == Long.MAX_VALUE) {
        // The building generation stays. A reconciliation resumes the migration after all
        // frequencies are finite.
        warnCacheNeverExpires(conn, indexName, currentData, currentIndex);
        return Outcome.CACHE_NEVER_EXPIRES;
      }
      cacheLag = Math.max(cacheLag, current);
    }
    // Wait until all metadata caches see the building generation. After the wait, each writer
    // maintains the building generation, so the catch-up build repairs all earlier writes.
    awaitMetadataRefresh(conn, claim, indexName, "building generation " + building, cacheLag);
    index = refresh(conn, index);
    migrate(conn, claim, index, dataTable, building);

    List<float[]> centroids = CentroidManager.loadCentroids(conn, indexName, building);
    // Record a first retire time before the promotion. If this rebuild stops after the promotion,
    // reconciliation and fsck still keep the outgoing generation until that time.
    long populationWait = props.getLong(QueryServices.INDEX_POPULATION_SLEEP_TIME,
      QueryServicesOptions.DEFAULT_INDEX_POPULATION_SLEEP_TIME);
    long clockSkew = props.getLong(QueryServices.VECTOR_SCORECARD_CLOCK_SKEW_MS_ATTRIB,
      QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_CLOCK_SKEW_MS);
    long retireAfter = retireAfter(EnvironmentEdgeManager.currentTimeMillis(),
      metadataWaitMs(populationWait, cacheLag), clockSkew);
    CentroidManager.persistGenerationSummary(conn, indexName, building,
      new GenerationSummary(null, null, null, null, null, null, retireAfter));
    claim.renew();
    CentroidManager.setGenerationAndLists(conn, index, building, centroids.size());
    CentroidManager.persistGenerationSummary(conn, indexName, building,
      new GenerationSummary(GenerationSummary.ACTIVE, null, null, null,
        EnvironmentEdgeManager.currentTimeMillis(), null));
    // The server time of the summary write follows the promotion
    Long promotedAt = CentroidManager.summaryWriteTime(conn, indexName, building);
    // Wait until all metadata caches see the promoted generation. Writers and queries with earlier
    // metadata still use the centroids of the outgoing generation. The promotion is complete, so a
    // frequency that becomes NEVER now does not stop the rebuild. The wait then keeps its value.
    PTable promoted = conn.getTableNoCache(indexName);
    long current = cacheLagMs(conn, readDataTable(conn, promoted), promoted);
    if (current == Long.MAX_VALUE) {
      LOGGER.warn(
        "A metadata cache of vector index {} never expires. The rebuild uses the earlier"
          + " frequency of {} ms before it retires the outgoing generation. {}.",
        indexName, cacheLag, CACHE_NEVER_EXPIRES_ADVICE);
    } else {
      cacheLag = Math.max(cacheLag, current);
    }
    if (promotedAt != null) {
      long serverRetireAfter =
        retireAfter(promotedAt, metadataWaitMs(populationWait, cacheLag), clockSkew);
      if (serverRetireAfter > retireAfter) {
        CentroidManager.persistGenerationSummary(conn, indexName, building,
          new GenerationSummary(null, null, null, null, null, null, serverRetireAfter));
      }
    }
    awaitMetadataRefresh(conn, claim, indexName, "active generation " + building, cacheLag);
    index = refresh(conn, index);
    // This rebuild waited for the caches on its own clock, so the retire time does not apply
    CentroidManager.deleteDebrisGenerations(conn, claim, index, true,
      CentroidManager.NO_DEBRIS_VISITOR);
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
    // build makes the index rows of each such data row again from its full state. The end of the
    // range is exclusive. Thus the range ends one millisecond after the current time. This
    // includes a write that completes in the current millisecond.
    build(conn, index, dataTable, start, EnvironmentEdgeManager.currentTimeMillis() + 1);
    claim.renew();
  }

  /** Runs the server side index build over the data cells written in [min, max). */
  static void build(PhoenixConnection conn, PTable index, PTable dataTable, long min, long max)
    throws SQLException {
    MutationPlan plan;
    try {
      plan = new ServerBuildIndexCompiler(conn,
        SchemaUtil.getEscapedFullTableName(dataTable.getName().getString())).compile(index);
    } catch (IllegalArgumentException e) {
      // The compiler throws this exception for more than one fault. Report a missing index only
      // when the resolved data table does not list the index.
      String dataTableName = dataTable.getName().getString();
      PTable resolved;
      try {
        resolved = conn.getTable(dataTableName);
      } catch (SQLException s) {
        s.addSuppressed(e);
        throw s;
      }
      if (listsIndex(resolved, index)) {
        throw new SQLException(e);
      }
      throw new SQLException(
        "Data table " + dataTableName + " does not list index " + index.getName().getString(), e);
    }
    setTimeRange(plan.getContext().getScan(), min, max);
    conn.getQueryServices().updateData(plan);
  }

  private static boolean listsIndex(PTable dataTable, PTable index) {
    for (PTable listed : dataTable.getIndexes()) {
      if (listed.getName().equals(index.getName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reconciles the scorecard of the active generation when it is due, assesses drift, and deletes
   * the debris generations. It queues a rebuild task if it finds drift and automatic rebuild is
   * enabled. For a migration in progress, it queues a resume task. All decisions use the index as
   * it is after this method takes the rebuild claim. For a dropped index, it deletes the centroid
   * rows that the index left.
   * @param conn      the Phoenix connection
   * @param indexName the full physical name of the index table
   */
  public static ReconcileOutcome reconcile(PhoenixConnection conn, String indexName)
    throws SQLException {
    try (PhoenixConnection internal = CentroidManager.newInternalConnection(conn)) {
      ReadOnlyProps props = internal.getQueryServices().getProps();
      // This read only decides whether to take the claim. A different holder can change the
      // index before the claim, so the work below reads the index again.
      PTable gate;
      try {
        gate = internal.getTableNoCache(indexName);
      } catch (TableNotFoundException e) {
        gate = null;
      }
      if (
        gate != null && !gate.isVectorRebuildInProgress() && !isDue(internal, gate, props)
          && CentroidManager.listDebrisGenerations(internal, gate, false).isEmpty()
      ) {
        return ReconcileOutcome.NOT_DUE;
      }
      try (CentroidManager.RebuildClaim claim =
        CentroidManager.claim(internal, indexName, UUID.randomUUID().toString())) {
        if (claim == null) {
          return ReconcileOutcome.NOT_DUE;
        }
        PTable index;
        try {
          index = internal.getTableNoCache(indexName);
        } catch (TableNotFoundException e) {
          // The index was dropped, so delete the centroid rows that it left
          return CentroidManager.purgeDroppedIndex(internal, claim, indexName)
            ? ReconcileOutcome.INDEX_DROPPED
            : ReconcileOutcome.NOT_DUE;
        }
        CentroidManager.deleteDebrisGenerations(internal, claim, index, false,
          CentroidManager.NO_DEBRIS_VISITOR);
        if (index.isVectorRebuildInProgress()) {
          if (canMigrate(internal, index)) {
            CentroidManager.enqueueTask(internal, index, TaskType.VECTOR_INDEX_REBUILD,
              rebuildTaskData(true, RESUME_REASON));
          }
          return ReconcileOutcome.NOT_DUE;
        }
        Long active = index.getVectorCentroidGeneration();
        if (active == null || !isDue(internal, index, props)) {
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
            && canMigrate(internal, index)
        ) {
          CentroidManager.enqueueTask(internal, index, TaskType.VECTOR_INDEX_REBUILD,
            rebuildTaskData(false, assessment.getReason()));
        }
        return ReconcileOutcome.RECONCILED;
      }
    }
  }

  /**
   * Returns true if the scorecard of the active generation is due for reconciliation. An untrained
   * index is never due.
   */
  private static boolean isDue(PhoenixConnection conn, PTable index, ReadOnlyProps props)
    throws SQLException {
    Long active = index.getVectorCentroidGeneration();
    if (active == null) {
      return false;
    }
    GenerationSummary summary =
      CentroidManager.loadGenerationSummary(conn, index.getName().getString(), active);
    return isDue(summary == null ? null : summary.getLastScorecardUpdate(),
      props.getLong(QueryServices.VECTOR_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_RECONCILE_INTERVAL_MS),
      EnvironmentEdgeManager.currentTimeMillis());
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

  /**
   * Returns true if all metadata caches of the index expire, so that a migration can complete. If a
   * cache never expires, a reconciliation does not queue a rebuild. A warning then tells the
   * operator once, and a later change to finite frequencies clears the condition.
   */
  private static boolean canMigrate(PhoenixConnection conn, PTable index) throws SQLException {
    String indexName = index.getName().getString();
    PTable dataTable = readDataTable(conn, index);
    if (cacheLagMs(conn, dataTable, index) != Long.MAX_VALUE) {
      CACHE_NEVER_EXPIRES_INDEXES.remove(indexName);
      return true;
    }
    if (CACHE_NEVER_EXPIRES_INDEXES.add(indexName)) {
      warnCacheNeverExpires(conn, indexName, dataTable, index);
    } else {
      LOGGER.debug("A metadata cache of vector index {} never expires", indexName);
    }
    return false;
  }

  /**
   * Logs that a metadata cache of the index never expires. If the data table and the index have
   * finite frequencies, the method walks the views again to name the view that never expires.
   */
  private static void warnCacheNeverExpires(PhoenixConnection conn, String indexName,
    PTable dataTable, PTable index) {
    long defaultFrequency = defaultUpdateCacheFrequency(conn);
    String view = null;
    if (
      cacheLagMs(defaultFrequency, dataTable.getUpdateCacheFrequency(),
        index.getUpdateCacheFrequency()) != Long.MAX_VALUE
    ) {
      try {
        ViewCacheLag lag = viewCacheLag(conn, dataTable, defaultFrequency,
          CHILD_LINK_PARENT_BATCH_SIZE, VIEW_HEADER_BATCH_SIZE);
        if (lag.lagMs == Long.MAX_VALUE && lag.view != null) {
          view = viewName(lag.view);
        }
      } catch (SQLException e) {
        LOGGER.debug("Cannot find the view of vector index {} that never expires", indexName, e);
      }
    }
    if (view != null) {
      LOGGER
        .warn("Not migrating vector index {}, because a metadata cache never expires. The effective"
          + " UPDATE_CACHE_FREQUENCY of view {} of the data table is NEVER. A view that does not"
          + " set its own frequency uses the frequency of its parent. Run ALTER VIEW ... SET"
          + " UPDATE_CACHE_FREQUENCY with a finite value on this view, as its tenant, or on the"
          + " ancestor that sets NEVER. {}.", indexName, view, CACHE_NEVER_EXPIRES_ADVICE);
      return;
    }
    LOGGER.warn(
      "Not migrating vector index {}, because a metadata cache never expires. The"
        + " UPDATE_CACHE_FREQUENCY of the data table is {}, of the index is {}, and the default"
        + " is {}. A view of the data table can also have the frequency NEVER. {}.",
      indexName, dataTable.getUpdateCacheFrequency(), index.getUpdateCacheFrequency(),
      defaultFrequency, CACHE_NEVER_EXPIRES_ADVICE);
  }

  private static PTable readDataTable(PhoenixConnection conn, PTable index) throws SQLException {
    return conn.getTableNoCache(index.getParentName().getString());
  }

  /**
   * Returns the largest effective UPDATE_CACHE_FREQUENCY of the data table, the index, and all
   * views of the data table. Returns {@link Long#MAX_VALUE} if a cache never expires. A table or
   * view with the frequency zero uses the default of the connection, as the metadata cache does.
   * <p>
   * The view lookup walks the parent to child links in {@code SYSTEM.CHILD_LINK}, one level of the
   * view tree at a time. Thus its cost depends on the number of views, not on the size of
   * {@code SYSTEM.CATALOG}. The caller needs READ access on {@code SYSTEM.CHILD_LINK} and
   * {@code SYSTEM.CATALOG}. The lookup has no fallback, so a denied read fails the call.
   */
  public static long cacheLagMs(PhoenixConnection conn, PTable dataTable, PTable index)
    throws SQLException {
    return cacheLagMs(conn, dataTable, index, CHILD_LINK_PARENT_BATCH_SIZE, VIEW_HEADER_BATCH_SIZE);
  }

  /**
   * Returns the cache lag as {@link #cacheLagMs(PhoenixConnection, PTable, PTable)} does, with the
   * given batch sizes of the view walk.
   */
  @VisibleForTesting
  static long cacheLagMs(PhoenixConnection conn, PTable dataTable, PTable index, int parentBatch,
    int headerBatch) throws SQLException {
    long defaultFrequency = defaultUpdateCacheFrequency(conn);
    long lag = cacheLagMs(defaultFrequency, dataTable.getUpdateCacheFrequency(),
      index.getUpdateCacheFrequency());
    if (lag == Long.MAX_VALUE) {
      return lag;
    }
    return Math.max(lag,
      viewCacheLag(conn, dataTable, defaultFrequency, parentBatch, headerBatch).lagMs);
  }

  /**
   * Returns the largest effective value of UPDATE_CACHE_FREQUENCY values. The value zero means the
   * default, so it counts as {@code defaultFrequency}. The result is {@link Long#MAX_VALUE} for the
   * frequency NEVER. A negative value counts as zero.
   */
  static long cacheLagMs(long defaultFrequency, long... frequencies) {
    long lag = 0;
    for (long frequency : frequencies) {
      long effective = frequency != QueryServicesOptions.DEFAULT_UPDATE_CACHE_FREQUENCY
        ? frequency
        : defaultFrequency;
      lag = Math.max(lag, effective);
    }
    return lag;
  }

  /**
   * Returns the population wait plus the cache lag. The result is {@link Long#MAX_VALUE} for an
   * unbounded lag and on overflow. A negative value counts as zero.
   */
  static long metadataWaitMs(long populationWaitMs, long cacheLagMs) {
    long populationWait = Math.max(0, populationWaitMs);
    long cacheLag = Math.max(0, cacheLagMs);
    return cacheLag > Long.MAX_VALUE - populationWait ? Long.MAX_VALUE : cacheLag + populationWait;
  }

  /**
   * Returns the retire time of the generations older than a promoted generation. It is the
   * promotion time plus the metadata wait plus the clock skew bound. The skew bound covers a check
   * on a different host. The result is {@link Long#MAX_VALUE} on overflow. A negative value counts
   * as zero.
   */
  static long retireAfter(long promotedAt, long metadataWaitMs, long clockSkewMs) {
    return VectorIndexScorecard.addNonNegative(promotedAt, metadataWaitMs, clockSkewMs);
  }

  private static long defaultUpdateCacheFrequency(PhoenixConnection conn) {
    return (Long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
      conn.getQueryServices().getProps().get(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB));
  }

  /** Largest effective frequency of the views of a table, and the view that has it. */
  static final class ViewCacheLag {
    /** The largest effective frequency, or {@link Long#MAX_VALUE} for NEVER. */
    final long lagMs;
    /** The header row key of the view with the largest frequency, or null if no view counts. */
    final byte[] view;

    ViewCacheLag(long lagMs, byte[] view) {
      this.lagMs = lagMs;
      this.view = view;
    }
  }

  /** Reads the child link rows of a batch of parents. */
  interface ChildLinkReader {
    /** Returns the row keys of the child links of the parents with the given header row keys. */
    List<byte[]> read(List<byte[]> parents) throws IOException;
  }

  /** Reads a batch of header rows. */
  interface HeaderReader {
    /** Returns one result for each header row key, in the same order. */
    Result[] read(List<byte[]> headers) throws IOException;
  }

  /** Gets the exact effective frequency of a view whose header row has the frequency NEVER. */
  interface NeverResolver {
    /** Returns the effective frequency, or an empty value if the view does not resolve. */
    OptionalLong resolve(byte[] header) throws SQLException;
  }

  /**
   * Returns the largest effective UPDATE_CACHE_FREQUENCY of all views of the data table, for all
   * tenants and all levels of views. The method reads {@code SYSTEM.CHILD_LINK} and
   * {@code SYSTEM.CATALOG} directly, and it resolves only the views whose header says NEVER.
   */
  private static ViewCacheLag viewCacheLag(PhoenixConnection conn, PTable dataTable,
    long defaultFrequency, int parentBatch, int headerBatch) throws SQLException {
    ConnectionQueryServices services = conn.getQueryServices();
    byte[] childLinkName = SchemaUtil
      .getPhysicalTableName(SYSTEM_CHILD_LINK_NAME_BYTES, services.getConfiguration()).getName();
    byte[] catalogName = SchemaUtil
      .getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, services.getConfiguration()).getName();
    try (Table childLinks = services.getTable(childLinkName);
      Table catalog = services.getTable(catalogName)) {
      return viewCacheLag(parents -> readChildLinks(childLinks, parents),
        headers -> readHeaders(catalog, headers), header -> resolveFrequency(conn, header),
        SchemaUtil.getTableKey(dataTable), defaultFrequency, parentBatch, headerBatch);
    } catch (IOException e) {
      throw ClientUtil.parseServerException(e);
    }
  }

  /**
   * Walks the view tree of the table with the header row key {@code root}, one level at a time.
   * Each level reads the child links of its parents in batches of {@code parentBatch}. Then it
   * reads the header rows of the new children in batches of {@code headerBatch}.
   * <p>
   * A view that does not set its own frequency uses the effective frequency of its parent. Its
   * header row keeps the frequency of the parent at the time of creation. Thus the largest header
   * value is an upper bound, and never too low. A header that says NEVER can be such an old copy.
   * The walk then resolves the view to get its exact effective frequency. A NEVER header stops the
   * walk, unless the resolved view has a finite frequency. A view that no longer resolves keeps its
   * header value as the bound.
   * <p>
   * The walk also follows the links of a child without a valid view header. A DROP ... CASCADE
   * removes child views later, and clients can still cache them until then. A set of visited keys
   * stops a cycle of links.
   */
  @VisibleForTesting
  static ViewCacheLag viewCacheLag(ChildLinkReader childLinks, HeaderReader headers,
    NeverResolver resolver, byte[] root, long defaultFrequency, int parentBatch, int headerBatch)
    throws IOException, SQLException {
    Set<ImmutableBytesPtr> visited = new HashSet<>();
    visited.add(new ImmutableBytesPtr(root));
    List<byte[]> level = Collections.singletonList(root);
    long lag = 0;
    byte[] maxView = null;
    while (!level.isEmpty()) {
      List<byte[]> children = new ArrayList<>();
      for (int i = 0; i < level.size(); i += parentBatch) {
        for (byte[] link : childLinks
          .read(level.subList(i, Math.min(level.size(), i + parentBatch)))) {
          byte[] child = childHeaderKey(link);
          if (child != null && visited.add(new ImmutableBytesPtr(child))) {
            children.add(child);
          }
        }
      }
      for (int i = 0; i < children.size(); i += headerBatch) {
        List<byte[]> keys = children.subList(i, Math.min(children.size(), i + headerBatch));
        Result[] results = headers.read(keys);
        for (int j = 0; j < keys.size(); j++) {
          OptionalLong frequency = viewFrequency(results[j]);
          if (!frequency.isPresent()) {
            continue;
          }
          long effective = cacheLagMs(defaultFrequency, frequency.getAsLong());
          if (effective == Long.MAX_VALUE) {
            OptionalLong exact = resolver.resolve(keys.get(j));
            if (exact.isPresent()) {
              effective = cacheLagMs(defaultFrequency, exact.getAsLong());
            }
            if (effective == Long.MAX_VALUE) {
              return new ViewCacheLag(Long.MAX_VALUE, keys.get(j));
            }
          }
          if (effective > lag || maxView == null) {
            lag = Math.max(lag, effective);
            maxView = keys.get(j);
          }
        }
      }
      level = children;
    }
    return new ViewCacheLag(lag, maxView);
  }

  /**
   * Returns the range of the child link rows of a parent. The range starts after the separator that
   * follows the parent key, so it holds no row of a table whose name extends the parent name.
   */
  static RowRange childLinkRange(byte[] parent) {
    byte[] start = ByteUtil.concat(parent, QueryConstants.SEPARATOR_BYTE_ARRAY);
    return new RowRange(start, true, ByteUtil.nextKey(start), false);
  }

  /**
   * Returns one scan of the child links of a batch of parents. The scan reads only the merged
   * ranges of the parents, and it keeps only rows of the link type CHILD_TABLE. The scan is not
   * raw, so it does not see deleted links.
   */
  static Scan childLinkScan(List<byte[]> parents) {
    List<RowRange> ranges = new ArrayList<>(parents.size());
    for (byte[] parent : parents) {
      ranges.add(childLinkRange(parent));
    }
    MultiRowRangeFilter rangeFilter = new MultiRowRangeFilter(ranges);
    List<RowRange> merged = rangeFilter.getRowRanges();
    SingleColumnValueFilter linkFilter = new SingleColumnValueFilter(TABLE_FAMILY_BYTES,
      LINK_TYPE_BYTES, CompareOperator.EQUAL, LinkType.CHILD_TABLE.getSerializedValueAsByteArray());
    linkFilter.setFilterIfMissing(true);
    Scan scan = new Scan().withStartRow(merged.get(0).getStartRow(), true)
      .withStopRow(merged.get(merged.size() - 1).getStopRow(), false);
    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, rangeFilter, linkFilter));
    scan.addColumn(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);
    scan.setCaching(CHILD_LINK_SCAN_CACHING);
    return scan;
  }

  /**
   * Returns the header row key of the child of a child link row, or null for a malformed row. The
   * child tenant is in the column name slot, and the full child name is in the family name slot.
   */
  static byte[] childHeaderKey(byte[] link) {
    byte[][] slots = new byte[5][];
    SchemaUtil.getVarChars(link, 5, slots);
    byte[] childName = slots[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX];
    if (childName == null || childName.length == 0) {
      return null;
    }
    return SchemaUtil.getTableKey(slots[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX],
      Bytes.toBytes(SchemaUtil.getSchemaNameFromFullName(childName)),
      Bytes.toBytes(SchemaUtil.getTableNameFromFullName(childName)));
  }

  /** Returns the name of a view from its header row key, as tenant:schema.view. */
  static String viewName(byte[] header) {
    byte[][] slots = new byte[3][];
    SchemaUtil.getVarChars(header, 3, slots);
    String name = SchemaUtil.getTableName(Bytes.toString(slots[1]), Bytes.toString(slots[2]));
    return slots[0] == null || slots[0].length == 0 ? name : Bytes.toString(slots[0]) + ":" + name;
  }

  private static List<byte[]> readChildLinks(Table childLinks, List<byte[]> parents)
    throws IOException {
    List<byte[]> links = new ArrayList<>();
    try (ResultScanner scanner = childLinks.getScanner(childLinkScan(parents))) {
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        links.add(result.getRow());
      }
    }
    return links;
  }

  private static Result[] readHeaders(Table catalog, List<byte[]> keys) throws IOException {
    List<Get> gets = new ArrayList<>(keys.size());
    for (byte[] key : keys) {
      Get get = new Get(key);
      get.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
      get.addColumn(TABLE_FAMILY_BYTES, UPDATE_CACHE_FREQUENCY_BYTES);
      gets.add(get);
    }
    return catalog.get(gets);
  }

  /**
   * Resolves a view from the server for its own tenant, and returns its effective frequency. The
   * resolution reads the view and all its ancestors again. A view that does not set its own
   * frequency thus gets the current frequency of its parent. Returns an empty value if the view or
   * an ancestor is gone, or if the name now resolves to a different table.
   */
  private static OptionalLong resolveFrequency(PhoenixConnection conn, byte[] header)
    throws SQLException {
    byte[][] slots = new byte[3][];
    SchemaUtil.getVarChars(header, 3, slots);
    byte[] tenant = slots[0] == null ? ByteUtil.EMPTY_BYTE_ARRAY : slots[0];
    PName tenantId = tenant.length == 0 ? null : PNameFactory.newName(tenant);
    PTable view;
    try {
      view = conn.getTableNoCache(tenantId,
        SchemaUtil.getTableName(Bytes.toString(slots[1]), Bytes.toString(slots[2])));
    } catch (TableNotFoundException e) {
      return OptionalLong.empty();
    }
    byte[] resolvedTenant =
      view.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY : view.getTenantId().getBytes();
    if (view.getType() != PTableType.VIEW || !Bytes.equals(tenant, resolvedTenant)) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(view.getUpdateCacheFrequency());
  }

  /**
   * Returns the frequency of a view header row, or an empty value for a row that is not a view
   * header. A view without the frequency cell uses the default. A row that is not a view header
   * belongs to a view that a DROP removed, or to a link without a view.
   */
  static OptionalLong viewFrequency(Result header) {
    Cell type = header.getColumnLatestCell(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
    if (type == null || !CellUtil.matchingValue(type, VIEW_TABLE_TYPE)) {
      return OptionalLong.empty();
    }
    Cell frequency = header.getColumnLatestCell(TABLE_FAMILY_BYTES, UPDATE_CACHE_FREQUENCY_BYTES);
    return OptionalLong.of(frequency == null || frequency.getValueLength() == 0
      ? QueryServicesOptions.DEFAULT_UPDATE_CACHE_FREQUENCY
      : PLong.INSTANCE.getCodec().decodeLong(frequency.getValueArray(), frequency.getValueOffset(),
        SortOrder.getDefault()));
  }

  /**
   * Waits so that metadata caches can see a generation change. The wait is the population wait plus
   * {@code cacheLag}. The wait checks the rebuild claim at each renewal period of the lease. Thus a
   * rebuild that lost its claim stops early, and it does not change the index without the claim.
   */
  private static void awaitMetadataRefresh(PhoenixConnection conn,
    CentroidManager.RebuildClaim claim, String indexName, String target, long cacheLag)
    throws SQLException {
    long populationWait =
      conn.getQueryServices().getProps().getLong(QueryServices.INDEX_POPULATION_SLEEP_TIME,
        QueryServicesOptions.DEFAULT_INDEX_POPULATION_SLEEP_TIME);
    long waitMs = metadataWaitMs(populationWait, cacheLag);
    if (waitMs > 0) {
      LOGGER.info(
        "Waiting {} ms (population wait {} ms, UPDATE_CACHE_FREQUENCY {} ms) until the metadata"
          + " caches see {} of vector index {}",
        waitMs, Math.max(0, populationWait), cacheLag, target, indexName);
    }
    sleepAndCheckClaim(waitMs, CLAIM_CHECK_INTERVAL_MS, claim::renew, indexName);
  }

  /**
   * Sleeps for {@code waitMs} in steps of at most {@code stepMs}. After each step, it checks the
   * claim and logs the progress. A failed check stops the wait with the exception of the check. An
   * interrupt stops the wait and sets the interrupt status again.
   */
  static void sleepAndCheckClaim(long waitMs, long stepMs, ClaimCheck check, String indexName)
    throws SQLException {
    long step = Math.max(1, stepMs);
    long remaining = waitMs;
    while (remaining > 0) {
      long sleep = Math.min(remaining, step);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e)
          .build().buildException();
      }
      remaining -= sleep;
      check.renew();
      if (remaining > 0) {
        LOGGER.info("Waiting {} ms more of {} ms for the metadata caches of vector index {}",
          remaining, waitMs, indexName);
      }
    }
  }

  /**
   * Reads the index again from the server. It removes the full data table entry from the shared
   * client cache, together with its indexes. It does not keep a data table copy without the index.
   * Such a copy makes writers skip the maintenance of the index. Other threads can write back a
   * data table that they read before the removal. That copy came from the server. It is current, or
   * the next read from the server replaces it.
   */
  static PTable refresh(PhoenixConnection conn, PTable index) throws SQLException {
    conn.removeTable(null, index.getParentName().getString(), null, HConstants.LATEST_TIMESTAMP);
    return conn.getTableNoCache(index.getName().getString());
  }

  private static void setTimeRange(Scan scan, long min, long max) throws SQLException {
    try {
      scan.setTimeRange(min, max);
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}
