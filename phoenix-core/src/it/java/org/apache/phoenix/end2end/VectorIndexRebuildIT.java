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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.end2end.VectorIndexTestUtil.KNOWN_CENTROIDS;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.assertIndexConsistent;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.assertIndexVerifies;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.bruteForceTopK;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.countCentroids;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.setupTableAndKnownCentroids;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_HBASE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.ScorecardAccumulator;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.index.vector.VectorIndexRebuilder;
import org.apache.phoenix.index.vector.VectorIndexRebuilder.Outcome;
import org.apache.phoenix.index.vector.VectorIndexRebuilderTestHooks;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.JacksonUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for vector index drift scorecarding, periodic reconciliation, and generational
 * online rebuild migration.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorIndexRebuildIT extends ParallelStatsDisabledIT {

  private static RegionCoprocessorEnvironment taskRegionEnvironment;

  @BeforeClass
  public static synchronized void findTaskRegion() throws Exception {
    taskRegionEnvironment = (RegionCoprocessorEnvironment) getUtility()
      .getRSForFirstRegionInTable(SYSTEM_TASK_HBASE_TABLE_NAME)
      .getRegions(SYSTEM_TASK_HBASE_TABLE_NAME).get(0).getCoprocessorHost()
      .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
  }

  @After
  public void clearHook() {
    VectorIndexRebuilderTestHooks.setMigrationHook(null);
  }

  /**
   * Returns test connection properties with zero intervals for metadata refresh, reconciliation,
   * and rebuild gating.
   */
  private static Properties props(boolean autoRebuild) {
    Properties props = new Properties();
    props.setProperty(QueryServices.INDEX_POPULATION_SLEEP_TIME, "0");
    props.setProperty(QueryServices.VECTOR_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB, "0");
    props.setProperty(QueryServices.VECTOR_DRIFT_MIN_POPULATION_ATTRIB, "10");
    props.setProperty(QueryServices.VECTOR_REBUILD_AUTO_ENABLED_ATTRIB,
      Boolean.toString(autoRebuild));
    props.setProperty(QueryServices.VECTOR_REBUILD_MIN_INTERVAL_MS_ATTRIB, "0");
    return props;
  }

  private static Connection connect(boolean autoRebuild) throws SQLException {
    return DriverManager.getConnection(getUrl(), props(autoRebuild));
  }

  /**
   * Returns a vector offset from centroid {@code c} of {@link VectorIndexTestUtil#KNOWN_CENTROIDS}.
   */
  private static Float[] near(int c, float jitter) {
    float[] centroid = KNOWN_CENTROIDS.get(c);
    Float[] v = new Float[4];
    for (int i = 0; i < 4; i++) {
      v[i] = centroid[i] + jitter;
    }
    return v;
  }

  private static void upsert(Connection conn, String table, String id, Float[] v, String label)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
      ps.setString(1, id);
      Array array = conn.createArrayOf("FLOAT", v);
      ps.setArray(2, array);
      ps.setString(3, label);
      ps.executeUpdate();
    }
  }

  private static void flushScorecards() throws SQLException {
    ScorecardAccumulator.getInstance().flush();
  }

  private static long[] scorecard(Connection conn, String index, long generation)
    throws SQLException {
    // In centroid id order
    List<ScorecardRow> rows = CentroidManager.loadScorecard(conn, index, generation);
    long[] sizes = new long[rows.size()];
    for (int i = 0; i < sizes.length; i++) {
      sizes[i] = rows.get(i).getClusterSize();
    }
    return sizes;
  }

  private static long[] groupedCount(Connection conn, String index, int lists) throws SQLException {
    long[] counts = new long[lists];
    try (ResultSet rs = conn.createStatement().executeQuery(
      "SELECT \"_CENTROID_ID\", COUNT(*) FROM " + index + " GROUP BY \"_CENTROID_ID\"")) {
      while (rs.next()) {
        counts[rs.getInt(1)] = rs.getLong(2);
      }
    }
    return counts;
  }

  private static long count(Connection conn, String table) throws SQLException {
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

  private static PTable index(Connection conn, String index) throws SQLException {
    return conn.unwrap(PhoenixConnection.class).getTableNoCache(index);
  }

  private static long reassigned(Connection conn, String index, long generation, int centroid)
    throws SQLException {
    return CentroidManager.loadScorecard(conn, index, generation).get(centroid).getReassignCount();
  }

  /**
   * Returns the reasons carried by the automatic rebuild tasks queued for an index, as the rebuild
   * task reads them.
   */
  private static List<String> queuedAutomaticRebuilds(Connection conn, String index)
    throws Exception {
    List<String> reasons = new ArrayList<>();
    for (Task.TaskRecord task : Task.queryTaskTable(conn, null, null, index,
      PTable.TaskType.VECTOR_INDEX_REBUILD, null, null)) {
      JsonNode data = JacksonUtil.getObjectReader().readTree(task.getData());
      if (!data.path("manual").asBoolean(false)) {
        reasons.add(data.path("reason").asText(null));
      }
    }
    return reasons;
  }

  private static void runTaskSweep() {
    new TaskRegionObserver.SelfHealingTask(taskRegionEnvironment,
      DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS).run();
  }

  /**
   * Verifies that inline scorecard maintenance tracks inserts, deletes, cross-centroid updates, and
   * covered column updates matching grouped index counts.
   */
  @Test
  public void testInlineCountsMatchGroupedCount() throws Exception {
    String table = "T_SC_" + generateUniqueName();
    String index = "IDX_SC_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 12; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f), "l" + i);
      }
      conn.commit();
      upsert(conn, table, "r0", near(1, 0.02f), "l0"); // centroid 0 -> 1
      upsert(conn, table, "r1", near(1, 0.03f), "l1"); // within centroid 1
      upsert(conn, table, "r2", near(2, 0.01f), "changed"); // covered column only
      conn.commit();
      conn.createStatement().execute("DELETE FROM " + table + " WHERE ID IN ('r3', 'r7')");
      conn.commit();
      flushScorecards();

      long[] expected = groupedCount(conn, index, 4);
      assertEquals(2, expected[0]);
      assertEquals(4, expected[1]);
      assertEquals(3, expected[2]);
      assertEquals(1, expected[3]);
      assertEquals(java.util.Arrays.toString(expected),
        java.util.Arrays.toString(scorecard(conn, index, gen)));
      assertEquals("The move into centroid 1 counts a reassignment", 1,
        reassigned(conn, index, gen, 1));
      assertEquals(0, reassigned(conn, index, gen, 0));
    }
  }

  /** Verifies that replaying an already applied batch produces no net delta in scorecard counts. */
  @Test
  public void testReplayIsNeutral() throws Exception {
    String table = "T_RP_" + generateUniqueName();
    String index = "IDX_RP_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      for (int round = 0; round < 2; round++) {
        for (int i = 0; i < 8; i++) {
          upsert(conn, table, "r" + i, near(i % 4, 0.01f), "l" + i);
        }
        conn.commit();
        flushScorecards();
        assertEquals("[2, 2, 2, 2]", java.util.Arrays.toString(scorecard(conn, index, gen)));
      }
    }
  }

  /** Verifies that scorecard flushes incrementally add to stored counts. */
  @Test
  public void testFlushesAccumulate() throws Exception {
    String index = "IDX_FL_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager.persistCentroids(conn, index, 1L,
        java.util.Arrays.asList(new float[] { 1 }, new float[] { 2 }));
      for (long delta : new long[] { 5, 7 }) {
        Map<ScorecardAccumulator.Key, long[]> batch = new HashMap<>();
        batch.put(new ScorecardAccumulator.Key(index, 1L, 0), new long[] { delta, 0 });
        ScorecardAccumulator.getInstance().accumulate(batch);
        flushScorecards();
      }
      assertEquals(12, scorecard(conn, index, 1L)[0]);
    }
  }

  /** Verifies that buffered scorecard deltas for retired generations are discarded during flush. */
  @Test
  public void testFlushDoesNotResurrectRetiredGeneration() throws Exception {
    String index = "IDX_RT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager.persistCentroids(conn, index, 1L, java.util.Arrays.asList(new float[] { 1 }));
      Map<ScorecardAccumulator.Key, long[]> batch = new HashMap<>();
      batch.put(new ScorecardAccumulator.Key(index, 1L, 0), new long[] { 3, 0 });
      ScorecardAccumulator.getInstance().accumulate(batch);
      CentroidManager.deleteGeneration(conn, index, 1L);
      flushScorecards();
      assertTrue(CentroidManager.listGenerations(conn, index).isEmpty());
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM "
        + SYSTEM_VECTOR_CENTROID_NAME + " WHERE INDEX_NAME = '" + index + "'")) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  /**
   * Verifies that index creation initializes scorecard counts and enqueues the initial
   * reconciliation task.
   */
  @Test
  public void testScorecardSeededAtBuildAndTaskEnqueued() throws Exception {
    String table = "T_SD_" + generateUniqueName();
    String index = "IDX_SD_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      VectorIndexTestUtil.loadClusteredVectors(conn, table, 100);
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) INCLUDE (LABEL) WITH (algorithm='IVF', metric='L2', lists=4, sample_size=100)");
      PTable idx = index(conn, index);
      long total = 0;
      for (long size : scorecard(conn, index, idx.getVectorCentroidGeneration())) {
        total += size;
      }
      assertEquals(100, total);
      GenerationSummary summary =
        CentroidManager.loadGenerationSummary(conn, index, idx.getVectorCentroidGeneration());
      assertEquals(GenerationSummary.ACTIVE, summary.getRebuildState());
      assertEquals(Integer.valueOf(4), summary.getRequestedLists());
      assertNotNull(summary.getLastScorecardUpdate());
      List<Task.TaskRecord> tasks = Task.queryTaskTable(conn, null, null, index,
        PTable.TaskType.VECTOR_SCORECARD_RECONCILE, null, null);
      assertEquals(1, tasks.size());
    }
  }

  /**
   * Verifies that scorecard reconciliation recalculates posting list populations, clears
   * reassignment counts, and updates timestamp metadata.
   */
  @Test
  public void testReconcileRepairsDivergence() throws Exception {
    String table = "T_RC_" + generateUniqueName();
    String index = "IDX_RC_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 6; i++) {
        upsert(conn, table, "r" + i, near(i % 3, 0.01f), "l");
      }
      conn.commit();
      flushScorecards();
      // Corrupt the counters: centroids 0 and 3 hold 2 and 0 rows
      CentroidManager.adjustScorecard(conn, index, gen,
        java.util.Arrays.asList(new ScorecardRow(0, 97, 5), new ScorecardRow(3, 42, 0)));
      assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
        VectorIndexRebuilder.reconcile(conn.unwrap(PhoenixConnection.class), index));
      assertEquals("[2, 2, 2, 0]", java.util.Arrays.toString(scorecard(conn, index, gen)));
      assertEquals(0, reassigned(conn, index, gen, 0));
      assertNotNull(
        CentroidManager.loadGenerationSummary(conn, index, gen).getLastScorecardUpdate());
    }
  }

  /**
   * Verifies that deltas a failed flush could not commit are kept and written by the next flush, so
   * reassignments, which reconciliation cannot recover, survive a catalog outage.
   */
  @Test
  public void testFailedFlushRetainsDeltas() throws Exception {
    String table = "T_FF_" + generateUniqueName();
    String index = "IDX_FF_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 4; i++) {
        upsert(conn, table, "r" + i, near(i, 0.01f), "l");
      }
      conn.commit();
      flushScorecards();
      upsert(conn, table, "r0", near(1, 0.02f), "l"); // centroid 0 -> 1
      conn.commit();
      try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
        admin.disableTable(SYSTEM_VECTOR_CENTROID_HBASE_TABLE_NAME);
        try {
          flushScorecards();
          fail("Flush to a disabled scorecard table should fail");
        } catch (SQLException expected) {
          // The deltas are retained
        } finally {
          admin.enableTable(SYSTEM_VECTOR_CENTROID_HBASE_TABLE_NAME);
        }
      }
      flushScorecards();
      assertEquals("[0, 2, 1, 1]", java.util.Arrays.toString(scorecard(conn, index, gen)));
      assertEquals(1, reassigned(conn, index, gen, 1));
    }
  }

  /**
   * Verifies that a RegionServer flush committed after reconciliation reads the scorecard, and
   * before it writes, survives: reconciliation corrects relative to what it read rather than
   * overwriting the counters with absolute values.
   */
  @Test
  public void testReconcilePreservesConcurrentFlush() throws Exception {
    String table = "T_RF_" + generateUniqueName();
    String index = "IDX_RF_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 6; i++) {
        upsert(conn, table, "r" + i, near(i % 3, 0.01f), "l");
      }
      conn.commit();
      flushScorecards();
      CentroidManager.adjustScorecard(conn, index, gen,
        java.util.Arrays.asList(new ScorecardRow(0, 7, 4)));
      VectorIndexRebuilderTestHooks.setReconcileHook((name, generation) -> {
        // A flush of one insert and three reassignments into centroid 0
        try (Connection c = connect(false)) {
          CentroidManager.adjustScorecard(c, name, generation,
            java.util.Arrays.asList(new ScorecardRow(0, 1, 3)));
        }
      });
      try {
        VectorIndexScorecard.reconcile(conn.unwrap(PhoenixConnection.class), index(conn, index),
          gen);
      } finally {
        VectorIndexRebuilderTestHooks.setReconcileHook(null);
      }
      // Centroid 0 holds 2 rows; the corrupt +7 and the 4 prior reassignments are corrected away,
      // and the concurrent flush's +1 and 3 reassignments are kept
      assertEquals("[3, 2, 2, 0]", java.util.Arrays.toString(scorecard(conn, index, gen)));
      assertEquals(3, reassigned(conn, index, gen, 0));
    }
  }

  /**
   * Verifies that reconciliation tasks execute recurringly and clean up upon index drop.
   */
  @Test
  public void testReconcileTaskRecursAndRetiresOnDrop() throws Exception {
    String table = "T_TK_" + generateUniqueName();
    String index = "IDX_TK_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      upsert(conn, table, "a", near(0, 0.01f), "l");
      conn.commit();
      flushScorecards();
      for (int sweep = 0; sweep < 2; sweep++) {
        // Corrupt centroid 0, which holds 1 row, to 50
        CentroidManager.adjustScorecard(conn, index, gen,
          java.util.Arrays.asList(new ScorecardRow(0, 49, 0)));
        // Reset timestamp to force task execution on next sweep
        conn.createStatement()
          .execute("UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME
            + " (INDEX_NAME, GENERATION_ID, CENTROID_ID, LAST_SCORECARD_UPDATE) VALUES ('" + index
            + "', " + gen + ", -1, NULL)");
        conn.commit();
        runTaskSweep();
        assertEquals("sweep " + sweep, 1, scorecard(conn, index, gen)[0]);
        Task.TaskRecord task = Task.queryTaskTable(conn, null, null, index,
          PTable.TaskType.VECTOR_SCORECARD_RECONCILE, null, null).get(0);
        assertEquals(PTable.TaskStatus.STARTED.toString(), task.getStatus());
      }
      conn.createStatement().execute("DROP INDEX " + index + " ON " + table);
      assertTrue(Task.queryTaskTable(conn, null, null, index,
        PTable.TaskType.VECTOR_SCORECARD_RECONCILE, null, null).isEmpty());
      assertEquals(0, countCentroids(conn, index, null));
    }
  }

  /**
   * Verifies synchronous ALTER INDEX REBUILD execution, including model retraining, row migration,
   * retirement of prior generation, and query result accuracy.
   */
  @Test
  public void testManualRebuild() throws Exception {
    String table = "T_MR_" + generateUniqueName();
    String index = "IDX_MR_" + generateUniqueName();
    Random rng = new Random(7);
    Map<String, float[]> rows = new LinkedHashMap<>();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long before = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 200; i++) {
        Float[] v = new Float[4];
        float[] f = new float[4];
        for (int d = 0; d < 4; d++) {
          f[d] = rng.nextFloat() * 10;
          v[d] = f[d];
        }
        upsert(conn, table, String.format("r%03d", i), v, "l" + i);
        rows.put(String.format("r%03d", i), f);
      }
      conn.commit();
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");

      PTable idx = index(conn, index);
      long after = idx.getVectorCentroidGeneration();
      assertTrue(after > before);
      assertFalse(idx.isVectorRebuildInProgress());
      assertEquals(0, countCentroids(conn, index, before));
      assertEquals(idx.getVectorIvfLists().intValue(), countCentroids(conn, index, after));
      assertEquals(200, count(conn, index));
      assertIndexVerifies(table, index, 200);
      long total = 0;
      for (long size : scorecard(conn, index, after)) {
        total += size;
      }
      assertEquals(200, total);
      GenerationSummary summary = CentroidManager.loadGenerationSummary(conn, index, after);
      assertEquals(GenerationSummary.ACTIVE, summary.getRebuildState());
      assertEquals(VectorIndexRebuilder.MANUAL_REASON, summary.getTriggerReason());
      assertNotNull(summary.getLastRebuildTime());

      float[] q = new float[] { 5, 5, 5, 5 };
      assertEquals(bruteForceTopK(rows, q, "L2", 5),
        topK(conn, table, "/*+ VECTOR_PROBE_COUNT(" + idx.getVectorIvfLists() + ") */", q, 5));
    }
  }

  /**
   * Verifies dual-generation coexistence and write fencing during an active index rebuild
   * migration, and that queries probe both generations so rows already migrated into the building
   * generation's posting lists remain visible.
   */
  @Test
  public void testMigrationIsFencedAndQueriesProbeBothGenerations() throws Exception {
    String table = "T_MG_" + generateUniqueName();
    String index = "IDX_MG_" + generateUniqueName();
    Map<String, float[]> rows = new LinkedHashMap<>();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long active = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 40; i++) {
        Float[] v = near(i % 4, 0.01f * i);
        upsert(conn, table, "r" + i, v, "l" + i);
        rows.put("r" + i, unbox(v));
      }
      conn.commit();
      AtomicReference<Throwable> failure = new AtomicReference<>();
      VectorIndexRebuilderTestHooks.setMigrationHook((idx, building) -> {
        try (Connection c = connect(false)) {
          PTable current = index(c, index);
          assertEquals(Long.valueOf(active), current.getVectorCentroidGeneration());
          assertEquals(Long.valueOf(building), current.getVectorBuildingGeneration());
          assertTrue(countCentroids(c, index, active) > 0);
          assertTrue(countCentroids(c, index, building) > 0);
          assertEquals(count(c, table), count(c, index));

          // Concurrent writes during active migration
          Float[] moved = near(3, 0.5f);
          upsert(c, table, "r0", moved, "moved");
          upsert(c, table, "r1", near(1, 0.01f), "covered-only");
          upsert(c, table, "new", near(2, 0.2f), "new");
          c.createStatement().execute("DELETE FROM " + table + " WHERE ID = 'r2'");
          c.commit();
          assertEquals(count(c, table), count(c, index));

          String explain = explain(c, table, new float[] { 1, 0, 0, 0 });
          assertTrue(explain, explain.contains("ACROSS ACTIVE AND BUILDING GENERATIONS"));
          // The build pass has moved every row under the building generation's centroid IDs, so
          // a plan probing only the active generation finds nothing
          Map<String, float[]> expected = new LinkedHashMap<>(rows);
          expected.put("r0", unbox(moved));
          expected.put("new", unbox(near(2, 0.2f)));
          expected.remove("r2");
          for (int k = 0; k < KNOWN_CENTROIDS.size(); k++) {
            float[] q = unbox(near(k, 0.005f));
            assertEquals("cluster " + k, bruteForceTopK(expected, q, "L2", 5),
              topK(c, table, "", q, 5));
          }
        } catch (Throwable t) {
          failure.set(t);
        }
      });
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
      if (failure.get() != null) {
        throw new AssertionError(failure.get());
      }
      rows.put("r0", unbox(near(3, 0.5f)));
      rows.put("new", unbox(near(2, 0.2f)));
      rows.remove("r2");
      assertEquals(rows.size(), count(conn, index));
      assertIndexConsistent(table, index);
      PTable idx = index(conn, index);
      assertEquals(0, countCentroids(conn, index, active));
      float[] q = new float[] { 0, 1, 0, 0 };
      assertEquals(bruteForceTopK(rows, q, "L2", 5),
        topK(conn, table, "/*+ VECTOR_PROBE_COUNT(" + idx.getVectorIvfLists() + ") */", q, 5));
    }
  }

  /**
   * Verifies that a migration interrupted by failure preserves building generation state and can
   * resume to completion.
   */
  @Test
  public void testFailedMigrationResumes() throws Exception {
    String table = "T_FM_" + generateUniqueName();
    String index = "IDX_FM_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long active = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f * i), "l" + i);
      }
      conn.commit();
      VectorIndexRebuilderTestHooks.setMigrationHook((idx, building) -> {
        throw new RuntimeException("injected");
      });
      try {
        conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
        fail();
      } catch (SQLException e) {
        // expected
      }
      VectorIndexRebuilderTestHooks.setMigrationHook(null);
      PTable failed = index(conn, index);
      assertTrue(failed.isVectorRebuildInProgress());
      long building = failed.getVectorBuildingGeneration();
      assertEquals(Long.valueOf(active), failed.getVectorCentroidGeneration());
      assertEquals(count(conn, table), count(conn, index));

      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
      PTable resumed = index(conn, index);
      assertEquals("The resumed rebuild completes the same generation", Long.valueOf(building),
        resumed.getVectorCentroidGeneration());
      assertFalse(resumed.isVectorRebuildInProgress());
      assertEquals(0, countCentroids(conn, index, active));
      assertIndexVerifies(table, index, 40);
    }
  }

  /**
   * Verifies that concurrent rebuild attempts on the same index are rejected when a lock is held.
   */
  @Test
  public void testConcurrentRebuildIsExcluded() throws Exception {
    String table = "T_CR_" + generateUniqueName();
    String index = "IDX_CR_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      for (int i = 0; i < 8; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f), "l");
      }
      conn.commit();
      assertTrue(CentroidManager.claimRebuild(conn, index, "other", 60000));
      assertEquals(Outcome.IN_PROGRESS,
        VectorIndexRebuilder.rebuild(conn.unwrap(PhoenixConnection.class), index, true, "x"));
      try {
        conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("IN_PROGRESS"));
      }
      CentroidManager.releaseRebuild(conn, index, "other");
    }
  }

  /**
   * Verifies that a drift flagged by reassignment rate alone starts the automatic rebuild it
   * queues. Reconciliation resets the reassignment counters when it assesses, so the queued rebuild
   * must act on that assessment rather than reconcile and assess again.
   */
  @Test
  public void testReassignmentDriftRebuilds() throws Exception {
    String table = "T_RD_" + generateUniqueName();
    String index = "IDX_RD_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "b" + i, near(i % 4, 0.001f * i), "l");
      }
      conn.commit();
      flushScorecards();
      // Balanced posting lists, with 20 reassignments among 40 rows (rate 0.5)
      CentroidManager.adjustScorecard(conn, index, gen,
        java.util.Arrays.asList(new ScorecardRow(0, 0, 20)));
      try (PhoenixConnection enabled = connect(true).unwrap(PhoenixConnection.class)) {
        assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
          VectorIndexRebuilder.reconcile(enabled, index));
        List<String> queued = queuedAutomaticRebuilds(conn, index);
        assertEquals(1, queued.size());
        assertTrue(queued.get(0), queued.get(0).startsWith("REASSIGN_RATE_EXCEEDED"));
        assertEquals(0, reassigned(conn, index, gen, 0));
        // Run the queued rebuild as the rebuild task does
        assertEquals(Outcome.REBUILT,
          VectorIndexRebuilder.rebuild(enabled, index, false, queued.get(0)));
      }
      long rebuilt = index(conn, index).getVectorCentroidGeneration();
      assertTrue(rebuilt > gen);
      assertTrue(CentroidManager.loadGenerationSummary(conn, index, rebuilt).getTriggerReason()
        .startsWith("REASSIGN_RATE_EXCEEDED"));
    }
  }

  /**
   * Verifies gating conditions for automatic index rebuilds: reconciliation erases counter error
   * before assessing, and queues a rebuild only for real drift with automatic rebuild enabled; the
   * queued rebuild rechecks the configuration flag and the minimum rebuild interval.
   */
  @Test
  public void testAutomaticRebuildGates() throws Exception {
    String table = "T_AG_" + generateUniqueName();
    String index = "IDX_AG_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      // Populate balanced clusters, then inject an artificially skewed scorecard
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "b" + i, near(i % 4, 0.001f * i), "l");
      }
      conn.commit();
      flushScorecards();
      // Skew centroids 0 and 1, which hold 10 rows each, to 1000 and 0
      CentroidManager.adjustScorecard(conn, index, gen,
        java.util.Arrays.asList(new ScorecardRow(0, 990, 0), new ScorecardRow(1, -10, 0)));
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      try (PhoenixConnection enabled = connect(true).unwrap(PhoenixConnection.class)) {
        // Reconciliation erases the injected skew before assessing, so nothing is queued
        assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
          VectorIndexRebuilder.reconcile(enabled, index));
        assertTrue(queuedAutomaticRebuilds(conn, index).isEmpty());
      }
      // Skew data distribution towards centroid 0
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "b" + i, near(0, 0.01f * (i % 10)), "l");
      }
      conn.commit();
      // Drift is assessed and recorded, but automatic rebuild is disabled, so nothing is queued
      assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
        VectorIndexRebuilder.reconcile(pconn, index));
      assertTrue(queuedAutomaticRebuilds(conn, index).isEmpty());
      String reason = CentroidManager.loadGenerationSummary(conn, index, gen).getTriggerReason();
      assertTrue(reason, reason.contains("SKEW_RATIO_EXCEEDED: unbounded"));
      // A queued rebuild that runs once automatic rebuild is disabled does nothing
      assertEquals(Outcome.DISABLED, VectorIndexRebuilder.rebuild(pconn, index, false, reason));
      assertEquals(gen, index(conn, index).getVectorCentroidGeneration().longValue());

      try (PhoenixConnection enabled = connect(true).unwrap(PhoenixConnection.class)) {
        assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
          VectorIndexRebuilder.reconcile(enabled, index));
        List<String> queued = queuedAutomaticRebuilds(conn, index);
        assertEquals(1, queued.size());
        assertEquals(Outcome.REBUILT,
          VectorIndexRebuilder.rebuild(enabled, index, false, queued.get(0)));
        long rebuilt = index(conn, index).getVectorCentroidGeneration();
        assertTrue(rebuilt > gen);
        assertTrue(CentroidManager.loadGenerationSummary(conn, index, rebuilt).getTriggerReason()
          .contains("SKEW_RATIO_EXCEEDED"));
        Properties soon = props(true);
        soon.setProperty(QueryServices.VECTOR_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, "0.5");
        soon.setProperty(QueryServices.VECTOR_REBUILD_MIN_INTERVAL_MS_ATTRIB, "86400000");
        try (PhoenixConnection eager =
          DriverManager.getConnection(getUrl(), soon).unwrap(PhoenixConnection.class)) {
          assertEquals(Outcome.TOO_SOON,
            VectorIndexRebuilder.rebuild(eager, index, false, "SKEW_RATIO_EXCEEDED"));
          assertEquals(Outcome.REBUILT, VectorIndexRebuilder.rebuild(eager, index, true, "OP"));
        }
      }
      // Ensure multi-generational row keys do not collide across migration passes
      assertEquals(40, count(conn, index));
      assertIndexConsistent(table, index);
    }
  }

  /** Verifies index rebuild support for functional vector indexes over BSON expressions. */
  @Test
  public void testFunctionalIndexRebuild() throws Exception {
    String table = "T_BR_" + generateUniqueName();
    String index = "IDX_BR_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        Random rng = new Random(3);
        for (int i = 0; i < 60; i++) {
          float[] v = new float[4];
          for (int d = 0; d < 4; d++) {
            v[d] = rng.nextFloat();
          }
          ps.setString(1, "d" + i);
          ps.setObject(2,
            new BsonDocument("embedding", new BsonBinary(BinaryVector.floatVector(v))));
          ps.executeUpdate();
        }
      }
      conn.commit();
      conn.createStatement()
        .execute("CREATE VECTOR INDEX " + index + " ON " + table
          + " (BSON_VECTOR_VALUE(DOC, 'embedding', 4)) WITH (algorithm='IVF', metric='L2', lists=4,"
          + " sample_size=60)");
      long before = index(conn, index).getVectorCentroidGeneration();
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
      assertNotEquals(before, index(conn, index).getVectorCentroidGeneration().longValue());
      assertEquals(60, count(conn, index));
      assertIndexVerifies(table, index, 60);
    }
  }

  /**
   * Verifies asynchronous ALTER INDEX REBUILD ASYNC task creation and background execution by the
   * task handler.
   */
  @Test
  public void testAsyncRebuildRunsAsTask() throws Exception {
    String table = "T_AR_" + generateUniqueName();
    String index = "IDX_AR_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long before = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 20; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f * i), "l");
      }
      conn.commit();
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD ASYNC");
      List<Task.TaskRecord> tasks = Task.queryTaskTable(conn, null, null, index,
        PTable.TaskType.VECTOR_INDEX_REBUILD, null, null);
      assertEquals(1, tasks.size());
      assertTrue(tasks.get(0).getData(), tasks.get(0).getData().contains("\"manual\":true"));
      runTaskSweep();
      assertTrue(index(conn, index).getVectorCentroidGeneration() > before);
      assertEquals(PTable.TaskStatus.COMPLETED.toString(),
        Task
          .queryTaskTable(conn, null, null, index, PTable.TaskType.VECTOR_INDEX_REBUILD, null, null)
          .get(0).getStatus());
      assertIndexVerifies(table, index, 20);
    }
  }

  private static float[] unbox(Float[] v) {
    float[] f = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      f[i] = v[i];
    }
    return f;
  }

  private static List<String> topK(Connection conn, String table, String hint, float[] q, int k)
    throws SQLException {
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(
      "SELECT " + hint + " ID FROM " + table + " ORDER BY L2_DISTANCE(V, ?) LIMIT " + k)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", box(q)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  private static String explain(Connection conn, String table, float[] q) throws SQLException {
    StringBuilder sb = new StringBuilder();
    try (PreparedStatement ps = conn.prepareStatement(
      "EXPLAIN SELECT ID FROM " + table + " ORDER BY L2_DISTANCE(V, ?) LIMIT 3")) {
      ps.setArray(1, conn.createArrayOf("FLOAT", box(q)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          sb.append(rs.getString(1)).append('\n');
        }
      }
    }
    return sb.toString();
  }

  private static Float[] box(float[] q) {
    Float[] b = new Float[q.length];
    for (int i = 0; i < q.length; i++) {
      b[i] = q[i];
    }
    return b;
  }
}
