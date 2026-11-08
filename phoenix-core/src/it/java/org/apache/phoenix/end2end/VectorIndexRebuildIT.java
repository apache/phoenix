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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_HBASE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSourceImpl;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.ScorecardAccumulator;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.index.vector.VectorIndexRebuilder;
import org.apache.phoenix.index.vector.VectorIndexRebuilder.Outcome;
import org.apache.phoenix.index.vector.VectorIndexRebuilderTestHooks;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for vector index drift scorecards, periodic reconciliation, and the online
 * rebuild that migrates an index from one centroid generation to the next.
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
   * Returns connection properties with no index population sleep, no reconciliation interval, and
   * no minimum rebuild interval. The drift minimum population is 10, and {@code autoRebuild} sets
   * the automatic rebuild flag.
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
   * Returns centroid {@code c} of {@link VectorIndexTestUtil#KNOWN_CENTROIDS} with {@code jitter}
   * added to each component.
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
    // loadScorecard returns the rows in centroid ID order
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

  private static long scorecardFence(Connection conn, String index, long generation, int centroid)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("SELECT SCORECARD_FENCE FROM " + SYSTEM_VECTOR_CENTROID_NAME
        + " WHERE INDEX_NAME = ? AND GENERATION_ID = ?" + " AND CENTROID_ID = ?")) {
      ps.setString(1, index);
      ps.setLong(2, generation);
      ps.setInt(3, centroid);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getLong(1);
      }
    }
  }

  private static long reassigned(Connection conn, String index, long generation, int centroid)
    throws SQLException {
    return CentroidManager.loadScorecard(conn, index, generation).get(centroid).getReassignCount();
  }

  /**
   * Returns the reason of each automatic rebuild task queued for an index. This method reads the
   * task data the same way as the rebuild task.
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
   * Runs task sweeps until {@code done} returns true, and fails after two minutes. Rebuilds and
   * reconciliations run outside the sweeps. A sweep starts them, and a later sweep collects their
   * results.
   */
  private static void sweepUntil(String what, Callable<Boolean> done) throws Exception {
    long deadline = System.currentTimeMillis() + 120000;
    while (true) {
      runTaskSweep();
      if (done.call()) {
        return;
      }
      assertTrue("Timed out awaiting " + what, System.currentTimeMillis() < deadline);
      Thread.sleep(100);
    }
  }

  private static List<Task.TaskRecord> tasks(Connection conn, String index, PTable.TaskType type)
    throws SQLException {
    return Task.queryTaskTable(conn, null, null, index, type, null, null);
  }

  private static String taskStatus(Connection conn, String index, PTable.TaskType type)
    throws SQLException {
    return tasks(conn, index, type).get(0).getStatus();
  }

  private static long reconciliations(String index) {
    return ((MetricsVectorIndexSourceImpl) MetricsIndexerSourceFactory.getInstance()
      .getMetricsVectorIndexSource()).getMetricsRegistry()
        .getHistogram(MetricsVectorIndexSource.VECTOR_SCORECARD_RECONCILE_TIME + "." + index)
        .getCount();
  }

  /**
   * Verifies that inline scorecard maintenance counts inserts, deletes, moves between centroids,
   * and covered column updates. The scorecard must match a grouped count of the index rows, and a
   * move counts as a reassignment to its new centroid.
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
      upsert(conn, table, "r1", near(1, 0.03f), "l1"); // stays in centroid 1
      upsert(conn, table, "r2", near(2, 0.01f), "changed"); // changes only a covered column
      conn.commit();
      conn.createStatement().execute("DELETE FROM " + table + " WHERE ID IN ('r3', 'r7')");
      conn.commit();
      flushScorecards();

      long[] expected = groupedCount(conn, index, 4);
      assertEquals(2, expected[0]);
      assertEquals(4, expected[1]);
      assertEquals(3, expected[2]);
      assertEquals(1, expected[3]);
      assertEquals(Arrays.toString(expected), Arrays.toString(scorecard(conn, index, gen)));
      assertEquals("The move into centroid 1 counts a reassignment", 1,
        reassigned(conn, index, gen, 1));
      assertEquals(0, reassigned(conn, index, gen, 0));
    }
  }

  /** Verifies that a replay of an applied batch does not change the scorecard counts. */
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
        assertEquals("[2, 2, 2, 2]", Arrays.toString(scorecard(conn, index, gen)));
      }
    }
  }

  /** Verifies that each scorecard flush adds its deltas to the stored counts. */
  @Test
  public void testFlushesAccumulate() throws Exception {
    String index = "IDX_FL_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager.persistCentroids(conn, index, 1L,
        Arrays.asList(new float[] { 1 }, new float[] { 2 }));
      for (long delta : new long[] { 5, 7 }) {
        Map<ScorecardAccumulator.Key, long[]> batch = new HashMap<>();
        batch.put(new ScorecardAccumulator.Key(index, 1L, 0), new long[] { delta, 0, 0 });
        ScorecardAccumulator.getInstance().accumulate(batch,
          EnvironmentEdgeManager.currentTimeMillis());
        flushScorecards();
      }
      assertEquals(12, scorecard(conn, index, 1L)[0]);
    }
  }

  /**
   * Verifies that a flush discards the buffered deltas of a deleted generation and does not create
   * its scorecard rows again.
   */
  @Test
  public void testFlushDoesNotResurrectRetiredGeneration() throws Exception {
    String index = "IDX_RT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager.persistCentroids(conn, index, 1L, Arrays.asList(new float[] { 1 }));
      Map<ScorecardAccumulator.Key, long[]> batch = new HashMap<>();
      batch.put(new ScorecardAccumulator.Key(index, 1L, 0), new long[] { 3, 0, 0 });
      ScorecardAccumulator.getInstance().accumulate(batch,
        EnvironmentEdgeManager.currentTimeMillis());
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
   * Verifies that CREATE VECTOR INDEX writes the initial scorecard counts and generation summary,
   * and queues the first reconciliation task.
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
   * Verifies that reconciliation sets each posting list population to its real row count, clears
   * the reassignment counts, and sets the last scorecard update time.
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
      // Corrupt the counters. Centroid 0 holds 2 rows and centroid 3 holds 0 rows.
      CentroidManager.adjustScorecard(conn, index, gen,
        Arrays.asList(new ScorecardRow(0, 97, 5), new ScorecardRow(3, 42, 0)));
      assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
        VectorIndexRebuilder.reconcile(conn.unwrap(PhoenixConnection.class), index));
      assertEquals("[2, 2, 2, 0]", Arrays.toString(scorecard(conn, index, gen)));
      assertEquals(0, reassigned(conn, index, gen, 0));
      assertNotNull(
        CentroidManager.loadGenerationSummary(conn, index, gen).getLastScorecardUpdate());
    }
  }

  /**
   * Verifies that a failed flush keeps its deltas and that the next flush writes them.
   * Reconciliation cannot recover reassignment counts, so they must survive an outage of the
   * scorecard table.
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
          // The accumulator keeps the deltas
        } finally {
          admin.enableTable(SYSTEM_VECTOR_CENTROID_HBASE_TABLE_NAME);
        }
      }
      flushScorecards();
      assertEquals("[0, 2, 1, 1]", Arrays.toString(scorecard(conn, index, gen)));
      assertEquals(1, reassigned(conn, index, gen, 1));
    }
  }

  /**
   * Verifies a RegionServer flush that commits after reconciliation reads the scorecard and before
   * it writes. Reconciliation writes the absolute row counts, so they replace the population delta
   * of the flush. It subtracts the reassignment counts that it read, so the reassignments of the
   * flush stay. A later delta that is older than the recount fence has no effect. A delta at the
   * fence applies. A merged delta keeps its older write time. A recount of a deleted generation
   * fails and does not create a row.
   */
  @Test
  public void testReconcileFencesConcurrentFlush() throws Exception {
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
      CentroidManager.adjustScorecard(conn, index, gen, Arrays.asList(new ScorecardRow(0, 7, 4)));
      VectorIndexRebuilderTestHooks.setReconcileHook((name, generation) -> {
        // A concurrent flush adds one row and three reassignments to centroid 0
        try (Connection c = connect(false)) {
          CentroidManager.adjustScorecard(c, name, generation,
            Arrays.asList(new ScorecardRow(0, 1, 3)));
        }
      });
      try {
        VectorIndexScorecard.reconcile(conn.unwrap(PhoenixConnection.class), index(conn, index),
          gen);
      } finally {
        VectorIndexRebuilderTestHooks.setReconcileHook(null);
      }
      // Centroid 0 holds 2 rows. The absolute count replaces the corrupt +7 and the +1 of the
      // concurrent flush. The reset removes the 4 earlier reassignments and keeps the 3 new ones.
      assertEquals("[2, 2, 2, 0]", Arrays.toString(scorecard(conn, index, gen)));
      assertEquals(3, reassigned(conn, index, gen, 0));
      // The recount already counts a row with a write time before its fence
      CentroidManager.adjustScorecard(conn, index, gen,
        Arrays.asList(new ScorecardRow(0, 1, 0, EnvironmentEdgeManager.currentTimeMillis())));
      assertEquals("[2, 2, 2, 0]", Arrays.toString(scorecard(conn, index, gen)));
      long fence = scorecardFence(conn, index, gen, 0);
      CentroidManager.adjustScorecard(conn, index, gen,
        Arrays.asList(new ScorecardRow(0, 1, 0, fence - 1)));
      assertEquals("[2, 2, 2, 0]", Arrays.toString(scorecard(conn, index, gen)));
      CentroidManager.adjustScorecard(conn, index, gen,
        Arrays.asList(new ScorecardRow(0, 1, 0, fence)));
      assertEquals("[3, 2, 2, 0]", Arrays.toString(scorecard(conn, index, gen)));
      // A merge of a write after the fence and a write before it keeps the older time
      ScorecardAccumulator.Key key = new ScorecardAccumulator.Key(index, gen, 0);
      Map<ScorecardAccumulator.Key, long[]> delta = new HashMap<>();
      delta.put(key, new long[] { 1, 0, 0 });
      ScorecardAccumulator.getInstance().accumulate(delta, fence + 1);
      ScorecardAccumulator.getInstance().accumulate(delta, fence - 1);
      flushScorecards();
      assertEquals("[3, 2, 2, 0]", Arrays.toString(scorecard(conn, index, gen)));
      // The next recount repairs the count and moves the fence forward
      VectorIndexScorecard.reconcile(conn.unwrap(PhoenixConnection.class), index(conn, index), gen);
      assertEquals("[2, 2, 2, 0]", Arrays.toString(scorecard(conn, index, gen)));
      assertTrue(scorecardFence(conn, index, gen, 0) > fence);
      // A recount of a generation with no rows fails and does not create its summary row
      long deleted = gen + 5;
      try {
        VectorIndexScorecard.reconcile(conn.unwrap(PhoenixConnection.class), index(conn, index),
          deleted);
        fail("A recount of a deleted generation must fail");
      } catch (SQLException expected) {
        // The summary row does not exist
      }
      assertFalse(CentroidManager.listGenerations(conn, index).contains(deleted));
    }
  }

  /**
   * Verifies that the debris delete stops when a different holder takes the rebuild claim. The
   * delete renews the claim before each generation.
   */
  @Test
  public void testDebrisDeleteStopsOnLostClaim() throws Exception {
    String table = "T_DL_" + generateUniqueName();
    String index = "IDX_DL_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      CentroidManager.deleteTasks(conn, index(conn, index));
      long gen = index(conn, index).getVectorCentroidGeneration();
      CentroidManager.persistCentroids(conn, index, gen + 1, KNOWN_CENTROIDS);
      CentroidManager.persistCentroids(conn, index, gen + 2, KNOWN_CENTROIDS);
      try (CentroidManager.RebuildClaim claim = CentroidManager.claim(pconn, index, "holder")) {
        assertNotNull(claim);
        CentroidManager.DebrisVisitor thief = new CentroidManager.DebrisVisitor() {
          @Override
          public void beforeDelete(long generation) {
          }

          @Override
          public void deleted(long generation) throws SQLException {
            try {
              Thread.sleep(5);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new SQLException(e);
            }
            assertTrue(CentroidManager.claimRebuild(conn, index, "thief", 1));
          }
        };
        try {
          CentroidManager.deleteDebrisGenerations(pconn, claim, index(conn, index), true, thief);
          fail("The debris delete must stop after the claim is lost");
        } catch (SQLException e) {
          assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(),
            e.getErrorCode());
        } finally {
          CentroidManager.releaseRebuild(conn, index, "thief");
        }
      }
      assertEquals(0, countCentroids(conn, index, gen + 1));
      assertEquals(KNOWN_CENTROIDS.size(), countCentroids(conn, index, gen + 2));
    }
  }

  /**
   * Verifies that the reconciliation task runs each time it is due, records its duration, and stays
   * STARTED between runs. DROP INDEX removes the task and the centroid rows of the index.
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
        // Set the count of centroid 0, which holds 1 row, to 50
        CentroidManager.adjustScorecard(conn, index, gen,
          Arrays.asList(new ScorecardRow(0, 49, 0)));
        // Clear the last scorecard update time, so the next sweep finds the task due
        conn.createStatement()
          .execute("UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME
            + " (INDEX_NAME, GENERATION_ID, CENTROID_ID, LAST_SCORECARD_UPDATE) VALUES ('" + index
            + "', " + gen + ", -1, NULL)");
        conn.commit();
        sweepUntil("reconciliation " + sweep, () -> scorecard(conn, index, gen)[0] == 1);
        assertEquals(PTable.TaskStatus.STARTED.toString(),
          taskStatus(conn, index, PTable.TaskType.VECTOR_SCORECARD_RECONCILE));
      }
      assertTrue("The reconciliation duration is recorded", reconciliations(index) >= 2);
      conn.createStatement().execute("DROP INDEX " + index + " ON " + table);
      assertTrue(Task.queryTaskTable(conn, null, null, index,
        PTable.TaskType.VECTOR_SCORECARD_RECONCILE, null, null).isEmpty());
      assertEquals(0, countCentroids(conn, index, null));
      // A reconcile of the dropped index deletes the rows that a late writer left
      CentroidManager.persistCentroids(conn, index, gen, KNOWN_CENTROIDS);
      assertEquals(VectorIndexRebuilder.ReconcileOutcome.INDEX_DROPPED,
        VectorIndexRebuilder.reconcile(conn.unwrap(PhoenixConnection.class), index));
      assertTrue(CentroidManager.listGenerations(conn, index).isEmpty());
    }
  }

  /**
   * Verifies that a synchronous ALTER INDEX ... REBUILD trains a new generation, migrates every row
   * to it, and deletes the previous generation. The new generation records a retire time for older
   * generations. After the rebuild, the scorecard counts every row and a query that probes all
   * lists returns the exact nearest neighbors.
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
      // Reconciliation and fsck keep an older generation until the retire time
      assertNotNull(summary.getRetireAfter());
      assertTrue(summary.getRetireAfter() > summary.getLastRebuildTime());

      float[] q = new float[] { 5, 5, 5, 5 };
      assertEquals(bruteForceTopK(rows, q, "L2", 5),
        topK(conn, table, "/*+ VECTOR_PROBE_COUNT(" + idx.getVectorIvfLists() + ") */", q, 5));
    }
  }

  /**
   * Verifies that during a migration both generations keep their centroids, concurrent writes keep
   * the index complete, and queries probe both generations. Queries must find the rows that the
   * migration already moved to the building generation.
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

          // Writes concurrent with the migration
          Float[] moved = near(3, 0.5f);
          upsert(c, table, "r0", moved, "moved");
          upsert(c, table, "r1", near(1, 0.01f), "covered-only");
          upsert(c, table, "new", near(2, 0.2f), "new");
          c.createStatement().execute("DELETE FROM " + table + " WHERE ID = 'r2'");
          c.commit();
          assertEquals(count(c, table), count(c, index));

          String explain = explain(c, table, new float[] { 1, 0, 0, 0 });
          assertTrue(explain, explain.contains("ACROSS ACTIVE AND BUILDING GENERATIONS"));
          // The first build pass moved every row to the centroid IDs of the building generation.
          // Thus a plan that probes only the active generation finds no rows.
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
   * Verifies that a failed migration keeps the building generation, and that a resumed rebuild
   * completes that generation. A resume does not enable an index that an operator disabled.
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
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      assertEquals("A resumption without a migration does nothing", Outcome.NOT_MIGRATING,
        VectorIndexRebuilder.rebuild(pconn, index, true, VectorIndexRebuilder.RESUME_REASON));
      assertEquals(Long.valueOf(active), index(conn, index).getVectorCentroidGeneration());
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
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " DISABLE");

      assertEquals(Outcome.REBUILT,
        VectorIndexRebuilder.rebuild(pconn, index, true, VectorIndexRebuilder.RESUME_REASON));
      PTable resumed = index(conn, index);
      assertEquals("The resumed rebuild completes the same generation", Long.valueOf(building),
        resumed.getVectorCentroidGeneration());
      assertFalse(resumed.isVectorRebuildInProgress());
      assertEquals("Resuming does not re-enable an index an operator disabled", PIndexState.DISABLE,
        resumed.getIndexState());
      assertEquals(0, countCentroids(conn, index, active));
      // An operator rebuild enables the index again
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
      assertEquals(PIndexState.ACTIVE, index(conn, index).getIndexState());
      assertIndexVerifies(table, index, 40);
    }
  }

  /**
   * Verifies that a rebuild returns IN_PROGRESS while another process holds the rebuild claim of
   * the index.
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
   * Verifies that a drift that only the reassignment rate shows starts the automatic rebuild that
   * it queues. Reconciliation resets the reassignment counters when it assesses drift. Thus the
   * queued rebuild must use that assessment, and must not reconcile and assess again.
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
      // Balanced posting lists with 20 reassignments among 40 rows, a rate of 0.5
      CentroidManager.adjustScorecard(conn, index, gen, Arrays.asList(new ScorecardRow(0, 0, 20)));
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
   * Verifies the conditions that gate an automatic rebuild. Reconciliation removes counter errors
   * before it assesses drift. It queues a rebuild only for real drift and only if automatic rebuild
   * is on. The queued rebuild checks the flag and the minimum rebuild interval again.
   */
  @Test
  public void testAutomaticRebuildGates() throws Exception {
    String table = "T_AG_" + generateUniqueName();
    String index = "IDX_AG_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      long gen = index(conn, index).getVectorCentroidGeneration();
      // Write balanced clusters, then put an artificial skew in the scorecard
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "b" + i, near(i % 4, 0.001f * i), "l");
      }
      conn.commit();
      flushScorecards();
      // Set the counts of centroids 0 and 1, which hold 10 rows each, to 1000 and 0
      CentroidManager.adjustScorecard(conn, index, gen,
        Arrays.asList(new ScorecardRow(0, 990, 0), new ScorecardRow(1, -10, 0)));
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      try (PhoenixConnection enabled = connect(true).unwrap(PhoenixConnection.class)) {
        // Reconciliation removes the skew before it assesses drift, so it queues no rebuild
        assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
          VectorIndexRebuilder.reconcile(enabled, index));
        assertTrue(queuedAutomaticRebuilds(conn, index).isEmpty());
      }
      // Move every row near centroid 0, so the data has a real skew
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "b" + i, near(0, 0.01f * (i % 10)), "l");
      }
      conn.commit();
      // Reconciliation records the drift but queues no rebuild, because automatic rebuild is off
      assertEquals(VectorIndexRebuilder.ReconcileOutcome.RECONCILED,
        VectorIndexRebuilder.reconcile(pconn, index));
      assertTrue(queuedAutomaticRebuilds(conn, index).isEmpty());
      String reason = CentroidManager.loadGenerationSummary(conn, index, gen).getTriggerReason();
      assertTrue(reason, reason.contains("SKEW_RATIO_EXCEEDED: unbounded"));
      // A queued rebuild that runs while automatic rebuild is off does nothing
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
      // After two rebuilds, the row keys of different generations do not collide
      assertEquals(40, count(conn, index));
      assertIndexConsistent(table, index);
    }
  }

  /** Verifies the rebuild of a functional vector index over a BSON expression. */
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
   * Verifies that ALTER INDEX ... REBUILD ASYNC queues a manual rebuild task and that task sweeps
   * run it to completion. A sweep that loses track of the completed rebuild must not run it again.
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
      CountDownLatch migrating = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      VectorIndexRebuilderTestHooks.setMigrationHook((idx, building) -> {
        migrating.countDown();
        assertTrue(release.await(30, TimeUnit.SECONDS));
      });
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD ASYNC");
      List<Task.TaskRecord> tasks = tasks(conn, index, PTable.TaskType.VECTOR_INDEX_REBUILD);
      assertEquals(1, tasks.size());
      assertTrue(tasks.get(0).getData(), tasks.get(0).getData().contains("\"manual\":true"));
      // The sweep starts the rebuild and returns before the migration completes
      runTaskSweep();
      assertTrue(migrating.await(60, TimeUnit.SECONDS));
      assertEquals(PTable.TaskStatus.STARTED.toString(),
        taskStatus(conn, index, PTable.TaskType.VECTOR_INDEX_REBUILD));
      release.countDown();
      sweepUntil("the rebuild", () -> PTable.TaskStatus.COMPLETED.toString()
        .equals(taskStatus(conn, index, PTable.TaskType.VECTOR_INDEX_REBUILD)));
      long rebuilt = index(conn, index).getVectorCentroidGeneration();
      assertTrue(rebuilt > before);
      assertIndexVerifies(table, index, 20);

      // A RegionServer that takes over SYSTEM.TASK after the rebuild completes, but before a sweep
      // records the result, finds the row STARTED and no known work. It completes the row and does
      // not rebuild the index again.
      try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + SYSTEM_TASK_NAME
        + " (TASK_TYPE, TASK_TS, TABLE_NAME, TASK_STATUS) VALUES (?, ?, ?, 'STARTED')")) {
        ps.setByte(1, PTable.TaskType.VECTOR_INDEX_REBUILD.getSerializedValue());
        ps.setTimestamp(2, tasks.get(0).getTimeStamp());
        ps.setString(3, index);
        ps.executeUpdate();
      }
      conn.commit();
      assertEquals(PTable.TaskStatus.STARTED.toString(),
        taskStatus(conn, index, PTable.TaskType.VECTOR_INDEX_REBUILD));
      sweepUntil("the request to complete again", () -> PTable.TaskStatus.COMPLETED.toString()
        .equals(taskStatus(conn, index, PTable.TaskType.VECTOR_INDEX_REBUILD)));
      assertEquals(rebuilt, index(conn, index).getVectorCentroidGeneration().longValue());
    }
  }

  /**
   * Verifies that the recurring reconciliation task moves to a new row before the SYSTEM.TASK TTL
   * expires its row. The TTL counts from TASK_TS, which does not change.
   */
  @Test
  public void testReconcileTaskRenewsItsRow() throws Exception {
    String table = "T_RN_" + generateUniqueName();
    String index = "IDX_RN_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      // Replace the task row with one queued six days ago, four days before the TTL expires it
      conn.createStatement()
        .execute("DELETE FROM " + SYSTEM_TASK_NAME + " WHERE TASK_TYPE = "
          + PTable.TaskType.VECTOR_SCORECARD_RECONCILE.getSerializedValue() + " AND TABLE_NAME = '"
          + index + "'");
      Timestamp enqueued = new Timestamp(System.currentTimeMillis() - 6 * 24 * 3600 * 1000L);
      try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + SYSTEM_TASK_NAME
        + " (TASK_TYPE, TASK_TS, TABLE_NAME, TASK_STATUS) VALUES (?, ?, ?, 'CREATED')")) {
        ps.setByte(1, PTable.TaskType.VECTOR_SCORECARD_RECONCILE.getSerializedValue());
        ps.setTimestamp(2, enqueued);
        ps.setString(3, index);
        ps.executeUpdate();
      }
      conn.commit();
      sweepUntil("the old row to complete",
        () -> tasks(conn, index, PTable.TaskType.VECTOR_SCORECARD_RECONCILE).stream()
          .anyMatch(t -> t.getTimeStamp().equals(enqueued)
            && PTable.TaskStatus.COMPLETED.toString().equals(t.getStatus())));
      List<Task.TaskRecord> live = new ArrayList<>();
      for (Task.TaskRecord task : tasks(conn, index, PTable.TaskType.VECTOR_SCORECARD_RECONCILE)) {
        if (!PTable.TaskStatus.COMPLETED.toString().equals(task.getStatus())) {
          live.add(task);
        }
      }
      assertEquals(1, live.size());
      assertTrue(live.get(0).getTimeStamp().getTime() > System.currentTimeMillis() - 3600000);
    }
  }

  /**
   * Verifies that ALTER INDEX ... REBUILD trains, builds, and activates an index that CREATE did
   * not train because the table was empty.
   */
  @Test
  public void testRebuildTrainsAndActivatesUntrainedIndex() throws Exception {
    String table = "T_UT_" + generateUniqueName();
    String index = "IDX_UT_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) INCLUDE (LABEL) WITH (algorithm='IVF', metric='L2', lists=4, sample_size=100)");
      assertEquals(null, index(conn, index).getVectorCentroidGeneration());
      assertEquals(PIndexState.BUILDING, index(conn, index).getIndexState());
      VectorIndexTestUtil.loadClusteredVectors(conn, table, 100);
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
      PTable idx = index(conn, index);
      assertNotNull(idx.getVectorCentroidGeneration());
      assertEquals(PIndexState.ACTIVE, idx.getIndexState());
      assertEquals(100, count(conn, index));
      assertIndexVerifies(table, index, 100);
      long total = 0;
      for (long size : scorecard(conn, index, idx.getVectorCentroidGeneration())) {
        total += size;
      }
      assertEquals(100, total);
    }
  }

  /**
   * Verifies that ALTER INDEX ... REBUILD ASYNC of a disabled index adds the rows written while the
   * index was disabled, and activates the index.
   */
  @Test
  public void testAsyncRebuildActivatesDisabledIndex() throws Exception {
    String table = "T_DR_" + generateUniqueName();
    String index = "IDX_DR_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      for (int i = 0; i < 20; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f * i), "l");
      }
      conn.commit();
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " DISABLE");
      for (int i = 20; i < 40; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f * i), "l");
      }
      conn.commit();
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD ASYNC");
      sweepUntil("the rebuild", () -> PTable.TaskStatus.COMPLETED.toString()
        .equals(taskStatus(conn, index, PTable.TaskType.VECTOR_INDEX_REBUILD)));
      assertEquals(PIndexState.ACTIVE, index(conn, index).getIndexState());
      assertIndexVerifies(table, index, 40);
    }
  }

  /**
   * Verifies that the catch up pass rebuilds a row that a write changed after the first build pass
   * started. The write did not go through index maintenance.
   */
  @Test
  public void testCatchUpRebuildsRowsChangedDuringMigration() throws Exception {
    String table = "T_CU_" + generateUniqueName();
    String index = "IDX_CU_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f * i), "l" + i);
      }
      conn.commit();
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable dataTable = pconn.getTableNoCache(table);
      PColumn v = dataTable.getColumnForColumnName("V");
      VectorIndexRebuilderTestHooks.setMigrationHook((idx, building) -> {
        // A raw HBase write has no index metadata, so index maintenance does not see it
        try (Table hTable =
          pconn.getQueryServices().getTable(dataTable.getPhysicalName().getBytes())) {
          Put put = new Put(Bytes.toBytes("r0"));
          put.addColumn(v.getFamilyName().getBytes(), v.getColumnQualifierBytes(),
            PVectorFloat.INSTANCE.toBytes(unbox(near(3, 0.5f))));
          hTable.put(put);
        }
      });
      conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
      assertEquals(40, count(conn, index));
      assertIndexConsistent(table, index);
    }
  }

  /**
   * Verifies that a rebuild waits until cached metadata expires before it migrates the index. The
   * table has an UPDATE_CACHE_FREQUENCY of some seconds. One writer sends its own index
   * maintainers. It has its own query services, and it caches the metadata of the active generation
   * before the rebuild. After the promotion, each writer upserts a row and deletes a row. A full
   * probe search then finds exactly the rows of the data table.
   * <p>
   * The other writer lets the region server build the index maintainers from its own metadata
   * cache. Background tasks on the region server can refresh that cache. Thus this test does not
   * reliably cover the cache of the region server. The test also does not cover the wait after the
   * promotion. One JVM keeps the outgoing centroids in its centroid cache, so a short second wait
   * has no visible effect.
   */
  @Test
  public void testRebuildWaitsForCachedMetadataToExpire() throws Exception {
    String table = "T_UCF_" + generateUniqueName();
    String index = "IDX_UCF_" + generateUniqueName();
    Map<String, float[]> rows = new LinkedHashMap<>();
    Properties clientMetadata = props(false);
    clientMetadata.setProperty(QueryServices.INDEX_USE_SERVER_METADATA_ATTRIB, "false");
    try (Connection conn = connect(false); Connection serverWriter = connect(false);
      Connection clientWriter = DriverManager.getConnection(
        QueryUtil.getConnectionUrl(clientMetadata, config, "ucf_client_" + generateUniqueName()),
        clientMetadata)) {
      // The rebuild refreshes the metadata cache of its own query services. The client writer
      // must keep its stale metadata, so it must not share that cache.
      assertNotSame(conn.unwrap(PhoenixConnection.class).getMetaDataCache(),
        clientWriter.unwrap(PhoenixConnection.class).getMetaDataCache());
      // The cache frequency is longer than a rebuild without the cache wait, and shorter than the
      // test timeout
      setupTableAndKnownCentroids(conn, table, index, "FLOAT", "UPDATE_CACHE_FREQUENCY=15000");
      long active = index(conn, index).getVectorCentroidGeneration();
      for (int i = 0; i < 40; i++) {
        Float[] v = near(i % 4, 0.01f * i);
        upsert(conn, table, "r" + i, v, "l" + i);
        rows.put("r" + i, unbox(v));
      }
      conn.commit();
      // Cache the metadata of the active generation in each writer and in the region server
      upsert(serverWriter, table, "s0", near(0, 0.405f), "s0");
      serverWriter.commit();
      rows.put("s0", unbox(near(0, 0.405f)));
      upsert(clientWriter, table, "c0", near(1, 0.415f), "c0");
      clientWriter.commit();
      rows.put("c0", unbox(near(1, 0.415f)));

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<Void> rebuild = executor.submit(() -> {
          Properties props = props(false);
          props.setProperty(QueryServices.INDEX_POPULATION_SLEEP_TIME, "2000");
          try (Connection c = DriverManager.getConnection(getUrl(), props)) {
            c.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
          }
          return null;
        });
        long deadline = System.currentTimeMillis() + 120000;
        while (Long.valueOf(active).equals(index(conn, index).getVectorCentroidGeneration())) {
          if (rebuild.isDone()) {
            rebuild.get();
            fail("The rebuild ended without a promotion");
          }
          assertTrue("Timed out awaiting the promotion", System.currentTimeMillis() < deadline);
          Thread.sleep(50);
        }
        // These writes are after the time range of the catch-up build. A writer with the metadata
        // of the outgoing generation does not maintain the promoted generation.
        upsert(serverWriter, table, "s1", near(2, 0.425f), "s1");
        serverWriter.createStatement().execute("DELETE FROM " + table + " WHERE ID = 'r1'");
        serverWriter.commit();
        upsert(clientWriter, table, "c1", near(3, 0.435f), "c1");
        clientWriter.createStatement().execute("DELETE FROM " + table + " WHERE ID = 'r2'");
        clientWriter.commit();
        rows.put("s1", unbox(near(2, 0.425f)));
        rows.put("c1", unbox(near(3, 0.435f)));
        rows.remove("r1");
        rows.remove("r2");
        rebuild.get(120, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(null, table, null, HConstants.LATEST_TIMESTAMP);
      PTable idx = index(conn, index);
      assertNotEquals(Long.valueOf(active), idx.getVectorCentroidGeneration());
      assertFalse(idx.isVectorRebuildInProgress());
      assertEquals(0, countCentroids(conn, index, active));
      assertEquals(rows.size(), count(conn, table));
      assertEquals("index rows", rows.size(), count(conn, index));
      List<String> found =
        topK(conn, table, "/*+ VECTOR_PROBE_COUNT(" + idx.getVectorIvfLists() + ") */",
          new float[] { 0, 0, 0, 0 }, rows.size() + 10);
      assertEquals("rows that a full probe search finds", new TreeSet<>(rows.keySet()),
        new TreeSet<>(found));
    }
  }

  /**
   * Verifies that a rebuild does not start if a metadata cache never expires. The rebuild then
   * leaves the index state, its generations, and its centroid rows unchanged.
   */
  @Test
  public void testRebuildRefusedIfCacheNeverExpires() throws Exception {
    String table = "T_NEVER_" + generateUniqueName();
    String index = "IDX_NEVER_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index, "FLOAT", "UPDATE_CACHE_FREQUENCY=NEVER");
      PTable before = index(conn, index);
      List<Long> generations = CentroidManager.listGenerations(conn, index);
      try {
        conn.createStatement().execute("ALTER INDEX " + index + " ON " + table + " REBUILD");
        fail("The rebuild did not refuse an index whose metadata cache never expires");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains(Outcome.CACHE_NEVER_EXPIRES.name()));
      }
      PTable after = index(conn, index);
      assertEquals(before.getVectorCentroidGeneration(), after.getVectorCentroidGeneration());
      assertFalse(after.isVectorRebuildInProgress());
      assertEquals(PIndexState.ACTIVE, after.getIndexState());
      assertEquals(generations, CentroidManager.listGenerations(conn, index));
    }
  }

  /**
   * Verifies that the cache lag includes the frequencies of all views of the data table. The views
   * are global views, tenant views, and views of views. A view of a different table does not count,
   * also if the name of that table starts with the name of the data table. An ALTER of a view and a
   * DROP of a view change the lag at once. A view whose header row keeps an old NEVER of its parent
   * counts with the current frequency of the parent.
   */
  @Test
  public void testCacheLagIncludesAllViews() throws Exception {
    String schema = "S_" + generateUniqueName();
    String table = schema + ".T_" + generateUniqueName();
    String index = "IDX_" + generateUniqueName();
    // The other table has a name with the data table name as a prefix.
    String other = table + "X";
    String view = schema + ".V_" + generateUniqueName();
    String viewOfView = schema + ".VV_" + generateUniqueName();
    String viewOfViewOfView = schema + ".VVV_" + generateUniqueName();
    String staleView = schema + ".SV_" + generateUniqueName();
    String tenantView = "TV_" + generateUniqueName();
    String tenantViewOfView = "TVV_" + generateUniqueName();
    Properties tenantProps = new Properties();
    tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "T_" + generateUniqueName());
    try (Connection conn = connect(false);
      Connection tenant = DriverManager.getConnection(getUrl(), tenantProps)) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + table + " (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL,"
          + " V VECTOR(FLOAT, 4) CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))"
          + " MULTI_TENANT=true, UPDATE_CACHE_FREQUENCY=1000");
        stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
        // A view of another table never expires. It must not count.
        stmt.execute("CREATE TABLE " + other + " (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL"
          + " CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true");
        stmt.execute("CREATE VIEW " + schema + ".OV_" + generateUniqueName() + " AS SELECT * FROM "
          + other + " UPDATE_CACHE_FREQUENCY=NEVER");
      }
      PTable dataTable = pconn.getTableNoCache(table);
      PTable idx = index(conn, schema + "." + index);
      assertEquals(1000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));

      conn.createStatement().execute(
        "CREATE VIEW " + view + " AS SELECT * FROM " + table + " UPDATE_CACHE_FREQUENCY=2000");
      assertEquals(2000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
      conn.createStatement().execute(
        "CREATE VIEW " + viewOfView + " AS SELECT * FROM " + view + " UPDATE_CACHE_FREQUENCY=4000");
      assertEquals(4000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
      tenant.createStatement().execute("CREATE VIEW " + tenantView + " AS SELECT * FROM " + table
        + " UPDATE_CACHE_FREQUENCY=8000");
      assertEquals(8000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
      tenant.createStatement().execute("CREATE VIEW " + tenantViewOfView + " AS SELECT * FROM "
        + tenantView + " UPDATE_CACHE_FREQUENCY=16000");
      assertEquals(16000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));

      conn.createStatement().execute("CREATE VIEW " + viewOfViewOfView + " AS SELECT * FROM "
        + viewOfView + " UPDATE_CACHE_FREQUENCY=32000");
      assertEquals(32000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
      // One parent and one header row in each batch give the same lag
      assertEquals(32000, VectorIndexRebuilderTestHooks.cacheLagMs(pconn, dataTable, idx, 1, 1));

      conn.createStatement()
        .execute("ALTER VIEW " + viewOfView + " SET UPDATE_CACHE_FREQUENCY=NEVER");
      assertEquals(Long.MAX_VALUE, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
      // A view without its own frequency keeps the NEVER of its parent in its header row. After
      // the parent becomes finite, the view resolves to the frequency of the parent.
      conn.createStatement()
        .execute("CREATE VIEW " + staleView + " AS SELECT * FROM " + viewOfView);
      assertEquals(Long.MAX_VALUE, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
      conn.createStatement()
        .execute("ALTER VIEW " + viewOfView + " SET UPDATE_CACHE_FREQUENCY=4000");
      assertEquals(32000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
      assertEquals(32000, VectorIndexRebuilderTestHooks.cacheLagMs(pconn, dataTable, idx, 1, 1));

      conn.createStatement().execute("DROP VIEW " + staleView);
      conn.createStatement().execute("DROP VIEW " + viewOfViewOfView);
      conn.createStatement().execute("DROP VIEW " + viewOfView);
      assertEquals(16000, VectorIndexRebuilder.cacheLagMs(pconn, dataTable, idx));
    }
  }

  /**
   * Verifies that IndexTool refuses a repair of a migrating index from the index (-fi). The repair
   * deletes each row under the outgoing generation. The rebuilt row of the building generation can
   * be in a different index region, which the repair region cannot write, so the row is lost.
   */
  @Test
  public void testIndexAsSourceRepairIsRefusedWhileMigrating() throws Exception {
    String table = "T_FI_" + generateUniqueName();
    String index = "IDX_FI_" + generateUniqueName();
    try (Connection conn = connect(false)) {
      setupTableAndKnownCentroids(conn, table, index);
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, "r" + i, near(i % 4, 0.01f * i), "l" + i);
      }
      conn.commit();
      // A migration whose rebuild stopped before it migrated any row
      PTable idx = index(conn, index);
      long building = CentroidManager.nextGeneration(idx.getVectorCentroidGeneration());
      CentroidManager.persistCentroids(conn, index, building, KNOWN_CENTROIDS,
        KNOWN_CENTROIDS.size());
      CentroidManager.setBuildingGeneration(conn.unwrap(PhoenixConnection.class), idx, building);
      // Split the index so that the rows of each generation are in a different region
      TableName physical = TableName.valueOf(idx.getPhysicalName().getBytes());
      try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
        admin.split(physical, PInteger.INSTANCE.toBytes(KNOWN_CENTROIDS.size()));
        getUtility().waitFor(60000, () -> admin.getRegions(physical).size() == 2);
      }
      IndexToolIT.runIndexTool(false, null, table, index, null, -1, IndexTool.IndexVerifyType.AFTER,
        "-fi");
      assertEquals(40, count(conn, index));
      // The refused repair closed its region scanners. No scanner holds a read point older than a
      // later write to the region of each generation.
      upsert(conn, table, "r40", near(0, 0.4f), "l40");
      conn.commit();
      for (HRegion region : getUtility().getHBaseCluster().getRegions(physical)) {
        assertEquals(region.getRegionInfo().getRegionNameAsString(),
          region.getMVCC().getReadPoint(), region.getSmallestReadPoint());
      }
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
