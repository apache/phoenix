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

import static org.apache.phoenix.end2end.IndexFsckToolIT.count;
import static org.apache.phoenix.end2end.IndexFsckToolIT.errors;
import static org.apache.phoenix.end2end.IndexFsckToolIT.finding;
import static org.apache.phoenix.end2end.IndexFsckToolIT.run;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.KNOWN_CENTROIDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InterruptedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.ScorecardAccumulator;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.fsck.Finding;
import org.apache.phoenix.mapreduce.index.fsck.GlobalIndexFsckProvider;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckContext;
import org.apache.phoenix.mapreduce.index.fsck.RepairAction;
import org.apache.phoenix.mapreduce.index.fsck.RepairPlan;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.RowKeyFormatter.KeyFormat;
import org.apache.phoenix.mapreduce.index.fsck.Severity;
import org.apache.phoenix.mapreduce.index.fsck.VerifyFindings;
import org.apache.phoenix.mapreduce.index.fsck.ivf.IvfIndexFsckProvider;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End-to-end tests of the verify, fsck, and repair commands of {@link IndexFsckTool} on IVF vector
 * indexes.
 */
@Category(ParallelStatsDisabledTest.class)
public class IvfIndexFsckIT extends ParallelStatsDisabledIT {

  /** Returns a test vector near the given known centroid, with an offset that {@code i} sets. */
  static float[] near(int centroid, int i) {
    float[] v = KNOWN_CENTROIDS.get(centroid).clone();
    for (int d = 0; d < v.length; d++) {
      v[d] = v[d] * (1 + 0.1f * i) + 0.01f * ((i + d) % 3);
    }
    return v;
  }

  static void upsert(Connection conn, String table, String id, float[] v) throws Exception {
    Float[] boxed = new Float[v.length];
    for (int i = 0; i < v.length; i++) {
      boxed[i] = v[i];
    }
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
      ps.setString(1, id);
      ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
      ps.setString(3, "label_" + id);
      ps.executeUpdate();
    }
    conn.commit();
  }

  /**
   * Makes a table and an IVF index whose active generation has the known centroids, and writes two
   * rows near each centroid.
   */
  static void knownCentroidIndex(Connection conn, String table, String index) throws Exception {
    VectorIndexTestUtil.setupTableAndKnownCentroids(conn, table, index);
    for (int c = 0; c < KNOWN_CENTROIDS.size(); c++) {
      for (int i = 0; i < 2; i++) {
        upsert(conn, table, "r" + c + "_" + i, near(c, i));
      }
    }
    // Flush the buffered scorecard updates, so that the baseline counts are deterministic
    ScorecardAccumulator.getInstance().flush();
  }

  /** Maps each data row ID to the centroid IDs of its index rows, from the index row keys. */
  static Map<String, Set<Integer>> centroidsById(PhoenixConnection pconn, String index)
    throws Exception {
    Map<String, Set<Integer>> byId = new HashMap<>();
    for (byte[] key : VectorIndexTestUtil.getHBaseRowKeys(pconn, pconn.getTableNoCache(index))) {
      String id =
        (String) PVarchar.INSTANCE.toObject(key, Bytes.SIZEOF_INT, key.length - Bytes.SIZEOF_INT);
      byId.computeIfAbsent(id, k -> new HashSet<>())
        .add(VectorIndexTestUtil.extractCentroidId(key));
    }
    return byId;
  }

  /**
   * Copies the index row of a data row to a different centroid ID to simulate corruption, and
   * deletes the source row if {@code deleteSource} is true. The test fails if there is no row.
   */
  static void copyIndexRow(PhoenixConnection pconn, String index, String id, int toCentroid,
    boolean deleteSource) throws Exception {
    PTable pindex = pconn.getTableNoCache(index);
    try (Table table = pconn.getQueryServices().getTable(pindex.getPhysicalName().getBytes());
      ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result result : scanner) {
        byte[] key = result.getRow();
        if (
          !id.equals(
            PVarchar.INSTANCE.toObject(key, Bytes.SIZEOF_INT, key.length - Bytes.SIZEOF_INT))
        ) {
          continue;
        }
        byte[] newKey = Bytes.add(PInteger.INSTANCE.toBytes(toCentroid),
          Bytes.copy(key, Bytes.SIZEOF_INT, key.length - Bytes.SIZEOF_INT));
        Put put = new Put(newKey);
        for (Cell cell : result.rawCells()) {
          put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
            cell.getTimestamp(), CellUtil.cloneValue(cell));
        }
        table.put(put);
        if (deleteSource) {
          table.delete(new Delete(key));
        }
        return;
      }
    }
    throw new AssertionError("No index row for " + id);
  }

  static List<String> rows(Connection conn, String sql) throws Exception {
    List<String> rows = new ArrayList<>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
          Object o = rs.getObject(i);
          sb.append(o instanceof byte[] ? Bytes.toStringBinary((byte[]) o) : String.valueOf(o))
            .append('|');
        }
        rows.add(sb.toString());
      }
    }
    return rows;
  }

  static boolean has(Report report, String rule) {
    return report.getFindings().stream().anyMatch(f -> f.getRule().equals(rule));
  }

  @Test
  public void testHealthyIndexes() throws Exception {
    String plain = generateUniqueName();
    String salted = generateUniqueName();
    String tenanted = generateUniqueName();
    String bson = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + plain
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      conn.createStatement().execute("CREATE TABLE " + salted
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR) SALT_BUCKETS=3");
      conn.createStatement()
        .execute("CREATE TABLE " + tenanted
          + " (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL, V VECTOR(FLOAT, 4), LABEL VARCHAR"
          + " CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true");
      conn.createStatement()
        .execute("CREATE TABLE " + bson + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      float[][] vectors = VectorIndexTestUtil.loadRandomVectors(conn, plain, null, null, 200, 1);
      VectorIndexTestUtil.loadRandomVectors(conn, salted, null, null, 200, 2);
      VectorIndexTestUtil.loadRandomVectors(conn, tenanted, "TENANT_ID",
        new String[] { "t1", "t2", "t3" }, 200, 3);
      try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + bson + " VALUES (?, ?)")) {
        for (int i = 0; i < vectors.length; i++) {
          ps.setString(1, "doc_" + i);
          ps.setObject(2,
            new BsonDocument("embedding", new BsonBinary(BinaryVector.floatVector(vectors[i]))));
          ps.executeUpdate();
        }
      }
      conn.commit();
      for (String table : new String[] { plain, salted, tenanted }) {
        conn.createStatement()
          .execute("CREATE VECTOR INDEX " + table + "_IDX ON " + table
            + " (V) INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4,"
            + " sample_size = 200)");
      }
      conn.createStatement()
        .execute("CREATE VECTOR INDEX " + bson + "_IDX ON " + bson
          + " (BSON_VECTOR_VALUE(doc, 'embedding', 4)) WITH (algorithm = 'IVF', metric = 'L2',"
          + " lists = 4, sample_size = 200)");
    }
    for (String table : new String[] { plain, salted, tenanted, bson }) {
      Report verify = run(0, "verify", "-dt", table, "-it", table + "_IDX");
      assertEquals(verify.toText(), 0, errors(verify));
      Report fsck = run(0, "fsck", "-dt", table, "-it", table + "_IDX");
      assertEquals(fsck.toText(), 0, errors(fsck) + fsck.getCount(Severity.WARN));
    }
  }

  @Test
  public void testRowDefectsVerifiedAndRepaired() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      // Write misassigned postings: one under an active centroid, one under an ID not in the model
      copyIndexRow(pconn, index, "r0_0", 1, false);
      copyIndexRow(pconn, index, "r1_0", 99, false);
      getUtility().getAdmin()
        .flush(TableName.valueOf(pconn.getTableNoCache(index).getPhysicalName().getBytes()));

      Report verify = run(1, "verify", "-dt", table, "-it", index);
      assertEquals(2, count(verify, VerifyFindings.ORPHAN_VERIFIED, "index"));
      assertEquals(1, errors(verify));
      Report fsck = run(1, "fsck", "-dt", table, "-it", index);
      assertEquals(Collections.singletonMap(99, 1L),
        finding(fsck, IvfIndexFsckProvider.POSTINGS_RETIRED_IDS, null).getDetails()
          .get("rowsByCentroidId"));

      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      assertEquals(2L, repair.getRepairPlan().get(GlobalIndexFsckProvider.DELETE_ORPHAN_ROWS)
        .getDetails().get("deletedOrphanRows"));
      Map<String, Set<Integer>> byId = centroidsById(pconn, index);
      assertEquals(Collections.singleton(0), byId.get("r0_0"));
      assertEquals(Collections.singleton(1), byId.get("r1_0"));
      assertEquals(8, byId.size());
      VectorIndexTestUtil.assertIndexVerifies(table, index, 8);
      assertFalse(
        has(run(0, "fsck", "-dt", table, "-it", index), IvfIndexFsckProvider.POSTINGS_RETIRED_IDS));
    }
  }

  @Test
  public void testMetadataFaultsDetectedAndRepaired() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    String dropped = "DROPPED_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pindex = pconn.getTableNoCache(index);
      long active = pindex.getVectorCentroidGeneration();
      long inactive = active - 1000;
      CentroidManager.persistCentroids(conn, index, inactive, KNOWN_CENTROIDS, 100);
      CentroidManager.persistCentroids(conn, dropped, 1L, KNOWN_CENTROIDS);
      try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO "
        + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME + " ("
        + PhoenixDatabaseMetaData.INDEX_NAME + ", " + PhoenixDatabaseMetaData.GENERATION_ID + ", "
        + PhoenixDatabaseMetaData.CENTROID_ID + ", " + PhoenixDatabaseMetaData.TRIGGER_REASON + ", "
        + PhoenixDatabaseMetaData.LAST_REBUILD_TIME + ") VALUES (?, 0, -1, 'dead', 1000)")) {
        ps.setString(1, index);
        ps.executeUpdate();
      }
      conn.commit();
      CentroidManager.deleteTasks(conn, pindex);
      CentroidManager.adjustScorecard(conn, index, active,
        Collections.singletonList(new ScorecardRow(0, 5, 0)));

      Report fsck = run(1, "fsck", "-dt", table, "-it", index);
      assertEquals(Severity.ERROR,
        finding(fsck, IvfIndexFsckProvider.RECONCILE_TASK_MISSING, null).getSeverity());
      assertEquals(inactive, finding(fsck, IvfIndexFsckProvider.GENERATION_INACTIVE, null)
        .getDetails().get("generation"));
      assertTrue(fsck.getFindings().stream()
        .anyMatch(f -> f.getRule().equals(IvfIndexFsckProvider.ORPHAN_INDEX_STATE)
          && dropped.equals(f.getDetails().get("index"))));
      finding(fsck, IvfIndexFsckProvider.CLAIM_EXPIRED, null);
      assertEquals(Severity.INFO,
        finding(fsck, IvfIndexFsckProvider.SCORECARD_DIVERGED, null).getSeverity());

      // Record the system table rows of the indexes of this test only
      String centroidRows = "SELECT * FROM " + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME
        + " WHERE " + PhoenixDatabaseMetaData.INDEX_NAME + " IN ('" + index + "', '" + dropped
        + "') ORDER BY " + PhoenixDatabaseMetaData.INDEX_NAME + ", "
        + PhoenixDatabaseMetaData.GENERATION_ID + ", " + PhoenixDatabaseMetaData.CENTROID_ID;
      String taskRows = "SELECT * FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME + " WHERE "
        + PhoenixDatabaseMetaData.TABLE_NAME + " = '" + index + "'";
      List<String> centroidsBefore = rows(conn, centroidRows);
      List<String> tasksBefore = rows(conn, taskRows);
      Report dryRun = run(1, "repair", "-dt", table, "-it", index);
      for (String action : Arrays.asList(IvfIndexFsckProvider.PURGE_GENERATION,
        IvfIndexFsckProvider.DELETE_ORPHAN_STATE, IvfIndexFsckProvider.ENQUEUE_RECONCILE_TASK,
        IvfIndexFsckProvider.RECONCILE_SCORECARD)) {
        assertEquals(action, RepairAction.Status.PLANNED,
          dryRun.getRepairPlan().get(action).getStatus());
      }
      assertEquals(centroidsBefore, rows(conn, centroidRows));
      assertEquals(tasksBefore, rows(conn, taskRows));

      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      RepairAction purge = repair.getRepairPlan().get(IvfIndexFsckProvider.PURGE_GENERATION);
      assertEquals(RepairAction.Status.EXECUTED, purge.getStatus());
      assertNotNull(purge.getDetails().get("removed"));
      assertFalse(CentroidManager.listGenerations(conn, index).contains(inactive));
      assertTrue(CentroidManager.listGenerations(conn, dropped).isEmpty());
      assertEquals(Collections.emptyList(),
        rows(conn,
          "SELECT * FROM " + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME + " WHERE "
            + PhoenixDatabaseMetaData.INDEX_NAME + " = '" + index + "' AND "
            + PhoenixDatabaseMetaData.GENERATION_ID + " = 0"));
      for (ScorecardRow row : CentroidManager.loadScorecard(conn, index, active)) {
        assertEquals(2, row.getClusterSize());
      }
      Report after = run(0, "fsck", "-dt", table, "-it", index);
      for (String rule : Arrays.asList(IvfIndexFsckProvider.GENERATION_INACTIVE,
        IvfIndexFsckProvider.CLAIM_EXPIRED, IvfIndexFsckProvider.RECONCILE_TASK_MISSING,
        IvfIndexFsckProvider.SCORECARD_DIVERGED)) {
        assertFalse(rule, has(after, rule));
      }
      assertFalse(after.getFindings().stream()
        .anyMatch(f -> f.getRule().equals(IvfIndexFsckProvider.ORPHAN_INDEX_STATE)
          && dropped.equals(f.getDetails().get("index"))));
      assertEquals(1,
        org.apache.phoenix.schema.task.Task
          .queryTaskTable(conn, null, "", index, TaskType.VECTOR_SCORECARD_RECONCILE, null, null)
          .size());
    }
  }

  /**
   * Verifies that repair deletes only generations older than the active one. A newer generation can
   * belong to a rebuild that the catalog does not record yet. An untrained index can have a first
   * generation that is not active yet.
   */
  @Test
  public void testRepairPurgesOnlyGenerationsOlderThanActive() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    String untrainedTable = generateUniqueName();
    String untrained = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      long active =
        conn.unwrap(PhoenixConnection.class).getTableNoCache(index).getVectorCentroidGeneration();
      long older = active - 1000;
      long newer = active + 1000;
      CentroidManager.persistCentroids(conn, index, older, KNOWN_CENTROIDS, 100);
      CentroidManager.persistCentroids(conn, index, newer, KNOWN_CENTROIDS, 200);
      conn.createStatement().execute("CREATE TABLE " + untrainedTable
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX " + untrained + " ON " + untrainedTable
        + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      CentroidManager.persistCentroids(conn, untrained, 1L, KNOWN_CENTROIDS);

      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      List<Long> generations = CentroidManager.listGenerations(conn, index);
      assertFalse(generations.contains(older));
      assertTrue(generations.contains(newer));
      assertTrue(repair.getRepairPlan().getActions().stream()
        .anyMatch(a -> a.getAction().equals(IvfIndexFsckProvider.PURGE_GENERATION)
          && a.getDescription().endsWith(" " + newer)
          && a.getStatus() == RepairAction.Status.SKIPPED));

      run(0, "repair", "-dt", untrainedTable, "-it", untrained, "--confirm");
      assertEquals(Collections.singletonList(1L), CentroidManager.listGenerations(conn, untrained));
    }
  }

  /**
   * Verifies that repair does not fix defects of the centroid model. Repair reports them as errors
   * and stops after a round that does not decrease the error count.
   */
  @Test
  public void testModelDefectsReportedNotRepaired() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      long active =
        conn.unwrap(PhoenixConnection.class).getTableNoCache(index).getVectorCentroidGeneration();
      CentroidManager.persistCentroids(conn, index, active,
        Collections.singletonList(new float[] { 1, 0, 0 }), 2);
      // The vector type rejects non-finite elements, so write the corrupt centroid as raw bytes.
      float[] nonFinite = { 0, Float.NaN, 0, 0 };
      byte[] encoded = new byte[nonFinite.length * Bytes.SIZEOF_FLOAT];
      PVectorFloat.writeElements(nonFinite, encoded, 0);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME
          + " (" + PhoenixDatabaseMetaData.INDEX_NAME + ", " + PhoenixDatabaseMetaData.GENERATION_ID
          + ", " + PhoenixDatabaseMetaData.CENTROID_ID + ", "
          + PhoenixDatabaseMetaData.CENTROID_VECTOR + ") VALUES (?, ?, 3, ?)")) {
        ps.setString(1, index);
        ps.setLong(2, active);
        ps.setBytes(3, encoded);
        ps.executeUpdate();
      }
      conn.commit();
      Report fsck = run(1, "fsck", "-dt", table, "-it", index);
      Finding invalid = finding(fsck, IvfIndexFsckProvider.MODEL_INVALID, null);
      assertEquals(Arrays.asList("centroid 2 has dimension 3", "centroid 3 is not finite"),
        invalid.getDetails().get("problems"));
      Report repair = run(1, "repair", "-dt", table, "-it", index, "--confirm");
      finding(repair, IvfIndexFsckProvider.MODEL_INVALID, null);
      assertEquals(2, repair.getRepairPlan().getActions().stream()
        .filter(a -> a.getAction().equals(GlobalIndexFsckProvider.REBUILD_INDEX_ROWS)).count());
    }
  }

  @Test
  public void testMigrationAndClaimRefusals() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      try (PhoenixConnection internal = CentroidManager.newInternalConnection(pconn)) {
        assertTrue(CentroidManager.claimRebuild(internal, index, "other", 60000));
        Report refused = run(1, "repair", "-dt", table, "-it", index, "--confirm");
        finding(refused, IvfIndexFsckProvider.REPAIR_REFUSED, null);
        CentroidManager.releaseRebuild(internal, index, "other");

        PTable pindex = internal.getTableNoCache(index);
        long building = CentroidManager.nextGeneration(pindex.getVectorCentroidGeneration());
        CentroidManager.persistCentroids(internal, index, building, KNOWN_CENTROIDS,
          KNOWN_CENTROIDS.size());
        CentroidManager.setBuildingGeneration(internal, pindex, building);
      }
      Report verify = run(1, "verify", "-dt", table, "-it", index);
      assertEquals(Severity.ERROR,
        finding(verify, GlobalIndexFsckProvider.ROWS_NOT_VERIFIED, null).getSeverity());
      Report fsck = run(0, "fsck", "-dt", table, "-it", index);
      assertEquals(Severity.WARN,
        finding(fsck, IvfIndexFsckProvider.MIGRATION_STALLED, null).getSeverity());
      assertFalse(has(fsck, IvfIndexFsckProvider.MODEL_MISSING));
      assertFalse(has(fsck, IvfIndexFsckProvider.GENERATION_ID_OVERLAP));
    }
  }

  /**
   * Verifies that an interrupted repair releases its rebuild claim, keeps the interrupt status of
   * the thread, and throws the interrupt without suppressed exceptions.
   */
  @Test
  public void testInterruptedRepairReleasesClaim() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexFsckContext context = new IndexFsckContext(conn, config, pconn.getTableNoCache(table),
        pconn.getTableNoCache(index), null, null, null, KeyFormat.HEX, true);
      InterruptedIOException interrupt = new InterruptedIOException("interrupted");
      IvfIndexFsckProvider provider = new IvfIndexFsckProvider() {
        @Override
        protected List<Finding> repairRound(IndexFsckContext ctx, RepairPlan plan, int round)
          throws Exception {
          Thread.currentThread().interrupt();
          throw interrupt;
        }
      };
      try {
        provider.repair(context);
        fail("Expected the interrupt");
      } catch (InterruptedIOException e) {
        assertSame(interrupt, e);
        assertEquals(0, e.getSuppressed().length);
      } finally {
        assertTrue("The interrupt status is kept", Thread.interrupted());
      }
      try (PhoenixConnection internal = CentroidManager.newInternalConnection(pconn)) {
        assertTrue("The claim was released",
          CentroidManager.claimRebuild(internal, index, "other", 60000));
        CentroidManager.releaseRebuild(internal, index, "other");
      }
    }
  }
}
