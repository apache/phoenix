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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration tests for HNSW vector index population, incremental maintenance, and durability. */
@Category(ParallelStatsDisabledTest.class)
public class HnswIndexIT extends ParallelStatsDisabledIT {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIM = 16;
  // The replay margin a test that waits it out shortens the default to
  private static final long REPLAY_MARGIN_MS = 8_000;

  @After
  public void restoreReplayMargin() {
    HnswIndexManager.setReplayMarginMs(HnswIndexManager.REPLAY_MARGIN_MS);
  }

  static float[] vector(Random random) {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = (float) random.nextGaussian();
    }
    return v;
  }

  static void upsert(Connection conn, String table, String id, float[] v) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
      ps.setString(1, id);
      Float[] boxed = new Float[v.length];
      for (int i = 0; i < v.length; i++) {
        boxed[i] = v[i];
      }
      ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
      ps.executeUpdate();
    }
    conn.commit();
  }

  static void delete(Connection conn, String table, String id) throws SQLException {
    conn.createStatement().execute("DELETE FROM " + table + " WHERE ID = '" + id + "'");
    conn.commit();
  }

  /** Helper to create a pre-split table and build an initial HNSW index over sample data. */
  static Map<String, float[]> createAndBuild(Connection conn, String table, String index, int count)
    throws Exception {
    return createAndBuild(conn, table, index, count, " SPLIT ON ('m')");
  }

  static Map<String, float[]> createAndBuild(Connection conn, String table, String index, int count,
    String split) throws Exception {
    conn.createStatement().execute("CREATE TABLE " + table
      + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + "))" + split);
    Map<String, float[]> rows = new HashMap<>();
    Random random = new Random(42);
    for (int i = 0; i < count; i++) {
      String id = (i % 2 == 0 ? "a" : "z") + i;
      float[] v = vector(random);
      upsert(conn, table, id, v);
      rows.put(id, v);
    }
    conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
      + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
    buildIndex(table, index);
    return rows;
  }

  /** Executes IndexTool to build HNSW segments across regions. */
  static void buildIndex(String table, String index) throws Exception {
    IndexTool tool = new IndexTool();
    tool.setConf(new Configuration(getUtility().getConfiguration()));
    assertEquals(0, tool.run(IndexToolIT.getArgValues(false, null, table, index, null,
      IndexTool.IndexVerifyType.NONE, IndexTool.IndexDisableLoggingType.NONE)));
  }

  static List<HRegion> regions(String table) {
    List<HRegion> regions = getUtility().getHBaseCluster().getRegions(TableName.valueOf(table));
    regions.sort(Comparator.comparing(r -> r.getRegionInfo().getStartKey(), Bytes::compareTo));
    return regions;
  }

  static HnswIndexManager manager(HRegion region, String index) throws IOException {
    IndexRegionObserver observer =
      region.getCoprocessorHost().findCoprocessor(IndexRegionObserver.class);
    return (HnswIndexManager) observer.getVectorIndexManager(index);
  }

  /** Executes nearest neighbor search against the region containing the specified row. */
  private static List<String> search(String table, String index, String id, float[] query, int k)
    throws Exception {
    List<String> found = new ArrayList<>();
    for (HRegion region : regions(table)) {
      if (region.getRegionInfo().containsRow(Bytes.toBytes(id))) {
        for (byte[] key : manager(region, index).search(query, k, 64)) {
          found.add(Bytes.toString(key));
        }
      }
    }
    return found;
  }

  private static String nearest(String table, String index, String id, float[] query)
    throws Exception {
    List<String> found = search(table, index, id, query, 1);
    return found.isEmpty() ? null : found.get(0);
  }

  /** Asserts that outdated vector representations are excluded from search results. */
  private static void assertNotFound(String table, String index, String id, float[] stale)
    throws Exception {
    List<String> found = search(table, index, id, stale, 3);
    assertFalse(id + " found by its stale vector: " + found, found.contains(id));
  }

  /** Waits for all table regions to complete segment construction after the specified timestamp. */
  private static void awaitRebuilt(Connection conn, String table, String index, long since)
    throws Exception {
    for (HRegion region : regions(table)) {
      awaitSegment(conn, index, region, since, 60);
    }
  }

  /** Waits for a region to complete segment construction and returns the descriptor. */
  private static HnswSegment.Descriptor awaitSegment(Connection conn, String index, HRegion region,
    long since, int seconds) throws Exception {
    PTable indexTable = PhoenixRuntime.getTableNoCache(conn, index);
    try (Table t = conn.unwrap(PhoenixConnection.class).getQueryServices()
      .getTable(indexTable.getPhysicalName().getBytes())) {
      for (int i = 0; i < seconds * 10; i++) {
        for (HnswSegment.Descriptor d : HnswSegment.list(t,
          QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES)) {
          if (
            d.time >= since
              && d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())
          ) {
            return d;
          }
        }
        Thread.sleep(100);
      }
    }
    throw new AssertionError(
      "segment of " + region.getRegionInfo().getEncodedName() + " not rebuilt");
  }

  /** Waits for a delta segment to be written for the specified region. */
  static HnswSegment.Descriptor awaitDelta(Connection conn, String index, HRegion region,
    long since, int seconds) throws Exception {
    PTable indexTable = PhoenixRuntime.getTableNoCache(conn, index);
    try (Table t = conn.unwrap(PhoenixConnection.class).getQueryServices()
      .getTable(indexTable.getPhysicalName().getBytes())) {
      for (int i = 0; i < seconds * 10; i++) {
        for (HnswSegment.Descriptor d : HnswSegment.list(t,
          QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES)) {
          if (
            d.time >= since && d.isDelta()
              && d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())
          ) {
            return d;
          }
        }
        Thread.sleep(100);
      }
    }
    throw new AssertionError(
      "delta segment of " + region.getRegionInfo().getEncodedName() + " not written");
  }

  static List<HnswSegment.Descriptor> segments(Connection conn, String index) throws Exception {
    PTable indexTable = PhoenixRuntime.getTableNoCache(conn, index);
    try (Table t = conn.unwrap(PhoenixConnection.class).getQueryServices()
      .getTable(indexTable.getPhysicalName().getBytes())) {
      return HnswSegment.list(t, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
    }
  }

  /** Waits for superseded segment rows to be retired until only {@code count} remain. */
  private static List<HnswSegment.Descriptor> awaitSegments(Connection conn, String index,
    int count) throws Exception {
    List<HnswSegment.Descriptor> segments = segments(conn, index);
    for (int i = 0; i < 300 && segments.size() != count; i++) {
      Thread.sleep(100);
      segments = segments(conn, index);
    }
    assertEquals("superseded segments should be retired", count, segments.size());
    return segments;
  }

  /** Waits until the table reaches the expected count of online regions. */
  private static void awaitRegions(String table, int count) throws Exception {
    for (int i = 0; i < 600 && regions(table).size() != count; i++) {
      Thread.sleep(100);
    }
    assertEquals(count, regions(table).size());
    getUtility().waitTableAvailable(TableName.valueOf(table));
  }

  static void rebuild(Connection conn, String table, String index) throws Exception {
    long since = EnvironmentEdgeManager.currentTimeMillis();
    for (HRegion region : regions(table)) {
      manager(region, index).rebuild();
    }
    awaitRebuilt(conn, table, index, since);
  }

  /** Closes and reopens every region of the table. */
  static void reopen(String table) throws Exception {
    try (Admin admin = getUtility().getAdmin()) {
      for (HRegion region : regions(table)) {
        byte[] name = region.getRegionInfo().getRegionName();
        admin.unassign(name, true);
        admin.assign(name);
      }
    }
    getUtility().waitTableAvailable(TableName.valueOf(table));
  }

  private static double recall(String table, String index, Map<String, float[]> rows)
    throws Exception {
    Random random = new Random(7);
    int k = 10;
    int hits = 0;
    int queries = 20;
    for (int q = 0; q < queries; q++) {
      float[] query = vector(random);
      Set<String> expected = rows.entrySet().stream()
        .sorted(Comparator.comparingDouble(e -> -cosine(query, e.getValue()))).limit(k)
        .map(Map.Entry::getKey).collect(Collectors.toSet());
      List<String> found = new ArrayList<>();
      for (HRegion region : regions(table)) {
        for (byte[] key : manager(region, index).search(query, k, 64)) {
          found.add(Bytes.toString(key));
        }
      }
      found.sort(Comparator.comparingDouble(id -> -cosine(query, rows.get(id))));
      for (String id : found.subList(0, Math.min(k, found.size()))) {
        hits += expected.contains(id) ? 1 : 0;
      }
    }
    return hits / (double) (queries * k);
  }

  // Invert vector values to simulate significant updates
  private static float[] negate(float[] v) {
    float[] n = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      n[i] = -v[i];
    }
    return n;
  }

  static double cosine(float[] a, float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }

  private static long count(Connection conn, String sql, String name) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getLong(1);
      }
    }
  }

  /** Tests coexistence and independent lifecycle of IVF and HNSW vector indexes. */
  @Test
  public void testIvfAndHnswOnSameTable() throws Exception {
    String table = generateUniqueName();
    String ivf = generateUniqueName();
    String hnsw = generateUniqueName();
    String centroids = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + PhoenixDatabaseMetaData.INDEX_NAME + " = ?";
    String tasks = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME + " WHERE "
      + PhoenixDatabaseMetaData.TABLE_NAME + " = ?";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + ")) SPLIT ON ('m')");
      Random random = new Random(5);
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, (i % 2 == 0 ? "a" : "z") + i, vector(random));
      }
      conn.createStatement().execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='COSINE', lists=2, sample_size=40)");
      conn.createStatement().execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      float[] early = vector(random);
      upsert(conn, table, "a-early", early);

      buildIndex(table, hnsw);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertEquals("a-early", nearest(table, hnsw, "a-early", early));
      assertEquals(41, VectorIndexTestUtil.getHBaseRowKeys(conn.unwrap(PhoenixConnection.class),
        PhoenixRuntime.getTableNoCache(conn, ivf)).size());
      assertTrue(count(conn, centroids, ivf) > 0);
      assertEquals(0, count(conn, centroids, hnsw));
      assertTrue(count(conn, tasks, ivf) > 0);
      assertEquals(0, count(conn, tasks, hnsw));

      conn.createStatement().execute("ALTER INDEX " + hnsw + " ON " + table + " REBUILD");
      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertEquals(0, count(conn, tasks, hnsw));
      float[] late = vector(random);
      upsert(conn, table, "z-late", late);
      assertEquals("z-late", nearest(table, hnsw, "z-late", late));
      buildIndex(table, hnsw);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertEquals("a-early", nearest(table, hnsw, "a-early", early));
    }
  }

  /** Tests building an HNSW index via IndexTool across multiple regions. */
  @Test
  public void testIndexToolBuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, index).getIndexState());
      for (HRegion region : regions(table)) {
        for (byte[] key : manager(region, index).search(rows.get("a0"), 50, 64)) {
          assertTrue("search returned a row outside the region",
            region.getRegionInfo().containsRow(key));
        }
      }
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /** Tests that incremental flushes write delta segments without scanning base table rows. */
  @Test
  public void testFlushWritesDelta() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);

      List<HnswSegment.Descriptor> initialSegments = segments(conn, index);
      for (HnswSegment.Descriptor d : initialSegments) {
        assertFalse(d.isDelta());
        assertNull(d.baseTime);
      }

      float[] oldA0 = rows.get("a0");
      float[] newA0 = negate(oldA0);
      upsert(conn, table, "a0", newA0);

      float[] oldA2 = rows.get("a2");
      float[] newA2 = negate(oldA2);
      upsert(conn, table, "a2", newA2);

      // Insert a row directly via HBase to verify delta flushes do not read base table rows
      Result existing = region.get(new Get(Bytes.toBytes("a4")));
      byte[] rawKey = Bytes.toBytes("a-raw");
      Put rawPut = new Put(rawKey);
      for (Cell cell : existing.rawCells()) {
        rawPut.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
          cell.getTimestamp(), CellUtil.cloneValue(cell));
      }
      region.put(rawPut);

      long since = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta = awaitDelta(conn, index, region, since, 60);
      assertTrue(delta.isDelta());
      assertNotNull(delta.baseTime);

      assertEquals("a0", nearest(table, index, "a0", newA0));
      assertNotFound(table, index, "a0", oldA0);
      assertEquals("a2", nearest(table, index, "a2", newA2));
      assertNotFound(table, index, "a2", oldA2);

      // Unindexed direct HBase row should not be found until a full rebuild
      List<String> found = search(table, index, "a-raw", rows.get("a4"), 10);
      assertFalse("raw row should be absent after delta flush: " + found, found.contains("a-raw"));

      long rebuildSince = EnvironmentEdgeManager.currentTimeMillis();
      mgr.rebuild();
      awaitSegment(conn, index, region, rebuildSince, 60);
      assertEquals("a-raw", nearest(table, index, "a-raw", rows.get("a4")));
    }
  }

  /** Tests that stacked delta segments and tombstones are properly restored when regions reopen. */
  @Test
  public void testDeltaStackOnReopen() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);

      float[] initialA0 = rows.get("a0");
      float[] initialA2 = rows.get("a2");
      float[] initialA4 = rows.get("a4");
      float[] initialA6 = rows.get("a6");

      float[] delta1A0 = negate(initialA0);
      upsert(conn, table, "a0", delta1A0);
      delete(conn, table, "a2");
      float[] delta1A4 = negate(initialA4);
      upsert(conn, table, "a4", delta1A4);
      float[] initialA10 = rows.get("a10");
      float[] delta1A10 = negate(initialA10);
      upsert(conn, table, "a10", delta1A10);

      long sinceDelta1 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta1 = awaitDelta(conn, index, region, sinceDelta1, 60);
      assertTrue(delta1.isDelta());

      // Sleep past a shortened replay lookback window so masking relies on persisted deltas
      HnswIndexManager.setReplayMarginMs(REPLAY_MARGIN_MS);
      Thread.sleep(REPLAY_MARGIN_MS + 2000);

      float[] delta2A0 = vector(new Random(99));
      upsert(conn, table, "a0", delta2A0);
      delete(conn, table, "a4");
      float[] delta2A6 = negate(initialA6);
      upsert(conn, table, "a6", delta2A6);

      long sinceDelta2 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta2 = awaitDelta(conn, index, region, sinceDelta2, 60);
      assertTrue(delta2.isDelta());

      assertEquals("a0", nearest(table, index, "a0", delta2A0));
      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a0", delta1A0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);
      assertNotFound(table, index, "a4", delta1A4);
      assertEquals("a6", nearest(table, index, "a6", delta2A6));
      assertNotFound(table, index, "a6", initialA6);
      assertEquals("a8", nearest(table, index, "a8", rows.get("a8")));

      List<HnswSegment.Descriptor> stack = new ArrayList<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        if (d.overlaps(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())) {
          stack.add(d);
        }
      }
      assertEquals("base and two deltas", 3, stack.size());

      // Reopen table to reload segments and stack masks from disk
      reopen(table);

      assertEquals("a10", nearest(table, index, "a10", delta1A10));
      assertNotFound(table, index, "a10", initialA10);
      assertEquals("a0", nearest(table, index, "a0", delta2A0));
      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a0", delta1A0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);
      assertNotFound(table, index, "a4", delta1A4);
      assertEquals("a6", nearest(table, index, "a6", delta2A6));
      assertNotFound(table, index, "a6", initialA6);
      assertEquals("a8", nearest(table, index, "a8", rows.get("a8")));

      // Verify that no rebuild was triggered
      Thread.sleep(5000);
      List<HnswSegment.Descriptor> after = new ArrayList<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        if (d.overlaps(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())) {
          after.add(d);
        }
      }
      assertEquals(stack.size(), after.size());
      for (int i = 0; i < stack.size(); i++) {
        assertTrue(Bytes.equals(stack.get(i).rowKey, after.get(i).rowKey));
      }
    }
  }

  /**
   * Tests that full rebuilds are triggered when delta count or mutation ratio thresholds are
   * exceeded.
   */
  @Test
  public void testRatioTriggersFullRebuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200, "");
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);

      // Exceeding the rebuild ratio threshold triggers a full rebuild
      for (int i = 0; i < 20; i++) {
        String id = "a" + (2 * i);
        float[] v = negate(rows.get(id));
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long since1 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor d1 = awaitDelta(conn, index, region, since1, 60);
      assertTrue(d1.isDelta());

      List<HnswSegment.Descriptor> stackAfterD1 = segments(conn, index);
      assertEquals(2, stackAfterD1.size());

      for (int i = 20; i < 55; i++) {
        String id = "a" + (2 * i);
        float[] v = negate(rows.get(id));
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long since2 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      awaitSegment(conn, index, region, since2, 60);
      List<HnswSegment.Descriptor> stackAfterRatio = awaitSegments(conn, index, 1);
      assertEquals(1, stackAfterRatio.size());
      assertFalse("must be a base segment", stackAfterRatio.get(0).isDelta());

      // Exceeding MAX_DELTAS triggers a full rebuild
      for (int deltaIdx = 1; deltaIdx <= 4; deltaIdx++) {
        String id = "a" + (2 * (60 + deltaIdx));
        float[] v = negate(rows.get(id));
        upsert(conn, table, id, v);
        rows.put(id, v);

        long sinceDelta = EnvironmentEdgeManager.currentTimeMillis();
        mgr.flush();
        HnswSegment.Descriptor d = awaitDelta(conn, index, region, sinceDelta, 60);
        assertTrue(d.isDelta());
      }

      List<HnswSegment.Descriptor> stackWith4Deltas = segments(conn, index);
      assertEquals(5, stackWith4Deltas.size());

      String id = "a" + (2 * 70);
      float[] v = negate(rows.get(id));
      upsert(conn, table, id, v);
      rows.put(id, v);

      long sinceMaxDeltas = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      awaitSegment(conn, index, region, sinceMaxDeltas, 60);
      List<HnswSegment.Descriptor> stackAfterMaxDeltas = awaitSegments(conn, index, 1);
      assertEquals(1, stackAfterMaxDeltas.size());
      assertFalse("must be a base segment", stackAfterMaxDeltas.get(0).isDelta());

      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /** Tests that a flush containing only deletions produces a tombstone-only delta segment. */
  @Test
  public void testDeleteOnlyDelta() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200, "");
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);

      float[] initialA0 = rows.get("a0");
      float[] initialA2 = rows.get("a2");
      float[] initialA4 = rows.get("a4");

      delete(conn, table, "a0");
      delete(conn, table, "a2");
      delete(conn, table, "a4");

      long since = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta = awaitDelta(conn, index, region, since, 60);
      assertTrue(delta.isDelta());

      PTable indexTable = PhoenixRuntime.getTableNoCache(conn, index);
      try (Table t = conn.unwrap(PhoenixConnection.class).getQueryServices()
        .getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = t.get(new Get(delta.rowKey));
        byte[] fam = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
        assertNull("delete-only delta should have no payload cell",
          r.getValue(fam, HnswSegment.PAYLOAD_QUALIFIER));
        assertNotNull("delete-only delta must have tombstones cell",
          r.getValue(fam, HnswSegment.TOMBSTONES_QUALIFIER));
        assertNotNull("delta must have base time cell",
          r.getValue(fam, HnswSegment.BASE_TIME_QUALIFIER));
      }

      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);

      reopen(table);

      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);

      assertEquals("a6", nearest(table, index, "a6", rows.get("a6")));
    }
  }

  /**
   * Committed upserts, updates, and deletes are searchable before any rebuild, and a rebuild keeps
   * them.
   */
  @Test
  public void testRowChangesBeforeAndAfterRebuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      Random random = new Random(99);
      float[] added = vector(random);
      float[] old = rows.get("z1");
      float[] moved = negate(old);
      upsert(conn, table, "a-new", added);
      upsert(conn, table, "z1", moved);
      delete(conn, table, "a2");

      assertEquals("a-new", nearest(table, index, "a-new", added));
      assertEquals("z1", nearest(table, index, "z1", moved));
      assertNotFound(table, index, "z1", old);
      assertNotFound(table, index, "a2", rows.get("a2"));

      rebuild(conn, table, index);
      assertEquals("a-new", nearest(table, index, "a-new", added));
      assertEquals("z1", nearest(table, index, "z1", moved));
      assertNotFound(table, index, "a2", rows.get("a2"));
    }
  }

  /** Tests that mutations during a segment rebuild are preserved and masked. */
  @Test
  public void testChangesDuringRebuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      Random random = new Random(5);
      Map<String, float[]> changed = new HashMap<>();
      for (int i = 0; i < 10; i++) {
        changed.put("a" + (2 * i), negate(rows.get("a" + (2 * i))));
      }
      long since = EnvironmentEdgeManager.currentTimeMillis();
      for (HRegion region : regions(table)) {
        manager(region, index).rebuild();
      }
      for (Map.Entry<String, float[]> e : changed.entrySet()) {
        upsert(conn, table, e.getKey(), e.getValue());
      }
      delete(conn, table, "z3");
      awaitRebuilt(conn, table, index, since);
      for (Map.Entry<String, float[]> e : changed.entrySet()) {
        assertEquals(e.getKey(), nearest(table, index, e.getKey(), e.getValue()));
        assertNotFound(table, index, e.getKey(), rows.get(e.getKey()));
      }
      assertNotFound(table, index, "z3", rows.get("z3"));
    }
  }

  /** Tests replay and recovery of unflushed mutations when regions are reopened. */
  @Test
  public void testReplayOnReopen() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      Random random = new Random(11);
      float[] added = vector(random);
      float[] moved = negate(rows.get("a4"));
      upsert(conn, table, "z-new", added);
      upsert(conn, table, "a4", moved);
      delete(conn, table, "z5");
      reopen(table);
      assertEquals("z-new", nearest(table, index, "z-new", added));
      assertEquals("a4", nearest(table, index, "a4", moved));
      assertNotFound(table, index, "z5", rows.get("z5"));
    }
  }

  /** Tests that major compaction delete marker removal is reflected in rebuilt segments. */
  @Test
  public void testRebuildDropsCompactedDeletes() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      delete(conn, table, "a6");
      try (Admin admin = getUtility().getAdmin()) {
        admin.flush(TableName.valueOf(table));
        admin.majorCompact(TableName.valueOf(table));
      }
      reopen(table);
      rebuild(conn, table, index);
      assertNotFound(table, index, "a6", rows.get("a6"));
    }
  }

  /** Tests that deletion workloads trigger segment flushes at threshold. */
  @Test
  public void testDeletesTriggerFlush() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + ")) SPLIT ON ('m')");
      conn.setAutoCommit(false);
      Random random = new Random(13);
      int count = HnswIndexManager.FLUSH_THRESHOLD;
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        for (int i = 0; i < count; i++) {
          float[] v = vector(random);
          Float[] boxed = new Float[v.length];
          for (int j = 0; j < v.length; j++) {
            boxed[j] = v[j];
          }
          ps.setString(1, "a" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
          if (i % 1000 == 999) {
            conn.commit();
          }
        }
      }
      conn.commit();
      upsert(conn, table, "z0", vector(random));
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      buildIndex(table, index);
      long since = EnvironmentEdgeManager.currentTimeMillis();
      conn.createStatement().execute("DELETE FROM " + table + " WHERE ID < 'm'");
      conn.commit();
      HnswSegment.Descriptor d = awaitSegment(conn, index, regions(table).get(0), since, 60);
      assertEquals(0, d.count);
    }
  }

  /** Tests retry behavior when segment rebuild operations encounter transient failures. */
  @Test
  public void testFailedRebuildIsRetried() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      float[] moved = negate(rows.get("a2"));
      upsert(conn, table, "a2", moved);
      TableName indexTable =
        TableName.valueOf(PhoenixRuntime.getTableNoCache(conn, index).getPhysicalName().getBytes());
      HRegion region = regions(table).get(0);
      long since = EnvironmentEdgeManager.currentTimeMillis();
      try (Admin admin = getUtility().getAdmin()) {
        admin.disableTable(indexTable);
        manager(region, index).rebuild();
        Thread.sleep(5000);
        admin.enableTable(indexTable);
      }
      awaitSegment(conn, index, region, since, (int) (HnswIndexManager.RETRY_DELAY_MS / 1000) + 60);
      assertEquals("a2", nearest(table, index, "a2", moved));
      assertNotFound(table, index, "a2", rows.get("a2"));
    }
  }

  /** Tests graph reachability and insertion after deleting all buffered entries. */
  @Test
  public void testInsertAfterDeletingEveryNewRow() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 20);
      Random random = new Random(3);
      upsert(conn, table, "a-x", vector(random));
      delete(conn, table, "a-x");
      float[] v = vector(random);
      upsert(conn, table, "a-y", v);
      assertEquals("a-y", nearest(table, index, "a-y", v));
    }
  }

  /** Tests HNSW indexing and maintenance over functional expressions on BSON documents. */
  @Test
  public void testBsonFunctionalIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      Random random = new Random(21);
      Map<String, float[]> rows = new HashMap<>();
      for (int i = 0; i < 50; i++) {
        rows.put("a" + i, vector(random));
        upsertDoc(conn, table, "a" + i, rows.get("a" + i));
      }
      conn.createStatement().execute(
        "CREATE VECTOR INDEX " + index + " ON " + table + " (BSON_VECTOR_VALUE(DOC, 'embedding', "
          + DIM + ")) WITH (algorithm='HNSW', " + "metric='COSINE') ASYNC");
      buildIndex(table, index);
      assertEquals("a7", nearest(table, index, "a7", rows.get("a7")));
      float[] added = vector(random);
      upsertDoc(conn, table, "a-new", added);
      assertEquals("a-new", nearest(table, index, "a-new", added));
    }
  }

  private static void upsertDoc(Connection conn, String table, String id, float[] v)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
      ps.setString(1, id);
      ps.setObject(2, new BsonDocument("embedding", new BsonBinary(BinaryVector.floatVector(v))));
      ps.executeUpdate();
    }
    conn.commit();
  }

  /** Tests that rows with null indexed vector values are omitted. */
  @Test
  public void testIvfIgnoresIncludedVectorWhenIndexedVectorIsNull() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY,"
        + " V VECTOR(FLOAT, 4), W VECTOR(FLOAT, 4))");
      conn.createStatement().execute("UPSERT INTO " + table
        + " VALUES ('r1', ARRAY[1.0, 0.0, 0.0, 0.0], ARRAY[0.0, 1.0, 0.0, 0.0])");
      conn.commit();
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) INCLUDE (W) WITH (algorithm='IVF', metric='L2', lists=1, sample_size=1)");
      conn.createStatement()
        .execute("UPSERT INTO " + table + " (ID, W) VALUES " + "('r2', ARRAY[0.0, 0.0, 1.0, 0.0])");
      conn.commit();
      assertEquals(1, VectorIndexTestUtil.getHBaseRowKeys(conn.unwrap(PhoenixConnection.class),
        PhoenixRuntime.getTableNoCache(conn, index)).size());
    }
  }

  /**
   * Tests region split handling: verifies daughter regions search the parent segment within their
   * respective key ranges, rebuild independent segments, preserve concurrent updates, and retire
   * the parent segment row only after all daughters construct their own segments.
   */
  @Test
  public void testSplit() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400, "");
      long split = EnvironmentEdgeManager.currentTimeMillis();
      try (Admin admin = getUtility().getAdmin()) {
        admin.split(TableName.valueOf(table), Bytes.toBytes("m"));
      }
      awaitRegions(table, 2);
      HRegion first = regions(table).get(0);
      for (byte[] key : manager(first, index).search(rows.get("z1"), 50, 64)) {
        assertTrue(Bytes.toString(key) + " is outside the daughter",
          first.getRegionInfo().containsRow(key));
      }

      Random random = new Random(17);
      float[] added = vector(random);
      float[] moved = negate(rows.get("z3"));
      upsert(conn, table, "a-new", added);
      upsert(conn, table, "z3", moved);
      // The second daughter initializes its index manager on the first write, triggering a rebuild
      awaitRebuilt(conn, table, index, split);
      List<HnswSegment.Descriptor> segments = awaitSegments(conn, index, 2);
      for (HRegion region : regions(table)) {
        boolean own = false;
        for (HnswSegment.Descriptor d : segments) {
          own |= d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
        }
        assertTrue(own);
      }
      assertEquals("a-new", nearest(table, index, "a-new", added));
      assertEquals("z3", nearest(table, index, "z3", moved));
      assertNotFound(table, index, "z3", rows.get("z3"));
      rows.put("a-new", added);
      rows.put("z3", moved);
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /** Tests region split behavior when predecessor regions contain stacked delta segments. */
  @Test
  public void testSplitWithDeltas() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400, "");
      HRegion parentRegion = regions(table).get(0);
      HnswIndexManager parentMgr = manager(parentRegion, index);

      float[] initialA0 = rows.get("a0");
      float[] newA0 = negate(initialA0);
      upsert(conn, table, "a0", newA0);
      float[] initialA2 = rows.get("a2");
      delete(conn, table, "a2");
      rows.put("a0", newA0);
      rows.remove("a2");

      long sinceDelta = EnvironmentEdgeManager.currentTimeMillis();
      parentMgr.flush();
      HnswSegment.Descriptor parentDelta = awaitDelta(conn, index, parentRegion, sinceDelta, 60);
      assertTrue(parentDelta.isDelta());

      long split = EnvironmentEdgeManager.currentTimeMillis();
      try (Admin admin = getUtility().getAdmin()) {
        admin.split(TableName.valueOf(table), Bytes.toBytes("m"));
      }
      awaitRegions(table, 2);

      List<HRegion> daughters = regions(table);
      HRegion first = daughters.get(0);
      HRegion second = daughters.get(1);

      for (byte[] key : manager(first, index).search(rows.get("z1"), 50, 64)) {
        assertTrue(Bytes.toString(key) + " is outside the daughter",
          first.getRegionInfo().containsRow(key));
      }
      assertEquals("a0", nearest(table, index, "a0", newA0));
      assertNotFound(table, index, "a2", initialA2);

      manager(second, index).search(rows.get("a0"), 10, 64);
      awaitRebuilt(conn, table, index, split);

      List<HnswSegment.Descriptor> segments = awaitSegments(conn, index, 2);
      for (HRegion region : regions(table)) {
        boolean own = false;
        for (HnswSegment.Descriptor d : segments) {
          own |= d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
          assertFalse("daughter segments should be base segments", d.isDelta());
        }
        assertTrue(own);
      }
      assertEquals("a0", nearest(table, index, "a0", newA0));
      assertNotFound(table, index, "a2", initialA2);
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /** A merged region searches both inputs' segments, then replaces them with its own. */
  @Test
  public void testMerge() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      long merge = EnvironmentEdgeManager.currentTimeMillis();
      List<HRegion> inputs = regions(table);
      try (Admin admin = getUtility().getAdmin()) {
        admin.mergeRegionsAsync(new byte[][] { inputs.get(0).getRegionInfo().getRegionName(),
          inputs.get(1).getRegionInfo().getRegionName() }, false).get();
      }
      awaitRegions(table, 1);
      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      assertEquals("z1", nearest(table, index, "z1", rows.get("z1")));
      awaitRebuilt(conn, table, index, merge);
      awaitSegments(conn, index, 1);
      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      assertEquals("z1", nearest(table, index, "z1", rows.get("z1")));
    }
  }

  /**
   * A region still searching a segment whose row was retired, as a split daughter searches its
   * parent until it cuts over to its own segment, searches the newer covering segment once the
   * retired one's evicted graph cannot reload. Buffered mutations still shadow that segment's rows.
   */
  @Test
  public void testSearchReplacesRetiredSegment() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400, "");
      long split = EnvironmentEdgeManager.currentTimeMillis();
      try (Admin admin = getUtility().getAdmin()) {
        admin.split(TableName.valueOf(table), Bytes.toBytes("m"));
      }
      awaitRegions(table, 2);
      HRegion second = regions(table).get(1);
      byte[] start = second.getRegionInfo().getStartKey();
      byte[] end = second.getRegionInfo().getEndKey();
      for (HRegion region : regions(table)) {
        manager(region, index);
      }
      awaitRebuilt(conn, table, index, split);
      HnswSegment.Descriptor own = null;
      for (HnswSegment.Descriptor d : awaitSegments(conn, index, 2)) {
        own = d.covers(start, end) ? d : own;
      }
      assertNotNull(own);

      // A change buffered in memory that the replacement segment does not hold
      float[] moved = negate(rows.get("z3"));
      upsert(conn, table, "z3", moved);

      // Retire the region's segment in favor of a newer one written from the original rows
      List<VectorFloat<?>> values = new ArrayList<>();
      List<byte[]> keys = new ArrayList<>();
      for (Map.Entry<String, float[]> row : rows.entrySet()) {
        if (second.getRegionInfo().containsRow(Bytes.toBytes(row.getKey()))) {
          keys.add(Bytes.toBytes(row.getKey()));
          values.add(VTS.createFloatVector(row.getValue()));
        }
      }
      PTable indexTable = PhoenixRuntime.getTableNoCache(conn, index);
      byte[] payload = HnswSegment.build(indexTable.getVectorIndex(),
        new ListRandomAccessVectorValues(values, DIM), keys.toArray(new byte[0][]));
      try (Table t = conn.unwrap(PhoenixConnection.class).getQueryServices()
        .getTable(indexTable.getPhysicalName().getBytes())) {
        HnswSegment.write(t, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, start, end,
          EnvironmentEdgeManager.currentTimeMillis(), payload, keys.size());
        t.delete(new Delete(own.rowKey));
      }
      HnswOffheapAllocator.get(getUtility().getConfiguration()).evictAll();

      assertEquals("z1", nearest(table, index, "z1", rows.get("z1")));
      assertEquals("z3", nearest(table, index, "z3", moved));
      assertNotFound(table, index, "z3", rows.get("z3"));
      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      rows.put("z3", moved);
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }
}
