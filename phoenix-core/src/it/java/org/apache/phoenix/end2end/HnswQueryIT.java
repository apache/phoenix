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

import static org.apache.phoenix.end2end.HnswIndexIT.awaitDelta;
import static org.apache.phoenix.end2end.HnswIndexIT.cosine;
import static org.apache.phoenix.end2end.HnswIndexIT.createAndBuild;
import static org.apache.phoenix.end2end.HnswIndexIT.delete;
import static org.apache.phoenix.end2end.HnswIndexIT.manager;
import static org.apache.phoenix.end2end.HnswIndexIT.regions;
import static org.apache.phoenix.end2end.HnswIndexIT.reopen;
import static org.apache.phoenix.end2end.HnswIndexIT.segments;
import static org.apache.phoenix.end2end.HnswIndexIT.upsert;
import static org.apache.phoenix.end2end.HnswIndexIT.vector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration tests for nearest neighbor vector queries using HNSW indexes. */
@Category(ParallelStatsDisabledTest.class)
public class HnswQueryIT extends ParallelStatsDisabledIT {

  private static String sql(String table, String hint, String where, int limit) {
    return "SELECT " + hint + " ID FROM " + table + where + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT "
      + limit;
  }

  private static List<String> query(Connection conn, String sql, float[] q) throws Exception {
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", box(q)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  private static String explain(Connection conn, String sql, float[] q) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", box(q)));
      return QueryUtil.getExplainPlan(ps.executeQuery());
    }
  }

  private static Float[] box(float[] v) {
    Float[] boxed = new Float[v.length];
    for (int i = 0; i < v.length; i++) {
      boxed[i] = v[i];
    }
    return boxed;
  }

  private static List<String> bruteForce(Map<String, float[]> rows, float[] q, int k) {
    return rows.entrySet().stream()
      .sorted(Comparator.comparingDouble(e -> -cosine(q, e.getValue()))).limit(k)
      .map(Map.Entry::getKey).collect(Collectors.toList());
  }

  /** Tests top-k query execution, distance ordering, and recall across multiple regions. */
  @Test
  public void testTopK() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      String sql = sql(table, "", "", 10);
      Random random = new Random(1);
      float[] q = vector(random);
      assertTrue(
        explain(conn, sql, q).contains("SERVER HNSW SEARCH " + index + " (64 CANDIDATES)"));
      int hits = 0;
      int queries = 20;
      for (int i = 0; i < queries; i++) {
        q = vector(random);
        List<String> found = query(conn, sql, q);
        assertEquals(10, found.size());
        for (int j = 1; j < found.size(); j++) {
          assertTrue("Results must be ordered by distance",
            cosine(q, rows.get(found.get(j - 1))) >= cosine(q, rows.get(found.get(j))) - 1e-6);
        }
        Set<String> expected = bruteForce(rows, q, 10).stream().collect(Collectors.toSet());
        for (String id : found) {
          hits += expected.contains(id) ? 1 : 0;
        }
      }
      double recall = hits / (double) (queries * 10);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /** Tests that queries scan only candidate rows returned by the graph index. */
  @Test
  public void testQueryReadsGraphCandidatesOnly() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      // Insert an unindexed raw row directly via HBase
      try (Table t = getUtility().getConnection().getTable(TableName.valueOf(table))) {
        Result a0 = t.get(new Get(Bytes.toBytes("a0")));
        Put raw = new Put(Bytes.toBytes("a0-raw"));
        for (Cell cell : a0.rawCells()) {
          raw.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
            CellUtil.cloneValue(cell));
        }
        t.put(raw);
      }
      String sql = sql(table, "", "", 2);
      List<String> found = query(conn, sql, rows.get("a0"));
      assertEquals("a0", found.get(0));
      assertFalse("Unindexed row should not be returned by HNSW query: " + found,
        found.contains("a0-raw"));
      List<String> exact = query(conn, sql(table, "/*+ NO_INDEX */", "", 2), rows.get("a0"));
      assertTrue(exact.contains("a0-raw"));

      HnswIndexIT.rebuild(conn, table, index);
      assertTrue(query(conn, sql, rows.get("a0")).contains("a0-raw"));
    }
  }

  /** Tests visibility of unflushed in-memory mutations during query execution. */
  @Test
  public void testUnflushedChanges() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      float[] added = vector(new Random(23));
      upsert(conn, table, "z-new", added);
      assertEquals("z-new", query(conn, sql(table, "", "", 1), added).get(0));
      conn.createStatement().execute("DELETE FROM " + table + " WHERE ID = 'a8'");
      conn.commit();
      assertFalse(query(conn, sql(table, "", "", 5), rows.get("a8")).contains("a8"));
    }
  }

  /** Tests that filtered queries bypass the HNSW index and execute exact scans. */
  @Test
  public void testFilteredQueryDoesNotUseIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      float[] q = vector(new Random(31));
      String sql = sql(table, "", " WHERE ID > 'm'", 5);
      assertFalse(explain(conn, sql, q).contains("HNSW"));
      Map<String, float[]> upper =
        rows.entrySet().stream().filter(e -> e.getKey().compareTo("m") > 0)
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      assertEquals(bruteForce(upper, q, 5), query(conn, sql, q));
    }
  }

  /** Tests candidate count sizing based on the HNSW_EF_SEARCH hint and query LIMIT. */
  @Test
  public void testCandidateCount() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 300);
      float[] q = vector(new Random(41));
      String hinted = sql(table, "/*+ HNSW_EF_SEARCH(150) */", "", 10);
      assertTrue(explain(conn, hinted, q).contains("(150 CANDIDATES)"));
      String wide = sql(table, "", "", 100);
      assertTrue(explain(conn, wide, q).contains("(100 CANDIDATES)"));
      assertEquals(100, query(conn, wide, q).size());
    }
  }

  /** Tests query execution when both IVF and HNSW indexes exist on the same column. */
  @Test
  public void testIvfAndHnswOnSameColumn() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    String ivf = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, hnsw, 200);
      conn.createStatement().execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='COSINE', lists=2, sample_size=100)");
      float[] added = vector(new Random(51));
      upsert(conn, table, "a-new", added);
      for (String idx : new String[] { hnsw, ivf }) {
        String sql = sql(table, "/*+ INDEX(" + table + " " + idx + ") */", "", 1);
        assertTrue(explain(conn, sql, added).contains(idx));
        assertEquals("a-new", query(conn, sql, added).get(0));
      }
    }
  }

  /**
   * Tests nearest neighbor query accuracy across stacked delta segments before and after reopen.
   */
  @Test
  public void testTopKWithDeltas() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      Random random = new Random(17);

      HRegion r0 = regions(table).get(0);
      HRegion r1 = regions(table).get(1);

      // Create stacked delta segments across regions
      for (int i = 0; i < 20; i += 2) {
        String id = "a" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      for (int i = 20; i < 30; i += 2) {
        String id = "a" + i;
        delete(conn, table, id);
        rows.remove(id);
      }
      for (int i = 0; i < 5; i++) {
        String id = "a100" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long sinceR0D1 = EnvironmentEdgeManager.currentTimeMillis();
      manager(r0, index).flush();
      awaitDelta(conn, index, r0, sinceR0D1, 60);

      for (int i = 30; i < 50; i += 2) {
        String id = "a" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      for (int i = 50; i < 60; i += 2) {
        String id = "a" + i;
        delete(conn, table, id);
        rows.remove(id);
      }
      for (int i = 5; i < 10; i++) {
        String id = "a100" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long sinceR0D2 = EnvironmentEdgeManager.currentTimeMillis();
      manager(r0, index).flush();
      awaitDelta(conn, index, r0, sinceR0D2, 60);

      for (int i = 1; i < 21; i += 2) {
        String id = "z" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      for (int i = 21; i < 31; i += 2) {
        String id = "z" + i;
        delete(conn, table, id);
        rows.remove(id);
      }
      for (int i = 0; i < 5; i++) {
        String id = "z100" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long sinceR1D1 = EnvironmentEdgeManager.currentTimeMillis();
      manager(r1, index).flush();
      awaitDelta(conn, index, r1, sinceR1D1, 60);

      for (int i = 31; i < 51; i += 2) {
        String id = "z" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      for (int i = 51; i < 61; i += 2) {
        String id = "z" + i;
        delete(conn, table, id);
        rows.remove(id);
      }
      for (int i = 5; i < 10; i++) {
        String id = "z100" + i;
        float[] v = vector(random);
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long sinceR1D2 = EnvironmentEdgeManager.currentTimeMillis();
      manager(r1, index).flush();
      awaitDelta(conn, index, r1, sinceR1D2, 60);

      assertEquals(6, segments(conn, index).size());

      String sql = sql(table, "", "", 10);
      float[] q = vector(random);
      assertTrue(explain(conn, sql, q).contains("SERVER HNSW SEARCH " + index));

      // Verify query recall before and after reopening the table
      assertRecall(conn, sql, rows, random, 20, 10, 0.9);
      reopen(table);
      assertRecall(conn, sql, rows, random, 20, 10, 0.9);
    }
  }

  private static void assertRecall(Connection conn, String sql, Map<String, float[]> rows,
    Random random, int queries, int k, double minRecall) throws Exception {
    int hits = 0;
    for (int i = 0; i < queries; i++) {
      float[] q = vector(random);
      List<String> found = query(conn, sql, q);
      assertEquals(k, found.size());
      for (int j = 1; j < found.size(); j++) {
        assertTrue("rows must be ordered by distance",
          cosine(q, rows.get(found.get(j - 1))) >= cosine(q, rows.get(found.get(j))) - 1e-6);
      }
      Set<String> expected = bruteForce(rows, q, k).stream().collect(Collectors.toSet());
      for (String id : found) {
        hits += expected.contains(id) ? 1 : 0;
      }
    }
    double recall = hits / (double) (queries * k);
    assertTrue("recall " + recall + " < " + minRecall, recall >= minRecall);
  }
}
