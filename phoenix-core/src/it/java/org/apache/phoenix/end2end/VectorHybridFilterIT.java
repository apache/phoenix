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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.QueryUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for hybrid vector queries with relational filters, verifying adaptive centroid
 * probing, selective secondary index pre-filtering, and covered vs. uncovered document predicate
 * evaluation.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorHybridFilterIT extends ParallelStatsDisabledIT {

  private static final List<float[]> CENTROIDS =
    Arrays.asList(new float[] { 0f, 0f, 0f, 0f }, new float[] { 10f, 0f, 0f, 0f },
      new float[] { 20f, 0f, 0f, 0f }, new float[] { 30f, 0f, 0f, 0f });
  private static final float[] ORIGIN = new float[] { 0f, 0f, 0f, 0f };

  private static Float[] boxed(float[] v) {
    Float[] b = new Float[v.length];
    for (int i = 0; i < v.length; i++) {
      b[i] = v[i];
    }
    return b;
  }

  /**
   * Test fixture with four centroid clusters partitioned across category values to validate
   * selective filtering and deferred projection across posting lists.
   */
  private static void createFixture(Connection conn, String table, String index)
    throws SQLException {
    createFixture(conn, table, index, "");
  }

  private static void createFixture(Connection conn, String table, String index,
    String tableOptions) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, "
        + "V VECTOR(FLOAT, 4), CATEGORY VARCHAR, DESCRIPTION VARCHAR) " + tableOptions);
      stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table + " (V) INCLUDE (CATEGORY) "
        + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
    }
    VectorIndexTestUtil.activateWithKnownCentroids(conn, table, index, CENTROIDS);
    try (PreparedStatement ps = conn.prepareStatement(
      "UPSERT INTO " + table + " (ID, V, CATEGORY, DESCRIPTION) VALUES (?, ?, ?, ?)")) {
      for (int c = 0; c < 4; c++) {
        for (int a = 1; a <= 4; a++) {
          upsert(conn, ps, "c" + c + "_a" + a, c * 10f + a * 0.1f, "A");
        }
        upsert(conn, ps, "c" + c + "_b1", c * 10f + 0.5f, "B");
      }
    }
    conn.commit();
  }

  private static void upsert(Connection conn, PreparedStatement ps, String id, float x,
    String category) throws SQLException {
    ps.setString(1, id);
    ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { x, 0f, 0f, 0f }));
    ps.setString(3, category);
    ps.setString(4, "description of " + id);
    ps.executeUpdate();
  }

  private static List<String> search(Connection conn, String sql, float[] q) throws SQLException {
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  private static String explain(Connection conn, String sql, float[] q) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
      return QueryUtil.getExplainPlan(ps.executeQuery());
    }
  }

  private static QueryPlan optimize(Connection conn, String sql, float[] q) throws SQLException {
    PreparedStatement ps = conn.prepareStatement(sql);
    ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
    return ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
  }

  @Test
  public void testAdaptiveProbingSatisfiesLimit() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createFixture(conn, table, index);
      // Requires expansion across multiple centroid posting lists to satisfy the limit
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, CATEGORY FROM " + table
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
      assertEquals(Arrays.asList("c0_b1", "c1_b1", "c2_b1"), search(conn, sql, ORIGIN));
      String plan = explain(conn, sql, ORIGIN);
      assertTrue(plan,
        plan.contains("CLIENT PROBING 1 OF 4 CENTROIDS (L2) EXPANDING UP TO 4 BATCHES"));
    }
  }

  @Test
  public void testAdaptiveProbingAppliesOffsetToGlobalOrder() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createFixture(conn, table, index);
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + table
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 2 OFFSET 1";
      assertEquals(Arrays.asList("c1_b1", "c2_b1"), search(conn, sql, ORIGIN));
    }
  }

  @Test
  public void testAdaptiveProbingStopsAtEveryCentroid() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createFixture(conn, table, index);
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + table
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 10";
      assertEquals(Arrays.asList("c0_b1", "c1_b1", "c2_b1", "c3_b1"), search(conn, sql, ORIGIN));
    }
  }

  @Test
  public void testAdaptiveProbingStopsAtMaxProbeLimit() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createFixture(conn, table, index);
      String hinted = "SELECT /*+ VECTOR_PROBE_COUNT(1) MAX_PROBE_LIMIT(2) */ ID FROM " + table
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
      assertEquals(Arrays.asList("c0_b1", "c1_b1"), search(conn, hinted, ORIGIN));
      assertTrue(explain(conn, hinted, ORIGIN).contains("EXPANDING UP TO 2 BATCHES"));

      Properties props = new Properties();
      props.setProperty(QueryServices.VECTOR_MAX_PROBE_LIMIT_ATTRIB, "2");
      try (Connection limited = DriverManager.getConnection(getUrl(), props)) {
        String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + table
          + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
        assertEquals(Arrays.asList("c0_b1", "c1_b1"), search(limited, sql, ORIGIN));
      }
      // Single-batch limit disables adaptive expansion
      String single = "SELECT /*+ VECTOR_PROBE_COUNT(1) MAX_PROBE_LIMIT(1) */ ID FROM " + table
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
      assertEquals(Arrays.asList("c0_b1"), search(conn, single, ORIGIN));
      assertFalse(explain(conn, single, ORIGIN).contains("EXPANDING"));
    }
  }

  @Test
  public void testUnfilteredQueryDoesNotProbeAdaptively() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createFixture(conn, table, index);
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + table
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 10";
      // Unfiltered queries execute without adaptive expansion
      assertEquals(Arrays.asList("c0_a1", "c0_a2", "c0_a3", "c0_a4", "c0_b1"),
        search(conn, sql, ORIGIN));
      assertFalse(explain(conn, sql, ORIGIN).contains("EXPANDING"));
    }
  }

  @Test
  public void testAdaptiveProbingWithDeferredProjection() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createFixture(conn, table, index);
      // Covered predicate evaluated during index probing with deferred projection for uncovered
      // attributes
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, DESCRIPTION FROM " + table
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
      QueryPlan plan = optimize(conn, sql, ORIGIN);
      assertTrue(plan.getClass().getName(), VectorIndexTestUtil.isDeferredProjection(plan));
      Map<String, String> rows = new LinkedHashMap<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed(ORIGIN)));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            rows.put(rs.getString(1), rs.getString(2));
          }
        }
      }
      assertEquals(Arrays.asList("c0_b1", "c1_b1", "c2_b1"), new ArrayList<>(rows.keySet()));
      for (Map.Entry<String, String> row : rows.entrySet()) {
        assertEquals("description of " + row.getKey(), row.getValue());
      }
    }
  }

  @Test
  public void testFilteredPlanHasEstimates() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createFixture(conn, table, index, "GUIDE_POSTS_WIDTH=20");
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + table);
      }
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + table
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
      VectorIndexScanPlan plan = VectorIndexTestUtil.vectorPlan(optimize(conn, sql, ORIGIN));
      assertTrue(plan.isAdaptive());
      assertNotNull("adaptive plan must estimate its first batch", plan.getEstimatedRowsToScan());
      assertFalse(plan.getCost().isUnknown());
    }
  }

  /**
   * Test fixture evaluating optimizer plan selection between secondary index filtering and vector
   * index probing across varying selectivity distributions.
   */
  private static void createAuthorFixture(Connection conn, String table, String authorIndex,
    String vectorIndex, boolean selective, boolean coverVector) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, "
        + "V VECTOR(FLOAT, 4), AUTHOR VARCHAR) GUIDE_POSTS_WIDTH=100");
      stmt.execute("CREATE INDEX " + authorIndex + " ON " + table + " (AUTHOR)"
        + (coverVector ? " INCLUDE (V)" : ""));
      stmt.execute("CREATE VECTOR INDEX " + vectorIndex + " ON " + table + " (V) INCLUDE (AUTHOR)"
        + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
    }
    VectorIndexTestUtil.activateWithKnownCentroids(conn, table, vectorIndex, CENTROIDS);
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, V, AUTHOR) VALUES (?, ?, ?)")) {
      for (int i = 0; i < 1000; i++) {
        ps.setString(1, String.format("row_%04d", i));
        ps.setArray(2,
          conn.createArrayOf("FLOAT", new Float[] { (i % 4) * 10f + i * 0.001f, 0f, 0f, 0f }));
        ps.setString(3, i < (selective ? 10 : 900) ? "Alice" : "Bob");
        ps.executeUpdate();
        if (i % 250 == 0) {
          conn.commit();
        }
      }
    }
    conn.commit();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("UPDATE STATISTICS " + table);
    }
  }

  private static List<String> bruteForceAlice(Connection conn, String table) throws SQLException {
    return search(conn, "SELECT /*+ NO_INDEX */ ID FROM " + table
      + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5", ORIGIN);
  }

  @Test
  public void testSelectiveFilterIsEvaluatedFirst() throws Exception {
    String table = generateUniqueName();
    String authorIndex = generateUniqueName();
    String vectorIndex = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAuthorFixture(conn, table, authorIndex, vectorIndex, true, true);
      String sql =
        "SELECT ID FROM " + table + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      String plan = explain(conn, sql, ORIGIN);
      assertTrue(plan, plan.contains(authorIndex));
      assertFalse(plan, plan.contains("CLIENT PROBING"));
      // Pre-filtered secondary index scan produces exact results matching brute force scan
      assertEquals(bruteForceAlice(conn, table), search(conn, sql, ORIGIN));
    }
  }

  @Test
  public void testUncoveredSelectiveIndexIsUsedWhenHinted() throws Exception {
    String table = generateUniqueName();
    String authorIndex = generateUniqueName();
    String vectorIndex = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAuthorFixture(conn, table, authorIndex, vectorIndex, true, false);
      // Uncovered secondary index requires explicit hint to be selected
      String sql =
        "SELECT ID FROM " + table + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      assertTrue(explain(conn, sql, ORIGIN).contains(vectorIndex));
      String hinted = "SELECT /*+ INDEX(" + table + " " + authorIndex + ") */ ID FROM " + table
        + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      String plan = explain(conn, hinted, ORIGIN);
      assertTrue(plan, plan.contains(authorIndex));
      assertFalse(plan, plan.contains("CLIENT PROBING"));
      assertEquals(bruteForceAlice(conn, table), search(conn, hinted, ORIGIN));
    }
  }

  @Test
  public void testUnselectiveFilterUsesVectorIndex() throws Exception {
    String table = generateUniqueName();
    String authorIndex = generateUniqueName();
    String vectorIndex = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAuthorFixture(conn, table, authorIndex, vectorIndex, false, true);
      String sql =
        "SELECT ID FROM " + table + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      String plan = explain(conn, sql, ORIGIN);
      assertTrue(plan, plan.contains(vectorIndex));
      assertTrue(plan, plan.contains("CLIENT PROBING"));
      assertFalse(plan, plan.contains(authorIndex));
    }
  }

  private static BsonDocument doc(float x, int i, String category) {
    float[] v = new float[] { x, 0.1f * i, 0f, 0f };
    BsonDocument doc = new BsonDocument("embedding", new BsonBinary(BinaryVector.floatVector(v)));
    doc.put("category", new BsonString(category));
    return doc;
  }

  private static void testDocumentFilter(boolean coverDocument) throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + table
          + " (PK VARCHAR NOT NULL PRIMARY KEY, DOC BSON) IMMUTABLE_ROWS=true");
        stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table
          + " (BSON_VECTOR_VALUE(DOC, 'embedding', 4))" + (coverDocument ? " INCLUDE (DOC)" : "")
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, table, index, CENTROIDS);
      float[] xs = { 1f, 3f, 4f, 9f, 11f, 13f, 19f, 21f, 23f, 28f, 29f, 31f };
      String[] categories = { "science", "art", "science", "art", "math", "science", "art", "math",
        "science", "art", "math", "science" };
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        for (int i = 0; i < xs.length; i++) {
          ps.setString(1, "r" + i);
          ps.setObject(2, doc(xs[i], i, categories[i]));
          ps.executeUpdate();
        }
      }
      conn.commit();

      float[] q = new float[] { 5f, 0f, 0f, 0f };
      String vector = "BSON_VECTOR_VALUE(DOC, 'embedding', 4)";
      for (String predicate : new String[] { "BSON_VALUE(DOC, 'category', 'VARCHAR') = 'science'",
        "BSON_VALUE(DOC, 'category', 'VARCHAR') IN ('science', 'math')" }) {
        String sql = "SELECT PK FROM " + table + " WHERE " + predicate + " ORDER BY L2_DISTANCE("
          + vector + ", ?) LIMIT 4";
        String plan = explain(conn, sql, q);
        assertTrue(plan, plan.contains(index));
        assertTrue(plan, plan.contains("CLIENT PROBING"));
        // Server merge required when document column is not covered by the vector index
        assertEquals(plan, !coverDocument, plan.contains("SERVER MERGE"));
        String exact = "SELECT /*+ NO_INDEX */ PK FROM " + table + " WHERE " + predicate
          + " ORDER BY L2_DISTANCE(" + vector + ", ?) LIMIT 4";
        String all = "SELECT /*+ VECTOR_PROBE_COUNT(4) */ PK FROM " + table + " WHERE " + predicate
          + " ORDER BY L2_DISTANCE(" + vector + ", ?) LIMIT 4";
        assertEquals(search(conn, exact, q), search(conn, all, q));
        assertEquals(search(conn, exact, q), search(conn, sql, q));
      }
    }
  }

  @Test
  public void testCoveredDocumentFilter() throws Exception {
    testDocumentFilter(true);
  }

  @Test
  public void testUncoveredDocumentFilter() throws Exception {
    testDocumentFilter(false);
  }
}
