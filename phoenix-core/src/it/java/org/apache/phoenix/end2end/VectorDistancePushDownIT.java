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

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verifies that ORDER BY distance + LIMIT queries push the distance evaluation to servers and
 * return the correct global top-K results.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorDistancePushDownIT extends ParallelStatsDisabledIT {

  /** Verifies server-side push-down using known reference vectors. */
  @Test
  public void testServerSidePushDownVerification() throws Exception {
    String tableName = generateUniqueName();

    // Structured vectors yielding deterministic nearest neighbors for [1.0, 0.0, 0.0].
    float[][] vectors = new float[50][3];
    for (int i = 0; i < 50; i++) {
      // Construct vectors such that rows 0-4 are closest to the query vector.
      vectors[i][0] = (i < 5) ? (0.9f + 0.02f * i) : ((i % 3) * 0.3f);
      vectors[i][1] = (i < 5) ? 0.05f * i : ((i % 5) * 0.2f);
      vectors[i][2] = (i < 5) ? 0.02f * i : ((i % 7) * 0.15f);
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 50; i++) {
          ps.setInt(1, i + 1);
          Float[] boxed = new Float[] { vectors[i][0], vectors[i][1], vectors[i][2] };
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Verify explain plan reflects server top-N push-down and client merge sort
      String query =
        "SELECT pk FROM " + tableName + " ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) LIMIT 5";
      ResultSet explainRs = conn.createStatement().executeQuery("EXPLAIN " + query);
      String explainPlan = QueryUtil.getExplainPlan(explainRs);
      assertNotNull("EXPLAIN plan must not be null", explainPlan);
      assertTrue("EXPLAIN plan should contain 'SERVER TOP-5' but was:\n" + explainPlan,
        explainPlan.contains("SERVER TOP-5"));
      assertTrue("EXPLAIN plan should contain 'CLIENT MERGE SORT TOP-5' but was:\n" + explainPlan,
        explainPlan.contains("CLIENT MERGE SORT TOP-5"));

      // Compute expected nearest neighbors via brute-force comparison
      final float[] queryVec = { 1.0f, 0.0f, 0.0f };
      double[] distances = new double[50];
      for (int i = 0; i < 50; i++) {
        distances[i] = bruteForceDist("L2_DISTANCE", vectors[i], queryVec);
      }
      Integer[] indices = new Integer[50];
      for (int i = 0; i < 50; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        expectedPks.add(indices[i] + 1);
      }

      ResultSet rs = conn.createStatement().executeQuery(query);
      List<Integer> resultPks = new ArrayList<>();
      while (rs.next()) {
        resultPks.add(rs.getInt(1));
      }

      assertEquals("Should return exactly 5 rows", 5, resultPks.size());
      assertEquals("Returned PKs should match brute-force top-5 nearest neighbors (in order). "
        + "Expected: " + expectedPks + ", Got: " + resultPks, expectedPks, resultPks);
    }
  }

  /**
   * Verifies push-down correctness across all supported distance metrics using candidate vectors
   * whose relative orderings differ by metric.
   */
  @Test
  public void testAllDistanceFunctionsPushDownCorrectness() throws Exception {
    String tableName = generateUniqueName();

    // Vectors configured to produce metric-specific nearest neighbor orderings.
    float[][] vectors = { { 1.0f, 0.0f, 0.0f }, { 0.0f, 1.0f, 0.0f }, { 0.0f, 0.0f, 1.0f },
      { 0.6f, 0.8f, 0.0f }, { 0.5f, 0.5f, 0.5f } };
    float[] queryVec = { 0.9f, 0.1f, 0.0f };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < vectors.length; i++) {
          ps.setInt(1, i + 1);
          ps.setArray(2, conn.createArrayOf("FLOAT",
            new Float[] { vectors[i][0], vectors[i][1], vectors[i][2] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      String[] functions =
        { "L2_DISTANCE", "L2_DISTANCE_SQUARED", "COSINE_DISTANCE", "INNER_PRODUCT" };
      for (String func : functions) {
        String query =
          "SELECT pk FROM " + tableName + " ORDER BY " + func + "(v, ARRAY[0.9,0.1,0.0]) LIMIT 2";

        // Compute expected ordering for this metric
        double[] dists = new double[vectors.length];
        for (int i = 0; i < vectors.length; i++) {
          dists[i] = bruteForceDist(func, vectors[i], queryVec);
        }
        Integer[] idx = new Integer[vectors.length];
        for (int i = 0; i < idx.length; i++) {
          idx[i] = i;
        }
        Arrays.sort(idx, (a, b) -> Double.compare(dists[a], dists[b]));
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
          expected.add(idx[i] + 1);
        }

        ResultSet rs = conn.createStatement().executeQuery(query);
        List<Integer> actual = new ArrayList<>();
        while (rs.next()) {
          actual.add(rs.getInt(1));
        }
        assertEquals(func + ": wrong top-2 result", expected, actual);
      }
    }
  }

  private static double bruteForceDist(String func, float[] a, float[] b) {
    double dot = 0, normA = 0, normB = 0, sumSq = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sumSq += diff * diff;
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    switch (func) {
      case "L2_DISTANCE":
        return Math.sqrt(sumSq);
      case "L2_DISTANCE_SQUARED":
        return sumSq;
      case "COSINE_DISTANCE":
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return (denom == 0) ? 1.0 : 1.0 - dot / denom;
      case "INNER_PRODUCT":
        return -dot;
      default:
        throw new IllegalArgumentException(func);
    }
  }

  /** Verifies push-down execution when the query vector is supplied as a bind parameter. */
  @Test
  public void testBindParameterQueryVectorPushDown() throws Exception {
    String tableName = generateUniqueName();

    float[][] vectors = { { 1.0f, 0.0f, 0.0f }, { 0.0f, 1.0f, 0.0f }, { 0.0f, 0.0f, 1.0f },
      { 0.5f, 0.5f, 0.0f }, { 0.7f, 0.2f, 0.1f } };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < vectors.length; i++) {
          ps.setInt(1, i + 1);
          ps.setArray(2, conn.createArrayOf("FLOAT",
            new Float[] { vectors[i][0], vectors[i][1], vectors[i][2] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Row pk=1 is the expected nearest match for [1.0, 0.0, 0.0]
      String query = "SELECT pk FROM " + tableName + " ORDER BY L2_DISTANCE(v, ?) LIMIT 1";
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        Array queryVec = conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f });
        ps.setArray(1, queryVec);
        ResultSet rs = ps.executeQuery();
        assertTrue("Should have at least one result", rs.next());
        assertEquals("Row with pk=1 (vector [1,0,0]) should be nearest to query [1,0,0]", 1,
          rs.getInt(1));
      }
    }
  }

  /** Verifies query plan and attributes for exact vector similarity search queries. */
  @Test
  public void testExactVectorSearchExplainPlan() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3), val INTEGER)");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setInt(1, 1);
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f }));
        ps.setInt(3, 42);
        ps.executeUpdate();
      }
      conn.commit();

      // Verify parameterized query explain plan
      String query = "SELECT * FROM " + tableName + " ORDER BY COSINE_DISTANCE(v, ?) LIMIT 10";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f }));
        ResultSet rs = ps.executeQuery();
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertNotNull("EXPLAIN plan must not be null", explainPlan);
        assertTrue("EXPLAIN plan should contain 'COSINE_DISTANCE' but was:\n" + explainPlan,
          explainPlan.contains("COSINE_DISTANCE"));
        assertTrue("EXPLAIN plan should contain 'TOP-10' but was:\n" + explainPlan,
          explainPlan.contains("TOP-10"));
        assertTrue(
          "EXPLAIN plan should contain 'CLIENT MERGE SORT TOP-10' but was:\n" + explainPlan,
          explainPlan.contains("CLIENT MERGE SORT TOP-10"));
        assertTrue(
          "EXPLAIN plan should contain 'SERVER TOP-10 BY COSINE_DISTANCE' but was:\n" + explainPlan,
          explainPlan.contains("SERVER TOP-10 BY COSINE_DISTANCE"));
      }

      // Verify explain plan attributes
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f }));
        ExplainPlan plan =
          ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        ExplainPlanAttributes attributes = plan.getPlanStepsAsAttributes();
        assertNotNull("ExplainPlanAttributes must not be null", attributes);
        assertTrue("isVectorSearch should be true", attributes.isVectorSearch());
        assertEquals("CLIENT MERGE SORT TOP-10", attributes.getClientSortAlgo());
        assertEquals(Integer.valueOf(10), attributes.getClientRowLimit());
        assertEquals(Long.valueOf(10), attributes.getServerRowLimit());
        assertNotNull(attributes.getServerSortedBy());
        assertTrue("serverSortedBy should contain 'COSINE_DISTANCE'",
          attributes.getServerSortedBy().contains("COSINE_DISTANCE"));
        assertTrue("serverSortedBy should contain 'TOP-10'",
          attributes.getServerSortedBy().contains("TOP-10"));
      }

      // Verify literal array query explain plan
      String literalQuery =
        "SELECT * FROM " + tableName + " ORDER BY COSINE_DISTANCE(v, ARRAY[1.0,0.0,0.0]) LIMIT 10";
      try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + literalQuery)) {
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertTrue("EXPLAIN plan should contain 'COSINE_DISTANCE' but was:\n" + explainPlan,
          explainPlan.contains("COSINE_DISTANCE"));
        assertTrue("EXPLAIN plan should contain 'TOP-10' but was:\n" + explainPlan,
          explainPlan.contains("TOP-10"));
        assertTrue(
          "EXPLAIN plan should contain 'CLIENT MERGE SORT TOP-10' but was:\n" + explainPlan,
          explainPlan.contains("CLIENT MERGE SORT TOP-10"));
      }

      // Verify remaining distance functions in explain plan
      String[] otherFuncs = { "L2_DISTANCE", "L2_DISTANCE_SQUARED", "INNER_PRODUCT" };
      for (String func : otherFuncs) {
        String funcQuery =
          "SELECT pk FROM " + tableName + " ORDER BY " + func + "(v, ARRAY[1.0,0.0,0.0]) LIMIT 10";
        try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + funcQuery)) {
          String explainPlan = QueryUtil.getExplainPlan(rs);
          assertTrue("EXPLAIN plan should contain " + func + " but was:\n" + explainPlan,
            explainPlan.contains(func));
          assertTrue(
            "EXPLAIN plan should contain 'SERVER TOP-10 BY " + func + "' but was:\n" + explainPlan,
            explainPlan.contains("SERVER TOP-10 BY " + func));
          assertTrue(
            "EXPLAIN plan should contain 'CLIENT MERGE SORT TOP-10' but was:\n" + explainPlan,
            explainPlan.contains("CLIENT MERGE SORT TOP-10"));
        }
      }

      // Verify non-vector query plan formatting is preserved
      String nonVectorQuery = "SELECT * FROM " + tableName + " ORDER BY val LIMIT 10";
      try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + nonVectorQuery)) {
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertTrue(
          "Non-vector EXPLAIN plan should contain 'CLIENT MERGE SORT' but was:\n" + explainPlan,
          explainPlan.contains("CLIENT MERGE SORT"));
        assertTrue(
          "Non-vector EXPLAIN plan should contain 'CLIENT LIMIT 10' but was:\n" + explainPlan,
          explainPlan.contains("CLIENT LIMIT 10"));
        assertFalse("Non-vector EXPLAIN plan must not contain 'TOP-10' but was:\n" + explainPlan,
          explainPlan.contains("TOP-10"));
        assertTrue(
          "Non-vector EXPLAIN plan should contain 'SERVER TOP 10 ROWS SORTED BY' but was:\n"
            + explainPlan,
          explainPlan.contains("SERVER TOP 10 ROWS SORTED BY"));
      }
      try (PreparedStatement ps = conn.prepareStatement(nonVectorQuery)) {
        ExplainPlan plan =
          ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        ExplainPlanAttributes attributes = plan.getPlanStepsAsAttributes();
        assertFalse("isVectorSearch should be false for non-vector query",
          attributes.isVectorSearch());
        assertEquals("CLIENT MERGE SORT", attributes.getClientSortAlgo());
        assertEquals(Integer.valueOf(10), attributes.getClientRowLimit());
        assertEquals(Long.valueOf(10), attributes.getServerRowLimit());
        assertEquals("[VAL]", attributes.getServerSortedBy());
      }
    }
  }

  /**
   * Verifies distributed top-N merge across salted regions, ensuring MergeSortTopNResultIterator
   * consolidates per-region candidates into the correct global nearest neighbors.
   */
  @Test
  public void testCrossRegionMergeWithSaltedTable() throws Exception {
    String tableName = generateUniqueName();
    final int numRows = 200;
    final int dim = 4;
    final int topK = 5;

    Random rng = new Random(42);
    float[][] vectors = new float[numRows][dim];
    for (int i = 0; i < numRows; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
    }

    float[] queryVec = { 0.5f, 0.5f, 0.5f, 0.5f };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, " + dim + ")) SALT_BUCKETS=4");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < numRows; i++) {
          ps.setInt(1, i + 1);
          Float[] boxed = new Float[dim];
          for (int d = 0; d < dim; d++) {
            boxed[d] = vectors[i][d];
          }
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Compute expected nearest neighbors via brute-force comparison
      double[] distances = new double[numRows];
      for (int i = 0; i < numRows; i++) {
        distances[i] = bruteForceDist("L2_DISTANCE", vectors[i], queryVec);
      }
      Integer[] indices = new Integer[numRows];
      for (int i = 0; i < numRows; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));
      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < topK; i++) {
        expectedPks.add(indices[i] + 1);
      }

      String query = "SELECT pk FROM " + tableName
        + " ORDER BY L2_DISTANCE(v, ARRAY[0.5,0.5,0.5,0.5]) LIMIT " + topK;

      // Verify explain plan reflects parallel scan with vector push-down
      ResultSet explainRs = conn.createStatement().executeQuery("EXPLAIN " + query);
      String explainPlan = QueryUtil.getExplainPlan(explainRs);
      assertNotNull("EXPLAIN plan must not be null", explainPlan);
      assertTrue("EXPLAIN plan should contain 'SERVER TOP-" + topK + "' but was:\n" + explainPlan,
        explainPlan.contains("SERVER TOP-" + topK));
      assertTrue(
        "EXPLAIN plan should contain 'CLIENT MERGE SORT TOP-" + topK + "' but was:\n" + explainPlan,
        explainPlan.contains("CLIENT MERGE SORT TOP-" + topK));
      assertTrue("EXPLAIN plan should show parallel scan (4-WAY) but was:\n" + explainPlan,
        explainPlan.contains("4-WAY"));

      ResultSet rs = conn.createStatement().executeQuery(query);
      List<Integer> resultPks = new ArrayList<>();
      while (rs.next()) {
        resultPks.add(rs.getInt(1));
      }

      assertEquals("Should return exactly " + topK + " rows", topK, resultPks.size());
      assertEquals(
        "Cross-region merge: returned PKs should match brute-force top-" + topK
          + " nearest neighbors (in order). Expected: " + expectedPks + ", Got: " + resultPks,
        expectedPks, resultPks);

      // Verify execution with a bind parameter
      String bindQuery =
        "SELECT pk FROM " + tableName + " ORDER BY L2_DISTANCE(v, ?) LIMIT " + topK;
      try (PreparedStatement ps = conn.prepareStatement(bindQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.5f, 0.5f, 0.5f, 0.5f }));
        ResultSet bindRs = ps.executeQuery();
        List<Integer> bindResultPks = new ArrayList<>();
        while (bindRs.next()) {
          bindResultPks.add(bindRs.getInt(1));
        }
        assertEquals("Bind-param cross-region merge: wrong top-" + topK, expectedPks,
          bindResultPks);
      }
    }
  }
}
