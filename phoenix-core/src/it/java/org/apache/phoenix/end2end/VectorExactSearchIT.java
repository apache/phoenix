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
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Exact vector search integration tests. */
@Category(ParallelStatsDisabledTest.class)
public class VectorExactSearchIT extends ParallelStatsDisabledIT {

  // Fixture 1: Unsalted 50-row 3-D float table with val INTEGER and 5 NULL vectors
  private static String tableUnsalted3D;
  private static final int UNSALTED_TOTAL_ROWS = 50;
  private static final int UNSALTED_NON_NULL_ROWS = 45;
  private static final float[][] unsaltedVectors = new float[UNSALTED_NON_NULL_ROWS][3];
  private static final int[] unsaltedVals = new int[UNSALTED_TOTAL_ROWS];

  // Fixture 2: Salted 200-row 4-D float table (seed 42, 4 buckets)
  private static String tableSalted4DFloat;
  private static final int SALTED_FLOAT_ROWS = 200;
  private static final float[][] saltedFloatVectors = new float[SALTED_FLOAT_ROWS][4];

  // Fixture 3: Salted 100-row 4-D double table (seed 777, 4 buckets)
  private static String tableSalted4DDouble;
  private static final int SALTED_DOUBLE_ROWS = 100;
  private static final double[][] saltedDoubleVectors = new double[SALTED_DOUBLE_ROWS][4];

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);

    tableUnsalted3D = generateUniqueName();
    tableSalted4DFloat = generateUniqueName();
    tableSalted4DDouble = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // 1. Setup Fixture 1
      conn.createStatement().execute("CREATE TABLE " + tableUnsalted3D
        + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3), val INTEGER)");

      for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
        unsaltedVectors[i][0] = (i < 5) ? (0.9f + 0.02f * i) : ((i % 3) * 0.3f);
        unsaltedVectors[i][1] = (i < 5) ? 0.05f * i : ((i % 5) * 0.2f);
        unsaltedVectors[i][2] = (i < 5) ? 0.02f * i : ((i % 7) * 0.15f);
        // Alternate val so pk 1 has val=10 (excluded by val > 50) while pk 2 has val=60
        unsaltedVals[i] = (i % 2 == 0) ? 10 : 60;
      }
      for (int i = UNSALTED_NON_NULL_ROWS; i < UNSALTED_TOTAL_ROWS; i++) {
        unsaltedVals[i] = 100;
      }

      String upsert1 = "UPSERT INTO " + tableUnsalted3D + " VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert1)) {
        for (int i = 0; i < UNSALTED_TOTAL_ROWS; i++) {
          ps.setInt(1, i + 1);
          if (i < UNSALTED_NON_NULL_ROWS) {
            Float[] boxed =
              new Float[] { unsaltedVectors[i][0], unsaltedVectors[i][1], unsaltedVectors[i][2] };
            ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          } else {
            ps.setNull(2, java.sql.Types.ARRAY);
          }
          ps.setInt(3, unsaltedVals[i]);
          ps.executeUpdate();
        }
      }
      conn.commit();

      // 2. Setup Fixture 2 (salted 200-row 4D float, seed 42)
      conn.createStatement().execute("CREATE TABLE " + tableSalted4DFloat
        + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 4)) SALT_BUCKETS=4");

      Random rng42 = new Random(42);
      for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
        for (int d = 0; d < 4; d++) {
          saltedFloatVectors[i][d] = rng42.nextFloat();
        }
      }
      String upsert2 = "UPSERT INTO " + tableSalted4DFloat + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert2)) {
        for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
          ps.setInt(1, i + 1);
          Float[] boxed = new Float[] { saltedFloatVectors[i][0], saltedFloatVectors[i][1],
            saltedFloatVectors[i][2], saltedFloatVectors[i][3] };
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // 3. Setup Fixture 3 (salted 100-row 4D double, seed 777)
      conn.createStatement().execute("CREATE TABLE " + tableSalted4DDouble
        + " (pk INTEGER PRIMARY KEY, v VECTOR(DOUBLE, 4)) SALT_BUCKETS=4");

      Random rng777 = new Random(777);
      for (int i = 0; i < SALTED_DOUBLE_ROWS; i++) {
        for (int d = 0; d < 4; d++) {
          saltedDoubleVectors[i][d] = rng777.nextDouble();
        }
      }
      String upsert3 = "UPSERT INTO " + tableSalted4DDouble + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert3)) {
        for (int i = 0; i < SALTED_DOUBLE_ROWS; i++) {
          ps.setInt(1, i + 1);
          Double[] boxed = new Double[] { saltedDoubleVectors[i][0], saltedDoubleVectors[i][1],
            saltedDoubleVectors[i][2], saltedDoubleVectors[i][3] };
          ps.setArray(2, conn.createArrayOf("DOUBLE", boxed));
          ps.executeUpdate();
        }
      }
      conn.commit();
    }
  }

  @Test
  public void testServerSidePushDownVerification() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query = "SELECT pk FROM " + tableUnsalted3D
        + " WHERE v IS NOT NULL ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) LIMIT 5";
      ResultSet explainRs = conn.createStatement().executeQuery("EXPLAIN " + query);
      String explainPlan = QueryUtil.getExplainPlan(explainRs);
      assertNotNull("EXPLAIN plan must not be null", explainPlan);
      assertTrue("EXPLAIN plan should contain 'SERVER TOP-5': " + explainPlan,
        explainPlan.contains("SERVER TOP-5"));
      assertTrue("EXPLAIN plan should contain 'CLIENT MERGE SORT TOP-5': " + explainPlan,
        explainPlan.contains("CLIENT MERGE SORT TOP-5"));

      float[] queryVec = { 1.0f, 0.0f, 0.0f };
      double[] distances = new double[UNSALTED_NON_NULL_ROWS];
      for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
        distances[i] = VectorIndexTestUtil.dist("L2_DISTANCE", unsaltedVectors[i], queryVec);
      }
      Integer[] indices = new Integer[UNSALTED_NON_NULL_ROWS];
      for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
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
      assertEquals(expectedPks, resultPks);
    }
  }

  @Test
  public void testAllDistanceFunctionsPushDownCorrectness() throws Exception {
    float[] queryVec = { 0.9f, 0.1f, 0.0f };
    String[] functions =
      { "L2_DISTANCE", "L2_DISTANCE_SQUARED", "COSINE_DISTANCE", "INNER_PRODUCT" };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      for (String func : functions) {
        String query = "SELECT pk FROM " + tableUnsalted3D + " WHERE v IS NOT NULL ORDER BY " + func
          + "(v, ARRAY[0.9,0.1,0.0]) LIMIT 2";

        double[] dists = new double[UNSALTED_NON_NULL_ROWS];
        for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
          dists[i] = VectorIndexTestUtil.dist(func, unsaltedVectors[i], queryVec);
        }
        Integer[] idx = new Integer[UNSALTED_NON_NULL_ROWS];
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

  @Test
  public void testBindParameterQueryVectorPushDown() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query = "SELECT pk FROM " + tableUnsalted3D
        + " WHERE v IS NOT NULL ORDER BY L2_DISTANCE(v, ?) LIMIT 1";
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        Array queryVec = conn.createArrayOf("FLOAT", new Float[] { 0.9f, 0.0f, 0.0f });
        ps.setArray(1, queryVec);
        ResultSet rs = ps.executeQuery();
        assertTrue("Should have at least one result", rs.next());
        assertEquals("Row with pk=1 should be nearest to query [0.9,0,0]", 1, rs.getInt(1));
      }
    }
  }

  @Test
  public void testExactVectorSearchExplainPlan() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query =
        "SELECT * FROM " + tableUnsalted3D + " ORDER BY COSINE_DISTANCE(v, ?) LIMIT 10";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f }));
        ResultSet rs = ps.executeQuery();
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertNotNull("EXPLAIN plan must not be null", explainPlan);
        assertTrue("EXPLAIN plan should contain 'COSINE_DISTANCE': " + explainPlan,
          explainPlan.contains("COSINE_DISTANCE"));
        assertTrue("EXPLAIN plan should contain 'TOP-10': " + explainPlan,
          explainPlan.contains("TOP-10"));
        assertTrue("EXPLAIN plan should contain 'CLIENT MERGE SORT TOP-10': " + explainPlan,
          explainPlan.contains("CLIENT MERGE SORT TOP-10"));
        assertTrue("EXPLAIN plan should contain 'SERVER TOP-10 BY COSINE_DISTANCE': " + explainPlan,
          explainPlan.contains("SERVER TOP-10 BY COSINE_DISTANCE"));
      }

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
        assertTrue(attributes.getServerSortedBy().contains("COSINE_DISTANCE"));
        assertTrue(attributes.getServerSortedBy().contains("TOP-10"));
      }
    }
  }

  @Test
  public void testCrossRegionMergeCorrectness() throws Exception {
    int limit = 5;
    float[] queryVec = new float[] { 0.25f, 0.50f, 0.75f, 0.10f };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query =
        "SELECT pk FROM " + tableSalted4DFloat + " ORDER BY L2_DISTANCE(v, ?) LIMIT " + limit;

      try (PreparedStatement psExplain = conn.prepareStatement("EXPLAIN " + query)) {
        Float[] boxedQuery = new Float[4];
        for (int d = 0; d < 4; d++) {
          boxedQuery[d] = queryVec[d];
        }
        psExplain.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
        ResultSet rsExplain = psExplain.executeQuery();
        String explainPlan = QueryUtil.getExplainPlan(rsExplain);
        assertNotNull(explainPlan);
        assertTrue(explainPlan.contains("CLIENT MERGE SORT"));
        assertTrue(explainPlan.contains("SERVER TOP-5"));
      }

      double[] distances = new double[SALTED_FLOAT_ROWS];
      for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
        distances[i] = VectorIndexTestUtil.dist("L2_DISTANCE", saltedFloatVectors[i], queryVec);
      }
      Integer[] indices = new Integer[SALTED_FLOAT_ROWS];
      for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < limit; i++) {
        expectedPks.add(indices[i] + 1);
      }

      List<Integer> actualPks = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        Float[] boxedQuery = new Float[4];
        for (int d = 0; d < 4; d++) {
          boxedQuery[d] = queryVec[d];
        }
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualPks.add(rs.getInt(1));
          }
        }
      }

      assertEquals(limit, actualPks.size());
      assertEquals(expectedPks, actualPks);
    }
  }

  @Test
  public void testCrossRegionMergeWithArrayLiteral() throws Exception {
    int limit = 5;
    float[] queryVec = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      double[] distances = new double[SALTED_FLOAT_ROWS];
      for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
        distances[i] = VectorIndexTestUtil.dist("L2_DISTANCE", saltedFloatVectors[i], queryVec);
      }
      Integer[] indices = new Integer[SALTED_FLOAT_ROWS];
      for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < limit; i++) {
        expectedPks.add(indices[i] + 1);
      }

      String query = "SELECT pk FROM " + tableSalted4DFloat
        + " ORDER BY L2_DISTANCE(v, ARRAY[0.1, 0.2, 0.3, 0.4]) LIMIT " + limit;
      List<Integer> actualPks = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        while (rs.next()) {
          actualPks.add(rs.getInt(1));
        }
      }

      assertEquals(limit, actualPks.size());
      assertEquals(expectedPks, actualPks);
    }
  }

  @Test
  public void testCrossRegionMergeAllMetrics() throws Exception {
    int limit = 5;
    float[] queryVec = new float[] { 0.3f, 0.4f, 0.5f, 0.6f };
    String[] metrics = { "L2_DISTANCE", "L2_DISTANCE_SQUARED", "COSINE_DISTANCE", "INNER_PRODUCT" };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      for (String metric : metrics) {
        double[] distances = new double[SALTED_FLOAT_ROWS];
        for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
          distances[i] = VectorIndexTestUtil.dist(metric, saltedFloatVectors[i], queryVec);
        }
        Integer[] indices = new Integer[SALTED_FLOAT_ROWS];
        for (int i = 0; i < SALTED_FLOAT_ROWS; i++) {
          indices[i] = i;
        }
        Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

        List<Integer> expectedPks = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
          expectedPks.add(indices[i] + 1);
        }

        String query =
          "SELECT pk FROM " + tableSalted4DFloat + " ORDER BY " + metric + "(v, ?) LIMIT " + limit;
        List<Integer> actualPks = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(query)) {
          Float[] boxedQuery = new Float[4];
          for (int d = 0; d < 4; d++) {
            boxedQuery[d] = queryVec[d];
          }
          ps.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              actualPks.add(rs.getInt(1));
            }
          }
        }

        assertEquals(metric + ": row count mismatch", limit, actualPks.size());
        assertEquals(metric + ": top-5 mismatch", expectedPks, actualPks);
      }
    }
  }

  @Test
  public void testCrossRegionMergeDoubleVectors() throws Exception {
    int limit = 5;
    double[] queryVec = new double[] { 0.2, 0.4, 0.6, 0.8 };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      double[] distances = new double[SALTED_DOUBLE_ROWS];
      for (int i = 0; i < SALTED_DOUBLE_ROWS; i++) {
        double sumSq = 0;
        for (int d = 0; d < 4; d++) {
          double diff = saltedDoubleVectors[i][d] - queryVec[d];
          sumSq += diff * diff;
        }
        distances[i] = Math.sqrt(sumSq);
      }
      Integer[] indices = new Integer[SALTED_DOUBLE_ROWS];
      for (int i = 0; i < SALTED_DOUBLE_ROWS; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < limit; i++) {
        expectedPks.add(indices[i] + 1);
      }

      String query =
        "SELECT pk FROM " + tableSalted4DDouble + " ORDER BY L2_DISTANCE(v, ?) LIMIT " + limit;
      List<Integer> actualPks = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        Double[] boxedQuery = new Double[4];
        for (int d = 0; d < 4; d++) {
          boxedQuery[d] = queryVec[d];
        }
        ps.setArray(1, conn.createArrayOf("DOUBLE", boxedQuery));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualPks.add(rs.getInt(1));
          }
        }
      }

      assertEquals(limit, actualPks.size());
      assertEquals(expectedPks, actualPks);
    }
  }

  @Test
  public void testWherePredicateWithDistanceOrderBy() throws Exception {
    // Verify distance ordering combined with a non-vector filter predicate.
    float[] queryVec = { 1.0f, 0.0f, 0.0f };

    // Compute unfiltered top candidates.
    double[] allDists = new double[UNSALTED_NON_NULL_ROWS];
    for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
      allDists[i] = VectorIndexTestUtil.dist("L2_DISTANCE", unsaltedVectors[i], queryVec);
    }
    Integer[] allIdx = new Integer[UNSALTED_NON_NULL_ROWS];
    for (int i = 0; i < allIdx.length; i++) {
      allIdx[i] = i;
    }
    Arrays.sort(allIdx, (a, b) -> Double.compare(allDists[a], allDists[b]));
    List<Integer> unfilteredTop5 = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      unfilteredTop5.add(allIdx[i] + 1);
    }
    assertTrue("Unfiltered top-5 must contain at least one excluded row (val <= 50)",
      unfilteredTop5.contains(1));

    // Compute expected results matching the filter predicate.
    List<Integer> matchingRows = new ArrayList<>();
    for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
      if (unsaltedVals[i] > 50) {
        matchingRows.add(i);
      }
    }
    matchingRows.sort((a, b) -> Double.compare(allDists[a], allDists[b]));
    List<Integer> expectedFilteredPks = new ArrayList<>();
    for (int i = 0; i < Math.min(5, matchingRows.size()); i++) {
      expectedFilteredPks.add(matchingRows.get(i) + 1);
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query = "SELECT pk FROM " + tableUnsalted3D
        + " WHERE v IS NOT NULL AND val > 50 ORDER BY L2_DISTANCE(v, ?) LIMIT 5";
      List<Integer> actualPks = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f }));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualPks.add(rs.getInt(1));
          }
        }
      }
      assertEquals("Filtered query must return exactly 5 rows", 5, actualPks.size());
      assertEquals("Filtered query must match filtered oracle", expectedFilteredPks, actualPks);
      assertFalse("Filtered results must not contain excluded pk 1", actualPks.contains(1));
    }
  }

  @Test
  public void testLimitGreaterThanRowCount() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // 45 non-null rows, LIMIT 100
      String query = "SELECT pk FROM " + tableUnsalted3D
        + " WHERE v IS NOT NULL ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) LIMIT 100";
      List<Integer> actualPks = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        while (rs.next()) {
          actualPks.add(rs.getInt(1));
        }
      }
      assertEquals("All non-null rows must be returned when LIMIT > row count",
        UNSALTED_NON_NULL_ROWS, actualPks.size());

      // Check distance ordering
      float[] queryVec = { 1.0f, 0.0f, 0.0f };
      double prevDist = -1.0;
      for (int pk : actualPks) {
        double d = VectorIndexTestUtil.dist("L2_DISTANCE", unsaltedVectors[pk - 1], queryVec);
        assertTrue("Distance should be ascending: " + d + " >= " + prevDist, d >= prevDist);
        prevDist = d;
      }
    }
  }

  @Test
  public void testBindLimit() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query = "SELECT pk FROM " + tableUnsalted3D
        + " WHERE v IS NOT NULL ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) LIMIT ?";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setInt(1, 5);
        ResultSet rs = ps.executeQuery();
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertTrue("EXPLAIN plan should indicate SERVER TOP-5: " + explainPlan,
          explainPlan.contains("SERVER TOP-5"));
      }

      List<Integer> actualPks = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setInt(1, 5);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualPks.add(rs.getInt(1));
          }
        }
      }
      assertEquals("Bound LIMIT ? should return 5 rows", 5, actualPks.size());
      assertEquals(Arrays.asList(2, 1, 3, 4, 5), actualPks);
    }
  }

  @Test
  public void testNullVectorsInOrderBy() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Verify NULLS LAST restricts results to non-null vector rows in distance order.
      String limitQuery = "SELECT pk FROM " + tableUnsalted3D
        + " ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) NULLS LAST LIMIT 25";
      List<Integer> limitPks = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(limitQuery)) {
        while (rs.next()) {
          limitPks.add(rs.getInt(1));
        }
      }
      assertEquals(25, limitPks.size());
      for (int pk : limitPks) {
        assertTrue("Returned row must have non-null vector (pk <= 45)",
          pk <= UNSALTED_NON_NULL_ROWS);
      }

      // Verify Phoenix default ASC ordering sorts null vector values first.
      String allQuery =
        "SELECT pk, v FROM " + tableUnsalted3D + " ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0])";
      List<Integer> allPks = new ArrayList<>();
      int nullCount = 0;
      try (ResultSet rs = conn.createStatement().executeQuery(allQuery)) {
        while (rs.next()) {
          int pk = rs.getInt(1);
          allPks.add(pk);
          if (rs.getArray(2) == null) {
            nullCount++;
          }
        }
      }
      assertEquals(UNSALTED_TOTAL_ROWS, allPks.size());
      assertEquals(5, nullCount);
      // In Phoenix ASC order, nulls sort first; verify the initial rows are null records (pk > 45)
      for (int i = 0; i < 5; i++) {
        int pk = allPks.get(i);
        assertTrue("Phoenix default ORDER BY ASC places NULL rows first; pk=" + pk,
          pk > UNSALTED_NON_NULL_ROWS);
      }
    }
  }

  @Test
  public void testOffsetWithDistanceOrderBy() throws Exception {
    float[] queryVec = { 1.0f, 0.0f, 0.0f };
    double[] distances = new double[UNSALTED_NON_NULL_ROWS];
    for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
      distances[i] = VectorIndexTestUtil.dist("L2_DISTANCE", unsaltedVectors[i], queryVec);
    }
    Integer[] indices = new Integer[UNSALTED_NON_NULL_ROWS];
    for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
      indices[i] = i;
    }
    Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

    List<Integer> expectedPks = new ArrayList<>();
    // Expect ranks 6 through 10 for OFFSET 5 LIMIT 5.
    for (int i = 5; i < 10; i++) {
      expectedPks.add(indices[i] + 1);
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query = "SELECT pk FROM " + tableUnsalted3D
        + " WHERE v IS NOT NULL ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) LIMIT 5 OFFSET 5";
      List<Integer> actualPks = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        while (rs.next()) {
          actualPks.add(rs.getInt(1));
        }
      }
      assertEquals(5, actualPks.size());
      assertEquals("LIMIT 5 OFFSET 5 must match ranks 6 to 10", expectedPks, actualPks);
    }
  }
}
