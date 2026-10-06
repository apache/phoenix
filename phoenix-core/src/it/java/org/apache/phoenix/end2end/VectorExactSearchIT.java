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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for exact vector search on a table without a vector index. The tests compare
 * query results with brute force rankings for filters, bound limits, NULL vectors, and OFFSET.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorExactSearchIT extends ParallelStatsDisabledIT {

  // Fixture: an unsalted table of 50 rows with a VECTOR(FLOAT, 3) and an INTEGER; 5 have no vector
  private static String tableUnsalted3D;
  private static final int UNSALTED_TOTAL_ROWS = 50;
  private static final int UNSALTED_NON_NULL_ROWS = 45;
  private static final float[][] unsaltedVectors = new float[UNSALTED_NON_NULL_ROWS][3];
  private static final int[] unsaltedVals = new int[UNSALTED_TOTAL_ROWS];

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);

    tableUnsalted3D = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Create the fixture table
      conn.createStatement().execute("CREATE TABLE " + tableUnsalted3D
        + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3), val INTEGER)");

      for (int i = 0; i < UNSALTED_NON_NULL_ROWS; i++) {
        unsaltedVectors[i][0] = (i < 5) ? (0.9f + 0.02f * i) : ((i % 3) * 0.3f);
        unsaltedVectors[i][1] = (i < 5) ? 0.05f * i : ((i % 5) * 0.2f);
        unsaltedVectors[i][2] = (i < 5) ? 0.02f * i : ((i % 7) * 0.15f);
        // Alternate the values, so that a filter on val selects half of the rows
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
    }
  }

  @Test
  public void testWherePredicateWithDistanceOrderBy() throws Exception {
    // A distance ORDER BY together with a filter on a column that is not a vector
    float[] queryVec = { 1.0f, 0.0f, 0.0f };

    // Rank all rows by brute force, without the filter
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

    // Rank by brute force only the rows that pass the filter
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
      // The LIMIT of 100 is more than the 45 rows that have a vector
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

      // Make sure that no distance is less than the distance before it
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
      // NULLS LAST puts rows without a vector after all rows with a vector
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

      // Without a NULLS clause, an ascending distance puts rows without a vector last
      String defaultQuery = "SELECT pk FROM " + tableUnsalted3D
        + " ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) LIMIT 25";
      List<Integer> defaultPks = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(defaultQuery)) {
        while (rs.next()) {
          defaultPks.add(rs.getInt(1));
        }
      }
      assertEquals(25, defaultPks.size());
      for (int pk : defaultPks) {
        assertTrue("Default ordering must return non-null vectors first; pk=" + pk,
          pk <= UNSALTED_NON_NULL_ROWS);
      }
      List<Integer> allPks = nullOrderedPks(conn, "");
      assertEquals(UNSALTED_TOTAL_ROWS, allPks.size());
      for (int i = UNSALTED_NON_NULL_ROWS; i < UNSALTED_TOTAL_ROWS; i++) {
        int pk = allPks.get(i);
        assertTrue("Default ascending distance places NULL rows last; pk=" + pk,
          pk > UNSALTED_NON_NULL_ROWS);
      }

      // An explicit NULLS FIRST puts rows without a vector first
      List<Integer> nullsFirstPks = nullOrderedPks(conn, " NULLS FIRST");
      assertEquals(UNSALTED_TOTAL_ROWS, nullsFirstPks.size());
      for (int i = 0; i < UNSALTED_TOTAL_ROWS - UNSALTED_NON_NULL_ROWS; i++) {
        int pk = nullsFirstPks.get(i);
        assertTrue("NULLS FIRST places NULL rows first; pk=" + pk, pk > UNSALTED_NON_NULL_ROWS);
      }
    }
  }

  private static List<Integer> nullOrderedPks(Connection conn, String nulls) throws Exception {
    String query = "SELECT pk, v FROM " + tableUnsalted3D
      + " ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0])" + nulls;
    List<Integer> pks = new ArrayList<>();
    int nullCount = 0;
    try (ResultSet rs = conn.createStatement().executeQuery(query)) {
      while (rs.next()) {
        pks.add(rs.getInt(1));
        if (rs.getArray(2) == null) {
          nullCount++;
        }
      }
    }
    assertEquals(UNSALTED_TOTAL_ROWS - UNSALTED_NON_NULL_ROWS, nullCount);
    return pks;
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
    // OFFSET 5 LIMIT 5 gives ranks 6 to 10 of the brute force ranking
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
