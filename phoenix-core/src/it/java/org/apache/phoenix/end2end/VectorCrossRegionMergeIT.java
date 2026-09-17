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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class VectorCrossRegionMergeIT extends ParallelStatsDisabledIT {

  /**
   * Verifies distributed top-N vector search on a multi-region salted table, ensuring client-side
   * merge matches brute-force nearest neighbor calculations across all regions.
   */
  @Test
  public void testCrossRegionMergeCorrectness() throws Exception {
    String tableName = generateUniqueName();
    int numRows = 200;
    int dim = 4;
    int limit = 5;

    Random rng = new Random(42);
    float[][] vectors = new float[numRows][dim];
    for (int i = 0; i < numRows; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
    }

    float[] queryVec = new float[] { 0.25f, 0.50f, 0.75f, 0.10f };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 4)) SALT_BUCKETS=4");

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

      // Verify explain plan reflects server top-N push-down and client merge sort
      String query = "SELECT pk FROM " + tableName + " ORDER BY L2_DISTANCE(v, ?) LIMIT " + limit;
      try (PreparedStatement psExplain = conn.prepareStatement("EXPLAIN " + query)) {
        Float[] boxedQuery = new Float[dim];
        for (int d = 0; d < dim; d++) {
          boxedQuery[d] = queryVec[d];
        }
        psExplain.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
        ResultSet rsExplain = psExplain.executeQuery();
        String explainPlan = QueryUtil.getExplainPlan(rsExplain);
        assertNotNull("EXPLAIN plan must not be null", explainPlan);
        assertTrue("EXPLAIN plan should indicate client merge sort: " + explainPlan,
          explainPlan.contains("CLIENT MERGE SORT"));
        assertTrue("EXPLAIN plan should indicate server top-N: " + explainPlan,
          explainPlan.contains("SERVER TOP-5"));
      }

      // Compute expected nearest neighbors via brute-force comparison
      double[] distances = new double[numRows];
      for (int i = 0; i < numRows; i++) {
        distances[i] = computeDist("L2_DISTANCE", vectors[i], queryVec);
      }
      Integer[] indices = new Integer[numRows];
      for (int i = 0; i < numRows; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < limit; i++) {
        expectedPks.add(indices[i] + 1);
      }

      List<Integer> actualPks = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        Float[] boxedQuery = new Float[dim];
        for (int d = 0; d < dim; d++) {
          boxedQuery[d] = queryVec[d];
        }
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualPks.add(rs.getInt(1));
          }
        }
      }

      assertEquals("Should return exactly " + limit + " rows", limit, actualPks.size());
      assertEquals("Global top-" + limit + " across 4 salt buckets must match brute-force",
        expectedPks, actualPks);
    }
  }

  /** Verifies cross-region merge using an array literal query vector. */
  @Test
  public void testCrossRegionMergeWithArrayLiteral() throws Exception {
    String tableName = generateUniqueName();
    int numRows = 100;
    int dim = 4;
    int limit = 5;

    Random rng = new Random(123);
    float[][] vectors = new float[numRows][dim];
    for (int i = 0; i < numRows; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
    }
    float[] queryVec = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 4)) SALT_BUCKETS=4");

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

      double[] distances = new double[numRows];
      for (int i = 0; i < numRows; i++) {
        distances[i] = computeDist("L2_DISTANCE", vectors[i], queryVec);
      }
      Integer[] indices = new Integer[numRows];
      for (int i = 0; i < numRows; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < limit; i++) {
        expectedPks.add(indices[i] + 1);
      }

      String query = "SELECT pk FROM " + tableName
        + " ORDER BY L2_DISTANCE(v, ARRAY[0.1, 0.2, 0.3, 0.4]) LIMIT " + limit;
      List<Integer> actualPks = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        while (rs.next()) {
          actualPks.add(rs.getInt(1));
        }
      }

      assertEquals("Should return exactly " + limit + " rows", limit, actualPks.size());
      assertEquals("Literal query vector: global top-" + limit + " must match brute-force",
        expectedPks, actualPks);
    }
  }

  /** Verifies cross-region merge correctness across all supported distance metrics. */
  @Test
  public void testCrossRegionMergeAllMetrics() throws Exception {
    String tableName = generateUniqueName();
    int numRows = 120;
    int dim = 4;
    int limit = 5;

    Random rng = new Random(999);
    float[][] vectors = new float[numRows][dim];
    for (int i = 0; i < numRows; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
    }
    float[] queryVec = new float[] { 0.3f, 0.4f, 0.5f, 0.6f };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 4)) SALT_BUCKETS=4");

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

      String[] metrics =
        { "L2_DISTANCE", "L2_DISTANCE_SQUARED", "COSINE_DISTANCE", "INNER_PRODUCT" };
      for (String metric : metrics) {
        double[] distances = new double[numRows];
        for (int i = 0; i < numRows; i++) {
          distances[i] = computeDist(metric, vectors[i], queryVec);
        }
        Integer[] indices = new Integer[numRows];
        for (int i = 0; i < numRows; i++) {
          indices[i] = i;
        }
        Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

        List<Integer> expectedPks = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
          expectedPks.add(indices[i] + 1);
        }

        String query =
          "SELECT pk FROM " + tableName + " ORDER BY " + metric + "(v, ?) LIMIT " + limit;
        List<Integer> actualPks = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(query)) {
          Float[] boxedQuery = new Float[dim];
          for (int d = 0; d < dim; d++) {
            boxedQuery[d] = queryVec[d];
          }
          ps.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              actualPks.add(rs.getInt(1));
            }
          }
        }

        assertEquals(metric + ": should return " + limit + " rows", limit, actualPks.size());
        assertEquals(metric + ": cross-region merge top-" + limit + " must match brute-force",
          expectedPks, actualPks);
      }
    }
  }

  /** Verifies cross-region merge correctness with double-precision vectors. */
  @Test
  public void testCrossRegionMergeDoubleVectors() throws Exception {
    String tableName = generateUniqueName();
    int numRows = 100;
    int dim = 4;
    int limit = 5;

    Random rng = new Random(777);
    double[][] vectors = new double[numRows][dim];
    for (int i = 0; i < numRows; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextDouble();
      }
    }
    double[] queryVec = new double[] { 0.2, 0.4, 0.6, 0.8 };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (pk INTEGER PRIMARY KEY, v VECTOR(DOUBLE, 4)) SALT_BUCKETS=4");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < numRows; i++) {
          ps.setInt(1, i + 1);
          Double[] boxed = new Double[dim];
          for (int d = 0; d < dim; d++) {
            boxed[d] = vectors[i][d];
          }
          ps.setArray(2, conn.createArrayOf("DOUBLE", boxed));
          ps.executeUpdate();
        }
      }
      conn.commit();

      double[] distances = new double[numRows];
      for (int i = 0; i < numRows; i++) {
        double sumSq = 0;
        for (int d = 0; d < dim; d++) {
          double diff = vectors[i][d] - queryVec[d];
          sumSq += diff * diff;
        }
        distances[i] = Math.sqrt(sumSq);
      }
      Integer[] indices = new Integer[numRows];
      for (int i = 0; i < numRows; i++) {
        indices[i] = i;
      }
      Arrays.sort(indices, (a, b) -> Double.compare(distances[a], distances[b]));

      List<Integer> expectedPks = new ArrayList<>();
      for (int i = 0; i < limit; i++) {
        expectedPks.add(indices[i] + 1);
      }

      String query = "SELECT pk FROM " + tableName + " ORDER BY L2_DISTANCE(v, ?) LIMIT " + limit;
      List<Integer> actualPks = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        Double[] boxedQuery = new Double[dim];
        for (int d = 0; d < dim; d++) {
          boxedQuery[d] = queryVec[d];
        }
        ps.setArray(1, conn.createArrayOf("DOUBLE", boxedQuery));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualPks.add(rs.getInt(1));
          }
        }
      }

      assertEquals("Should return " + limit + " rows", limit, actualPks.size());
      assertEquals("Double vectors cross-region merge top-" + limit + " must match brute-force",
        expectedPks, actualPks);
    }
  }

  private static double computeDist(String metric, float[] a, float[] b) {
    double dot = 0, normA = 0, normB = 0, sumSq = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sumSq += diff * diff;
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    switch (metric) {
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
        throw new IllegalArgumentException(metric);
    }
  }
}
