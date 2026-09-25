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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.query.QueryServices;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration tests for cost model and plan ordering for vector index plans. */
@Category(ParallelStatsEnabledTest.class)
public class VectorIvfCostIT extends ParallelStatsEnabledIT {

  private static String tableName;
  private static String indexCoveringCategory;
  private static List<float[]> l2Centroids;
  private static final Float[] queryVectorBoxed = new Float[] { 0.0f, 0.0f, 0.0f, 0.0f };

  private static Connection getCostBasedConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty(QueryServices.COST_BASED_OPTIMIZER_ENABLED, "true");
    return DriverManager.getConnection(getUrl(), props);
  }

  @BeforeClass
  public static synchronized void doTableSetup() throws Exception {
    tableName = generateUniqueName();
    indexCoveringCategory = generateUniqueName();

    try (Connection conn = getCostBasedConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR, DESCRIPTION VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + indexCoveringCategory + " ON " + tableName + " (V) "
          + "INCLUDE (CATEGORY) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      l2Centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 100.0f, 0.0f, 0.0f, 0.0f }, new float[] { 200.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 300.0f, 0.0f, 0.0f, 0.0f });

      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexCoveringCategory,
        l2Centroids, 1L);

      Random rng = new Random(42);
      String upsert =
        "UPSERT INTO " + tableName + " (ID, V, CATEGORY, DESCRIPTION) VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        for (int k = 0; k < 4; k++) {
          float[] center = l2Centroids.get(k);
          for (int i = 0; i < 1000; i++) {
            String id = String.format("k%d_r%04d", k, i);
            float[] v = new float[] { center[0] + (float) (rng.nextGaussian() * 0.1),
              center[1] + (float) (rng.nextGaussian() * 0.1),
              center[2] + (float) (rng.nextGaussian() * 0.1),
              center[3] + (float) (rng.nextGaussian() * 0.1) };
            String cat = (i % 2 == 0) ? "catA" : "catB";
            String desc = "desc_" + id;
            ps.setString(1, id);
            ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.setString(3, cat);
            ps.setString(4, desc);
            ps.addBatch();
            if (i % 250 == 0) {
              ps.executeBatch();
            }
          }
          ps.executeBatch();
        }
        conn.commit();
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + tableName);
        stmt.execute("UPDATE STATISTICS " + indexCoveringCategory);
      }
    }
  }

  @Test
  public void testProbeFractionAppliedOnce() throws Exception {
    try (Connection conn = getCostBasedConnection()) {
      String query1 = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      String query4 = "SELECT /*+ VECTOR_PROBE_COUNT(4) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      String queryExact =
        "SELECT /*+ NO_INDEX */ ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      Cost cost1;
      try (PreparedStatement ps1 = conn.prepareStatement(query1)) {
        ps1.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps1 = ps1.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan1 = pps1.optimizeQuery();
        assertTrue("plan1 should be VectorIndexScanPlan", plan1 instanceof VectorIndexScanPlan);
        cost1 = plan1.getCost();
      }

      Cost cost4;
      try (PreparedStatement ps4 = conn.prepareStatement(query4)) {
        ps4.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps4 = ps4.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan4 = pps4.optimizeQuery();
        assertTrue("plan4 should be VectorIndexScanPlan", plan4 instanceof VectorIndexScanPlan);
        cost4 = plan4.getCost();
      }

      Cost exactCost;
      try (PreparedStatement psExact = conn.prepareStatement(queryExact)) {
        psExact.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement ppsExact = psExact.unwrap(PhoenixPreparedStatement.class);
        QueryPlan planExact = ppsExact.optimizeQuery();
        assertFalse("planExact should not be VectorIndexScanPlan",
          planExact instanceof VectorIndexScanPlan);
        exactCost = planExact.getCost();
      }

      assertFalse("cost1 should be known", cost1.isUnknown());
      assertFalse("cost4 should be known", cost4.isUnknown());
      assertFalse("exactCost should be known", exactCost.isUnknown());

      double ratioProbe = cost1.getIo() / cost4.getIo();
      assertTrue("cost(probe=1)/cost(probe=4) (" + ratioProbe + ") should be within [0.18, 0.35]",
        ratioProbe >= 0.18 && ratioProbe <= 0.35);

      double ratioExact = cost4.getIo() / exactCost.getIo();
      assertTrue("cost(probe=4)/exactCost (" + ratioExact + ") should be within [0.75, 1.25]",
        ratioExact >= 0.75 && ratioExact <= 1.25);
    }
  }

  @Test
  public void testLookupsAreCosted() throws Exception {
    try (Connection conn = getCostBasedConnection()) {
      // Fully covered vector index scan with no base table lookups
      String queryCovered = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, CATEGORY FROM " + tableName
        + " WHERE CATEGORY = 'catA' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      // Vector index scan with deferred projection lookups for top-k rows
      String queryProjLookup = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, DESCRIPTION FROM "
        + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      // Vector index scan with filter-time lookups to evaluate predicates before sorting
      String queryFilterLookup = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, CATEGORY FROM "
        + tableName + " WHERE DESCRIPTION = 'desc_k0_r0001' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      Cost costCovered;
      try (PreparedStatement ps = conn.prepareStatement(queryCovered)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("plan should be VectorIndexScanPlan", plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        assertFalse("Covered query should not have projection lookup",
          vPlan.isProjectionTimeUncoveredLookup());
        assertFalse("Covered query should not have filter lookup",
          vPlan.isFilterTimeUncoveredLookup());
        costCovered = plan.getCost();
      }

      Cost costProj;
      try (PreparedStatement ps = conn.prepareStatement(queryProjLookup)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("plan should be VectorIndexScanPlan", plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        assertTrue("Projection-time query should have projection lookup",
          vPlan.isProjectionTimeUncoveredLookup());
        assertFalse("Projection-time query should not have filter lookup",
          vPlan.isFilterTimeUncoveredLookup());
        costProj = plan.getCost();
      }

      Cost costFilter;
      try (PreparedStatement ps = conn.prepareStatement(queryFilterLookup)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("plan should be VectorIndexScanPlan", plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        assertTrue("Filter-time query should have filter lookup",
          vPlan.isFilterTimeUncoveredLookup());
        costFilter = plan.getCost();
      }

      assertFalse("costCovered should be known", costCovered.isUnknown());
      assertFalse("costProj should be known", costProj.isUnknown());
      assertFalse("costFilter should be known", costFilter.isUnknown());

      assertTrue(
        "cost(covered) [" + costCovered.getIo()
          + "] should be strictly less than cost(projectionLookup) [" + costProj.getIo() + "]",
        costCovered.getIo() < costProj.getIo());
      assertTrue(
        "cost(projectionLookup) [" + costProj.getIo()
          + "] should be strictly less than cost(filterLookup) [" + costFilter.getIo() + "]",
        costProj.getIo() < costFilter.getIo());
    }
  }

  @Test
  public void testVectorIndexWithoutStatsNotDiscarded() throws Exception {
    try (Connection conn = getCostBasedConnection()) {
      String dataTable = generateUniqueName();
      String vecIndex = generateUniqueName();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + dataTable
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR)");
      }

      // Seed data table rows before collecting statistics
      Random rng = new Random(42);
      String upsert = "UPSERT INTO " + dataTable + " (ID, V, CATEGORY) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        for (int k = 0; k < 4; k++) {
          float[] center = l2Centroids.get(k);
          for (int i = 0; i < 200; i++) {
            String id = String.format("t3_k%d_r%04d", k, i);
            float[] v = new float[] { center[0] + (float) (rng.nextGaussian() * 0.1),
              center[1] + (float) (rng.nextGaussian() * 0.1),
              center[2] + (float) (rng.nextGaussian() * 0.1),
              center[3] + (float) (rng.nextGaussian() * 0.1) };
            ps.setString(1, id);
            ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.setString(3, "catA");
            ps.addBatch();
          }
          ps.executeBatch();
        }
        conn.commit();
      }

      // Collect statistics on the data table prior to creating the vector index so the index lacks
      // guideposts
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + dataTable);
      }

      // Create and activate the vector index without collecting index statistics
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + vecIndex + " ON " + dataTable + " (V) "
          + "INCLUDE (CATEGORY) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, dataTable, vecIndex, l2Centroids, 1L);

      // Verify the cost-based optimizer derives an index cost from data table statistics instead of
      // rejecting the index
      String query =
        "SELECT ID, CATEGORY FROM " + dataTable + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Optimized plan should be VectorIndexScanPlan despite index having no stats",
          plan instanceof VectorIndexScanPlan);
        assertFalse("Plan cost should not be unknown", plan.getCost().isUnknown());
      }
    }
  }

  @Test
  public void testCostBasedChoiceBetweenTwoEligibleVectorIndexes() throws Exception {
    try (Connection conn = getCostBasedConnection()) {
      String dataTable = generateUniqueName();
      String idxCovering = generateUniqueName();
      String idxNonCovering = generateUniqueName();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + dataTable
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), DESCRIPTION VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + idxCovering + " ON " + dataTable + " (V) "
          + "INCLUDE (DESCRIPTION) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
        stmt.execute("CREATE VECTOR INDEX " + idxNonCovering + " ON " + dataTable + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      VectorIndexTestUtil.activateWithKnownCentroids(conn, dataTable, idxCovering, l2Centroids, 1L);
      VectorIndexTestUtil.activateWithKnownCentroids(conn, dataTable, idxNonCovering, l2Centroids,
        1L);

      Random rng = new Random(42);
      String upsert = "UPSERT INTO " + dataTable + " (ID, V, DESCRIPTION) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        for (int k = 0; k < 4; k++) {
          float[] center = l2Centroids.get(k);
          for (int i = 0; i < 3; i++) {
            String id = String.format("t4_k%d_r%04d", k, i);
            float[] v = new float[] { center[0] + (float) (rng.nextGaussian() * 0.1),
              center[1] + (float) (rng.nextGaussian() * 0.1),
              center[2] + (float) (rng.nextGaussian() * 0.1),
              center[3] + (float) (rng.nextGaussian() * 0.1) };
            ps.setString(1, id);
            ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.setString(3, "desc_" + id);
            ps.addBatch();
          }
          ps.executeBatch();
        }
        conn.commit();
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + dataTable);
        stmt.execute("UPDATE STATISTICS " + idxCovering);
        stmt.execute("UPDATE STATISTICS " + idxNonCovering);
      }

      String defaultQuery =
        "SELECT ID, DESCRIPTION FROM " + dataTable + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(defaultQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Optimized plan should be VectorIndexScanPlan",
          plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        assertEquals(idxCovering, vPlan.getTableRef().getTable().getTableName().getString());
        assertFalse("Covering index should not have projection-time lookup",
          vPlan.isProjectionTimeUncoveredLookup());
      }

      String hintedQuery = "SELECT /*+ INDEX(" + dataTable + " " + idxNonCovering
        + ") */ ID, DESCRIPTION FROM " + dataTable + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(hintedQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", queryVectorBoxed));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Optimized plan should be VectorIndexScanPlan",
          plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        assertEquals(idxNonCovering, vPlan.getTableRef().getTable().getTableName().getString());
        assertTrue("Non-covering index should have projection-time lookup",
          vPlan.isProjectionTimeUncoveredLookup());
      }
    }
  }
}
