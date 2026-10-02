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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Read only query path integration tests for vector indexes. */
@Category(ParallelStatsDisabledTest.class)
public class VectorIvfQueryIT extends ParallelStatsDisabledIT {

  // Fixture (1): L2 clustered, INCLUDE (CATEGORY), with DESCRIPTION
  private static VectorIndexTestUtil.ClusteredFixture fixL2Covered;

  // Fixture (2): L2 clustered, no INCLUDE, with CATEGORY, DESCRIPTION, V2 VECTOR(FLOAT,3)
  private static VectorIndexTestUtil.ClusteredFixture fixL2Uncovered;

  // Fixture (3): Hand-placed 6-row probe fixture
  private static VectorIndexTestUtil.HandPlacedFixture fixProbe;

  // Fixture (4): COSINE index
  private static String cosineTable;
  private static String cosineIndex;
  private static List<float[]> cosineCentroids;
  private static Map<String, float[]> cosineRows;

  // Fixture (5): INNER_PRODUCT index
  private static String ipTable;
  private static String ipIndex;
  private static List<float[]> ipCentroids;
  private static Map<String, float[]> ipRows;

  // Fixture (6): Salted hand-placed
  private static VectorIndexTestUtil.HandPlacedFixture fixSaltedProbe;

  // Fixture (7): Multi-tenant hand-placed
  private static String mtTable;
  private static String mtIndex;
  private static List<float[]> mtCentroids;
  private static Map<String, float[]> mtT1Rows;
  private static Map<String, float[]> mtT2Rows;

  // Multi-tenant with salt buckets
  private static String mtSaltTable;
  private static String mtSaltIndex;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // (1) L2 clustered, INCLUDE (CATEGORY), with DESCRIPTION
      String t1 = generateUniqueName();
      String idx1 = generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + t1
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR, DESCRIPTION VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + idx1 + " ON " + t1 + " (V) "
          + "INCLUDE (CATEGORY) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      List<float[]> l2Centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 50.0f, 0.0f, 0.0f, 0.0f }, new float[] { 100.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 150.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t1, idx1, l2Centroids, 1L);

      Random rng = new Random(42);
      Map<String, float[]> r1 = new LinkedHashMap<>();
      Map<String, String> cat1 = new LinkedHashMap<>();
      Map<String, String> desc1 = new LinkedHashMap<>();
      String upsert1 = "UPSERT INTO " + t1 + " (ID, V, CATEGORY, DESCRIPTION) VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert1)) {
        for (int k = 0; k < 4; k++) {
          float[] center = l2Centroids.get(k);
          for (int i = 0; i < 20; i++) {
            String id = String.format("k%d_r%02d", k, i);
            float[] v = new float[] { center[0] + (float) (rng.nextGaussian() * 0.5),
              center[1] + (float) (rng.nextGaussian() * 0.5),
              center[2] + (float) (rng.nextGaussian() * 0.5),
              center[3] + (float) (rng.nextGaussian() * 0.5) };
            String cat = (i % 2 == 0) ? "science" : "art";
            String desc = "desc_" + id;
            r1.put(id, v);
            cat1.put(id, cat);
            desc1.put(id, desc);
            ps.setString(1, id);
            ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.setString(3, cat);
            ps.setString(4, desc);
            ps.executeUpdate();
          }
        }
        conn.commit();
      }
      fixL2Covered =
        new VectorIndexTestUtil.ClusteredFixture(t1, idx1, l2Centroids, r1, cat1, desc1, null);

      // (2) L2 clustered, no INCLUDE, with CATEGORY, DESCRIPTION, V2 VECTOR(FLOAT,3)
      String t2 = generateUniqueName();
      String idx2 = generateUniqueName();
      fixL2Uncovered = VectorIndexTestUtil.buildClusteredL2Fixture(conn, t2, idx2, false, true);

      // (3) Hand-placed 6-row probe fixture
      String t3 = generateUniqueName();
      String idx3 = generateUniqueName();
      fixProbe = VectorIndexTestUtil.buildProbeFixture(conn, t3, idx3, null, false);

      // (4) COSINE index
      cosineTable = generateUniqueName();
      cosineIndex = generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + cosineTable + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + cosineIndex + " ON " + cosineTable + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'COSINE', lists = 4, sample_size = 100)");
      }
      cosineCentroids = Arrays.asList(new float[] { 1.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 0.0f, 1.0f, 0.0f, 0.0f }, new float[] { 0.0f, 0.0f, 1.0f, 0.0f },
        new float[] { 0.0f, 0.0f, 0.0f, 1.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, cosineTable, cosineIndex,
        cosineCentroids, 1L);

      cosineRows = new LinkedHashMap<>();
      String upsertCos = "UPSERT INTO " + cosineTable + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertCos)) {
        for (int k = 0; k < 4; k++) {
          float[] c = cosineCentroids.get(k);
          for (int i = 0; i < 20; i++) {
            String id = String.format("cos_k%d_r%02d", k, i);
            float[] v = new float[] { c[0] + 0.05f * (i + 1), c[1] + 0.02f * (i % 3),
              c[2] + 0.02f * (i % 5), c[3] + 0.02f * (i % 7) };
            cosineRows.put(id, v);
            ps.setString(1, id);
            ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.executeUpdate();
          }
        }
        conn.commit();
      }

      // (5) INNER_PRODUCT index
      ipTable = generateUniqueName();
      ipIndex = generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + ipTable + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + ipIndex + " ON " + ipTable + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'INNER_PRODUCT', lists = 4, sample_size = 100)");
      }
      ipCentroids = Arrays.asList(new float[] { 10.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 0.0f, 10.0f, 0.0f, 0.0f }, new float[] { 0.0f, 0.0f, 10.0f, 0.0f },
        new float[] { 0.0f, 0.0f, 0.0f, 10.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, ipTable, ipIndex, ipCentroids, 1L);

      ipRows = new LinkedHashMap<>();
      String upsertIp = "UPSERT INTO " + ipTable + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertIp)) {
        for (int k = 0; k < 4; k++) {
          float[] c = ipCentroids.get(k);
          for (int i = 0; i < 20; i++) {
            String id = String.format("ip_k%d_r%02d", k, i);
            float[] v = new float[] { c[0] + 0.1f * (i + 1), c[1] + 0.05f * (i % 3),
              c[2] + 0.05f * (i % 5), c[3] + 0.05f * (i % 7) };
            ipRows.put(id, v);
            ps.setString(1, id);
            ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.executeUpdate();
          }
        }
        conn.commit();
      }

      // (6) Salted hand-placed
      String t6 = generateUniqueName();
      String idx6 = generateUniqueName();
      fixSaltedProbe = VectorIndexTestUtil.buildProbeFixture(conn, t6, idx6, 4, false);

      // (7) Multi-tenant hand-placed
      mtTable = generateUniqueName();
      mtIndex = generateUniqueName();
      VectorIndexTestUtil.HandPlacedFixture mtFixture =
        VectorIndexTestUtil.buildProbeFixture(conn, mtTable, mtIndex, null, true);
      mtCentroids = mtFixture.centroids;

      mtT1Rows = new LinkedHashMap<>();
      mtT2Rows = new LinkedHashMap<>();
      for (Map.Entry<String, float[]> entry : mtFixture.rows.entrySet()) {
        mtT1Rows.put(entry.getKey(), entry.getValue());
        float[] v2 = new float[] { entry.getValue()[0] + 0.5f, 0.0f, 0.0f, 0.0f };
        mtT2Rows.put(entry.getKey(), v2);
      }

      // Insert for Tenant T1
      try (Connection t1Conn = getTenantConnection("T1")) {
        String upsertT = "UPSERT INTO " + mtTable + " (ID, V) VALUES (?, ?)";
        try (PreparedStatement ps = t1Conn.prepareStatement(upsertT)) {
          for (Map.Entry<String, float[]> entry : mtT1Rows.entrySet()) {
            ps.setString(1, entry.getKey());
            float[] v = entry.getValue();
            ps.setArray(2, t1Conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.executeUpdate();
          }
          t1Conn.commit();
        }
      }

      // Insert for Tenant T2
      try (Connection t2Conn = getTenantConnection("T2")) {
        String upsertT = "UPSERT INTO " + mtTable + " (ID, V) VALUES (?, ?)";
        try (PreparedStatement ps = t2Conn.prepareStatement(upsertT)) {
          for (Map.Entry<String, float[]> entry : mtT2Rows.entrySet()) {
            ps.setString(1, entry.getKey());
            float[] v = entry.getValue();
            ps.setArray(2, t2Conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.executeUpdate();
          }
          t2Conn.commit();
        }
      }

      // Multi-tenant with SALT_BUCKETS=3
      mtSaltTable = generateUniqueName();
      mtSaltIndex = generateUniqueName();
      VectorIndexTestUtil.buildProbeFixture(conn, mtSaltTable, mtSaltIndex, 3, true);
      try (Connection t1Conn = getTenantConnection("T1")) {
        String upsertT = "UPSERT INTO " + mtSaltTable + " (ID, V) VALUES (?, ?)";
        try (PreparedStatement ps = t1Conn.prepareStatement(upsertT)) {
          for (Map.Entry<String, float[]> entry : mtT1Rows.entrySet()) {
            ps.setString(1, entry.getKey());
            float[] v = entry.getValue();
            ps.setArray(2, t1Conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.executeUpdate();
          }
          t1Conn.commit();
        }
      }
    }
  }

  private static Connection getTenantConnection(String tenantId) throws Exception {
    Properties props = new Properties();
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    return DriverManager.getConnection(getUrl(), props);
  }

  @Before
  public void rePrimeCentroidCache() {
    // Re-prime VectorCentroidCache in-memory for order independence across test runs
    VectorCentroidCache cache = VectorCentroidCache.getInstance();
    cache.putFloatCentroids(fixL2Covered.indexName, 1L, fixL2Covered.centroids);
    cache.putFloatCentroids(fixL2Uncovered.indexName, 1L, fixL2Uncovered.centroids);
    cache.putFloatCentroids(fixProbe.indexName, 1L, fixProbe.centroids);
    cache.putFloatCentroids(cosineIndex, 1L, cosineCentroids);
    cache.putFloatCentroids(ipIndex, 1L, ipCentroids);
    cache.putFloatCentroids(fixSaltedProbe.indexName, 1L, fixSaltedProbe.centroids);
    cache.putFloatCentroids(mtIndex, 1L, mtCentroids);
    cache.putFloatCentroids(mtSaltIndex, 1L, mtCentroids);
  }

  @Test
  public void testIvfProbeRestrictionIsReal() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(fixProbe.indexName);

      // Verify raw index row keys match expected centroid assignments.
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(6, rowKeys.size());
      for (byte[] rk : rowKeys) {
        int cid = VectorIndexTestUtil.extractCentroidId(rk, false);
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        if (id.startsWith("A")) {
          assertEquals("A-rows must be in centroid 0", 0, cid);
        } else {
          assertEquals("B-rows must be in centroid 1", 1, cid);
        }
      }

      // Single-probe query restricts scan to centroid 0 posting list.
      Float[] qVec = new Float[] { fixProbe.queryVector[0], fixProbe.queryVector[1],
        fixProbe.queryVector[2], fixProbe.queryVector[3] };
      String probe1Sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + fixProbe.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + probe1Sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("EXPLAIN must contain CLIENT PROBING 1 OF 2 CENTROIDS: " + plan,
          plan.contains("CLIENT PROBING 1 OF 2 CENTROIDS"));
      }

      List<String> actualProbe1 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(probe1Sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualProbe1.add(rs.getString(1));
          }
        }
      }
      assertEquals("Probe=1 must return only A-rows [A1, A2]", Arrays.asList("A1", "A2"),
        actualProbe1);

      // Two-probe query expands scan across both centroid posting lists.
      String probe2Sql = "SELECT /*+ VECTOR_PROBE_COUNT(2) */ ID FROM " + fixProbe.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + probe2Sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("EXPLAIN must contain CLIENT PROBING 2 OF 2 CENTROIDS: " + plan,
          plan.contains("CLIENT PROBING 2 OF 2 CENTROIDS"));
      }

      List<String> actualProbe2 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(probe2Sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualProbe2.add(rs.getString(1));
          }
        }
      }
      assertEquals("Probe=2 must return global exact top-2 [A1, B1]", Arrays.asList("A1", "B1"),
        actualProbe2);

      // Verify session property configuration controls probe count when hints are absent.
      Properties sessionProps = new Properties();
      sessionProps.setProperty("phoenix.vector.probe.count", "1");
      try (Connection conn2 = DriverManager.getConnection(getUrl(), sessionProps)) {
        String noHintSql =
          "SELECT ID FROM " + fixProbe.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
        List<String> actualSession = new ArrayList<>();
        try (PreparedStatement ps = conn2.prepareStatement(noHintSql)) {
          ps.setArray(1, conn2.createArrayOf("FLOAT", qVec));
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              actualSession.add(rs.getString(1));
            }
          }
        }
        assertEquals("Session property probe=1 must return [A1, A2]", Arrays.asList("A1", "A2"),
          actualSession);
      }

      // Base table scan without index returns exact distance ordering.
      String noIndexSql = "SELECT /*+ NO_INDEX */ ID FROM " + fixProbe.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> actualNoIndex = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(noIndexSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualNoIndex.add(rs.getString(1));
          }
        }
      }
      assertEquals("NO_INDEX exact search must return [A1, B1]", Arrays.asList("A1", "B1"),
        actualNoIndex);
    }
  }

  @Test
  public void testIvfRecallOnClusteredData() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Query near cluster 2 center: (100.0, 0, 0, 0)
      float[] queryVector = new float[] { 100.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQuery =
        new Float[] { queryVector[0], queryVector[1], queryVector[2], queryVector[3] };

      List<String> oracleTop5 =
        VectorIndexTestUtil.bruteForceTopK(fixL2Covered.rows, queryVector, "L2", 5);

      int[] probeCounts = { 1, 3, 4 }; // lists = 4 on fixture 1
      for (int probe : probeCounts) {
        String sql = "SELECT /*+ VECTOR_PROBE_COUNT(" + probe + ") */ ID FROM "
          + fixL2Covered.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

        try (PhoenixPreparedStatement pps =
          conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class)) {
          pps.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
          VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
          assertEquals(probe, plan.getProbeCount());
          if (probe == 1) {
            assertEquals("Probe=1 must select centroid 2", Arrays.asList(2),
              plan.getProbeCentroids());
          }
        }

        List<String> actualIds = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setArray(1, conn.createArrayOf("FLOAT", boxedQuery));
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              actualIds.add(rs.getString(1));
            }
          }
        }
        assertEquals("Probe=" + probe + " must achieve 100% recall on well-separated clusters",
          oracleTop5, actualIds);
      }
    }
  }

  @Test
  public void testCoveredIndexNoLookup() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      // Reference nearest neighbors within science category
      Map<String, float[]> scienceRows = new HashMap<>();
      for (Map.Entry<String, float[]> e : fixL2Covered.rows.entrySet()) {
        if ("science".equals(fixL2Covered.categories.get(e.getKey()))) {
          scienceRows.put(e.getKey(), e.getValue());
        }
      }
      List<String> expectedScienceTop5 =
        VectorIndexTestUtil.bruteForceTopK(scienceRows, q, "L2", 5);

      // Unfiltered oracle: assert it contains at least one 'art' row
      List<String> unfilteredTop5 =
        VectorIndexTestUtil.bruteForceTopK(fixL2Covered.rows, q, "L2", 5);
      boolean hasArt = false;
      for (String id : unfilteredTop5) {
        if ("art".equals(fixL2Covered.categories.get(id))) {
          hasArt = true;
          break;
        }
      }
      assertTrue("Unfiltered top-5 must contain at least one excluded 'art' row", hasArt);

      String query = "SELECT ID, CATEGORY FROM " + fixL2Covered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
            assertEquals("science", rs.getString(2));
          }
        }
      }
      assertEquals(5, actualIds.size());
      assertEquals("Covered query must match brute-force science top-5 in exact order",
        expectedScienceTop5, actualIds);
    }
  }

  @Test
  public void testUncoveredFilterColumnLookup() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      Map<String, float[]> scienceRows = new HashMap<>();
      for (Map.Entry<String, float[]> e : fixL2Uncovered.rows.entrySet()) {
        if ("science".equals(fixL2Uncovered.categories.get(e.getKey()))) {
          scienceRows.put(e.getKey(), e.getValue());
        }
      }
      List<String> expectedScienceTop5 =
        VectorIndexTestUtil.bruteForceTopK(scienceRows, q, "L2", 5);

      List<String> unfilteredTop5 =
        VectorIndexTestUtil.bruteForceTopK(fixL2Uncovered.rows, q, "L2", 5);
      boolean hasArt = false;
      for (String id : unfilteredTop5) {
        if ("art".equals(fixL2Uncovered.categories.get(id))) {
          hasArt = true;
          break;
        }
      }
      assertTrue("Unfiltered top-5 must contain at least one excluded 'art' row", hasArt);

      String query = "SELECT ID, CATEGORY FROM " + fixL2Uncovered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertTrue("Uncovered filter must set filterTimeUncoveredLookup",
          plan.isFilterTimeUncoveredLookup());
        assertFalse("Uncovered filter must not set projectionTimeUncoveredLookup",
          plan.isProjectionTimeUncoveredLookup());
      }

      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
            assertEquals("science", rs.getString(2));
          }
        }
      }
      assertEquals(5, actualIds.size());
      assertEquals(expectedScienceTop5, actualIds);
    }
  }

  @Test
  public void testDeferredProjection() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      List<String> expectedTop5 =
        VectorIndexTestUtil.bruteForceTopK(fixL2Uncovered.rows, q, "L2", 5);

      String query = "SELECT ID, DESCRIPTION FROM " + fixL2Uncovered.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      // PreparedStatement execution through PhoenixStatement
      List<String> actualIds = new ArrayList<>();
      try (PhoenixPreparedStatement ps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            actualIds.add(id);
            assertEquals("Description mismatch for " + id, fixL2Uncovered.descriptions.get(id),
              rs.getString(2));
          }
        }
        VectorIndexScanPlan plan = (VectorIndexScanPlan) ps.getQueryPlan();
        assertNotNull(plan);
        assertTrue(plan.isProjectionTimeUncoveredLookup());
        assertFalse(plan.isFilterTimeUncoveredLookup());
        assertEquals("Deferred lookups must occur only for final 5 rows", 5,
          plan.getLastDeferredLookupCount());
      }
      assertEquals(expectedTop5, actualIds);
    }
  }

  @Test
  public void testUncoveredFilterAndUncoveredProjection() throws Exception {
    // Evaluate query with uncovered filter and projection columns.
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      Map<String, float[]> scienceRows = new HashMap<>();
      for (Map.Entry<String, float[]> e : fixL2Uncovered.rows.entrySet()) {
        if ("science".equals(fixL2Uncovered.categories.get(e.getKey()))) {
          scienceRows.put(e.getKey(), e.getValue());
        }
      }
      List<String> expectedScienceTop5 =
        VectorIndexTestUtil.bruteForceTopK(scienceRows, q, "L2", 5);

      String query = "SELECT ID, DESCRIPTION FROM " + fixL2Uncovered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("EXPLAIN must show SERVER MERGE for uncovered filter: " + plan,
          plan.contains("SERVER MERGE"));
      }

      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertTrue("filterTime must be true", plan.isFilterTimeUncoveredLookup());
        assertFalse("projectionTime must be false when filterTime is already active",
          plan.isProjectionTimeUncoveredLookup());
      }

      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            actualIds.add(id);
            assertEquals("Description mismatch for " + id, fixL2Uncovered.descriptions.get(id),
              rs.getString(2));
          }
        }
      }
      assertEquals(expectedScienceTop5, actualIds);
    }
  }

  @Test
  public void testDeferredProjectionWithCoveredFilter() throws Exception {
    // Evaluate query with covered filter column and uncovered projection column.
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      Map<String, float[]> scienceRows = new HashMap<>();
      for (Map.Entry<String, float[]> e : fixL2Covered.rows.entrySet()) {
        if ("science".equals(fixL2Covered.categories.get(e.getKey()))) {
          scienceRows.put(e.getKey(), e.getValue());
        }
      }
      List<String> expectedScienceTop5 =
        VectorIndexTestUtil.bruteForceTopK(scienceRows, q, "L2", 5);

      String query = "SELECT ID, DESCRIPTION FROM " + fixL2Covered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse("EXPLAIN must not contain SERVER MERGE when filter is covered: " + plan,
          plan.contains("SERVER MERGE"));
      }

      List<String> actualIds = new ArrayList<>();
      try (PhoenixPreparedStatement ps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            actualIds.add(id);
            assertEquals(fixL2Covered.descriptions.get(id), rs.getString(2));
          }
        }
        VectorIndexScanPlan plan = (VectorIndexScanPlan) ps.getQueryPlan();
        assertNotNull(plan);
        assertFalse("filterTime must be false for covered filter",
          plan.isFilterTimeUncoveredLookup());
        assertTrue("projectionTime must be true for uncovered description",
          plan.isProjectionTimeUncoveredLookup());
        assertEquals("Deferred lookup count must be 5", 5, plan.getLastDeferredLookupCount());
      }
      assertEquals(expectedScienceTop5, actualIds);
    }
  }

  @Test
  public void testDeferredProjectionSelectStar() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      List<String> expectedTop5 =
        VectorIndexTestUtil.bruteForceTopK(fixL2Uncovered.rows, q, "L2", 5);

      String query =
        "SELECT * FROM " + fixL2Uncovered.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertTrue("SELECT * with uncovered columns must use deferred projection",
          plan.isProjectionTimeUncoveredLookup());
      }

      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            actualIds.add(id);
            // Verify V
            Object vObj = rs.getObject(2);
            assertNotNull(vObj);
            assertTrue(vObj instanceof float[]);
            float[] actualV = (float[]) vObj;
            float[] expectedV = fixL2Uncovered.rows.get(id);
            for (int d = 0; d < 4; d++) {
              assertEquals(expectedV[d], actualV[d], 1e-5f);
            }
            // Verify CATEGORY
            assertEquals(fixL2Uncovered.categories.get(id), rs.getString(3));
            // Verify DESCRIPTION
            assertEquals(fixL2Uncovered.descriptions.get(id), rs.getString(4));
            // Verify V2
            Object v2Obj = rs.getObject(5);
            assertNotNull(v2Obj);
            assertTrue(v2Obj instanceof float[]);
            float[] actualV2 = (float[]) v2Obj;
            float[] expectedV2 = fixL2Uncovered.v2Rows.get(id);
            for (int d = 0; d < 3; d++) {
              assertEquals(expectedV2[d], actualV2[d], 1e-5f);
            }
          }
        }
      }
      assertEquals(expectedTop5, actualIds);
    }
  }

  @Test
  public void testSecondVectorColumnNotCovered() throws Exception {
    // V2 is a VECTOR(FLOAT,3) in fixL2Uncovered that is not in the index definition
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      List<String> expectedTop5 =
        VectorIndexTestUtil.bruteForceTopK(fixL2Uncovered.rows, q, "L2", 5);

      String query =
        "SELECT ID, V2 FROM " + fixL2Uncovered.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertTrue("Second vector column must trigger deferred projection",
          plan.isProjectionTimeUncoveredLookup());
      }

      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            actualIds.add(id);
            Object v2Obj = rs.getObject(2);
            assertNotNull("V2 must not be null via deferred projection", v2Obj);
            assertTrue(v2Obj instanceof float[]);
            float[] actualV2 = (float[]) v2Obj;
            float[] expectedV2 = fixL2Uncovered.v2Rows.get(id);
            for (int d = 0; d < 3; d++) {
              assertEquals(expectedV2[d], actualV2[d], 1e-5f);
            }
          }
        }
      }
      assertEquals(expectedTop5, actualIds);
    }
  }

  @Test
  public void testDeferredProjectionRowDeletedBetweenScanAndLookup() throws Exception {
    String t = "T_VEC_DEL_R_" + generateUniqueName();
    String idx = "IDX_VEC_DEL_R_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.ClusteredFixture fix =
        VectorIndexTestUtil.buildClusteredL2Fixture(conn, t, idx, false, true);

      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };
      List<String> top5 = VectorIndexTestUtil.bruteForceTopK(fix.rows, q, "L2", 5);

      // Delete one of the top-5 rows directly in HBase base table (leave index intact)
      String deletedId = top5.get(0);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable dataTable = pconn.getTableNoCache(t);
      try (
        Table hTable = pconn.getQueryServices().getTable(dataTable.getPhysicalName().getBytes())) {
        hTable.delete(new Delete(Bytes.toBytes(deletedId)));
      }

      String query = "SELECT ID, DESCRIPTION FROM " + t + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
          }
        }
      }
      // Verify DeferredProjectionResultIterator skips index entries when corresponding data table
      // rows are missing.
      assertEquals(4, actualIds.size());
      assertFalse("Deleted row must be skipped without exception", actualIds.contains(deletedId));
      assertEquals(top5.subList(1, 5), actualIds);
    }
  }

  @Test
  public void testNonVectorQueryDoesNotUseVectorIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Non-vector query on covered column
      String query1 = "SELECT ID FROM " + fixL2Covered.tableName + " WHERE CATEGORY = 'science'";
      ResultSet rs1 = conn.createStatement().executeQuery("EXPLAIN " + query1);
      String plan1 = QueryUtil.getExplainPlan(rs1);
      assertFalse("Non-vector query must not use vector index: " + plan1,
        plan1.contains(fixL2Covered.indexName));
      assertFalse("Non-vector query must not probe centroids: " + plan1,
        plan1.contains("CLIENT PROBING"));

      // Aggregate query without distance ordering
      String query2 = "SELECT COUNT(*) FROM " + fixL2Covered.tableName;
      ResultSet rs2 = conn.createStatement().executeQuery("EXPLAIN " + query2);
      String plan2 = QueryUtil.getExplainPlan(rs2);
      assertFalse("COUNT(*) must not use vector index: " + plan2,
        plan2.contains(fixL2Covered.indexName));
    }
  }

  @Test
  public void testMetricMismatchFallsBackToExactScan() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      // Query L2 index with COSINE_DISTANCE
      String cosQuery =
        "SELECT ID FROM " + fixL2Covered.tableName + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + cosQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse("Cosine query on L2 index must not probe centroids: " + plan,
          plan.contains("CLIENT PROBING"));
        assertFalse("Cosine query on L2 index must not reference index table: " + plan,
          plan.contains(fixL2Covered.indexName));
      }

      List<String> expectedCos =
        VectorIndexTestUtil.bruteForceTopK(fixL2Covered.rows, q, "COSINE", 5);
      List<String> actualCos = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(cosQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualCos.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedCos, actualCos);

      // Repeat with INNER_PRODUCT
      String ipQuery =
        "SELECT ID FROM " + fixL2Covered.tableName + " ORDER BY INNER_PRODUCT(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + ipQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse("Inner product query on L2 index must not probe centroids: " + plan,
          plan.contains("CLIENT PROBING"));
        assertFalse("Inner product query on L2 index must not reference index table: " + plan,
          plan.contains(fixL2Covered.indexName));
      }

      List<String> expectedIp =
        VectorIndexTestUtil.bruteForceTopK(fixL2Covered.rows, q, "INNER_PRODUCT", 5);
      List<String> actualIp = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(ipQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIp.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedIp, actualIp);
    }
  }

  @Test
  public void testOrderByOnDifferentVectorColumnThanIndexed() throws Exception {
    // When a table has multiple vector columns, an index on one vector column must not be chosen
    // for queries ordering by a different vector column even if distance metrics match.
    String t = generateUniqueName();
    String idx = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + t
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), V2 VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + idx + " ON " + t + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idx,
        Arrays.asList(new float[] { 0f, 0f, 0f, 0f }, new float[] { 10f, 0f, 0f, 0f }), 1L);

      // V clusters A-rows near centroid 0 and B-rows near centroid 1; V2 is the exact opposite
      // for each row, so a wrong plan that probes V's centroids with a V2 query vector returns
      // the wrong rows outright.
      String[] ids = { "A1", "A2", "A3", "B1", "B2", "B3" };
      float[] vx = { 2f, 3f, 4f, 6f, 7f, 8f };
      float[] v2x = { 100f, 101f, 102f, 0f, 1f, 2f };
      Map<String, float[]> v2Rows = new LinkedHashMap<>();
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + t + " (ID, V, V2) VALUES (?, ?, ?)")) {
        for (int i = 0; i < ids.length; i++) {
          ps.setString(1, ids[i]);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { vx[i], 0f, 0f, 0f }));
          ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { v2x[i], 0f, 0f, 0f }));
          v2Rows.put(ids[i], new float[] { v2x[i], 0f, 0f, 0f });
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] q = new float[] { 0f, 0f, 0f, 0f };
      Float[] boxedQ = new Float[] { 0f, 0f, 0f, 0f };
      List<String> expected = VectorIndexTestUtil.bruteForceTopK(v2Rows, q, "L2", 2);

      String sql = "SELECT ID FROM " + t + " ORDER BY L2_DISTANCE(V2, ?) LIMIT 2";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        String plan = QueryUtil.getExplainPlan(ps.executeQuery());
        assertFalse("Index built on V must not be used for an ORDER BY on V2: " + plan,
          plan.contains(idx));
      }

      List<String> actual = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actual.add(rs.getString(1));
          }
        }
      }
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testL2SquaredUsesL2Index() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      String sql =
        "SELECT ID FROM " + fixL2Covered.tableName + " ORDER BY L2_DISTANCE_SQUARED(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("L2 squared must probe centroids on L2 index: " + plan,
          plan.contains("CLIENT PROBING"));
        assertTrue(plan.contains(fixL2Covered.indexName));
      }

      List<String> expectedTop5 =
        VectorIndexTestUtil.bruteForceTopK(fixL2Covered.rows, q, "L2_SQUARED", 5);
      List<String> actualTop5 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualTop5.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedTop5, actualTop5);
    }
  }

  @Test
  public void testOperatorSyntaxUsesIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      // L2 distance operator (<->) on L2 index
      String l2OpSql = "SELECT ID FROM " + fixL2Covered.tableName + " ORDER BY V <-> ? LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + l2OpSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("<-> operator must probe on L2 index: " + plan, plan.contains("CLIENT PROBING"));
        assertTrue(plan.contains(fixL2Covered.indexName));
      }
      List<String> expectedL2 = VectorIndexTestUtil.bruteForceTopK(fixL2Covered.rows, q, "L2", 5);
      List<String> actualL2 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(l2OpSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualL2.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedL2, actualL2);

      // Cosine distance operator (<=>) on COSINE index
      float[] qCos = new float[] { 1.0f, 0.1f, 0.0f, 0.0f };
      Float[] boxedCos = new Float[] { qCos[0], qCos[1], qCos[2], qCos[3] };
      String cosOpSql = "SELECT ID FROM " + cosineTable + " ORDER BY V <=> ? LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + cosOpSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedCos));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("<=> operator must probe on COSINE index: " + plan,
          plan.contains("CLIENT PROBING"));
        assertTrue(plan.contains(cosineIndex));
      }
      List<String> expectedCos = VectorIndexTestUtil.bruteForceTopK(cosineRows, qCos, "COSINE", 5);
      List<String> actualCos = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(cosOpSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedCos));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualCos.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedCos, actualCos);

      // Inner product operator (<#>) on INNER_PRODUCT index
      float[] qIp = new float[] { 10.0f, 0.5f, 0.0f, 0.0f };
      Float[] boxedIp = new Float[] { qIp[0], qIp[1], qIp[2], qIp[3] };
      String ipOpSql = "SELECT ID FROM " + ipTable + " ORDER BY V <#> ? LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + ipOpSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedIp));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("<#> operator must probe on INNER_PRODUCT index: " + plan,
          plan.contains("CLIENT PROBING"));
        assertTrue(plan.contains(ipIndex));
      }
      List<String> expectedIp = VectorIndexTestUtil.bruteForceTopK(ipRows, qIp, "INNER_PRODUCT", 5);
      List<String> actualIp = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(ipOpSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedIp));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIp.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedIp, actualIp);
    }
  }

  @Test
  public void testBuildingIndexNotUsed() throws Exception {
    String t = "T_VEC_BLD_" + generateUniqueName();
    String idx = "IDX_VEC_BLD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt
          .execute("CREATE TABLE " + t + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
        // Empty table -> CREATE VECTOR INDEX leaves index in BUILDING
        stmt.execute("CREATE VECTOR INDEX " + idx + " ON " + t + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      // Upsert rows after index creation
      Map<String, float[]> rows = new LinkedHashMap<>();
      String upsert = "UPSERT INTO " + t + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        for (int i = 0; i < 20; i++) {
          String id = "row_" + i;
          float[] v = new float[] { (float) i, 0.0f, 0.0f, 0.0f };
          rows.put(id, v);
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { (float) i, 0.0f, 0.0f, 0.0f }));
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] q = new float[] { 2.1f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { 2.1f, 0.0f, 0.0f, 0.0f };
      String searchSql = "SELECT ID FROM " + t + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      // BUILDING index must NOT be used
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + searchSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse("BUILDING index must not probe centroids: " + plan,
          plan.contains("CLIENT PROBING"));
        assertFalse("BUILDING index must not be in explain plan: " + plan, plan.contains(idx));
      }

      List<String> expectedTop5 = VectorIndexTestUtil.bruteForceTopK(rows, q, "L2", 5);
      List<String> actualBuilding = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(searchSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualBuilding.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedTop5, actualBuilding);

      // Now activate with known centroids and re-run: EXPLAIN now probes
      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 5.0f, 0.0f, 0.0f, 0.0f }, new float[] { 10.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 15.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idx, centroids, 1L);

      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + searchSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("ACTIVE index must probe centroids: " + plan, plan.contains("CLIENT PROBING"));
        assertTrue(plan.contains(idx));
      }
    }
  }

  @Test
  public void testDescendingOrderDoesNotUseIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      String sql =
        "SELECT ID FROM " + fixL2Covered.tableName + " ORDER BY L2_DISTANCE(V, ?) DESC LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse("DESC ordering must not probe centroids: " + plan,
          plan.contains("CLIENT PROBING"));
        assertFalse(plan.contains(fixL2Covered.indexName));
      }

      // Compute farthest 5
      List<Map.Entry<String, Double>> scored = new ArrayList<>();
      for (Map.Entry<String, float[]> e : fixL2Covered.rows.entrySet()) {
        double d = VectorIndexTestUtil.dist("L2", q, e.getValue());
        scored.add(new java.util.AbstractMap.SimpleEntry<>(e.getKey(), d));
      }
      scored.sort((e1, e2) -> {
        int cmp = Double.compare(e2.getValue(), e1.getValue()); // descending
        if (cmp != 0) return cmp;
        return e1.getKey().compareTo(e2.getKey());
      });
      List<String> expectedFarthest5 = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        expectedFarthest5.add(scored.get(i).getKey());
      }

      List<String> actualFarthest5 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualFarthest5.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedFarthest5, actualFarthest5);
    }
  }

  @Test
  public void testNoLimitDoesNotUseIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      String sql = "SELECT ID FROM " + fixL2Covered.tableName + " ORDER BY L2_DISTANCE(V, ?)";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse("No LIMIT must not probe centroids: " + plan, plan.contains("CLIENT PROBING"));
        assertFalse(plan.contains(fixL2Covered.indexName));
      }

      List<String> actual = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actual.add(rs.getString(1));
          }
        }
      }
      assertEquals(fixL2Covered.rows.size(), actual.size());
      double prevDist = -1.0;
      for (String id : actual) {
        double d = VectorIndexTestUtil.dist("L2", q, fixL2Covered.rows.get(id));
        assertTrue("Results must be in ascending distance order: " + d + " >= " + prevDist,
          d >= prevDist);
        prevDist = d;
      }
    }
  }

  @Test
  public void testExplicitIndexHintForcesVectorIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 2.0f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { 2.0f, 0.0f, 0.0f, 0.0f };
      String hintSql = "SELECT /*+ INDEX(" + fixL2Covered.tableName + " " + fixL2Covered.indexName
        + ") */ ID, CATEGORY FROM " + fixL2Covered.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + hintSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        ResultSet rsExplain = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rsExplain);
        assertTrue("INDEX hint must use index table: " + plan,
          plan.contains(fixL2Covered.indexName));
        assertTrue("INDEX hint must probe centroids: " + plan, plan.contains("CLIENT PROBING"));
      }

      List<String> actual = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(hintSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            String cat = rs.getString(2);
            assertEquals(fixL2Covered.categories.get(id), cat);
            actual.add(id);
          }
        }
      }
      assertEquals(5, actual.size());

      String indexSql = "SELECT \":CENTROID_ID\", \":ID\" FROM " + fixL2Covered.indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(indexSql)) {
        int count = 0;
        while (rs.next()) {
          count++;
          int cid = rs.getInt(1);
          String id = rs.getString(2);
          float[] v = fixL2Covered.rows.get(id);
          assertNotNull(v);
          int expectedCid = VectorIndexTestUtil.nearestCentroid(v, fixL2Covered.centroids, "L2");
          assertEquals(":CENTROID_ID must equal nearest centroid", expectedCid, cid);
        }
        assertEquals(fixL2Covered.rows.size(), count);
      }
    }
  }

  @Test
  public void testIvfCosineEndToEnd() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(cosineIndex);

      // Verify each index row's centroid prefix equals nearestCentroid(v, centroids, "COSINE")
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(cosineRows.size(), rowKeys.size());
      for (byte[] rk : rowKeys) {
        int cid = VectorIndexTestUtil.extractCentroidId(rk, false);
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        float[] v = cosineRows.get(id);
        assertNotNull(v);
        int expectedCid = VectorIndexTestUtil.nearestCentroid(v, cosineCentroids, "COSINE");
        assertEquals("Cosine centroid assignment mismatch for " + id, expectedCid, cid);
      }

      // Query near centroid 0: [1, 0, 0, 0]
      float[] q = new float[] { 1.0f, 0.05f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };
      List<String> oracleTop5 = VectorIndexTestUtil.bruteForceTopK(cosineRows, q, "COSINE", 5);

      // Probe=4: ordered equality with brute force top-5
      String queryProbe4 = "SELECT /*+ VECTOR_PROBE_COUNT(4) */ ID FROM " + cosineTable
        + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT 5";
      List<String> actualProbe4 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(queryProbe4)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualProbe4.add(rs.getString(1));
          }
        }
      }
      assertEquals(oracleTop5, actualProbe4);

      // Probe=1: all results must be in nearest centroid (0)
      String queryProbe1 = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + cosineTable
        + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT 5";
      List<String> actualProbe1 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(queryProbe1)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            actualProbe1.add(id);
            int cid =
              VectorIndexTestUtil.nearestCentroid(cosineRows.get(id), cosineCentroids, "COSINE");
            assertEquals("Probe=1 result must belong to nearest centroid 0", 0, cid);
          }
        }
      }
      assertEquals(5, actualProbe1.size());
    }
  }

  @Test
  public void testIvfInnerProductEndToEnd() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(ipIndex);

      // Verify each index row's centroid prefix equals nearestCentroid(v, centroids,
      // "INNER_PRODUCT")
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(ipRows.size(), rowKeys.size());
      for (byte[] rk : rowKeys) {
        int cid = VectorIndexTestUtil.extractCentroidId(rk, false);
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        float[] v = ipRows.get(id);
        assertNotNull(v);
        int expectedCid = VectorIndexTestUtil.nearestCentroid(v, ipCentroids, "INNER_PRODUCT");
        assertEquals("Inner product centroid assignment mismatch for " + id, expectedCid, cid);
      }

      // Query along axis 0
      float[] q = new float[] { 10.0f, 0.1f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };
      List<String> oracleTop5 = VectorIndexTestUtil.bruteForceTopK(ipRows, q, "INNER_PRODUCT", 5);

      // Probe=4: ordered equality with brute force top-5
      String queryProbe4 = "SELECT /*+ VECTOR_PROBE_COUNT(4) */ ID FROM " + ipTable
        + " ORDER BY INNER_PRODUCT(V, ?) LIMIT 5";
      List<String> actualProbe4 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(queryProbe4)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualProbe4.add(rs.getString(1));
          }
        }
      }
      assertEquals(oracleTop5, actualProbe4);

      // Probe=1: all results must belong to nearest centroid (0)
      String queryProbe1 = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + ipTable
        + " ORDER BY INNER_PRODUCT(V, ?) LIMIT 5";
      List<String> actualProbe1 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(queryProbe1)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String id = rs.getString(1);
            actualProbe1.add(id);
            int cid =
              VectorIndexTestUtil.nearestCentroid(ipRows.get(id), ipCentroids, "INNER_PRODUCT");
            assertEquals("Probe=1 result must belong to nearest centroid 0", 0, cid);
          }
        }
      }
      assertEquals(5, actualProbe1.size());
    }
  }

  @Test
  public void testIvfProbeRestrictionOnSaltedTable() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(fixSaltedProbe.indexName);

      // In raw index scan, assert rows appear under more than 1 salt byte
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(6, rowKeys.size());
      Set<Byte> distinctSalts = new HashSet<>();
      for (byte[] rk : rowKeys) {
        distinctSalts.add(rk[0]);
      }
      assertTrue("Salted table rows must span multiple salt buckets: count=" + distinctSalts.size(),
        distinctSalts.size() > 1);

      // Probe=1 on salted table returns [A1, A2]
      Float[] qVec = new Float[] { fixSaltedProbe.queryVector[0], fixSaltedProbe.queryVector[1],
        fixSaltedProbe.queryVector[2], fixSaltedProbe.queryVector[3] };
      String probe1Sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + fixSaltedProbe.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> actualProbe1 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(probe1Sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualProbe1.add(rs.getString(1));
          }
        }
      }
      assertEquals("Probe=1 on salted table must return [A1, A2]", Arrays.asList("A1", "A2"),
        actualProbe1);

      // Probe=2 on salted table returns [A1, B1]
      String probe2Sql = "SELECT /*+ VECTOR_PROBE_COUNT(2) */ ID FROM " + fixSaltedProbe.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> actualProbe2 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(probe2Sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualProbe2.add(rs.getString(1));
          }
        }
      }
      assertEquals("Probe=2 on salted table must return [A1, B1]", Arrays.asList("A1", "B1"),
        actualProbe2);
    }
  }

  @Test
  public void testIvfMultiTenant() throws Exception {
    Float[] qVec = new Float[] { fixProbe.queryVector[0], fixProbe.queryVector[1],
      fixProbe.queryVector[2], fixProbe.queryVector[3] };

    // Tenant T1 connection
    try (Connection t1Conn = getTenantConnection("T1")) {
      String queryT1 = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + mtTable
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";

      try (PreparedStatement ps = t1Conn.prepareStatement("EXPLAIN " + queryT1)) {
        ps.setArray(1, t1Conn.createArrayOf("FLOAT", qVec));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("T1 EXPLAIN must probe centroids: " + plan, plan.contains("CLIENT PROBING"));
      }

      try (PhoenixPreparedStatement pps =
        t1Conn.prepareStatement(queryT1).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, t1Conn.createArrayOf("FLOAT", qVec));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        byte[] expectedPrefix =
          ByteUtil.concat(Bytes.toBytes("T1"), QueryConstants.SEPARATOR_BYTE_ARRAY);
        for (KeyRange kr : plan.getKeyRanges()) {
          byte[] lower = kr.getLowerRange();
          assertTrue("KeyRange lower bound must start with T1 + 0x00",
            Bytes.startsWith(lower, expectedPrefix));
        }
      }

      List<String> actualT1 = new ArrayList<>();
      try (PreparedStatement ps = t1Conn.prepareStatement(queryT1)) {
        ps.setArray(1, t1Conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualT1.add(rs.getString(1));
          }
        }
      }
      assertEquals("T1 probe=1 must return [A1, A2]", Arrays.asList("A1", "A2"), actualT1);
    }

    // Tenant T2 connection
    try (Connection t2Conn = getTenantConnection("T2")) {
      String queryT2 = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + mtTable
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";

      try (PhoenixPreparedStatement pps =
        t2Conn.prepareStatement(queryT2).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, t2Conn.createArrayOf("FLOAT", qVec));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        byte[] expectedPrefix =
          ByteUtil.concat(Bytes.toBytes("T2"), QueryConstants.SEPARATOR_BYTE_ARRAY);
        for (KeyRange kr : plan.getKeyRanges()) {
          byte[] lower = kr.getLowerRange();
          assertTrue("KeyRange lower bound must start with T2 + 0x00",
            Bytes.startsWith(lower, expectedPrefix));
        }
      }

      List<String> actualT2 = new ArrayList<>();
      try (PreparedStatement ps = t2Conn.prepareStatement(queryT2)) {
        ps.setArray(1, t2Conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualT2.add(rs.getString(1));
          }
        }
      }
      // T2 vectors are shifted by +0.5, so A1(4.5), A2(3.5), A3(2.5). Q(4.9) has nearest in C0 as
      // A1(0.4), A2(1.4)
      assertEquals("T2 probe=1 must return [A1, A2]", Arrays.asList("A1", "A2"), actualT2);
    }

    // Multi-tenant + SALT_BUCKETS=3 (exercises tenantColIndex = 1 branch)
    try (Connection t1Conn = getTenantConnection("T1")) {
      String querySalt = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + mtSaltTable
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> actualSalt = new ArrayList<>();
      try (PreparedStatement ps = t1Conn.prepareStatement(querySalt)) {
        ps.setArray(1, t1Conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualSalt.add(rs.getString(1));
          }
        }
      }
      assertEquals("Salted multi-tenant probe=1 must return [A1, A2]", Arrays.asList("A1", "A2"),
        actualSalt);
    }
  }

  @Test
  public void testGlobalConnectionOnMultiTenantVectorIndex() throws Exception {
    // A global connection querying a multi-tenant vector index lacks a tenant prefix to scope
    // centroid ranges and falls back to a full scan across tenants.
    String t = generateUniqueName();
    String idx = generateUniqueName();
    VectorIndexTestUtil.HandPlacedFixture fixture;
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      fixture = VectorIndexTestUtil.buildProbeFixture(conn, t, idx, null, true);
    }
    try (Connection t1Conn = getTenantConnection("T1")) {
      try (PreparedStatement ps =
        t1Conn.prepareStatement("UPSERT INTO " + t + " (ID, V) VALUES (?, ?)")) {
        for (Map.Entry<String, float[]> e : fixture.rows.entrySet()) {
          float[] v = e.getValue();
          ps.setString(1, e.getKey());
          ps.setArray(2, t1Conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
        t1Conn.commit();
      }
    }

    Float[] qVec = new Float[] { fixture.queryVector[0], fixture.queryVector[1],
      fixture.queryVector[2], fixture.queryVector[3] };
    List<String> expectedGlobalTop2 =
      VectorIndexTestUtil.bruteForceTopK(fixture.rows, fixture.queryVector, "L2", 2);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String sql = "SELECT ID FROM " + t + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> actual = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actual.add(rs.getString(1));
          }
        }
      }
      assertEquals("Global connection on multi-tenant vector index must return correct exact"
        + " top-2 rather than silently matching nothing", expectedGlobalTop2, actual);
    }
  }

  @Test
  public void testOversampleHintExplainPlan() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Float[] qVec = new Float[] { fixProbe.queryVector[0], fixProbe.queryVector[1],
        fixProbe.queryVector[2], fixProbe.queryVector[3] };

      // Two-phase scoring with explicit oversampling factor 5.0.
      String sqlWithHint = "SELECT /*+ OVERSAMPLE(5.0) */ ID FROM " + fixProbe.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 10";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sqlWithHint)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          StringBuilder sb = new StringBuilder();
          while (rs.next()) {
            sb.append(rs.getString(1)).append("\n");
          }
          String plan = sb.toString();
          assertTrue("Plan should contain SERVER RESCORE TOP-10 OF 50 CANDIDATES, but was: " + plan,
            plan.contains("SERVER RESCORE TOP-10 OF 50 CANDIDATES"));
        }
      }

      // Two-phase scoring with default oversampling factor 3.0.
      String sqlDefault =
        "SELECT ID FROM " + fixProbe.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 10";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sqlDefault)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          StringBuilder sb = new StringBuilder();
          while (rs.next()) {
            sb.append(rs.getString(1)).append("\n");
          }
          String plan = sb.toString();
          assertTrue(
            "Plan should contain default SERVER RESCORE TOP-10 OF 30 CANDIDATES, but was: " + plan,
            plan.contains("SERVER RESCORE TOP-10 OF 30 CANDIDATES"));
        }
      }

      // Oversampling factor 1.0 disables two-phase rescore.
      String sqlNoRescore = "SELECT /*+ OVERSAMPLE(1.0) */ ID FROM " + fixProbe.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 10";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sqlNoRescore)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          StringBuilder sb = new StringBuilder();
          while (rs.next()) {
            sb.append(rs.getString(1)).append("\n");
          }
          String plan = sb.toString();
          assertFalse(
            "Plan should NOT contain SERVER RESCORE when oversample is 1.0, but was: " + plan,
            plan.contains("SERVER RESCORE"));
        }
      }
    }
  }

  @Test
  public void testTwoPhaseCorrectnessOnHandPlacedVectors() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Float[] qVec = new Float[] { fixProbe.queryVector[0], fixProbe.queryVector[1],
        fixProbe.queryVector[2], fixProbe.queryVector[3] };

      // Evaluate query in single-phase mode (OVERSAMPLE(1.0)).
      String sqlSingle = "SELECT /*+ OVERSAMPLE(1.0) VECTOR_PROBE_COUNT(2) */ ID FROM "
        + fixProbe.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> resultsSingle = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sqlSingle)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            resultsSingle.add(rs.getString(1));
          }
        }
      }

      // Evaluate query in two-phase mode (OVERSAMPLE(3.0)).
      String sqlTwoPhase = "SELECT /*+ OVERSAMPLE(3.0) VECTOR_PROBE_COUNT(2) */ ID FROM "
        + fixProbe.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> resultsTwoPhase = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sqlTwoPhase)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            resultsTwoPhase.add(rs.getString(1));
          }
        }
      }

      assertEquals("Two-phase should match single-phase exactly on hand-placed vectors",
        resultsSingle, resultsTwoPhase);
    }
  }

  @Test
  public void testTwoPhaseRecallAtLeastSinglePhase() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Float[] qVec = new Float[] { 25.0f, 0.0f, 0.0f, 0.0f };
      float[] q = new float[] { 25.0f, 0.0f, 0.0f, 0.0f };
      int k = 10;
      List<String> groundTruth = VectorIndexTestUtil.bruteForceTopK(fixL2Covered.rows, q, "L2", k);
      Set<String> groundTruthSet = new HashSet<>(groundTruth);

      // Single-phase evaluation with probe=1.
      String singleSql = "SELECT /*+ OVERSAMPLE(1.0) VECTOR_PROBE_COUNT(1) */ ID FROM "
        + fixL2Covered.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT " + k;
      List<String> singlePhaseResults = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(singleSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            singlePhaseResults.add(rs.getString(1));
          }
        }
      }

      // Two-phase evaluation with probe=1.
      String twoPhaseSql = "SELECT /*+ OVERSAMPLE(3.0) VECTOR_PROBE_COUNT(1) */ ID FROM "
        + fixL2Covered.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT " + k;
      List<String> twoPhaseResults = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(twoPhaseSql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", qVec));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            twoPhaseResults.add(rs.getString(1));
          }
        }
      }

      int singlePhaseRecallCount = 0;
      for (String id : singlePhaseResults) {
        if (groundTruthSet.contains(id)) {
          singlePhaseRecallCount++;
        }
      }
      int twoPhaseRecallCount = 0;
      for (String id : twoPhaseResults) {
        if (groundTruthSet.contains(id)) {
          twoPhaseRecallCount++;
        }
      }

      assertTrue(
        "Two-phase recall count (" + twoPhaseRecallCount
          + ") should be >= single-phase recall count (" + singlePhaseRecallCount + ")",
        twoPhaseRecallCount >= singlePhaseRecallCount);
    }
  }

  @Test
  public void testStaticOrderingPrefersCoveringVectorIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String t = generateUniqueName();
      String idxCovering = generateUniqueName();
      String idxNonCovering = generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + t
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR, DESCRIPTION VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + idxCovering + " ON " + t + " (V) "
          + "INCLUDE (DESCRIPTION) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
        stmt.execute("CREATE VECTOR INDEX " + idxNonCovering + " ON " + t + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      List<float[]> l2Centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 50.0f, 0.0f, 0.0f, 0.0f }, new float[] { 100.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 150.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idxCovering, l2Centroids, 1L);
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idxNonCovering, l2Centroids, 1L);

      String query = "SELECT ID, DESCRIPTION FROM " + t + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Expected VectorIndexScanPlan", plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        assertEquals(idxCovering, vPlan.getTableRef().getTable().getTableName().getString());
        assertFalse("Covering index should not do projection-time lookup",
          vPlan.isProjectionTimeUncoveredLookup());
      }
    }
  }

  @Test
  public void testStaticOrderingPrefersVectorIndexOverRegularIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String t = generateUniqueName();
      String regularIdx = generateUniqueName();
      String vectorIdx = generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + t
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR, DESCRIPTION VARCHAR)");
        stmt.execute(
          "CREATE INDEX " + regularIdx + " ON " + t + " (CATEGORY) INCLUDE (V, DESCRIPTION)");
        stmt.execute("CREATE VECTOR INDEX " + vectorIdx + " ON " + t + " (V) "
          + "INCLUDE (CATEGORY, DESCRIPTION) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      List<float[]> l2Centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 50.0f, 0.0f, 0.0f, 0.0f }, new float[] { 100.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 150.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, vectorIdx, l2Centroids, 1L);

      Map<String, float[]> scienceVectors = new LinkedHashMap<>();
      String upsert = "UPSERT INTO " + t + " (ID, V, CATEGORY, DESCRIPTION) VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        for (int i = 0; i < 20; i++) {
          String id = "r" + i;
          float[] v = new float[] { (float) i, 0.0f, 0.0f, 0.0f };
          String cat = (i % 2 == 0) ? "science" : "art";
          if ("science".equals(cat)) {
            scienceVectors.put(id, v);
          }
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.setString(3, cat);
          ps.setString(4, "desc_" + id);
          ps.executeUpdate();
        }
        conn.commit();
      }

      // Verify static ordering favors vector indexes over standard secondary indexes for distance
      // queries
      String defaultQuery =
        "SELECT ID FROM " + t + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(defaultQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 0.0f }));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Default plan should be VectorIndexScanPlan",
          plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        assertEquals(vectorIdx, vPlan.getTableRef().getTable().getTableName().getString());
      }

      // Verify an explicit index hint overrides the default vector index selection
      String hintedQuery = "SELECT /*+ INDEX(" + t + " " + regularIdx + ") */ ID FROM " + t
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      float[] queryVec = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      List<String> expectedGroundTruth =
        VectorIndexTestUtil.bruteForceTopK(scienceVectors, queryVec, "L2", 5);
      try (PreparedStatement ps = conn.prepareStatement(hintedQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 0.0f }));
        PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pps.optimizeQuery();
        assertFalse("Hinted plan should not be VectorIndexScanPlan",
          plan instanceof VectorIndexScanPlan);
        assertEquals(regularIdx, plan.getTableRef().getTable().getTableName().getString());

        List<String> results = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            results.add(rs.getString(1));
          }
        }
        assertEquals(expectedGroundTruth, results);
      }
    }
  }

  @Test
  public void testExplainDistinguishesLookupStrategies() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      // Fully covered vector scan where all referenced columns reside in the index
      String queryCov = "SELECT ID, CATEGORY FROM " + fixL2Covered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + queryCov)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          assertTrue("Covered plan must probe centroids: " + plan, plan.contains("CLIENT PROBING"));
          assertTrue("Covered plan must contain metric (L2): " + plan, plan.contains("(L2)"));
          assertFalse("Covered plan must not contain SERVER MERGE: " + plan,
            plan.contains("SERVER MERGE"));
          assertFalse("Covered plan must not contain CLIENT MERGE [: " + plan,
            plan.contains("CLIENT MERGE ["));
        }
      }

      // Projection-time lookup plan merging uncovered columns on the client for top rows
      String queryProj = "SELECT ID, DESCRIPTION FROM " + fixL2Covered.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + queryProj)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          assertTrue("Projection lookup plan must probe centroids: " + plan,
            plan.contains("CLIENT PROBING"));
          assertTrue("Projection lookup plan must contain metric (L2): " + plan,
            plan.contains("(L2)"));
          assertTrue(
            "Projection lookup plan must contain CLIENT MERGE [0.DESCRIPTION] FOR TOP-5 ROWS: "
              + plan,
            plan.contains("CLIENT MERGE [0.DESCRIPTION] FOR TOP-5 ROWS"));
          assertFalse("Projection lookup plan must not contain SERVER MERGE: " + plan,
            plan.contains("SERVER MERGE"));
        }
      }

      // Filter-time lookup plan joining against the data table before sorting
      String queryFilter = "SELECT ID, DESCRIPTION FROM " + fixL2Uncovered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + queryFilter)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          assertTrue("Filter lookup plan must probe centroids: " + plan,
            plan.contains("CLIENT PROBING"));
          assertTrue("Filter lookup plan must contain metric (L2): " + plan, plan.contains("(L2)"));
          assertTrue("Filter lookup plan must contain SERVER MERGE: " + plan,
            plan.contains("SERVER MERGE"));
          assertFalse("Filter lookup plan must not contain CLIENT MERGE [: " + plan,
            plan.contains("CLIENT MERGE ["));
        }
      }
    }
  }

  @Test
  public void testStructuredExplainAttributes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      float[] q = new float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };

      // Projection-time lookup plan specifying an explicit probe count hint
      String queryProj = "SELECT /*+ VECTOR_PROBE_COUNT(3) */ ID, DESCRIPTION FROM "
        + fixL2Covered.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(queryProj).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Plan must be VectorIndexScanPlan", plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        ExplainPlan explainPlan = vPlan.getExplainPlan();
        ExplainPlanAttributes attrs = explainPlan.getPlanStepsAsAttributes();
        assertNotNull("ExplainPlanAttributes must not be null", attrs);

        assertEquals(Integer.valueOf(3), attrs.getVectorProbeCount());
        assertEquals(Integer.valueOf(4), attrs.getVectorCentroidCount());
        assertEquals("L2", attrs.getVectorDistanceMetric());
        assertNotNull("Client merge columns must not be null", attrs.getClientMergeColumns());
        assertEquals(1, attrs.getClientMergeColumns().size());
        PColumn mergeCol = attrs.getClientMergeColumns().iterator().next();
        assertEquals("DESCRIPTION", mergeCol.getName().getString());
      }

      // Fully covered vector index plan
      String queryCov = "SELECT ID, CATEGORY FROM " + fixL2Covered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(queryCov).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Plan must be VectorIndexScanPlan", plan instanceof VectorIndexScanPlan);
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) plan;
        ExplainPlan explainPlan = vPlan.getExplainPlan();
        ExplainPlanAttributes attrs = explainPlan.getPlanStepsAsAttributes();
        assertNotNull("ExplainPlanAttributes must not be null", attrs);

        assertNull(
          "Covered query must report no client merge columns: " + attrs.getClientMergeColumns(),
          attrs.getClientMergeColumns());
      }
    }
  }

  @Test
  public void testVectorLiteralAbbreviationInExplain() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = new ArrayList<>();
      for (int k = 0; k < 4; k++) {
        float[] c = new float[128];
        c[0] = k * 10.0f;
        centroids.add(c);
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      float[] rowVector = new float[128];
      rowVector[0] = 1.0f;
      Float[] boxedRow = new Float[128];
      for (int i = 0; i < 128; i++) {
        boxedRow[i] = rowVector[i];
      }

      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)")) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", boxedRow));
        ps.executeUpdate();
        conn.commit();
      }

      float[] queryVector = new float[128];
      queryVector[0] = 1.0f;
      queryVector[99] = 77.125f; // 100th element
      Float[] boxedQ = new Float[128];
      for (int i = 0; i < 128; i++) {
        boxedQ[i] = queryVector[i];
      }

      String sql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          String[] lines = plan.split("\\r?\\n");
          String serverTopLine = null;
          for (String line : lines) {
            if (line.contains("SERVER TOP-")) {
              serverTopLine = line;
              break;
            }
          }
          assertNotNull("EXPLAIN plan must contain SERVER TOP- line: " + plan, serverTopLine);
          assertTrue("SERVER TOP- line must be < 256 chars (was " + serverTopLine.trim().length()
            + "): " + serverTopLine, serverTopLine.trim().length() < 256);
          assertTrue("SERVER TOP- line must contain VECTOR(FLOAT, 128)[: " + serverTopLine,
            serverTopLine.contains("VECTOR(FLOAT, 128)["));
          assertFalse("SERVER TOP- line must not contain 77.125: " + serverTopLine,
            serverTopLine.contains("77.125"));
        }
      }

      // Verify that abbreviation applies only to EXPLAIN output, leaving
      // LiteralExpression.toString() unchanged
      LiteralExpression litExpr = LiteralExpression.newConstant(queryVector, PVectorFloat.INSTANCE);
      assertTrue("LiteralExpression.toString() must contain 77.125: " + litExpr.toString(),
        litExpr.toString().contains("77.125"));
    }
  }

  @Test
  public void testSingleCellVectorIndexUsed() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4)) "
          + "IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      Map<String, float[]> rows = new LinkedHashMap<>();
      rows.put("A1", new float[] { 1.0f, 0.0f, 0.0f, 0.0f });
      rows.put("A2", new float[] { 2.0f, 0.0f, 0.0f, 0.0f });
      rows.put("B1", new float[] { 9.0f, 0.0f, 0.0f, 0.0f });
      rows.put("B2", new float[] { 10.0f, 0.0f, 0.0f, 0.0f });

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V) VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (Map.Entry<String, float[]> entry : rows.entrySet()) {
          ps.setString(1, entry.getKey());
          float[] v = entry.getValue();
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] queryVec = new float[] { 0.5f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] };
      int k = 2;
      List<String> expectedTopK = VectorIndexTestUtil.bruteForceTopK(rows, queryVec, "L2", k);

      String querySql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT " + k;
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          assertTrue("EXPLAIN plan must reference index " + indexName + ": " + plan,
            plan.contains(indexName));
          assertTrue("EXPLAIN plan must contain CLIENT PROBING: " + plan,
            plan.contains("CLIENT PROBING"));
        }
      }

      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
          }
        }
      }
      assertEquals("Returned IDs must match brute-force top-k", expectedTopK, actualIds);
    }
  }

  @Test
  public void testSingleCellVectorIndexWithCoveredVectorColumn() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), COV_V VECTOR(FLOAT, 4)) "
          + "IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
        stmt
          .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (COV_V) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      Map<String, float[]> rowsV = new LinkedHashMap<>();
      rowsV.put("A1", new float[] { 1.0f, 0.0f, 0.0f, 0.0f });
      rowsV.put("A2", new float[] { 2.0f, 0.0f, 0.0f, 0.0f });
      rowsV.put("B1", new float[] { 9.0f, 0.0f, 0.0f, 0.0f });
      rowsV.put("B2", new float[] { 10.0f, 0.0f, 0.0f, 0.0f });

      Map<String, float[]> rowsCovV = new LinkedHashMap<>();
      rowsCovV.put("A1", new float[] { 100.0f, 0.0f, 0.0f, 0.0f });
      rowsCovV.put("A2", new float[] { 90.0f, 0.0f, 0.0f, 0.0f });
      rowsCovV.put("B1", new float[] { 20.0f, 0.0f, 0.0f, 0.0f });
      rowsCovV.put("B2", new float[] { 10.0f, 0.0f, 0.0f, 0.0f });

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, COV_V) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (String id : rowsV.keySet()) {
          ps.setString(1, id);
          float[] v = rowsV.get(id);
          float[] covV = rowsCovV.get(id);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.setArray(3,
            conn.createArrayOf("FLOAT", new Float[] { covV[0], covV[1], covV[2], covV[3] }));
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] queryVec = new float[] { 0.5f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] };
      int k = 2;
      List<String> expectedTopKOverV = VectorIndexTestUtil.bruteForceTopK(rowsV, queryVec, "L2", k);

      String querySql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT " + k;
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          assertTrue("EXPLAIN plan must reference index " + indexName + ": " + plan,
            plan.contains(indexName));
          assertTrue("EXPLAIN plan must contain CLIENT PROBING: " + plan,
            plan.contains("CLIENT PROBING"));
        }
      }

      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
          }
        }
      }
      assertEquals("Returned IDs must match brute force over indexed column V", expectedTopKOverV,
        actualIds);
    }
  }
}
