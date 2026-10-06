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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.OptimizerReasons;
import org.apache.phoenix.optimize.VectorSearchUtil;
import org.apache.phoenix.query.explain.ExplainPlanTestUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration tests for IVF vector index queries: plan selection, probes and row lookups. */
@Category(ParallelStatsDisabledTest.class)
public class VectorIvfQueryIT extends ParallelStatsDisabledIT {

  // Fixture (1): L2 index on clustered rows that covers CATEGORY but not DESCRIPTION
  private static VectorIndexTestUtil.ClusteredFixture fixL2Covered;

  // Fixture (2): L2 index on clustered rows with no INCLUDE, so CATEGORY, DESCRIPTION and the
  // second vector V2 are not covered
  private static VectorIndexTestUtil.ClusteredFixture fixL2Uncovered;

  // Fixture (3): Six hand-placed rows in the posting lists of two known centroids
  private static VectorIndexTestUtil.HandPlacedFixture fixProbe;

  // Fixture (4): COSINE index with known centroids on the four axes
  private static String cosineTable;
  private static String cosineIndex;
  private static List<float[]> cosineCentroids;
  private static Map<String, float[]> cosineRows;

  // Fixture (5): INNER_PRODUCT index with known centroids on the four axes
  private static String ipTable;
  private static String ipIndex;
  private static List<float[]> ipCentroids;
  private static Map<String, float[]> ipRows;

  // Fixture (6): The hand-placed rows of fixture (3) on a salted table
  private static VectorIndexTestUtil.HandPlacedFixture fixSaltedProbe;

  // Fixture (7): The hand-placed rows on a multi-tenant table, with rows for T1 and T2
  private static String mtTable;
  private static String mtIndex;
  private static List<float[]> mtCentroids;
  private static Map<String, float[]> mtT1Rows;
  private static Map<String, float[]> mtT2Rows;

  // Fixture (8): The hand-placed rows on a salted multi-tenant table, with rows for T1
  private static String mtSaltTable;
  private static String mtSaltIndex;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // (1) L2 index that covers CATEGORY but not DESCRIPTION
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
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t1, idx1, l2Centroids);

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

      // (2) L2 index with no INCLUDE, so CATEGORY, DESCRIPTION and the second vector V2 are not
      // covered
      String t2 = generateUniqueName();
      String idx2 = generateUniqueName();
      fixL2Uncovered = VectorIndexTestUtil.buildClusteredL2Fixture(conn, t2, idx2, false, true);

      // (3) Six hand-placed rows in the posting lists of two known centroids
      String t3 = generateUniqueName();
      String idx3 = generateUniqueName();
      fixProbe = VectorIndexTestUtil.buildProbeFixture(conn, t3, idx3, null, false);

      // (4) COSINE index with known centroids
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
        cosineCentroids);

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

      // (5) INNER_PRODUCT index with known centroids
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
      VectorIndexTestUtil.activateWithKnownCentroids(conn, ipTable, ipIndex, ipCentroids);

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

      // (6) Hand-placed rows on a table with four salt buckets
      String t6 = generateUniqueName();
      String idx6 = generateUniqueName();
      fixSaltedProbe = VectorIndexTestUtil.buildProbeFixture(conn, t6, idx6, 4, false);

      // (7) Hand-placed rows on a multi-tenant table
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

      // Write the rows of tenant T1
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

      // Write the rows of tenant T2, moved along the first axis
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

      // (8) Multi-tenant table with SALT_BUCKETS=3, with rows for tenant T1
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

  @Test
  public void testIvfProbeRestrictionIsReal() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(fixProbe.indexName);

      // Each index row key must start with the ID of the centroid that the row is assigned to
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

      // A probe count of 1 must scan only the posting list of the nearest centroid
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

      // A probe count of 2 scans both posting lists and must give the exact top 2
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

      // A connection property can also set the probe count
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

      // The exact plan does not read the index and gives the reference result
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
      // The query vector is near the centroid of cluster 2 at (100, 0, 0, 0)
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
          QueryPlan plan = pps.optimizeQuery();
          assertEquals(probe, VectorIndexTestUtil.vectorPlan(plan).getProbeCount());
          if (probe == 1) {
            assertArrayEquals("Probe=1 must select centroid 2", new int[] { 2 },
              VectorIndexTestUtil.vectorPlan(plan).getProbeCentroids());
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

      // The expected result is the nearest rows in the science category
      Map<String, float[]> scienceRows = new HashMap<>();
      for (Map.Entry<String, float[]> e : fixL2Covered.rows.entrySet()) {
        if ("science".equals(fixL2Covered.categories.get(e.getKey()))) {
          scienceRows.put(e.getKey(), e.getValue());
        }
      }
      List<String> expectedScienceTop5 =
        VectorIndexTestUtil.bruteForceTopK(scienceRows, q, "L2", 5);

      // Without the filter, the nearest rows include art rows, so the filter must change the result
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
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        QueryPlan plan = pps.optimizeQuery();
        assertNotNull("Covered query must use the vector index",
          VectorIndexTestUtil.vectorPlan(plan));
        assertFalse("Covered filter must not look up the data table",
          VectorIndexTestUtil.isFilterTimeLookup(plan));
        assertFalse("Covered projection must not be deferred",
          VectorIndexTestUtil.isDeferredProjection(plan));
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
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Uncovered filter must set filterTimeUncoveredLookup",
          VectorIndexTestUtil.isFilterTimeLookup(plan));
        assertFalse("Uncovered filter must not set projectionTimeUncoveredLookup",
          VectorIndexTestUtil.isDeferredProjection(plan));
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

  private static List<String> ids(Connection conn, String sql, Float[] q) throws Exception {
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", q));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  private static QueryPlan plan(Connection conn, String sql, Float[] q) throws Exception {
    PhoenixPreparedStatement pps =
      conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
    pps.setArray(1, conn.createArrayOf("FLOAT", q));
    return pps.optimizeQuery();
  }

  /**
   * For an ascending distance, the exact plan puts the rows that have no vector last. The index has
   * no row for them, so the index plan agrees with the exact plan only in this order. An explicit
   * NULLS FIRST must use the exact plan.
   */
  @Test
  public void testNullVectorsOrderAlikeAcrossPlans() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.setupTableAndKnownCentroids(conn, table, index);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + table + " (ID, V) VALUES (?, ?)")) {
        for (int i = 0; i < 8; i++) {
          ps.setString(1, "V" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { i * 0.5f, 1f, 0f, 0f }));
          ps.executeUpdate();
        }
        for (int i = 0; i < 3; i++) {
          ps.setString(1, "N" + i);
          ps.setNull(2, java.sql.Types.ARRAY);
          ps.executeUpdate();
        }
      }
      conn.commit();
      Float[] q = new Float[] { 0f, 1f, 0f, 0f };
      List<String> expected = Arrays.asList("V0", "V1", "V2", "V3", "V4");
      String indexed =
        "SELECT /*+ VECTOR_PROBE_COUNT(4) */ ID FROM " + table + " ORDER BY L2_DISTANCE(V, ?)";
      String exact = "SELECT /*+ NO_INDEX */ ID FROM " + table + " ORDER BY L2_DISTANCE(V, ?)";
      assertNotNull(VectorIndexTestUtil.vectorPlan(plan(conn, indexed + " LIMIT 5", q)));
      assertEquals(expected, ids(conn, indexed + " LIMIT 5", q));
      assertEquals(expected, ids(conn, exact + " LIMIT 5", q));
      List<String> all = ids(conn, exact, q);
      assertEquals(11, all.size());
      assertEquals(new HashSet<>(Arrays.asList("N0", "N1", "N2")),
        new HashSet<>(all.subList(8, 11)));

      // An ORDER BY ordinal that names the projected distance must also use the index
      String ordinal = "SELECT /*+ VECTOR_PROBE_COUNT(4) */ ID, L2_DISTANCE(V, ?) FROM " + table
        + " ORDER BY 2 LIMIT 5";
      assertNotNull(VectorIndexTestUtil.vectorPlan(plan(conn, ordinal, q)));
      assertEquals(expected, ids(conn, ordinal, q));

      // NULLS FIRST needs the rows that the index does not have, so only the exact plan can run it
      String nullsFirst = indexed + " NULLS FIRST LIMIT 5";
      assertNull(VectorIndexTestUtil.vectorPlan(plan(conn, nullsFirst, q)));
      List<String> first = ids(conn, nullsFirst, q);
      assertEquals(new HashSet<>(Arrays.asList("N0", "N1", "N2")),
        new HashSet<>(first.subList(0, 3)));
      assertEquals(Arrays.asList("V0", "V1"), first.subList(3, 5));
    }
  }

  /**
   * The key query of a deferred projection projects only the primary key. An ORDER BY ordinal that
   * names the projected distance must still rank the keys by that distance, not by a key column.
   */
  @Test
  public void testDeferredProjectionOrderedByDistanceOrdinal() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + table + " (G VARCHAR NOT NULL, ID VARCHAR NOT NULL, "
          + "V VECTOR(FLOAT, 4), DESCRIPTION VARCHAR CONSTRAINT PK PRIMARY KEY (G, ID))");
        stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, table, index,
        VectorIndexTestUtil.KNOWN_CENTROIDS);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + table + " VALUES ('g', ?, ?, ?)")) {
        // Distance to the query decreases as ID increases, so an ID rank gives the farthest rows
        for (int i = 0; i < 8; i++) {
          ps.setString(1, "R" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { (7 - i) * 0.5f, 1f, 0f, 0f }));
          ps.setString(3, "D" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();
      Float[] q = new Float[] { 0f, 1f, 0f, 0f };
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(4) */ ID, L2_DISTANCE(V, ?), DESCRIPTION FROM "
        + table + " ORDER BY 2 LIMIT 3";
      assertTrue(VectorIndexTestUtil.isDeferredProjection(plan(conn, sql, q)));
      List<String> expected = Arrays.asList("R7", "R6", "R5");
      assertEquals(expected, ids(conn, sql, q));
      assertEquals(expected, ids(conn, sql.replace("VECTOR_PROBE_COUNT(4)", "NO_INDEX"), q));
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", q));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            assertEquals("D" + rs.getString(1).substring(1), rs.getString(3));
          }
        }
      }
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
      // The plan must defer the lookup of DESCRIPTION until after the top-K rank
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
        QueryPlan plan = ps.getQueryPlan();
        assertNotNull(plan);
        assertTrue(VectorIndexTestUtil.isDeferredProjection(plan));
        assertFalse(VectorIndexTestUtil.isFilterTimeLookup(plan));
      }
      assertEquals(expectedTop5, actualIds);
    }
  }

  @Test
  public void testUncoveredFilterAndUncoveredProjection() throws Exception {
    // An uncovered filter column needs a server merge lookup for each candidate, not a deferred one
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
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("filterTime must be true", VectorIndexTestUtil.isFilterTimeLookup(plan));
        assertFalse("projectionTime must be false when filterTime is already active",
          VectorIndexTestUtil.isDeferredProjection(plan));
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
    // A covered filter with an uncovered projection must use a deferred lookup, not a server merge
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
        QueryPlan plan = ps.getQueryPlan();
        assertNotNull(plan);
        assertFalse("filterTime must be false for covered filter",
          VectorIndexTestUtil.isFilterTimeLookup(plan));
        assertTrue("projectionTime must be true for uncovered description",
          VectorIndexTestUtil.isDeferredProjection(plan));
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
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("SELECT * with uncovered columns must use deferred projection",
          VectorIndexTestUtil.isDeferredProjection(plan));
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
    // A second vector column that the index does not cover must come from a deferred projection
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
        QueryPlan plan = pps.optimizeQuery();
        assertTrue("Second vector column must trigger deferred projection",
          VectorIndexTestUtil.isDeferredProjection(plan));
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

      // A direct HBase delete bypasses index maintenance, so the index row of the data row stays
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
      // The semi-join with the data table drops the index row whose data row is missing
      assertEquals(4, actualIds.size());
      assertFalse("Deleted row must be skipped without exception", actualIds.contains(deletedId));
      assertEquals(top5.subList(1, 5), actualIds);
    }
  }

  @Test
  public void testNonVectorQueryDoesNotUseVectorIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // A filter on a covered column with no distance order must not use the vector index
      String query1 = "SELECT ID FROM " + fixL2Covered.tableName + " WHERE CATEGORY = 'science'";
      ResultSet rs1 = conn.createStatement().executeQuery("EXPLAIN " + query1);
      String plan1 = QueryUtil.getExplainPlan(rs1);
      assertFalse("Non-vector query must not use vector index: " + plan1,
        plan1.contains(fixL2Covered.indexName));
      assertFalse("Non-vector query must not probe centroids: " + plan1,
        plan1.contains("CLIENT PROBING"));

      // An aggregate with no distance order must not use the vector index
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

      // A COSINE distance on an L2 index must not use the index, so the exact plan runs
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

      // An INNER_PRODUCT distance on an L2 index must not use the index, so the exact plan runs
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
    // The index must not serve an ORDER BY on a vector column that the index does not contain
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
        Arrays.asList(new float[] { 0f, 0f, 0f, 0f }, new float[] { 10f, 0f, 0f, 0f }));

      // V and V2 cluster in opposite orders, so a wrong index choice gives a wrong result
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

      // L2 distance operator (<->)
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

      // Cosine distance operator (<=>)
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

      // Inner product distance operator (<#>)
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
        // An empty table gives no vectors to train centroids, so the index stays in BUILDING state
        stmt.execute("CREATE VECTOR INDEX " + idx + " ON " + t + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      // Write the data rows after the index exists
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

      // The optimizer must not select a vector index in BUILDING state
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

      // After activation with known centroids, the optimizer must select the vector index
      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 5.0f, 0.0f, 0.0f, 0.0f }, new float[] { 10.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 15.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idx, centroids);

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

      // The expected result is the farthest rows, in descending distance order
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

      String indexSql = "SELECT \"_CENTROID_ID\", \":ID\" FROM " + fixL2Covered.indexName;
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

      // Each index row key must start with the nearest centroid under the COSINE metric
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

      // The query vector is near centroid 0
      float[] q = new float[] { 1.0f, 0.05f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };
      List<String> oracleTop5 = VectorIndexTestUtil.bruteForceTopK(cosineRows, q, "COSINE", 5);

      // A probe of all 4 centroids must give the exact brute force result
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

      // A probe of 1 centroid must scan only the posting list of the nearest centroid
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

      // Each index row key must start with the nearest centroid under the INNER_PRODUCT metric
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

      // The query vector is near centroid 0
      float[] q = new float[] { 10.0f, 0.1f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { q[0], q[1], q[2], q[3] };
      List<String> oracleTop5 = VectorIndexTestUtil.bruteForceTopK(ipRows, q, "INNER_PRODUCT", 5);

      // A probe of all 4 centroids must give the exact brute force result
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

      // A probe of 1 centroid must scan only the posting list of the nearest centroid
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

      // The index rows must occupy more than one salt bucket
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(6, rowKeys.size());
      Set<Byte> distinctSalts = new HashSet<>();
      for (byte[] rk : rowKeys) {
        distinctSalts.add(rk[0]);
      }
      assertTrue("Salted table rows must span multiple salt buckets: count=" + distinctSalts.size(),
        distinctSalts.size() > 1);

      // A probe of 1 centroid must scan its posting list in each salt bucket
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

      // A probe of 2 centroids across the salt buckets must give the exact top 2
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

    // Queries for tenant T1
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
        QueryPlan plan = pps.optimizeQuery();
        // The posting list scan ranges must start with the tenant ID
        assertEquals(1, VectorIndexTestUtil.vectorPlan(plan).getProbeCount());
        assertArrayEquals(Bytes.toBytes("T1"), VectorIndexTestUtil.vectorPlan(plan).getContext()
          .getScanRanges().getRanges().get(0).get(0).getLowerRange());
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

    // Queries for tenant T2
    try (Connection t2Conn = getTenantConnection("T2")) {
      String queryT2 = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + mtTable
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";

      try (PhoenixPreparedStatement pps =
        t2Conn.prepareStatement(queryT2).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, t2Conn.createArrayOf("FLOAT", qVec));
        QueryPlan plan = pps.optimizeQuery();
        // The posting list scan ranges must start with the tenant ID
        assertEquals(1, VectorIndexTestUtil.vectorPlan(plan).getProbeCount());
        assertArrayEquals(Bytes.toBytes("T2"), VectorIndexTestUtil.vectorPlan(plan).getContext()
          .getScanRanges().getRanges().get(0).get(0).getLowerRange());
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
      // The T2 rows sit 0.5 farther along the first axis. A probe of 1 centroid still gives A1 and
      // A2, as for T1
      assertEquals("T2 probe=1 must return [A1, A2]", Arrays.asList("A1", "A2"), actualT2);
    }

    // Multi-tenant index on a salted table, where the tenant column follows the salt byte
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
    // A global connection cannot scope the tenant prefix of a multi-tenant index. The plan must
    // scan the whole index and must still give the exact result.
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

  /**
   * A deferred projection must keep a ranked row whose uncovered projected column is null. The data
   * row still exists, because its empty column cell is present.
   */
  @Test
  public void testDeferredProjectionKeepsRowWithNullProjectedColumns() throws Exception {
    String t = "T_VEC_NULLP_" + generateUniqueName();
    String idx = "IDX_VEC_NULLP_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + t
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), DESCRIPTION VARCHAR)");
      conn.createStatement().execute("CREATE VECTOR INDEX " + idx + " ON " + t
        + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idx, Arrays
        .asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f }, new float[] { 10.0f, 0.0f, 0.0f, 0.0f }));
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + t + " (ID, V, DESCRIPTION) VALUES (?, ?, ?)")) {
        for (int i = 0; i < 4; i++) {
          ps.setString(1, "R" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { (float) i, 0f, 0f, 0f }));
          ps.setString(3, i == 0 ? null : "d" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();
      String query = "SELECT /*+ VECTOR_PROBE_COUNT(2) */ ID, DESCRIPTION FROM " + t
        + " ORDER BY L2_DISTANCE(V, ARRAY[0.0, 0.0, 0.0, 0.0]) LIMIT 3";
      try (PhoenixPreparedStatement ps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        assertTrue(VectorIndexTestUtil.isDeferredProjection(ps.optimizeQuery()));
      }
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        assertTrue(rs.next());
        assertEquals("R0", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.next());
        assertEquals("R1", rs.getString(1));
        assertEquals("d1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("R2", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * A deferred projection must return an uncovered BSON column. A document expression on that
   * column must give the same values as the exact plan.
   */
  @Test
  public void testDeferredProjectionOfUncoveredBsonValue() throws Exception {
    String t = "T_VEC_BSONP_" + generateUniqueName();
    String idx = "IDX_VEC_BSONP_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE " + t + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), DOC BSON)");
      conn.createStatement().execute("CREATE VECTOR INDEX " + idx + " ON " + t
        + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idx, Arrays
        .asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f }, new float[] { 10.0f, 0.0f, 0.0f, 0.0f }));
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + t + " (ID, V, DOC) VALUES (?, ?, ?)")) {
        for (int i = 0; i < 4; i++) {
          ps.setString(1, "R" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { (float) i, 0f, 0f, 0f }));
          ps.setObject(3, RawBsonDocument.parse("{\"name\": \"n" + i + "\"}"));
          ps.executeUpdate();
        }
      }
      conn.commit();
      String query = "SELECT /*+ VECTOR_PROBE_COUNT(2) */ ID, DOC FROM " + t
        + " ORDER BY L2_DISTANCE(V, ARRAY[0.0, 0.0, 0.0, 0.0]) LIMIT 2";
      try (PhoenixPreparedStatement ps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        assertTrue(VectorIndexTestUtil.isDeferredProjection(ps.optimizeQuery()));
      }
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        assertTrue(rs.next());
        assertEquals("R0", rs.getString(1));
        assertEquals("n0", ((BsonDocument) rs.getObject(2)).getString("name").getValue());
        assertTrue(rs.next());
        assertEquals("R1", rs.getString(1));
        assertEquals("n1", ((BsonDocument) rs.getObject(2)).getString("name").getValue());
        assertFalse(rs.next());
      }
      String parsed = " ID, BSON_VALUE(DOC, 'name', 'VARCHAR') FROM " + t
        + " ORDER BY L2_DISTANCE(V, ARRAY[0.0, 0.0, 0.0, 0.0]) LIMIT 2";
      List<String> deferred = new ArrayList<>();
      List<String> exact = new ArrayList<>();
      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT /*+ VECTOR_PROBE_COUNT(2) */" + parsed)) {
        while (rs.next()) {
          deferred.add(rs.getString(1) + "=" + rs.getString(2));
        }
      }
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */" + parsed)) {
        while (rs.next()) {
          exact.add(rs.getString(1) + "=" + rs.getString(2));
        }
      }
      List<String> expected = new ArrayList<>();
      expected.add("R0=n0");
      expected.add("R1=n1");
      assertEquals(expected, exact);
      assertEquals(expected, deferred);
    }
  }

  /** A WHERE clause that is always false must return no rows and must not scan a posting list. */
  @Test
  public void testDegenerateWhereReturnsNothing() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String query = "SELECT ID FROM " + fixL2Covered.tableName
        + " WHERE 1 = 0 ORDER BY L2_DISTANCE(V, ARRAY[50.0, 0.0, 0.0, 0.0]) LIMIT 5";
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        assertFalse(rs.next());
      }
    }
  }

  /**
   * A server merge over a covered global index must verify the index rows. It must not return the
   * unverified index row of a failed data table write.
   */
  @Test
  public void testServerMergeOverCoveredGlobalIndexVerifiesRows() throws Exception {
    String t = "T_MERGE_VFY_" + generateUniqueName();
    String idx = "IDX_MERGE_VFY_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + t
        + " (ID VARCHAR NOT NULL PRIMARY KEY, A VARCHAR, B VARCHAR, C VARCHAR)");
      conn.createStatement().execute("CREATE INDEX " + idx + " ON " + t + " (A) INCLUDE (B)");
      conn.createStatement().execute("UPSERT INTO " + t + " VALUES ('r1', 'x', 'b1', 'c1')");
      conn.commit();
      IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
      try {
        conn.createStatement().execute("UPSERT INTO " + t + " (ID, A) VALUES ('r1', 'y')");
        try {
          conn.commit();
          fail("The data table write was configured to fail");
        } catch (Exception e) {
          // expected
        }
      } finally {
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
      }
      String query =
        "SELECT /*+ INDEX(" + t + " " + idx + ") */ ID, C FROM " + t + " WHERE A = 'y'";
      String plan =
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + query));
      assertTrue(plan, plan.contains(idx) && plan.contains("SERVER MERGE"));
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        assertFalse("An unverified index row must not be returned", rs.next());
      }
    }
  }

  private static final List<float[]> STEP_50_CENTROIDS =
    Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f }, new float[] { 50.0f, 0.0f, 0.0f, 0.0f },
      new float[] { 100.0f, 0.0f, 0.0f, 0.0f }, new float[] { 150.0f, 0.0f, 0.0f, 0.0f });

  @Test
  public void testStaticOrderingPrefersCoveringVectorIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String t = generateUniqueName();
      String idxCovering = generateUniqueName();
      String idxNonCovering = generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + t
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR, DESCRIPTION VARCHAR)");
        // Create the non-covering index first. Then the result shows the static plan order, not
        // the order of index creation.
        stmt.execute("CREATE VECTOR INDEX " + idxNonCovering + " ON " + t + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
        stmt.execute("CREATE VECTOR INDEX " + idxCovering + " ON " + t + " (V) "
          + "INCLUDE (DESCRIPTION) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idxCovering, STEP_50_CENTROIDS);
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, idxNonCovering, STEP_50_CENTROIDS);

      String query = "SELECT ID, DESCRIPTION FROM " + t + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        QueryPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
        assertTrue("Expected the covering index to answer the query alone",
          plan instanceof VectorIndexScanPlan);
        assertEquals(idxCovering, plan.getTableRef().getTable().getTableName().getString());
      }
      assertEquals(OptimizerReasons.RULE_NEAREST_NEIGHBOR_INDEX,
        ExplainPlanTestUtil.getExplainAttributes(conn, "SELECT ID, DESCRIPTION FROM " + t
          + " ORDER BY L2_DISTANCE(V, ARRAY[1.0, 0.0, 0.0, 0.0]) LIMIT 5").getIndexRule());
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
      VectorIndexTestUtil.activateWithKnownCentroids(conn, t, vectorIdx, STEP_50_CENTROIDS);

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

      // Without statistics, the static order puts the vector index plan before a regular index
      // plan that binds the leading key column.
      String defaultQuery =
        "SELECT ID FROM " + t + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(defaultQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 0.0f }));
        QueryPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
        assertTrue("Default plan should be VectorIndexScanPlan",
          plan instanceof VectorIndexScanPlan);
        assertEquals(vectorIdx, plan.getTableRef().getTable().getTableName().getString());
      }

      // An explicit INDEX hint selects the regular index, which gives an exact search.
      String hintedQuery = "SELECT /*+ INDEX(" + t + " " + regularIdx + ") */ ID FROM " + t
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      List<String> expected = VectorIndexTestUtil.bruteForceTopK(scienceVectors,
        new float[] { 0.0f, 0.0f, 0.0f, 0.0f }, "L2", 5);
      try (PreparedStatement ps = conn.prepareStatement(hintedQuery)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 0.0f }));
        QueryPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
        assertFalse(VectorSearchUtil.usesVectorIndex(plan));
        assertEquals(regularIdx, plan.getTableRef().getTable().getTableName().getString());
        List<String> results = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            results.add(rs.getString(1));
          }
        }
        assertEquals(expected, results);
      }

      // The USE_DATA_OVER_INDEX_TABLE hint puts vector index plans after all other candidates.
      String dataHinted = "SELECT /*+ USE_DATA_OVER_INDEX_TABLE */ ID FROM " + t
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PreparedStatement ps = conn.prepareStatement(dataHinted)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 0.0f }));
        QueryPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
        assertFalse(VectorSearchUtil.usesVectorIndex(plan));
      }
    }
  }

  @Test
  public void testExplainDistinguishesLookupStrategies() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Float[] q = new Float[] { 50.2f, 0.1f, -0.1f, 0.05f };

      // Covered: the index answers the query without data table access.
      String covered = explain(conn, "SELECT ID, CATEGORY FROM " + fixL2Covered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5", q);
      assertTrue(covered, covered.contains("CLIENT PROBING 2 OF 4 CENTROIDS (L2)"));
      assertFalse(covered, covered.contains("SERVER MERGE"));
      assertFalse(covered, covered.contains("SKIP-SCAN-JOIN"));

      // Deferred projection: the index ranks rows, then the top-k rows come from the data table.
      String projection = explain(conn, "SELECT ID, DESCRIPTION FROM " + fixL2Covered.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5", q);
      assertTrue(projection, projection.contains("CLIENT PROBING 2 OF 4 CENTROIDS (L2)"));
      assertTrue(projection, projection.contains("SKIP-SCAN-JOIN"));
      assertFalse(projection, projection.contains("SERVER MERGE"));

      // Filter-time join: every candidate row joins to the data table before the top-k step.
      String filter = explain(conn, "SELECT ID, DESCRIPTION FROM " + fixL2Uncovered.tableName
        + " WHERE CATEGORY = 'science' ORDER BY L2_DISTANCE(V, ?) LIMIT 5", q);
      assertTrue(filter, filter.contains("CLIENT PROBING 2 OF 4 CENTROIDS (L2)"));
      assertTrue(filter, filter.contains("SERVER MERGE"));
      assertFalse(filter, filter.contains("SKIP-SCAN-JOIN"));
    }
  }

  private static String explain(Connection conn, String query, Float[] q) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", q));
      try (ResultSet rs = ps.executeQuery()) {
        return QueryUtil.getExplainPlan(rs);
      }
    }
  }

  @Test
  public void testStructuredExplainAttributes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Float[] q = new Float[] { 50.2f, 0.1f, -0.1f, 0.05f };
      String query = "SELECT /*+ VECTOR_PROBE_COUNT(3) */ ID, CATEGORY FROM "
        + fixL2Covered.tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", q));
        ExplainPlanAttributes attrs =
          pps.optimizeQuery().getExplainPlan().getPlanStepsAsAttributes();
        assertEquals(Integer.valueOf(3), attrs.getVectorProbeCount());
        assertEquals(Integer.valueOf(4), attrs.getVectorCentroidCount());
        assertEquals("L2", attrs.getVectorDistanceMetric());
        assertTrue(attrs.isVectorSearch());
      }

      // A full scan plan does not set the vector probe attributes.
      String exact = "SELECT /*+ NO_INDEX */ ID FROM " + fixL2Covered.tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(exact).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", q));
        ExplainPlanAttributes attrs =
          pps.optimizeQuery().getExplainPlan().getPlanStepsAsAttributes();
        assertEquals(null, attrs.getVectorProbeCount());
        assertEquals(null, attrs.getVectorDistanceMetric());
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
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids);

      float[] queryVector = new float[128];
      queryVector[0] = 1.0f;
      queryVector[99] = 77.125f;
      Float[] boxedQ = new Float[128];
      for (int i = 0; i < 128; i++) {
        boxedQ[i] = queryVector[i];
      }
      String plan = explain(conn,
        "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5", boxedQ);
      String serverTopLine = null;
      for (String line : plan.split("\\r?\\n")) {
        if (line.contains("SERVER TOP-")) {
          serverTopLine = line.trim();
        }
      }
      assertNotNull(plan, serverTopLine);
      assertTrue(serverTopLine, serverTopLine.length() < 256);
      assertTrue(serverTopLine, serverTopLine.contains("VECTOR(FLOAT, 128)[1.0, 0.0"));
      assertFalse(serverTopLine, serverTopLine.contains("77.125"));

      // The expression string must stay exact, because persistence and DDL use it.
      assertTrue(LiteralExpression.newConstant(queryVector, PVectorFloat.INSTANCE).toString()
        .contains("77.125"));
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
      // The vector index does not specify a storage scheme, so it gets the immutable storage
      // scheme of the base table.
      assertEquals(PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, conn
        .unwrap(PhoenixConnection.class).getTableNoCache(indexName).getImmutableStorageScheme());

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids);

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
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids);

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

  /**
   * Tests writes and a vector top-K query through a view with a WHERE clause. The view inherits the
   * vector index of the table under a view-specific name. That inherited index must assign and
   * probe with the centroids of the table index.
   */
  @Test
  public void testWriteAndQueryThroughViewWithWhereClause() throws Exception {
    String tableName = "T_VEC_VIEW_" + generateUniqueName();
    String indexName = "IDX_VEC_VIEW_" + generateUniqueName();
    String viewName = "V_VEC_VIEW_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, K VARCHAR, V VECTOR(FLOAT, 4))");
        // A view inherits an index only if the index covers the view's WHERE columns
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (V) INCLUDE (K) WITH (algorithm = 'IVF', metric = 'L2', lists = 2,"
          + " sample_size = 100)");
      }
      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids);
      try (Statement stmt = conn.createStatement()) {
        stmt
          .execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " WHERE K = 'k'");
        // A nearer row outside the view must not appear in the result of a query on the view
        stmt.execute("UPSERT INTO " + tableName + " VALUES ('X', 'x', ARRAY[5, 0, 0, 0])");
      }
      Map<String, float[]> rows = new LinkedHashMap<>();
      rows.put("A1", new float[] { 1.0f, 0.0f, 0.0f, 0.0f });
      rows.put("A2", new float[] { 3.0f, 0.0f, 0.0f, 0.0f });
      rows.put("B1", new float[] { 6.0f, 0.0f, 0.0f, 0.0f });
      rows.put("B2", new float[] { 9.0f, 0.0f, 0.0f, 0.0f });
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + viewName + " (ID, V) VALUES (?, ?)")) {
        for (Map.Entry<String, float[]> row : rows.entrySet()) {
          float[] v = row.getValue();
          ps.setString(1, row.getKey());
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      try (
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexName)) {
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
      }

      Float[] boxedQ = new Float[] { 5.0f, 0.0f, 0.0f, 0.0f };
      String querySql = "SELECT /*+ VECTOR_PROBE_COUNT(2) */ ID FROM " + viewName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        String plan = QueryUtil.getExplainPlan(ps.executeQuery());
        assertTrue("EXPLAIN plan must probe the inherited index: " + plan,
          plan.contains("CLIENT PROBING 2 OF 2 CENTROIDS"));
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
      assertEquals(
        VectorIndexTestUtil.bruteForceTopK(rows, new float[] { 5.0f, 0.0f, 0.0f, 0.0f }, "L2", 2),
        actualIds);
    }
  }
}
