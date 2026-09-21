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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.PhoenixIndexImportDirectMapper;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Lifecycle integration tests for vector indexes. */
@Category(ParallelStatsDisabledTest.class)
public class VectorIvfLifecycleIT extends ParallelStatsDisabledIT {

  @Test
  public void testSynchronousVectorIndexPopulationAndActivation() throws Exception {
    String tableName = "T_VEC_POP_" + generateUniqueName();
    String indexName = "IDX_VEC_POP_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }

      // 4 distinct input vectors
      float[][] distinctInputs = new float[][] { { 0.0f, 1.0f, 2.0f, 3.0f },
        { 1.0f, 2.0f, 3.0f, 0.0f }, { 2.0f, 3.0f, 0.0f, 1.0f }, { 3.0f, 0.0f, 1.0f, 2.0f } };

      Map<String, float[]> rowVectors = new HashMap<>();
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          String id = String.format("id_%03d", i);
          float[] v = distinctInputs[i % 4];
          rowVectors.put(id, v);
          Float[] vec = new Float[] { v[0], v[1], v[2], v[3] };
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", vec));
          ps.setString(3, "label_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Create synchronous vector index with lists = 4, INCLUDE (LABEL)
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull("Index table must exist", indexTable);
      assertEquals("Index state must be ACTIVE", PIndexState.ACTIVE, indexTable.getIndexState());

      // 1. Load trained centroids and assert they match the 4 distinct input vectors
      List<byte[]> rawCentroids = CentroidManager.loadCentroids(conn, indexName, 1L);
      assertEquals(4, rawCentroids.size());
      List<float[]> trainedCentroids = new ArrayList<>();
      for (byte[] b : rawCentroids) {
        trainedCentroids.add(PVectorFloat.readElements(b, 0, b.length));
      }

      boolean[] matched = new boolean[4];
      for (float[] tc : trainedCentroids) {
        int best = -1;
        double bestDist = Double.MAX_VALUE;
        for (int j = 0; j < 4; j++) {
          double d = VectorIndexTestUtil.dist("L2", tc, distinctInputs[j]);
          if (d < bestDist) {
            bestDist = d;
            best = j;
          }
        }
        assertTrue("Trained centroid must match one of 4 inputs closely: dist=" + bestDist,
          bestDist < 1e-4);
        matched[best] = true;
      }
      for (int j = 0; j < 4; j++) {
        assertTrue("Distinct input vector " + j + " must be recovered as a centroid", matched[j]);
      }

      // 2 & 3. For every index row, assert centroidId == nearestCentroid, and 25 rows per centroid
      int[] centroidCounts = new int[4];
      String selectIndex = "SELECT \":CENTROID_ID\", \":ID\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectIndex)) {
        int count = 0;
        while (rs.next()) {
          count++;
          int cid = rs.getInt(1);
          String id = rs.getString(2);
          float[] v = rowVectors.get(id);
          assertNotNull(v);
          int expectedCid = VectorIndexTestUtil.nearestCentroid(v, trainedCentroids, "L2");
          assertEquals("Centroid ID for " + id + " mismatch", expectedCid, cid);
          centroidCounts[cid]++;
        }
        assertEquals(100, count);
      }
      for (int c = 0; c < 4; c++) {
        assertEquals("Each centroid must have exactly 25 rows", 25, centroidCounts[c]);
      }

      // 4. Query with probe=1: exactly 25 rows, all distance 0
      Float[] q0 = new Float[] { distinctInputs[0][0], distinctInputs[0][1], distinctInputs[0][2],
        distinctInputs[0][3] };
      String queryProbe1 = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 30";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + queryProbe1)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", q0));
        ResultSet rs = ps.executeQuery();
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("EXPLAIN must contain CLIENT PROBING 1 OF 4: " + plan,
          plan.contains("CLIENT PROBING 1 OF 4"));
      }

      List<String> probe1Ids = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(queryProbe1)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", q0));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            probe1Ids.add(rs.getString(1));
          }
        }
      }
      assertEquals("Probe=1 must return exactly 25 rows from the probed cluster", 25,
        probe1Ids.size());
      for (String id : probe1Ids) {
        float[] v = rowVectors.get(id);
        double d = VectorIndexTestUtil.dist("L2", distinctInputs[0], v);
        assertEquals("Row in probed cluster must have distance 0", 0.0, d, 1e-5);
      }

      // Query with probe=4: 30 rows, first 25 at distance 0, next 5 at distance sqrt(12)
      String queryProbe4 = "SELECT /*+ VECTOR_PROBE_COUNT(4) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 30";
      List<String> probe4Ids = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(queryProbe4)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", q0));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            probe4Ids.add(rs.getString(1));
          }
        }
      }
      assertEquals(30, probe4Ids.size());
      for (int i = 0; i < 25; i++) {
        float[] v = rowVectors.get(probe4Ids.get(i));
        double d = VectorIndexTestUtil.dist("L2", distinctInputs[0], v);
        assertEquals("First 25 results must have distance 0", 0.0, d, 1e-5);
      }
      double expectedDist = Math.sqrt(12.0);
      for (int i = 25; i < 30; i++) {
        float[] v = rowVectors.get(probe4Ids.get(i));
        double d = VectorIndexTestUtil.dist("L2", distinctInputs[0], v);
        assertEquals("Rows 26-30 must be at distance sqrt(12)", expectedDist, d, 1e-3);
      }
    }
  }

  @Test
  public void testFullLifecycleWithClusteredData() throws Exception {
    String tableName = "T_VEC_FULL_CLUST_" + generateUniqueName();
    String indexName = "IDX_VEC_FULL_CLUST_" + generateUniqueName();

    int numClusters = 4;
    int rowsPerCluster = 100;
    float[][] clusterCenters = new float[][] { { 0.0f, 0.0f, 0.0f, 0.0f },
      { 50.0f, 0.0f, 0.0f, 0.0f }, { 100.0f, 0.0f, 0.0f, 0.0f }, { 150.0f, 0.0f, 0.0f, 0.0f } };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      }

      Random rng = new Random(42);
      Map<String, float[]> allRows = new LinkedHashMap<>();
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        int count = 0;
        for (int k = 0; k < numClusters; k++) {
          for (int i = 0; i < rowsPerCluster; i++) {
            String id = String.format("k%d_r%03d", k, i);
            float[] v = new float[] { clusterCenters[k][0] + (float) (rng.nextGaussian() * 0.5),
              (float) (rng.nextGaussian() * 0.5), (float) (rng.nextGaussian() * 0.5),
              (float) (rng.nextGaussian() * 0.5) };
            allRows.put(id, v);
            ps.setString(1, id);
            ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
            ps.executeUpdate();
            count++;
            if (count % 100 == 0) {
              conn.commit();
            }
          }
        }
        conn.commit();
      }

      // Create synchronous vector index via real DDL path (unseeded k-means)
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 400)");
      }

      // 1. Assert 4 trained centroids each within 2.0 of a distinct cluster center
      List<byte[]> rawCentroids = CentroidManager.loadCentroids(conn, indexName, 1L);
      assertEquals(4, rawCentroids.size());
      List<float[]> trainedCentroids = new ArrayList<>();
      for (byte[] b : rawCentroids) {
        trainedCentroids.add(PVectorFloat.readElements(b, 0, b.length));
      }

      Set<Integer> matchedClusters = new HashSet<>();
      for (float[] tc : trainedCentroids) {
        int closestCluster = -1;
        double minD = Double.MAX_VALUE;
        for (int k = 0; k < numClusters; k++) {
          double d = VectorIndexTestUtil.dist("L2", tc, clusterCenters[k]);
          if (d < minD) {
            minD = d;
            closestCluster = k;
          }
        }
        assertTrue("Trained centroid must be within 2.0 of a cluster center; dist=" + minD,
          minD < 2.0);
        matchedClusters.add(closestCluster);
      }
      assertEquals("Each of 4 clusters must be matched by a distinct trained centroid", 4,
        matchedClusters.size());

      // 2. Per-row assignment equals nearest trained centroid
      String selectIndex = "SELECT \":CENTROID_ID\", \":ID\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectIndex)) {
        int count = 0;
        while (rs.next()) {
          count++;
          int cid = rs.getInt(1);
          String id = rs.getString(2);
          float[] v = allRows.get(id);
          assertNotNull(v);
          int expectedCid = VectorIndexTestUtil.nearestCentroid(v, trainedCentroids, "L2");
          assertEquals("Centroid assignment mismatch for " + id, expectedCid, cid);
        }
        assertEquals(400, count);
      }

      // 3. ORDER BY ... LIMIT 5 at cluster center 2 (100,0,0,0) returns brute force top-5 in order
      float[] queryVec = new float[] { 100.1f, 0.05f, -0.05f, 0.0f };
      List<String> expectedTop5 = VectorIndexTestUtil.bruteForceTopK(allRows, queryVec, "L2", 5);

      String query = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      List<String> actualTop5 = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT",
          new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] }));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualTop5.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedTop5, actualTop5);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE " + tableName);
      }
    }
  }

  @Test
  public void testIndexToolPopulation() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }

      Random rng = new Random(42);
      float[][] rawVectors = new float[500][4];
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 500; i++) {
          String id = String.format("row_%03d", i);
          Float[] vec = new Float[4];
          for (int d = 0; d < 4; d++) {
            float val = rng.nextFloat() * 10f;
            vec[d] = val;
            rawVectors[i][d] = val;
          }
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", vec));
          ps.setString(3, "label_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      }

      List<float[]> knownCentroids = Arrays.asList(new float[] { 10.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 0.0f, 10.0f, 0.0f, 0.0f }, new float[] { 0.0f, 0.0f, 10.0f, 0.0f },
        new float[] { 0.0f, 0.0f, 0.0f, 10.0f });
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 1L, knownCentroids);
      CentroidManager.setGenerationAndLists(conn, indexName, 1L, 4);

      VectorCentroidCache.resetInstance();

      IndexTool indexingTool = new IndexTool();
      Configuration conf = new Configuration(getUtility().getConfiguration());
      indexingTool.setConf(conf);
      String[] args = new String[] { "-dt", tableName, "-it", indexName, "-op",
        getUtility().getDataTestDir().toString() + "/it_" + generateUniqueName(), "-runfg" };
      int status = indexingTool.run(args);
      assertEquals(0, status);
      assertEquals(PhoenixIndexImportDirectMapper.class, indexingTool.getJob().getMapperClass());

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());

      byte[] physicalIndexName = pIndex.getPhysicalName().getBytes();
      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(pIndex);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(pIndex).getFirst();
      int hbaseRowCount = 0;
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName);
        ResultScanner scanner = hIndexTable.getScanner(new Scan())) {
        for (Result r : scanner) {
          hbaseRowCount++;
          byte[] rowKey = r.getRow();
          int centroidId = VectorIndexTestUtil.extractCentroidId(rowKey, false);
          String idStr = (String) PVarchar.INSTANCE.toObject(rowKey, Bytes.SIZEOF_INT,
            rowKey.length - Bytes.SIZEOF_INT);
          int rowIdx = Integer.parseInt(idStr.replace("row_", ""));
          int expectedCentroid =
            VectorIndexTestUtil.nearestCentroid(rawVectors[rowIdx], knownCentroids, "L2");
          assertEquals("Centroid ID for " + idStr + " mismatch", expectedCentroid, centroidId);

          byte[] emptyVal = r.getValue(emptyCF, emptyCQ);
          assertNotNull("Empty column must exist", emptyVal);
          assertTrue("Index row must be VERIFIED",
            Bytes.equals(QueryConstants.VERIFIED_BYTES, emptyVal));
        }
      }
      assertEquals(500, hbaseRowCount);
    }
  }

  @Test
  public void testIndexToolAutoTrainAndPopulate() throws Exception {
    String tableName = "T_VEC_AUTO_" + generateUniqueName();
    String indexName = "IDX_VEC_AUTO_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Table with V_OTHER (degenerate constant vectors) and V (clustered vectors), index on V
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V_OTHER VECTOR(FLOAT, 4), V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }

      int numClusters = 4;
      float[][] clusterCenters = new float[][] { { 0.0f, 0.0f, 0.0f, 0.0f },
        { 50.0f, 0.0f, 0.0f, 0.0f }, { 100.0f, 0.0f, 0.0f, 0.0f }, { 150.0f, 0.0f, 0.0f, 0.0f } };

      Random rng = new Random(123);
      Map<String, float[]> indexedVectors = new LinkedHashMap<>();
      String upsertSql =
        "UPSERT INTO " + tableName + " (ID, V_OTHER, V, LABEL) VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          int k = i % numClusters;
          float[] vOther = new float[] { 1.0f, 1.0f, 1.0f, 1.0f }; // degenerate
          float[] v = new float[] { clusterCenters[k][0] + (float) (rng.nextGaussian() * 0.5),
            (float) (rng.nextGaussian() * 0.5), (float) (rng.nextGaussian() * 0.5),
            (float) (rng.nextGaussian() * 0.5) };
          String id = String.format("row_%03d", i);
          indexedVectors.put(id, v);
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 1.0f, 1.0f, 1.0f }));
          ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.setString(4, "label_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      }

      VectorCentroidCache.resetInstance();

      IndexTool indexingTool = new IndexTool();
      Configuration conf = new Configuration(getUtility().getConfiguration());
      indexingTool.setConf(conf);
      String[] args = new String[] { "-dt", tableName, "-it", indexName, "-op",
        getUtility().getDataTestDir().toString() + "/it_" + generateUniqueName(), "-runfg" };
      int status = indexingTool.run(args);
      assertEquals(0, status);

      // Verify catalog VECTOR_CENTROID_GENERATION == 1 and VECTOR_IVF_LISTS == 4
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      assertEquals(Long.valueOf(1L), pIndex.getVectorCentroidGeneration());
      assertEquals(Integer.valueOf(4), pIndex.getVectorIvfLists());

      // Verify centroids were trained on V (not V_OTHER): trained centroids must match V's clusters
      List<byte[]> rawCentroids = CentroidManager.loadCentroids(conn, indexName, 1L);
      assertEquals(4, rawCentroids.size());
      List<float[]> trainedCentroids = new ArrayList<>();
      for (byte[] b : rawCentroids) {
        trainedCentroids.add(PVectorFloat.readElements(b, 0, b.length));
      }

      for (float[] tc : trainedCentroids) {
        double minD = Double.MAX_VALUE;
        for (int k = 0; k < numClusters; k++) {
          double d = VectorIndexTestUtil.dist("L2", tc, clusterCenters[k]);
          if (d < minD) minD = d;
        }
        assertTrue(
          "Trained centroid must match one of V's cluster centers, not V_OTHER: minD=" + minD,
          minD < 2.0);
      }

      // Assert each index row's centroid prefix equals nearestCentroid(v, trainedCentroids, "L2")
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, pIndex);
      assertEquals(100, rowKeys.size());
      for (byte[] rk : rowKeys) {
        int centroidId = VectorIndexTestUtil.extractCentroidId(rk, false);
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        float[] v = indexedVectors.get(id);
        assertNotNull(v);
        int expectedCid = VectorIndexTestUtil.nearestCentroid(v, trainedCentroids, "L2");
        assertEquals("Index row centroid ID must match nearest trained centroid", expectedCid,
          centroidId);
      }
    }
  }

  @Test
  public void testIndexToolRebuildWithGeneration() throws Exception {
    // Verify MapReduce index rebuild under a new generation and live query visibility.
    String tableName = "T_VEC_REB_" + generateUniqueName();
    String indexName = "IDX_VEC_REB_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.HandPlacedFixture fixture =
        VectorIndexTestUtil.buildProbeFixture(conn, tableName, indexName, null, false);

      // Under generation 1 centroids, probe=1 routes to centroid 0 and returns [A1, A2].
      String probeQuery = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      List<String> gen1Actual = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(probeQuery)) {
        Float[] boxed = new Float[] { fixture.queryVector[0], fixture.queryVector[1],
          fixture.queryVector[2], fixture.queryVector[3] };
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            gen1Actual.add(rs.getString(1));
          }
        }
      }
      assertEquals("Gen 1 probe=1 must return [A1, A2]", Arrays.asList("A1", "A2"), gen1Actual);

      // Persist Generation 2 centroids with labels swapped: C0=(10,0,0,0), C1=(0,0,0,0)
      List<float[]> gen2Centroids = Arrays.asList(new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, // ID 0
                                                                                           // is now
                                                                                           // at 10
        new float[] { 0.0f, 0.0f, 0.0f, 0.0f } // ID 1 is now at 0
      );
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 2L, gen2Centroids);
      CentroidManager.setGenerationAndLists(conn, indexName, 2L, 2);

      // Run IndexTool with -g 2
      IndexTool indexingTool = new IndexTool();
      Configuration conf = new Configuration(getUtility().getConfiguration());
      indexingTool.setConf(conf);
      String[] args = new String[] { "-dt", tableName, "-it", indexName, "-g", "2", "-op",
        getUtility().getDataTestDir().toString() + "/it_" + generateUniqueName(), "-deleteall",
        "-runfg" };
      int status = indexingTool.run(args);
      assertEquals(0, status);

      // Assert catalog VECTOR_CENTROID_GENERATION == 2
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      assertEquals(Long.valueOf(2L), pIndex.getVectorCentroidGeneration());

      // Assert every index row's prefix equals gen-2 assignment (A-rows under 1, B-rows under 0)
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, pIndex);
      assertEquals(6, rowKeys.size());
      for (byte[] rk : rowKeys) {
        int centroidId = VectorIndexTestUtil.extractCentroidId(rk, false);
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        if (id.startsWith("A")) {
          assertEquals("A-rows must be assigned to centroid 1 in Gen 2", 1, centroidId);
        } else {
          assertEquals("B-rows must be assigned to centroid 0 in Gen 2", 0, centroidId);
        }
      }

      // Verify probe queries observe generation 2 centroid assignments without manual cache
      // eviction.
      List<String> gen2Actual = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(probeQuery)) {
        Float[] boxed = new Float[] { fixture.queryVector[0], fixture.queryVector[1],
          fixture.queryVector[2], fixture.queryVector[3] };
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            gen2Actual.add(rs.getString(1));
          }
        }
      }
      assertEquals("Query under Gen 2 centroids must still return [A1, A2]",
        Arrays.asList("A1", "A2"), gen2Actual);
    }
  }

  @Test
  public void testCentroidGenerationTracksIndexMetadataNotInMemoryCache() throws Exception {
    // Simulates a rebuild whose generation bump was committed to the catalog by another process.
    String tableName = "T_VEC_GEN_" + generateUniqueName();
    String indexName = "IDX_VEC_GEN_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.HandPlacedFixture fixture =
        VectorIndexTestUtil.buildProbeFixture(conn, tableName, indexName, null, false);
      Float[] boxed = new Float[] { fixture.queryVector[0], fixture.queryVector[1],
        fixture.queryVector[2], fixture.queryVector[3] };
      String probeQuery = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";

      // Under generation 1 centroids, a single-probe query selects centroid 0.
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(probeQuery).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        VectorIndexScanPlan gen1Plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertEquals(Arrays.asList(0), gen1Plan.getProbeCentroids());
      }

      // Persist generation 2 to the catalog directly without mutating local in-memory cache
      // structures.
      List<float[]> gen2Centroids = Arrays.asList(new float[] { 10.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 0.0f, 0.0f, 0.0f, 0.0f });
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 2L, gen2Centroids);
      CentroidManager.setGenerationAndLists(conn, indexName, 2L, 2);

      // Invalidate connection table cache to simulate metadata synchronization after a rebuild.
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      PTable refreshed = pconn.getTableNoCache(indexName);
      assertEquals(Long.valueOf(2L), refreshed.getVectorCentroidGeneration());

      // Verify query compilation evaluates against generation 2 centroids.
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(probeQuery).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        VectorIndexScanPlan gen2Plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertEquals("Probe selection must reflect Gen 2's (swapped) centroid positions, not a"
          + " stale in-memory Gen 1", Arrays.asList(1), gen2Plan.getProbeCentroids());
      }
    }
  }

  @Test
  public void testQueryWithColdCentroidCache() throws Exception {
    // Verify query execution when the client centroid cache is uninitialized.
    String tableName = "T_VEC_COLD_" + generateUniqueName();
    String indexName = "IDX_VEC_COLD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.HandPlacedFixture fixture =
        VectorIndexTestUtil.buildProbeFixture(conn, tableName, indexName, null, false);

      // Reset client cache and thread locals
      VectorCentroidCache.resetInstance();
      CentroidManager.clearThreadLocalConnection();
      CentroidManager.setDefaultConnection(null);

      // Run probe=1 query with cold cache
      String query = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      Float[] boxed = new Float[] { fixture.queryVector[0], fixture.queryVector[1],
        fixture.queryVector[2], fixture.queryVector[3] };

      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        ResultSet rs = ps.executeQuery();
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertTrue("EXPLAIN must contain CLIENT PROBING 1 OF 2: " + explainPlan,
          explainPlan.contains("CLIENT PROBING 1 OF 2"));
      }

      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertTrue("VectorIndexScanPlan must be probing even with cold cache", plan.isProbing());
      }

      List<String> actual = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actual.add(rs.getString(1));
          }
        }
      }
      assertEquals("Cold cache probe=1 query must return [A1, A2]", Arrays.asList("A1", "A2"),
        actual);
    }
  }

  @Test
  public void testWritePathAfterCacheReset() throws Exception {
    // Verify write path correctly resolves centroids and populates index rows when cache is reset.
    String tableName = "T_VEC_WR_RST_" + generateUniqueName();
    String indexName = "IDX_VEC_WR_RST_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.HandPlacedFixture fixture =
        VectorIndexTestUtil.buildProbeFixture(conn, tableName, indexName, null, false);

      VectorCentroidCache.resetInstance();
      CentroidManager.clearThreadLocalConnection();
      CentroidManager.setDefaultConnection(null);

      // Upsert a new row C1 with vector [1.0, 0, 0, 0] -> nearest centroid is C0 (0,0,0,0)
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V) VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "C1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.executeUpdate();
      }
      conn.commit();

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected 7 rows in index", 7, rowKeys.size());

      boolean foundC1 = false;
      for (byte[] rk : rowKeys) {
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        if ("C1".equals(id)) {
          foundC1 = true;
          int cid = VectorIndexTestUtil.extractCentroidId(rk, false);
          assertEquals("C1 must be assigned to centroid 0", 0, cid);
        }
      }
      assertTrue("Row C1 must exist in index table", foundC1);
    }
  }
}
