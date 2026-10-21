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

import static org.apache.phoenix.end2end.VectorIndexTestUtil.assertIndexVerifies;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.countCentroids;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.extractCentroidId;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.findNearestCentroid;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.getHBaseRowKeys;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.loadClusteredVectors;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.loadRandomVectors;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.VectorIndexRebuilder;
import org.apache.phoenix.index.vector.VectorIndexTrainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the life cycle of an IVF vector index. The tests cover synchronous
 * training, index population, activation, offline builds with IndexTool, and the centroid cache.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorIvfLifecycleIT extends ParallelStatsDisabledIT {

  /**
   * Tests that a synchronous CREATE VECTOR INDEX trains and records a centroid generation. The
   * server must write a verified index row for each data row, and the index must become ACTIVE.
   */
  @Test
  public void testSynchronousVectorIndexPopulationAndActivation() throws Exception {
    String tableName = "T_VEC_POP_" + generateUniqueName();
    String indexName = "IDX_VEC_POP_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }
      loadClusteredVectors(conn, tableName, 100);
      long before = System.currentTimeMillis();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) " + "INCLUDE (LABEL) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable index = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, index.getIndexState());
      Long generation = index.getVectorCentroidGeneration();
      assertNotNull("Training records a generation", generation);
      assertTrue("The first generation is the training time", generation >= before);
      int lists = index.getVectorIvfLists();
      assertTrue(lists >= 4);
      assertEquals(lists, countCentroids(conn, indexName, generation));

      String sql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      int rows = 0;
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        while (rs.next()) {
          rows++;
          assertTrue(rs.getInt(1) >= 0 && rs.getInt(1) < lists);
          assertNotNull(rs.getString(2));
          assertNotNull(rs.getString(3));
        }
      }
      assertEquals(100, rows);
      IndexTestUtil.assertRowsForEmptyColValue(conn, indexName, QueryConstants.VERIFIED_BYTES);
      assertIndexVerifies(tableName, indexName, 100);
    }
  }

  /**
   * Tests that IndexTool trains the first generation of an ASYNC vector index. Each index row must
   * be under its nearest centroid, and the index must become ACTIVE.
   */
  @Test
  public void testIndexToolTrainsAndBuilds() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      float[][] vectors = loadRandomVectors(conn, tableName, null, null, 500, 42);
      conn.createStatement()
        .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (LABEL)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.BUILDING, pIndex.getIndexState());
      assertNull(pIndex.getVectorCentroidGeneration());

      // Run the IndexTool build job
      IndexToolIT.runIndexTool(false, null, tableName, indexName);

      pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      Long generation = pIndex.getVectorCentroidGeneration();
      assertNotNull("IndexTool trains the first generation", generation);
      List<float[]> centroids = CentroidManager.loadCentroids(conn, indexName, generation);
      assertEquals(pIndex.getVectorIvfLists().intValue(), centroids.size());

      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(pIndex);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(pIndex).getFirst();
      int rows = 0;
      try (
        Table hIndexTable = pconn.getQueryServices().getTable(pIndex.getPhysicalName().getBytes());
        ResultScanner scanner = hIndexTable.getScanner(new Scan())) {
        for (Result r : scanner) {
          rows++;
          byte[] rowKey = r.getRow();
          String id = (String) PVarchar.INSTANCE.toObject(rowKey, Bytes.SIZEOF_INT,
            rowKey.length - Bytes.SIZEOF_INT);
          int rowIdx = Integer.parseInt(id.replace("row_", ""));
          assertEquals("Centroid of " + id, findNearestCentroid(vectors[rowIdx], centroids),
            extractCentroidId(rowKey));
          assertArrayEquals(QueryConstants.VERIFIED_BYTES, r.getValue(emptyCF, emptyCQ));
        }
      }
      assertEquals(500, rows);
      assertIndexVerifies(tableName, indexName, 500);
    }
  }

  /** Tests that IndexTool does not train if the table has fewer vectors than the IVF lists. */
  @Test
  public void testIndexToolDefersTrainingOnTooFewVectors() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      loadRandomVectors(conn, tableName, null, null, 2, 7);
      conn.createStatement().execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
        + " (V)" + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      IndexTool tool = new IndexTool();
      tool.setConf(new Configuration(getUtility().getConfiguration()));
      assertEquals(0, tool.run(new String[] { "-dt", tableName, "-it", indexName, "-runfg" }));
      PTable pIndex = conn.unwrap(PhoenixConnection.class).getTableNoCache(indexName);
      assertEquals(PIndexState.BUILDING, pIndex.getIndexState());
      assertNull(pIndex.getVectorCentroidGeneration());
    }
  }

  /**
   * Tests that IndexTool trains and builds the first generation under the rebuild claim of the
   * index. IndexTool fails while another process holds the claim, and releases the claim after it
   * completes.
   */
  @Test
  public void testIndexToolTrainsUnderRebuildClaim() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      loadRandomVectors(conn, tableName, null, null, 100, 42);
      conn.createStatement().execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
        + " (V)" + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      String[] args = { "-dt", tableName, "-it", indexName, "-runfg" };
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      assertTrue(CentroidManager.claimRebuild(conn, indexName, "other", 60000));
      IndexTool tool = new IndexTool();
      tool.setConf(new Configuration(getUtility().getConfiguration()));
      assertEquals(-1, tool.run(args));
      assertNull(pconn.getTableNoCache(indexName).getVectorCentroidGeneration());
      CentroidManager.releaseRebuild(conn, indexName, "other");
      tool = new IndexTool();
      tool.setConf(new Configuration(getUtility().getConfiguration()));
      assertEquals(0, tool.run(args));
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      assertNotNull(pIndex.getVectorCentroidGeneration());
      assertTrue("IndexTool releases the claim",
        CentroidManager.claimRebuild(conn, indexName, "after", 60000));
      CentroidManager.releaseRebuild(conn, indexName, "after");
      assertIndexVerifies(tableName, indexName, 100);
    }
  }

  /**
   * Tests that a synchronous CREATE VECTOR INDEX that finds the rebuild claim of the index held
   * does not train or build the index. The index stays BUILDING for the claim holder. A first
   * generation training that gets the claim after another process trained the index returns
   * UP_TO_DATE, so CREATE does not build the index again.
   */
  @Test
  public void testSynchronousCreateDefersToRebuildClaimHolder() throws Exception {
    String tableName = "T_VEC_POP_" + generateUniqueName();
    String indexName = "IDX_VEC_POP_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      loadClusteredVectors(conn, tableName, 100);
      assertTrue(CentroidManager.claimRebuild(conn, indexName, "other", 60000));
      conn.createStatement().execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
        + " (V)" + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.BUILDING, pIndex.getIndexState());
      assertNull(pIndex.getVectorCentroidGeneration());
      assertTrue(getHBaseRowKeys(pconn, pIndex).isEmpty());
      CentroidManager.releaseRebuild(conn, indexName, "other");
      conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + tableName + " REBUILD");
      PTable rebuilt = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, rebuilt.getIndexState());
      assertNotNull(rebuilt.getVectorCentroidGeneration());
      assertEquals(VectorIndexRebuilder.Outcome.UP_TO_DATE,
        VectorIndexTrainer.trainFirstGeneration(pconn, pconn.getTableNoCache(tableName), pIndex));
      assertEquals(rebuilt.getVectorCentroidGeneration(),
        pconn.getTableNoCache(indexName).getVectorCentroidGeneration());
      assertIndexVerifies(tableName, indexName, 100);
    }
  }

  /**
   * Tests that a synchronous CREATE VECTOR INDEX holds the rebuild claim of the index through the
   * build that follows training. An ALTER INDEX ... REBUILD during that build must find the rebuild
   * IN_PROGRESS and must not run concurrently with the build. The build waits for the index
   * population sleep time between its two passes. During that wait, the rows of the first pass are
   * visible and CREATE still runs.
   */
  @Test
  public void testSynchronousCreateHoldsRebuildClaimThroughBuild() throws Exception {
    String tableName = "T_VEC_POP_" + generateUniqueName();
    String indexName = "IDX_VEC_POP_" + generateUniqueName();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      loadClusteredVectors(conn, tableName, 100);
      Future<?> create = executor.submit(() -> {
        try (Connection createConn = DriverManager.getConnection(getUrl())) {
          createConn.createStatement()
            .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V)"
              + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
        }
        return null;
      });
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable building = null;
      while (building == null || getHBaseRowKeys(pconn, building).isEmpty()) {
        assertFalse("CREATE writes index rows before its build completes", create.isDone());
        Thread.sleep(10);
        try {
          building = pconn.getTableNoCache(indexName);
        } catch (TableNotFoundException e) {
          // CREATE did not add the index to the catalog yet
        }
      }
      try {
        conn.createStatement()
          .execute("ALTER INDEX " + indexName + " ON " + tableName + " REBUILD");
        fail("ALTER INDEX ... REBUILD ran while CREATE built the index");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("IN_PROGRESS"));
      }
      assertFalse("CREATE was building when the rebuild found the claim held", create.isDone());
      create.get();
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      assertNotNull(pIndex.getVectorCentroidGeneration());
      assertTrue("CREATE releases the claim",
        CentroidManager.claimRebuild(conn, indexName, "after", 60000));
      CentroidManager.releaseRebuild(conn, indexName, "after");
      assertIndexVerifies(tableName, indexName, 100);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Tests that an IndexTool run that builds no index rows does not train the index. Verify-only
   * runs and runs that read the index as the source are such runs.
   */
  @Test
  public void testIndexToolVerifyOnlyDoesNotTrain() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      loadRandomVectors(conn, tableName, null, null, 500, 42);
      conn.createStatement().execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
        + " (V)" + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      for (String[] verifyOnly : new String[][] { { "-v", "ONLY" }, { "-fi", "-v", "AFTER" } }) {
        List<String> args =
          new ArrayList<>(Arrays.asList("-dt", tableName, "-it", indexName, "-runfg"));
        args.addAll(Arrays.asList(verifyOnly));
        IndexTool tool = new IndexTool();
        tool.setConf(new Configuration(getUtility().getConfiguration()));
        assertEquals(0, tool.run(args.toArray(new String[0])));
        PTable pIndex = conn.unwrap(PhoenixConnection.class).getTableNoCache(indexName);
        assertEquals(PIndexState.BUILDING, pIndex.getIndexState());
        assertNull(pIndex.getVectorCentroidGeneration());
        assertEquals(0, countCentroids(conn, indexName, null));
      }
    }
  }

  /**
   * Tests a global IndexTool build of a multitenant vector index. Each index row key must start
   * with the tenant ID, followed by the ID of the nearest centroid.
   */
  @Test
  public void testIndexToolBuildsMultiTenantVectorIndex() throws Exception {
    String tableName = "T_VEC_MT_" + generateUniqueName();
    String indexName = "IDX_VEC_MT_" + generateUniqueName();
    String[] tenants = { "TA", "TB" };
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL,"
          + " V VECTOR(FLOAT, 4), LABEL VARCHAR CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))"
          + " MULTI_TENANT = true");
      float[][] vectors = loadRandomVectors(conn, tableName, "TENANT_ID", tenants, 200, 11);
      conn.createStatement()
        .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (LABEL)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");

      IndexToolIT.runIndexTool(false, null, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      List<float[]> centroids =
        CentroidManager.loadCentroids(conn, indexName, pIndex.getVectorCentroidGeneration());
      int rows = 0;
      try (
        Table hIndexTable = pconn.getQueryServices().getTable(pIndex.getPhysicalName().getBytes());
        ResultScanner scanner = hIndexTable.getScanner(new Scan())) {
        for (Result r : scanner) {
          rows++;
          byte[] rowKey = r.getRow();
          // The row key is [TENANT_ID][separator][centroid ID][ID]
          int sep = Bytes.indexOf(rowKey, QueryConstants.SEPARATOR_BYTE);
          String tenant = Bytes.toString(rowKey, 0, sep);
          int centroid = (Integer) PInteger.INSTANCE.toObject(rowKey, sep + 1, Bytes.SIZEOF_INT);
          String id = Bytes.toString(rowKey, sep + 1 + Bytes.SIZEOF_INT,
            rowKey.length - sep - 1 - Bytes.SIZEOF_INT);
          int rowIdx = Integer.parseInt(id.replace("row_", ""));
          assertEquals(tenants[rowIdx % tenants.length], tenant);
          assertEquals("Centroid of " + id, findNearestCentroid(vectors[rowIdx], centroids),
            centroid);
        }
      }
      assertEquals(200, rows);
      assertIndexVerifies(tableName, indexName, 200);
    }
  }

  /**
   * Tests the full life cycle on clustered data. The trained centroids must be near the cluster
   * centers, and each index row must be under its nearest centroid. A top 5 query must give the
   * brute force result.
   */
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
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)")) {
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
          }
        }
      }
      conn.commit();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 400)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable index = pconn.getTableNoCache(indexName);
      List<float[]> trainedCentroids =
        CentroidManager.loadCentroids(conn, indexName, index.getVectorCentroidGeneration());
      assertEquals(4, trainedCentroids.size());
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
      assertEquals(4, matchedClusters.size());

      String selectIndex = "SELECT \"_CENTROID_ID\", \":ID\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectIndex)) {
        int count = 0;
        while (rs.next()) {
          count++;
          float[] v = allRows.get(rs.getString(2));
          assertNotNull(v);
          assertEquals(VectorIndexTestUtil.nearestCentroid(v, trainedCentroids, "L2"),
            rs.getInt(1));
        }
        assertEquals(400, count);
      }

      float[] queryVec = new float[] { 100.1f, 0.05f, -0.05f, 0.0f };
      List<String> expectedTop5 = VectorIndexTestUtil.bruteForceTopK(allRows, queryVec, "L2", 5);
      List<String> actualTop5 = new ArrayList<>();
      try (PreparedStatement ps = conn
        .prepareStatement("SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5")) {
        ps.setArray(1, conn.createArrayOf("FLOAT",
          new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] }));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualTop5.add(rs.getString(1));
          }
        }
      }
      assertEquals(expectedTop5, actualTop5);
    }
  }

  /**
   * Tests that query compilation uses the centroid generation in the index metadata. After the test
   * records a new generation, the plan probes by the new centroids, although the centroid cache
   * still holds the old generation.
   */
  @Test
  public void testCentroidGenerationTracksIndexMetadataNotInMemoryCache() throws Exception {
    String tableName = "T_VEC_GEN_" + generateUniqueName();
    String indexName = "IDX_VEC_GEN_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.HandPlacedFixture fixture =
        VectorIndexTestUtil.buildProbeFixture(conn, tableName, indexName, null, false);
      Float[] boxed = new Float[] { fixture.queryVector[0], fixture.queryVector[1],
        fixture.queryVector[2], fixture.queryVector[3] };
      String probeQuery = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(probeQuery).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertArrayEquals(new int[] { 0 }, plan.getProbeCentroids());
      }

      // Record a new generation that swaps the positions of the two centroids
      long generation = VectorIndexTestUtil.recordKnownCentroids(conn, indexName, Arrays
        .asList(new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 0.0f, 0.0f, 0.0f, 0.0f }));
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
      assertEquals(Long.valueOf(generation),
        pconn.getTableNoCache(indexName).getVectorCentroidGeneration());
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(probeQuery).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertArrayEquals(new int[] { 1 }, plan.getProbeCentroids());
      }
    }
  }

  /** Tests a vector query after the centroid cache drops the index. */
  @Test
  public void testQueryWithColdCentroidCache() throws Exception {
    String tableName = "T_VEC_COLD_" + generateUniqueName();
    String indexName = "IDX_VEC_COLD_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.HandPlacedFixture fixture =
        VectorIndexTestUtil.buildProbeFixture(conn, tableName, indexName, null, false);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      VectorCentroidCache.getInstance(pconn.getQueryServices().getConfiguration())
        .invalidate(indexName);

      String query = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
      Float[] boxed = new Float[] { fixture.queryVector[0], fixture.queryVector[1],
        fixture.queryVector[2], fixture.queryVector[3] };
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed));
        String explainPlan = QueryUtil.getExplainPlan(ps.executeQuery());
        assertTrue(explainPlan, explainPlan.contains("CLIENT PROBING 1 OF 2"));
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
      assertEquals(Arrays.asList("A1", "A2"), actual);
    }
  }

  /** Tests centroid assignment on the write path after the centroid cache drops the index. */
  @Test
  public void testWritePathAfterCacheReset() throws Exception {
    String tableName = "T_VEC_WR_RST_" + generateUniqueName();
    String indexName = "IDX_VEC_WR_RST_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.buildProbeFixture(conn, tableName, indexName, null, false);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      VectorCentroidCache.getInstance(pconn.getQueryServices().getConfiguration())
        .invalidate(indexName);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V) VALUES (?, ?)")) {
        ps.setString(1, "C1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeys = getHBaseRowKeys(pconn, pconn.getTableNoCache(indexName));
      assertEquals(7, rowKeys.size());
      boolean foundC1 = false;
      for (byte[] rk : rowKeys) {
        if ("C1".equals(Bytes.toString(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT))) {
          foundC1 = true;
          assertEquals(0, extractCentroidId(rk));
        }
      }
      assertTrue(foundC1);
    }
  }
}
