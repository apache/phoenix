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

import static org.apache.phoenix.end2end.IndexFsckToolIT.run;
import static org.apache.phoenix.end2end.IvfIndexFsckIT.knownCentroidIndex;
import static org.apache.phoenix.end2end.IvfIndexFsckIT.near;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.KNOWN_CENTROIDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckTool;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End-to-end integration tests for {@link IndexFsckTool} introspection subcommands on IVF vector
 * indexes.
 */
@Category(ParallelStatsDisabledTest.class)
public class IvfIndexInspectIT extends ParallelStatsDisabledIT {

  @SuppressWarnings("unchecked")
  private static <T> T get(Report report, String key) {
    return (T) report.getInspection().get(key);
  }

  /** Initiates a test centroid migration to a subsequent generation. */
  private static long startMigration(Connection conn, String index) throws Exception {
    try (PhoenixConnection internal =
      CentroidManager.newInternalConnection(conn.unwrap(PhoenixConnection.class))) {
      PTable pindex = internal.getTableNoCache(index);
      long building = CentroidManager.nextGeneration(pindex.getVectorCentroidGeneration());
      CentroidManager.persistCentroids(internal, index, building, KNOWN_CENTROIDS,
        KNOWN_CENTROIDS.size());
      CentroidManager.persistGenerationSummary(internal, index, building,
        new GenerationSummary(GenerationSummary.BUILDING, "TEST", 4, null, null, null));
      CentroidManager.setBuildingGeneration(internal, pindex, building);
      return building;
    }
  }

  @Test
  public void testGenerationsAndExport() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      long active =
        conn.unwrap(PhoenixConnection.class).getTableNoCache(index).getVectorCentroidGeneration();
      long building = startMigration(conn, index);

      Report report = run(0, "inspect", "generations", "-dt", table, "-it", index);
      assertEquals(active, (long) get(report, "activeGeneration"));
      assertEquals(building, (long) get(report, "buildingGeneration"));
      List<Map<String, Object>> generations = get(report, "generations");
      assertEquals(2, generations.size());
      assertEquals("ACTIVE", generations.get(0).get("role"));
      assertEquals(Arrays.asList(0, 3),
        Arrays.asList(generations.get(0).get("firstId"), generations.get(0).get("lastId")));
      assertEquals("BUILDING", generations.get(1).get("role"));
      assertEquals(Arrays.asList(4, 7),
        Arrays.asList(generations.get(1).get("firstId"), generations.get(1).get("lastId")));
      List<Map<String, Object>> tasks = get(report, "tasks");
      assertTrue(tasks.stream()
        .anyMatch(t -> t.get("type").toString().equals("VECTOR_SCORECARD_RECONCILE")));

      IndexFsckTool tool = new IndexFsckTool();
      tool.setConf(config);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      tool.setOutStream(new PrintStream(out, true));
      assertEquals(0,
        tool.run(new String[] { "inspect", "export", "-dt", table, "-it", index, "-of", "JSON" }));
      JsonNode json = new ObjectMapper().readTree(out.toString());
      assertEquals(Report.SCHEMA_VERSION, json.get("schemaVersion").asText());
      JsonNode exported = json.get("inspection").get("generations");
      assertEquals(2, exported.size());
      assertEquals(4, exported.get(1).get("centroids").size());
      assertEquals(4, exported.get(1).get("centroids").get(0).get("id").asInt());
      assertEquals("B", exported.get(1).get("summary").get("rebuildState").asText());
    }
  }

  @Test
  public void testScorecardAndPostings() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      Report scorecard = run(0, "inspect", "scorecard", "-dt", table, "-it", index);
      List<Map<String, Object>> rows = get(scorecard, "scorecard");
      assertEquals(4, rows.size());
      for (Map<String, Object> row : rows) {
        assertEquals(2L, row.get("rows"));
        assertEquals(row.get("rows"), row.get("clusterSize"));
      }

      // Inject an unverified posting entry to test raw postings inspection
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IvfIndexFsckIT.copyIndexRow(pconn, index, "r0_0", 2, false);
      PTable pindex = pconn.getTableNoCache(index);
      try (Table hTable = pconn.getQueryServices().getTable(pindex.getPhysicalName().getBytes());
        ResultScanner scanner = hTable.getScanner(new Scan())) {
        for (Result result : scanner) {
          if (
            VectorIndexTestUtil.extractCentroidId(result.getRow()) == 2
              && new String(result.getRow()).endsWith("r0_0")
          ) {
            Put put = new Put(result.getRow());
            put.addColumn(SchemaUtil.getEmptyColumnFamily(pindex),
              EncodedColumnsUtil.getEmptyKeyValueInfo(pindex).getFirst(),
              result.rawCells()[0].getTimestamp(), QueryConstants.UNVERIFIED_BYTES);
            hTable.put(put);
          }
        }
      }
      Report postings = run(0, "inspect", "postings", "2", "-dt", table, "-it", index);
      assertEquals(3L, (long) get(postings, "rows"));
      assertEquals(1L, (long) get(postings, "unverifiedRows"));
      assertEquals(1, (int) get(postings, "regions"));
      List<Map<String, Object>> sample = get(postings, "sample");
      assertTrue(sample.get(0).get("key").toString().contains("_CENTROID_ID=2"));
      Report hex = run(0, "inspect", "postings", "2", "-dt", table, "-it", index, "-k", "HEX");
      List<Map<String, Object>> hexSample = get(hex, "sample");
      assertTrue(hexSample.get(0).get("key").toString().startsWith("00000002")
        || hexSample.get(0).get("key").toString().startsWith("80000002"));
    }
  }

  @Test
  public void testLookupByCompositeKey() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table + " (K1 VARCHAR NOT NULL, "
        + "K2 INTEGER NOT NULL, V VECTOR(FLOAT, 4), LABEL VARCHAR CONSTRAINT PK PRIMARY KEY (K1, K2))");
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 10)");
      VectorIndexTestUtil.activateWithKnownCentroids(conn, table, index, KNOWN_CENTROIDS);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + table + " (K1, K2, V) VALUES (?, ?, ?)")) {
        for (int c = 0; c < 4; c++) {
          float[] v = near(c, 1);
          Float[] boxed = new Float[v.length];
          for (int i = 0; i < v.length; i++) {
            boxed[i] = v[i];
          }
          ps.setString(1, "a");
          ps.setInt(2, c);
          ps.setArray(3, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
        }
      }
      conn.commit();
      Report lookup = run(0, "inspect", "lookup", "a", "3", "-dt", table, "-it", index);
      List<Map<String, Object>> assignments = get(lookup, "assignments");
      assertEquals(1, assignments.size());
      assertEquals(3, assignments.get(0).get("centroidId"));
      assertTrue((double) assignments.get(0).get("margin") > 0);
      assertEquals(Collections.singletonList(3), get(lookup, "indexRows"));
      run(-1, "inspect", "lookup", "a", "-dt", table, "-it", index);
    }
  }

  @Test
  public void testProbeOrderAndRecall() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      knownCentroidIndex(conn, table, index);
      Report probe = run(0, "inspect", "probe", "[1,0,0,0]", "-dt", table, "-it", index);
      List<Map<String, Object>> order = get(probe, "probeOrder");
      assertEquals(2, order.get(0).get("centroidId"));
      assertEquals(2L, order.get(0).get("rows"));
      List<Double> recall = get(probe, "recallByProbeCount");
      assertEquals(1.0, recall.get(recall.size() - 1), 0);
      assertEquals(4, (int) get(probe, "probesForFullRecall"));
      assertEquals(8, ((List<?>) get(probe, "exactNearest")).size());

      long active =
        conn.unwrap(PhoenixConnection.class).getTableNoCache(index).getVectorCentroidGeneration();
      long building = startMigration(conn, index);
      float[] q = { 0.2f, 0.9f, 0.1f, 0.3f };
      CachedCentroids activeModel = new CachedCentroids(KNOWN_CENTROIDS, DistanceMetric.L2, 0);
      CachedCentroids buildingModel = new CachedCentroids(KNOWN_CENTROIDS, DistanceMetric.L2, 4);
      int[] expected =
        VectorIndexScanPlan.interleave(activeModel.nearest(q, 4), buildingModel.nearest(q, 4));
      probe = run(0, "inspect", "probe", "[0.2,0.9,0.1,0.3]", "-dt", table, "-it", index);
      List<Integer> actual = new ArrayList<>();
      for (Map<String, Object> p : (List<Map<String, Object>>) get(probe, "probeOrder")) {
        actual.add((Integer) p.get("centroidId"));
      }
      List<Integer> expectedList = new ArrayList<>();
      for (int id : expected) {
        expectedList.add(id);
      }
      assertEquals(expectedList, actual);
      assertEquals(active,
        (long) ((List<Map<String, Object>>) get(probe, "probeOrder")).get(0).get("generation"));
      assertEquals(building,
        (long) ((List<Map<String, Object>>) get(probe, "probeOrder")).get(1).get("generation"));
    }
  }
}
