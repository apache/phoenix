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

import static org.apache.phoenix.end2end.HnswIndexFsckIT.createAndBuild;
import static org.apache.phoenix.end2end.IndexFsckToolIT.run;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.mapreduce.index.fsck.Finding;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.Severity;
import org.apache.phoenix.mapreduce.index.fsck.hnsw.HnswIndexFsckProvider;
import org.apache.phoenix.query.QueryConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the inspect commands of {@code IndexFsckTool} on HNSW indexes, and for the
 * way that the tools find the segments of each region.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswIndexInspectIT extends ParallelStatsDisabledIT {

  static Report inspect(String table, String index, String... command) throws Exception {
    List<String> args = new ArrayList<>(Arrays.asList("inspect", "-dt", table, "-it", index));
    args.addAll(Arrays.asList(command));
    return run(0, args.toArray(new String[0]));
  }

  @SuppressWarnings("unchecked")
  static List<Map<String, Object>> list(Report report, String key) {
    return (List<Map<String, Object>>) report.getInspection().get(key);
  }

  static float[] vectorOf(Connection conn, String table, String id) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement("SELECT V FROM " + table + " WHERE ID = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return org.apache.phoenix.index.vector.VectorIndexTrainer.toFloats(rs.getObject(1));
      }
    }
  }

  static String text(float[] v) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < v.length; i++) {
      sb.append(i == 0 ? "" : ",").append(v[i]);
    }
    return sb.append("]").toString();
  }

  @Test
  public void testInspectCommands() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 100, "");

      List<Map<String, Object>> segments = list(inspect(table, index, "list"), "segments");
      assertEquals(2, segments.size());
      List<HRegion> regions = HnswIndexIT.regions(table);
      for (int i = 0; i < segments.size(); i++) {
        Map<String, Object> s = segments.get(i);
        assertEquals(50, s.get("rows"));
        assertEquals("base", s.get("kind"));
        assertTrue(s.toString(), ((Number) s.get("payloadBytes")).longValue() > 1024);
        // Each region holds its own segment in its anchor row, and is the writer of that segment
        RegionInfo region = regions.get(i).getRegionInfo();
        assertEquals(region.getEncodedName(), s.get("region"));
        assertEquals(region.getEncodedName(), s.get("writer"));
        assertEquals(
          Bytes.toStringBinary(HnswSegment.anchor(region.getStartKey(), region.getEndKey())),
          s.get("anchor"));
        assertEquals(true, s.get("currentDefinition"));
      }

      Report coverage = inspect(table, index, "coverage");
      for (Map<String, Object> r : list(coverage, "regions")) {
        assertEquals(r.toString(), true, r.get("exact"));
        assertEquals(r.toString(), true, r.get("covered"));
        assertEquals(1, ((List<?>) r.get("segments")).size());
      }
      assertTrue(((List<?>) coverage.getInspection().get("retirable")).isEmpty());

      Report dump = inspect(table, index, "dump", "nodes", "dot");
      long segmentTime = 0;
      for (Map<String, Object> s : list(dump, "segments")) {
        segmentTime = (Long) s.get("time");
        assertEquals(16, s.get("dimension"));
        assertEquals("484e5357", s.get("trailerMagic"));
        int nodes = 0;
        for (Object n : ((Map<?, ?>) s.get("degreeHistogram")).values()) {
          nodes += (Integer) n;
        }
        assertEquals(50, nodes);
        assertEquals(50, ((List<?>) s.get("nodes")).size());
        assertTrue(((String) s.get("dot")).contains(" -> "));
      }
      assertEquals(1,
        list(inspect(table, index, "dump", "time=" + segmentTime), "segments").size());

      Path directory = Files.createTempDirectory("hnsw-export");
      Report export = inspect(table, index, "export", directory.toString());
      assertEquals(4, ((List<?>) export.getInspection().get("files")).size());
      ObjectMapper mapper = new ObjectMapper();
      int payloads = 0;
      for (Object file : (List<?>) export.getInspection().get("files")) {
        Path path = Paths.get(file.toString());
        if (path.toString().endsWith(".json")) {
          JsonNode metadata =
            mapper.readTree(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
          assertEquals(50, metadata.get("rows").asInt());
          assertEquals("L2", metadata.get("index").get("metric").asText());
          byte[] payload = Files.readAllBytes(directory.resolve(metadata.get("payload").asText()));
          assertEquals(0x484E5357, Bytes.toInt(payload, payload.length - 4));
          // The file name is the segment time and the MD5 of the identity and the anchor row. The
          // JSON holds both
          String base = metadata.get("payload").asText().replace(".hnsw", "");
          assertEquals(base + ".json", path.getFileName().toString());
          assertTrue(base, base.startsWith(metadata.get("time").asText() + "-"));
          assertTrue(metadata.get("identity").asText().length() > 0);
          assertTrue(metadata.get("anchor").asText().length() > 0);
          payloads++;
        }
      }
      assertEquals(2, payloads);

      float[] query = vectorOf(conn, table, "a000");
      Report search = inspect(table, index, "search", text(query), "k=5");
      List<Map<String, Object>> results = list(search, "results");
      assertEquals(5, results.size());
      assertTrue(results.get(0).toString(), results.get(0).get("key").toString().contains("a000"));
      assertEquals(0f, (Float) results.get(0).get("score") - 1f, 1e-6);
      for (Map<String, Object> r : results) {
        assertEquals((Float) r.get("score"), (Float) r.get("baseScore"), 1e-6);
      }
      assertEquals(1.0, (Double) search.getInspection().get("recall"), 0);
      assertTrue((Long) search.getInspection().get("visited") > 0);
      Path file = Files.createTempFile("hnsw-query", ".txt");
      Files.write(file, text(query).getBytes(StandardCharsets.UTF_8));
      assertEquals(results.get(0).get("key"),
        list(inspect(table, index, "search", "@" + file), "results").get(0).get("key"));
      assertEquals(results.get(0).get("key"),
        list(inspect(table, index, "search", "a000"), "results").get(0).get("key"));

      Report lookup = inspect(table, index, "lookup", "a000");
      List<Map<String, Object>> entries = list(lookup, "entries");
      assertEquals(1, entries.size());
      Map<String, Object> entry = entries.get(0);
      assertEquals(0, entry.get("ordinal"));
      assertEquals(true, entry.get("live"));
      assertEquals(true, entry.get("inRegion"));
      assertEquals(false, entry.get("diverges"));
      assertEquals(0.0, (Double) entry.get("divergence"), 0);
      assertTrue(entry.get("key").toString(), entry.get("key").toString().contains("ID=a000"));
      long lowerTime = (Long) entry.get("time");
      Map<String, Object> byOrdinal =
        list(inspect(table, index, "lookup", "time=" + lowerTime, "0"), "entries").get(0);
      assertEquals(entry.get("key"), byOrdinal.get("key"));
      assertEquals(true, byOrdinal.get("baseRowHasVector"));

      Map<String, Object> neighbors =
        list(inspect(table, index, "neighbors", "a000"), "entries").get(0);
      assertFalse(((List<?>) neighbors.get("forward")).isEmpty());
      assertFalse(((List<?>) neighbors.get("reverse")).isEmpty());
      for (Object edge : (List<?>) neighbors.get("forward")) {
        assertTrue(edge.toString(), ((Map<?, ?>) edge).get("key").toString().contains("ID=a"));
      }

      HnswIndexIT.upsert(conn, table, "z900", HnswIndexIT.vector(new Random(5)));
      List<Map<String, Object>> delta = list(inspect(table, index, "delta"), "regions");
      assertEquals(false, delta.get(0).get("overflowed"));
      assertEquals(false, delta.get(1).get("overflowed"));
      assertEquals(0, delta.get(0).get("changedRows"));
      assertEquals(1, delta.get(1).get("changedRows"));
      // No write batch ran when the newest segment swapped, so the replay starts at its time
      assertEquals(delta.get(1).get("newestSegmentTime"), delta.get(1).get("replayStart"));
    }
  }

  /** Inspect shows and finds the row keys of salted, composite and multi-tenant tables. */
  @Test
  public void testKeyLayouts() throws Exception {
    String salted = generateUniqueName();
    String tenanted = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + salted + " (K1 VARCHAR NOT NULL, K2 "
        + "INTEGER NOT NULL, V VECTOR(FLOAT, 16) CONSTRAINT PK PRIMARY KEY (K1, K2)) SALT_BUCKETS=2");
      conn.createStatement()
        .execute("CREATE TABLE " + tenanted + " (TENANT_ID VARCHAR NOT NULL, "
          + "ID VARCHAR NOT NULL, V VECTOR(FLOAT, 16) CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) "
          + "MULTI_TENANT=true");
      Random random = new Random(13);
      for (int i = 0; i < 30; i++) {
        try (PreparedStatement ps =
          conn.prepareStatement("UPSERT INTO " + salted + " VALUES (?, ?, ?)")) {
          ps.setString(1, "k" + i);
          ps.setInt(2, i);
          ps.setArray(3,
            conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(HnswIndexIT.vector(random))));
          ps.executeUpdate();
        }
        try (PreparedStatement ps =
          conn.prepareStatement("UPSERT INTO " + tenanted + " VALUES (?, ?, ?)")) {
          ps.setString(1, "t" + (i % 3));
          ps.setString(2, "r" + i);
          ps.setArray(3,
            conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(HnswIndexIT.vector(random))));
          ps.executeUpdate();
        }
      }
      conn.commit();
      for (String table : new String[] { salted, tenanted }) {
        conn.createStatement().execute("CREATE VECTOR INDEX " + table + "_IDX ON " + table
          + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
        HnswIndexIT.buildIndex(table, table + "_IDX");
        Report verify = run(0, "verify", "-dt", table, "-it", table + "_IDX");
        assertEquals(30L, HnswIndexFsckIT.verified(verify).get("valid"));
      }
      String key = list(inspect(salted, salted + "_IDX", "lookup", "k7", "7"), "entries").get(0)
        .get("key").toString();
      assertTrue(key, key.contains("K1=k7") && key.contains("K2=7"));
      key = list(inspect(tenanted, tenanted + "_IDX", "lookup", "t1", "r7"), "entries").get(0)
        .get("key").toString();
      assertTrue(key, key.contains("TENANT_ID=t1") && key.contains("ID=r7"));
    }
  }

  /**
   * The tools find the segments of each region after a split, and repair rebuilds an uncovered key
   * range. The daughters cannot write segments until the test releases them. The lower daughter
   * holds the anchor row of the parent. It searches the parent stack clipped to its range, and
   * rebuilds because another region wrote that stack. The upper daughter holds no segment, so it is
   * not covered until its first build. Then a client delete removes the base of a region, and
   * repair rebuilds the region.
   */
  @Test
  public void testDiscoveryAcrossSplitAndGap() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 100, "");
      HnswIndexIT.installFaults(table);
      HnswSegment.Descriptor parent =
        HnswIndexFsckIT.segmentOf(conn, index, HnswIndexIT.regions(table).get(1).getRegionInfo());
      HnswIndexIT.SegmentFaults.hold = new CountDownLatch(1);
      try {
        try (Admin admin = getUtility().getAdmin()) {
          admin.split(TableName.valueOf(table), Bytes.toBytes("z050"));
        }
        for (int i = 0; i < 600 && HnswIndexIT.regions(table).size() != 3; i++) {
          Thread.sleep(100);
        }
        getUtility().waitTableAvailable(TableName.valueOf(table));
        List<HRegion> daughters = HnswIndexIT.regions(table);
        assertEquals(3, daughters.size());
        List<Map<String, Object>> regions = list(inspect(table, index, "coverage"), "regions");
        assertEquals(3, regions.size());
        Map<String, Object> lower = regions.get(1);
        assertEquals(lower.toString(), false, lower.get("exact"));
        assertEquals(lower.toString(), true, lower.get("covered"));
        assertEquals(Arrays.asList(Bytes.toStringBinary(parent.identity)), lower.get("wider"));
        Map<String, Object> upper = regions.get(2);
        assertEquals(upper.toString(), false, upper.get("covered"));
        assertTrue(upper.toString(), ((List<?>) upper.get("segments")).isEmpty());
        Map<String, Object> listed = list(inspect(table, index, "list"), "segments").get(1);
        assertEquals(daughters.get(1).getRegionInfo().getEncodedName(), listed.get("region"));
        assertEquals(Bytes.toStringBinary(parent.anchor), listed.get("anchor"));
        assertFalse(listed.get("writer").equals(listed.get("region")));

        Report fsck = run(0, "fsck", "-dt", table, "-it", index);
        assertEquals(fsck.toText(), 0, fsck.getCount(Severity.ERROR));
        Finding uncovered =
          IndexFsckToolIT.finding(fsck, HnswIndexFsckProvider.RANGE_UNCOVERED, null);
        assertEquals(Severity.WARN, uncovered.getSeverity());
        assertEquals(daughters.get(2).getRegionInfo().getEncodedName(),
          uncovered.getDetails().get("region"));
        Finding untrusted =
          IndexFsckToolIT.finding(fsck, HnswIndexFsckProvider.STACK_UNTRUSTED, null);
        assertEquals(Severity.WARN, untrusted.getSeverity());
        assertEquals(daughters.get(1).getRegionInfo().getEncodedName(),
          untrusted.getDetails().get("region"));
        assertEquals(fsck.toText(), 2, fsck.getCount(Severity.WARN));
        // The lower daughter searches the parent segment clipped to its range. The rows of the
        // upper daughter are in no segment until its build. The upper daughter reads them without
        // the index, so they are not missing
        Report verify = run(0, "verify", "-dt", table, "-it", index);
        assertEquals(verify.toText(), 0, verify.getCount(Severity.ERROR));
        assertEquals(75L, HnswIndexFsckIT.verified(verify).get("valid"));
        Finding rowsUncovered =
          IndexFsckToolIT.finding(verify, HnswIndexFsckProvider.ROWS_UNCOVERED, null);
        assertEquals(Severity.WARN, rowsUncovered.getSeverity());
        assertEquals(25, IndexFsckToolIT.count(verify, HnswIndexFsckProvider.ROWS_UNCOVERED, null));
      } finally {
        HnswIndexIT.SegmentFaults.hold.countDown();
        HnswIndexIT.SegmentFaults.hold = null;
      }

      HnswIndexIT.rebuild(conn, table, index);
      Report coverage = inspect(table, index, "coverage");
      for (Map<String, Object> r : list(coverage, "regions")) {
        assertEquals(r.toString(), true, r.get("exact"));
      }
      assertTrue(((List<?>) coverage.getInspection().get("retirable")).isEmpty());
      assertEquals(100L,
        HnswIndexFsckIT.verified(run(0, "verify", "-dt", table, "-it", index)).get("valid"));
      Report fsck = run(0, "fsck", "-dt", table, "-it", index);
      assertEquals(fsck.toText(), 0, fsck.getCount(Severity.ERROR) + fsck.getCount(Severity.WARN));

      HRegion lower = HnswIndexIT.regions(table).get(0);
      HnswSegment.Descriptor gap = HnswIndexFsckIT.segmentOf(conn, index, lower.getRegionInfo());
      try (Table t = HnswIndexFsckIT.dataTable(conn, index)) {
        t.delete(
          new Delete(gap.anchor).addFamily(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES));
      }
      assertEquals(false, list(inspect(table, index, "coverage"), "regions").get(0).get("covered"));
      fsck = run(0, "fsck", "-dt", table, "-it", index);
      assertEquals(Severity.WARN,
        IndexFsckToolIT.finding(fsck, HnswIndexFsckProvider.RANGE_UNCOVERED, null).getSeverity());
      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      assertEquals(Collections.singletonList(lower.getRegionInfo().getEncodedName()), repair
        .getRepairPlan().get(HnswIndexFsckProvider.REBUILD_REGIONS).getDetails().get("regions"));
      assertArrayEquals(lower.getRegionInfo().getStartKey(),
        HnswIndexFsckIT.segmentOf(conn, index, lower.getRegionInfo()).startKey);
      fsck = run(0, "fsck", "-dt", table, "-it", index);
      assertFalse(fsck.toText(), fsck.getFindings().stream()
        .anyMatch(f -> f.getRule().equals(HnswIndexFsckProvider.RANGE_UNCOVERED)));
      assertEquals(100L,
        HnswIndexFsckIT.verified(run(0, "verify", "-dt", table, "-it", index)).get("valid"));
    }
  }
}
