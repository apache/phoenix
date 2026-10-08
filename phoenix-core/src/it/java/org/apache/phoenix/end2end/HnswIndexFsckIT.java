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

import static org.apache.phoenix.end2end.IndexFsckToolIT.count;
import static org.apache.phoenix.end2end.IndexFsckToolIT.finding;
import static org.apache.phoenix.end2end.IndexFsckToolIT.run;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRow;
import org.apache.phoenix.mapreduce.index.fsck.Finding;
import org.apache.phoenix.mapreduce.index.fsck.GlobalIndexFsckProvider;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckProviders;
import org.apache.phoenix.mapreduce.index.fsck.RepairAction;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.Severity;
import org.apache.phoenix.mapreduce.index.fsck.VerifyFindings;
import org.apache.phoenix.mapreduce.index.fsck.hnsw.HnswIndexContext;
import org.apache.phoenix.mapreduce.index.fsck.hnsw.HnswIndexFsckProvider;
import org.apache.phoenix.mapreduce.index.fsck.hnsw.HnswIndexReader;
import org.apache.phoenix.mapreduce.index.fsck.ivf.IvfIndexFsckProvider;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for {@code IndexFsckTool} verification, consistency checking, and repair
 * workflows on HNSW vector indexes.
 * <p>
 * Evaluates error detection and automated repair by synthesizing corrupted segment descriptors,
 * payloads, and orphan entries. Tests requiring index row verification account for replay margin
 * windowing to prevent in-flight memstore mutations from being classified as false positives.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswIndexFsckIT extends ParallelStatsDisabledIT {
  private static final int DIM = 16;
  private static final byte[] FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
  // Replay margin threshold used to ensure mutations age out of the memstore backlog window
  private static final long REPLAY_MARGIN_MS = 9_000;
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();

  static Float[] boxed(float[] v) {
    Float[] boxed = new Float[v.length];
    for (int i = 0; i < v.length; i++) {
      boxed[i] = v[i];
    }
    return boxed;
  }

  /**
   * Creates a pre-split data table populated with vector rows and builds its HNSW index.
   */
  static void createAndBuild(Connection conn, String table, String index, int count, String options)
    throws Exception {
    conn.createStatement().execute("CREATE TABLE " + table
      + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + ")) SPLIT ON ('m')");
    Random random = new Random(7);
    for (int i = 0; i < count; i++) {
      HnswIndexIT.upsert(conn, table, String.format("%s%03d", i % 2 == 0 ? "a" : "z", i),
        HnswIndexIT.vector(random));
    }
    conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
      + " (V) WITH (algorithm='HNSW', metric='L2'" + options + ") ASYNC");
    HnswIndexIT.buildIndex(table, index);
  }

  static HnswIndexContext context(Connection conn, String table, String index) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    return new HnswIndexContext(pconn, pconn.getTableNoCache(table), pconn.getTableNoCache(index));
  }

  /** Scans and returns all row keys and vector data within the specified region boundaries. */
  static TreeMap<byte[], float[]> rows(HnswIndexContext context, RegionInfo region)
    throws Exception {
    TreeMap<byte[], float[]> rows = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    try (HnswIndexReader reader = new HnswIndexReader(context)) {
      reader.scanVectors(region.getStartKey(), region.getEndKey(), rows::put);
    }
    return rows;
  }

  static byte[] payload(PTable.VectorIndex vi, Map<byte[], float[]> rows) throws Exception {
    List<VectorFloat<?>> vectors = new ArrayList<>();
    for (float[] v : rows.values()) {
      vectors.add(VTS.createFloatVector(v));
    }
    return HnswSegment.build(vi, new ListRandomAccessVectorValues(vectors, DIM),
      rows.keySet().toArray(new byte[0][]));
  }

  static Table indexTable(Connection conn, String index) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    return pconn.getQueryServices()
      .getTable(pconn.getTableNoCache(index).getPhysicalName().getBytes());
  }

  static HnswSegment.Descriptor write(Connection conn, String index, RegionInfo region, long time,
    byte[] payload, int count) throws Exception {
    try (Table table = indexTable(conn, index)) {
      return HnswSegment.write(table, FAMILY, region.getStartKey(), region.getEndKey(), time,
        payload, count);
    }
  }

  /** Returns the latest base segment exactly matching the specified region key boundary. */
  static HnswSegment.Descriptor segmentOf(Connection conn, String index, RegionInfo region)
    throws Exception {
    return HnswIndexIT.segments(conn, index).stream()
      .filter(d -> !d.isDelta() && d.covers(region.getStartKey(), region.getEndKey()))
      .max(Comparator.comparingLong(d -> d.time)).get();
  }

  @After
  public void restoreReplayMargin() {
    HnswIndexManager.setReplayMarginMs(HnswIndexManager.REPLAY_MARGIN_MS);
  }

  static boolean has(Report report, String rule) {
    return report.getFindings().stream().anyMatch(f -> f.getRule().equals(rule));
  }

  static Map<String, Object> verified(Report report) {
    return finding(report, HnswIndexFsckProvider.ROWS_VERIFIED, null).getDetails();
  }

  /** Retrieves verification failure records logged to {@code PHOENIX_INDEX_TOOL}. */
  static List<IndexVerificationOutputRow> failures(Connection conn, String index, Report verify)
    throws Exception {
    byte[] physical =
      conn.unwrap(PhoenixConnection.class).getTableNoCache(index).getPhysicalName().getBytes();
    try (IndexVerificationOutputRepository output =
      new IndexVerificationOutputRepository(physical, conn)) {
      return output.getOutputRows((Long) verified(verify).get("scanMaxTs"), physical);
    }
  }

  @Test
  public void testDispatchAndIndexToolRedirect() throws Exception {
    String table = generateUniqueName();
    String ivf = generateUniqueName();
    String hnsw = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + "))");
      Random random = new Random(3);
      for (int i = 0; i < 40; i++) {
        HnswIndexIT.upsert(conn, table, "r" + i, HnswIndexIT.vector(random));
      }
      conn.createStatement().execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=40)");
      conn.createStatement().execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='L2') ASYNC");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      assertTrue(
        IndexFsckProviders.forIndex(pconn.getTableNoCache(ivf)) instanceof IvfIndexFsckProvider);
      assertTrue(
        IndexFsckProviders.forIndex(pconn.getTableNoCache(hnsw)) instanceof HnswIndexFsckProvider);

      // IndexTool rejects verify-only and from-index operations for HNSW, while standard builds
      // remain supported
      IndexTool tool = new IndexTool();
      tool.setConf(new Configuration(getUtility().getConfiguration()));
      assertEquals(-1, tool.run(IndexToolIT.getArgValues(false, null, table, hnsw, null,
        IndexTool.IndexVerifyType.ONLY, IndexTool.IndexDisableLoggingType.NONE)));
      List<String> fromIndex =
        new ArrayList<>(Arrays.asList(IndexToolIT.getArgValues(false, null, table, hnsw, null,
          IndexTool.IndexVerifyType.NONE, IndexTool.IndexDisableLoggingType.NONE)));
      fromIndex.add("-fi");
      tool = new IndexTool();
      tool.setConf(new Configuration(getUtility().getConfiguration()));
      assertEquals(-1, tool.run(fromIndex.toArray(new String[0])));
      assertTrue(HnswIndexIT.segments(conn, hnsw).isEmpty());
      HnswIndexIT.buildIndex(table, hnsw);
      assertEquals(1, HnswIndexIT.segments(conn, hnsw).size());

      Report verifyIvf = run(0, "verify", "-dt", table, "-it", ivf);
      assertFalse(has(verifyIvf, HnswIndexFsckProvider.ROWS_VERIFIED));
      Report verifyHnsw = run(0, "verify", "-dt", table, "-it", hnsw);
      assertEquals(40L, verified(verifyHnsw).get("valid"));
      run(-1, "verify", "-dt", table, "-it", hnsw, "-et", "1");
    }
  }

  @Test
  public void testHealthyIndexes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      for (String options : new String[] { "", ", quantization='SQ8'",
        ", quantization='PQ', pq_segments=4" }) {
        String table = generateUniqueName();
        String index = generateUniqueName();
        // Product quantization requires at least 256 training vectors per region codebook
        int rows = options.contains("PQ") ? 600 : 100;
        createAndBuild(conn, table, index, rows, options);
        Report verify = run(0, "verify", "-dt", table, "-it", index);
        assertEquals(options, (long) rows, verified(verify).get("rows"));
        assertEquals(options, (long) rows, verified(verify).get("valid"));
        assertEquals(verify.toText(), 1, verify.getFindings().size());

        Report fsck = run(0, "fsck", "-dt", table, "-it", index);
        assertEquals(fsck.toText(), 0,
          fsck.getCount(Severity.ERROR) + fsck.getCount(Severity.WARN));
        List<Finding> recall =
          fsck.getFindings().stream().filter(f -> f.getRule().equals(HnswIndexFsckProvider.RECALL))
            .collect(Collectors.toList());
        assertEquals(fsck.toText(), 2, recall.size());
        Finding last = finding(fsck, VerifyFindings.LAST_VERIFY, null);
        assertEquals(Severity.INFO, last.getSeverity());

        Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
        assertTrue(repair.toText(), repair.getRepairPlan().getActions().isEmpty());
      }
    }
  }

  /**
   * Tests mutation tracking in the memstore backlog and subsequent delta segment generation on
   * flush.
   */
  @Test
  public void testBacklogAndDeltas() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 100, "");
      conn.createStatement().execute("DELETE FROM " + table + " WHERE ID = 'z001'");
      conn.commit();
      HnswIndexIT.upsert(conn, table, "z900", HnswIndexIT.vector(new Random(9)));
      Report verify = run(0, "verify", "-dt", table, "-it", index);
      assertEquals(2L, count(verify, HnswIndexFsckProvider.ROWS_BACKLOG, null));
      assertEquals(99L, verified(verify).get("valid"));

      HRegion upper = HnswIndexIT.regions(table).get(1);
      long since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexIT.manager(upper, index).flush();
      HnswSegment.Descriptor delta = HnswIndexIT.awaitDelta(conn, index, upper, since, 60);
      assertEquals(1, delta.count);
      verify = run(0, "verify", "-dt", table, "-it", index);
      assertFalse(verify.toText(), has(verify, HnswIndexFsckProvider.ROWS_BACKLOG));
      assertEquals(100L, verified(verify).get("valid"));
      Report fsck = run(0, "fsck", "-dt", table, "-it", index);
      assertEquals(fsck.toText(), 0, fsck.getCount(Severity.ERROR) + fsck.getCount(Severity.WARN));
    }
  }

  @Test
  public void testRowDefectsVerifiedAndRepaired() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 100, "");
      // Adjust replay margin to evaluate rows beyond the memstore replay window
      HnswIndexManager.setReplayMarginMs(REPLAY_MARGIN_MS);
      Thread.sleep(REPLAY_MARGIN_MS + 1000);
      HnswIndexContext context = context(conn, table, index);
      RegionInfo lower = HnswIndexIT.regions(table).get(0).getRegionInfo();
      HnswSegment.Descriptor real = segmentOf(conn, index, lower);

      // Construct a segment containing missing, corrupted, and orphaned entries
      TreeMap<byte[], float[]> rows = rows(context, lower);
      byte[] missing = rows.firstKey();
      rows.remove(missing);
      byte[] invalid = rows.firstKey();
      float[] other = rows.get(invalid).clone();
      for (int i = 0; i < other.length; i++) {
        other[i] = -other[i];
      }
      rows.put(invalid, other);
      byte[] orphan = Bytes.add(rows.lastKey(), new byte[] { 0 });
      rows.put(orphan, HnswIndexIT.vector(new Random(11)));
      HnswSegment.Descriptor crafted =
        write(conn, index, lower, EnvironmentEdgeManager.currentTimeMillis(),
          payload(context.getVectorIndex(), rows), rows.size());

      Report verify = run(1, "verify", "-dt", table, "-it", index);
      assertEquals(1, count(verify, VerifyFindings.MISSING, "data"));
      assertEquals(1, count(verify, VerifyFindings.INVALID, "data"));
      assertEquals(1, count(verify, VerifyFindings.ORPHAN_VERIFIED, "index"));
      assertEquals(98L, verified(verify).get("valid"));
      Map<String, byte[]> failed = new TreeMap<>();
      for (IndexVerificationOutputRow row : failures(conn, index, verify)) {
        assertArrayEquals(crafted.rowKey, row.getIndexTableRowKey());
        failed.put(row.getErrorMessage(), row.getDataTableRowKey());
      }
      assertEquals(3, failed.size());
      assertArrayEquals(missing, failed.get("Missing from the HNSW segments"));
      assertArrayEquals(invalid, failed.get("Vector differs from the HNSW segment's"));
      assertArrayEquals(orphan, failed.get("Not a data row with a vector"));
      Finding last =
        finding(run(0, "fsck", "-dt", table, "-it", index), VerifyFindings.LAST_VERIFY, null);
      assertEquals(Severity.WARN, last.getSeverity());

      Report dryRun = run(1, "repair", "-dt", table, "-it", index);
      assertEquals(RepairAction.Status.PLANNED,
        dryRun.getRepairPlan().get(HnswIndexFsckProvider.REBUILD_REGIONS).getStatus());
      assertTrue(HnswIndexIT.segments(conn, index).stream()
        .anyMatch(d -> Bytes.equals(d.rowKey, crafted.rowKey)));

      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      RepairAction rebuild = repair.getRepairPlan().get(HnswIndexFsckProvider.REBUILD_REGIONS);
      assertEquals(RepairAction.Status.EXECUTED, rebuild.getStatus());
      assertEquals(Collections.singletonList(lower.getEncodedName()),
        rebuild.getDetails().get("regions"));
      assertFalse(HnswIndexIT.segments(conn, index).stream()
        .anyMatch(d -> Bytes.equals(d.rowKey, crafted.rowKey)));
      verify = run(0, "verify", "-dt", table, "-it", index);
      assertEquals(100L, verified(verify).get("valid"));
    }
  }

  @Test
  public void testSegmentFaultsDetectedAndRepaired() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 100, "");
      HnswIndexContext context = context(conn, table, index);
      PTable.VectorIndex vi = context.getVectorIndex();
      List<HRegion> regions = HnswIndexIT.regions(table);
      RegionInfo lower = regions.get(0).getRegionInfo();
      RegionInfo upper = regions.get(1).getRegionInfo();
      HnswSegment.Descriptor realLower = segmentOf(conn, index, lower);
      HnswSegment.Descriptor realUpper = segmentOf(conn, index, upper);
      TreeMap<byte[], float[]> lowerRows = rows(context, lower);
      TreeMap<byte[], float[]> upperRows = rows(context, upper);
      TableName physical = context.getIndexPhysicalName();

      // Simulate missing MOB file by writing and flushing a segment payload, then deleting the
      // backing MOB file
      getUtility().getAdmin().flush(physical);
      HnswSegment.Descriptor unresolved =
        write(conn, index, upper, realUpper.time + 3, payload(vi, upperRows), upperRows.size());
      Path mobDir = MobUtils.getMobFamilyPath(getUtility().getConfiguration(), physical,
        Bytes.toString(FAMILY));
      FileSystem fs = mobDir.getFileSystem(getUtility().getConfiguration());
      Set<Path> before = new HashSet<>();
      for (FileStatus status : fs.listStatus(mobDir)) {
        before.add(status.getPath());
      }
      getUtility().getAdmin().flush(physical);
      for (FileStatus status : fs.listStatus(mobDir)) {
        if (!before.contains(status.getPath())) {
          assertTrue(fs.delete(status.getPath(), false));
        }
      }

      // Corrupt graph trailer magic
      byte[] flipped = payload(vi, lowerRows);
      int mappingLength = Bytes.toInt(flipped, flipped.length - 8);
      flipped[flipped.length - 8 - mappingLength - 1] ^= 0x01;
      HnswSegment.Descriptor corrupt =
        write(conn, index, lower, realLower.time + 1, flipped, lowerRows.size());
      // Desynchronize row count metadata from internal mapping size
      HnswSegment.Descriptor miscounted =
        write(conn, index, upper, realUpper.time + 1, payload(vi, upperRows), upperRows.size() + 1);
      // Introduce keys that violate region boundary constraints
      HnswSegment.Descriptor outOfRange =
        write(conn, index, upper, realUpper.time + 2, payload(vi, lowerRows), lowerRows.size());
      // Invert key ordering to violate monotonic sorting requirements
      byte[] swapped = payload(vi, lowerRows);
      int mapping = swapped.length - 8 - Bytes.toInt(swapped, swapped.length - 8);
      int length = Bytes.toInt(swapped, mapping + 4);
      byte[] first = Arrays.copyOfRange(swapped, mapping + 8, mapping + 8 + length);
      System.arraycopy(swapped, mapping + 12 + length, swapped, mapping + 8, length);
      System.arraycopy(first, 0, swapped, mapping + 12 + length, length);
      HnswSegment.Descriptor unordered =
        write(conn, index, lower, realLower.time + 2, swapped, lowerRows.size());
      // Inject an obsolete segment overshadowed by a newer base segment
      HnswSegment.Descriptor superseded =
        write(conn, index, lower, realLower.time - 1000, payload(vi, lowerRows), lowerRows.size());
      try (Table t = indexTable(conn, index)) {
        t.put(
          new Put(Bytes.toBytes("stray")).addColumn(FAMILY, Bytes.toBytes("X"), Bytes.toBytes(1)));
        t.put(new Put(Bytes.add(Bytes.toBytes("bad"), Bytes.toBytes(1L)))
          .addColumn(FAMILY, HnswSegment.END_KEY_QUALIFIER, new byte[0])
          .addColumn(FAMILY, HnswSegment.COUNT_QUALIFIER, new byte[] { 1 }));
      }

      Report fsck = run(1, "fsck", "-dt", table, "-it", index);
      assertEquals(Severity.ERROR,
        finding(fsck, HnswIndexFsckProvider.STRAY_ROW, null).getSeverity());
      assertEquals(Severity.ERROR,
        finding(fsck, HnswIndexFsckProvider.SEGMENT_MALFORMED, null).getSeverity());
      assertSegmentFinding(fsck, HnswIndexFsckProvider.PAYLOAD_UNRESOLVED, unresolved, null);
      assertSegmentFinding(fsck, HnswIndexFsckProvider.SEGMENT_CORRUPT, corrupt, null);
      assertSegmentFinding(fsck, HnswIndexFsckProvider.SEGMENT_INVALID, miscounted,
        "N records " + (upperRows.size() + 1) + " rows but the mapping holds " + upperRows.size());
      assertSegmentFinding(fsck, HnswIndexFsckProvider.SEGMENT_INVALID, outOfRange,
        "lies outside the segment's range");
      assertSegmentFinding(fsck, HnswIndexFsckProvider.SEGMENT_INVALID, unordered,
        "the mapping's row keys are not strictly ascending at ordinal 1");
      Finding retire = finding(fsck, HnswIndexFsckProvider.SEGMENT_SUPERSEDED, null);
      assertEquals(Severity.WARN, retire.getSeverity());
      assertEquals(Bytes.toStringBinary(superseded.rowKey), retire.getDetails().get("segment"));

      List<HnswSegment.Descriptor> segmentsBefore = HnswIndexIT.segments(conn, index);
      Report dryRun = run(1, "repair", "-dt", table, "-it", index);
      for (String action : Arrays.asList(HnswIndexFsckProvider.DELETE_STRAY_ROWS,
        HnswIndexFsckProvider.DELETE_CORRUPT_SEGMENTS, HnswIndexFsckProvider.REBUILD_REGIONS,
        HnswIndexFsckProvider.RETIRE_SEGMENTS)) {
        assertEquals(action, RepairAction.Status.PLANNED,
          dryRun.getRepairPlan().get(action).getStatus());
      }
      assertEquals(segmentsBefore.size(), HnswIndexIT.segments(conn, index).size());

      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      assertEquals(
        Arrays.asList(Bytes.toStringBinary(Bytes.add(Bytes.toBytes("bad"), Bytes.toBytes(1L))),
          Bytes.toStringBinary(Bytes.toBytes("stray"))),
        repair.getRepairPlan().get(HnswIndexFsckProvider.DELETE_STRAY_ROWS).getDetails()
          .get("deletedRows"));
      assertEquals(new HashSet<>(Arrays.asList(lower.getEncodedName(), upper.getEncodedName())),
        new HashSet<>((List<?>) repair.getRepairPlan().get(HnswIndexFsckProvider.REBUILD_REGIONS)
          .getDetails().get("regions")));
      List<HnswSegment.Descriptor> after = HnswIndexIT.segments(conn, index);
      assertEquals(after.toString(), 2, after.size());
      assertTrue(after.stream().allMatch(d -> d.time > realUpper.time + 3));
      Report clean = run(0, "fsck", "-dt", table, "-it", index);
      assertEquals(clean.toText(), 0,
        clean.getCount(Severity.ERROR) + clean.getCount(Severity.WARN));
      for (Finding recall : clean.getFindings()) {
        if (recall.getRule().equals(HnswIndexFsckProvider.RECALL)) {
          assertTrue(recall.toString(), (double) recall.getDetails().get("recall") >= 0.9);
        }
      }
      assertEquals(100L, verified(run(0, "verify", "-dt", table, "-it", index)).get("valid"));
    }
  }

  private static void assertSegmentFinding(Report report, String rule,
    HnswSegment.Descriptor segment, String problem) {
    Finding finding = report.getFindings().stream()
      .filter(f -> f.getRule().equals(rule)
        && Bytes.toStringBinary(segment.rowKey).equals(f.getDetails().get("segment")))
      .findFirst().orElseThrow(() -> new AssertionError(
        rule + " for " + Bytes.toStringBinary(segment.rowKey) + " not in " + report.getFindings()));
    assertEquals(Severity.ERROR, finding.getSeverity());
    if (problem != null) {
      List<?> problems = (List<?>) finding.getDetails().get("problems");
      assertTrue(problem + " not in " + problems,
        problems.stream().anyMatch(p -> p.toString().contains(problem)));
    }
  }

  @Test
  public void testInterlocks() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + "))");
      HnswIndexIT.upsert(conn, table, "r1", HnswIndexIT.vector(new Random(1)));
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='L2') ASYNC");
      Report repair = run(1, "repair", "-dt", table, "-it", index, "--confirm");
      assertNotNull(finding(repair, HnswIndexFsckProvider.REPAIR_REFUSED, null));
      assertTrue(repair.getRepairPlan().getActions().isEmpty());
      Report verify = run(0, "verify", "-dt", table, "-it", index);
      assertEquals(Severity.WARN,
        finding(verify, GlobalIndexFsckProvider.ROWS_NOT_VERIFIED, null).getSeverity());
      run(-1, "repair", "-dt", table, "-it", index, "-st", "1");
    }
  }
}
