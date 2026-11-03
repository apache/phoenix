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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.HnswSegmentStore;
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
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the verify, fsck and repair commands of {@code IndexFsckTool} on HNSW
 * vector indexes.
 * <p>
 * The tests write crafted segments, payloads and orphan entries to cause the faults that the tool
 * must find and repair. They write these cells to the hidden family {@code V#} of the data table
 * with plain HBase client puts, which {@code IndexRegionObserver} does not maintain. A row that
 * changed at or after the replay start of its region is in the memory of the region, not in a
 * segment. Verification counts such a row as backlog, not as a failure.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswIndexFsckIT extends ParallelStatsDisabledIT {
  private static final int DIM = 16;
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
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
   * Creates a data table with two regions, adds vector rows to both regions, and builds its HNSW
   * index.
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

  /** Returns the row keys and the vectors of the data rows in a region. */
  static TreeMap<byte[], float[]> rows(HnswIndexContext context, RegionInfo region)
    throws Exception {
    TreeMap<byte[], float[]> rows = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    try (HnswIndexReader reader = new HnswIndexReader(context);
      Table data = getUtility().getConnection().getTable(region.getTable()); ResultScanner scanner =
        data.getScanner(reader.vectorScan(region.getStartKey(), region.getEndKey()))) {
      for (Result r : scanner) {
        float[] vector = reader.vectorOf(r);
        if (vector != null) {
          rows.put(r.getRow(), vector);
        }
      }
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

  /** Returns the physical table of the index, which is the data table. */
  static Table dataTable(Connection conn, String index) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    return pconn.getQueryServices()
      .getTable(pconn.getTableNoCache(index).getPhysicalName().getBytes());
  }

  /**
   * Writes a base segment of the index for the key range of the region, as the region writes it. A
   * plain client put writes the cells to the anchor row of {@code V#}.
   */
  static HnswSegment.Descriptor write(Connection conn, String index, RegionInfo region, long time,
    byte[] payload, int count) throws Exception {
    return write(conn, index, region, time, payload, count, null, region.getEncodedNameAsBytes());
  }

  /**
   * Writes a base segment with the given definition digest and writer region. A null digest gives
   * the definition of the index.
   */
  static HnswSegment.Descriptor write(Connection conn, String index, RegionInfo region, long time,
    byte[] payload, int count, byte[] definition, byte[] writer) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    HnswIndexContext context =
      context(conn, pconn.getTableNoCache(index).getParentName().getString(), index);
    if (definition == null) {
      definition = context.getDefinition();
    }
    HnswSegment.Descriptor d = new HnswSegment.Descriptor(context.getIndexKey(),
      region.getStartKey(), time, region.getEndKey(), count, null, time, definition);
    Put put = new Put(d.anchor, time)
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG), d.endKey)
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG),
        Bytes.toBytes(count))
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG),
        Bytes.toBytes(time))
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG), definition)
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG), writer);
    if (payload != null) {
      put.addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.PAYLOAD_TAG), payload);
    }
    try (Table table = dataTable(conn, index)) {
      table.put(put);
    }
    return d;
  }

  /** Returns the newest base segment with the exact key range of the region. */
  static HnswSegment.Descriptor segmentOf(Connection conn, String index, RegionInfo region)
    throws Exception {
    return HnswIndexIT.segments(conn, index).stream()
      .filter(d -> !d.isDelta() && d.covers(region.getStartKey(), region.getEndKey()))
      .max(Comparator.comparingLong(d -> d.time)).get();
  }

  static boolean has(Report report, String rule) {
    return report.getFindings().stream().anyMatch(f -> f.getRule().equals(rule));
  }

  static Map<String, Object> verified(Report report) {
    return finding(report, HnswIndexFsckProvider.ROWS_VERIFIED, null).getDetails();
  }

  /** Returns the verification failures that a verify run wrote to {@code PHOENIX_INDEX_TOOL}. */
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

      // IndexTool refuses verify runs and from-index runs for HNSW, but it builds HNSW indexes. A
      // refused run does not change the state of a disabled index.
      conn.createStatement().execute("ALTER INDEX " + hnsw + " ON " + table + " DISABLE");
      IndexTool tool = new IndexTool();
      tool.setConf(new Configuration(getUtility().getConfiguration()));
      assertEquals(-1, tool.run(IndexToolIT.getArgValues(false, null, table, hnsw, null,
        IndexTool.IndexVerifyType.ONLY, IndexTool.IndexDisableLoggingType.NONE)));
      assertEquals(PIndexState.DISABLE, pconn.getTableNoCache(hnsw).getIndexState());
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
      Report fsckHnsw = run(0, "fsck", "-dt", table, "-it", hnsw);
      assertFalse(fsckHnsw.toText(), has(fsckHnsw, GlobalIndexFsckProvider.MISSING_COPROCESSOR));

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
        // A region needs at least 256 vectors to train PQ codebooks. With fewer vectors, the
        // segment stores full vectors
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
   * Rows that changed at or after the replay start are in the memory of the region, and
   * verification counts them as backlog. A flush writes them to a delta segment. After the flush,
   * verification finds no backlog.
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
      HnswIndexContext context = context(conn, table, index);
      RegionInfo lower = HnswIndexIT.regions(table).get(0).getRegionInfo();
      HnswSegment.Descriptor real = segmentOf(conn, index, lower);

      // Write a segment with a missing row, a row with a different vector, and an orphan row
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
      // The ITName column holds the physical name of the index, which is the data table, as in the
      // rows that IndexTool writes
      byte[] physical = context.getIndexTable().getPhysicalName().getBytes();
      byte[] prefix =
        Bytes.toBytes(verified(verify).get("scanMaxTs") + "|" + Bytes.toString(physical) + "|");
      try (
        Table output = conn.unwrap(PhoenixConnection.class).getQueryServices()
          .getTable(IndexVerificationOutputRepository.getOutputTableNameBytes());
        ResultScanner scanner = output.getScanner(new Scan().withStartRow(prefix)
          .withStopRow(ByteUtil.calculateTheClosestNextRowKeyForPrefix(prefix)))) {
        int logged = 0;
        for (Result r : scanner) {
          assertArrayEquals(physical,
            r.getValue(IndexVerificationOutputRepository.OUTPUT_TABLE_COLUMN_FAMILY,
              IndexVerificationOutputRepository.INDEX_TABLE_NAME_BYTES));
          logged++;
        }
        assertEquals(3, logged);
      }
      Map<String, byte[]> failed = new TreeMap<>();
      for (IndexVerificationOutputRow row : failures(conn, index, verify)) {
        assertArrayEquals(crafted.identity, row.getIndexTableRowKey());
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
        .anyMatch(d -> Bytes.equals(d.identity, crafted.identity)));

      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      RepairAction rebuild = repair.getRepairPlan().get(HnswIndexFsckProvider.REBUILD_REGIONS);
      assertEquals(RepairAction.Status.EXECUTED, rebuild.getStatus());
      assertEquals(Collections.singletonList(lower.getEncodedName()),
        rebuild.getDetails().get("regions"));
      assertFalse(HnswIndexIT.segments(conn, index).stream()
        .anyMatch(d -> Bytes.equals(d.identity, crafted.identity)));
      assertFalse(HnswIndexIT.segments(conn, index).stream()
        .anyMatch(d -> Bytes.equals(d.identity, real.identity)));
      verify = run(0, "verify", "-dt", table, "-it", index);
      assertEquals(100L, verified(verify).get("valid"));
    }
  }

  /**
   * Client puts write faults to {@code V#}. The test checks that fsck finds each fault and that
   * repair fixes it. The faults are:
   * <ul>
   * <li>a payload whose MOB file is gone, and a corrupt graph</li>
   * <li>a miscounted, out-of-range and unordered key mapping</li>
   * <li>a superseded base, and a segment of another definition</li>
   * <li>a newest base that another region wrote</li>
   * <li>the cells of a dropped index, a segment without its W cell, and a qualifier of the index
   * that does not decode</li>
   * </ul>
   * Repair never writes {@code V#}. It purges the cells of the dropped index, and it rebuilds both
   * regions. After the cutovers, the regions retire every other segment and delete the malformed
   * cells.
   */
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
      TableName physical = context.getDataPhysicalName();

      // Make a payload with a missing MOB file: flush the table, write a segment, flush again,
      // and delete the new MOB file
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

      // Change one bit of the footer magic at the end of the JVector graph
      byte[] flipped = payload(vi, lowerRows);
      int mappingLength = Bytes.toInt(flipped, flipped.length - 8);
      flipped[flipped.length - 8 - mappingLength - 1] ^= 0x01;
      HnswSegment.Descriptor corrupt =
        write(conn, index, lower, realLower.time + 1, flipped, lowerRows.size());
      // Write an N cell that does not agree with the size of the key mapping
      HnswSegment.Descriptor miscounted =
        write(conn, index, upper, realUpper.time + 1, payload(vi, upperRows), upperRows.size() + 1);
      // Write the keys of the lower region to a segment of the upper region, outside its range
      HnswSegment.Descriptor outOfRange =
        write(conn, index, upper, realUpper.time + 2, payload(vi, lowerRows), lowerRows.size());
      // Swap the first two keys of the mapping, so the keys are not in ascending order
      byte[] swapped = payload(vi, lowerRows);
      int mapping = swapped.length - 8 - Bytes.toInt(swapped, swapped.length - 8);
      int length = Bytes.toInt(swapped, mapping + 4);
      byte[] first = Arrays.copyOfRange(swapped, mapping + 8, mapping + 8 + length);
      System.arraycopy(swapped, mapping + 12 + length, swapped, mapping + 8, length);
      System.arraycopy(first, 0, swapped, mapping + 12 + length, length);
      HnswSegment.Descriptor unordered =
        write(conn, index, lower, realLower.time + 2, swapped, lowerRows.size());
      // Write a base segment that is older than the current base, which supersedes it
      HnswSegment.Descriptor superseded =
        write(conn, index, lower, realLower.time - 1000, payload(vi, lowerRows), lowerRows.size());
      // Write a segment of an earlier definition of the index, and a newest base of the upper
      // region that another region wrote
      HnswSegment.Descriptor stale =
        write(conn, index, lower, realLower.time + 3, payload(vi, lowerRows), lowerRows.size(),
          HnswSegment.definitionDigest("earlier"), lower.getEncodedNameAsBytes());
      HnswSegment.Descriptor foreign = write(conn, index, upper, realUpper.time + 4,
        payload(vi, upperRows), upperRows.size(), null, Bytes.toBytes("0123456789abcdef"));
      // Write the cells of a dropped index, a segment without its W cell, and a qualifier of the
      // index that does not decode
      String dropped = generateUniqueName();
      HnswSegment.Descriptor orphan = new HnswSegment.Descriptor(Bytes.toBytes(dropped),
        upper.getStartKey(), 1L, upper.getEndKey(), 0, null, 1L, context.getDefinition());
      HnswSegment.Descriptor noWriter =
        new HnswSegment.Descriptor(context.getIndexKey(), lower.getStartKey(), realLower.time + 4,
          lower.getEndKey(), 0, null, realLower.time + 4, context.getDefinition());
      byte[] junk = Bytes.add(HnswSegment.prefix(context.getIndexKey()), Bytes.toBytes("junk"));
      try (Table t = dataTable(conn, index)) {
        t.put(new Put(orphan.anchor).addColumn(FAMILY,
          HnswSegment.qualifier(orphan.identity, HnswSegment.END_KEY_TAG), orphan.endKey)
          .addColumn(FAMILY, HnswSegment.qualifier(orphan.identity, HnswSegment.COUNT_TAG),
            Bytes.toBytes(0)));
        t.put(new Put(noWriter.anchor)
          .addColumn(FAMILY, HnswSegment.qualifier(noWriter.identity, HnswSegment.END_KEY_TAG),
            noWriter.endKey)
          .addColumn(FAMILY, HnswSegment.qualifier(noWriter.identity, HnswSegment.COUNT_TAG),
            Bytes.toBytes(0))
          .addColumn(FAMILY, HnswSegment.qualifier(noWriter.identity, HnswSegment.REPLAY_FROM_TAG),
            Bytes.toBytes(noWriter.time))
          .addColumn(FAMILY, HnswSegment.qualifier(noWriter.identity, HnswSegment.DEFINITION_TAG),
            noWriter.definition));
        t.put(new Put(realLower.anchor).addColumn(FAMILY, junk, Bytes.toBytes(1)));
      }

      Report fsck = run(1, "fsck", "-dt", table, "-it", index);
      Finding stray = finding(fsck, HnswIndexFsckProvider.STRAY_ROW, null);
      assertEquals(Severity.ERROR, stray.getSeverity());
      assertEquals(dropped, stray.getDetails().get("indexKey"));
      assertEquals(Collections.singletonList(upper.getEncodedName()),
        stray.getDetails().get("regions"));
      assertSegmentFinding(fsck, HnswIndexFsckProvider.STRAY_ROW, stale, null);
      List<Object> malformed = fsck.getFindings().stream()
        .filter(f -> f.getRule().equals(HnswIndexFsckProvider.SEGMENT_MALFORMED))
        .map(f -> f.getDetails().get("cells")).collect(Collectors.toList());
      assertEquals(
        new HashSet<>(
          Arrays.asList(Bytes.toStringBinary(noWriter.identity), Bytes.toStringBinary(junk))),
        new HashSet<>(malformed));
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
      assertEquals(Bytes.toStringBinary(superseded.identity), retire.getDetails().get("segment"));
      Finding untrusted = finding(fsck, HnswIndexFsckProvider.STACK_UNTRUSTED, null);
      assertEquals(Severity.WARN, untrusted.getSeverity());
      assertEquals(upper.getEncodedName(), untrusted.getDetails().get("region"));
      assertEquals("0123456789abcdef", untrusted.getDetails().get("writer"));

      List<HnswSegment.Descriptor> segmentsBefore = HnswIndexIT.segments(conn, index);
      Report dryRun = run(1, "repair", "-dt", table, "-it", index);
      for (String action : Arrays.asList(HnswIndexFsckProvider.PURGE_STRAY_CELLS,
        HnswIndexFsckProvider.REBUILD_REGIONS)) {
        assertEquals(action, RepairAction.Status.PLANNED,
          dryRun.getRepairPlan().get(action).getStatus());
      }
      assertEquals(segmentsBefore.size(), HnswIndexIT.segments(conn, index).size());

      Report repair = run(0, "repair", "-dt", table, "-it", index, "--confirm");
      assertEquals(Collections.singletonList(dropped), repair.getRepairPlan()
        .get(HnswIndexFsckProvider.PURGE_STRAY_CELLS).getDetails().get("indexKeys"));
      assertEquals(new HashSet<>(Arrays.asList(lower.getEncodedName(), upper.getEncodedName())),
        new HashSet<>((List<?>) repair.getRepairPlan().get(HnswIndexFsckProvider.REBUILD_REGIONS)
          .getDetails().get("regions")));
      List<HnswSegment.Descriptor> after = HnswIndexIT.segments(conn, index);
      assertEquals(after.toString(), 2, after.size());
      assertTrue(after.stream().allMatch(d -> d.time > foreign.time));
      try (Table t = dataTable(conn, index)) {
        new HnswSegmentStore.TableStore(t).listCells(cell -> {
          assertFalse(Bytes.startsWith(CellUtil.cloneQualifier(cell),
            HnswSegment.prefix(Bytes.toBytes(dropped))));
          assertFalse(Bytes.startsWith(CellUtil.cloneQualifier(cell), junk));
        });
      }
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
        && Bytes.toStringBinary(segment.identity).equals(f.getDetails().get("segment")))
      .findFirst().orElseThrow(() -> new AssertionError(rule + " for "
        + Bytes.toStringBinary(segment.identity) + " not in " + report.getFindings()));
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
