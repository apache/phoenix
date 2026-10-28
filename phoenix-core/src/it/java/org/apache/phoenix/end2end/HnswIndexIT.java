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

import static org.apache.phoenix.hbase.index.metrics.HnswMetricValues.value;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.HnswSegmentStore;
import org.apache.phoenix.hbase.index.metrics.HnswMetricValues;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource.HnswMetric;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.index.vector.HnswRegionRequests;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.CsvBulkLoadTool;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.hnsw.HnswIndexFsckProvider;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the build, maintenance, durability, region lifecycle and purge of HNSW
 * indexes. The segments are in the hidden family V# of the data table. Other HNSW tests use the
 * static helpers of this class.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswIndexIT extends ParallelStatsDisabledIT {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIM = 16;
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;

  /**
   * Injects faults into the segment writes, the V# flushes and the purges of a table. While hold is
   * set, a segment write waits for it. While failWrites is set, a segment write fails. While
   * dropFlushes is set, a flush of V# writes no cell. The V# memstore is then lost, as in a crash
   * of the region server before the flush. While failPurges is set, a purge request fails. While
   * flushDelayMs is set, a flush of V# waits that long before it writes the cells. The cells then
   * stay in the memstore, where a listing of V# finds them.
   */
  public static class SegmentFaults implements RegionCoprocessor, RegionObserver {
    static volatile CountDownLatch hold;
    static volatile boolean failWrites;
    static volatile boolean dropFlushes;
    static volatile boolean failPurges;
    static volatile long flushDelayMs;
    // The number of dropped flushes
    static final AtomicInteger DROPPED = new AtomicInteger();
    // The number of delayed flushes
    static final AtomicInteger DELAYED = new AtomicInteger();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
      throws IOException {
      if (failPurges && scan.getAttribute(BaseScannerRegionObserverConstants.HNSW_PURGE) != null) {
        throw new DoNotRetryIOException("Injected purge failure");
      }
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> batch) throws IOException {
      for (int i = 0; i < batch.size(); i++) {
        Mutation m = batch.getOperation(i);
        if (m instanceof Put && m.getFamilyCellMap().containsKey(FAMILY)) {
          if (failWrites) {
            throw new IOException("Injected segment write failure");
          }
          CountDownLatch latch = hold;
          if (latch != null) {
            try {
              latch.await(300, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              throw new InterruptedIOException();
            }
          }
        }
      }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
      if (!Bytes.equals(FAMILY, store.getColumnFamilyDescriptor().getName())) {
        return scanner;
      }
      long delay = flushDelayMs;
      if (delay > 0) {
        DELAYED.incrementAndGet();
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
      if (!dropFlushes) {
        return scanner;
      }
      DROPPED.incrementAndGet();
      scanner.close();
      return new InternalScanner() {
        @Override
        public boolean next(List<Cell> result, ScannerContext context) {
          return false;
        }

        @Override
        public void close() {
        }
      };
    }
  }

  /** Adds {@link SegmentFaults} to the table. The regions of the table then open again. */
  static void installFaults(String table) throws Exception {
    TableName name = TableName.valueOf(table);
    try (Admin admin = getUtility().getAdmin()) {
      admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(name))
        .setCoprocessor(SegmentFaults.class.getName()).build());
    }
    getUtility().waitTableAvailable(name);
  }

  static float[] vector(Random random) {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = (float) random.nextGaussian();
    }
    return v;
  }

  static void upsert(Connection conn, String table, String id, float[] v) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
      ps.setString(1, id);
      Float[] boxed = new Float[v.length];
      for (int i = 0; i < v.length; i++) {
        boxed[i] = v[i];
      }
      ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
      ps.executeUpdate();
    }
    conn.commit();
  }

  static void delete(Connection conn, String table, String id) throws SQLException {
    conn.createStatement().execute("DELETE FROM " + table + " WHERE ID = '" + id + "'");
    conn.commit();
  }

  /**
   * Creates a table split at 'm', upserts {@code count} random rows, and builds an HNSW index with
   * IndexTool. The row IDs start with 'a' or 'z' in turn, so each region gets half of the rows.
   * Returns the vector of each row by its ID.
   */
  static Map<String, float[]> createAndBuild(Connection conn, String table, String index, int count)
    throws Exception {
    return createAndBuild(conn, table, index, count, " SPLIT ON ('m')");
  }

  static Map<String, float[]> createAndBuild(Connection conn, String table, String index, int count,
    String split) throws Exception {
    conn.createStatement().execute("CREATE TABLE " + table
      + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + "))" + split);
    Map<String, float[]> rows = new HashMap<>();
    Random random = new Random(42);
    for (int i = 0; i < count; i++) {
      String id = (i % 2 == 0 ? "a" : "z") + i;
      float[] v = vector(random);
      upsert(conn, table, id, v);
      rows.put(id, v);
    }
    conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
      + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
    buildIndex(table, index);
    return rows;
  }

  /**
   * Builds the index with IndexTool. A region writes its segment and waits for the flush of V#.
   * Then it cuts over to the segment and retires the older segments. IndexTool ends when each
   * region has cut over to its new segment, so the test waits for the retirement. It also checks
   * the coverage of each region.
   */
  static void buildIndex(String table, String index) throws Exception {
    IndexTool tool = new IndexTool();
    tool.setConf(new Configuration(getUtility().getConfiguration()));
    assertEquals(0, tool.run(IndexToolIT.getArgValues(false, null, table, index, null,
      IndexTool.IndexVerifyType.NONE, IndexTool.IndexDisableLoggingType.NONE)));
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      awaitSegments(conn, index, regions(table).size());
    }
    awaitCovered(table, index);
  }

  /** Waits until the base segments of each region of the table cover the region. */
  static void awaitCovered(String table, String index) throws Exception {
    for (HRegion region : regions(table)) {
      HnswIndexManager manager = manager(region, index);
      for (int i = 0; i < 600 && !manager.covered(); i++) {
        Thread.sleep(100);
      }
      assertTrue(region + " is not covered", manager.covered());
    }
  }

  static List<HRegion> regions(String table) {
    List<HRegion> regions = getUtility().getHBaseCluster().getRegions(TableName.valueOf(table));
    regions.sort(Comparator.comparing(r -> r.getRegionInfo().getStartKey(), Bytes::compareTo));
    return regions;
  }

  static HnswIndexManager manager(HRegion region, String index) throws IOException {
    IndexRegionObserver observer =
      region.getCoprocessorHost().findCoprocessor(IndexRegionObserver.class);
    return (HnswIndexManager) observer.getVectorIndexManager(index);
  }

  /**
   * Returns the keys of the k rows of the region of the manager that are nearest to the query. A
   * search uses its candidate count as its beam width, so this method asks for up to 64 candidates.
   * It asks for fewer candidates than the region holds, because a search for all rows returns them
   * without rank order.
   */
  static List<byte[]> nearestKeys(HnswIndexManager manager, float[] query, int k)
    throws IOException {
    int candidates = Math.max(k,
      Math.min(64, manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW) - 1));
    List<byte[]> keys =
      manager.search(query, candidates, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null);
    return keys.subList(0, Math.min(k, keys.size()));
  }

  /** Returns the keys of the k nearest rows from the manager of the region that holds the row. */
  private static List<String> search(String table, String index, String id, float[] query, int k)
    throws Exception {
    List<String> found = new ArrayList<>();
    for (HRegion region : regions(table)) {
      if (region.getRegionInfo().containsRow(Bytes.toBytes(id))) {
        for (byte[] key : nearestKeys(manager(region, index), query, k)) {
          found.add(Bytes.toString(key));
        }
      }
    }
    return found;
  }

  private static String nearest(String table, String index, String id, float[] query)
    throws Exception {
    List<String> found = search(table, index, id, query, 1);
    return found.isEmpty() ? null : found.get(0);
  }

  /** Asserts that a search by a stale vector of the row does not return the row. */
  private static void assertNotFound(String table, String index, String id, float[] stale)
    throws Exception {
    List<String> found = search(table, index, id, stale, 3);
    assertFalse(id + " found by its stale vector: " + found, found.contains(id));
  }

  /** Waits until each region of the table has a segment written at or after {@code since}. */
  private static void awaitRebuilt(Connection conn, String table, String index, long since)
    throws Exception {
    for (HRegion region : regions(table)) {
      awaitSegment(conn, index, region, since, 60);
    }
  }

  /**
   * Waits for a segment that covers the region and was written at or after {@code since}, and
   * returns its descriptor.
   */
  private static HnswSegment.Descriptor awaitSegment(Connection conn, String index, HRegion region,
    long since, int seconds) throws Exception {
    for (int i = 0; i < seconds * 10; i++) {
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        if (
          d.time >= since
            && d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())
        ) {
          return d;
        }
      }
      Thread.sleep(100);
    }
    throw new AssertionError(
      "segment of " + region.getRegionInfo().getEncodedName() + " not rebuilt");
  }

  /**
   * Waits for a delta segment that covers the region and has a time at or after {@code since}.
   * Returns its descriptor.
   */
  static HnswSegment.Descriptor awaitDelta(Connection conn, String index, HRegion region,
    long since, int seconds) throws Exception {
    for (int i = 0; i < seconds * 10; i++) {
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        if (
          d.time >= since && d.isDelta()
            && d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())
        ) {
          return d;
        }
      }
      Thread.sleep(100);
    }
    throw new AssertionError(
      "delta segment of " + region.getRegionInfo().getEncodedName() + " not written");
  }

  /** Returns the base segment that covers the region. */
  private static HnswSegment.Descriptor baseOf(List<HnswSegment.Descriptor> segments,
    HRegion region) {
    for (HnswSegment.Descriptor d : segments) {
      if (
        !d.isDelta()
          && d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())
      ) {
        return d;
      }
    }
    throw new AssertionError("no base segment of " + region.getRegionInfo().getEncodedName());
  }

  /** Returns the segments of the index in the V# family of its data table. */
  static List<HnswSegment.Descriptor> segments(Connection conn, String index) throws Exception {
    try (Table t = dataTable(conn, index)) {
      return new HnswSegmentStore.TableStore(t).list(Bytes.toBytes(index));
    }
  }

  // Returns the data table of an HNSW index, which is its physical table
  private static Table dataTable(Connection conn, String index) throws Exception {
    PTable indexTable = PhoenixRuntime.getTableNoCache(conn, index);
    return conn.unwrap(PhoenixConnection.class).getQueryServices()
      .getTable(indexTable.getPhysicalName().getBytes());
  }

  // Returns the V# cells of the table as qualifier strings. The MOB payloads stay references
  private static List<String> hiddenCells(Connection conn, String table) throws Exception {
    Scan scan = new Scan().addFamily(FAMILY);
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    List<String> cells = new ArrayList<>();
    try (
      Table t =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(table));
      ResultScanner scanner = t.getScanner(scan)) {
      for (Result result : scanner) {
        for (Cell cell : result.rawCells()) {
          cells.add(Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
        }
      }
    }
    return cells;
  }

  /**
   * Waits until V# holds {@code count} segments of the index. A full rebuild retires the segments
   * that its new base supersedes after it cuts over to the new base.
   */
  private static List<HnswSegment.Descriptor> awaitSegments(Connection conn, String index,
    int count) throws Exception {
    List<HnswSegment.Descriptor> segments = segments(conn, index);
    for (int i = 0; i < 300 && segments.size() != count; i++) {
      Thread.sleep(100);
      segments = segments(conn, index);
    }
    assertEquals("superseded segments should be retired", count, segments.size());
    return segments;
  }

  /** Waits until the table has {@code count} online regions and is available. */
  private static void awaitRegions(String table, int count) throws Exception {
    for (int i = 0; i < 600 && regions(table).size() != count; i++) {
      Thread.sleep(100);
    }
    assertEquals(count, regions(table).size());
    getUtility().waitTableAvailable(TableName.valueOf(table));
  }

  /**
   * Waits until no region of the table holds reference files. A split daughter or a merged region
   * compacts its reference files when it opens. HStore calls postCompact before it removes them, so
   * a manager that opens after this wait does not flush for that compaction.
   */
  private static void awaitCompacted(String table) throws Exception {
    for (HRegion region : regions(table)) {
      for (int i = 0; i < 600 && region.hasReferences(); i++) {
        Thread.sleep(100);
      }
      assertFalse(region + " holds reference files", region.hasReferences());
    }
  }

  /**
   * Rebuilds each region of the table. Then waits until each region has retired its older segments,
   * which it does after its cutover to the new segment.
   */
  static void rebuild(Connection conn, String table, String index) throws Exception {
    long since = EnvironmentEdgeManager.currentTimeMillis();
    for (HRegion region : regions(table)) {
      manager(region, index).rebuild(Long.MAX_VALUE);
    }
    awaitRebuilt(conn, table, index, since);
    awaitSegments(conn, index, regions(table).size());
  }

  /** Closes and reopens every region of the table. */
  static void reopen(String table) throws Exception {
    try (Admin admin = getUtility().getAdmin()) {
      for (HRegion region : regions(table)) {
        byte[] name = region.getRegionInfo().getRegionName();
        admin.unassign(name, true);
        admin.assign(name);
      }
    }
    getUtility().waitTableAvailable(TableName.valueOf(table));
  }

  private static double recall(String table, String index, Map<String, float[]> rows)
    throws Exception {
    Random random = new Random(7);
    int k = 10;
    int hits = 0;
    int queries = 20;
    for (int q = 0; q < queries; q++) {
      float[] query = vector(random);
      Set<String> expected = rows.entrySet().stream()
        .sorted(Comparator.comparingDouble(e -> -cosine(query, e.getValue()))).limit(k)
        .map(Map.Entry::getKey).collect(Collectors.toSet());
      List<String> found = new ArrayList<>();
      for (HRegion region : regions(table)) {
        for (byte[] key : nearestKeys(manager(region, index), query, k)) {
          found.add(Bytes.toString(key));
        }
      }
      found.sort(Comparator.comparingDouble(id -> -cosine(query, rows.get(id))));
      for (String id : found.subList(0, Math.min(k, found.size()))) {
        hits += expected.contains(id) ? 1 : 0;
      }
    }
    return hits / (double) (queries * k);
  }

  // Returns the opposite vector, which is the farthest from the old vector by cosine distance. A
  // search by the old vector then does not find the new version of the row
  private static float[] negate(float[] v) {
    float[] n = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      n[i] = -v[i];
    }
    return n;
  }

  static double cosine(float[] a, float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }

  private static long count(Connection conn, String sql, String name) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getLong(1);
      }
    }
  }

  /**
   * An IVF index and an HNSW index on one table have separate lifecycles. The HNSW index trains no
   * centroids and enqueues no tasks. Its regions apply writes while it is BUILDING, and ALTER INDEX
   * REBUILD keeps it BUILDING until IndexTool builds it again.
   */
  @Test
  public void testIvfAndHnswOnSameTable() throws Exception {
    String table = generateUniqueName();
    String ivf = generateUniqueName();
    String hnsw = generateUniqueName();
    String centroids = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + PhoenixDatabaseMetaData.INDEX_NAME + " = ?";
    String tasks = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME + " WHERE "
      + PhoenixDatabaseMetaData.TABLE_NAME + " = ?";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + ")) SPLIT ON ('m')");
      Random random = new Random(5);
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, (i % 2 == 0 ? "a" : "z") + i, vector(random));
      }
      conn.createStatement().execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='COSINE', lists=2, sample_size=40)");
      conn.createStatement().execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      float[] early = vector(random);
      upsert(conn, table, "a-early", early);

      buildIndex(table, hnsw);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertEquals("a-early", nearest(table, hnsw, "a-early", early));
      assertEquals(41, VectorIndexTestUtil.getHBaseRowKeys(conn.unwrap(PhoenixConnection.class),
        PhoenixRuntime.getTableNoCache(conn, ivf)).size());
      assertTrue(count(conn, centroids, ivf) > 0);
      assertEquals(0, count(conn, centroids, hnsw));
      assertTrue(count(conn, tasks, ivf) > 0);
      assertEquals(0, count(conn, tasks, hnsw));

      conn.createStatement().execute("ALTER INDEX " + hnsw + " ON " + table + " REBUILD");
      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertEquals(0, count(conn, tasks, hnsw));
      float[] late = vector(random);
      upsert(conn, table, "z-late", late);
      assertEquals("z-late", nearest(table, hnsw, "z-late", late));
      buildIndex(table, hnsw);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertEquals("a-early", nearest(table, hnsw, "a-early", early));
    }
  }

  /**
   * IndexTool builds a graph in each region. A region search returns only rows of that region, and
   * the recall over the two regions is at least 0.9.
   */
  @Test
  public void testIndexToolBuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, index).getIndexState());
      for (HRegion region : regions(table)) {
        for (byte[] key : manager(region, index).search(rows.get("a0"), 50,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null)) {
          assertTrue("search returned a row outside the region",
            region.getRegionInfo().containsRow(key));
        }
      }
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /**
   * IndexTool ends only after each region cuts over to its new base. A region writes its base to
   * V#, waits for the flush of V#, and then cuts over to the base. Here each flush of V# waits for
   * five seconds, so V# holds the base of a region well before the region searches it. As soon as
   * IndexTool returns, each region is covered, and the metrics count the rebuild of each region.
   */
  @Test
  public void testIndexToolWaitsForCutover() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + ")) SPLIT ON ('m')");
      Random random = new Random(13);
      for (int i = 0; i < 40; i++) {
        upsert(conn, table, (i % 2 == 0 ? "a" : "z") + i, vector(random));
      }
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      installFaults(table);
      int delayed = SegmentFaults.DELAYED.get();
      SegmentFaults.flushDelayMs = 5000;
      try {
        IndexTool tool = new IndexTool();
        tool.setConf(new Configuration(getUtility().getConfiguration()));
        assertEquals(0, tool.run(IndexToolIT.getArgValues(false, null, table, index, null,
          IndexTool.IndexVerifyType.NONE, IndexTool.IndexDisableLoggingType.NONE)));
        List<HRegion> regions = regions(table);
        assertEquals(2, regions.size());
        for (HRegion region : regions) {
          assertTrue(region + " is not covered", manager(region, index).covered());
        }
        assertEquals(0, value(HnswMetric.REGIONS_UNCOVERED, index));
        assertEquals(2, value(HnswMetric.REBUILDS, index));
      } finally {
        SegmentFaults.flushDelayMs = 0;
      }
      // The base write of each region waited for a delayed flush
      assertTrue(SegmentFaults.DELAYED.get() - delayed >= 2);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, index).getIndexState());
    }
  }

  /**
   * A bulk load writes data files that bypass the region observers. It writes nothing for an HNSW
   * index. After the load, the tool makes each region rebuild its graph from its rows. The bulk
   * load tools read no VECTOR column, so the index is on a path of a BSON document.
   */
  @Test
  public void testBulkLoad() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON) SPLIT ON ('m')");
      conn.createStatement().execute(
        "CREATE VECTOR INDEX " + index + " ON " + table + " (BSON_VECTOR_VALUE(DOC, 'embedding', "
          + DIM + ")) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      buildIndex(table, index);
      assertEquals(PIndexState.ACTIVE, PhoenixRuntime.getTableNoCache(conn, index).getIndexState());
      Configuration conf = new Configuration(getUtility().getConfiguration());
      Path input = new Path("/tmp/" + table + ".csv");
      Map<String, float[]> rows = new HashMap<>();
      Random random = new Random(42);
      try (PrintWriter writer =
        new PrintWriter(new OutputStreamWriter(FileSystem.get(conf).create(input)))) {
        for (int i = 0; i < 400; i++) {
          String id = (i % 2 == 0 ? "a" : "z") + i;
          float[] v = vector(random);
          rows.put(id, v);
          String doc =
            new BsonDocument("embedding", new BsonBinary(BinaryVector.floatVector(v))).toJson();
          writer.println(id + ",\"" + doc.replace("\"", "\"\"") + "\"");
        }
      }
      CsvBulkLoadTool tool = new CsvBulkLoadTool();
      tool.setConf(conf);
      assertEquals(0, tool.run(new String[] { "--input", input.toString(), "--table", table,
        "--zookeeper", "localhost:" + getUtility().getZkCluster().getClientPort() }));
      for (HRegion region : regions(table)) {
        HnswIndexManager manager = manager(region, index);
        long expected = rows.keySet().stream()
          .filter(id -> region.getRegionInfo().containsRow(Bytes.toBytes(id))).count();
        for (int i = 0; i < 300
          && manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW) != expected; i++) {
          Thread.sleep(100);
        }
        assertEquals(expected, manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
      }
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
      // The load wrote nothing to V#. V# holds only the cells of the segments of the regions
      List<HnswSegment.Descriptor> segments = segments(conn, index);
      assertEquals(regions(table).size(), segments.size());
      List<String> cells = hiddenCells(conn, table);
      assertEquals(segments.size() * 6, cells.size());
      for (String qualifier : cells) {
        byte[] bytes = Bytes.toBytesBinary(qualifier);
        assertNotNull(qualifier, HnswSegment.parseQualifier(bytes, 0, bytes.length));
      }
    }
  }

  /**
   * A flush writes the buffered changes as a delta on the base segment, and does not scan the data
   * rows. Only a full rebuild, which scans the region, adds a row that bypassed index maintenance.
   */
  @Test
  public void testFlushWritesDelta() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);

      List<HnswSegment.Descriptor> initialSegments = segments(conn, index);
      for (HnswSegment.Descriptor d : initialSegments) {
        assertFalse(d.isDelta());
        assertNull(d.baseTime);
      }

      float[] oldA0 = rows.get("a0");
      float[] newA0 = negate(oldA0);
      upsert(conn, table, "a0", newA0);

      float[] oldA2 = rows.get("a2");
      float[] newA2 = negate(oldA2);
      upsert(conn, table, "a2", newA2);

      // The test puts a row directly into the region, so Phoenix index maintenance does not see it
      Result existing = region.get(new Get(Bytes.toBytes("a4")));
      byte[] rawKey = Bytes.toBytes("a-raw");
      Put rawPut = new Put(rawKey);
      for (Cell cell : existing.rawCells()) {
        rawPut.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
          cell.getTimestamp(), CellUtil.cloneValue(cell));
      }
      region.put(rawPut);

      long since = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta = awaitDelta(conn, index, region, since, 60);
      assertEquals(Long.valueOf(baseOf(initialSegments, region).time), delta.baseTime);

      assertEquals("a0", nearest(table, index, "a0", newA0));
      assertNotFound(table, index, "a0", oldA0);
      assertEquals("a2", nearest(table, index, "a2", newA2));
      assertNotFound(table, index, "a2", oldA2);

      // The delta holds only buffered changes, so the raw row stays out of the graph
      List<String> found = search(table, index, "a-raw", rows.get("a4"), 10);
      assertFalse("raw row should be absent after delta flush: " + found, found.contains("a-raw"));

      long rebuildSince = EnvironmentEdgeManager.currentTimeMillis();
      mgr.rebuild(Long.MAX_VALUE);
      awaitSegment(conn, index, region, rebuildSince, 60);
      // The new base is visible in V# before the cutover. The region retires the old stack after
      // the cutover, so the test waits for the retirement before it searches
      awaitSegments(conn, index, regions(table).size());
      // a-raw has the vector of a4, so the two rows are at the same distance from that vector
      found = search(table, index, "a-raw", rows.get("a4"), 2);
      assertTrue("raw row should be found after a full rebuild: " + found, found.contains("a-raw"));
    }
  }

  /**
   * A region that reopens loads its base and its two deltas, and applies the stack masks again.
   * Thus the newest version of each row hides the older versions. The region trusts this stack and
   * does not rebuild.
   */
  @Test
  public void testDeltaStackOnReopen() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);
      long baseTime = baseOf(segments(conn, index), region).time;

      float[] initialA0 = rows.get("a0");
      float[] initialA2 = rows.get("a2");
      float[] initialA4 = rows.get("a4");
      float[] initialA6 = rows.get("a6");

      float[] delta1A0 = negate(initialA0);
      upsert(conn, table, "a0", delta1A0);
      delete(conn, table, "a2");
      float[] delta1A4 = negate(initialA4);
      upsert(conn, table, "a4", delta1A4);
      float[] initialA10 = rows.get("a10");
      float[] delta1A10 = negate(initialA10);
      upsert(conn, table, "a10", delta1A10);

      long sinceDelta1 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta1 = awaitDelta(conn, index, region, sinceDelta1, 60);
      assertEquals(Long.valueOf(baseTime), delta1.baseTime);

      float[] delta2A0 = vector(new Random(99));
      upsert(conn, table, "a0", delta2A0);
      delete(conn, table, "a4");
      float[] delta2A6 = negate(initialA6);
      upsert(conn, table, "a6", delta2A6);

      long sinceDelta2 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta2 = awaitDelta(conn, index, region, sinceDelta2, 60);
      // Each delta records the time of the base, not the time of the delta below it
      assertEquals(Long.valueOf(baseTime), delta2.baseTime);

      assertEquals("a0", nearest(table, index, "a0", delta2A0));
      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a0", delta1A0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);
      assertNotFound(table, index, "a4", delta1A4);
      assertEquals("a6", nearest(table, index, "a6", delta2A6));
      assertNotFound(table, index, "a6", initialA6);
      assertEquals("a8", nearest(table, index, "a8", rows.get("a8")));

      List<HnswSegment.Descriptor> stack = new ArrayList<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        if (d.overlaps(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())) {
          stack.add(d);
        }
      }
      assertEquals("base and two deltas", 3, stack.size());

      // The regions reopen and load the segment stacks from V#
      reopen(table);

      assertEquals("a10", nearest(table, index, "a10", delta1A10));
      assertNotFound(table, index, "a10", initialA10);
      assertEquals("a0", nearest(table, index, "a0", delta2A0));
      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a0", delta1A0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);
      assertNotFound(table, index, "a4", delta1A4);
      assertEquals("a6", nearest(table, index, "a6", delta2A6));
      assertNotFound(table, index, "a6", initialA6);
      assertEquals("a8", nearest(table, index, "a8", rows.get("a8")));

      // No rebuild runs, so the segments of the region do not change
      Thread.sleep(5000);
      List<HnswSegment.Descriptor> after = new ArrayList<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        if (d.overlaps(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey())) {
          after.add(d);
        }
      }
      assertEquals(stack.size(), after.size());
      for (int i = 0; i < stack.size(); i++) {
        assertTrue(Bytes.equals(stack.get(i).identity, after.get(i).identity));
      }
    }
  }

  /**
   * A flush starts a full rebuild, not a delta, when the changes of the flush and the deltas reach
   * REBUILD_RATIO of the base rows. A flush also starts a full rebuild when the stack already holds
   * MAX_DELTAS deltas. The new base retires the old stack.
   */
  @Test
  public void testRatioTriggersFullRebuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200, "");
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);
      long baseTime = baseOf(segments(conn, index), region).time;

      // 20 changes stay below REBUILD_RATIO of the 200 rows, and 55 changes reach it
      for (int i = 0; i < 20; i++) {
        String id = "a" + (2 * i);
        float[] v = negate(rows.get(id));
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long since1 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor d1 = awaitDelta(conn, index, region, since1, 60);
      assertEquals(Long.valueOf(baseTime), d1.baseTime);

      List<HnswSegment.Descriptor> stackAfterD1 = segments(conn, index);
      assertEquals(2, stackAfterD1.size());

      for (int i = 20; i < 55; i++) {
        String id = "a" + (2 * i);
        float[] v = negate(rows.get(id));
        upsert(conn, table, id, v);
        rows.put(id, v);
      }
      long since2 = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      awaitSegment(conn, index, region, since2, 60);
      List<HnswSegment.Descriptor> stackAfterRatio = awaitSegments(conn, index, 1);
      assertFalse("must be a base segment", stackAfterRatio.get(0).isDelta());

      // The stack takes MAX_DELTAS deltas, and the next flush is a full rebuild
      for (int deltaIdx = 1; deltaIdx <= 4; deltaIdx++) {
        String id = "a" + (2 * (60 + deltaIdx));
        float[] v = negate(rows.get(id));
        upsert(conn, table, id, v);
        rows.put(id, v);

        long sinceDelta = EnvironmentEdgeManager.currentTimeMillis();
        mgr.flush();
        HnswSegment.Descriptor d = awaitDelta(conn, index, region, sinceDelta, 60);
        assertEquals(Long.valueOf(stackAfterRatio.get(0).time), d.baseTime);
      }

      List<HnswSegment.Descriptor> stackWith4Deltas = segments(conn, index);
      assertEquals(5, stackWith4Deltas.size());

      String id = "a" + (2 * 70);
      float[] v = negate(rows.get(id));
      upsert(conn, table, id, v);
      rows.put(id, v);

      long sinceMaxDeltas = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      awaitSegment(conn, index, region, sinceMaxDeltas, 60);
      List<HnswSegment.Descriptor> stackAfterMaxDeltas = awaitSegments(conn, index, 1);
      assertFalse("must be a base segment", stackAfterMaxDeltas.get(0).isDelta());

      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /**
   * A flush of only deletes writes a delta with tombstones and no payload. The deleted rows stay
   * hidden after the regions reopen.
   */
  @Test
  public void testDeleteOnlyDelta() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200, "");
      HRegion region = regions(table).get(0);
      HnswIndexManager mgr = manager(region, index);
      long baseTime = baseOf(segments(conn, index), region).time;

      float[] initialA0 = rows.get("a0");
      float[] initialA2 = rows.get("a2");
      float[] initialA4 = rows.get("a4");

      delete(conn, table, "a0");
      delete(conn, table, "a2");
      delete(conn, table, "a4");

      long since = EnvironmentEdgeManager.currentTimeMillis();
      mgr.flush();
      HnswSegment.Descriptor delta = awaitDelta(conn, index, region, since, 60);
      assertEquals(Long.valueOf(baseTime), delta.baseTime);
      assertEquals(0, delta.count);

      try (Table t = dataTable(conn, index)) {
        byte[] payload = HnswSegment.qualifier(delta.identity, HnswSegment.PAYLOAD_TAG);
        byte[] tombstones = HnswSegment.qualifier(delta.identity, HnswSegment.TOMBSTONES_TAG);
        byte[] base = HnswSegment.qualifier(delta.identity, HnswSegment.BASE_TIME_TAG);
        Result r = t.get(new Get(delta.anchor).addColumn(FAMILY, payload)
          .addColumn(FAMILY, tombstones).addColumn(FAMILY, base));
        assertNull("delete-only delta should have no payload cell", r.getValue(FAMILY, payload));
        assertNotNull("delete-only delta must have tombstones cell",
          r.getValue(FAMILY, tombstones));
        assertNotNull("delta must have base time cell", r.getValue(FAMILY, base));
      }

      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);

      reopen(table);

      assertNotFound(table, index, "a0", initialA0);
      assertNotFound(table, index, "a2", initialA2);
      assertNotFound(table, index, "a4", initialA4);

      assertEquals("a6", nearest(table, index, "a6", rows.get("a6")));
    }
  }

  /**
   * A search returns the committed upserts and updates, and does not return deleted rows, before
   * and after a rebuild.
   */
  @Test
  public void testRowChangesBeforeAndAfterRebuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      Random random = new Random(99);
      float[] added = vector(random);
      float[] old = rows.get("z1");
      float[] moved = negate(old);
      upsert(conn, table, "a-new", added);
      upsert(conn, table, "z1", moved);
      delete(conn, table, "a2");

      assertEquals("a-new", nearest(table, index, "a-new", added));
      assertEquals("z1", nearest(table, index, "z1", moved));
      assertNotFound(table, index, "z1", old);
      assertNotFound(table, index, "a2", rows.get("a2"));

      rebuild(conn, table, index);
      assertEquals("a-new", nearest(table, index, "a-new", added));
      assertEquals("z1", nearest(table, index, "z1", moved));
      assertNotFound(table, index, "a2", rows.get("a2"));
    }
  }

  /**
   * Row changes that commit after a full rebuild is requested stay visible after the cutover, and
   * the old versions of the rows stay hidden. The cutover masks the entries of the new base for the
   * rows that changed after the swap.
   */
  @Test
  public void testChangesDuringRebuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      Random random = new Random(5);
      Map<String, float[]> changed = new HashMap<>();
      for (int i = 0; i < 10; i++) {
        changed.put("a" + (2 * i), negate(rows.get("a" + (2 * i))));
      }
      long since = EnvironmentEdgeManager.currentTimeMillis();
      for (HRegion region : regions(table)) {
        manager(region, index).rebuild(Long.MAX_VALUE);
      }
      for (Map.Entry<String, float[]> e : changed.entrySet()) {
        upsert(conn, table, e.getKey(), e.getValue());
      }
      delete(conn, table, "z3");
      awaitRebuilt(conn, table, index, since);
      for (Map.Entry<String, float[]> e : changed.entrySet()) {
        assertEquals(e.getKey(), nearest(table, index, e.getKey(), e.getValue()));
        assertNotFound(table, index, e.getKey(), rows.get(e.getKey()));
      }
      assertNotFound(table, index, "z3", rows.get("z3"));
    }
  }

  /**
   * A region that reopens replays the rows that changed at or after the replay start of its newest
   * segments, so the changes that were not flushed stay visible.
   */
  @Test
  public void testReplayOnReopen() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      Random random = new Random(11);
      float[] added = vector(random);
      float[] moved = negate(rows.get("a4"));
      upsert(conn, table, "z-new", added);
      upsert(conn, table, "a4", moved);
      delete(conn, table, "z5");
      reopen(table);
      assertEquals("z-new", nearest(table, index, "z-new", added));
      assertEquals("a4", nearest(table, index, "a4", moved));
      assertNotFound(table, index, "z5", rows.get("z5"));
    }
  }

  /**
   * A major compaction purges delete markers that a replay needs, so each region writes a delta
   * after it. A region that opens after the compaction then trusts its segments and does not
   * rebuild. A later rebuild does not find the row that the purged marker deleted.
   */
  @Test
  public void testRebuildDropsCompactedDeletes() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      delete(conn, table, "a6");
      long compacted = EnvironmentEdgeManager.currentTimeMillis();
      try (Admin admin = getUtility().getAdmin()) {
        admin.flush(TableName.valueOf(table));
      }
      getUtility().compact(TableName.valueOf(table), true);
      // A flush for a compaction writes a delta also without buffered changes. Thus each region
      // writes a delta
      for (HRegion region : regions(table)) {
        awaitDelta(conn, index, region, compacted, 60);
      }
      Set<String> before = new HashSet<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        before.add(Bytes.toStringBinary(d.identity));
      }
      reopen(table);
      assertNotFound(table, index, "a6", rows.get("a6"));
      for (HRegion region : regions(table)) {
        manager(region, index);
      }
      // The managers open above, and no region rebuilds when it opens
      Thread.sleep(3000);
      Set<String> after = new HashSet<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        after.add(Bytes.toStringBinary(d.identity));
      }
      assertEquals(before, after);
      rebuild(conn, table, index);
      assertNotFound(table, index, "a6", rows.get("a6"));
    }
  }

  /**
   * Deletes count toward FLUSH_THRESHOLD, so a region that deletes that many rows flushes without a
   * flush call. Here the flush is a full rebuild that writes an empty base.
   */
  @Test
  public void testDeletesTriggerFlush() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + ")) SPLIT ON ('m')");
      conn.setAutoCommit(false);
      Random random = new Random(13);
      int count = HnswIndexManager.FLUSH_THRESHOLD;
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        for (int i = 0; i < count; i++) {
          float[] v = vector(random);
          Float[] boxed = new Float[v.length];
          for (int j = 0; j < v.length; j++) {
            boxed[j] = v[j];
          }
          ps.setString(1, "a" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
          if (i % 1000 == 999) {
            conn.commit();
          }
        }
      }
      conn.commit();
      upsert(conn, table, "z0", vector(random));
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      buildIndex(table, index);
      long since = EnvironmentEdgeManager.currentTimeMillis();
      conn.createStatement().execute("DELETE FROM " + table + " WHERE ID < 'm'");
      conn.commit();
      HnswSegment.Descriptor d = awaitSegment(conn, index, regions(table).get(0), since, 60);
      assertEquals(0, d.count);
    }
  }

  /**
   * A delta flush and a full rebuild that fail run again after RETRY_DELAY_MS. The test fails the
   * segment writes of the table, so the delta of the lower region and the rebuild of the upper
   * region fail together. Thus the test waits for one delay only. Rows that change while the delta
   * waits change after its swap. The delta holds their buffered versions, and its cutover masks
   * them.
   */
  @Test
  public void testFailedFlushAndRebuildAreRetried() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      installFaults(table);
      float[] buffered = negate(rows.get("a2"));
      upsert(conn, table, "a2", buffered);
      float[] deleted = negate(rows.get("a4"));
      upsert(conn, table, "a4", deleted);
      float[] rebuilt = negate(rows.get("z5"));
      upsert(conn, table, "z5", rebuilt);
      HRegion lower = regions(table).get(0);
      HRegion upper = regions(table).get(1);
      HnswSegment.Descriptor base = awaitSegment(conn, index, lower, 0, 60);
      long since = EnvironmentEdgeManager.currentTimeMillis();
      float[] moved = vector(new Random(19));
      SegmentFaults.failWrites = true;
      try {
        manager(lower, index).flush();
        manager(upper, index).rebuild(Long.MAX_VALUE);
        Thread.sleep(5000);
        // These changes commit while the failed delta waits for its retry
        upsert(conn, table, "a2", moved);
        delete(conn, table, "a4");
      } finally {
        SegmentFaults.failWrites = false;
      }
      int seconds = (int) (HnswIndexManager.RETRY_DELAY_MS / 1000) + 60;
      HnswSegment.Descriptor delta = awaitDelta(conn, index, lower, since, seconds);
      assertEquals(Long.valueOf(base.time), delta.baseTime);
      assertEquals(2, delta.count);
      assertFalse(awaitSegment(conn, index, upper, since, seconds).isDelta());
      assertEquals("a2", nearest(table, index, "a2", moved));
      assertNotFound(table, index, "a2", rows.get("a2"));
      assertNotFound(table, index, "a4", deleted);
      assertNotFound(table, index, "a4", rows.get("a4"));
      assertEquals("z5", nearest(table, index, "z5", rebuilt));
      assertNotFound(table, index, "z5", rows.get("z5"));
    }
  }

  /**
   * A search finds a row that is inserted after the delete of every other buffered row. The nodes
   * of deleted rows stay in the mutable graph as routing nodes, so the new node gets links.
   */
  @Test
  public void testInsertAfterDeletingEveryNewRow() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 20);
      Random random = new Random(3);
      upsert(conn, table, "a-x", vector(random));
      delete(conn, table, "a-x");
      float[] v = vector(random);
      upsert(conn, table, "a-y", v);
      assertEquals("a-y", nearest(table, index, "a-y", v));
    }
  }

  /** Tests the build and the maintenance of an HNSW index on a vector in a BSON document. */
  @Test
  public void testBsonFunctionalIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      Random random = new Random(21);
      Map<String, float[]> rows = new HashMap<>();
      for (int i = 0; i < 50; i++) {
        rows.put("a" + i, vector(random));
        upsertDoc(conn, table, "a" + i, rows.get("a" + i));
      }
      conn.createStatement().execute(
        "CREATE VECTOR INDEX " + index + " ON " + table + " (BSON_VECTOR_VALUE(DOC, 'embedding', "
          + DIM + ")) WITH (algorithm='HNSW', " + "metric='COSINE') ASYNC");
      buildIndex(table, index);
      assertEquals("a7", nearest(table, index, "a7", rows.get("a7")));
      float[] added = vector(random);
      upsertDoc(conn, table, "a-new", added);
      assertEquals("a-new", nearest(table, index, "a-new", added));
    }
  }

  private static void upsertDoc(Connection conn, String table, String id, float[] v)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
      ps.setString(1, id);
      ps.setObject(2, new BsonDocument("embedding", new BsonBinary(BinaryVector.floatVector(v))));
      ps.executeUpdate();
    }
    conn.commit();
  }

  /**
   * An IVF index writes no index row for a row whose indexed vector is null, also when the row has
   * a value for an included vector column.
   */
  @Test
  public void testIvfIgnoresIncludedVectorWhenIndexedVectorIsNull() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY,"
        + " V VECTOR(FLOAT, 4), W VECTOR(FLOAT, 4))");
      conn.createStatement().execute("UPSERT INTO " + table
        + " VALUES ('r1', ARRAY[1.0, 0.0, 0.0, 0.0], ARRAY[0.0, 1.0, 0.0, 0.0])");
      conn.commit();
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) INCLUDE (W) WITH (algorithm='IVF', metric='L2', lists=1, sample_size=1)");
      conn.createStatement()
        .execute("UPSERT INTO " + table + " (ID, W) VALUES " + "('r2', ARRAY[0.0, 0.0, 1.0, 0.0])");
      conn.commit();
      assertEquals(1, VectorIndexTestUtil.getHBaseRowKeys(conn.unwrap(PhoenixConnection.class),
        PhoenixRuntime.getTableNoCache(conn, index)).size());
    }
  }

  /**
   * Writes a copy of a data row under another row key with a plain HBase put. No index maintenance
   * sees the put. The cells keep the timestamps of the row, so a region that opens later does not
   * replay the copy. No graph holds the copy. Thus an HNSW query finds it only when its region
   * reads its range without candidate restriction, or after a rebuild of the region.
   */
  private static void copyRow(String table, String from, String to) throws Exception {
    try (Table t = getUtility().getConnection().getTable(TableName.valueOf(table))) {
      Result row = t.get(new Get(Bytes.toBytes(from)));
      Put raw = new Put(Bytes.toBytes(to));
      for (Cell cell : row.rawCells()) {
        raw.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
          cell.getTimestamp(), CellUtil.cloneValue(cell));
      }
      t.put(raw);
    }
  }

  // Returns the IDs of the two rows nearest to the vector, from an HNSW query
  private static List<String> query2(Connection conn, String table, float[] v) throws Exception {
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn
      .prepareStatement("SELECT ID FROM " + table + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT 2")) {
      ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(v)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  /**
   * Tests a split while the daughters cannot write segments. The parent segment is at the anchor
   * row of the parent, so only the lower daughter holds it. The lower daughter searches only its
   * own range of the parent segment. The upper daughter holds the row at the split key, and
   * inherits no segment. Thus it reads its range without candidate restriction, and returns exact
   * results: it finds a row that no graph holds. After the daughters write their own segments,
   * their scans are restricted again. Then the lower daughter has retired the parent segment, and
   * each daughter has one segment of its own.
   */
  @Test
  public void testSplit() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400, "");
      // The parent segment holds a row at the split key
      float[] atSplit = vector(new Random(23));
      upsert(conn, table, "m", atSplit);
      rows.put("m", atSplit);
      rebuild(conn, table, index);
      HnswSegment.Descriptor parent = segments(conn, index).get(0);
      installFaults(table);
      long split = EnvironmentEdgeManager.currentTimeMillis();
      SegmentFaults.hold = new CountDownLatch(1);
      try {
        try (Admin admin = getUtility().getAdmin()) {
          admin.split(TableName.valueOf(table), Bytes.toBytes("m"));
        }
        awaitRegions(table, 2);
        awaitCompacted(table);
        // Copies that no graph holds. The test writes them before the daughters open their
        // managers, so that the rebuild scans of the daughters read them
        copyRow(table, "m", "m-copy");
        copyRow(table, "a0", "a0-copy");
        List<HRegion> daughters = regions(table);
        HnswIndexManager lower = manager(daughters.get(0), index);
        HnswIndexManager upper = manager(daughters.get(1), index);
        assertTrue(lower.covered());
        assertFalse(upper.covered());
        for (String id : new String[] { "a0", "m", "z1" }) {
          List<byte[]> found = lower.search(rows.get(id), 50, HConstants.EMPTY_START_ROW,
            HConstants.EMPTY_END_ROW, null);
          assertFalse(found.isEmpty());
          for (byte[] key : found) {
            assertTrue(Bytes.toString(key) + " is outside the lower daughter",
              daughters.get(0).getRegionInfo().containsRow(key));
          }
        }
        assertTrue(upper
          .search(rows.get("z1"), 50, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null)
          .isEmpty());
        // The query reads the range of the upper daughter, and finds the copy with the row at the
        // split key. The lower daughter searches its graph, which does not hold the copy
        assertEquals(new HashSet<>(Arrays.asList("m", "m-copy")),
          new HashSet<>(query2(conn, table, atSplit)));
        assertEquals(Arrays.asList("a0"), query2(conn, table, rows.get("a0")).subList(0, 1));
        assertFalse(query2(conn, table, rows.get("a0")).contains("a0-copy"));
        // The daughters cannot write segments yet
        assertEquals(1, segments(conn, index).size());
      } finally {
        SegmentFaults.hold.countDown();
        SegmentFaults.hold = null;
      }

      Random random = new Random(17);
      float[] added = vector(random);
      float[] moved = negate(rows.get("z3"));
      upsert(conn, table, "a-new", added);
      upsert(conn, table, "z3", moved);
      awaitRebuilt(conn, table, index, split);
      List<HnswSegment.Descriptor> segments = awaitSegments(conn, index, 2);
      awaitCovered(table, index);
      for (HRegion region : regions(table)) {
        boolean own = false;
        for (HnswSegment.Descriptor d : segments) {
          own |= d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
          assertFalse(Bytes.equals(parent.identity, d.identity));
        }
        assertTrue(own);
      }
      // The new segments hold the copies, which the rebuild scans read
      assertTrue(query2(conn, table, atSplit).contains("m-copy"));
      assertTrue(query2(conn, table, rows.get("a0")).contains("a0-copy"));
      // A copy written after the builds is again in no graph
      copyRow(table, "z5", "z5-copy");
      assertEquals(Arrays.asList("z5"), query2(conn, table, rows.get("z5")).subList(0, 1));
      assertFalse(query2(conn, table, rows.get("z5")).contains("z5-copy"));
      assertEquals("a-new", nearest(table, index, "a-new", added));
      assertEquals("z3", nearest(table, index, "z3", moved));
      assertNotFound(table, index, "z3", rows.get("z3"));
      rows.put("a-new", added);
      rows.put("z3", moved);
      rows.put("m-copy", atSplit);
      rows.put("a0-copy", rows.get("a0"));
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /**
   * Tests the split of a region whose stack has a delta, while the daughters cannot write segments.
   * The parent base and delta are at the anchor row of the parent, so only the lower daughter holds
   * them. The lower daughter searches the parent base and delta, clipped to its own range. In that
   * range, the delta replaces and deletes rows of the base. Only the parent delta holds the row at
   * the split key, which is in the upper daughter. The upper daughter inherits no segment, so it
   * reads its range without candidate restriction until its build. After the builds, each daughter
   * has one base of its own, and the lower daughter has retired the parent stack.
   */
  @Test
  public void testSplitWithDeltas() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400, "");
      installFaults(table);
      HRegion parentRegion = regions(table).get(0);
      HnswIndexManager parentMgr = manager(parentRegion, index);
      HnswSegment.Descriptor parentBase = baseOf(segments(conn, index), parentRegion);

      float[] initialA0 = rows.get("a0");
      float[] newA0 = negate(initialA0);
      upsert(conn, table, "a0", newA0);
      float[] initialA2 = rows.get("a2");
      delete(conn, table, "a2");
      // The parent delta holds a row at the split key
      float[] atSplit = vector(new Random(23));
      upsert(conn, table, "m", atSplit);
      rows.put("a0", newA0);
      rows.remove("a2");
      rows.put("m", atSplit);

      long sinceDelta = EnvironmentEdgeManager.currentTimeMillis();
      parentMgr.flush();
      HnswSegment.Descriptor parentDelta = awaitDelta(conn, index, parentRegion, sinceDelta, 60);
      assertEquals(Long.valueOf(parentBase.time), parentDelta.baseTime);
      assertEquals(2, parentDelta.count);

      long split = EnvironmentEdgeManager.currentTimeMillis();
      SegmentFaults.hold = new CountDownLatch(1);
      try {
        try (Admin admin = getUtility().getAdmin()) {
          admin.split(TableName.valueOf(table), Bytes.toBytes("m"));
        }
        awaitRegions(table, 2);
        awaitCompacted(table);
        List<HRegion> daughters = regions(table);
        HnswIndexManager lower = manager(daughters.get(0), index);
        HnswIndexManager upper = manager(daughters.get(1), index);
        assertTrue(lower.covered());
        assertFalse(upper.covered());
        // The lower daughter searches the parent base and delta in its own range. The delta hides
        // the old version of a0 and the deleted a2 in the base
        assertEquals("a0", Bytes.toString(nearestKeys(lower, newA0, 1).get(0)));
        for (float[] stale : new float[][] { initialA0, initialA2 }) {
          for (byte[] key : nearestKeys(lower, stale, 3)) {
            assertFalse(Bytes.toString(key),
              Bytes.toString(key).equals("a0") || Bytes.toString(key).equals("a2"));
          }
        }
        for (String id : new String[] { "a0", "m", "z1" }) {
          List<byte[]> found = lower.search(rows.get(id), 50, HConstants.EMPTY_START_ROW,
            HConstants.EMPTY_END_ROW, null);
          assertFalse(found.isEmpty());
          for (byte[] key : found) {
            assertTrue(Bytes.toString(key) + " is outside the lower daughter",
              daughters.get(0).getRegionInfo().containsRow(key));
          }
        }
        // The upper daughter has no graph, so its scans read its range without candidate
        // restriction. Thus the query finds the row at the split key, which only the parent delta
        // holds
        assertTrue(
          upper.search(atSplit, 50, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null)
            .isEmpty());
        assertEquals("m", query2(conn, table, atSplit).get(0));
        assertEquals("a0", query2(conn, table, newA0).get(0));
        // The daughters cannot write segments yet
        List<HnswSegment.Descriptor> held = segments(conn, index);
        assertEquals(2, held.size());
        assertNotNull(baseOf(held, parentRegion));
      } finally {
        SegmentFaults.hold.countDown();
        SegmentFaults.hold = null;
      }

      awaitRebuilt(conn, table, index, split);
      List<HnswSegment.Descriptor> segments = awaitSegments(conn, index, 2);
      awaitCovered(table, index);
      for (HRegion region : regions(table)) {
        boolean own = false;
        for (HnswSegment.Descriptor d : segments) {
          own |= d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
          assertFalse("daughter segments should be base segments", d.isDelta());
          assertFalse(Bytes.equals(parentBase.identity, d.identity));
        }
        assertTrue(own);
      }
      assertEquals("a0", nearest(table, index, "a0", newA0));
      assertNotFound(table, index, "a2", initialA2);
      assertEquals("m", nearest(table, index, "m", atSplit));
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /**
   * A region whose writes are faster than the flushes of its graph asks the master to split it. The
   * split limits the rebuild time of each region, and thus the time that a region can stay in
   * overflow. A split multiplier of 1 makes the region ask when its mutable graph reaches the flush
   * threshold, far below the overflow limit. The daughters write their own segments.
   */
  @Test
  public void testWriteBacklogSplitsRegion() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 10, "");
      TableName data = TableName.valueOf(table);
      try (Admin admin = getUtility().getAdmin()) {
        admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(data))
          .setValue(QueryServices.HNSW_SPLIT_MULTIPLIER_ATTRIB, "1").build());
      }
      getUtility().waitTableAvailable(data);
      long since = EnvironmentEdgeManager.currentTimeMillis();
      conn.setAutoCommit(false);
      Random random = new Random(31);
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        for (int i = 0; i < HnswIndexManager.FLUSH_THRESHOLD; i++) {
          float[] v = vector(random);
          Float[] boxed = new Float[v.length];
          for (int j = 0; j < v.length; j++) {
            boxed[j] = v[j];
          }
          ps.setString(1, String.format("b%05d", i));
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
          if (i % 1000 == 999) {
            conn.commit();
          }
        }
      }
      conn.commit();
      awaitRegions(table, 2);
      // The managers of the daughters open, rebuild, and write their segments
      for (HRegion region : regions(table)) {
        manager(region, index);
      }
      awaitRebuilt(conn, table, index, since);
    }
  }

  /**
   * A region whose mutable graph overflows keeps its writes available. The test holds the segment
   * writes of the table, so no flush empties the graph. It then writes past the overflow limit, and
   * no write fails. The region enters overflow and drops its mutable graph. Its scans read the
   * range without candidate restriction, so the results are exact. A query also finds a copy of a
   * row that no graph holds. Verification counts the rows that changed after the segments as
   * backlog. Inspect delta reports the overflow. After the test releases the segment writes, the
   * repair rebuild cuts over. The region leaves overflow, and its scans are restricted again. Its
   * base holds every row, also the rows that the region dropped from its graph.
   */
  @Test
  public void testOverflowKeepsWritesAvailable() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 10, "");
      TableName data = TableName.valueOf(table);
      // A held segment write blocks the close of the region, so the region must not split
      try (Admin admin = getUtility().getAdmin()) {
        admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(data))
          .setValue(QueryServices.HNSW_SPLIT_MULTIPLIER_ATTRIB, "0")
          .setCoprocessor(SegmentFaults.class.getName()).build());
      }
      getUtility().waitTableAvailable(data);
      HnswIndexManager manager = manager(regions(table).get(0), index);
      int limit = HnswIndexManager.OVERFLOW_MULTIPLIER * HnswIndexManager.FLUSH_THRESHOLD;
      Random random = new Random(37);
      int written = 0;
      SegmentFaults.hold = new CountDownLatch(1);
      try {
        conn.setAutoCommit(false);
        // The held flush holds the first entries of the graph, and the mutable graph then fills to
        // the limit. The test writes until the region overflows. It then writes 1000 rows more,
        // which the region drops
        int overflowedAt = -1;
        try (
          PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
          while (overflowedAt < 0 || written < overflowedAt + 1000) {
            assertTrue("no overflow after " + written + " rows", written < 2 * limit);
            for (int i = 0; i < 1000; i++) {
              String id = String.format("b%06d", written++);
              float[] v = vector(random);
              ps.setString(1, id);
              ps.setArray(2, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(v)));
              ps.executeUpdate();
              rows.put(id, v);
            }
            conn.commit();
            if (overflowedAt < 0 && manager.overflowed()) {
              overflowedAt = written;
            }
          }
        }
        conn.setAutoCommit(true);
        assertTrue(manager.overflowed());
        assertTrue(manager.covered());
        assertEquals(1, value(HnswMetric.OVERFLOW_ENTRIES, index));
        assertEquals(1, value(HnswMetric.REGIONS_OVERFLOWED, index));
        assertEquals(1, value(HnswMetric.REGIONS_UNRESTRICTED, index));
        assertTrue(value(HnswMetric.CHANGES_DROPPED, index) >= 1000);
        long overflowScans = value(HnswMetric.UNRESTRICTED_OVERFLOW, index);
        Random queries = new Random(38);
        for (int q = 0; q < 5; q++) {
          float[] query = vector(queries);
          Set<String> expected = rows.entrySet().stream()
            .sorted(Comparator.comparingDouble(e -> -cosine(query, e.getValue()))).limit(2)
            .map(Map.Entry::getKey).collect(Collectors.toSet());
          assertEquals(expected, new HashSet<>(query2(conn, table, query)));
        }
        assertEquals(overflowScans + 5, value(HnswMetric.UNRESTRICTED_OVERFLOW, index));
        // No graph holds the copy of the row. The query reads the range of the region, so it finds
        // the copy
        copyRow(table, "b000005", "b000005-copy");
        assertEquals(new HashSet<>(Arrays.asList("b000005", "b000005-copy")),
          new HashSet<>(query2(conn, table, rows.get("b000005"))));
        rows.put("b000005-copy", rows.get("b000005"));

        // The base holds the 10 rows of the build. All other rows changed after the replay start of
        // the base, so they are backlog
        Report verify = IndexFsckToolIT.run(0, "verify", "-dt", table, "-it", index);
        assertEquals(10L, HnswIndexFsckIT.verified(verify).get("valid"));
        assertEquals(written + 1,
          IndexFsckToolIT.count(verify, HnswIndexFsckProvider.ROWS_BACKLOG, null));
        List<Map<String, Object>> delta =
          HnswIndexInspectIT.list(HnswIndexInspectIT.inspect(table, index, "delta"), "regions");
        assertEquals(true, delta.get(0).get("overflowed"));
      } finally {
        conn.setAutoCommit(true);
        SegmentFaults.hold.countDown();
        SegmentFaults.hold = null;
      }

      for (int i = 0; i < 1800 && manager.overflowed(); i++) {
        Thread.sleep(100);
      }
      assertFalse(manager.overflowed());
      assertTrue(manager.covered());
      assertEquals(0, value(HnswMetric.REGIONS_OVERFLOWED, index));
      for (int i = 0; i < 100 && value(HnswMetric.OVERFLOW_TIME, index) == 0; i++) {
        Thread.sleep(100);
      }
      assertEquals(1, value(HnswMetric.OVERFLOW_TIME, index));
      List<HnswSegment.Descriptor> segments = awaitSegments(conn, index, 1);
      assertEquals(rows.size(), segments.get(0).count);
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
        assertTrue(rs.next());
        assertEquals(rows.size(), rs.getLong(1));
      }
      // The rows that the region dropped from its graph are in the base of the repair
      for (int i = written - 3; i < written; i++) {
        String id = String.format("b%06d", i);
        assertTrue(id, search(table, index, id, rows.get(id), 10).contains(id));
      }
      // A copy written after the cutover is in no graph, so the restricted scan does not find it
      copyRow(table, "b000007", "b000007-copy");
      List<String> found = query2(conn, table, rows.get("b000007"));
      assertEquals(2, found.size());
      assertFalse(found.toString(), found.contains("b000007-copy"));
      List<Map<String, Object>> delta =
        HnswIndexInspectIT.list(HnswIndexInspectIT.inspect(table, index, "delta"), "regions");
      assertEquals(false, delta.get(0).get("overflowed"));
    }
  }

  /**
   * A merged region searches the segments of both predecessors, and then replaces them with its own
   * base segment.
   */
  @Test
  public void testMerge() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      long merge = EnvironmentEdgeManager.currentTimeMillis();
      List<HRegion> inputs = regions(table);
      try (Admin admin = getUtility().getAdmin()) {
        admin.mergeRegionsAsync(new byte[][] { inputs.get(0).getRegionInfo().getRegionName(),
          inputs.get(1).getRegionInfo().getRegionName() }, false).get();
      }
      awaitRegions(table, 1);
      // A manager of the merged region that is open when the reference compaction ends writes a
      // segment. Thus the test waits for the compaction before the searches open the manager
      awaitCompacted(table);
      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      assertEquals("z1", nearest(table, index, "z1", rows.get("z1")));
      awaitRebuilt(conn, table, index, merge);
      awaitSegments(conn, index, 1);
      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      assertEquals("z1", nearest(table, index, "z1", rows.get("z1")));
    }
  }

  /**
   * A search that loads an evicted segment again, after the cells of the segment are gone from its
   * region, fails one time with a retryable exception. The region leaves the segment out and
   * rebuilds. The retry of the scan reads the range of the region without candidate restriction, so
   * queries stay exact. Buffered changes still replace the old versions of their rows.
   */
  @Test
  public void testSearchReplacesRetiredSegment() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400);
      HRegion upper = regions(table).get(1);
      HnswSegment.Descriptor own = null;
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        own = d.covers(upper.getRegionInfo().getStartKey(), upper.getRegionInfo().getEndKey())
          ? d
          : own;
      }
      assertNotNull(own);
      // The mutable graph holds this change, and the segment does not
      float[] moved = negate(rows.get("z3"));
      upsert(conn, table, "z3", moved);
      assertTrue(manager(upper, index).covered());

      // Delete the cells of the segment with a plain HBase client, as for a lost segment
      long lost = EnvironmentEdgeManager.currentTimeMillis();
      Delete delete = new Delete(own.anchor);
      for (byte tag : new byte[] { HnswSegment.PAYLOAD_TAG, HnswSegment.END_KEY_TAG,
        HnswSegment.COUNT_TAG, HnswSegment.REPLAY_FROM_TAG, HnswSegment.DEFINITION_TAG }) {
        delete.addColumns(FAMILY, HnswSegment.qualifier(own.identity, tag));
      }
      try (Table t = dataTable(conn, index)) {
        t.delete(delete);
      }
      HnswOffheapAllocator.get(getUtility().getConfiguration()).evictAll();

      try (PreparedStatement ps = conn
        .prepareStatement("SELECT ID FROM " + table + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT 1")) {
        for (String id : new String[] { "z1", "a0" }) {
          ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(rows.get(id))));
          try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(id, rs.getString(1));
          }
        }
        ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(moved)));
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("z3", rs.getString(1));
        }
      }
      awaitSegment(conn, index, upper, lost, 60);
      awaitCovered(table, index);
      assertEquals("z1", nearest(table, index, "z1", rows.get("z1")));
      assertEquals("z3", nearest(table, index, "z3", moved));
      assertNotFound(table, index, "z3", rows.get("z3"));
      assertEquals("a0", nearest(table, index, "a0", rows.get("a0")));
      rows.put("z3", moved);
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /**
   * Tests a segment larger than the default 10 MB cell size limit. The build writes it as one cell
   * of V#, and loads the graph from the payload in memory. After an eviction, a search loads the
   * segment again from its MOB file.
   */
  @Test
  public void testLargeSegment() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    int dim = 1024;
    int count = 3000;
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + dim + "))");
      Map<String, float[]> rows = new HashMap<>();
      Random random = new Random(11);
      conn.setAutoCommit(false);
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        for (int i = 0; i < count; i++) {
          float[] v = new float[dim];
          Float[] boxed = new Float[dim];
          for (int d = 0; d < dim; d++) {
            v[d] = (float) random.nextGaussian();
            boxed[d] = v[d];
          }
          rows.put("r" + i, v);
          ps.setString(1, "r" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed));
          ps.executeUpdate();
          if (i % 200 == 199) {
            conn.commit();
          }
        }
      }
      conn.commit();
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      buildIndex(table, index);

      List<HnswSegment.Descriptor> segments = segments(conn, index);
      assertEquals(1, segments.size());
      assertEquals(count, segments.get(0).count);
      try (Table t = dataTable(conn, index)) {
        Cell payload = new HnswSegmentStore.TableStore(t).readPayload(segments.get(0));
        assertTrue("payload of " + payload.getValueLength() + " bytes",
          payload.getValueLength() > 10 * 1024 * 1024);
      }

      HRegion region = regions(table).get(0);
      assertEquals("r7",
        Bytes.toString(nearestKeys(manager(region, index), rows.get("r7"), 1).get(0)));
      HnswOffheapAllocator.get(getUtility().getConfiguration()).evictAll();
      for (String id : new String[] { "r0", "r1500", "r2999" }) {
        assertEquals(id,
          Bytes.toString(nearestKeys(manager(region, index), rows.get(id), 1).get(0)));
      }
    }
  }

  private static void upsert(Connection conn, String table, String id, float[] v, float[] w)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, V, W) VALUES (?, ?, ?)")) {
      ps.setString(1, id);
      ps.setArray(2, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(v)));
      ps.setArray(3, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(w)));
      ps.executeUpdate();
    }
    conn.commit();
  }

  /**
   * The regions serve an index by its identity. A view inherits the index as
   * {@code <view>#<index>}, and its writes change the graph of the index. A scan of another table
   * cannot name the index. When an index is dropped and created again with another definition, it
   * gets new managers, which do not keep the graph of the old index.
   */
  @Test
  public void testIndexIdentity() throws Exception {
    String table = generateUniqueName();
    String view = generateUniqueName();
    String other = generateUniqueName();
    String index = generateUniqueName();
    int dimW = 8;
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY,"
        + " V VECTOR(FLOAT, " + DIM + "), W VECTOR(FLOAT, " + dimW + ")) SPLIT ON ('m')");
      Random random = new Random(21);
      Map<String, float[]> ws = new HashMap<>();
      for (int i = 0; i < 40; i++) {
        String id = (i % 2 == 0 ? "a" : "z") + i;
        float[] w = new float[dimW];
        for (int d = 0; d < dimW; d++) {
          w[d] = (float) random.nextGaussian();
        }
        ws.put(id, w);
        upsert(conn, table, id, vector(random), w);
      }
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      buildIndex(table, index);

      // A write through a view changes the graph of the index
      conn.createStatement()
        .execute("CREATE VIEW " + view + " AS SELECT * FROM " + table + " WHERE ID = 'zview'");
      float[] v = vector(new Random(22));
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + view + " (V, W) VALUES (?, ?)")) {
        ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(v)));
        ps.setArray(2, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(new float[dimW])));
        ps.executeUpdate();
      }
      conn.commit();
      assertEquals("zview", nearest(table, index, "zview", v));
      try (PreparedStatement ps = conn
        .prepareStatement("SELECT ID FROM " + table + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT 1")) {
        ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(v)));
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("zview", rs.getString(1));
        }
      }

      // A build scan of another table that names the index fails, and no region rebuilds the index
      conn.createStatement().execute("CREATE TABLE " + other
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + "))");
      upsert(conn, other, "x", vector(random));
      List<String> before = new ArrayList<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        before.add(Bytes.toStringBinary(d.identity));
      }
      Scan scan = new Scan();
      scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_BUILD,
        Bytes.add(Bytes.toBytes(Long.MAX_VALUE), Bytes.toBytes(index)));
      try (
        Table t =
          conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(other));
        ResultScanner scanner = t.getScanner(scan)) {
        scanner.next();
        fail("a scan of another table must not name the index");
      } catch (IOException e) {
        // The region refuses the scan, because the index is not an index of its table
      }
      Thread.sleep(2000);
      List<String> after = new ArrayList<>();
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        after.add(Bytes.toStringBinary(d.identity));
      }
      assertEquals(before, after);

      // The index created again on W does not keep the graph of V
      conn.createStatement().execute("DROP INDEX " + index + " ON " + table);
      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (W) WITH (algorithm='HNSW', metric='L2') ASYNC");
      buildIndex(table, index);
      for (String id : new String[] { "a0", "z1", "a10", "z21" }) {
        assertEquals(id, nearest(table, index, id, ws.get(id)));
      }
    }
  }

  // Returns the cells of V# whose qualifier has the prefix of the segments of the index
  private static List<String> cellsOf(Connection conn, String table, String index)
    throws Exception {
    List<String> cells = new ArrayList<>();
    String prefix = Bytes.toStringBinary(HnswSegment.prefix(Bytes.toBytes(index)));
    for (String qualifier : hiddenCells(conn, table)) {
      if (qualifier.startsWith(prefix)) {
        cells.add(qualifier);
      }
    }
    return cells;
  }

  // Returns the regions of the table, as the client sees them
  private static List<RegionInfo> regionInfos(Connection conn, String table) throws Exception {
    List<RegionInfo> regions = new ArrayList<>();
    for (HRegionLocation location : conn.unwrap(PhoenixConnection.class).getQueryServices()
      .getAllTableRegions(Bytes.toBytes(table))) {
      regions.add(location.getRegion());
    }
    return regions;
  }

  // Drops the cached readers of the given MOB file from the region servers
  private static void evictMobFile(String name) {
    for (JVMClusterUtil.RegionServerThread t : getUtility().getHBaseCluster()
      .getLiveRegionServerThreads()) {
      t.getRegionServer().getMobFileCache().ifPresent(c -> c.evictFile(name));
    }
  }

  /**
   * Simulates the MOB cleaner, which archives a payload that a split daughter inherits some time
   * after HBase removes the parent region. The test deletes the MOB file of the parent segment. The
   * lower daughter opens while the daughters cannot write segments. It finds the payload lost,
   * leaves the segment out, and reads its range without candidate restriction, so queries stay
   * exact. It never decodes the empty payload. Its rebuild then replaces the segment.
   */
  @Test
  public void testInheritedPayloadLostTriggersRebuild() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 400, "");
      HnswSegment.Descriptor parent = segments(conn, index).get(0);
      installFaults(table);
      long split = EnvironmentEdgeManager.currentTimeMillis();
      SegmentFaults.hold = new CountDownLatch(1);
      try {
        try (Admin admin = getUtility().getAdmin()) {
          admin.split(TableName.valueOf(table), Bytes.toBytes("m"));
        }
        awaitRegions(table, 2);
        awaitCompacted(table);
        // A copy that no graph holds. The test writes it before the lower daughter opens its
        // manager, so that the rebuild scan of the lower daughter reads it
        copyRow(table, "a0", "a0-copy");
        // The lower daughter holds the parent segment. Delete the MOB file of its payload
        Scan raw = new Scan().withStartRow(parent.anchor).withStopRow(parent.anchor, true)
          .addColumn(FAMILY, HnswSegment.qualifier(parent.identity, HnswSegment.PAYLOAD_TAG));
        raw.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
        Cell reference;
        try (Table t = dataTable(conn, index); ResultScanner scanner = t.getScanner(raw)) {
          reference = scanner.next().rawCells()[0];
        }
        assertTrue(MobUtils.hasValidMobRefCellValue(reference));
        Configuration conf = getUtility().getConfiguration();
        Path mob = new Path(MobUtils.getMobFamilyPath(conf, TableName.valueOf(table),
          QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY), MobUtils.getMobFileName(reference));
        assertTrue(mob.getFileSystem(conf).delete(mob, false));
        evictMobFile(MobUtils.getMobFileName(reference));

        HnswIndexManager lower = manager(regions(table).get(0), index);
        assertFalse(lower.covered());
        assertEquals(0, lower.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
        assertEquals(new HashSet<>(Arrays.asList("a0", "a0-copy")),
          new HashSet<>(query2(conn, table, rows.get("a0"))));
        assertEquals("z1", query2(conn, table, rows.get("z1")).get(0));
      } finally {
        SegmentFaults.hold.countDown();
        SegmentFaults.hold = null;
      }
      awaitRebuilt(conn, table, index, split);
      awaitSegments(conn, index, 2);
      awaitCovered(table, index);
      assertEquals(new HashSet<>(Arrays.asList("a0", "a0-copy")),
        new HashSet<>(query2(conn, table, rows.get("a0"))));
      rows.put("a0-copy", rows.get("a0"));
      double recall = recall(table, index, rows);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }

  /**
   * A region trusts only the segments that it wrote itself. The test gives the base of the lower
   * region the name of another region. A merge and a split, a split and a merge back, or a clone
   * can cause this state. When the region opens again, it searches that base at once, so queries
   * stay exact. It also rebuilds at once, and its new base records its own name. The upper region
   * trusts its own base and keeps it.
   */
  @Test
  public void testBaseOfAnotherRegionIsRebuilt() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      installFaults(table);
      List<HRegion> regions = regions(table);
      HnswSegment.Descriptor lower = null;
      HnswSegment.Descriptor upper = null;
      for (HnswSegment.Descriptor d : segments(conn, index)) {
        HRegion region = regions.get(d.startKey.length == 0 ? 0 : 1);
        assertTrue(
          d.covers(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey()));
        assertArrayEquals(region.getRegionInfo().getEncodedNameAsBytes(), d.writer);
        if (d.startKey.length == 0) {
          lower = d;
        } else {
          upper = d;
        }
      }
      assertNotNull(lower);
      assertNotNull(upper);
      try (Table t = dataTable(conn, index)) {
        t.put(new Put(lower.anchor).addColumn(FAMILY,
          HnswSegment.qualifier(lower.identity, HnswSegment.WRITER_TAG), lower.time,
          Bytes.toBytes("another region")));
      }
      long reopened = EnvironmentEdgeManager.currentTimeMillis();
      SegmentFaults.hold = new CountDownLatch(1);
      try {
        reopen(table);
        HnswIndexManager manager = manager(regions(table).get(0), index);
        assertTrue(manager.covered());
        for (String id : new String[] { "a0", "a10", "z1", "z21" }) {
          assertEquals(id, nearest(table, index, id, rows.get(id)));
          assertEquals(id, query2(conn, table, rows.get(id)).get(0));
        }
        // The rebuild of the lower region cannot write its base yet
        List<HnswSegment.Descriptor> segments = segments(conn, index);
        assertEquals(2, segments.size());
        for (HnswSegment.Descriptor d : segments) {
          assertTrue(d.toString(),
            Bytes.equals(lower.identity, d.identity) || Bytes.equals(upper.identity, d.identity));
        }
      } finally {
        SegmentFaults.hold.countDown();
        SegmentFaults.hold = null;
      }
      regions = regions(table);
      HnswSegment.Descriptor rebuilt = awaitSegment(conn, index, regions.get(0), reopened, 60);
      assertArrayEquals(regions.get(0).getRegionInfo().getEncodedNameAsBytes(), rebuilt.writer);
      List<HnswSegment.Descriptor> segments = awaitSegments(conn, index, 2);
      boolean kept = false;
      for (HnswSegment.Descriptor d : segments) {
        assertFalse(Bytes.equals(lower.identity, d.identity));
        kept |= Bytes.equals(upper.identity, d.identity);
      }
      assertTrue("the upper region must keep its base", kept);
      awaitCovered(table, index);
      for (String id : new String[] { "a0", "a10", "z1", "z21" }) {
        assertEquals(id, nearest(table, index, id, rows.get(id)));
        assertEquals(id, query2(conn, table, rows.get(id)).get(0));
      }
    }
  }

  /**
   * A segment write and the retirement after it skip the WAL. If the region server fails after them
   * and before the flush of V#, the V# memstore is lost, and the data rows stay. The test drops the
   * V# memstore at its flushes through a rebuild and the close of the region, which has the same
   * effect. The region opens with the previous segment, replays the row changes since its replay
   * start, and returns correct results.
   */
  @Test
  public void testSegmentWriteLostBeforeFlushRecovers() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200, "");
      installFaults(table);
      HnswSegment.Descriptor base = segments(conn, index).get(0);
      // Row changes after the base
      Random random = new Random(37);
      float[] added = vector(random);
      float[] moved = negate(rows.get("a2"));
      upsert(conn, table, "a-new", added);
      upsert(conn, table, "a2", moved);
      delete(conn, table, "z3");
      int dropped = SegmentFaults.DROPPED.get();
      SegmentFaults.dropFlushes = true;
      try {
        manager(regions(table).get(0), index).rebuild(Long.MAX_VALUE);
        // The flush after the segment write drops the segment. The region then cuts over to the
        // segment in memory. It finds no newer base in V#, so it does not retire the old base
        for (int i = 0; i < 600 && SegmentFaults.DROPPED.get() == dropped; i++) {
          Thread.sleep(100);
        }
        assertTrue(SegmentFaults.DROPPED.get() > dropped);
        Thread.sleep(1000);
        reopen(table);
      } finally {
        SegmentFaults.dropFlushes = false;
      }

      HnswIndexManager manager = manager(regions(table).get(0), index);
      assertTrue(manager.covered());
      List<HnswSegment.Descriptor> segments = segments(conn, index);
      assertEquals(1, segments.size());
      assertTrue(Bytes.equals(base.identity, segments.get(0).identity));
      assertEquals("a-new", nearest(table, index, "a-new", added));
      assertEquals("a2", nearest(table, index, "a2", moved));
      assertNotFound(table, index, "a2", rows.get("a2"));
      assertNotFound(table, index, "z3", rows.get("z3"));
      try (PreparedStatement ps = conn
        .prepareStatement("SELECT ID FROM " + table + " ORDER BY COSINE_DISTANCE(V, ?) LIMIT 1")) {
        ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(moved)));
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("a2", rs.getString(1));
        }
      }
    }
  }

  /**
   * DROP INDEX purges the segment cells of the index from every region. The data rows, the other
   * HNSW index of the table and its segments stay. A DROP COLUMN of the vector column drops the
   * other index, and also purges its segment cells.
   */
  @Test
  public void testDropIndexPurgesSegments() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    String other = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      conn.createStatement().execute("CREATE VECTOR INDEX " + other + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      buildIndex(table, other);
      assertEquals(12, cellsOf(conn, table, index).size());
      List<String> kept = cellsOf(conn, table, other);
      assertEquals(12, kept.size());

      conn.createStatement().execute("DROP INDEX " + index + " ON " + table);
      assertTrue(cellsOf(conn, table, index).isEmpty());
      assertEquals(kept, cellsOf(conn, table, other));
      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + table)) {
        assertTrue(rs.next());
        assertEquals(rows.size(), rs.getInt(1));
      }
      assertEquals("a0", nearest(table, other, "a0", rows.get("a0")));
      assertEquals("a0", query2(conn, table, rows.get("a0")).get(0));

      conn.createStatement().execute("ALTER TABLE " + table + " DROP COLUMN V");
      assertTrue(cellsOf(conn, table, other).isEmpty());
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
        assertTrue(rs.next());
        assertEquals(rows.size(), rs.getInt(1));
      }
    }
  }

  /**
   * A region refuses the purge of a live HNSW index of its table. Thus any client can send a purge,
   * and a purge never deletes live segments. A purge of a name that is not an HNSW index of the
   * table deletes the cells of that name, for example the cells that the purge of a dropped index
   * missed.
   */
  @Test
  public void testPurgeRefusedForLiveIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    String stray = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      List<String> before = hiddenCells(conn, table);
      List<RegionInfo> regions = regionInfos(conn, table);
      List<IOException> failures = new ArrayList<>();
      try (Table t = dataTable(conn, index)) {
        HnswRegionRequests.send(t, regions, BaseScannerRegionObserverConstants.HNSW_PURGE,
          region -> Bytes.toBytes(index), (region, value, failure) -> {
            assertNull(value);
            failures.add(failure);
          });
      }
      assertEquals(regions.size(), failures.size());
      for (IOException failure : failures) {
        assertNotNull(failure);
        assertTrue(failure.toString(), failure.toString().contains("not purged"));
      }
      assertEquals(before, hiddenCells(conn, table));

      // Cells of a dropped index that a purge missed
      HnswSegment.Descriptor orphan =
        new HnswSegment.Descriptor(Bytes.toBytes(stray), HConstants.EMPTY_START_ROW, 1,
          Bytes.toBytes("m"), 0, null, 1, HnswSegment.definitionDigest("dropped"));
      try (Table t = dataTable(conn, index)) {
        t.put(new Put(orphan.anchor).addColumn(FAMILY,
          HnswSegment.qualifier(orphan.identity, HnswSegment.END_KEY_TAG), orphan.endKey)
          .addColumn(FAMILY, HnswSegment.qualifier(orphan.identity, HnswSegment.COUNT_TAG),
            Bytes.toBytes(0)));
        List<Long> purged = new ArrayList<>();
        HnswRegionRequests.send(t, regions, BaseScannerRegionObserverConstants.HNSW_PURGE,
          region -> Bytes.toBytes(stray), (region, value, failure) -> {
            assertNull(failure);
            purged.add(Bytes.toLong(value));
          });
        assertEquals(Arrays.asList(2L, 0L), purged);
      }
      assertEquals(before, hiddenCells(conn, table));
      assertEquals("a0", query2(conn, table, rows.get("a0")).get(0));
    }
  }

  /**
   * If the purge of a dropped index misses the regions, the segment cells of the index stay. An
   * index created again with the same name and another definition ignores them. The definition
   * fence leaves them out of searches and of the build, and the first base of each region retires
   * them.
   */
  @Test
  public void testRecreatedIndexIgnoresStaleSegments() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, float[]> rows = createAndBuild(conn, table, index, 200);
      List<HnswSegment.Descriptor> old = segments(conn, index);
      assertEquals(2, old.size());
      installFaults(table);
      // The purge misses the regions
      SegmentFaults.failPurges = true;
      try {
        conn.createStatement().execute("DROP INDEX " + index + " ON " + table);
      } finally {
        SegmentFaults.failPurges = false;
      }
      assertEquals(12, cellsOf(conn, table, index).size());

      conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='L2', M=8) ASYNC");
      buildIndex(table, index);
      List<HnswSegment.Descriptor> segments = awaitSegments(conn, index, 2);
      for (HnswSegment.Descriptor d : segments) {
        for (HnswSegment.Descriptor o : old) {
          assertFalse(Bytes.equals(o.identity, d.identity));
          assertFalse(Bytes.equals(o.definition, d.definition));
        }
      }
      for (String id : new String[] { "a0", "z1", "a10", "z21" }) {
        assertEquals(id, nearest(table, index, id, rows.get(id)));
      }
    }
  }

  /**
   * The data paths of a table with segments ignore the cells of V#. SELECT *, COUNT(*), ROW_SIZE()
   * and RAW_ROW_SIZE() see only the data rows. UPSERT SELECT copies only the data rows, and DELETE
   * deletes no segment. UPDATE STATISTICS writes no statistics for V#. A major compaction of a
   * table with a TTL keeps a segment of each region.
   */
  @Test
  public void testHiddenFamilyIsolation() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    String copy = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, "
        + DIM + ")) TTL=86400 SPLIT ON ('m')");
      Map<String, float[]> rows = new HashMap<>();
      Random random = new Random(43);
      for (int i = 0; i < 100; i++) {
        String id = (i % 2 == 0 ? "a" : "z") + i;
        rows.put(id, vector(random));
        upsert(conn, table, id, rows.get(id));
      }
      stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
      buildIndex(table, index);
      assertEquals(2, segments(conn, index).size());

      Set<String> ids = new HashSet<>();
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
      assertEquals(rows.keySet(), ids);
      assertEquals(100, count(stmt, "SELECT COUNT(*) FROM " + table));
      for (String size : new String[] { "ROW_SIZE()", "RAW_ROW_SIZE()" }) {
        assertEquals(100, count(stmt,
          "SELECT COUNT(*) FROM (SELECT ID, SUM(" + size + ") FROM " + table + " GROUP BY ID)"));
      }
      stmt.execute("CREATE TABLE " + copy + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, "
        + DIM + "))");
      stmt.execute("UPSERT INTO " + copy + " SELECT * FROM " + table);
      conn.commit();
      assertEquals(100, count(stmt, "SELECT COUNT(*) FROM " + copy));

      stmt.execute("DELETE FROM " + table + " WHERE ID < 'a2'");
      conn.commit();
      assertEquals(2, segments(conn, index).size());
      rows.remove("a0");
      rows.remove("a10");
      rows.remove("a12");
      rows.remove("a14");
      rows.remove("a16");
      rows.remove("a18");
      assertEquals(rows.size(), count(stmt, "SELECT COUNT(*) FROM " + table));

      stmt.execute("UPDATE STATISTICS " + table);
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM SYSTEM.STATS WHERE PHYSICAL_NAME = ? AND COLUMN_FAMILY = ?")) {
        ps.setString(1, table);
        ps.setString(2, QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(0, rs.getLong(1));
        }
      }

      long compacted = EnvironmentEdgeManager.currentTimeMillis();
      try (Admin admin = getUtility().getAdmin()) {
        admin.flush(TableName.valueOf(table));
      }
      getUtility().compact(TableName.valueOf(table), true);
      // A major compaction of a table with a TTL rebuilds each region
      for (HRegion region : regions(table)) {
        awaitSegment(conn, index, region, compacted, 60);
      }
      awaitSegments(conn, index, 2);
      assertEquals(rows.size(), count(stmt, "SELECT COUNT(*) FROM " + table));
      for (String id : new String[] { "a2", "z1" }) {
        assertEquals(id, query2(conn, table, rows.get(id)).get(0));
      }
    }
  }

  private static long count(Statement stmt, String sql) throws SQLException {
    try (ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

  /**
   * The VectorIndex metrics source publishes the HNSW telemetry of each index. A build, writes past
   * the flush threshold and a filtered query update the metrics of the index. A failed open of a
   * name that is not an HNSW index counts only for the server. A drop of the index closes its
   * managers, so the gauges of the index return to 0.
   */
  @Test
  public void testMetrics() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndBuild(conn, table, index, 400);
      assertEquals(2, value(HnswMetric.REGIONS, index));
      assertEquals(2, value(HnswMetric.SEGMENTS, index));
      assertEquals(0, value(HnswMetric.REGIONS_UNCOVERED, index));
      assertEquals(0, value(HnswMetric.REGIONS_UNRESTRICTED, index));
      assertEquals(2, value(HnswMetric.OPEN_TIME, index));
      assertTrue(value(HnswMetric.REBUILDS, index) >= 2);
      assertEquals(value(HnswMetric.REBUILDS, index), value(HnswMetric.REBUILD_TIME, index));
      assertTrue(value(HnswMetric.STORE_FLUSH_WAIT_TIME, index) >= 2);
      assertTrue(value(HnswMetric.SEGMENT_BYTES_WRITTEN, index) > 0);
      assertTrue(value(HnswMetric.OFFHEAP_RESIDENT_SEGMENTS) >= 2);
      assertTrue(value(HnswMetric.OFFHEAP_ALLOCATED_BYTES) > 0);
      assertTrue(value(HnswMetric.OFFHEAP_MAX_BYTES) > 0);

      // Write past the flush threshold of the lower region. Its base is small, so the flush
      // queues a full rebuild
      long rebuilds = value(HnswMetric.REBUILDS, index);
      long applied = value(HnswMetric.CHANGES_APPLIED, index);
      int written = HnswIndexManager.FLUSH_THRESHOLD + 100;
      conn.setAutoCommit(false);
      Random random = new Random(71);
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        for (int i = 0; i < written; i++) {
          ps.setString(1, String.format("b%05d", i));
          ps.setArray(2, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(vector(random))));
          ps.executeUpdate();
          if (i % 1000 == 999) {
            conn.commit();
          }
        }
      }
      conn.commit();
      conn.setAutoCommit(true);
      assertEquals(applied + written, value(HnswMetric.CHANGES_APPLIED, index));
      for (int i = 0; i < 600 && value(HnswMetric.REBUILDS, index) <= rebuilds; i++) {
        Thread.sleep(100);
      }
      assertTrue(value(HnswMetric.REBUILDS, index) > rebuilds);
      awaitCovered(table, index);
      assertTrue(value(HnswMetric.MUTABLE_ENTRIES, index) < HnswIndexManager.FLUSH_THRESHOLD);

      // A filtered query probes its candidates in each region
      String query =
        "SELECT ID FROM " + table + " WHERE ID LIKE '%5' ORDER BY COSINE_DISTANCE(V, ?) LIMIT 3";
      float[] v = vector(random);
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(v)));
        assertTrue(QueryUtil.getExplainPlan(ps.executeQuery()).contains("HNSW SEARCH"));
      }
      long searches = value(HnswMetric.SEARCHES, index);
      long searchTimes = value(HnswMetric.SEARCH_TIME, index);
      long rounds = value(HnswMetric.PROBE_ROUNDS, index);
      long rowsRead = value(HnswMetric.PROBE_ROWS_READ, index);
      long visited = value(HnswMetric.NODES_VISITED, index);
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", HnswIndexFsckIT.boxed(v)));
        try (ResultSet rs = ps.executeQuery()) {
          int found = 0;
          while (rs.next()) {
            assertTrue(rs.getString(1).endsWith("5"));
            found++;
          }
          assertEquals(3, found);
        }
      }
      long searched = value(HnswMetric.SEARCHES, index) - searches;
      assertTrue("searches " + searched, searched >= 2);
      assertEquals(searched, value(HnswMetric.SEARCH_TIME, index) - searchTimes);
      assertTrue(value(HnswMetric.PROBE_ROUNDS, index) >= rounds + 2);
      assertTrue(value(HnswMetric.PROBE_ROWS_READ, index) > rowsRead);
      assertTrue(value(HnswMetric.NODES_VISITED, index) > visited);

      // A build request that names no HNSW index fails to open a manager. The failure counts only
      // for the server, so a request cannot add metrics for any name
      String other = generateUniqueName();
      long failures = value(HnswMetric.OPEN_FAILURES);
      List<RegionInfo> regions = regionInfos(conn, table);
      try (Table t = dataTable(conn, index)) {
        HnswRegionRequests.send(t, regions, BaseScannerRegionObserverConstants.HNSW_BUILD,
          region -> Bytes.add(Bytes.toBytes(Long.MAX_VALUE), Bytes.toBytes(other)),
          (region, value, failure) -> assertNotNull(failure));
      }
      assertEquals(failures + regions.size(), value(HnswMetric.OPEN_FAILURES));
      assertEquals(0, value(HnswMetric.OPEN_FAILURES, index));
      assertNull(HnswMetricValues.server().getMetricsRegistry()
        .get(HnswMetric.OPEN_FAILURES.metricName() + "." + other));

      // The drop purges the segments, and the purge closes the managers
      conn.createStatement().execute("DROP INDEX " + index + " ON " + table);
      for (int i = 0; i < 600 && value(HnswMetric.REGIONS, index) > 0; i++) {
        Thread.sleep(100);
      }
      for (HnswMetric gauge : new HnswMetric[] { HnswMetric.REGIONS, HnswMetric.SEGMENTS,
        HnswMetric.REGIONS_UNCOVERED, HnswMetric.REGIONS_UNRESTRICTED,
        HnswMetric.MUTABLE_ENTRIES }) {
        assertEquals(gauge.metricName(), 0, value(gauge, index));
      }
    }
  }
}
