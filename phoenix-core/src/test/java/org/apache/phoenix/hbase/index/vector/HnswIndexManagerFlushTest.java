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
package org.apache.phoenix.hbase.index.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.SegmentCells;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.LiteralTTLExpression;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ServerUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for the flush, rebuild, write and split paths of {@link HnswIndexManager}. The tests
 * use mock data regions that keep their rows in memory, and the {@code V#} cells of these regions.
 */
public class HnswIndexManagerFlushTest {
  private static final int DIM = 8;
  private static final PTable.VectorIndex VI = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
    .setDistanceMetric("L2").setDimension(DIM).setHnswM(16).setHnswEfConstruction(100)
    .setHnswAlpha(1.2).setQuantizationType("NONE").build();
  private static final byte[] FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
  private static final byte[] SEGMENT_FAMILY =
    QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] V = Bytes.toBytes("V");
  private static final byte[] X = Bytes.toBytes("X");
  // A column family that the index does not read
  private static final byte[] OTHER = Bytes.toBytes("F");
  private static final ColumnReference V_REF = new ColumnReference(FAMILY, V);
  private static final long TIMEOUT_MS = 30_000;

  /**
   * Runs before a region stores the cells of a segment. The hook can block the write, or make it
   * fail.
   */
  interface PutHook {
    void before(Put put) throws IOException;
  }

  /**
   * Runs before a region deletes the cells of segments in one anchor row. The hook can block the
   * delete, or make it fail.
   */
  interface DeleteHook {
    void before(Delete delete) throws IOException;
  }

  // The V# cells of every region. A region reads only the anchor rows in its own range
  private static final SegmentCells CELLS = new SegmentCells();
  private static volatile PutHook putHook = put -> {
  };
  private static volatile DeleteHook deleteHook = delete -> {
  };
  // The scans of V# that the regions receive
  private static final List<Scan> SEGMENT_SCANS = new CopyOnWriteArrayList<>();
  // The split requests that the managers send to the master
  private static final List<SplitRequest> SPLITS = new CopyOnWriteArrayList<>();
  // The server connection that the split requests use
  private static Connection connection;

  /** A split request: the region name, and the thread that sent the request. */
  private static final class SplitRequest {
    final byte[] regionName;
    final Thread thread;

    SplitRequest(byte[] regionName) {
      this.regionName = regionName;
      this.thread = Thread.currentThread();
    }
  }

  private final List<HnswIndexManager> managers = new ArrayList<>();

  @BeforeClass
  public static void setUpClass() throws IOException {
    ServerUtil.ConnectionFactory.shutdown();
    CELLS.setHook(m -> {
      if (m instanceof Put) {
        putHook.before((Put) m);
      } else if (m instanceof Delete) {
        deleteHook.before((Delete) m);
      }
    });
    connection = mock(Connection.class);
    Admin admin = mock(Admin.class);
    when(admin.splitRegionAsync(any(byte[].class))).thenAnswer(inv -> {
      SPLITS.add(new SplitRequest(inv.getArgument(0)));
      return CompletableFuture.completedFuture(null);
    });
    when(connection.getAdmin()).thenReturn(admin);
  }

  @AfterClass
  public static void tearDownClass() {
    ServerUtil.ConnectionFactory.shutdown();
  }

  @After
  public void tearDown() {
    putHook = put -> {
    };
    deleteHook = delete -> {
    };
    for (HnswIndexManager manager : managers) {
      manager.close();
    }
  }

  // Returns the segment time of a segment write
  private static long time(Put put) {
    Cell cell = put.getFamilyCellMap().values().iterator().next().get(0);
    return HnswSegment.parseQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength()).time;
  }

  // Returns true if a segment write writes a delta segment, which has a base time cell
  private static boolean isDelta(Put put) {
    for (List<Cell> cells : put.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        HnswSegment.Qualifier q = HnswSegment.parseQualifier(cell.getQualifierArray(),
          cell.getQualifierOffset(), cell.getQualifierLength());
        if (q != null && q.tag == HnswSegment.BASE_TIME_TAG) {
          return true;
        }
      }
    }
    return false;
  }

  // Stores the cells of a segment write, as a region does before the writer gets a failure
  private static void store(Put put) {
    for (List<Cell> cells : put.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        CELLS.put(CellUtil.cloneRow(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp(),
          CellUtil.cloneValue(cell));
      }
    }
  }

  // Returns true if the delete removes cells of the segment
  private static boolean deletes(Delete delete, HnswSegment.Descriptor d) {
    for (List<Cell> cells : delete.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        if (Bytes.startsWith(CellUtil.cloneQualifier(cell), d.identity)) {
          return true;
        }
      }
    }
    return false;
  }

  private static byte[] encode(float[] v) {
    byte[] bytes = new byte[v.length * Bytes.SIZEOF_FLOAT];
    for (int i = 0; i < v.length; i++) {
      Bytes.putFloat(bytes, i * Bytes.SIZEOF_FLOAT, v[i]);
    }
    return bytes;
  }

  private static float[] decode(ImmutableBytesWritable ptr) {
    float[] v = new float[ptr.getLength() / Bytes.SIZEOF_FLOAT];
    for (int i = 0; i < v.length; i++) {
      v[i] = Bytes.toFloat(ptr.get(), ptr.getOffset() + i * Bytes.SIZEOF_FLOAT);
    }
    return v;
  }

  private static float[] point(float x, float y) {
    float[] v = new float[DIM];
    v[0] = x;
    v[1] = y;
    return v;
  }

  private static float[] vector(Random random) {
    float[] v = new float[DIM];
    for (int d = 0; d < DIM; d++) {
      v[d] = (float) random.nextGaussian();
    }
    return v;
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    if (!poll(condition, TIMEOUT_MS)) {
      fail("timed out waiting for " + what);
    }
  }

  private static boolean poll(BooleanSupplier condition, long timeoutMs)
    throws InterruptedException {
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + timeoutMs;
    while (!condition.getAsBoolean()) {
      if (EnvironmentEdgeManager.currentTimeMillis() > deadline) {
        return false;
      }
      Thread.sleep(10);
    }
    return true;
  }

  /**
   * Requests a flush or a rebuild until the condition holds. A request that arrives while the task
   * of an earlier request ends can have no effect, so this helper sends the request again.
   * IndexTool also sends its build requests again.
   */
  private static void request(String what, Runnable request, BooleanSupplier condition)
    throws InterruptedException {
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + TIMEOUT_MS;
    do {
      request.run();
      if (poll(condition, 2000)) {
        return;
      }
    } while (EnvironmentEdgeManager.currentTimeMillis() < deadline);
    fail("timed out waiting for " + what);
  }

  /**
   * A mock data region that keeps its rows in an in-memory map. The harness opens managers of one
   * HNSW index on the region.
   */
  private final class Harness {
    final byte[] start;
    final byte[] end;
    // The name of the index, which is also the key of its segments
    String index;
    final NavigableMap<byte[], float[]> rows = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
    // The cell timestamp of each row. A row without an entry has LATEST_TIMESTAMP
    final Map<byte[], Long> times = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
    final Region region = mock(Region.class);
    final RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    final Configuration conf = HBaseConfiguration.create();
    // The timestamp of the oldest batch that has not applied its graph changes
    volatile long oldestBatch = Long.MAX_VALUE;
    volatile HnswIndexManager.Catalog catalog = scan -> true;
    // Selects the rows whose vector cannot be read. A change of such a row fails to apply
    volatile Predicate<byte[]> failing = row -> false;
    // When the test sets these latches, a region scan counts down scanEntered, and then waits for
    // scanGate before it returns each row
    volatile CountDownLatch scanEntered;
    volatile CountDownLatch scanGate;
    // The scans of the data rows that the managers send to the region
    final List<Scan> scans = new CopyOnWriteArrayList<>();

    Harness(String name) throws IOException {
      this(name, 0);
    }

    Harness(String name, long regionId) throws IOException {
      this(Bytes.toBytes(name + "/"), Bytes.toBytes(name + "0"), regionId);
    }

    Harness(byte[] start, byte[] end, long regionId) throws IOException {
      this.start = start;
      this.end = end;
      this.index = "I" + Bytes.toString(start);
      RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf("T")).setStartKey(start)
        .setEndKey(end).setRegionId(regionId).build();
      when(region.getRegionInfo()).thenReturn(info);
      when(region.getTableDescriptor())
        .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T"))
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
      CELLS.stub(region);
      when(region.getScanner(any(Scan.class))).thenAnswer(inv -> {
        Scan scan = inv.getArgument(0);
        if (SegmentCells.isSegmentScan(scan)) {
          SEGMENT_SCANS.add(scan);
          return CELLS.scanner(scan, info);
        }
        scans.add(scan);
        Iterator<Map.Entry<byte[], float[]>> it = new TreeMap<>(rows).entrySet().iterator();
        CountDownLatch entered = scanEntered;
        CountDownLatch gate = scanGate;
        RegionScanner scanner = mock(RegionScanner.class);
        when(scanner.next(anyList())).thenAnswer(next -> {
          if (gate != null) {
            entered.countDown();
            gate.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
          }
          if (it.hasNext()) {
            Map.Entry<byte[], float[]> e = it.next();
            long ts = times.getOrDefault(e.getKey(), HConstants.LATEST_TIMESTAMP);
            List<Cell> cells = next.getArgument(0);
            cells.add(new KeyValue(e.getKey(), FAMILY, QueryConstants.EMPTY_COLUMN_BYTES, ts,
              QueryConstants.EMPTY_COLUMN_VALUE_BYTES));
            cells.add(new KeyValue(e.getKey(), FAMILY, V, ts, encode(e.getValue())));
          }
          return it.hasNext();
        });
        return scanner;
      });
      when(region.get(any(Get.class))).thenAnswer(inv -> {
        if (SegmentCells.isSegmentGet(inv.getArgument(0))) {
          return CELLS.get(inv.getArgument(0));
        }
        byte[] row = ((Get) inv.getArgument(0)).getRow();
        float[] v = rows.get(row);
        return v == null
          ? Result.create(Collections.emptyList())
          : Result.create(Collections.singletonList(new KeyValue(row, FAMILY, V, encode(v))));
      });
      when(env.getRegion()).thenReturn(region);
      when(env.getConfiguration()).thenReturn(conf);
      when(env.createConnection(any(Configuration.class))).thenReturn(connection);
    }

    byte[] key(String key) {
      return Bytes.add(start, Bytes.toBytes(key));
    }

    HnswIndexManager open(boolean active) throws IOException {
      return open(active, false);
    }

    HnswIndexManager open(boolean active, boolean ttl) throws IOException {
      PColumn column = mock(PColumn.class);
      when(column.getFamilyName()).thenReturn(PNameFactory.newName("0"));
      doReturn(PVectorFloat.INSTANCE).when(column).getDataType();
      when(column.getExpressionStr()).thenReturn("\"0\".\"V\"");
      PTable index = mock(PTable.class);
      when(index.getName()).thenReturn(PNameFactory.newName(this.index));
      when(index.getVectorIndex()).thenReturn(VI);
      when(index.getColumns()).thenReturn(Collections.singletonList(column));
      when(index.getColumnFamilies()).thenReturn(Collections.emptyList());
      IndexMaintainer maintainer = mock(IndexMaintainer.class);
      when(maintainer.getVectorAsFloats(any(ValueGetter.class), anyLong(), anyBoolean()))
        .thenAnswer(inv -> {
          ValueGetter getter = inv.getArgument(0);
          if (failing.test(getter.getRowKey())) {
            throw new IllegalStateException("injected");
          }
          ImmutableBytesWritable ptr = getter.getLatestValue(V_REF, HConstants.LATEST_TIMESTAMP);
          return ptr == null ? null : decode(ptr);
        });
      when(maintainer.getAllColumnsForDataTable()).thenReturn(Collections.singleton(V_REF));
      HnswIndexManager manager = new HnswIndexManager(env, index, maintainer, active, ttl,
        scan -> catalog.isCurrent(scan), () -> oldestBatch);
      managers.add(manager);
      return manager;
    }

    private Put put(byte[] row, float[] v) {
      return v == null ? null : new Put(row).addColumn(FAMILY, V, encode(v));
    }

    // Commits a row change, and then gives the change to the manager, as a write batch does
    void upsert(HnswIndexManager manager, String key, float[] v) {
      byte[] row = key(key);
      Put current = put(row, rows.get(row));
      rows.put(row, v);
      manager.onMutation(current, put(row, v), v);
    }

    void delete(HnswIndexManager manager, String key) {
      byte[] row = key(key);
      Put current = put(row, rows.remove(row));
      manager.onMutation(current, null, null);
    }

    // Returns the segments with the start key of this region, newest first
    List<HnswSegment.Descriptor> segments() throws IOException {
      List<HnswSegment.Descriptor> segments = new ArrayList<>();
      for (HnswSegment.Descriptor d : CELLS.list(Bytes.toBytes(index))) {
        if (Bytes.equals(d.startKey, start)) {
          segments.add(d);
        }
      }
      segments.sort((a, b) -> Long.compare(b.time, a.time));
      return segments;
    }

    // Whether the region has exactly one segment, and the manager searches it. That segment must be
    // a base with a time at or after the given time. The manager retires older segments only after
    // it installs a new segment. The row count shows the install of the first segment, because the
    // manager counts no rows before then
    boolean hasBase(HnswIndexManager manager, long since) {
      try {
        List<HnswSegment.Descriptor> segments = segments();
        return segments.size() == 1 && !segments.get(0).isDelta() && segments.get(0).time >= since
          && manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW)
              >= segments.get(0).count;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Whether the region has a segment with the given time
    boolean hasSegment(long time) {
      try {
        return segments().stream().anyMatch(d -> d.time == time);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    HnswSegment.Descriptor awaitBase(HnswIndexManager manager, long since)
      throws InterruptedException, IOException {
      await("a base segment written after " + since, () -> hasBase(manager, since));
      return segments().get(0);
    }

    HnswSegment.Descriptor rebuild(HnswIndexManager manager)
      throws InterruptedException, IOException {
      long since = EnvironmentEdgeManager.currentTimeMillis();
      request("a rebuild", () -> manager.rebuild(Long.MAX_VALUE), () -> hasBase(manager, since));
      return segments().get(0);
    }

    boolean isRow(Put put) {
      return Bytes.startsWith(put.getRow(), start);
    }

    // Returns the keys of the k rows nearest to the vector. A search uses its candidate count as
    // its beam width, so this helper asks for up to 50 candidates. It asks for fewer candidates
    // than the indexed rows, because a search for all rows returns them without rank order
    List<byte[]> nearest(HnswIndexManager manager, float[] v, int k) throws IOException {
      int candidates = Math.max(k,
        Math.min(50, manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW) - 1));
      List<byte[]> found =
        manager.search(v, candidates, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null);
      return found.subList(0, Math.min(k, found.size()));
    }

    String nearest(HnswIndexManager manager, float[] v) throws IOException {
      List<byte[]> found = nearest(manager, v, 1);
      return found.isEmpty()
        ? null
        : Bytes.toString(found.get(0), start.length, found.get(0).length - start.length);
    }

    // Returns true if a search for the ten rows nearest to the vector finds the key
    boolean found(HnswIndexManager manager, float[] v, String key) throws IOException {
      for (byte[] row : nearest(manager, v, 10)) {
        if (Bytes.equals(row, key(key))) {
          return true;
        }
      }
      return false;
    }

    // Gives the store of the family a file that a major compaction wrote at the given time
    void compacted(byte[] family, long time) throws IOException {
      StoreFile file = mock(StoreFile.class);
      when(file.isMajorCompactionResult()).thenReturn(true);
      when(file.getModificationTimestamp()).thenReturn(time);
      Store store = mock(Store.class);
      doReturn(Collections.singletonList(file)).when(store).getStorefiles();
      when(region.getStore(family)).thenReturn(store);
    }

    void load(int count, Random random) {
      for (int i = 0; i < count; i++) {
        rows.put(key(String.format("r%03d", i)), vector(random));
      }
    }
  }

  /**
   * A full rebuild writes only the rows that its region scan reads, and does not add the
   * swapped-out graph. The scan starts after the swap, so it holds each buffered change. The scan
   * also holds writes that did not reach the manager, for example while the index was disabled.
   */
  @Test
  public void testFullRebuildStoresRegionScan() throws Exception {
    Harness h = new Harness("scan");
    h.load(30, new Random(1));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);

    float[] buffered = point(50, 0);
    float[] scanned = point(-50, 0);
    float[] reinserted = point(0, 50);
    h.upsert(manager, "k0", point(0, -50));
    h.upsert(manager, "k1", point(0, -60));
    h.upsert(manager, "k0", buffered);
    h.delete(manager, "k1");
    // Changes that do not reach the manager
    h.rows.put(h.key("k0"), scanned);
    h.rows.put(h.key("k1"), reinserted);

    h.rebuild(manager);
    assertEquals("k0", h.nearest(manager, scanned));
    assertEquals("k1", h.nearest(manager, reinserted));
    assertNotEquals("k0", h.nearest(manager, buffered));
  }

  /**
   * A rebuild request that comes while a delta flush runs must swap after the delta flush ends.
   * IndexTool must then find a base segment that is newer than the request.
   */
  @Test
  public void testRebuildRequestedDuringDeltaFlush() throws Exception {
    Harness h = new Harness("pending");
    h.load(30, new Random(2));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);

    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (h.isRow(put) && isDelta(put)) {
        entered.countDown();
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    h.upsert(manager, "r000", point(50, 0));
    request("a delta flush", manager::flush, () -> entered.getCount() == 0);
    since = EnvironmentEdgeManager.currentTimeMillis();
    manager.rebuild(Long.MAX_VALUE);
    release.countDown();
    h.awaitBase(manager, since);
  }

  /**
   * A failed flush runs again when its retry delay ends. A flush request before then has no effect.
   */
  @Test
  public void testFailedFlushWaitsForRetry() throws Exception {
    Harness h = new Harness("retry");
    h.load(30, new Random(3));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);

    AtomicInteger attempts = new AtomicInteger();
    putHook = put -> {
      if (h.isRow(put)) {
        attempts.incrementAndGet();
        throw new IOException("injected");
      }
    };
    h.upsert(manager, "r000", point(50, 0));
    request("a failed flush", manager::flush, () -> attempts.get() > 0);
    assertEquals(1, attempts.get());
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + 1000;
    while (EnvironmentEdgeManager.currentTimeMillis() < deadline) {
      manager.flush();
      Thread.sleep(10);
    }
    assertEquals(1, attempts.get());
  }

  /**
   * When its retry runs, a failed delta flush writes the segment of its original swap. A full
   * rebuild swaps when its task starts, so its retry writes a base that is newer than the failed
   * base. The manager then searches the new base, and retires the base that it replaces.
   */
  @Test
  public void testFailedFlushIsRetried() throws Exception {
    // The retry delay is one minute, so the test calls the retry method directly
    Method retryFlush = HnswIndexManager.class.getDeclaredMethod("retryFlush");
    retryFlush.setAccessible(true);
    for (boolean full : new boolean[] { false, true }) {
      Harness h = new Harness(full ? "retryfull" : "retrydelta");
      h.load(30, new Random(25));
      long since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager manager = h.open(true);
      HnswSegment.Descriptor base = h.awaitBase(manager, since);

      AtomicReference<Long> failed = new AtomicReference<>();
      putHook = put -> {
        if (h.isRow(put)) {
          failed.compareAndSet(null, time(put));
          throw new IOException("injected");
        }
      };
      float[] moved = point(50, 0);
      h.upsert(manager, "r000", moved);
      long requested = EnvironmentEdgeManager.currentTimeMillis();
      request(full ? "a failed rebuild" : "a failed delta flush",
        full ? () -> manager.rebuild(Long.MAX_VALUE) : manager::flush, () -> failed.get() != null);
      putHook = put -> {
      };
      // A retry that starts before the failed task ends has no effect, so the test runs it again
      request("the retry", () -> {
        try {
          retryFlush.invoke(manager);
        } catch (ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }, () -> full ? h.hasBase(manager, requested) : h.hasSegment(failed.get()));
      HnswSegment.Descriptor written = h.segments().get(0);
      if (full) {
        assertTrue(written.time > failed.get());
        assertEquals(30, written.count);
      } else {
        assertEquals((long) failed.get(), written.time);
        assertEquals(Long.valueOf(base.time), written.baseTime);
        assertEquals(1, written.count);
      }
      assertEquals("full=" + full, "r000", h.nearest(manager, moved));
    }
  }

  /**
   * A forced rebuild after a failed delta flush takes a new swap time, so its base has a new
   * identity. {@code V#} can still hold the cells of the failed delta. A base with the identity of
   * the delta would have the base time and tombstones cells of the delta.
   */
  @Test
  public void testForcedRebuildOfFailedDeltaWritesNewRow() throws Exception {
    Harness h = new Harness("rewrite");
    h.load(30, new Random(4));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(manager, since);

    AtomicInteger failed = new AtomicInteger();
    putHook = put -> {
      if (h.isRow(put) && isDelta(put)) {
        // V# stores the cells of the put, but the writer gets a failure
        store(put);
        failed.incrementAndGet();
        throw new IOException("injected");
      }
    };
    h.upsert(manager, "r000", point(50, 0));
    h.delete(manager, "r001");
    request("a failed delta flush", manager::flush, () -> failed.get() > 0);
    List<HnswSegment.Descriptor> segments = h.segments();
    assertEquals(2, segments.size());
    assertEquals(Long.valueOf(base.time), segments.get(0).baseTime);

    since = EnvironmentEdgeManager.currentTimeMillis();
    manager.rebuild(Long.MAX_VALUE);
    HnswSegment.Descriptor rebuilt = h.awaitBase(manager, since);
    assertTrue(rebuilt.time > segments.get(0).time);
    for (byte tag : new byte[] { HnswSegment.BASE_TIME_TAG, HnswSegment.TOMBSTONES_TAG }) {
      assertTrue(CELLS.get(new Get(rebuilt.anchor).addColumn(SEGMENT_FAMILY,
        HnswSegment.qualifier(rebuilt.identity, tag))).isEmpty());
    }
    assertEquals(29, rebuilt.count);
  }

  /**
   * While a flush runs, the manager refuses writes when the mutable graph reaches its limit. A
   * refused write does not start a flush, so the flush that runs starts the next flush when it
   * ends.
   */
  @Test
  public void testWritesWaitForFullGraph() throws Exception {
    Harness h = new Harness("full");
    HnswIndexManager manager = h.open(false);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (h.isRow(put)) {
        entered.countDown();
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    int limit = HnswIndexManager.BLOCK_MULTIPLIER * HnswIndexManager.FLUSH_THRESHOLD;
    Put current = new Put(h.key("x")).addColumn(FAMILY, V, encode(point(1, 1)));
    for (int i = 0; i < HnswIndexManager.FLUSH_THRESHOLD; i++) {
      manager.onMutation(new Put(h.key("d" + i)).addColumn(FAMILY, V, encode(point(1, 1))), null,
        null);
    }
    assertTrue(entered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
    for (int i = HnswIndexManager.FLUSH_THRESHOLD; i
        < HnswIndexManager.FLUSH_THRESHOLD + limit - 1; i++) {
      manager.onMutation(new Put(h.key("d" + i)).addColumn(FAMILY, V, encode(point(1, 1))), null,
        null);
    }
    manager.checkResources();
    manager.onMutation(current, null, null);
    try {
      manager.checkResources();
      fail("a full mutable graph must refuse writes");
    } catch (RegionTooBusyException e) {
      // Expected
    }
    release.countDown();
    await("writes to be accepted again", () -> {
      try {
        manager.checkResources();
        return true;
      } catch (RegionTooBusyException e) {
        return false;
      }
    });
  }

  /**
   * Each vector change adds a graph node, so repeated updates of one row start a flush. The write
   * that fills the graph starts the first flush. The flush that runs starts the next flush when it
   * ends.
   */
  @Test
  public void testUpdatesOfOneRowStartFlush() throws Exception {
    Harness h = new Harness("hot");
    h.load(30, new Random(7));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);

    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (h.isRow(put) && isDelta(put) && entered.getCount() > 0) {
        entered.countDown();
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    Random random = new Random(8);
    for (int i = 0; i < HnswIndexManager.FLUSH_THRESHOLD; i++) {
      h.upsert(manager, "r000", vector(random));
    }
    assertTrue("updates of one row must start a flush",
      entered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
    for (int i = 0; i < HnswIndexManager.FLUSH_THRESHOLD; i++) {
      h.upsert(manager, "r001", vector(random));
    }
    release.countDown();
    await("a second delta flush", () -> {
      try {
        return h.segments().stream().filter(HnswSegment.Descriptor::isDelta).count() == 2;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * A search that overlaps a swap of the mutable graph must find a segment row that changes after
   * the swap. The new state shares the segment masks, so the change masks the row in the segment.
   * The mutable graph that the search started with does not hold the row, so the search must run
   * again on the new state.
   */
  @Test
  public void testRowChangedDuringSwapIsFound() throws Exception {
    // With the first topK, the test checks a graph search. With the second topK, it checks the
    // collection of all rows, because the region holds no more than topK rows
    for (int topK : new int[] { 5, 100 }) {
      Harness h = new Harness("swap" + topK);
      h.load(30, new Random(9));
      long since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager manager = h.open(true);
      h.awaitBase(manager, since);
      // Make a buffered change, so that the flush below swaps the mutable graph
      h.upsert(manager, "m", point(-100, 0));

      CountDownLatch blocked = new CountDownLatch(1);
      CountDownLatch resume = new CountDownLatch(1);
      AtomicReference<byte[]> first = new AtomicReference<>();
      // This key filter stops the search at the first segment row that it tests
      Predicate<byte[]> keyFilter = key -> {
        if (first.compareAndSet(null, key)) {
          blocked.countDown();
          try {
            resume.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      };
      float[] query = point(100, 0);
      ExecutorService pool = Executors.newSingleThreadExecutor();
      try {
        Future<List<byte[]>> search = pool.submit(() -> manager.search(query, topK,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, keyFilter));
        assertTrue(blocked.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        // Swap the mutable graph, then change a segment row that the search has not tested yet
        String target = Bytes.equals(first.get(), h.key("r000")) ? "r001" : "r000";
        manager.flush();
        h.upsert(manager, target, query);
        resume.countDown();
        boolean found = false;
        for (byte[] key : search.get(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          found |= Bytes.equals(key, h.key(target));
        }
        assertTrue("topK=" + topK + ": the search must find " + target, found);
      } finally {
        resume.countDown();
        pool.shutdownNow();
      }
    }
  }

  /**
   * The cutover masks the entry of the new segment for a row that changes after the swap and before
   * the cutover. The new segment holds the version of the row from before the change. For a delta,
   * this is the buffered version. For a full rebuild, this is the scanned version. The mutable
   * graph does not hold a deleted row, so only the mask hides the old version of that row.
   */
  @Test
  public void testRowChangedBeforeCutoverIsMasked() throws Exception {
    for (boolean full : new boolean[] { false, true }) {
      Harness h = new Harness(full ? "cutfull" : "cutdelta");
      h.load(30, new Random(12));
      long since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager manager = h.open(true);
      h.awaitBase(manager, since);
      // Changes buffered before the swap, so a delta holds these versions
      float[] deleted = point(50, 0);
      float[] stale = point(0, 50);
      h.upsert(manager, "r000", deleted);
      h.upsert(manager, "r001", stale);

      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      putHook = put -> {
        if (h.isRow(put)) {
          entered.countDown();
          try {
            release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      };
      since = EnvironmentEdgeManager.currentTimeMillis();
      request(full ? "a held rebuild" : "a held delta flush",
        full ? () -> manager.rebuild(Long.MAX_VALUE) : manager::flush,
        () -> entered.getCount() == 0);
      // Changes after the swap. A full rebuild scans the region before it writes, so its scan does
      // not hold these changes
      h.delete(manager, "r000");
      float[] moved = point(-50, 0);
      h.upsert(manager, "r001", moved);
      release.countDown();
      if (full) {
        h.awaitBase(manager, since);
      } else {
        // The next flush can swap only after the cutover of the held flush
        request("a second delta", manager::flush, () -> {
          try {
            return h.segments().stream().filter(HnswSegment.Descriptor::isDelta).count() == 2;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      }
      assertFalse("full=" + full + ": a deleted row found by its old vector",
        h.found(manager, deleted, "r000"));
      assertEquals("full=" + full, "r001", h.nearest(manager, moved));
    }
  }

  /**
   * Returns true if a search near the vector, with a key filter that accepts only the given row,
   * finds the row. The region holds more rows than the candidate count, so the search uses the
   * graph. The search cannot fill its results, so it visits each node that it can reach. Thus it
   * misses the row only if the row is outside its range or no node links to the row.
   */
  private static boolean reaches(HnswIndexManager manager, byte[] row, float[] v)
    throws IOException {
    List<byte[]> found = manager.search(v, 10, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      key -> Bytes.equals(key, row));
    return !found.isEmpty() && Bytes.equals(row, found.get(0));
  }

  private static boolean reachesAll(HnswIndexManager manager, Map<byte[], float[]> rows)
    throws IOException {
    for (Map.Entry<byte[], float[]> row : rows.entrySet()) {
      if (!reaches(manager, row.getKey(), row.getValue())) {
        return false;
      }
    }
    return true;
  }

  /**
   * The parent segment is at the anchor row of the parent, which is in the range of the lower
   * daughter. The lower daughter searches the parent segment, clipped to its range, until its own
   * segment replaces it. The rows of the lower daughter are older than the replay start of the
   * parent segment, so replay masks none of them. Thus the lower daughter finds each of its rows in
   * the parent segment. The upper daughter inherits no segment. It is not covered, so its scans
   * read its range without candidate restriction. Each daughter retires only the segments of its
   * own family, so the lower daughter retires the parent segment.
   */
  @Test
  public void testSplitDaughtersSearchOwnRange() throws Exception {
    Harness parent = new Harness("split");
    Random random = new Random(13);
    for (int i = 0; i < 20; i++) {
      parent.rows.put(parent.key(String.format("a%02d", i)), vector(random));
      parent.rows.put(parent.key(String.format("z%02d", i)), vector(random));
    }
    byte[] split = parent.key("m");
    parent.rows.put(split, vector(random));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = parent.open(true);
    HnswSegment.Descriptor parentBase = parent.awaitBase(manager, since);
    // A parallel graph build can leave a row that no node links to. Thus the parent rebuilds until
    // a search reaches each row
    for (int attempt = 0; !reachesAll(manager, parent.rows); attempt++) {
      assertTrue("a search of the parent does not find every row", attempt < 10);
      parentBase = parent.rebuild(manager);
    }
    manager.close();

    // The hook holds the segment writes of the daughters while the test searches
    CountDownLatch release = new CountDownLatch(1);
    List<byte[]> written = new CopyOnWriteArrayList<>();
    putHook = put -> {
      if (parent.isRow(put)) {
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        written.add(put.getRow());
      }
    };
    Harness lower = new Harness(parent.start, split, 1);
    Harness upper = new Harness(split, parent.end, 1);
    HnswIndexManager[] opened = new HnswIndexManager[2];
    try {
      Harness[] daughters = { lower, upper };
      for (int d = 0; d < daughters.length; d++) {
        daughters[d].index = parent.index;
        // The region gets its rows after the open, so the replay scan reads none of them
        opened[d] = daughters[d].open(true);
        daughters[d].rows.putAll(parent.rows.subMap(daughters[d].start, daughters[d].end));
      }
      RegionInfo info = lower.region.getRegionInfo();
      assertTrue(opened[0].covered());
      // The lower daughter has an empty mutable graph, so the searches read only the parent segment
      assertEquals(lower.rows.size(),
        opened[0].count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
      // A search for at least as many rows as the range holds returns all rows of the range
      List<byte[]> all = opened[0].search(point(0, 0), 50, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW, null);
      assertEquals(lower.rows.size(), all.size());
      for (byte[] key : all) {
        assertTrue(Bytes.toStringBinary(key) + " is outside " + info, info.containsRow(key));
      }
      for (Map.Entry<byte[], float[]> row : parent.rows.entrySet()) {
        List<byte[]> found = lower.nearest(opened[0], row.getValue(), 10);
        assertEquals(10, found.size());
        for (byte[] key : found) {
          assertTrue(Bytes.toStringBinary(key) + " is outside " + info, info.containsRow(key));
        }
        assertEquals(Bytes.toStringBinary(row.getKey()) + " in " + info,
          info.containsRow(row.getKey()), reaches(opened[0], row.getKey(), row.getValue()));
      }
      // The upper daughter has no segment of the parent
      assertFalse(opened[1].covered());
      assertEquals(0, opened[1].count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
      assertTrue(opened[1]
        .search(point(0, 0), 50, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null)
        .isEmpty());
    } finally {
      release.countDown();
    }
    upper.awaitBase(opened[1], since);
    lower.awaitBase(opened[0], since);
    assertTrue(opened[1].covered());
    assertEquals(2, written.size());
    // The lower daughter retired the parent segment
    for (HnswSegment.Descriptor d : CELLS.list(Bytes.toBytes(parent.index))) {
      assertFalse(d.toString(), d.time == parentBase.time);
    }
  }

  /**
   * A strict conditional TTL reads an expired row as absent. A later write of the row without a
   * vector must remove the old vector from the index, here from the base segment.
   */
  @Test
  public void testExpiredRowWithoutVectorLeavesGraph() throws Exception {
    Harness h = new Harness("ttl");
    h.load(30, new Random(5));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);
    byte[] expired = h.key("r005");
    float[] old = h.rows.remove(expired);
    manager.onMutation(null, new Put(expired).addColumn(FAMILY, X, Bytes.toBytes(1)), null);
    List<byte[]> found =
      manager.search(old, 100, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null);
    assertEquals(29, found.size());
    for (byte[] key : found) {
      assertFalse(Bytes.equals(expired, key));
    }
  }

  /**
   * A search that runs while a segment row changes for the first time must find the row. The change
   * adds the new version before it masks the old version, and the search reads the segments first.
   */
  @Test
  public void testRowChangedDuringSearchesIsFound() throws Exception {
    Harness h = new Harness("race");
    Random random = new Random(6);
    int keys = 300;
    List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < keys; i++) {
      float[] v = vector(random);
      for (int d = 0; d < DIM; d++) {
        v[d] *= 10;
      }
      vectors.add(v);
      h.rows.put(h.key(String.format("h%03d", i)), v);
    }
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);

    AtomicInteger changing = new AtomicInteger();
    AtomicBoolean done = new AtomicBoolean();
    AtomicInteger searched = new AtomicInteger();
    AtomicInteger missed = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<?>> searches = new ArrayList<>();
      for (int t = 0; t < 2; t++) {
        searches.add(pool.submit(() -> {
          while (!done.get()) {
            int i = changing.get();
            searched.incrementAndGet();
            if (!h.found(manager, vectors.get(i), String.format("h%03d", i))) {
              missed.incrementAndGet();
            }
          }
          return null;
        }));
      }
      for (int i = 0; i < keys; i++) {
        changing.set(i);
        Thread.sleep(1);
        float[] moved = vectors.get(i).clone();
        moved[0] += 0.01f;
        h.upsert(manager, String.format("h%03d", i), moved);
      }
      done.set(true);
      for (Future<?> search : searches) {
        search.get(60, TimeUnit.SECONDS);
      }
    } finally {
      done.set(true);
      pool.shutdownNow();
    }
    // The search is approximate. A built graph can leave a node unreachable, and JVector can miss a
    // node while a concurrent insert changes the links of its neighbors. Thus a few searches can
    // miss the row. A change that masks the old version before it adds the new version causes many
    // misses
    assertTrue(missed + " of " + searched + " searches missed the changed row",
      missed.get() * 100 <= searched.get());
  }

  /**
   * The index of the manager was dropped, or created again with another definition. A full rebuild
   * then closes the manager and writes no segment.
   */
  @Test
  public void testStaleManagerClosesWithoutWriting() throws Exception {
    Harness h = new Harness("stale");
    h.load(30, new Random(10));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(manager, since);
    h.catalog = scan -> false;
    manager.rebuild(Long.MAX_VALUE);
    await("the stale manager to close", manager::isClosed);
    List<HnswSegment.Descriptor> segments = h.segments();
    assertEquals(1, segments.size());
    assertEquals(base.time, segments.get(0).time);
  }

  /**
   * A batch takes its timestamp before its commit, and changes the graph after the commit. Thus a
   * batch older than a swap changes the new mutable graph, and the segment of the swap replays from
   * the timestamp of the batch.
   */
  @Test
  public void testSegmentReplaysBatchesInFlight() throws Exception {
    Harness h = new Harness("inflight");
    h.load(30, new Random(12));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);
    long batch = EnvironmentEdgeManager.currentTimeMillis() - 300_000;
    h.oldestBatch = batch;
    h.upsert(manager, "r000", point(50, 0));
    request("a delta flush", manager::flush, () -> {
      try {
        return h.segments().get(0).isDelta();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    List<HnswSegment.Descriptor> segments = h.segments();
    assertEquals(batch, segments.get(0).replayFrom);
    assertEquals(batch, HnswIndexManager.replayStartTime(segments));
  }

  /**
   * A region trusts an exact base that it wrote itself. It keeps the base and replays. A region
   * keeps its encoded name when it moves, when its server restarts and after a snapshot restore. A
   * TRUNCATE removes the segments with the rows, so no region finds the base of a truncated region.
   */
  @Test
  public void testBaseOfRegionIsTrusted() throws Exception {
    Harness restored = new Harness("restored");
    restored.load(30, new Random(13));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager before = restored.open(true);
    HnswSegment.Descriptor base = restored.awaitBase(before, since);
    before.close();
    // The same region opens again, with the same encoded name
    Harness h = new Harness("restored");
    byte[] name = h.region.getRegionInfo().getEncodedNameAsBytes();
    assertArrayEquals(name, restored.region.getRegionInfo().getEncodedNameAsBytes());
    HnswIndexManager manager = h.open(true);
    Thread.sleep(500);
    assertTrue(manager.covered());
    // The open sends only the replay scan, and no rebuild scan
    assertEquals(1, h.scans.size());
    List<HnswSegment.Descriptor> segments = h.segments();
    assertEquals(1, segments.size());
    assertEquals(base.time, segments.get(0).time);
    assertArrayEquals(name, segments.get(0).writer);
  }

  /**
   * A region trusts only the segments that it wrote itself. Here a new region of the same range
   * finds an exact base that another region wrote. This occurs after a split and a merge back to
   * the same range, or in a cloned table. The region searches that base at once, and also rebuilds
   * at once. Its new base records its own name, and its cutover retires the base of the other
   * region.
   */
  @Test
  public void testBaseOfAnotherRegionIsRebuilt() throws Exception {
    Harness first = new Harness("foreign");
    first.load(30, new Random(15));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager before = first.open(true);
    HnswSegment.Descriptor base = first.awaitBase(before, since);
    before.close();
    // A new region of the same range, with another encoded name. Its rebuild waits in the catalog
    // check until the region has its rows. The region gets its rows after the open. Thus the replay
    // scan reads none of them, and the searches read only the base
    Harness h = new Harness("foreign", base.time + 1);
    byte[] name = h.region.getRegionInfo().getEncodedNameAsBytes();
    assertFalse(Bytes.equals(name, base.writer));
    CountDownLatch release = new CountDownLatch(1);
    h.catalog = scan -> {
      try {
        release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      return true;
    };
    long reopened = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager;
    try {
      manager = h.open(true);
      h.rows.putAll(first.rows);
      assertTrue(manager.covered());
      assertEquals(base.count, manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
      List<byte[]> found =
        manager.search(point(0, 0), 50, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null);
      assertFalse(found.isEmpty());
      for (byte[] key : found) {
        assertTrue(Bytes.toStringBinary(key), first.rows.containsKey(key));
      }
    } finally {
      release.countDown();
    }
    HnswSegment.Descriptor rebuilt = h.awaitBase(manager, reopened);
    assertEquals(base.count, rebuilt.count);
    assertArrayEquals(name, rebuilt.writer);
    assertTrue(manager.covered());
  }

  /**
   * The writer fence applies to each segment of an exact stack, not only to its base. The region
   * wrote the base of its stack, and another region wrote the delta. The region searches the stack
   * at once, and also rebuilds at once. Its new base records its own name, and holds the change of
   * the delta.
   */
  @Test
  public void testDeltaOfAnotherRegionIsRebuilt() throws Exception {
    Harness h = new Harness("foreigndelta");
    h.load(30, new Random(31));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager first = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(first, since);
    float[] moved = point(50, 0);
    h.upsert(first, "r000", moved);
    request("a delta flush", first::flush, () -> {
      try {
        return h.segments().get(0).isDelta();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    HnswSegment.Descriptor delta = h.segments().get(0);
    first.close();
    byte[] name = h.region.getRegionInfo().getEncodedNameAsBytes();
    assertArrayEquals(name, base.writer);
    assertArrayEquals(name, delta.writer);
    CELLS.put(delta.anchor, HnswSegment.qualifier(delta.identity, HnswSegment.WRITER_TAG),
      delta.time, Bytes.toBytes("another region"));

    // The rebuild waits, so the test sees the region search the stack
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (h.isRow(put)) {
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    long reopened = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager second;
    try {
      second = h.open(true);
      assertTrue(second.covered());
      assertEquals(2, h.segments().size());
      assertEquals("r000", h.nearest(second, moved));
    } finally {
      release.countDown();
    }
    HnswSegment.Descriptor rebuilt = h.awaitBase(second, reopened);
    assertEquals(30, rebuilt.count);
    assertArrayEquals(name, rebuilt.writer);
    assertTrue(second.covered());
    assertEquals("r000", h.nearest(second, moved));
  }

  /**
   * A major compaction purges the delete markers of rows that only the memory of a closed manager
   * deleted. Thus a manager rebuilds when it opens after a major compaction of an indexed family
   * that is newer than the replay start of its segments. Each store compacts on its own schedule,
   * so an older compaction of another family does not hide the newer compaction.
   */
  @Test
  public void testMajorCompactionAfterSegmentsRebuilds() throws Exception {
    Harness h = new Harness("compacted");
    h.load(30, new Random(14));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager first = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(first, since);
    float[] deleted = h.rows.get(h.key("r000"));
    h.delete(first, "r000");
    first.close();
    h.compacted(FAMILY, base.replayFrom + 1);
    h.compacted(OTHER, base.replayFrom - 1);
    long reopened = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager second = h.open(true);
    h.awaitBase(second, reopened);
    assertFalse(h.found(second, deleted, "r000"));
  }

  /**
   * A major compaction of an indexed family makes an idle region write a segment. The segment moves
   * the replay start of the region past the compaction, so the region keeps its segments when it
   * opens again. A later major compaction of a family that the index does not read does not make
   * the region rebuild.
   */
  @Test
  public void testCompactionOfIdleRegionKeepsSegments() throws Exception {
    Harness h = new Harness("idle");
    h.load(30, new Random(21));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager first = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(first, since);
    assertTrue(first.indexes(FAMILY));
    assertFalse(first.indexes(OTHER));
    Thread.sleep(2);
    long compacted = EnvironmentEdgeManager.currentTimeMillis();
    assertTrue(compacted > base.replayFrom);
    h.compacted(FAMILY, compacted);
    first.flush();
    await("a delta segment written after the compaction", () -> {
      try {
        List<HnswSegment.Descriptor> segments = h.segments();
        return segments.size() == 2 && segments.get(0).isDelta()
          && segments.get(0).replayFrom >= compacted;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    first.close();
    h.compacted(OTHER, EnvironmentEdgeManager.currentTimeMillis() + 60_000);
    h.scans.clear();
    HnswIndexManager second = h.open(true);
    Thread.sleep(500);
    // The open sends only the replay scan, and no rebuild scan
    assertEquals(1, h.scans.size());
    assertEquals(base.time, h.segments().get(1).time);
    assertEquals("r005", h.nearest(second, h.rows.get(h.key("r005"))));
  }

  /**
   * A major compaction can end while a flush that swapped before the compaction runs. The manager
   * then flushes again, so the replay start of the region moves past the compaction.
   */
  @Test
  public void testCompactionDuringFlushFlushesAgain() throws Exception {
    Harness h = new Harness("during");
    h.load(30, new Random(22));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (h.isRow(put) && isDelta(put) && entered.getCount() > 0) {
        entered.countDown();
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    h.upsert(manager, "r000", point(50, 0));
    manager.flush();
    assertTrue(entered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
    Thread.sleep(2);
    long compacted = EnvironmentEdgeManager.currentTimeMillis();
    manager.flush();
    release.countDown();
    await("a second delta segment written after the compaction", () -> {
      try {
        List<HnswSegment.Descriptor> segments = h.segments();
        return segments.size() == 3 && segments.get(0).isDelta()
          && segments.get(0).replayFrom >= compacted;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * The index of the manager is stale: a user dropped it, or created it again with another
   * definition or on another table. A delta flush then closes the manager and writes no segment,
   * because the index name can now belong to another index or definition.
   */
  @Test
  public void testStaleManagerDeltaFlushClosesWithoutWriting() throws Exception {
    Harness h = new Harness("staledelta");
    h.load(30, new Random(23));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(manager, since);
    h.upsert(manager, "r000", point(50, 0));
    List<Scan> checked = new CopyOnWriteArrayList<>();
    h.catalog = scan -> {
      checked.add(scan);
      return false;
    };
    manager.flush();
    await("the stale manager to close", manager::isClosed);
    // A delta flush gives no rebuild scan to the catalog check
    assertNull(checked.get(0));
    List<HnswSegment.Descriptor> segments = h.segments();
    assertEquals(1, segments.size());
    assertEquals(base.time, segments.get(0).time);
  }

  /**
   * A search that finds the payload cell of its base missing fails with a plain retryable
   * exception, so a client can retry the scan. The region leaves the base out and rebuilds. Until
   * the rebuild lands, the region is not covered, so the retry of the scan reads the range without
   * candidate restriction.
   */
  @Test
  public void testLostBaseIsRebuilt() throws Exception {
    Harness h = new Harness("lost");
    h.load(30, new Random(24));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(manager, since);
    // The rebuild waits, so the test sees the uncovered region
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (h.isRow(put)) {
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    long lost = EnvironmentEdgeManager.currentTimeMillis();
    try {
      CELLS.remove(base, HnswSegment.PAYLOAD_TAG);
      HnswOffheapAllocator.get(h.env.getConfiguration()).evictAll();
      try {
        h.nearest(manager, h.rows.get(h.key("r005")));
        fail("a search of a segment without its payload must fail");
      } catch (IOException e) {
        assertFalse(e.toString(), e instanceof DoNotRetryIOException);
        assertTrue(e.toString(), e.getCause() instanceof HnswSegment.NotFoundException);
      }
      assertFalse(manager.covered());
      assertEquals(0, manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    } finally {
      release.countDown();
    }
    assertEquals(30, h.awaitBase(manager, lost).count);
    assertTrue(manager.covered());
    assertEquals("r005", h.nearest(manager, h.rows.get(h.key("r005"))));
  }

  /**
   * HBase returns an empty value for a payload whose MOB file is gone, for example after the MOB
   * cleaner archived a payload that a split daughter inherited. A search that loads such a payload
   * again fails with a retryable exception, and never decodes the empty value as a graph. The
   * region leaves the segment out and rebuilds.
   */
  @Test
  public void testLostPayloadIsRebuilt() throws Exception {
    Harness h = new Harness("lostmob");
    h.load(30, new Random(27));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(manager, since);
    CELLS.put(base.anchor, HnswSegment.qualifier(base.identity, HnswSegment.PAYLOAD_TAG), base.time,
      new byte[0]);
    HnswOffheapAllocator.get(h.env.getConfiguration()).evictAll();
    long lost = EnvironmentEdgeManager.currentTimeMillis();
    try {
      h.nearest(manager, h.rows.get(h.key("r005")));
      fail("a search of a segment without its payload must fail");
    } catch (IOException e) {
      assertFalse(e.toString(), e instanceof DoNotRetryIOException);
      assertTrue(e.toString(), e.getCause() instanceof DoNotRetryIOException);
    }
    assertEquals(30, h.awaitBase(manager, lost).count);
    assertEquals("r005", h.nearest(manager, h.rows.get(h.key("r005"))));
  }

  /**
   * A segment whose payload cell is missing when its region opens is not readable. A new listing
   * still returns the segment, so the region does not list again without end. It opens without the
   * segment, and rebuilds.
   */
  @Test
  public void testMissingPayloadAtOpenIsRebuilt() throws Exception {
    Harness h = new Harness("missing");
    h.load(30, new Random(28));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager first = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(first, since);
    first.close();
    CELLS.remove(base, HnswSegment.PAYLOAD_TAG);
    long reopened = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager second = h.open(true);
    assertEquals(30, h.awaitBase(second, reopened).count);
    assertEquals("r005", h.nearest(second, h.rows.get(h.key("r005"))));
  }

  /**
   * HBase returns an empty value for a payload or a tombstones cell whose MOB file is gone. The
   * region of a delta with such a cell cannot read the delta at open. The region opens without the
   * delta, and rebuilds. The replay starts after the changes of the lost delta, so the base of its
   * stack does not hold them. Thus the region is not covered until the rebuild lands, and an HNSW
   * scan reads the range without candidate restriction.
   */
  @Test
  public void testLostDeltaIsRebuilt() throws Exception {
    for (byte tag : new byte[] { HnswSegment.TOMBSTONES_TAG, HnswSegment.PAYLOAD_TAG }) {
      Harness h = new Harness("lostdelta" + (char) tag);
      h.load(30, new Random(29));
      long since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager first = h.open(true);
      h.awaitBase(first, since);
      float[] moved = point(50, 0);
      float[] deleted = h.rows.get(h.key("r001"));
      h.upsert(first, "r000", moved);
      h.delete(first, "r001");
      request("a delta flush", first::flush, () -> {
        try {
          return h.segments().get(0).isDelta();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      HnswSegment.Descriptor delta = h.segments().get(0);
      first.close();
      CELLS.put(delta.anchor, HnswSegment.qualifier(delta.identity, tag), delta.time, new byte[0]);

      // The rebuild waits, so the test sees the uncovered region
      CountDownLatch release = new CountDownLatch(1);
      putHook = put -> {
        if (h.isRow(put)) {
          try {
            release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      };
      long reopened = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager second;
      try {
        second = h.open(true);
        assertFalse("lost " + (char) tag, second.covered());
      } finally {
        release.countDown();
      }
      assertEquals(29, h.awaitBase(second, reopened).count);
      assertTrue(second.covered());
      assertEquals("r000", h.nearest(second, moved));
      assertFalse(h.found(second, deleted, "r001"));
    }
  }

  /**
   * A region of a table with a TTL rebuilds on a flush, so that expired rows leave its segments.
   */
  @Test
  public void testFlushOfTtlTableRebuilds() throws Exception {
    Harness h = new Harness("ttlflush");
    h.load(30, new Random(15));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true, true);
    h.awaitBase(manager, since);
    // The row expired, and a major compaction purged it
    h.rows.remove(h.key("r004"));
    long compacted = EnvironmentEdgeManager.currentTimeMillis();
    request("a rebuild", manager::flush, () -> h.hasBase(manager, compacted));
    assertEquals(29, h.segments().get(0).count);
  }

  /** A full rebuild masks expired rows as a client scan does, so these rows leave the segments. */
  @Test
  public void testFullRebuildMasksExpiredRows() throws Exception {
    Harness h = new Harness("expiry");
    h.load(30, new Random(16));
    h.times.put(h.key("r003"), EnvironmentEdgeManager.currentTimeMillis() - 3_600_000);
    byte[] ttl = new LiteralTTLExpression(60).serialize();
    h.catalog = scan -> {
      if (scan == null) {
        return true;
      }
      scan.setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME, FAMILY);
      scan.setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME,
        QueryConstants.EMPTY_COLUMN_BYTES);
      scan.setAttribute(BaseScannerRegionObserverConstants.TTL, ttl);
      return true;
    };
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    assertEquals(29, h.awaitBase(manager, since).count);
  }

  /** A manager that closes while a rebuild scans its region writes no segment. */
  @Test
  public void testCloseStopsRebuild() throws Exception {
    Harness h = new Harness("closing");
    h.load(30, new Random(17));
    h.scanEntered = new CountDownLatch(1);
    h.scanGate = new CountDownLatch(1);
    HnswIndexManager manager = h.open(true);
    assertTrue(h.scanEntered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
    manager.close();
    h.scanGate.countDown();
    Thread.sleep(500);
    assertTrue(h.segments().isEmpty());
  }

  /**
   * A segment with a corrupt payload does not stop the writes of its region. The manager opens
   * without that segment, and then rebuilds.
   */
  @Test
  public void testCorruptSegmentIsRebuilt() throws Exception {
    Harness h = new Harness("corrupt");
    h.load(30, new Random(18));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager first = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(first, since);
    first.close();
    byte[] qualifier = HnswSegment.qualifier(base.identity, HnswSegment.PAYLOAD_TAG);
    byte[] payload =
      CellUtil
        .cloneValue(
          CELLS
            .get(new Get(base.anchor)
              .addColumn(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES, qualifier))
            .rawCells()[0]);
    CELLS.put(base.anchor, qualifier, base.time, Arrays.copyOf(payload, payload.length - 4));
    long reopened = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager second = h.open(true);
    h.awaitBase(second, reopened);
    assertEquals("r005", h.nearest(second, h.rows.get(h.key("r005"))));
  }

  /** A search of a closed manager fails, and does not search again. */
  @Test
  public void testSearchOfClosedManagerFails() throws Exception {
    Harness h = new Harness("closed");
    h.load(30, new Random(19));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);
    manager.close();
    try {
      h.nearest(manager, h.rows.get(h.key("r001")));
      fail("a search of a closed manager must fail");
    } catch (IOException e) {
      // Expected
    }
  }

  /**
   * A region lists its segments with scans of only its own {@code V#} family, and these scans read
   * no MOB file. The region scans of data rows do not fill the block cache.
   */
  @Test
  public void testOpenReadsOwnSegmentsWithoutCaching() throws Exception {
    Harness h = new Harness("own");
    h.load(30, new Random(20));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager first = h.open(true);
    h.awaitBase(first, since);
    first.close();
    SEGMENT_SCANS.clear();
    h.open(true);
    assertFalse(SEGMENT_SCANS.isEmpty());
    for (Scan scan : SEGMENT_SCANS) {
      assertEquals(1, scan.getFamilyMap().size());
      assertTrue(Bytes.toBoolean(scan.getAttribute(MobConstants.MOB_SCAN_RAW)));
    }
    assertEquals(2, h.scans.size());
    for (Scan scan : h.scans) {
      assertFalse(scan.getCacheBlocks());
    }
    // The replay scan is raw, and reads only the family of the vector
    assertTrue(h.scans.get(1).isRaw());
    assertEquals(1, h.scans.get(1).getFamilyMap().size());
    assertTrue(Bytes.equals(FAMILY, h.scans.get(1).getFamilyMap().keySet().iterator().next()));
  }

  // Adds deleted keys to the mutable graph of the region. Each deleted key is a buffered entry that
  // builds no graph node, so the keys fill the graph quickly
  private static void deleteKeys(HnswIndexManager manager, Harness h, String prefix, int from,
    int to) {
    for (int i = from; i < to; i++) {
      manager.onMutation(new Put(h.key(prefix + i)).addColumn(FAMILY, V, encode(point(1, 1))), null,
        null);
    }
  }

  // Returns the requests to split the data region of the harness
  private static List<SplitRequest> regionSplits(Harness h) {
    List<SplitRequest> found = new ArrayList<>();
    for (SplitRequest request : SPLITS) {
      if (Bytes.equals(request.regionName, h.region.getRegionInfo().getRegionName())) {
        found.add(request);
      }
    }
    return found;
  }

  /**
   * When the writes are faster than the flushes, the manager asks the master to split the region.
   * It sends one request, from the split thread, when the mutable graph reaches the trigger. A swap
   * that drains the graph arms the trigger again.
   */
  @Test
  public void testWriteBacklogRequestsSplit() throws Exception {
    Harness h = new Harness("backlog");
    h.conf.setInt(QueryServices.HNSW_SPLIT_MULTIPLIER_ATTRIB, 2);
    int threshold = HnswIndexManager.FLUSH_THRESHOLD;
    int trigger = 2 * threshold;
    HnswIndexManager manager = h.open(false);
    AtomicReference<CountDownLatch> gate = new AtomicReference<>(new CountDownLatch(1));
    AtomicInteger puts = new AtomicInteger();
    putHook = put -> {
      if (h.isRow(put)) {
        CountDownLatch held = gate.get();
        puts.incrementAndGet();
        try {
          held.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    try {
      // A full mutable graph starts a rebuild. The hook holds its segment write
      deleteKeys(manager, h, "a", 0, threshold);
      await("a held rebuild", () -> puts.get() == 1);
      deleteKeys(manager, h, "b", 0, trigger - 1);
      Thread.sleep(200);
      assertTrue(regionSplits(h).isEmpty());
      deleteKeys(manager, h, "c", 0, 1);
      await("a split request", () -> regionSplits(h).size() == 1);
      deleteKeys(manager, h, "d", 0, threshold / 10);
      Thread.sleep(200);
      assertEquals(1, regionSplits(h).size());
      assertNotEquals(Thread.currentThread(), regionSplits(h).get(0).thread);

      // The next rebuild swaps the graph when it starts, and the swap arms the trigger again
      CountDownLatch held = gate.get();
      gate.set(new CountDownLatch(1));
      held.countDown();
      await("the next held rebuild", () -> puts.get() == 2);
      deleteKeys(manager, h, "e", 0, trigger);
      await("a second split request", () -> regionSplits(h).size() == 2);
    } finally {
      gate.get().countDown();
    }
  }

  /**
   * A full rebuild swaps the mutable graph when its task starts. While the rebuild waits behind the
   * rebuild of another region, a region with a base flushes deltas. It flushes a delta when the
   * graph reaches QUEUED_FLUSH_MULTIPLIER times the flush threshold, so its writes do not fill the
   * graph.
   */
  @Test
  public void testDeltasFlushWhileRebuildQueued() throws Exception {
    Harness blocker = new Harness("blocker");
    blocker.load(30, new Random(27));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager blocking = blocker.open(true);
    blocker.awaitBase(blocking, since);
    Harness h = new Harness("waiting");
    h.load(30, new Random(28));
    since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(manager, since);

    // The rebuild of the other region holds the rebuild thread in its region scan
    blocker.scanEntered = new CountDownLatch(1);
    blocker.scanGate = new CountDownLatch(1);
    long requested;
    try {
      blocking.rebuild(Long.MAX_VALUE);
      assertTrue(blocker.scanEntered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      requested = manager.rebuild(Long.MAX_VALUE);
      deleteKeys(manager, h, "w", 0,
        HnswIndexManager.QUEUED_FLUSH_MULTIPLIER * HnswIndexManager.FLUSH_THRESHOLD);
      await("a delta flush while the rebuild waits", () -> {
        try {
          return h.segments().stream()
            .anyMatch(d -> d.isDelta() && d.baseTime == base.time && d.time > requested);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      manager.checkResources();
      for (HnswSegment.Descriptor d : h.segments()) {
        assertTrue(d.isDelta() || d.time == base.time);
      }
    } finally {
      blocker.scanGate.countDown();
    }
    h.awaitBase(manager, requested + 1);
  }

  /**
   * A build request that arrives while a full rebuild runs gets a time no earlier than the swap of
   * the running rebuild. Another rebuild follows, and its base is newer than the request. A
   * repeated request that the running rebuild serves joins the running rebuild.
   */
  @Test
  public void testRebuildRunningAtRequestIsFollowed() throws Exception {
    Harness h = new Harness("running");
    h.load(30, new Random(29));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);
    List<Long> written = new CopyOnWriteArrayList<>();
    putHook = put -> {
      if (h.isRow(put) && !isDelta(put)) {
        written.add(time(put));
      }
    };
    h.scanEntered = new CountDownLatch(1);
    h.scanGate = new CountDownLatch(1);
    long first;
    long second;
    try {
      first = manager.rebuild(Long.MAX_VALUE);
      assertTrue(h.scanEntered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertEquals(first, manager.rebuild(first));
      second = manager.rebuild(Long.MAX_VALUE);
    } finally {
      h.scanGate.countDown();
    }
    await("a base newer than the second request", () -> h.hasBase(manager, second + 1));
    assertEquals(written.toString(), 2, written.size());
    assertTrue(written.get(0) > first);
    assertTrue(written.get(0) <= second);
  }

  /** A build scan returns one cell, with the time at which the region accepted the request. */
  @Test
  public void testAcceptedScanner() throws Exception {
    RegionScanner scanner = mock(RegionScanner.class);
    when(scanner.getRegionInfo()).thenReturn(
      RegionInfoBuilder.newBuilder(TableName.valueOf("T")).setStartKey(Bytes.toBytes("k")).build());
    RegionScanner accepted = HnswIndexManager.acceptedScanner(scanner, 1234L);
    List<Cell> cells = new ArrayList<>();
    assertFalse(accepted.nextRaw(cells));
    assertEquals(1, cells.size());
    assertEquals(1234L, Bytes.toLong(CellUtil.cloneValue(cells.get(0))));
    assertTrue(Bytes.equals(Bytes.toBytes("k"), CellUtil.cloneRow(cells.get(0))));
    cells.clear();
    assertFalse(accepted.next(cells));
    assertTrue(cells.isEmpty());
  }

  /** A split multiplier of 0 or less turns the split requests off. */
  @Test
  public void testNonPositiveSplitMultiplierRequestsNoSplit() throws Exception {
    Harness h = new Harness("nosplit");
    h.conf.setInt(QueryServices.HNSW_SPLIT_MULTIPLIER_ATTRIB, 0);
    HnswIndexManager manager = h.open(false);
    deleteKeys(manager, h, "a", 0, 10);
    Thread.sleep(200);
    assertTrue(regionSplits(h).isEmpty());
  }

  /**
   * A change that fails to apply queues a full rebuild that must swap before the next delta flush.
   * A major compaction of a table with a TTL does the same. A stack that a region does not trust
   * when it opens also does the same. A delta that the region writes while the rebuild waits would
   * move the replay start of the region past the change or the compaction. A region that opens
   * would then trust its stack. Thus writes buffer until the rebuild starts, and the base of the
   * rebuild holds the change.
   */
  @Test
  public void testRepairRebuildPrecedesDeltas() throws Exception {
    Harness blocker = new Harness("rblocker");
    blocker.load(30, new Random(32));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager blocking = blocker.open(true);
    blocker.awaitBase(blocking, since);
    Harness failed = new Harness("rfailed");
    Harness ttl = new Harness("rttl");
    Harness reopened = new Harness("ropened");
    List<Harness> regions = Arrays.asList(failed, ttl, reopened);
    Map<Harness, HnswIndexManager> opened = new HashMap<>();
    for (Harness h : regions) {
      h.load(30, new Random(33));
      since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager manager = h.open(true, h == ttl);
      HnswSegment.Descriptor base = h.awaitBase(manager, since);
      opened.put(h, manager);
      if (h == reopened) {
        manager.close();
        h.compacted(FAMILY, base.replayFrom + 1);
      }
    }

    // The rebuild of the other region holds the rebuild thread in its region scan
    blocker.scanEntered = new CountDownLatch(1);
    blocker.scanGate = new CountDownLatch(1);
    float[] moved = point(50, 0);
    long requested;
    try {
      blocking.rebuild(Long.MAX_VALUE);
      assertTrue(blocker.scanEntered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      requested = EnvironmentEdgeManager.currentTimeMillis();
      failed.failing = row -> Bytes.equals(row, failed.key("r000"));
      failed.upsert(opened.get(failed), "r000", moved);
      failed.failing = row -> false;
      opened.get(ttl).flush();
      opened.put(reopened, reopened.open(true));
      for (Harness h : regions) {
        deleteKeys(opened.get(h), h, "w", 0,
          HnswIndexManager.QUEUED_FLUSH_MULTIPLIER * HnswIndexManager.FLUSH_THRESHOLD + 1);
      }
      Thread.sleep(500);
      for (Harness h : regions) {
        List<HnswSegment.Descriptor> segments = h.segments();
        assertEquals(Bytes.toString(h.start), 1, segments.size());
        assertTrue(HnswIndexManager.replayStartTime(segments) < requested);
        opened.get(h).checkResources();
      }
    } finally {
      blocker.scanGate.countDown();
    }
    for (Harness h : regions) {
      h.awaitBase(opened.get(h), requested);
    }
    assertEquals("r000", failed.nearest(opened.get(failed), moved));
  }

  /**
   * The rebuild thread runs the rebuild of a region without a covering base, for example the upper
   * daughter of a split, before the routine rebuilds that wait for the thread. The order of the
   * requests does not change this.
   */
  @Test
  public void testUncoveredRegionBuildsFirst() throws Exception {
    Harness blocker = new Harness("ublocker");
    blocker.load(30, new Random(36));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager blocking = blocker.open(true);
    blocker.awaitBase(blocking, since);
    Harness covered = new Harness("ucovered");
    covered.load(30, new Random(37));
    since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager routine = covered.open(true);
    covered.awaitBase(routine, since);
    Harness uncovered = new Harness("uuncovered");
    uncovered.load(30, new Random(38));
    List<String> order = new CopyOnWriteArrayList<>();
    putHook = put -> {
      for (Harness h : Arrays.asList(covered, uncovered)) {
        if (h.isRow(put)) {
          order.add(Bytes.toString(h.start));
        }
      }
    };
    // The rebuild of another region holds the rebuild thread in its region scan
    blocker.scanEntered = new CountDownLatch(1);
    blocker.scanGate = new CountDownLatch(1);
    HnswIndexManager opened;
    try {
      blocking.rebuild(Long.MAX_VALUE);
      assertTrue(blocker.scanEntered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      routine.rebuild(Long.MAX_VALUE);
      opened = uncovered.open(true);
      assertFalse(opened.covered());
    } finally {
      blocker.scanGate.countDown();
    }
    await("the rebuilds of both regions", () -> order.size() == 2);
    assertEquals(Arrays.asList(Bytes.toString(uncovered.start), Bytes.toString(covered.start)),
      order);
    uncovered.awaitBase(opened, since);
    assertTrue(opened.covered());
  }

  /**
   * A region whose routine rebuild waits for the rebuild thread can lose the coverage of its bases,
   * for example when a search finds the payload of its base lost. Its rebuild then moves ahead of
   * the routine rebuilds of covered regions that wait before it.
   */
  @Test
  public void testRegionThatLosesCoverageBuildsFirst() throws Exception {
    Harness blocker = new Harness("lblocker");
    blocker.load(30, new Random(42));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager blocking = blocker.open(true);
    blocker.awaitBase(blocking, since);
    Harness covered = new Harness("lcovered");
    covered.load(30, new Random(43));
    since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager routine = covered.open(true);
    covered.awaitBase(routine, since);
    Harness losing = new Harness("llosing");
    losing.load(30, new Random(44));
    since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager lost = losing.open(true);
    HnswSegment.Descriptor base = losing.awaitBase(lost, since);
    List<String> order = new CopyOnWriteArrayList<>();
    putHook = put -> {
      for (Harness h : Arrays.asList(covered, losing)) {
        if (h.isRow(put)) {
          order.add(Bytes.toString(h.start));
        }
      }
    };
    // The rebuild of another region holds the rebuild thread in its region scan
    blocker.scanEntered = new CountDownLatch(1);
    blocker.scanGate = new CountDownLatch(1);
    try {
      blocking.rebuild(Long.MAX_VALUE);
      assertTrue(blocker.scanEntered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      routine.rebuild(Long.MAX_VALUE);
      lost.rebuild(Long.MAX_VALUE);
      CELLS.put(base.anchor, HnswSegment.qualifier(base.identity, HnswSegment.PAYLOAD_TAG),
        base.time, new byte[0]);
      HnswOffheapAllocator.get(losing.env.getConfiguration()).evictAll();
      try {
        losing.nearest(lost, losing.rows.get(losing.key("r005")));
        fail("a search of a segment without its payload must fail");
      } catch (IOException e) {
        assertTrue(e.toString(), e.getCause() instanceof DoNotRetryIOException);
      }
      assertFalse(lost.covered());
    } finally {
      blocker.scanGate.countDown();
    }
    await("the rebuilds of both regions", () -> order.size() == 2);
    assertEquals(Arrays.asList(Bytes.toString(losing.start), Bytes.toString(covered.start)), order);
  }

  /**
   * The coverage of a search comes from the state that it searches. A merged region searches the
   * bases of its two predecessors. A search finds the payload of the newer base lost, and leaves
   * the base out, so the region is not covered. Another candidate search starts on the covered
   * state, and reads the closed base later. It then runs again on the new state. That state is not
   * covered, so the search gives no candidates. Its scan then reads the range of the region without
   * candidate restriction. Candidates from the new state would leave out all the rows of the lost
   * base. The searches ask for fewer candidates than the region holds, so they search the graphs
   * and read the payloads.
   */
  @Test
  public void testSearchOfStateWithoutCoverageGivesNoCandidates() throws Exception {
    byte[] start = Bytes.toBytes("cover/");
    byte[] end = Bytes.toBytes("cover0");
    byte[] mid = Bytes.toBytes("cover/m");
    Harness lower = new Harness(start, mid, 1);
    Harness upper = new Harness(mid, end, 1);
    upper.index = lower.index;
    Random random = new Random(48);
    for (int i = 0; i < 20; i++) {
      lower.rows.put(lower.key(String.format("a%02d", i)), vector(random));
      upper.rows.put(upper.key(String.format("z%02d", i)), vector(random));
    }
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager older = lower.open(true);
    lower.awaitBase(older, since);
    older.close();
    HnswIndexManager newer = upper.open(true);
    HnswSegment.Descriptor lost = upper.awaitBase(newer, since);
    newer.close();

    Harness merged = new Harness(start, end, 2);
    merged.index = lower.index;
    // The search that the test holds waits at the first key that its key filter tests. The search
    // tests that key when it reads the older base, before it reads the newer base
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    Predicate<byte[]> hold = key -> {
      if (entered.getCount() > 0) {
        entered.countDown();
        try {
          resume.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return true;
    };
    // The rebuild of the merged region waits, so the region keeps the bases of its predecessors
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (merged.isRow(put)) {
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      HnswIndexManager manager = merged.open(true);
      merged.rows.putAll(lower.rows);
      merged.rows.putAll(upper.rows);
      assertTrue(manager.covered());
      assertEquals(merged.rows.size(),
        manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
      CELLS.put(lost.anchor, HnswSegment.qualifier(lost.identity, HnswSegment.PAYLOAD_TAG),
        lost.time, new byte[0]);
      HnswOffheapAllocator.get(merged.env.getConfiguration()).evictAll();
      float[] query = upper.rows.get(upper.key("z00"));
      Future<HnswIndexManager.Candidates> candidates = executor.submit(() -> manager
        .candidates(query, 10, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, hold));
      assertTrue(entered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      try {
        manager.search(query, 10, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, null);
        fail("a search of a segment without its payload must fail");
      } catch (IOException e) {
        assertTrue(e.toString(), e.getCause() instanceof DoNotRetryIOException);
      }
      assertFalse(manager.covered());
      resume.countDown();
      assertNull(candidates.get(TIMEOUT_MS, TimeUnit.MILLISECONDS));
    } finally {
      resume.countDown();
      release.countDown();
      executor.shutdownNow();
    }
  }

  /**
   * A purge of a dropped index closes the manager of the index and deletes its cells. A rebuild or
   * a delta flush that writes its segment after the purge scan deletes the cells that it wrote.
   * Thus no cells of the dropped index stay.
   */
  @Test
  public void testPurgeDuringSegmentWriteLeavesNoCells() throws Exception {
    for (boolean full : new boolean[] { true, false }) {
      Harness h = new Harness(full ? "purgewrite" : "purgedelta");
      h.load(30, new Random(45));
      long since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager manager = h.open(true);
      h.awaitBase(manager, since);
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      putHook = put -> {
        if (h.isRow(put)) {
          entered.countDown();
          try {
            release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      };
      try {
        if (full) {
          manager.rebuild(Long.MAX_VALUE);
        } else {
          h.upsert(manager, "r000", point(50, 0));
          manager.flush();
        }
        assertTrue(entered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        // The purge closes the manager, and deletes the cells before the flush writes its segment
        manager.close();
        assertEquals(6, HnswIndexManager.purge(h.env, h.index));
        assertTrue(CELLS.list(Bytes.toBytes(h.index)).isEmpty());
      } finally {
        release.countDown();
      }
      putHook = put -> {
      };
      // The rebuild thread and the delta thread run the tasks of other regions after the tasks of
      // the purged index. Thus the flush of the purged index has ended when the other segment lands
      Harness next = new Harness(full ? "purgenext" : "purgenextdelta");
      next.load(10, new Random(46));
      since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager other = next.open(true);
      next.awaitBase(other, since);
      if (!full) {
        next.upsert(other, "r000", point(50, 0));
        request("a delta flush", other::flush, () -> {
          try {
            return next.segments().get(0).isDelta();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      }
      assertTrue(CELLS.list(Bytes.toBytes(h.index)).isEmpty());
    }
  }

  /**
   * A region ignores the segments of another definition of its index, for example of an index that
   * was dropped and created again with the same name. This is also true when these segments cover
   * its range and can be read. The region is not covered, searches no segment, and rebuilds. Its
   * first base retires them.
   */
  @Test
  public void testSegmentOfAnotherDefinitionIsIgnored() throws Exception {
    Harness h = new Harness("fence");
    h.load(30, new Random(47));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager first = h.open(true);
    HnswSegment.Descriptor base = h.awaitBase(first, since);
    first.close();
    CELLS.put(base.anchor, HnswSegment.qualifier(base.identity, HnswSegment.DEFINITION_TAG),
      base.time, HnswSegment.definitionDigest("another definition"));
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (h.isRow(put)) {
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    long reopened = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager second;
    try {
      second = h.open(true);
      assertFalse(second.covered());
      assertEquals(0, second.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    } finally {
      release.countDown();
    }
    HnswSegment.Descriptor rebuilt = h.awaitBase(second, reopened);
    assertEquals(30, rebuilt.count);
    assertTrue(second.covered());
  }

  /**
   * A build request can repeat an earlier request after the rebuild that serves it ends. The
   * repeated request returns the time of the earlier request, and queues no other rebuild.
   */
  @Test
  public void testRepeatedRequestAfterRebuildIsServed() throws Exception {
    Harness h = new Harness("repeat");
    h.load(30, new Random(34));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);
    long accepted = manager.rebuild(Long.MAX_VALUE);
    HnswSegment.Descriptor base = h.awaitBase(manager, accepted + 1);
    assertEquals(accepted, manager.rebuild(accepted));
    Thread.sleep(200);
    List<HnswSegment.Descriptor> segments = h.segments();
    assertEquals(1, segments.size());
    assertEquals(base.time, segments.get(0).time);
  }

  /**
   * A server with a clock that is ahead can have written the segments of a region. A manager that
   * opens the region on another server accepts requests at times no earlier than those segments,
   * and swaps at later times. Thus an older base does not serve a new request, and a new base
   * supersedes the older base.
   */
  @Test
  public void testClockFollowsSegments() throws Exception {
    Harness h = new Harness("ahead");
    h.load(30, new Random(35));
    HnswSegment.Descriptor ahead;
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return System.currentTimeMillis() + 3_600_000;
      }
    });
    try {
      long since = EnvironmentEdgeManager.currentTimeMillis();
      HnswIndexManager first = h.open(true);
      ahead = h.awaitBase(first, since);
      first.close();
    } finally {
      EnvironmentEdgeManager.reset();
    }
    HnswIndexManager manager = h.open(true);
    long accepted = manager.rebuild(Long.MAX_VALUE);
    assertTrue(accepted >= ahead.time);
    assertTrue(h.awaitBase(manager, accepted + 1).time > ahead.time);
  }

  /**
   * A delta flush can swap after the cutover of a full rebuild, while the rebuild retires the
   * segments that it replaces. When this delta flush fails, its retry runs it again, because the
   * full rebuild does not own its graph.
   */
  @Test
  public void testFailedDeltaAfterCutoverIsRetried() throws Exception {
    Method retryFlush = HnswIndexManager.class.getDeclaredMethod("retryFlush");
    retryFlush.setAccessible(true);
    Harness h = new Harness("aftercut");
    h.load(30, new Random(36));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    HnswSegment.Descriptor first = h.awaitBase(manager, since);
    CountDownLatch retiring = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    deleteHook = delete -> {
      if (deletes(delete, first)) {
        retiring.countDown();
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    AtomicInteger deltaPuts = new AtomicInteger();
    putHook = put -> {
      if (h.isRow(put) && isDelta(put) && deltaPuts.getAndIncrement() == 0) {
        throw new IOException("injected");
      }
    };
    BooleanSupplier hasDelta = () -> {
      try {
        return h.segments().stream().anyMatch(HnswSegment.Descriptor::isDelta);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    try {
      manager.rebuild(Long.MAX_VALUE);
      assertTrue(retiring.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      float[] moved = point(50, 0);
      h.upsert(manager, "r000", moved);
      request("a failed delta flush", manager::flush, () -> deltaPuts.get() > 0);
      assertFalse(hasDelta.getAsBoolean());
      // A retry that starts before the failed task ends has no effect, so the test runs it again
      request("the retry while the rebuild retires", () -> {
        try {
          retryFlush.invoke(manager);
        } catch (ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }, hasDelta);
      assertEquals("r000", h.nearest(manager, moved));
    } finally {
      release.countDown();
    }
  }

  /**
   * A full rebuild can start while a delta flush task of its region waits behind the delta flushes
   * of other regions. The rebuild then takes over the graph of the task, and does not wait for the
   * task. The task then writes nothing.
   */
  @Test
  public void testRebuildTakesOverQueuedDelta() throws Exception {
    Harness blocker = new Harness("dblocker");
    blocker.load(30, new Random(37));
    long since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager blocking = blocker.open(true);
    blocker.awaitBase(blocking, since);
    Harness h = new Harness("dqueued");
    h.load(30, new Random(38));
    since = EnvironmentEdgeManager.currentTimeMillis();
    HnswIndexManager manager = h.open(true);
    h.awaitBase(manager, since);

    // The delta flush of the other region holds the delta thread in its segment write
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    putHook = put -> {
      if (blocker.isRow(put) && isDelta(put)) {
        held.countDown();
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
    float[] moved = point(-50, 0);
    try {
      blocker.upsert(blocking, "r000", point(50, 0));
      request("a held delta flush", blocking::flush, () -> held.getCount() == 0);
      h.upsert(manager, "r000", moved);
      manager.flush();
      long requested = manager.rebuild(Long.MAX_VALUE);
      h.awaitBase(manager, requested + 1);
      assertEquals("r000", h.nearest(manager, moved));
    } finally {
      release.countDown();
    }
    await("the held delta flush", () -> {
      try {
        return blocker.segments().get(0).isDelta();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    Thread.sleep(200);
    List<HnswSegment.Descriptor> segments = h.segments();
    assertEquals(1, segments.size());
    assertFalse(segments.get(0).isDelta());
  }
}
