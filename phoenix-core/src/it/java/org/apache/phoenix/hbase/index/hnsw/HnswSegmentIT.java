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
package org.apache.phoenix.hbase.index.hnsw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.io.ByteStreams;

/**
 * Integration tests for the HNSW segment build, the storage in the hidden family {@code V#} of a
 * data table, the off-heap load and nearest-neighbor search. The tests use the segment store of the
 * region of a table with an HNSW index, in the RegionServer process. They read the same cells
 * through the read-only store of the data table.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswSegmentIT extends ParallelStatsDisabledIT {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] DEFINITION = HnswSegment.definitionDigest("HnswSegmentIT");
  private static final int DIM = 32;
  private static final int COUNT = 1000;
  private static final int TOP_K = 10;

  private static TableName table;
  private static Connection connection;
  private static HRegion region;
  private static RegionCoprocessorEnvironment env;
  private static HnswSegmentStore.RegionStore store;
  private static List<float[]> vectors;
  private static byte[][] keys;
  private static long time = 1000L;

  /**
   * Records the mutations of {@code V#} cells. It keeps one entry for each batch, with the kind,
   * durability and row of each mutation.
   */
  public static class Recorder implements RegionCoprocessor, RegionObserver {
    static final List<List<String>> BATCHES = Collections.synchronizedList(new ArrayList<>());

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> batch) {
      List<String> mutations = new ArrayList<>();
      for (int i = 0; i < batch.size(); i++) {
        Mutation m = batch.getOperation(i);
        if (m.getFamilyCellMap().containsKey(FAMILY)) {
          mutations
            .add(mutation(m.getClass().getSimpleName() + " " + m.getDurability(), m.getRow()));
        }
      }
      if (!mutations.isEmpty()) {
        BATCHES.add(mutations);
      }
    }
  }

  private static String mutation(String kind, byte[] row) {
    return kind + " " + Bytes.toStringBinary(row);
  }

  @BeforeClass
  public static synchronized void setup() throws Exception {
    setUpTestDriver(org.apache.phoenix.util.ReadOnlyProps.EMPTY_PROPS);
    connection = getUtility().getConnection();
    String name = generateUniqueName();
    try (java.sql.Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + name + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, "
        + DIM + "))");
      // The first HNSW index adds V# to the data table
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + name
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
    }
    table = TableName.valueOf(name);
    try (Admin admin = connection.getAdmin()) {
      assertTrue(admin.getDescriptor(table).getColumnFamily(FAMILY).isMobEnabled());
      admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(table))
        .setCoprocessor(Recorder.class.getName()).build());
    }
    getUtility().waitTableAvailable(table);
    region = getUtility().getHBaseCluster().getRegions(table).get(0);
    env = region.getCoprocessorHost().findCoprocessorEnvironment(Recorder.class.getName());
    store = new HnswSegmentStore.RegionStore(env);
    Random random = new Random(42);
    vectors = new ArrayList<>();
    keys = new byte[COUNT][];
    for (int i = 0; i < COUNT; i++) {
      vectors.add(randomVector(random, DIM));
      keys[i] = Bytes.toBytes("row-" + i);
    }
  }

  private static float[] randomVector(Random random, int dim) {
    float[] v = new float[dim];
    for (int d = 0; d < dim; d++) {
      v[d] = (float) random.nextGaussian();
    }
    return v;
  }

  private static PTable.VectorIndex index(String quantization, Integer pqSegments) {
    return new PTable.VectorIndex.Builder().setAlgorithm("HNSW").setDistanceMetric("COSINE")
      .setDimension(DIM).setHnswM(16).setHnswEfConstruction(100).setHnswAlpha(1.2)
      .setQuantizationType(quantization).setPqSegments(pqSegments).build();
  }

  // The descriptor of a new segment of an index, which covers the range of the region
  private static synchronized HnswSegment.Descriptor descriptor(String indexKey, int count) {
    long t = ++time;
    return new HnswSegment.Descriptor(Bytes.toBytes(indexKey), region.getRegionInfo().getStartKey(),
      t, region.getRegionInfo().getEndKey(), count, null, t, DEFINITION);
  }

  // Returns the descriptor of a new delta segment of an index on the given base. The delta covers
  // the range of the region
  private static synchronized HnswSegment.Descriptor delta(String indexKey, int count,
    long baseTime) {
    long t = ++time;
    return new HnswSegment.Descriptor(Bytes.toBytes(indexKey), region.getRegionInfo().getStartKey(),
      t, region.getRegionInfo().getEndKey(), count, baseTime, t, DEFINITION);
  }

  private static HnswSegment.Descriptor store(PTable.VectorIndex vi, String indexKey)
    throws IOException {
    List<VectorFloat<?>> jv =
      vectors.stream().map(VTS::createFloatVector).collect(Collectors.toList());
    return write(descriptor(indexKey, COUNT), build(vi, jv, keys, DIM), new byte[0][]);
  }

  // Returns the descriptor with another layout
  private static HnswSegment.Descriptor withLayout(HnswSegment.Descriptor d,
    HnswSegment.Layout layout) {
    return new HnswSegment.Descriptor(d.indexKey, d.startKey, d.time, d.endKey, d.count, d.baseTime,
      d.replayFrom, d.definition, layout);
  }

  // Returns the payload of a new segment of the vectors
  private static byte[] build(PTable.VectorIndex vi, List<VectorFloat<?>> vectors, byte[][] keys,
    int dim) throws IOException {
    try (HnswSegment.Built built =
      HnswSegment.build(vi, new ListRandomAccessVectorValues(vectors, dim), keys,
        new HnswOffheapAllocator(Long.MAX_VALUE));
      InputStream in = built.payload()) {
      return ByteStreams.toByteArray(in);
    }
  }

  // Writes a segment through the region store, and returns its descriptor with its layout. A null
  // payload writes no payload. The layout takes the graph length from the trailer of the payload
  private static HnswSegment.Descriptor write(HnswSegment.Descriptor d, byte[] payload,
    byte[][] tombstones) throws IOException {
    HnswSegment.Layout p = payload == null
      ? HnswSegment.Layout.EMPTY
      : SegmentCells.layoutOf(payload, HnswSegment.CHUNK_SIZE);
    long t = tombstones.length > 0 ? HnswSegment.KeyStream.length(tombstones) : 0;
    HnswSegment.Descriptor written = withLayout(d, store.layout(p.payloadLength, p.graphLength, t));
    store.write(written, payload == null ? null : new ByteArrayInputStream(payload),
      t > 0 ? new HnswSegment.KeyStream(tombstones) : null, () -> false);
    return written;
  }

  private static HnswSegment open(HnswSegment.Descriptor d, HnswOffheapAllocator allocator)
    throws IOException {
    return HnswSegment.open(store, d, allocator, "COSINE");
  }

  private static Set<String> bruteForce(float[] query) {
    VectorFloat<?> q = VTS.createFloatVector(query);
    return IntStream.range(0, COUNT).boxed()
      .sorted(Comparator.comparingDouble(
        i -> -VectorSimilarityFunction.COSINE.compare(q, VTS.createFloatVector(vectors.get(i)))))
      .limit(TOP_K).map(i -> Bytes.toString(keys[i])).collect(Collectors.toSet());
  }

  private static double recall(HnswSegment segment) throws IOException {
    Random random = new Random(7);
    int hits = 0;
    int queries = 20;
    for (int i = 0; i < queries; i++) {
      float[] query = randomVector(random, DIM);
      Set<String> expected = bruteForce(query);
      SearchResult result = segment.search(VTS.createFloatVector(query), TOP_K, 64, Bits.ALL);
      for (SearchResult.NodeScore ns : result.getNodes()) {
        if (expected.contains(Bytes.toString(segment.getKey(ns.node)))) {
          hits++;
        }
      }
    }
    return hits / (double) (queries * TOP_K);
  }

  // The first payload chunk of a segment as stored, a MOB reference after the V# flush
  private static Cell storedPayload(HnswSegment.Descriptor d) throws IOException {
    return stored(d, HnswSegment.PAYLOAD_TAG, 0);
  }

  // Returns a stored chunk cell of a segment. After the V# flush, a large cell is a MOB reference
  private static Cell stored(HnswSegment.Descriptor d, byte tag, int chunk) throws IOException {
    byte[] qualifier = HnswSegment.qualifier(d.identity, chunk, tag);
    Scan raw =
      new Scan().withStartRow(d.anchor).withStopRow(d.anchor, true).addColumn(FAMILY, qualifier);
    raw.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    try (Table t = connection.getTable(table); ResultScanner scanner = t.getScanner(raw)) {
      return scanner.next().getColumnLatestCell(FAMILY, qualifier);
    }
  }

  private static Path mobFile(Cell reference) {
    return new Path(MobUtils.getMobFamilyPath(getUtility().getConfiguration(), table,
      QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY), MobUtils.getMobFileName(reference));
  }

  // Writes a base segment of another index, so that V# flushes the deltas of the test. A base write
  // waits until the V# memstore is empty. A delta write only asks for the V# flush. HBase does not
  // do the requested flush while another flush of the region is queued or runs
  private static void flushDeltas() throws IOException {
    write(descriptor("seg-flush", 0), null, new byte[0][]);
    assertEquals(0, region.getStore(FAMILY).getFlushableSize().getDataSize());
  }

  // Returns true if the store lists the segment with all parts of its descriptor. The writer of the
  // segment must be the region of the test
  private static boolean listed(HnswSegmentStore s, HnswSegment.Descriptor d) throws IOException {
    for (HnswSegment.Descriptor l : s.list(d.indexKey)) {
      if (Bytes.equals(l.identity, d.identity)) {
        HnswSegmentTest.assertDescriptor(d, l);
        assertArrayEquals(region.getRegionInfo().getEncodedNameAsBytes(), l.writer);
        return true;
      }
    }
    return false;
  }

  /**
   * Verifies the round trip of an unquantized segment through V#. The write leaves the payload in a
   * MOB file and the V# memstore empty. Both stores list the segment and read the payload. The row
   * keys keep their order, and the search has a high recall.
   */
  @Test
  public void testMobRoundTripAndRecall() throws Exception {
    HnswSegment.Descriptor d = store(index("NONE", null), "seg-none");
    assertEquals(0, region.getStore(FAMILY).getFlushableSize().getDataSize());
    Cell cell = storedPayload(d);
    assertTrue("segment payload should be a MOB reference", MobUtils.hasValidMobRefCellValue(cell));
    assertTrue(MobUtils.getMobValueLength(cell) > cell.getValueLength());
    Path mob = mobFile(cell);
    assertTrue(mob + " must exist", mob.getFileSystem(getUtility().getConfiguration()).exists(mob));

    HnswSegment segment = open(d, new HnswOffheapAllocator(Long.MAX_VALUE));
    assertEquals(COUNT, segment.size());
    Set<String> stored = new HashSet<>();
    for (int i = 0; i < COUNT; i++) {
      stored.add(Bytes.toString(segment.getKey(i)));
      // The ordinals follow row key order
      if (i > 0) {
        assertTrue(Bytes.compareTo(segment.getKey(i - 1), segment.getKey(i)) < 0);
      }
    }
    assertEquals(COUNT, stored.size());
    assertEquals(0, segment.ceiling(new byte[0]));
    assertEquals(COUNT, segment.ceiling(Bytes.toBytes("s")));
    double recall = recall(segment);
    assertTrue("recall " + recall, recall >= 0.95);
    assertTrue(listed(store, d));

    try (Table t = connection.getTable(table)) {
      HnswSegmentStore client = new HnswSegmentStore.TableStore(t);
      assertTrue(listed(client, d));
      HnswSegment read =
        HnswSegment.open(client, d, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
      assertEquals(COUNT, read.size());
      recall = recall(read);
      assertTrue("recall " + recall, recall >= 0.95);
      read.close();
    }
  }

  /** Verifies the search recall of SQ8 and PQ segments. */
  @Test
  public void testQuantizedRecall() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
    HnswSegment sq8 = open(store(index("SQ8", null), "seg-sq8"), allocator);
    double recall = recall(sq8);
    assertTrue("SQ8 recall " + recall, recall >= 0.9);
    HnswSegment pq = open(store(index("PQ", 8), "seg-pq"), allocator);
    recall = recall(pq);
    assertTrue("PQ recall " + recall, recall >= 0.7);
  }

  /** A full off-heap budget evicts the LRU segment, and the next search of it loads it again. */
  @Test
  public void testEvictionAndReload() throws Exception {
    HnswSegment.Descriptor rowA = store(index("NONE", null), "seg-a");
    HnswSegment.Descriptor rowB = store(index("NONE", null), "seg-b");
    // Measure the segments, to set a budget that holds only one of them
    HnswOffheapAllocator probeA = new HnswOffheapAllocator(Long.MAX_VALUE);
    open(rowA, probeA);
    long sizeA = probeA.getAllocatedBytes();
    HnswOffheapAllocator probeB = new HnswOffheapAllocator(Long.MAX_VALUE);
    open(rowB, probeB);
    long sizeB = probeB.getAllocatedBytes();

    HnswOffheapAllocator allocator =
      new HnswOffheapAllocator(Math.max(sizeA, sizeB) + Math.min(sizeA, sizeB) / 2);
    HnswSegment a = open(rowA, allocator);
    VectorFloat<?> query = VTS.createFloatVector(vectors.get(3));
    int[] before = nodes(a.search(query, TOP_K, 64, Bits.ALL));
    HnswSegment b = open(rowB, allocator);
    assertEquals("Opening a second segment should evict the first segment", sizeB,
      allocator.getAllocatedBytes());
    assertArrayEquals(before, nodes(a.search(query, TOP_K, 64, Bits.ALL)));
    assertEquals("Accessing the first segment should trigger reload and evict the second", sizeA,
      allocator.getAllocatedBytes());
    assertEquals("row-3",
      Bytes.toString(a.getKey(a.search(query, 1, 64, Bits.ALL).getNodes()[0].node)));
    b.search(query, 1, 64, Bits.ALL);
  }

  /** A PQ segment with too few vectors to train PQ stores the full vectors. */
  @Test
  public void testSmallPqSegment() throws Exception {
    int count = 50;
    List<VectorFloat<?>> subset = new ArrayList<>();
    byte[][] subKeys = new byte[count][];
    for (int i = 0; i < count; i++) {
      subset.add(VTS.createFloatVector(vectors.get(i)));
      subKeys[i] = Bytes.toBytes("small-pq-" + i);
    }
    HnswSegment.Descriptor d = write(descriptor("seg-pq-small", count),
      build(index("PQ", 8), subset, subKeys, DIM), new byte[0][]);
    HnswSegment segment = open(d, new HnswOffheapAllocator(Long.MAX_VALUE));
    for (int i = 0; i < count; i++) {
      assertArrayEquals(subKeys[i],
        segment.getKey(segment.search(subset.get(i), 1, 16, Bits.ALL).getNodes()[0].node));
    }
  }

  private static int[] nodes(SearchResult result) {
    return Arrays.stream(result.getNodes()).mapToInt(ns -> ns.node).toArray();
  }

  @Test
  public void testCorruptPayloadRejected() throws Exception {
    HnswSegment.Descriptor d =
      write(descriptor("seg-corrupt", 1), Bytes.toBytes("not a segment"), new byte[0][]);
    assertCorrupt(store, d, HnswSegment.CorruptSegmentException.Kind.INVALID);
  }

  /**
   * A payload can be larger than the 10 MB cell size limit of the RegionServer RPC path, and larger
   * than one chunk. The store streams the payload from the build result in chunks, and flushes each
   * chunk to a MOB file. The chunks read back give the same payload. The segment that the store
   * reads gives the same search results as the segment of the build result. A graph of random
   * vectors in many dimensions does not find each vector as its own nearest neighbor, so the test
   * compares the two searches.
   */
  @Test
  public void testLargePayload() throws Exception {
    int dim = 768;
    int count = 4000;
    Random random = new Random(11);
    List<VectorFloat<?>> large = new ArrayList<>(count);
    byte[][] largeKeys = new byte[count][];
    for (int i = 0; i < count; i++) {
      large.add(VTS.createFloatVector(randomVector(random, dim)));
      largeKeys[i] = Bytes.toBytes("large-" + i);
    }
    PTable.VectorIndex vi = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
      .setDistanceMetric("COSINE").setDimension(dim).setHnswM(8).setHnswEfConstruction(32)
      .setHnswAlpha(1.2).setQuantizationType("NONE").build();
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
    try (HnswSegment.Built built =
      HnswSegment.build(vi, new ListRandomAccessVectorValues(large, dim), largeKeys, allocator)) {
      assertTrue(built.length() + " bytes", built.length() > 10L * 1024 * 1024);
      HnswSegment.Descriptor d = withLayout(descriptor("seg-large", count),
        store.layout(built.length(), built.graphLength(), 0));
      store.write(d, built.payload(), null, () -> false);
      assertEquals(0, region.getStore(FAMILY).getFlushableSize().getDataSize());
      int chunks = d.layout.chunks(HnswSegment.PAYLOAD_TAG);
      assertTrue(chunks + " chunks", chunks > 1);
      for (int i = 0; i < chunks; i++) {
        Cell reference = stored(d, HnswSegment.PAYLOAD_TAG, i);
        assertTrue(MobUtils.hasValidMobRefCellValue(reference));
        assertEquals(d.layout.chunkLength(HnswSegment.PAYLOAD_TAG, i),
          MobUtils.getMobValueLength(reference));
      }
      ByteArrayOutputStream read = new ByteArrayOutputStream();
      store.copyPayload(d, read);
      try (InputStream in = built.payload()) {
        assertArrayEquals(ByteStreams.toByteArray(in), read.toByteArray());
      }
      HnswSegment segment = open(d, new HnswOffheapAllocator(Long.MAX_VALUE));
      assertEquals(count, segment.size());
      HnswSegment expected = HnswSegment.of(null, d, allocator, "COSINE", built, new byte[0][]);
      for (int i = 0; i < count; i += 1000) {
        int[] nodes = nodes(segment.search(large.get(i), TOP_K, 100, Bits.ALL));
        assertEquals(TOP_K, nodes.length);
        assertArrayEquals(nodes(expected.search(large.get(i), TOP_K, 100, Bits.ALL)), nodes);
      }
      expected.close();
      segment.close();
    }
  }

  /**
   * Segment writes and retirements skip the WAL, but a purge uses it. One retirement mutation
   * deletes all cells of its segments in all their anchor rows. A purge deletes all cells of one
   * index.
   */
  @Test
  public void testSegmentWritesSkipWal() throws Exception {
    Recorder.BATCHES.clear();
    HnswSegment.Descriptor d1 = descriptor("seg-wal", 0);
    // A segment of a key range in the region, with a different anchor row
    HnswSegment.Descriptor d2 = new HnswSegment.Descriptor(d1.indexKey, Bytes.toBytes("m"),
      d1.time + 1, Bytes.toBytes("t"), 0, null, d1.time + 1, DEFINITION);
    assertFalse(Bytes.equals(d1.anchor, d2.anchor));
    write(d1, null, new byte[0][]);
    write(d2, null, new byte[0][]);
    assertEquals(Arrays.asList(Collections.singletonList(mutation("Put SKIP_WAL", d1.anchor)),
      Collections.singletonList(mutation("Put SKIP_WAL", d2.anchor))), Recorder.BATCHES);
    assertTrue(listed(store, d1));
    assertTrue(listed(store, d2));

    Recorder.BATCHES.clear();
    store.retire(Arrays.asList(d1, d2));
    // One batch, so one atomic mutation, over both anchor rows
    assertEquals(Collections.singletonList(Arrays.asList(mutation("Delete SKIP_WAL", d1.anchor),
      mutation("Delete SKIP_WAL", d2.anchor))), Recorder.BATCHES);
    assertTrue(store.list(d1.indexKey).isEmpty());

    // A delta with a payload and tombstones has a cell for each small tag, and one chunk of each
    // stream
    HnswSegment.Descriptor d3 = write(delta("seg-purge", 1, d1.time), Bytes.toBytes("payload!"),
      new byte[][] { Bytes.toBytes("deleted") });
    HnswSegment.Descriptor d4 =
      write(descriptor("seg-purge2", 1), Bytes.toBytes("payload!"), new byte[0][]);
    Recorder.BATCHES.clear();
    assertEquals(HnswSegment.SMALL_TAGS.length + 2, store.purge(d3.indexKey));
    assertEquals(Collections.singletonList(
      Collections.singletonList(mutation("Delete USE_DEFAULT", d3.anchor))), Recorder.BATCHES);
    assertTrue(store.list(d3.indexKey).isEmpty());
    // The purge keeps the cells of another index whose key starts with the purged key
    assertTrue(listed(store, d4));
    Recorder.BATCHES.clear();
    assertEquals(0, store.purge(d3.indexKey));
    assertTrue(Recorder.BATCHES.isEmpty());
  }

  /**
   * The cells of a segment have the segment time as their timestamp. A retirement or a purge
   * deletes each cell at its own timestamp. As a result, a server whose clock is behind the clock
   * of the writer still deletes the cells of the segment.
   */
  @Test
  public void testDeletesUseCellTimestamps() throws Exception {
    // The segment time of a writer whose clock is one hour ahead of the clock of this server
    long ahead = EnvironmentEdgeManager.currentTime() + TimeUnit.HOURS.toMillis(1);
    byte[] start = region.getRegionInfo().getStartKey();
    byte[] end = region.getRegionInfo().getEndKey();
    // Deltas with tombstones, so that the deletes cover the cells of every tag
    byte[][] tombstones = { Bytes.toBytes("deleted") };
    HnswSegment.Descriptor retired = new HnswSegment.Descriptor(Bytes.toBytes("seg-clock"), start,
      ahead, end, 0, ahead - 1, ahead, DEFINITION);
    HnswSegment.Descriptor purged = new HnswSegment.Descriptor(Bytes.toBytes("seg-clock-purge"),
      start, ahead, end, 1, ahead - 1, ahead, DEFINITION);
    retired = write(retired, null, tombstones);
    purged = write(purged, Bytes.toBytes("payload!"), tombstones);
    assertEquals(HnswSegment.SMALL_TAGS.length + 1, cells(retired.indexKey).size());
    assertEquals(HnswSegment.SMALL_TAGS.length + 2, cells(purged.indexKey).size());
    for (HnswSegment.Descriptor d : Arrays.asList(retired, purged)) {
      for (Cell cell : cells(d.indexKey)) {
        assertEquals(ahead, cell.getTimestamp());
      }
    }

    store.retire(Collections.singletonList(retired));
    assertTrue(cells(retired.indexKey).isEmpty());
    assertEquals(HnswSegment.SMALL_TAGS.length + 2, store.purge(purged.indexKey));
    assertTrue(cells(purged.indexKey).isEmpty());
  }

  // The visible cells of an index in V#, as stored
  private static List<Cell> cells(byte[] indexKey) throws IOException {
    List<Cell> cells = new ArrayList<>();
    try (Table t = connection.getTable(table);
      ResultScanner scanner = t.getScanner(HnswSegmentStore.indexScan(indexKey))) {
      for (Result r : scanner) {
        cells.addAll(Arrays.asList(r.rawCells()));
      }
    }
    return cells;
  }

  /**
   * A listing returns no chunk cell. Until the V# flush after a segment write, the chunks are plain
   * cells in the memstore with the full values. MOB_SCAN_RAW has no effect on these cells. The
   * server filter removes them from the listing scan, so no chunk goes to the client or to the
   * listing.
   */
  @Test
  public void testListingSkipsPayloadInMemstore() throws Exception {
    // A flush that an earlier delta write asked for must not flush the cells of this test
    flushDeltas();
    HnswSegment.Descriptor d = withLayout(delta("seg-memstore", 1, time),
      new HnswSegment.Layout(HnswSegment.CHUNK_SIZE, 1 << 16, (1 << 16) - 8, 1 << 16));
    // The cells of a segment write, without the V# flush that follows the write
    Put put = new Put(d.anchor, d.time)
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG), d.endKey)
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG),
        Bytes.toBytes(d.count))
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG),
        Bytes.toBytes(d.replayFrom))
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG),
        d.definition)
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG),
        region.getRegionInfo().getEncodedNameAsBytes())
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.BASE_TIME_TAG),
        Bytes.toBytes(d.baseTime))
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.LAYOUT_TAG),
        d.layout.encode())
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, 0, HnswSegment.PAYLOAD_TAG),
        new byte[1 << 16])
      .addColumn(FAMILY, HnswSegment.qualifier(d.identity, 0, HnswSegment.TOMBSTONES_TAG),
        new byte[1 << 16]);
    put.setDurability(Durability.SKIP_WAL);
    region.put(put);
    assertTrue(region.getStore(FAMILY).getFlushableSize().getDataSize() > 2 << 16);

    List<Cell> returned = new ArrayList<>();
    try (Table t = connection.getTable(table)) {
      HnswSegmentStore server = new HnswSegmentStore.RegionStore(env) {
        @Override
        void scan(Scan scan, Consumer<Cell> consumer) throws IOException {
          super.scan(scan, cell -> {
            returned.add(cell);
            consumer.accept(cell);
          });
        }
      };
      HnswSegmentStore client = new HnswSegmentStore.TableStore(t) {
        @Override
        void scan(Scan scan, Consumer<Cell> consumer) throws IOException {
          super.scan(scan, cell -> {
            returned.add(cell);
            consumer.accept(cell);
          });
        }
      };
      for (HnswSegmentStore s : Arrays.asList(server, client)) {
        returned.clear();
        assertTrue(listed(s, d));
        assertEquals(s.toString(), HnswSegment.SMALL_TAGS.length, returned.size());
        for (Cell cell : returned) {
          byte tag =
            cell.getQualifierArray()[cell.getQualifierOffset() + cell.getQualifierLength() - 1];
          assertFalse(s.toString(),
            tag == HnswSegment.PAYLOAD_TAG || tag == HnswSegment.TOMBSTONES_TAG);
        }
      }
    }
    store.retire(Collections.singletonList(d));
  }

  /**
   * A listing never reads a MOB file, so the listing still shows a segment whose MOB file is lost.
   * A read of the chunk in that file returns an empty value. The open of the segment fails as for a
   * corrupt segment, not as for a retired segment. The region then rebuilds, and does not list
   * again.
   */
  @Test
  public void testListingDoesNotResolvePayloads() throws Exception {
    HnswSegment.Descriptor d = store(index("NONE", null), "seg-lost-mob");
    Cell reference = storedPayload(d);
    assertTrue(MobUtils.hasValidMobRefCellValue(reference));
    Path mob = mobFile(reference);
    FileSystem fs = mob.getFileSystem(getUtility().getConfiguration());
    assertTrue(fs.delete(mob, false));
    // Drop the cached reader of the MOB file, which can still read a deleted file
    for (JVMClusterUtil.RegionServerThread t : getUtility().getHBaseCluster()
      .getLiveRegionServerThreads()) {
      t.getRegionServer().getMobFileCache()
        .ifPresent(c -> c.evictFile(MobUtils.getMobFileName(reference)));
    }

    assertTrue(listed(store, d));
    try (Table t = connection.getTable(table)) {
      HnswSegmentStore client = new HnswSegmentStore.TableStore(t);
      assertTrue(listed(client, d));
      for (HnswSegmentStore s : Arrays.asList(store, client)) {
        assertCorrupt(s, d, HnswSegment.CorruptSegmentException.Kind.UNRESOLVED);
      }
    }
  }

  /**
   * A missing L cell means that the region retired the segment after its listing. A missing chunk
   * or an empty chunk is corrupt.
   */
  @Test
  public void testLostPayload() throws Exception {
    HnswSegment.Descriptor retired = store(index("NONE", null), "seg-retired");
    HnswSegment.Descriptor missing = store(index("NONE", null), "seg-missing");
    HnswSegment.Descriptor empty = store(index("NONE", null), "seg-empty");
    region.delete(new Delete(retired.anchor).addColumns(FAMILY,
      HnswSegment.qualifier(retired.identity, HnswSegment.LAYOUT_TAG), retired.time));
    region.delete(new Delete(missing.anchor).addColumns(FAMILY,
      HnswSegment.qualifier(missing.identity, 0, HnswSegment.PAYLOAD_TAG), missing.time));
    region.put(new Put(empty.anchor, empty.time).addColumn(FAMILY,
      HnswSegment.qualifier(empty.identity, 0, HnswSegment.PAYLOAD_TAG), new byte[0]));
    assertFalse(listed(store, retired));
    try {
      HnswSegment.open(store, retired, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
      fail("a retired segment must not load");
    } catch (HnswSegment.NotFoundException expected) {
    }
    assertTrue(listed(store, missing));
    assertCorrupt(store, missing, HnswSegment.CorruptSegmentException.Kind.MISSING);
    assertTrue(listed(store, empty));
    assertCorrupt(store, empty, HnswSegment.CorruptSegmentException.Kind.UNRESOLVED);
  }

  /**
   * A delta segment keeps its entries, its tombstones and its base time through a write and a load.
   * Both stores list the base and the delta, tell them apart by the base time, and read the
   * tombstones of the delta.
   */
  @Test
  public void testDeltaSegmentRoundTripAndTombstones() throws Exception {
    HnswSegment.Descriptor base = store(index("NONE", null), "seg-delta");
    List<VectorFloat<?>> jv =
      vectors.stream().map(VTS::createFloatVector).collect(Collectors.toList());
    int deltaCount = 10;
    List<VectorFloat<?>> deltaVectors = new ArrayList<>();
    byte[][] deltaKeys = new byte[deltaCount][];
    for (int i = 0; i < deltaCount; i++) {
      deltaVectors.add(jv.get(i));
      deltaKeys[i] = Bytes.toBytes("delta-row-" + i);
    }
    byte[] deltaPayload = build(index("NONE", null), deltaVectors, deltaKeys, DIM);
    byte[][] tombstones = new byte[][] { Bytes.toBytes("deleted-row-1"),
      Bytes.toBytes("deleted-row-2"), Bytes.toBytes("deleted-row-3") };
    HnswSegment.Descriptor delta =
      write(delta("seg-delta", deltaCount, base.time), deltaPayload, tombstones);
    flushDeltas();

    try (Table t = connection.getTable(table)) {
      for (HnswSegmentStore s : Arrays.asList(store, new HnswSegmentStore.TableStore(t))) {
        List<HnswSegment.Descriptor> listed = s.list(delta.indexKey);
        assertEquals(s.toString(), 2, listed.size());
        for (HnswSegment.Descriptor d : listed) {
          HnswSegmentTest.assertDescriptor(d.isDelta() ? delta : base, d);
          assertArrayEquals(region.getRegionInfo().getEncodedNameAsBytes(), d.writer);
        }
        HnswSegment deltaSegment =
          HnswSegment.open(s, delta, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
        assertEquals(deltaCount, deltaSegment.size());
        byte[][] read = deltaSegment.getTombstones();
        assertEquals(3, read.length);
        for (int i = 0; i < read.length; i++) {
          assertArrayEquals(tombstones[i], read[i]);
        }
        SearchResult result = deltaSegment.search(deltaVectors.get(0), 1, 16, Bits.ALL);
        assertEquals(1, result.getNodes().length);
        assertArrayEquals(deltaKeys[0], deltaSegment.getKey(result.getNodes()[0].node));
        deltaSegment.close();
        HnswSegment baseSegment =
          HnswSegment.open(s, base, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
        assertEquals(0, baseSegment.getTombstones().length);
        baseSegment.close();
      }
    }
  }

  /**
   * A delta segment can hold only tombstones. It has no payload chunks, and a search of it returns
   * no rows. Many tombstones are larger than the MOB threshold, so they go to a MOB file, as a
   * payload does. The listing never reads them. If the MOB file of the tombstones is lost, the
   * tombstones chunk reads as an empty value. The open of the delta then fails as for a corrupt
   * segment, not as for a retired segment.
   */
  @Test
  public void testDeleteOnlyDeltaSegment() throws Exception {
    byte[][] tombstones = new byte[][] { Bytes.toBytes("tomb-a"), Bytes.toBytes("tomb-b") };
    HnswSegment.Descriptor small = write(delta("seg-delonly", 0, time), null, tombstones);
    assertEquals(0, small.layout.chunks(HnswSegment.PAYLOAD_TAG));
    assertTrue(listed(store, small));
    assertTrue(store.list(small.indexKey).get(0).isDelta());
    HnswSegment segment =
      HnswSegment.open(store, small, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
    assertEquals(0, segment.size());
    assertEquals(2, segment.getTombstones().length);
    assertArrayEquals(tombstones[0], segment.getTombstones()[0]);
    assertArrayEquals(tombstones[1], segment.getTombstones()[1]);
    assertEquals(0,
      segment.search(VTS.createFloatVector(new float[DIM]), 5, 16, Bits.ALL).getNodes().length);
    segment.close();

    byte[][] many = new byte[500][];
    for (int i = 0; i < many.length; i++) {
      many[i] = Bytes.toBytes(String.format("deleted-row-%05d", i));
    }
    HnswSegment.Descriptor large = write(delta("seg-delonly-large", 0, time), null, many);
    flushDeltas();
    Cell reference = stored(large, HnswSegment.TOMBSTONES_TAG, 0);
    assertTrue("tombstones should be a MOB reference", MobUtils.hasValidMobRefCellValue(reference));
    try (Table t = connection.getTable(table)) {
      HnswSegmentStore client = new HnswSegmentStore.TableStore(t);
      for (HnswSegmentStore s : Arrays.asList(store, client)) {
        assertTrue(listed(s, large));
        HnswSegment read =
          HnswSegment.open(s, large, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
        assertEquals(many.length, read.getTombstones().length);
        assertArrayEquals(many[many.length - 1], read.getTombstones()[many.length - 1]);
        read.close();
      }

      Path mob = mobFile(reference);
      assertTrue(mob.getFileSystem(getUtility().getConfiguration()).delete(mob, false));
      for (JVMClusterUtil.RegionServerThread rs : getUtility().getHBaseCluster()
        .getLiveRegionServerThreads()) {
        rs.getRegionServer().getMobFileCache()
          .ifPresent(c -> c.evictFile(MobUtils.getMobFileName(reference)));
      }
      for (HnswSegmentStore s : Arrays.asList(store, client)) {
        assertTrue(listed(s, large));
        assertCorrupt(s, large, HnswSegment.CorruptSegmentException.Kind.UNRESOLVED);
      }
    }
  }

  // Asserts that the open of a segment fails as for a corrupt segment of the given kind, and not as
  // for a retired one
  private static void assertCorrupt(HnswSegmentStore s, HnswSegment.Descriptor d,
    HnswSegment.CorruptSegmentException.Kind kind) throws IOException {
    try {
      HnswSegment.open(s, d, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
      fail("a corrupt segment must not load from " + s);
    } catch (HnswSegment.NotFoundException e) {
      fail("the segment is corrupt, not retired: " + e);
    } catch (HnswSegment.CorruptSegmentException expected) {
      assertEquals(expected.getMessage(), kind, expected.getKind());
      assertTrue(expected.getMessage(), expected.getMessage().startsWith("Corrupt HNSW segment"));
    }
  }
}
