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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.schema.PTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for HNSW segment construction, MOB storage, off-heap materialization, and
 * nearest-neighbor search.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswSegmentIT extends ParallelStatsDisabledIT {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final byte[] FAMILY = Bytes.toBytes("0");
  private static final int DIM = 32;
  private static final int COUNT = 1000;
  private static final int TOP_K = 10;

  private static TableName table;
  private static Connection connection;
  private static List<float[]> vectors;
  private static byte[][] keys;

  @BeforeClass
  public static synchronized void setup() throws Exception {
    setUpTestDriver(org.apache.phoenix.util.ReadOnlyProps.EMPTY_PROPS);
    connection = getUtility().getConnection();
    table = TableName.valueOf(generateUniqueName());
    try (Admin admin = connection.getAdmin()) {
      admin.createTable(
        TableDescriptorBuilder.newBuilder(table).setColumnFamily(ColumnFamilyDescriptorBuilder
          .newBuilder(FAMILY).setMobEnabled(true).setMobThreshold(1024).build()).build());
    }
    Random random = new Random(42);
    vectors = new ArrayList<>();
    keys = new byte[COUNT][];
    for (int i = 0; i < COUNT; i++) {
      vectors.add(randomVector(random));
      keys[i] = Bytes.toBytes("row-" + i);
    }
  }

  private static float[] randomVector(Random random) {
    float[] v = new float[DIM];
    for (int d = 0; d < DIM; d++) {
      v[d] = (float) random.nextGaussian();
    }
    return v;
  }

  private static PTable.VectorIndex index(String quantization, Integer pqSegments) {
    return new PTable.VectorIndex.Builder().setAlgorithm("HNSW").setDistanceMetric("COSINE")
      .setDimension(DIM).setHnswM(16).setHnswEfConstruction(100).setHnswAlpha(1.2)
      .setQuantizationType(quantization).setPqSegments(pqSegments).build();
  }

  private static byte[] store(PTable.VectorIndex vi, String row) throws IOException {
    List<VectorFloat<?>> jv =
      vectors.stream().map(VTS::createFloatVector).collect(Collectors.toList());
    byte[] payload = HnswSegment.build(vi, new ListRandomAccessVectorValues(jv, DIM), keys);
    byte[] rowKey = Bytes.toBytes(row);
    try (Table t = connection.getTable(table)) {
      t.put(new Put(rowKey).addColumn(FAMILY, HnswSegment.PAYLOAD_QUALIFIER, payload));
    }
    return rowKey;
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
      float[] query = randomVector(random);
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

  /**
   * Tests unquantized segment persistence via MOB storage, row key retrieval, and search recall.
   */
  @Test
  public void testMobRoundTripAndRecall() throws Exception {
    byte[] rowKey = store(index("NONE", null), "seg-none");
    try (Admin admin = connection.getAdmin()) {
      admin.flush(table);
    }
    Scan raw = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);
    raw.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    try (Table t = connection.getTable(table); ResultScanner scanner = t.getScanner(raw)) {
      Result r = scanner.next();
      Cell cell = r.getColumnLatestCell(FAMILY, HnswSegment.PAYLOAD_QUALIFIER);
      // Verify that the stored cell contains a valid MOB reference value
      assertTrue("segment payload should be a MOB reference",
        MobUtils.hasValidMobRefCellValue(cell));
      assertTrue(MobUtils.getMobValueLength(cell) > cell.getValueLength());
    }

    HnswSegment segment = HnswSegment.open(connection, table, FAMILY, rowKey,
      new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
    assertEquals(COUNT, segment.size());
    Set<String> stored = new HashSet<>();
    for (int i = 0; i < COUNT; i++) {
      stored.add(Bytes.toString(segment.getKey(i)));
      // Ordinals must strictly maintain lexicographical row key order
      if (i > 0) {
        assertTrue(Bytes.compareTo(segment.getKey(i - 1), segment.getKey(i)) < 0);
      }
    }
    assertEquals(COUNT, stored.size());
    assertEquals(0, segment.ceiling(new byte[0]));
    assertEquals(COUNT, segment.ceiling(Bytes.toBytes("s")));
    double recall = recall(segment);
    assertTrue("recall " + recall, recall >= 0.95);
  }

  /** Tests search recall on segments using scalar (SQ8) and product (PQ) quantization. */
  @Test
  public void testQuantizedRecall() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
    HnswSegment sq8 = HnswSegment.open(connection, table, FAMILY,
      store(index("SQ8", null), "seg-sq8"), allocator, "COSINE");
    double recall = recall(sq8);
    assertTrue("SQ8 recall " + recall, recall >= 0.9);
    HnswSegment pq = HnswSegment.open(connection, table, FAMILY, store(index("PQ", 8), "seg-pq"),
      allocator, "COSINE");
    recall = recall(pq);
    assertTrue("PQ recall " + recall, recall >= 0.7);
  }

  /** Tests LRU segment eviction and automatic reload when memory budget limits are exceeded. */
  @Test
  public void testEvictionAndReload() throws Exception {
    byte[] rowA = store(index("NONE", null), "seg-a");
    byte[] rowB = store(index("NONE", null), "seg-b");
    // Determine segment sizes to size the constrained allocator
    HnswOffheapAllocator probeA = new HnswOffheapAllocator(Long.MAX_VALUE);
    HnswSegment.open(connection, table, FAMILY, rowA, probeA, "COSINE");
    long sizeA = probeA.getAllocatedBytes();
    HnswOffheapAllocator probeB = new HnswOffheapAllocator(Long.MAX_VALUE);
    HnswSegment.open(connection, table, FAMILY, rowB, probeB, "COSINE");
    long sizeB = probeB.getAllocatedBytes();

    HnswOffheapAllocator allocator =
      new HnswOffheapAllocator(Math.max(sizeA, sizeB) + Math.min(sizeA, sizeB) / 2);
    HnswSegment a = HnswSegment.open(connection, table, FAMILY, rowA, allocator, "COSINE");
    VectorFloat<?> query = VTS.createFloatVector(vectors.get(3));
    int[] before = nodes(a.search(query, TOP_K, 64, Bits.ALL));
    HnswSegment b = HnswSegment.open(connection, table, FAMILY, rowB, allocator, "COSINE");
    assertEquals("Opening a second segment should evict the first segment", sizeB,
      allocator.getAllocatedBytes());
    assertArrayEquals(before, nodes(a.search(query, TOP_K, 64, Bits.ALL)));
    assertEquals("Accessing the first segment should trigger reload and evict the second", sizeA,
      allocator.getAllocatedBytes());
    assertEquals("row-3",
      Bytes.toString(a.getKey(a.search(query, 1, 64, Bits.ALL).getNodes()[0].node)));
    b.search(query, 1, 64, Bits.ALL);
  }

  /** Tests that product quantization falls back to unquantized when the training set is small. */
  @Test
  public void testSmallPqSegment() throws Exception {
    int count = 50;
    List<VectorFloat<?>> subset = new ArrayList<>();
    byte[][] subKeys = new byte[count][];
    for (int i = 0; i < count; i++) {
      subset.add(VTS.createFloatVector(vectors.get(i)));
      subKeys[i] = Bytes.toBytes("small-pq-" + i);
    }
    byte[] payload =
      HnswSegment.build(index("PQ", 8), new ListRandomAccessVectorValues(subset, DIM), subKeys);
    byte[] rowKey = Bytes.toBytes("seg-pq-small");
    try (Table t = connection.getTable(table)) {
      t.put(new Put(rowKey).addColumn(FAMILY, HnswSegment.PAYLOAD_QUALIFIER, payload));
    }
    HnswSegment segment = HnswSegment.open(connection, table, FAMILY, rowKey,
      new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
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
    byte[] rowKey = Bytes.toBytes("seg-corrupt");
    try (Table t = connection.getTable(table)) {
      t.put(new Put(rowKey).addColumn(FAMILY, HnswSegment.PAYLOAD_QUALIFIER,
        Bytes.toBytes("not a segment")));
    }
    try {
      HnswSegment.open(connection, table, FAMILY, rowKey, new HnswOffheapAllocator(1 << 20),
        "COSINE");
      fail("expected IOException");
    } catch (IOException expected) {
    }
  }

  /** Tests serialization and deserialization of delta segments with tombstones. */
  @Test
  public void testDeltaSegmentRoundTripAndTombstones() throws Exception {
    byte[] baseStart = Bytes.toBytes("seg-delta-test-");
    byte[] endKey = Bytes.toBytes("seg-delta-test-z");
    long baseTime = 1000L;
    long deltaTime = 2000L;

    List<VectorFloat<?>> jv =
      vectors.stream().map(VTS::createFloatVector).collect(Collectors.toList());
    byte[] basePayload =
      HnswSegment.build(index("NONE", null), new ListRandomAccessVectorValues(jv, DIM), keys);
    try (Table t = connection.getTable(table)) {
      HnswSegment.write(t, FAMILY, baseStart, endKey, baseTime, basePayload, COUNT);
    }

    int deltaCount = 10;
    List<VectorFloat<?>> deltaVectors = new ArrayList<>();
    byte[][] deltaKeys = new byte[deltaCount][];
    for (int i = 0; i < deltaCount; i++) {
      deltaVectors.add(jv.get(i));
      deltaKeys[i] = Bytes.toBytes("delta-row-" + i);
    }
    byte[] deltaPayload = HnswSegment.build(index("NONE", null),
      new ListRandomAccessVectorValues(deltaVectors, DIM), deltaKeys);
    byte[][] tombstones = new byte[][] { Bytes.toBytes("deleted-row-1"),
      Bytes.toBytes("deleted-row-2"), Bytes.toBytes("deleted-row-3") };

    HnswSegment.Descriptor deltaDesc;
    try (Table t = connection.getTable(table)) {
      deltaDesc = HnswSegment.write(t, FAMILY, baseStart, endKey, deltaTime, deltaPayload,
        deltaCount, baseTime, tombstones);
    }
    assertTrue(deltaDesc.isDelta());
    assertEquals(Long.valueOf(baseTime), deltaDesc.baseTime);

    // Verify segment listing distinguishes bases and deltas without reading payloads
    try (Table t = connection.getTable(table)) {
      List<HnswSegment.Descriptor> listed = HnswSegment.list(t, FAMILY);
      HnswSegment.Descriptor foundBase = null;
      HnswSegment.Descriptor foundDelta = null;
      for (HnswSegment.Descriptor d : listed) {
        if (Arrays.equals(d.startKey, baseStart)) {
          if (d.time == baseTime) {
            foundBase = d;
          } else if (d.time == deltaTime) {
            foundDelta = d;
          }
        }
      }
      assertNotNull("Base segment must be found", foundBase);
      assertFalse("Base segment must not be delta", foundBase.isDelta());
      assertNull(foundBase.baseTime);
      assertEquals(COUNT, foundBase.count);

      assertNotNull("Delta segment must be found", foundDelta);
      assertTrue("Delta segment must be delta", foundDelta.isDelta());
      assertEquals(Long.valueOf(baseTime), foundDelta.baseTime);
      assertEquals(deltaCount, foundDelta.count);
    }

    HnswSegment deltaSegment = HnswSegment.open(connection, table, FAMILY, deltaDesc.rowKey,
      new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
    assertEquals(deltaCount, deltaSegment.size());
    byte[][] readTombs = deltaSegment.getTombstones();
    assertEquals(3, readTombs.length);
    assertArrayEquals(tombstones[0], readTombs[0]);
    assertArrayEquals(tombstones[1], readTombs[1]);
    assertArrayEquals(tombstones[2], readTombs[2]);

    SearchResult result = deltaSegment.search(deltaVectors.get(0), 1, 16, Bits.ALL);
    assertEquals(1, result.getNodes().length);
    assertArrayEquals(deltaKeys[0], deltaSegment.getKey(result.getNodes()[0].node));
  }

  /** Tests behavior of delta segments containing only deletion tombstones. */
  @Test
  public void testDeleteOnlyDeltaSegment() throws Exception {
    byte[] baseStart = Bytes.toBytes("seg-delonly-test-");
    byte[] endKey = Bytes.toBytes("seg-delonly-test-z");
    long baseTime = 5000L;
    long deltaTime = 6000L;

    byte[][] tombstones = new byte[][] { Bytes.toBytes("tomb-a"), Bytes.toBytes("tomb-b") };

    HnswSegment.Descriptor deleteOnlyDesc;
    try (Table t = connection.getTable(table)) {
      deleteOnlyDesc =
        HnswSegment.write(t, FAMILY, baseStart, endKey, deltaTime, null, 0, baseTime, tombstones);
    }
    assertTrue(deleteOnlyDesc.isDelta());
    assertEquals(0, deleteOnlyDesc.count);

    try (Table t = connection.getTable(table)) {
      List<HnswSegment.Descriptor> listed = HnswSegment.list(t, FAMILY);
      HnswSegment.Descriptor found = null;
      for (HnswSegment.Descriptor d : listed) {
        if (Arrays.equals(d.rowKey, deleteOnlyDesc.rowKey)) {
          found = d;
          break;
        }
      }
      assertNotNull(found);
      assertTrue(found.isDelta());
      assertEquals(0, found.count);
      assertEquals(Long.valueOf(baseTime), found.baseTime);
    }

    HnswSegment delSegment = HnswSegment.open(connection, table, FAMILY, deleteOnlyDesc.rowKey,
      new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
    assertEquals(0, delSegment.size());
    byte[][] readTombs = delSegment.getTombstones();
    assertEquals(2, readTombs.length);
    assertArrayEquals(tombstones[0], readTombs[0]);
    assertArrayEquals(tombstones[1], readTombs[1]);

    SearchResult res = delSegment.search(VTS.createFloatVector(new float[DIM]), 5, 16, Bits.ALL);
    assertEquals(0, res.getNodes().length);
  }

}
