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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Verifies the range coverage of {@link HnswSegment} descriptors and the cell layout: anchor rows,
 * qualifiers, identities, definition digests and the listing. The tests also verify the row key
 * order of ordinals, ordinal lookup, payload serialization and the off-heap load.
 */
public class HnswSegmentTest {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIM = 32;
  private static final int COUNT = 300;
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] KEY = Bytes.toBytes("HNSW_INDEX");
  private static final byte[] DEFINITION = HnswSegment.definitionDigest("definition");
  // The encoded name of the region that wrote the segments of the listing tests
  static final byte[] WRITER = Bytes.toBytes("0123456789abcdef0123456789abcdef");
  private static final HnswSegment.Descriptor DESCRIPTOR =
    new HnswSegment.Descriptor(KEY, new byte[0], 100L, new byte[0], COUNT, null, 100L, DEFINITION);

  @Test
  public void testDescriptorCoversAndOverlaps() {
    byte[] start = Bytes.toBytes("b");
    byte[] end = Bytes.toBytes("f");
    long time = 100L;
    HnswSegment.Descriptor d =
      new HnswSegment.Descriptor(KEY, start, time, end, 10, null, time, DEFINITION);

    assertTrue(d.covers(Bytes.toBytes("b"), Bytes.toBytes("f")));
    assertFalse(d.covers(Bytes.toBytes("a"), Bytes.toBytes("f")));
    assertFalse(d.covers(Bytes.toBytes("b"), Bytes.toBytes("g")));

    assertTrue(d.overlaps(Bytes.toBytes("a"), Bytes.toBytes("c")));
    assertTrue(d.overlaps(Bytes.toBytes("e"), Bytes.toBytes("z")));
    assertTrue(d.overlaps(Bytes.toBytes("c"), Bytes.toBytes("d")));
    assertFalse(d.overlaps(Bytes.toBytes("a"), Bytes.toBytes("b")));
    assertFalse(d.overlaps(Bytes.toBytes("f"), Bytes.toBytes("z")));
  }

  /** Adjacent segments cover a range together. A gap or a short end does not cover it. */
  @Test
  public void testCovered() {
    byte[] startA = Bytes.toBytes("a");
    byte[] midM = Bytes.toBytes("m");
    byte[] endZ = Bytes.toBytes("z");

    HnswSegment.Descriptor base1 = segment(startA, 100L, midM);
    HnswSegment.Descriptor base2 = segment(midM, 300L, endZ);
    assertTrue("Two adjacent bases must cover",
      HnswSegment.covered(startA, endZ, Arrays.asList(base2, base1)));
    assertTrue(base1.coveredBy(Arrays.asList(base1)));
    assertFalse("One base must not cover the range of both",
      HnswSegment.covered(startA, endZ, Arrays.asList(base1)));
    HnswSegment.Descriptor gap = segment(Bytes.toBytes("n"), 400L, endZ);
    assertFalse("A gap must not cover",
      HnswSegment.covered(startA, endZ, Arrays.asList(base1, gap)));
    HnswSegment.Descriptor unbounded = segment(startA, 500L, new byte[0]);
    assertTrue(HnswSegment.covered(midM, new byte[0], Arrays.asList(unbounded)));
    assertFalse(HnswSegment.covered(startA, new byte[0], Arrays.asList(base1, base2)));
  }

  private static HnswSegment.Descriptor segment(byte[] start, long time, byte[] end) {
    return new HnswSegment.Descriptor(KEY, start, time, end, 10, null, time, DEFINITION);
  }

  private static HnswSegment.Descriptor delta(byte[] start, long time, byte[] end, long base) {
    return new HnswSegment.Descriptor(KEY, start, time, end, 5, base, time, DEFINITION);
  }

  /** A delta segment never covers a range, alone or with bases. */
  @Test
  public void testCoverageOnlyConsidersBases() {
    byte[] startA = Bytes.toBytes("a");
    byte[] midM = Bytes.toBytes("m");
    byte[] endZ = Bytes.toBytes("z");

    HnswSegment.Descriptor base1 = segment(startA, 100L, midM);
    HnswSegment.Descriptor delta2 = delta(midM, 200L, endZ, 50L);
    HnswSegment.Descriptor base2 = segment(midM, 300L, endZ);

    List<HnswSegment.Descriptor> mixed = Arrays.asList(base1, delta2);
    assertFalse("Delta must not contribute to coverage", HnswSegment.covered(startA, endZ, mixed));

    List<HnswSegment.Descriptor> bases = Arrays.asList(base1, base2);
    assertTrue("Two adjacent bases must cover", HnswSegment.covered(startA, endZ, bases));

    List<HnswSegment.Descriptor> deltaOnly = Collections.singletonList(delta2);
    assertFalse("Delta alone must not cover", HnswSegment.covered(midM, endZ, deltaOnly));
  }

  /**
   * The anchor row of a key range is in the range. It is the start key followed by 0x00, or the
   * start key if the range holds only that key. The range from the empty key to 0x00 holds no rows
   * and has no anchor.
   */
  @Test
  public void testAnchor() {
    byte[] empty = new byte[0];
    byte[] zero = { 0 };
    byte[] a = Bytes.toBytes("a");
    byte[] a0 = { 'a', 0 };
    byte[][][] cases = { { empty, empty, zero }, { empty, a, zero }, { empty, zero, null },
      { empty, { 0, 0 }, zero }, { a, empty, a0 }, { a, Bytes.toBytes("b"), a0 }, { a, a0, a },
      { a, { 'a', 0, 0 }, a0 }, { a, { 'a', 1 }, a0 }, { zero, empty, { 0, 0 } },
      { zero, { 0, 0 }, zero } };
    for (byte[][] c : cases) {
      String what = Bytes.toStringBinary(c[0]) + ", " + Bytes.toStringBinary(c[1]);
      byte[] anchor = HnswSegment.anchor(c[0], c[1]);
      if (c[2] == null) {
        assertNull(what, anchor);
        continue;
      }
      assertArrayEquals(what, c[2], anchor);
      assertTrue(what, anchor.length > 0);
      assertTrue(what, Bytes.compareTo(c[0], anchor) <= 0);
      assertTrue(what, c[1].length == 0 || Bytes.compareTo(anchor, c[1]) < 0);
    }
    try {
      new HnswSegment.Descriptor(KEY, empty, 1L, zero, 0, null, 1L, DEFINITION);
      fail("a segment of a range without an anchor row must not be described");
    } catch (IllegalArgumentException expected) {
    }
    assertArrayEquals(a0, segment(a, 1L, empty).anchor);
    assertArrayEquals(a, segment(a, 1L, a0).anchor);
  }

  /** The qualifier of each tag and identity decodes to its original parts, at any offset. */
  @Test
  public void testQualifierRoundTrip() {
    byte[][] indexKeys = { KEY, Bytes.toBytes("I"), Bytes.toBytes("S.IDX") };
    byte[][] startKeys =
      { new byte[0], Bytes.toBytes("a"), { 'a', 0, 'b' }, { 0, 0, 0, 0 }, { (byte) 0xFF, 0, 5 } };
    long[] times = { 0L, 1L, 1760000000000L, Long.MAX_VALUE };
    for (byte[] indexKey : indexKeys) {
      for (byte[] startKey : startKeys) {
        for (long time : times) {
          byte[] identity = HnswSegment.identity(indexKey, startKey, time);
          assertTrue(Bytes.startsWith(identity, HnswSegment.prefix(indexKey)));
          for (byte tag : HnswSegment.TAGS) {
            byte[] qualifier = HnswSegment.qualifier(identity, tag);
            byte[] padded = Bytes.add(Bytes.toBytes("xx"), qualifier, Bytes.toBytes("yy"));
            HnswSegment.Qualifier q = HnswSegment.parseQualifier(padded, 2, qualifier.length);
            assertNotNull(Bytes.toStringBinary(qualifier), q);
            assertArrayEquals(indexKey, q.indexKey);
            assertArrayEquals(startKey, q.startKey);
            assertEquals(time, q.time);
            assertEquals(tag, q.tag);
            assertArrayEquals(identity, q.identity());
          }
        }
      }
    }
  }

  /** A qualifier that is not a segment cell qualifier does not decode. */
  @Test
  public void testMalformedQualifiers() {
    byte[] identity = HnswSegment.identity(KEY, Bytes.toBytes("start"), 7L);
    byte[] good = HnswSegment.qualifier(identity, HnswSegment.COUNT_TAG);
    byte[] negative = good.clone();
    Bytes.putInt(negative, KEY.length + 1, -1);
    byte[] longer = good.clone();
    Bytes.putInt(longer, KEY.length + 1, 6);
    byte[][] malformed = { new byte[0], KEY, HnswSegment.prefix(KEY), identity,
      Arrays.copyOf(good, good.length - 2), Bytes.add(good, new byte[] { 'N' }),
      HnswSegment.qualifier(identity, (byte) 'X'), HnswSegment.qualifier(identity, (byte) 0),
      Bytes.add(new byte[] { 0 }, Arrays.copyOfRange(good, KEY.length + 1, good.length)), negative,
      longer, Bytes.toBytes("0:VECTOR"), QueryConstants.EMPTY_COLUMN_BYTES };
    for (byte[] q : malformed) {
      assertNull(Bytes.toStringBinary(q), HnswSegment.parseQualifier(q, 0, q.length));
    }
    assertNotNull(HnswSegment.parseQualifier(good, 0, good.length));
  }

  /**
   * The identity names a segment by its index key, start key and time. The index key prefix of the
   * qualifiers selects the cells of only one index. Start keys of different lengths never give the
   * same identity. The identities of one index and start key sort by time.
   */
  @Test
  public void testIdentityOrdering() {
    byte[] a = Bytes.toBytes("a");
    byte[] ab = Bytes.toBytes("ab");
    assertFalse(Bytes.startsWith(HnswSegment.identity(ab, a, 1L), HnswSegment.prefix(a)));
    assertFalse(Bytes.startsWith(HnswSegment.identity(a, a, 1L), HnswSegment.prefix(ab)));
    assertFalse(Bytes.equals(HnswSegment.identity(KEY, a, 1L),
      HnswSegment.identity(KEY, new byte[] { 'a', 0 }, 1L)));
    assertFalse(Bytes.equals(HnswSegment.identity(KEY, new byte[0], 1L),
      HnswSegment.identity(KEY, new byte[] { 0 }, 1L)));
    long[] times = { 0L, 1L, 255L, 256L, 1760000000000L, Long.MAX_VALUE };
    for (int i = 1; i < times.length; i++) {
      assertTrue(Bytes.compareTo(HnswSegment.identity(KEY, ab, times[i - 1]),
        HnswSegment.identity(KEY, ab, times[i])) < 0);
    }
    HnswSegment.Descriptor d = segment(ab, 9L, Bytes.toBytes("b"));
    assertArrayEquals(HnswSegment.identity(KEY, ab, 9L), d.identity);
    assertArrayEquals(KEY, d.indexKey);
    assertArrayEquals(ab, d.startKey);
    assertEquals(9L, d.time);
  }

  /** The definition digest is the 32-byte SHA-256 digest of the definition. */
  @Test
  public void testDefinitionDigest() {
    assertEquals(HnswSegment.DEFINITION_LENGTH, DEFINITION.length);
    assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
      Bytes.toHex(HnswSegment.definitionDigest("abc")));
    assertArrayEquals(DEFINITION, HnswSegment.definitionDigest("definition"));
    assertFalse(Bytes.equals(DEFINITION, HnswSegment.definitionDigest("definition2")));
  }

  /**
   * A listing returns the well-formed segments at their anchor rows, with the region that wrote
   * them, and never reads a payload cell or a tombstones cell. It tells deltas from bases by their
   * base time. It skips cells whose qualifiers do not decode. It also skips a segment with a
   * missing cell or a cell of the wrong length. A segment without its writer is malformed. It skips
   * a segment whose row is not the anchor of its range.
   */
  @Test
  public void testListing() {
    byte[] start = Bytes.toBytes("m");
    byte[] end = Bytes.toBytes("t");
    byte[] anchor = HnswSegment.anchor(start, end);
    HnswSegment.Descriptor good =
      new HnswSegment.Descriptor(KEY, start, 10L, end, 5, null, 8L, DEFINITION);
    HnswSegment.Descriptor empty =
      new HnswSegment.Descriptor(KEY, start, 11L, end, 0, null, 11L, DEFINITION);
    HnswSegment.Descriptor delta =
      new HnswSegment.Descriptor(KEY, start, 16L, end, 2, 10L, 16L, DEFINITION);
    List<KeyValue> cells = new ArrayList<>();
    cells.addAll(cells(anchor, good, true));
    cells.addAll(cells(anchor, empty, false));
    cells.addAll(cells(anchor, delta, true));
    cells.add(new KeyValue(anchor, FAMILY,
      HnswSegment.qualifier(delta.identity, HnswSegment.TOMBSTONES_TAG), new byte[0]));
    // A delta with a base time of the wrong length
    HnswSegment.Descriptor shortBase = delta(start, 17L, end, 10L);
    for (KeyValue kv : cells(anchor, shortBase, true)) {
      boolean base = kv.getQualifierArray()[kv.getQualifierOffset() + kv.getQualifierLength() - 1]
          == HnswSegment.BASE_TIME_TAG;
      cells.add(base
        ? new KeyValue(anchor, FAMILY,
          HnswSegment.qualifier(shortBase.identity, HnswSegment.BASE_TIME_TAG), new byte[4])
        : kv);
    }
    // A segment at a row that is not the anchor of its range
    cells.addAll(cells(Bytes.toBytes("m1"), segment(start, 12L, end), true));
    // A segment without its definition digest, and one with a short count
    List<KeyValue> noDigest = cells(anchor, segment(start, 13L, end), true);
    noDigest
      .removeIf(kv -> kv.getQualifierArray()[kv.getQualifierOffset() + kv.getQualifierLength() - 1]
          == HnswSegment.DEFINITION_TAG);
    cells.addAll(noDigest);
    HnswSegment.Descriptor shortCount = segment(start, 14L, end);
    for (KeyValue kv : cells(anchor, shortCount, true)) {
      boolean count = kv.getQualifierArray()[kv.getQualifierOffset() + kv.getQualifierLength() - 1]
          == HnswSegment.COUNT_TAG;
      cells.add(count
        ? new KeyValue(anchor, FAMILY,
          HnswSegment.qualifier(shortCount.identity, HnswSegment.COUNT_TAG), new byte[2])
        : kv);
    }
    // A segment without its writer
    List<KeyValue> noWriter = cells(anchor, segment(start, 18L, end), true);
    noWriter
      .removeIf(kv -> kv.getQualifierArray()[kv.getQualifierOffset() + kv.getQualifierLength() - 1]
          == HnswSegment.WRITER_TAG);
    cells.addAll(noWriter);
    // Cells whose qualifiers do not decode
    cells.add(new KeyValue(anchor, FAMILY, Bytes.toBytes("junk"), Bytes.toBytes(1)));
    cells.add(new KeyValue(anchor, FAMILY,
      HnswSegment.qualifier(HnswSegment.identity(KEY, start, 15L), (byte) 'X'), Bytes.toBytes(1)));

    HnswSegment.Listing listing = new HnswSegment.Listing();
    for (KeyValue kv : cells) {
      listing.add(kv);
    }
    List<HnswSegment.Descriptor> listed = listing.descriptors();
    assertEquals(listed.toString(), 3, listed.size());
    assertDescriptor(good, listed.get(0));
    assertDescriptor(empty, listed.get(1));
    assertDescriptor(delta, listed.get(2));
    assertFalse(listed.get(0).isDelta());
    assertTrue(listed.get(2).isDelta());
    for (HnswSegment.Descriptor d : listed) {
      assertArrayEquals(WRITER, d.writer);
    }
  }

  // The cells that a region store writes for a segment. The payload cell holds an empty value, and
  // the W cell holds WRITER.
  static List<KeyValue> cells(byte[] row, HnswSegment.Descriptor d, boolean payload) {
    List<KeyValue> cells = new ArrayList<>();
    cells.add(new KeyValue(row, FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG),
      d.endKey));
    cells.add(new KeyValue(row, FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG),
      Bytes.toBytes(d.count)));
    cells.add(new KeyValue(row, FAMILY,
      HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG), Bytes.toBytes(d.replayFrom)));
    cells.add(new KeyValue(row, FAMILY,
      HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG), d.definition));
    cells.add(
      new KeyValue(row, FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG), WRITER));
    if (d.isDelta()) {
      cells.add(new KeyValue(row, FAMILY,
        HnswSegment.qualifier(d.identity, HnswSegment.BASE_TIME_TAG), Bytes.toBytes(d.baseTime)));
    }
    if (payload) {
      cells.add(new KeyValue(row, FAMILY,
        HnswSegment.qualifier(d.identity, HnswSegment.PAYLOAD_TAG), new byte[0]));
    }
    return cells;
  }

  static void assertDescriptor(HnswSegment.Descriptor expected, HnswSegment.Descriptor actual) {
    assertArrayEquals(expected.identity, actual.identity);
    assertArrayEquals(expected.indexKey, actual.indexKey);
    assertArrayEquals(expected.startKey, actual.startKey);
    assertArrayEquals(expected.endKey, actual.endKey);
    assertEquals(expected.time, actual.time);
    assertEquals(expected.count, actual.count);
    assertEquals(expected.baseTime, actual.baseTime);
    assertEquals(expected.replayFrom, actual.replayFrom);
    assertArrayEquals(expected.anchor, actual.anchor);
    assertArrayEquals(expected.definition, actual.definition);
  }

  /** HNSW maps L2 and COSINE. It refuses INNER_PRODUCT, because DOT_PRODUCT cannot serve it. */
  @Test
  public void testSimilarityFunction() {
    assertEquals(VectorSimilarityFunction.EUCLIDEAN, HnswSegment.similarityFunction("L2"));
    assertEquals(VectorSimilarityFunction.COSINE, HnswSegment.similarityFunction("COSINE"));
    try {
      HnswSegment.similarityFunction("INNER_PRODUCT");
      fail("HNSW must not map INNER_PRODUCT to DOT_PRODUCT");
    } catch (IllegalArgumentException expected) {
    }
  }

  /** The ceiling is the first ordinal whose key is equal to or greater than the search key. */
  @Test
  public void testCeiling() {
    byte[][] keys =
      { Bytes.toBytes("b"), Bytes.toBytes("d"), Bytes.toBytes("d0"), Bytes.toBytes("f") };
    assertEquals(0, HnswSegment.ceiling(keys, new byte[0]));
    assertEquals(0, HnswSegment.ceiling(keys, Bytes.toBytes("a")));
    assertEquals(0, HnswSegment.ceiling(keys, Bytes.toBytes("b")));
    assertEquals(1, HnswSegment.ceiling(keys, Bytes.toBytes("c")));
    assertEquals(1, HnswSegment.ceiling(keys, Bytes.toBytes("d")));
    assertEquals(2, HnswSegment.ceiling(keys, Bytes.toBytes("d\0")));
    assertEquals(3, HnswSegment.ceiling(keys, Bytes.toBytes("e")));
    assertEquals(4, HnswSegment.ceiling(keys, Bytes.toBytes("g")));
    assertEquals(0, HnswSegment.ceiling(new byte[0][], Bytes.toBytes("a")));
  }

  private static PTable.VectorIndex index(String quantization) {
    return index(quantization, "COSINE");
  }

  private static PTable.VectorIndex index(String quantization, String metric) {
    return index(quantization, metric, 1.2);
  }

  private static PTable.VectorIndex index(String quantization, String metric, double alpha) {
    return new PTable.VectorIndex.Builder().setAlgorithm("HNSW").setDistanceMetric(metric)
      .setDimension(DIM).setHnswM(16).setHnswEfConstruction(100).setHnswAlpha(alpha)
      .setQuantizationType(quantization).setPqSegments("PQ".equals(quantization) ? 8 : null)
      .build();
  }

  private static Object field(GraphIndexBuilder builder, String name) throws Exception {
    Field field = GraphIndexBuilder.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(builder);
  }

  /**
   * The graph builders of segments and of the mutable graph take the M, ef_construction and alpha
   * of the index. JVector takes the neighbor overflow before alpha. The default alpha of 1.2 is
   * equal to the neighbor overflow, so at that value a swap of the two arguments is not visible.
   * For this reason, the test uses other values of alpha. JVector has no getters for these
   * parameters, so the test reads its fields.
   */
  @Test
  public void testGraphBuilderParameters() throws Exception {
    for (double alpha : new double[] { 1.0, 2.0 }) {
      for (boolean refine : new boolean[] { true, false }) {
        try (GraphIndexBuilder builder = HnswSegment.graphBuilder(index("NONE", "COSINE", alpha),
          vectors(), VectorSimilarityFunction.COSINE, refine)) {
          String what = "alpha=" + alpha + ", refine=" + refine;
          assertEquals(what, (float) alpha, (float) field(builder, "alpha"), 0f);
          assertEquals(what, HnswSegment.NEIGHBOR_OVERFLOW,
            (float) field(builder, "neighborOverflow"), 0f);
          assertEquals(what, 100, (int) field(builder, "beamWidth"));
          assertEquals(what, 16, builder.getGraph().maxDegree());
          assertTrue(what, (boolean) field(builder, "addHierarchy"));
          assertEquals(what, refine, field(builder, "refineFinalGraph"));
        }
      }
    }
  }

  /**
   * A search for more rows than its beam width uses a beam of the row count, because JVector
   * refuses a rerank count below the row count. An inspection that asks for more rows than the
   * default beam width makes such a search.
   */
  @Test
  public void testSearchForMoreRowsThanBeamWidth() throws Exception {
    RandomAccessVectorValues vectors = vectors();
    for (String quantization : new String[] { "NONE", "SQ8", "PQ" }) {
      HnswSegment segment =
        HnswSegment.of(null, DESCRIPTOR, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE",
          HnswSegment.build(index(quantization), vectors, keys()), new byte[0][]);
      try {
        assertEquals(quantization, 100,
          segment.search(vectors.getVector(0), 100, 16, Bits.ALL).getNodes().length);
      } finally {
        segment.close();
      }
    }
  }

  private static RandomAccessVectorValues vectors() {
    Random random = new Random(5);
    List<VectorFloat<?>> vectors = new ArrayList<>(COUNT);
    for (int i = 0; i < COUNT; i++) {
      float[] v = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        v[d] = (float) random.nextGaussian();
      }
      vectors.add(VTS.createFloatVector(v));
    }
    return new ListRandomAccessVectorValues(vectors, DIM);
  }

  // Row keys out of order, so the build must sort its input
  private static byte[][] keys() {
    byte[][] keys = new byte[COUNT][];
    for (int i = 0; i < COUNT; i++) {
      keys[i] = Bytes.toBytes("row-" + ((i * 7) % COUNT));
    }
    return keys;
  }

  // Decodes the ordinal-to-row-key mapping from the segment trailer
  private static byte[][] mapping(byte[] payload) {
    ByteBuffer trailer = ByteBuffer.wrap(payload);
    int length = trailer.getInt(payload.length - 8);
    trailer.position(payload.length - 8 - length);
    byte[][] keys = new byte[trailer.getInt()][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = new byte[trailer.getInt()];
      trailer.get(keys[i]);
    }
    return keys;
  }

  /** A segment gives contiguous ordinals in row key order, whatever the input order. */
  @Test
  public void testOrdinalsFollowKeyOrder() throws Exception {
    byte[][] keys = keys();
    byte[][] mapped = mapping(HnswSegment.build(index("NONE"), vectors(), keys));
    assertEquals(COUNT, mapped.length);
    Set<String> expected = new HashSet<>();
    Set<String> actual = new HashSet<>();
    for (int i = 0; i < COUNT; i++) {
      expected.add(Bytes.toString(keys[i]));
      actual.add(Bytes.toString(mapped[i]));
      if (i > 0) {
        assertTrue(Bytes.compareTo(mapped[i - 1], mapped[i]) < 0);
      }
    }
    assertEquals(expected, actual);
  }

  /**
   * For each codec, the payload array ends with the trailer, so it holds no unused bytes. A segment
   * loads from this array.
   */
  @Test
  public void testPayloadIsExact() throws Exception {
    RandomAccessVectorValues vectors = vectors();
    byte[][] keys = keys();
    for (String quantization : new String[] { "NONE", "SQ8", "PQ" }) {
      byte[] payload = HnswSegment.build(index(quantization), vectors, keys);
      assertEquals(quantization, 0x484E5357,
        Bytes.toInt(payload, payload.length - Bytes.SIZEOF_INT));
      HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
      HnswSegment segment =
        HnswSegment.of(null, DESCRIPTOR, allocator, "COSINE", payload, new byte[0][]);
      assertEquals(COUNT, segment.size());
      assertEquals(quantization, segment.ordinalOf(new ImmutableBytesPtr(keys[0])),
        segment.search(vectors.getVector(0), 1, 32, Bits.ALL).getNodes()[0].node);
      segment.close();
      assertEquals(0, allocator.getAllocatedBytes());
    }
  }

  /**
   * Only an L2 index centers its PQ codebooks. JVector scores a COSINE query against the centered
   * vectors, which changes the order of cosine similarities.
   */
  @Test
  public void testPqCentering() {
    RandomAccessVectorValues vectors = vectors();
    assertTrue(
      HnswSegment.trainPq(vectors, index("PQ", "L2")).toString().contains("centered=true"));
    assertTrue(
      HnswSegment.trainPq(vectors, index("PQ", "COSINE")).toString().contains("centered=false"));
  }

  /** Ordinal lookup finds each key, and reads only the offset and length of the pointer. */
  @Test
  public void testOrdinalOf() throws Exception {
    byte[] payload = HnswSegment.build(index("NONE"), vectors(), keys());
    HnswSegment segment = HnswSegment.of(null, DESCRIPTOR, new HnswOffheapAllocator(Long.MAX_VALUE),
      "COSINE", payload, new byte[0][]);
    for (int i = 0; i < segment.size(); i++) {
      byte[] key = segment.getKey(i);
      byte[] padded = new byte[key.length + 4];
      Arrays.fill(padded, (byte) 0xFF);
      System.arraycopy(key, 0, padded, 2, key.length);
      assertEquals(i, segment.ordinalOf(new ImmutableBytesPtr(padded, 2, key.length)));
    }
    assertEquals(-1, segment.ordinalOf(new ImmutableBytesPtr(new byte[0])));
    assertEquals(-1, segment.ordinalOf(new ImmutableBytesPtr(Bytes.toBytes("row-"))));
    assertEquals(-1, segment.ordinalOf(new ImmutableBytesPtr(Bytes.toBytes("row-10x"))));
    assertEquals(-1, segment.ordinalOf(new ImmutableBytesPtr(Bytes.toBytes("zzz"))));
    segment.close();
  }

  /**
   * A segment that the server writes loads from the payload in memory. After eviction, a search
   * reads the payload again from the segment store. After close, a search fails and the budget
   * counts no memory.
   */
  @Test
  public void testMaterializeReloadAndClose() throws Exception {
    byte[][] keys = keys();
    byte[] payload = HnswSegment.build(index("NONE"), vectors(), keys);
    HnswSegmentStore store = store(payload);
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
    HnswSegment segment =
      HnswSegment.of(store, DESCRIPTOR, allocator, "COSINE", payload, new byte[0][]);
    verifyNoInteractions(store);
    long charge = allocator.getAllocatedBytes();
    assertTrue(charge > 0);
    VectorFloat<?> query = vectors().getVector(0);
    int nearest = segment.ordinalOf(new ImmutableBytesPtr(keys[0]));
    assertEquals(nearest, segment.search(query, 1, 16, Bits.ALL).getNodes()[0].node);

    allocator.evictAll();
    assertEquals(0, allocator.getAllocatedBytes());
    assertEquals(nearest, segment.search(query, 1, 16, Bits.ALL).getNodes()[0].node);
    verify(store).readPayload(DESCRIPTOR);
    assertEquals(charge, allocator.getAllocatedBytes());

    segment.close();
    assertEquals(0, allocator.getAllocatedBytes());
    try {
      segment.search(query, 1, 16, Bits.ALL);
      fail("a closed segment must not reload");
    } catch (HnswSegment.NotFoundException expected) {
    }
    assertEquals(0, allocator.getAllocatedBytes());
  }

  /**
   * A graph larger than the off-heap budget does not stop the open of its segment, because writes
   * need only the row keys. A search fails. A later search also fails, and does not read the
   * payload again.
   */
  @Test
  public void testOverBudgetGraphIsReadOnce() throws Exception {
    byte[][] keys = keys();
    byte[] payload = HnswSegment.build(index("NONE"), vectors(), keys);
    HnswSegmentStore store = store(payload);
    HnswSegment segment =
      HnswSegment.open(store, DESCRIPTOR, new HnswOffheapAllocator(64), "COSINE");
    assertEquals(keys.length, segment.size());
    VectorFloat<?> query = vectors().getVector(0);
    for (int i = 0; i < 2; i++) {
      try {
        segment.search(query, 1, 16, Bits.ALL);
        fail("a graph larger than the off-heap budget must not load");
      } catch (HnswSegment.TooLargeException expected) {
      }
    }
    verify(store).readPayload(DESCRIPTOR);
    segment.close();
  }

  /**
   * A graph that JVector cannot read fails to load as a corrupt segment. It does not fail with the
   * unchecked exception of JVector, because a RegionServer hook must not throw that exception.
   */
  @Test
  public void testCorruptGraph() throws Exception {
    byte[] payload = HnswSegment.build(index("NONE"), vectors(), keys());
    // The graph is at the start of the payload. Its header holds the format version after the
    // JVector magic.
    Bytes.putInt(payload, Bytes.SIZEOF_INT, Integer.MAX_VALUE);
    try {
      HnswSegment.of(null, DESCRIPTOR, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE", payload,
        new byte[0][]);
      fail("a segment with a corrupt graph must not load");
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("Corrupt HNSW segment"));
    }
  }

  /**
   * HBase returns an empty payload for a MOB reference whose file is lost. The payload framing
   * check rejects it, so an empty payload never loads as a graph.
   */
  @Test
  public void testEmptyPayloadRejected() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
    try {
      HnswSegment.of(null, DESCRIPTOR, allocator, "COSINE", new byte[0], new byte[0][]);
      fail("an empty payload must not load");
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("Corrupt HNSW segment"));
    }
    try {
      HnswSegment.open(store(new byte[0]), DESCRIPTOR, allocator, "COSINE");
      fail("an empty payload must not load");
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("Corrupt HNSW segment"));
    }
    assertEquals(0, allocator.getAllocatedBytes());
  }

  /** A segment that holds no vectors has no payload, and opens without a read. */
  @Test
  public void testOpenWithoutVectors() throws Exception {
    HnswSegmentStore store = mock(HnswSegmentStore.class);
    HnswSegment.Descriptor d =
      new HnswSegment.Descriptor(KEY, new byte[0], 100L, new byte[0], 0, null, 100L, DEFINITION);
    HnswSegment segment =
      HnswSegment.open(store, d, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
    assertEquals(0, segment.size());
    assertEquals(0, segment.search(vectors().getVector(0), 5, 16, Bits.ALL).getNodes().length);
    verifyNoInteractions(store);
    segment.close();
  }

  /**
   * A delta segment that holds only deletes has no payload. Its open reads only its tombstones.
   */
  @Test
  public void testOpenDeleteOnlyDelta() throws Exception {
    HnswSegment.Descriptor d =
      new HnswSegment.Descriptor(KEY, new byte[0], 200L, new byte[0], 0, 100L, 200L, DEFINITION);
    byte[][] tombstones = { Bytes.toBytes("a"), Bytes.toBytes("b") };
    HnswSegmentStore store = mock(HnswSegmentStore.class);
    when(store.readTombstones(d)).thenReturn(
      new KeyValue(d.anchor, FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.TOMBSTONES_TAG),
        HnswSegment.encodeKeys(tombstones)));
    HnswSegment segment =
      HnswSegment.open(store, d, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
    assertEquals(0, segment.size());
    assertEquals(2, segment.getTombstones().length);
    assertArrayEquals(tombstones[0], segment.getTombstones()[0]);
    assertArrayEquals(tombstones[1], segment.getTombstones()[1]);
    verify(store).readTombstones(d);
    verifyNoMoreInteractions(store);
    segment.close();
  }

  /**
   * A delta segment with corrupt tombstones fails to open with a corrupt segment error. Thus the
   * region rebuilds its segments. HBase returns an empty value for a tombstones cell whose MOB file
   * is lost, and this value is also corrupt. The open does not read the payload.
   */
  @Test
  public void testCorruptTombstones() throws Exception {
    HnswSegment.Descriptor d =
      new HnswSegment.Descriptor(KEY, new byte[0], 200L, new byte[0], 3, 100L, 200L, DEFINITION);
    byte[] qualifier = HnswSegment.qualifier(d.identity, HnswSegment.TOMBSTONES_TAG);
    // Two corrupt values: a count of five with no keys, and the empty value of a lost MOB file
    for (byte[] value : new byte[][] { Bytes.toBytes(5), new byte[0] }) {
      HnswSegmentStore store = mock(HnswSegmentStore.class);
      when(store.readTombstones(d)).thenReturn(new KeyValue(d.anchor, FAMILY, qualifier, value));
      try {
        HnswSegment.open(store, d, new HnswOffheapAllocator(Long.MAX_VALUE), "COSINE");
        fail("a segment with corrupt tombstones must not open");
      } catch (DoNotRetryIOException e) {
        assertTrue(e.getMessage(), e.getMessage().startsWith("Corrupt HNSW segment"));
      }
      verify(store).readTombstones(d);
      verifyNoMoreInteractions(store);
    }
  }

  // A mock segment store whose payload cell for DESCRIPTOR holds the given value
  private static HnswSegmentStore store(byte[] payload) throws Exception {
    HnswSegmentStore store = mock(HnswSegmentStore.class);
    when(store.readPayload(DESCRIPTOR)).thenReturn(new KeyValue(DESCRIPTOR.anchor, FAMILY,
      HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.PAYLOAD_TAG), payload));
    return store;
  }
}
