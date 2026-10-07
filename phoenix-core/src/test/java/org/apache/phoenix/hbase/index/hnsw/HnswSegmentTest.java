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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.disk.feature.NVQ;
import io.github.jbellis.jvector.quantization.NVQuantization;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Verifies {@link HnswSegment} descriptor range coverage, key order normalization, ordinal ceiling
 * resolution, and serialization buffer size estimation.
 */
public class HnswSegmentTest {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIM = 32;
  private static final int COUNT = 300;

  @Test
  public void testDescriptorCoversAndOverlaps() {
    byte[] start = Bytes.toBytes("b");
    byte[] end = Bytes.toBytes("f");
    long time = 100L;
    byte[] rowKey = Bytes.add(start, Bytes.toBytes(time));
    HnswSegment.Descriptor d = new HnswSegment.Descriptor(rowKey, end, 10, null);

    assertTrue(d.covers(Bytes.toBytes("b"), Bytes.toBytes("f")));
    assertFalse(d.covers(Bytes.toBytes("a"), Bytes.toBytes("f")));
    assertFalse(d.covers(Bytes.toBytes("b"), Bytes.toBytes("g")));

    assertTrue(d.overlaps(Bytes.toBytes("a"), Bytes.toBytes("c")));
    assertTrue(d.overlaps(Bytes.toBytes("e"), Bytes.toBytes("z")));
    assertTrue(d.overlaps(Bytes.toBytes("c"), Bytes.toBytes("d")));
    assertFalse(d.overlaps(Bytes.toBytes("a"), Bytes.toBytes("b")));
    assertFalse(d.overlaps(Bytes.toBytes("f"), Bytes.toBytes("z")));
  }

  @Test
  public void testCoverageOnlyConsidersBases() {
    byte[] startA = Bytes.toBytes("a");
    byte[] midM = Bytes.toBytes("m");
    byte[] endZ = Bytes.toBytes("z");

    HnswSegment.Descriptor base1 =
      new HnswSegment.Descriptor(Bytes.add(startA, Bytes.toBytes(100L)), midM, 10, null);
    HnswSegment.Descriptor delta2 =
      new HnswSegment.Descriptor(Bytes.add(midM, Bytes.toBytes(200L)), endZ, 5, 50L);
    HnswSegment.Descriptor base2 =
      new HnswSegment.Descriptor(Bytes.add(midM, Bytes.toBytes(300L)), endZ, 20, null);

    List<HnswSegment.Descriptor> mixed = Arrays.asList(base1, delta2);
    assertFalse("Delta must not contribute to coverage", HnswSegment.covered(startA, endZ, mixed));

    List<HnswSegment.Descriptor> bases = Arrays.asList(base1, base2);
    assertTrue("Two adjacent bases must cover", HnswSegment.covered(startA, endZ, bases));

    List<HnswSegment.Descriptor> deltaOnly = Collections.singletonList(delta2);
    assertFalse("Delta alone must not cover", HnswSegment.covered(midM, endZ, deltaOnly));
  }

  /**
   * Verify binary search ceiling finds the first ordinal with key greater than or equal to search
   * key.
   */
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
    return new PTable.VectorIndex.Builder().setAlgorithm("HNSW").setDistanceMetric("COSINE")
      .setDimension(DIM).setHnswM(16).setHnswEfConstruction(100).setHnswAlpha(1.2)
      .setQuantizationType(quantization).setPqSegments("PQ".equals(quantization) ? 8 : null)
      .build();
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

  // Non-sequential row keys used to verify input reordering during segment build
  private static byte[][] keys() {
    byte[][] keys = new byte[COUNT][];
    for (int i = 0; i < COUNT; i++) {
      keys[i] = Bytes.toBytes("row-" + ((i * 7) % COUNT));
    }
    return keys;
  }

  // Extracts the ordinal-to-rowkey mapping encoded in the segment trailer
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

  /**
   * Verify built segments assign contiguous ordinals matching sorted row key order regardless of
   * input order.
   */
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
   * Verify serialized size estimation bounds the actual segment payload across supported
   * quantization formats without excessive memory overallocation.
   */
  @Test
  public void testSizeEstimate() throws Exception {
    RandomAccessVectorValues vectors = vectors();
    for (String quantization : new String[] { "NONE", "SQ8", "PQ" }) {
      PTable.VectorIndex vi = index(quantization);
      byte[] payload = HnswSegment.build(vi, vectors, keys());
      int trailer = Bytes.toInt(payload, payload.length - 8) + 8;
      try (GraphIndexBuilder builder = new GraphIndexBuilder(vectors,
        VectorSimilarityFunction.COSINE, 16, 100, 1.2f, 1.2f, true, true)) {
        ImmutableGraphIndex graph = builder.build(vectors);
        List<Feature> features = new ArrayList<>();
        if ("NONE".equals(quantization)) {
          features.add(new InlineVectors(DIM));
        } else {
          features.add(new NVQ(NVQuantization.compute(vectors, 1)));
          if ("PQ".equals(quantization)) {
            features.add(
              new FusedPQ(graph.maxDegree(), ProductQuantization.compute(vectors, 8, 256, true)));
          }
        }
        int estimate = HnswSegment.estimateSize(graph, features, trailer);
        assertTrue(quantization + ": estimate " + estimate + " < payload " + payload.length,
          estimate >= payload.length);
        assertTrue(quantization + ": estimate " + estimate + " > 1.5 x payload " + payload.length,
          estimate <= payload.length * 3L / 2);
      }
    }
  }
}
