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
package org.apache.phoenix.mapreduce.index.fsck.hnsw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.github.jbellis.jvector.disk.ByteBufferReader;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/** Unit tests for standalone HNSW tooling helper methods. */
public class HnswIndexFsckProviderTest {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIM = 8;
  private static final int COUNT = 200;

  private static OnDiskGraphIndex load(byte[] payload) {
    int graphLength = payload.length - 8 - Bytes.toInt(payload, payload.length - 8);
    ByteBuffer graph = ByteBuffer.wrap(payload, 0, graphLength).slice();
    return OnDiskGraphIndex.load(() -> new ByteBufferReader(graph.duplicate()));
  }

  private static int indexOf(byte[] haystack, byte[] needle) {
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      if (Bytes.equals(haystack, i, needle.length, needle, 0, needle.length)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Verifies node reachability calculation from graph entry node, including behavior when level-0
   * edges are severed.
   */
  @Test
  public void testReachable() throws Exception {
    Random random = new Random(3);
    List<VectorFloat<?>> vectors = new ArrayList<>();
    byte[][] keys = new byte[COUNT][];
    for (int i = 0; i < COUNT; i++) {
      float[] v = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        v[d] = (float) random.nextGaussian();
      }
      vectors.add(VTS.createFloatVector(v));
      keys[i] = Bytes.toBytes(String.format("k%04d", i));
    }
    PTable.VectorIndex vi = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
      .setDistanceMetric("L2").setDimension(DIM).setHnswM(8).setHnswEfConstruction(64)
      .setHnswAlpha(1.2).setQuantizationType("NONE").build();
    byte[] payload = HnswSegment.build(vi, new ListRandomAccessVectorValues(vectors, DIM), keys);
    OnDiskGraphIndex graph = load(payload);
    // Parallel construction allows a small tolerance of nodes without inbound edges
    int built = HnswIndexFsckProvider.reachable(graph).cardinality();
    assertTrue(built + " of " + COUNT, built >= COUNT * 0.95);

    // Level-0 node record layout: [ordinal][vector][degree][neighbors]
    byte[] first = Bytes.toBytes(0);
    for (int d = 0; d < DIM; d++) {
      first = Bytes.add(first, Bytes.toBytes(vectors.get(0).get(d)));
    }
    int base = indexOf(payload, first);
    assertTrue(base >= 0);
    int record = Bytes.SIZEOF_INT * (2 + DIM + graph.getDegree(0));
    for (int node = 0; node < COUNT; node++) {
      int offset = base + node * record;
      assertEquals(node, Bytes.toInt(payload, offset));
      Bytes.putInt(payload, offset + Bytes.SIZEOF_INT * (1 + DIM), 0);
    }
    int upper = 0;
    for (int level = 1; level <= graph.getMaxLevel(); level++) {
      upper = Math.max(upper, graph.size(level));
    }
    int reachable = HnswIndexFsckProvider.reachable(load(payload)).cardinality();
    assertTrue(reachable + " of " + upper, reachable <= upper && reachable < built);
  }

  @Test
  public void testParseVector() {
    assertArrayEquals(new float[] { 1f, -2.5f, 3f },
      HnswIndexInspector.parseVector(" [1, -2.5,3] "), 0f);
    assertArrayEquals(new float[] { 1f, 2f }, HnswIndexInspector.parseVector("[1 2]"), 0f);
    try {
      HnswIndexInspector.parseVector("1,2");
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testArgs() {
    HnswIndexInspector.Args args =
      new HnswIndexInspector.Args(Arrays.asList("a", "k=5", "nodes", "b=1", "time=12"));
    assertEquals(Arrays.asList("a", "b=1"), args.positional);
    assertEquals(5, args.getInt("k", 10));
    assertEquals(10, args.getInt("ef", 10));
    assertEquals(Long.valueOf(12), args.getLong("time"));
    assertTrue(args.has("nodes"));
  }
}
