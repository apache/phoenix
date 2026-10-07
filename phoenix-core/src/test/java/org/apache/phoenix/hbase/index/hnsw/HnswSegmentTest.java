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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/** Unit tests for {@link HnswSegment} descriptor range coverage and overlap calculations. */
public class HnswSegmentTest {

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
}
