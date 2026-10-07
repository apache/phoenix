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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.junit.Test;

/**
 * Unit tests for {@link HnswIndexManager} segment stack discovery, replay, and retirement logic.
 */
public class HnswIndexManagerTest {
  private static final byte[] START = Bytes.toBytes("a");
  private static final byte[] END = Bytes.toBytes("m");

  private static HnswSegment.Descriptor baseDescriptor(long time, int count) {
    return new HnswSegment.Descriptor(Bytes.add(START, Bytes.toBytes(time)), END, count, null);
  }

  private static HnswSegment.Descriptor deltaDescriptor(long time, long baseTime, int count) {
    return new HnswSegment.Descriptor(Bytes.add(START, Bytes.toBytes(time)), END, count, baseTime);
  }

  @Test
  public void testExactBase() {
    HnswSegment.Descriptor base = baseDescriptor(100L, 1000);
    HnswSegment.Descriptor delta1 = deltaDescriptor(110L, 100L, 50);
    HnswSegment.Descriptor delta2 = deltaDescriptor(120L, 100L, 50);

    assertNull(HnswIndexManager.exactBase(Collections.emptyList(), START, END));
    assertEquals(base, HnswIndexManager.exactBase(Collections.singletonList(base), START, END));
    assertEquals(base, HnswIndexManager.exactBase(Arrays.asList(base, delta1, delta2), START, END));
    assertNull(HnswIndexManager.exactBase(Collections.singletonList(delta1), START, END));

    HnswSegment.Descriptor base2 = baseDescriptor(150L, 500);
    assertNull(HnswIndexManager.exactBase(Arrays.asList(base, base2), START, END));

    HnswSegment.Descriptor rogueDelta = deltaDescriptor(130L, 999L, 50);
    assertNull(HnswIndexManager.exactBase(Arrays.asList(base, rogueDelta), START, END));

    byte[] otherEnd = Bytes.toBytes("z");
    byte[] parentKey = Bytes.add(START, Bytes.toBytes(100L));
    HnswSegment.Descriptor parent = new HnswSegment.Descriptor(parentKey, otherEnd, 1000, null);
    assertNull(HnswIndexManager.exactBase(Collections.singletonList(parent), START, END));
  }

  @Test
  public void testCurrentSegmentsOnlyBasesCoverAndDeltasOpenedWithBase() {
    HnswSegment.Descriptor base1 = baseDescriptor(100L, 1000);
    HnswSegment.Descriptor delta1_1 = deltaDescriptor(110L, 100L, 50);
    HnswSegment.Descriptor delta1_2 = deltaDescriptor(120L, 100L, 50);

    List<HnswSegment.Descriptor> current =
      HnswIndexManager.currentSegments(Arrays.asList(base1, delta1_1, delta1_2), START, END);
    assertEquals(3, current.size());
    assertEquals(delta1_2, current.get(0));
    assertEquals(delta1_1, current.get(1));
    assertEquals(base1, current.get(2));

    HnswSegment.Descriptor base2 = baseDescriptor(200L, 2000);
    HnswSegment.Descriptor delta2_1 = deltaDescriptor(210L, 200L, 30);
    current = HnswIndexManager
      .currentSegments(Arrays.asList(base1, delta1_1, delta1_2, base2, delta2_1), START, END);
    assertEquals(2, current.size());
    assertEquals(delta2_1, current.get(0));
    assertEquals(base2, current.get(1));

    HnswSegment.Descriptor orphanDelta = deltaDescriptor(300L, 999L, 10);
    current =
      HnswIndexManager.currentSegments(Arrays.asList(base2, delta2_1, orphanDelta), START, END);
    assertEquals(2, current.size());
    assertEquals(delta2_1, current.get(0));
    assertEquals(base2, current.get(1));

    HnswSegment.Descriptor baseA = baseDescriptor(100L, 100);
    HnswSegment.Descriptor deltaOther =
      new HnswSegment.Descriptor(Bytes.add(START, Bytes.toBytes(150L)), END, 50, 50L);
    current = HnswIndexManager.currentSegments(Arrays.asList(baseA, deltaOther), START, END);
    assertEquals(1, current.size());
    assertEquals(baseA, current.get(0));
  }

  @Test
  public void testCurrentSegmentsSplitParentAndDaughterBases() {
    byte[] parentStart = Bytes.toBytes("a");
    byte[] parentEnd = Bytes.toBytes("z");
    byte[] midKey = Bytes.toBytes("m");

    HnswSegment.Descriptor parentBase =
      new HnswSegment.Descriptor(Bytes.add(parentStart, Bytes.toBytes(50L)), parentEnd, 1000, null);
    HnswSegment.Descriptor parentDelta =
      new HnswSegment.Descriptor(Bytes.add(parentStart, Bytes.toBytes(60L)), parentEnd, 50, 50L);

    List<HnswSegment.Descriptor> d1Current =
      HnswIndexManager.currentSegments(Arrays.asList(parentBase, parentDelta), parentStart, midKey);
    assertEquals(2, d1Current.size());
    assertEquals(parentDelta, d1Current.get(0));
    assertEquals(parentBase, d1Current.get(1));

    HnswSegment.Descriptor d1Base =
      new HnswSegment.Descriptor(Bytes.add(parentStart, Bytes.toBytes(100L)), midKey, 500, null);

    d1Current = HnswIndexManager.currentSegments(Arrays.asList(parentBase, parentDelta, d1Base),
      parentStart, midKey);
    assertEquals(1, d1Current.size());
    assertEquals(d1Base, d1Current.get(0));

    List<HnswSegment.Descriptor> d2Current = HnswIndexManager
      .currentSegments(Arrays.asList(parentBase, parentDelta, d1Base), midKey, parentEnd);
    assertEquals(2, d2Current.size());
    assertEquals(parentDelta, d2Current.get(0));
    assertEquals(parentBase, d2Current.get(1));
  }

  @Test
  public void testReplayStartTime() {
    HnswSegment.Descriptor base = baseDescriptor(100_000L, 1000);
    assertEquals(100_000L - HnswIndexManager.REPLAY_MARGIN_MS,
      HnswIndexManager.replayStartTime(Collections.singletonList(base)));

    HnswSegment.Descriptor delta1 = deltaDescriptor(150_000L, 100_000L, 50);
    assertEquals(150_000L - HnswIndexManager.REPLAY_MARGIN_MS,
      HnswIndexManager.replayStartTime(Arrays.asList(base, delta1)));

    HnswSegment.Descriptor delta2 = deltaDescriptor(200_000L, 100_000L, 50);
    assertEquals(200_000L - HnswIndexManager.REPLAY_MARGIN_MS,
      HnswIndexManager.replayStartTime(Arrays.asList(base, delta1, delta2)));

    byte[] otherStart = Bytes.toBytes("m");
    byte[] otherEnd = Bytes.toBytes("z");
    HnswSegment.Descriptor base2 = new HnswSegment.Descriptor(
      Bytes.add(otherStart, Bytes.toBytes(80_000L)), otherEnd, 1000, null);
    HnswSegment.Descriptor delta2_1 = new HnswSegment.Descriptor(
      Bytes.add(otherStart, Bytes.toBytes(180_000L)), otherEnd, 50, 80_000L);
    HnswSegment.Descriptor delta1_3 = deltaDescriptor(250_000L, 100_000L, 50);

    assertEquals(180_000L - HnswIndexManager.REPLAY_MARGIN_MS,
      HnswIndexManager.replayStartTime(Arrays.asList(base, delta1, delta1_3, base2, delta2_1)));

    HnswSegment.Descriptor base3 = new HnswSegment.Descriptor(
      Bytes.add(otherStart, Bytes.toBytes(120_000L)), otherEnd, 1000, null);
    assertEquals(120_000L - HnswIndexManager.REPLAY_MARGIN_MS,
      HnswIndexManager.replayStartTime(Arrays.asList(base, delta1_3, base3)));
  }

  @Test
  public void testSegmentsToRetireBasesAndTheirDeltasRetiredInSamePass() {
    HnswSegment.Descriptor oldBase = baseDescriptor(100L, 1000);
    HnswSegment.Descriptor oldDelta1 = deltaDescriptor(110L, 100L, 50);
    HnswSegment.Descriptor oldDelta2 = deltaDescriptor(120L, 100L, 50);
    HnswSegment.Descriptor newBase = baseDescriptor(200L, 1100);
    HnswSegment.Descriptor newDelta = deltaDescriptor(210L, 200L, 10);

    List<HnswSegment.Descriptor> all =
      Arrays.asList(oldBase, oldDelta1, oldDelta2, newBase, newDelta);
    List<HnswSegment.Descriptor> toRetire = HnswIndexManager.segmentsToRetire(all, START, END);

    assertEquals(3, toRetire.size());
    assertTrue(toRetire.contains(oldBase));
    assertTrue(toRetire.contains(oldDelta1));
    assertTrue(toRetire.contains(oldDelta2));

    assertFalse(toRetire.contains(newBase));
    assertFalse(toRetire.contains(newDelta));
  }

  @Test
  public void testSegmentsToRetireSplitParentDeltasLiveAndDieWithParent() {
    byte[] parentStart = Bytes.toBytes("a");
    byte[] parentEnd = Bytes.toBytes("z");
    byte[] midKey = Bytes.toBytes("m");

    HnswSegment.Descriptor parentBase =
      new HnswSegment.Descriptor(Bytes.add(parentStart, Bytes.toBytes(50L)), parentEnd, 1000, null);
    HnswSegment.Descriptor parentDelta1 =
      new HnswSegment.Descriptor(Bytes.add(parentStart, Bytes.toBytes(60L)), parentEnd, 50, 50L);
    HnswSegment.Descriptor parentDelta2 =
      new HnswSegment.Descriptor(Bytes.add(parentStart, Bytes.toBytes(70L)), parentEnd, 50, 50L);

    HnswSegment.Descriptor d1Base =
      new HnswSegment.Descriptor(Bytes.add(parentStart, Bytes.toBytes(100L)), midKey, 500, null);

    List<HnswSegment.Descriptor> allAfterD1 =
      Arrays.asList(parentBase, parentDelta1, parentDelta2, d1Base);
    List<HnswSegment.Descriptor> d1Retire =
      HnswIndexManager.segmentsToRetire(allAfterD1, parentStart, midKey);
    assertTrue(d1Retire.isEmpty());

    HnswSegment.Descriptor d2Base =
      new HnswSegment.Descriptor(Bytes.add(midKey, Bytes.toBytes(150L)), parentEnd, 500, null);

    List<HnswSegment.Descriptor> allAfterD2 =
      Arrays.asList(parentBase, parentDelta1, parentDelta2, d1Base, d2Base);
    List<HnswSegment.Descriptor> d2Retire =
      HnswIndexManager.segmentsToRetire(allAfterD2, midKey, parentEnd);
    assertEquals(3, d2Retire.size());
    assertTrue(d2Retire.contains(parentBase));
    assertTrue(d2Retire.contains(parentDelta1));
    assertTrue(d2Retire.contains(parentDelta2));
    assertFalse(d2Retire.contains(d1Base));
    assertFalse(d2Retire.contains(d2Base));
  }

  @Test
  public void testSegmentsToRetireOrphanDeltaDeleted() {
    HnswSegment.Descriptor activeBase = baseDescriptor(200L, 1000);
    HnswSegment.Descriptor orphanDelta = deltaDescriptor(110L, 100L, 50);

    List<HnswSegment.Descriptor> all = Arrays.asList(activeBase, orphanDelta);
    List<HnswSegment.Descriptor> toRetire = HnswIndexManager.segmentsToRetire(all, START, END);

    assertEquals(1, toRetire.size());
    assertTrue(toRetire.contains(orphanDelta));
    assertFalse(toRetire.contains(activeBase));
  }

  @Test
  public void testSegmentsToRetireNonOverlappingUntouched() {
    byte[] otherStart = Bytes.toBytes("m");
    byte[] otherEnd = Bytes.toBytes("z");
    HnswSegment.Descriptor otherOldBase =
      new HnswSegment.Descriptor(Bytes.add(otherStart, Bytes.toBytes(50L)), otherEnd, 100, null);
    HnswSegment.Descriptor otherNewBase =
      new HnswSegment.Descriptor(Bytes.add(otherStart, Bytes.toBytes(100L)), otherEnd, 200, null);

    List<HnswSegment.Descriptor> all = Arrays.asList(otherOldBase, otherNewBase);
    List<HnswSegment.Descriptor> toRetire = HnswIndexManager.segmentsToRetire(all, START, END);
    assertTrue(toRetire.isEmpty());
  }
}
