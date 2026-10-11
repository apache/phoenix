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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.VectorIndexType;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

/**
 * Unit tests for the rules of {@link HnswIndexManager}: segment selection, replay start,
 * retirement, rebuild order, retry decisions and index checks.
 */
public class HnswIndexManagerTest {
  private static final byte[] START = Bytes.toBytes("a");
  private static final byte[] END = Bytes.toBytes("m");
  private static final byte[] INDEX = Bytes.toBytes("I");
  private static final byte[] DEFINITION = HnswSegment.definitionDigest("definition");

  // Returns a base segment of [start, end) at the given time. Its replay start is the same time
  private static HnswSegment.Descriptor base(byte[] start, byte[] end, long time, int count) {
    return new HnswSegment.Descriptor(INDEX, start, time, end, count, null, time, DEFINITION);
  }

  // Returns a delta segment of [start, end) at the given time on the given base. Its replay start
  // is the same time
  private static HnswSegment.Descriptor delta(byte[] start, byte[] end, long time, long baseTime,
    int count) {
    return new HnswSegment.Descriptor(INDEX, start, time, end, count, baseTime, time, DEFINITION);
  }

  private static HnswSegment.Descriptor baseDescriptor(long time, int count) {
    return base(START, END, time, count);
  }

  private static HnswSegment.Descriptor deltaDescriptor(long time, long baseTime, int count) {
    return delta(START, END, time, baseTime, count);
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
    HnswSegment.Descriptor parent = base(START, otherEnd, 100L, 1000);
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
    HnswSegment.Descriptor deltaOther = deltaDescriptor(150L, 50L, 50);
    current = HnswIndexManager.currentSegments(Arrays.asList(baseA, deltaOther), START, END);
    assertEquals(1, current.size());
    assertEquals(baseA, current.get(0));
  }

  @Test
  public void testCurrentSegmentsSplitParentAndDaughterBases() {
    byte[] parentStart = Bytes.toBytes("a");
    byte[] parentEnd = Bytes.toBytes("z");
    byte[] midKey = Bytes.toBytes("m");

    HnswSegment.Descriptor parentBase = base(parentStart, parentEnd, 50L, 1000);
    HnswSegment.Descriptor parentDelta = delta(parentStart, parentEnd, 60L, 50L, 50);

    List<HnswSegment.Descriptor> d1Current =
      HnswIndexManager.currentSegments(Arrays.asList(parentBase, parentDelta), parentStart, midKey);
    assertEquals(2, d1Current.size());
    assertEquals(parentDelta, d1Current.get(0));
    assertEquals(parentBase, d1Current.get(1));

    HnswSegment.Descriptor d1Base = base(parentStart, midKey, 100L, 500);

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
    assertEquals(100_000L, HnswIndexManager.replayStartTime(Collections.singletonList(base)));

    HnswSegment.Descriptor delta1 = deltaDescriptor(150_000L, 100_000L, 50);
    assertEquals(150_000L, HnswIndexManager.replayStartTime(Arrays.asList(base, delta1)));

    HnswSegment.Descriptor delta2 = deltaDescriptor(200_000L, 100_000L, 50);
    assertEquals(200_000L, HnswIndexManager.replayStartTime(Arrays.asList(base, delta1, delta2)));

    byte[] otherStart = Bytes.toBytes("m");
    byte[] otherEnd = Bytes.toBytes("z");
    HnswSegment.Descriptor base2 = base(otherStart, otherEnd, 80_000L, 1000);
    HnswSegment.Descriptor delta2_1 = delta(otherStart, otherEnd, 180_000L, 80_000L, 50);
    HnswSegment.Descriptor delta1_3 = deltaDescriptor(250_000L, 100_000L, 50);

    // The replay start is the oldest replay start of the newest segments of the stacks
    assertEquals(180_000L,
      HnswIndexManager.replayStartTime(Arrays.asList(base, delta1, delta1_3, base2, delta2_1)));

    HnswSegment.Descriptor base3 = base(otherStart, otherEnd, 120_000L, 1000);
    assertEquals(120_000L, HnswIndexManager.replayStartTime(Arrays.asList(base, delta1_3, base3)));

    // A batch took its timestamp before the swap of the newest segment, and applied its changes
    // after the swap. Thus the newest segment replays from the batch timestamp
    HnswSegment.Descriptor late =
      new HnswSegment.Descriptor(INDEX, START, 300_000L, END, 50, 100_000L, 120_000L, DEFINITION);
    assertEquals(120_000L,
      HnswIndexManager.replayStartTime(Arrays.asList(base, delta1, delta2, late)));
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
    List<HnswSegment.Descriptor> toRetire =
      HnswIndexManager.segmentsToRetire(all, START, END, DEFINITION);

    assertEquals(3, toRetire.size());
    assertTrue(toRetire.contains(oldBase));
    assertTrue(toRetire.contains(oldDelta1));
    assertTrue(toRetire.contains(oldDelta2));

    assertFalse(toRetire.contains(newBase));
    assertFalse(toRetire.contains(newDelta));
  }

  @Test
  public void testSegmentsToRetireOrphanDeltaDeleted() {
    HnswSegment.Descriptor activeBase = baseDescriptor(200L, 1000);
    HnswSegment.Descriptor orphanDelta = deltaDescriptor(110L, 100L, 50);

    List<HnswSegment.Descriptor> all = Arrays.asList(activeBase, orphanDelta);
    List<HnswSegment.Descriptor> toRetire =
      HnswIndexManager.segmentsToRetire(all, START, END, DEFINITION);

    assertEquals(1, toRetire.size());
    assertTrue(toRetire.contains(orphanDelta));
    assertFalse(toRetire.contains(activeBase));
  }

  /**
   * A region retires only the segments of its own family, when newer bases cover their part of the
   * range of the region. The lower daughter of a split holds the parent stack. It retires the
   * parent base and its deltas when its own base lands, and it does not wait for the upper
   * daughter. The upper daughter does not hold the parent stack.
   */
  @Test
  public void testSegmentsToRetireIsRegionScoped() {
    byte[] parentStart = Bytes.toBytes("a");
    byte[] parentEnd = Bytes.toBytes("z");
    byte[] midKey = Bytes.toBytes("m");
    HnswSegment.Descriptor parentBase = base(parentStart, parentEnd, 50L, 1000);
    HnswSegment.Descriptor parentDelta1 = delta(parentStart, parentEnd, 60L, 50L, 50);
    HnswSegment.Descriptor parentDelta2 = delta(parentStart, parentEnd, 70L, 50L, 50);
    assertTrue(
      HnswIndexManager.segmentsToRetire(Arrays.asList(parentBase, parentDelta1, parentDelta2),
        parentStart, midKey, DEFINITION).isEmpty());

    HnswSegment.Descriptor d1Base = base(parentStart, midKey, 100L, 500);
    assertEquals(Arrays.asList(parentBase, parentDelta1, parentDelta2),
      HnswIndexManager.segmentsToRetire(
        Arrays.asList(parentBase, parentDelta1, parentDelta2, d1Base), parentStart, midKey,
        DEFINITION));

    // A merged region retires both predecessor bases with its own base
    HnswSegment.Descriptor d2Base = base(midKey, parentEnd, 150L, 500);
    HnswSegment.Descriptor merged = base(parentStart, parentEnd, 200L, 1000);
    assertEquals(Arrays.asList(d1Base, d2Base), HnswIndexManager
      .segmentsToRetire(Arrays.asList(d1Base, d2Base, merged), parentStart, parentEnd, DEFINITION));
    // A newer base for a part of the range does not retire a segment
    assertTrue(HnswIndexManager
      .segmentsToRetire(Arrays.asList(parentBase, d2Base), parentStart, parentEnd, DEFINITION)
      .isEmpty());
  }

  @Test
  public void testSegmentsToRetireNonOverlappingUntouched() {
    byte[] otherStart = Bytes.toBytes("m");
    byte[] otherEnd = Bytes.toBytes("z");
    HnswSegment.Descriptor otherOldBase = base(otherStart, otherEnd, 50L, 100);
    HnswSegment.Descriptor otherNewBase = base(otherStart, otherEnd, 100L, 200);

    List<HnswSegment.Descriptor> all = Arrays.asList(otherOldBase, otherNewBase);
    List<HnswSegment.Descriptor> toRetire =
      HnswIndexManager.segmentsToRetire(all, START, END, DEFINITION);
    assertTrue(toRetire.isEmpty());
  }

  /**
   * The segments of another definition are not current. An example is a segment of an index that a
   * user dropped and created again with the same name. A newer base of the definition retires them,
   * with their deltas. They never retire a segment of the definition.
   */
  @Test
  public void testSegmentsOfAnotherDefinition() {
    byte[] other = HnswSegment.definitionDigest("other");
    HnswSegment.Descriptor stale =
      new HnswSegment.Descriptor(INDEX, START, 50L, END, 10, null, 50L, other);
    HnswSegment.Descriptor staleDelta =
      new HnswSegment.Descriptor(INDEX, START, 60L, END, 10, 50L, 60L, other);
    HnswSegment.Descriptor base = baseDescriptor(100L, 10);
    HnswSegment.Descriptor newer =
      new HnswSegment.Descriptor(INDEX, START, 150L, END, 10, null, 150L, other);
    assertEquals(Arrays.asList(stale, staleDelta), HnswIndexManager
      .segmentsToRetire(Arrays.asList(stale, staleDelta, base, newer), START, END, DEFINITION));
  }

  /**
   * The rebuild thread runs the rebuilds of uncovered regions first, and the other rebuilds in the
   * order of their requests.
   */
  @Test
  public void testRebuildOrder() {
    Runnable task = () -> {
    };
    HnswIndexManager.Rebuild covered1 = new HnswIndexManager.Rebuild(task, false);
    HnswIndexManager.Rebuild uncovered1 = new HnswIndexManager.Rebuild(task, true);
    HnswIndexManager.Rebuild covered2 = new HnswIndexManager.Rebuild(task, false);
    HnswIndexManager.Rebuild uncovered2 = new HnswIndexManager.Rebuild(task, true);
    List<HnswIndexManager.Rebuild> queue =
      new ArrayList<>(Arrays.asList(covered1, uncovered1, covered2, uncovered2));
    Collections.sort(queue);
    assertEquals(Arrays.asList(uncovered1, uncovered2, covered1, covered2), queue);
  }

  /**
   * A segment that is too large to write or to load fails the same way on each retry. Thus the
   * manager schedules no retry for a flush that fails that way. The manager retries a flush that
   * fails because a table is disabled, a call times out, or other segments hold the off-heap budget
   * for a short time.
   */
  @Test
  public void testPermanentFailuresAreNotRetried() throws Exception {
    ScheduledExecutorService retries = mock(ScheduledExecutorService.class);
    Runnable retry = () -> {
    };
    int dim = 8;
    Random random = new Random(3);
    List<VectorFloat<?>> vectors = new ArrayList<>();
    byte[][] keys = new byte[20][];
    for (int i = 0; i < keys.length; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = (float) random.nextGaussian();
      }
      vectors.add(VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(v));
      keys[i] = Bytes.toBytes("row-" + i);
    }
    PTable.VectorIndex vi = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
      .setDistanceMetric("L2").setDimension(dim).setHnswM(16).setHnswEfConstruction(100)
      .setHnswAlpha(1.2).setQuantizationType("NONE").build();
    byte[] payload = HnswSegment.build(vi, new ListRandomAccessVectorValues(vectors, dim), keys);
    try {
      HnswSegment.of(null, baseDescriptor(1, keys.length), new HnswOffheapAllocator(64), "L2",
        payload, new byte[0][]);
      fail("a segment larger than the off-heap budget must not load");
    } catch (HnswSegment.TooLargeException e) {
      assertFalse(HnswIndexManager.scheduleRetry(e, retries, retry));
    }
    assertFalse(
      HnswIndexManager.scheduleRetry(new RequestTooBigException("too big"), retries, retry));
    assertFalse(HnswIndexManager
      .scheduleRetry(new IllegalArgumentException("KeyValue size too large"), retries, retry));
    verifyNoInteractions(retries);

    HnswOffheapAllocator busy = new HnswOffheapAllocator(payload.length);
    HnswSegment held =
      HnswSegment.of(null, baseDescriptor(1, keys.length), busy, "L2", payload, new byte[0][]);
    try {
      // The segment holds its graph until it closes. Thus the budget still counts the graph after
      // the eviction
      held.graph();
      busy.evictAll();
      HnswSegment.of(null, baseDescriptor(2, keys.length), busy, "L2", payload, new byte[0][]);
      fail("an allocation that the budget cannot fit must be refused");
    } catch (IOException e) {
      assertTrue(HnswIndexManager.scheduleRetry(e, retries, retry));
    } finally {
      held.close();
    }
    assertTrue(
      HnswIndexManager.scheduleRetry(new TableNotEnabledException("disabled"), retries, retry));
    assertTrue(HnswIndexManager.scheduleRetry(new IOException("timeout"), retries, retry));
    verify(retries, times(3)).schedule(retry, HnswIndexManager.RETRY_DELAY_MS,
      TimeUnit.MILLISECONDS);
  }

  /**
   * A scan names the index that a region rebuilds or searches. Thus the region accepts only an HNSW
   * index of its own data table. An HNSW index is a VECTOR_LOCAL index.
   */
  @Test
  public void testCheckIndexOf() throws Exception {
    TableName table = TableName.valueOf("T");
    PTable data = mock(PTable.class);
    when(data.getType()).thenReturn(PTableType.TABLE);
    when(data.getName()).thenReturn(PNameFactory.newName("T"));
    when(data.getPhysicalName()).thenReturn(PNameFactory.newName("T"));
    PTable index = mock(PTable.class);
    when(index.getType()).thenReturn(PTableType.INDEX);
    when(index.getName()).thenReturn(PNameFactory.newName("I"));
    when(index.getIndexType()).thenReturn(PTable.IndexType.VECTOR_LOCAL);
    when(index.getVectorIndexType()).thenReturn(VectorIndexType.HNSW);
    HnswIndexManager.checkIndexOf(index, data, table);

    PTable global = mock(PTable.class);
    when(global.getType()).thenReturn(PTableType.INDEX);
    when(global.getName()).thenReturn(PNameFactory.newName("G"));
    when(global.getIndexType()).thenReturn(PTable.IndexType.VECTOR_GLOBAL);
    when(global.getVectorIndexType()).thenReturn(VectorIndexType.HNSW);

    PTable ivf = mock(PTable.class);
    when(ivf.getType()).thenReturn(PTableType.INDEX);
    when(ivf.getName()).thenReturn(PNameFactory.newName("IVF"));
    when(ivf.getIndexType()).thenReturn(PTable.IndexType.VECTOR_GLOBAL);
    when(ivf.getVectorIndexType()).thenReturn(VectorIndexType.IVF);
    Object[][] rejected = { { index, data, TableName.valueOf("OTHER") }, { index, null, table },
      { ivf, data, table }, { global, data, table }, { data, null, table } };
    for (Object[] args : rejected) {
      try {
        HnswIndexManager.checkIndexOf((PTable) args[0], (PTable) args[1], (TableName) args[2]);
        fail("must reject " + ((PTable) args[0]).getName() + " for " + args[2]);
      } catch (DoNotRetryIOException e) {
        // Expected
      }
    }
  }

  // Returns a mock HNSW index named I, with the given parameters, on the vector column V
  private static PTable hnsw(PTable.VectorIndex vi) {
    PColumn column = mock(PColumn.class);
    when(column.getFamilyName()).thenReturn(PNameFactory.newName("0"));
    doReturn(PVectorFloat.INSTANCE).when(column).getDataType();
    when(column.getExpressionStr()).thenReturn("\"0\".\"V\"");
    PTable index = mock(PTable.class);
    when(index.getType()).thenReturn(PTableType.INDEX);
    when(index.getName()).thenReturn(PNameFactory.newName("I"));
    when(index.getIndexType()).thenReturn(PTable.IndexType.VECTOR_LOCAL);
    when(index.getVectorIndexType()).thenReturn(VectorIndexType.HNSW);
    when(index.getVectorIndex()).thenReturn(vi);
    when(index.getColumns()).thenReturn(Collections.singletonList(column));
    return index;
  }

  /**
   * A manager serves its index only while the index is an HNSW index of the data table of its
   * region. The index must also keep the definition that the manager opened with. An index that is
   * created again with the same name can have other parameters, another data table, or another
   * index type.
   */
  @Test
  public void testIsCurrent() {
    TableName table = TableName.valueOf("T");
    PTable data = mock(PTable.class);
    when(data.getPhysicalName()).thenReturn(PNameFactory.newName("T"));
    PTable other = mock(PTable.class);
    when(other.getPhysicalName()).thenReturn(PNameFactory.newName("T2"));
    PTable.VectorIndex l2 = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
      .setDistanceMetric("L2").setDimension(8).setHnswM(16).setHnswEfConstruction(100)
      .setHnswAlpha(1.2).setQuantizationType("NONE").build();
    PTable index = hnsw(l2);
    String definition = HnswIndexManager.definition(index);
    assertTrue(HnswIndexManager.isCurrent(index, data, table, definition));
    assertFalse(
      HnswIndexManager.isCurrent(hnsw(new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
        .setDistanceMetric("COSINE").setDimension(8).setHnswM(16).setHnswEfConstruction(100)
        .setHnswAlpha(1.2).setQuantizationType("NONE").build()), data, table, definition));
    assertFalse(HnswIndexManager.isCurrent(index, other, table, definition));
    PTable global = mock(PTable.class);
    when(global.getType()).thenReturn(PTableType.INDEX);
    when(global.getName()).thenReturn(PNameFactory.newName("I"));
    when(global.getIndexType()).thenReturn(PTable.IndexType.GLOBAL);
    assertFalse(HnswIndexManager.isCurrent(global, data, table, definition));
  }
}
