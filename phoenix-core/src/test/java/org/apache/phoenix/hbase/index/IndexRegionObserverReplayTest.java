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
package org.apache.phoenix.hbase.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.Test;

/**
 * Unit tests for the standby-side index-regeneration helpers in {@link IndexRegionObserver}:
 * {@link IndexRegionObserver#decodePreImage}, {@link IndexRegionObserver#applyDeleteToPut},
 * {@link IndexRegionObserver#deriveNextState}, and
 * {@link IndexRegionObserver#buildReplicatedRowGroups}. These exercise the (row, ts) fold that
 * recovers the active-side {@code nextDataRowState} from the per-row PRE_IMAGE plus the replicated
 * cell stream. The reader/reconstruct side (peeling the PRE_IMAGE cell off the wire and stamping
 * the attribute) is covered separately by {@code MutationCellGrouperTest}.
 */
public class IndexRegionObserverReplayTest {

  private static final byte[] ROW = Bytes.toBytes("r1");
  private static final byte[] R2 = Bytes.toBytes("r2");
  private static final byte[] CF = Bytes.toBytes("0");
  private static final byte[] Q1 = Bytes.toBytes("c1");
  private static final byte[] Q2 = Bytes.toBytes("c2");
  private static final byte[] Q3 = Bytes.toBytes("c3");
  private static final long TS = 100L;

  private final IndexRegionObserver iro = new IndexRegionObserver("T");

  /** A Put on the given row at the given ts, one cell per qualifier. */
  private static Put putRowTs(byte[] row, long ts, byte[]... qualifiers) {
    Put put = new Put(row);
    for (byte[] q : qualifiers) {
      put.addColumn(CF, q, ts, Bytes.toBytes("v"));
    }
    return put;
  }

  /** Stamps the given pre-image onto a mutation as its PRE_IMAGE attribute and returns it. */
  private static <M extends Mutation> M withPreImage(M m, Put preImage) throws IOException {
    m.setAttribute(IndexRegionObserver.PRE_IMAGE, IndexRegionObserver.encodePreImage(preImage));
    return m;
  }

  /** Wraps the given mutations in a MiniBatchOperationInProgress spanning the whole array. */
  private static MiniBatchOperationInProgress<Mutation> batchOf(Mutation... mutations) {
    return new MiniBatchOperationInProgress<>(mutations, new OperationStatus[mutations.length],
      new WALEdit[mutations.length], 0, mutations.length, mutations.length);
  }

  // ---- isReplayBatch ----

  @Test
  public void testIsReplayBatchTrueWhenFirstMutationCarriesMarker() {
    Put replayed = putRowTs(ROW, TS, Q1);
    replayed.setAttribute(IndexRegionObserver.REPLICATED_MUTATION, Bytes.toBytes(true));
    assertTrue("a batch whose first mutation carries REPLICATED_MUTATION is a replay batch",
      iro.isReplayBatch(batchOf(replayed)));
  }

  @Test
  public void testIsReplayBatchFalseForLiveWrite() {
    // A live client write with no REPLICATED_MUTATION marker (and, as here, no _HAGroupName)
    // must not be treated as replay -- this is exactly the case the bypass counter must count.
    assertFalse("a batch without the marker is not a replay batch",
      iro.isReplayBatch(batchOf(putRowTs(ROW, TS, Q1))));
  }

  @Test
  public void testIsReplayBatchFalseForEmptyBatch() {
    assertFalse("an empty batch is not a replay batch", iro.isReplayBatch(batchOf()));
  }

  // ---- classifyBatch ----

  @Test
  public void testClassifyBatchClientHaWhenHAGroupPresent() {
    assertEquals(IndexRegionObserver.BatchOrigin.CLIENT_HA,
      iro.classifyBatch(batchOf(putRowTs(ROW, TS, Q1)), Optional.of("g")));
  }

  @Test
  public void testClassifyBatchClientNonHaWhenNoHAGroup() {
    assertEquals(IndexRegionObserver.BatchOrigin.CLIENT_NON_HA,
      iro.classifyBatch(batchOf(putRowTs(ROW, TS, Q1)), Optional.empty()));
  }

  @Test
  public void testClassifyBatchNativeInWhenClusterIdsPresent() {
    // A write replicated in via HBase-native replication carries source cluster ids and no
    // _HAGroupName -- it must not be mistaken for a client write that forgot the attribute.
    Put p = putRowTs(ROW, TS, Q1);
    p.setClusterIds(Collections.singletonList(UUID.randomUUID()));
    assertEquals(IndexRegionObserver.BatchOrigin.NATIVE_IN,
      iro.classifyBatch(batchOf(p), Optional.empty()));
  }

  @Test
  public void testClassifyBatchReplayMarkerWinsOverClusterIds() {
    // The replay marker is authoritative and checked first.
    Put p = putRowTs(ROW, TS, Q1);
    p.setClusterIds(Collections.singletonList(UUID.randomUUID()));
    p.setAttribute(IndexRegionObserver.REPLICATED_MUTATION, Bytes.toBytes(true));
    assertEquals(IndexRegionObserver.BatchOrigin.PHX_REPLAY,
      iro.classifyBatch(batchOf(p), Optional.empty()));
  }

  @Test
  public void testClassifyBatchClusterIdsWinOverHAGroup() {
    // Origin precedence: native-replicated-in (cluster ids) before _HAGroupName presence.
    Put p = putRowTs(ROW, TS, Q1);
    p.setClusterIds(Collections.singletonList(UUID.randomUUID()));
    assertEquals(IndexRegionObserver.BatchOrigin.NATIVE_IN,
      iro.classifyBatch(batchOf(p), Optional.of("g")));
  }

  @Test
  public void testClassifyBatchEmptyIsClientNonHa() {
    assertEquals(IndexRegionObserver.BatchOrigin.CLIENT_NON_HA,
      iro.classifyBatch(batchOf(), Optional.empty()));
  }

  @Test
  public void testClassifyBatchFirstMutationIsAuthoritativeForMultiMutationBatch() {
    // A batch is uniform in origin by construction (the standby reader stamps every reconstructed
    // mutation), so classifyBatch/isReplayBatch inspect only the first mutation. Prove that
    // contract holds for a multi-mutation batch: the replay marker on the FIRST mutation classifies
    // the whole batch as PHX_REPLAY even though the trailing mutations carry no marker.
    Put replayed = putRowTs(ROW, TS, Q1);
    replayed.setAttribute(IndexRegionObserver.REPLICATED_MUTATION, Bytes.toBytes(true));
    Put unmarked = putRowTs(R2, TS, Q2);
    assertEquals("first mutation's marker classifies the whole batch",
      IndexRegionObserver.BatchOrigin.PHX_REPLAY,
      iro.classifyBatch(batchOf(replayed, unmarked), Optional.empty()));
    assertTrue("isReplayBatch keys off the first mutation for a multi-mutation batch",
      iro.isReplayBatch(batchOf(replayed, unmarked)));
  }

  @Test
  public void testReplayBatchIsNotBypassCounterCandidate() {
    // The bypassedMutationBlockCount gate fires only for CLIENT_NON_HA. A standby replay batch
    // carries REPLICATED_MUTATION and classifies as PHX_REPLAY -- never CLIENT_NON_HA -- so the
    // counter is not incremented for replayed mutations.
    Put replayed = putRowTs(ROW, TS, Q1);
    replayed.setAttribute(IndexRegionObserver.REPLICATED_MUTATION, Bytes.toBytes(true));
    IndexRegionObserver.BatchOrigin origin = iro.classifyBatch(batchOf(replayed), Optional.empty());
    assertEquals("a replay batch classifies as PHX_REPLAY",
      IndexRegionObserver.BatchOrigin.PHX_REPLAY, origin);
    assertNotEquals("a replay batch must not be the counter-triggering origin",
      IndexRegionObserver.BatchOrigin.CLIENT_NON_HA, origin);
  }

  // ---- decodePreImage ----

  @Test
  public void testDecodePreImageMissingAttributeThrows() {
    Put m = putRowTs(ROW, TS, Q1);
    try {
      iro.decodePreImage(m);
      fail("expected DoNotRetryIOException when PRE_IMAGE attribute is absent");
    } catch (DoNotRetryIOException expected) {
      assertTrue(expected.getMessage().contains(IndexRegionObserver.PRE_IMAGE));
    } catch (IOException e) {
      fail("expected DoNotRetryIOException, got " + e);
    }
  }

  @Test
  public void testDecodePreImageEmptySentinelReturnsNull() throws IOException {
    Put m = putRowTs(ROW, TS, Q1);
    m.setAttribute(IndexRegionObserver.PRE_IMAGE, HConstants.EMPTY_BYTE_ARRAY);
    assertNull("zero-length PRE_IMAGE is the 'active saw empty row' sentinel",
      iro.decodePreImage(m));
  }

  @Test
  public void testDecodePreImageRoundTrip() throws IOException {
    Put preImage = putRowTs(ROW, TS, Q1, Q2);
    Put carrier = withPreImage(new Put(ROW), preImage);

    Put decoded = iro.decodePreImage(carrier);
    assertTrue(decoded.has(CF, Q1));
    assertTrue(decoded.has(CF, Q2));
    assertTrue(CellUtil.matchingRow(decoded.getFamilyCellMap().get(CF).get(0), ROW));
  }

  // ---- applyDeleteToPut ----

  @Test
  public void testApplyDeleteToNullPutIsNull() {
    Delete d = new Delete(ROW);
    d.addColumns(CF, Q1, TS);
    assertNull(IndexRegionObserver.applyDeleteToPut(d, null));
  }

  @Test
  public void testApplyDeleteColumnRemovesOnlyThatColumn() {
    Put put = putRowTs(ROW, TS, Q1, Q2);
    Delete d = new Delete(ROW);
    d.addColumns(CF, Q1, TS);

    Put result = IndexRegionObserver.applyDeleteToPut(d, put);
    assertFalse("Q1 should be removed", result.has(CF, Q1));
    assertTrue("Q2 should remain", result.has(CF, Q2));
  }

  @Test
  public void testApplyDeleteFamilyRemovesWholeFamily() {
    Put put = putRowTs(ROW, TS, Q1, Q2);
    Delete d = new Delete(ROW);
    d.addFamily(CF, TS);

    Put result = IndexRegionObserver.applyDeleteToPut(d, put);
    assertNull("removing the only family empties the row", result);
  }

  @Test
  public void testApplyDeleteEmptiesRowReturnsNull() {
    Put put = putRowTs(ROW, TS, Q1);
    Delete d = new Delete(ROW);
    d.addColumns(CF, Q1, TS);

    assertNull("deleting the last column empties the row",
      IndexRegionObserver.applyDeleteToPut(d, put));
  }

  // ---- deriveNextState ----

  @Test
  public void testDeriveNextStateNoPreImageNoPutIsNull() throws IOException {
    Delete d = new Delete(ROW);
    d.addColumns(CF, Q1, TS);
    assertNull("a Delete-only group with no pre-image yields no state",
      IndexRegionObserver.deriveNextState(null, Collections.<Mutation> singletonList(d)));
  }

  @Test
  public void testDeriveNextStatePutOnNullPreImageInsert() throws IOException {
    Put put = putRowTs(ROW, TS, Q1);
    Put next = IndexRegionObserver.deriveNextState(null, Collections.<Mutation> singletonList(put));
    assertTrue(next.has(CF, Q1));
  }

  @Test
  public void testDeriveNextStatePutMergesOntoPreImage() throws IOException {
    Put preImage = putRowTs(ROW, TS, Q1);
    Put put = putRowTs(ROW, TS, Q2);
    Put next =
      IndexRegionObserver.deriveNextState(preImage, Collections.<Mutation> singletonList(put));
    assertTrue("new column present", next.has(CF, Q2));
    assertTrue("pre-image column carried forward", next.has(CF, Q1));
  }

  @Test
  public void testDeriveNextStatePutThenDelete() throws IOException {
    Put preImage = putRowTs(ROW, TS, Q1);
    Put put = putRowTs(ROW, TS, Q2);
    Delete del = new Delete(ROW);
    del.addColumns(CF, Q1, TS);

    List<Mutation> group = Arrays.<Mutation> asList(put, del);
    Put next = IndexRegionObserver.deriveNextState(preImage, group);
    assertFalse("Q1 deleted", next.has(CF, Q1));
    assertTrue("Q2 added", next.has(CF, Q2));
  }

  @Test
  public void testDeriveNextStateDeleteEmptiesRow() throws IOException {
    Put preImage = putRowTs(ROW, TS, Q1);
    Delete del = new Delete(ROW);
    del.addFamily(CF, TS);

    Put next =
      IndexRegionObserver.deriveNextState(preImage, Collections.<Mutation> singletonList(del));
    assertNull("deleting the family empties the derived state", next);
  }

  // ---- buildReplicatedRowGroups ----

  @Test
  public void testBuildReplicatedRowGroupsSplitsByTimestamp() throws IOException {
    Put g1 = withPreImage(putRowTs(ROW, 100L, Q1), null);
    Put g2 = withPreImage(putRowTs(ROW, 200L, Q2), putRowTs(ROW, 200L, Q1));

    List<IndexRegionObserver.ReplicatedRowGroup> groups =
      iro.buildReplicatedRowGroups(Arrays.<Mutation> asList(g1, g2));

    assertEquals("two distinct (row, ts) groups", 2, groups.size());
    assertEquals(100L, groups.get(0).ts);
    assertEquals(200L, groups.get(1).ts);
    assertNull("first group's pre-image is the empty-row sentinel", groups.get(0).preImage);
    assertTrue("second group's pre-image carries Q1", groups.get(1).preImage.has(CF, Q1));
  }

  @Test
  public void testBuildReplicatedRowGroupsEachGroupKeepsItsOwnPreImage() throws IOException {
    // Two batches on the same row: each ships its own authoritative pre-image. The second group's
    // pre-image must NOT be derived from the first group — it is the active's shipped value.
    Put g1 = withPreImage(putRowTs(ROW, 100L, Q1), null);
    Put g2 = withPreImage(putRowTs(ROW, 200L, Q2), putRowTs(ROW, 200L, Q1));

    List<IndexRegionObserver.ReplicatedRowGroup> groups =
      iro.buildReplicatedRowGroups(Arrays.<Mutation> asList(g1, g2));

    // group 1: no pre-image + a Put(Q1) -> next has Q1
    assertTrue(groups.get(0).nextState.has(CF, Q1));
    // group 2: pre-image(Q1) + Put(Q2) -> next has both
    assertTrue(groups.get(1).nextState.has(CF, Q1));
    assertTrue(groups.get(1).nextState.has(CF, Q2));
  }

  @Test
  public void testBuildReplicatedRowGroupsMergesSameRowTs() throws IOException {
    Put a = withPreImage(putRowTs(ROW, 100L, Q1), null);
    Put b = withPreImage(putRowTs(ROW, 100L, Q2), null);

    List<IndexRegionObserver.ReplicatedRowGroup> groups =
      iro.buildReplicatedRowGroups(Arrays.<Mutation> asList(a, b));

    assertEquals("same (row, ts) collapses into one group", 1, groups.size());
    assertEquals(2, groups.get(0).mutations.size());
    assertTrue(groups.get(0).nextState.has(CF, Q1));
    assertTrue(groups.get(0).nextState.has(CF, Q2));
  }

  /**
   * The canonical (row, ts) grouping case: one mini-batch carrying four mutations across three
   * groups -- (R1, ts1) holds a Put and a Delete, (R1, ts2) a Put on the same row at a later ts,
   * (R2, ts1) a Put on a different row. Proves the groups are isolated: each folds only its own
   * cells onto its own pre-image, and the (R1, ts1) Put+Delete fold does not leak into (R1, ts2) or
   * (R2, ts1).
   */
  @Test
  public void testBuildReplicatedRowGroupsMultiRowMultiTsIsolation() throws IOException {
    long ts1 = 100L, ts2 = 200L;

    // (R1, ts1): pre-image {Q1, Q3}; Put A adds Q2, Delete C removes Q3 -> next {Q1, Q2}.
    Put r1t1Put = withPreImage(putRowTs(ROW, ts1, Q2), putRowTs(ROW, ts1, Q1, Q3));
    Delete r1t1Del = new Delete(ROW);
    r1t1Del.addColumns(CF, Q3, ts1);
    withPreImage(r1t1Del, putRowTs(ROW, ts1, Q1, Q3));
    // (R1, ts2): pre-image {Q1}; Put B adds Q3 -> next {Q1, Q3}.
    Put r1t2Put = withPreImage(putRowTs(ROW, ts2, Q3), putRowTs(ROW, ts2, Q1));
    // (R2, ts1): no pre-image; Put X adds Q1 -> next {Q1}.
    Put r2t1Put = withPreImage(putRowTs(R2, ts1, Q1), null);

    List<IndexRegionObserver.ReplicatedRowGroup> groups =
      iro.buildReplicatedRowGroups(Arrays.<Mutation> asList(r1t1Put, r1t1Del, r1t2Put, r2t1Put));

    // (a) exactly three groups, in first-seen order.
    assertEquals("three (row, ts) groups", 3, groups.size());
    IndexRegionObserver.ReplicatedRowGroup g1 = groups.get(0);
    IndexRegionObserver.ReplicatedRowGroup g2 = groups.get(1);
    IndexRegionObserver.ReplicatedRowGroup g3 = groups.get(2);

    // (d) each group carries its own (row, ts).
    assertTrue(Bytes.equals(ROW, g1.row.copyBytesIfNecessary()));
    assertEquals(ts1, g1.ts);
    assertTrue(Bytes.equals(ROW, g2.row.copyBytesIfNecessary()));
    assertEquals(ts2, g2.ts);
    assertTrue(Bytes.equals(R2, g3.row.copyBytesIfNecessary()));
    assertEquals(ts1, g3.ts);

    // (b) (R1, ts1): both Put A and Delete C folded onto the ts1 pre-image.
    assertEquals("R1/ts1 group holds the Put and the Delete", 2, g1.mutations.size());
    assertTrue("Q1 carried from pre-image", g1.nextState.has(CF, Q1));
    assertTrue("Q2 added by Put A", g1.nextState.has(CF, Q2));
    assertFalse("Q3 removed by Delete C", g1.nextState.has(CF, Q3));

    // (c) (R1, ts2): only Put B on the ts2 pre-image -- no leak from the ts1 group.
    assertTrue("Q1 carried from ts2 pre-image", g2.nextState.has(CF, Q1));
    assertTrue("Q3 added by Put B", g2.nextState.has(CF, Q3));
    assertFalse("Q2 must not leak in from the ts1 group", g2.nextState.has(CF, Q2));

    // (R2, ts1): independent row, no pre-image -- only Put X's Q1.
    assertNull("R2 group has the empty-row sentinel pre-image", g3.preImage);
    assertTrue("Q1 inserted by Put X", g3.nextState.has(CF, Q1));
    assertFalse("no leak from R1 groups", g3.nextState.has(CF, Q2));
    assertFalse("no leak from R1 groups", g3.nextState.has(CF, Q3));
  }
}
