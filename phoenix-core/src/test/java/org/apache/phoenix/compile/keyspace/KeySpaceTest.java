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
package org.apache.phoenix.compile.keyspace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.junit.Test;

/**
 * Algebra-level tests for {@link KeySpace}. No {@code StatementContext} or database setup:
 * builds ranges directly from byte arrays.
 */
public class KeySpaceTest {

  private static KeyRange pt(String v) {
    byte[] b = Bytes.toBytes(v);
    return KeyRange.getKeyRange(b, true, b, true);
  }

  private static KeyRange range(String lo, boolean loInc, String hi, boolean hiInc) {
    return KeyRange.getKeyRange(Bytes.toBytes(lo), loInc, Bytes.toBytes(hi), hiInc);
  }

  private static KeyRange gt(String lo) {
    return KeyRange.getKeyRange(Bytes.toBytes(lo), false, KeyRange.UNBOUND, false);
  }

  private static KeyRange gte(String lo) {
    return KeyRange.getKeyRange(Bytes.toBytes(lo), true, KeyRange.UNBOUND, false);
  }

  private static KeyRange lt(String hi) {
    return KeyRange.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes(hi), false);
  }

  @Test
  public void everythingAndEmptyAreRecognized() {
    KeySpace all = KeySpace.everything(3);
    assertTrue(all.isEverything());
    assertFalse(all.isEmpty());

    KeySpace none = KeySpace.empty(3);
    assertTrue(none.isEmpty());
    assertFalse(none.isEverything());
  }

  @Test
  public void singleDimConstructor() {
    KeySpace ks = KeySpace.single(1, pt("a"), 3);
    assertEquals(KeyRange.EVERYTHING_RANGE, ks.get(0));
    assertEquals(pt("a"), ks.get(1));
    assertEquals(KeyRange.EVERYTHING_RANGE, ks.get(2));
  }

  @Test
  public void andIntersectsEachDim() {
    KeySpace a = KeySpace.of(
      new KeyRange[] { gte("a"), KeyRange.EVERYTHING_RANGE, range("1", true, "9", false) });
    KeySpace b = KeySpace.of(
      new KeyRange[] { lt("z"), pt("m"), range("5", true, "7", true) });

    KeySpace c = a.and(b);
    assertEquals(range("a", true, "z", false), c.get(0));
    assertEquals(pt("m"), c.get(1));
    // Intersection of [1,9) and [5,7] is [5,7] (closed on upper because 7 < 9 and
    // the tighter side's inclusivity wins when it's strictly smaller).
    assertEquals(range("5", true, "7", true), c.get(2));
  }

  @Test
  public void andCollapsesWhenAnyDimIsDisjoint() {
    KeySpace a = KeySpace.of(
      new KeyRange[] { pt("a"), pt("x") });
    KeySpace b = KeySpace.of(
      new KeyRange[] { pt("a"), pt("y") });
    assertTrue(a.and(b).isEmpty());
  }

  @Test
  public void andWithEmptyReturnsEmpty() {
    KeySpace a = KeySpace.everything(2);
    KeySpace none = KeySpace.empty(2);
    assertTrue(a.and(none).isEmpty());
    assertTrue(none.and(a).isEmpty());
  }

  @Test
  public void containsIdentifiesSubspaces() {
    KeySpace outer = KeySpace.of(
      new KeyRange[] { range("a", true, "z", false), KeyRange.EVERYTHING_RANGE });
    KeySpace inner = KeySpace.of(
      new KeyRange[] { range("c", true, "e", false), pt("q") });
    assertTrue(outer.contains(inner));
    assertFalse(inner.contains(outer));
  }

  @Test
  public void unionMergesWhenOneContainsOther() {
    KeySpace big = KeySpace.of(
      new KeyRange[] { range("a", true, "z", false), KeyRange.EVERYTHING_RANGE });
    KeySpace small = KeySpace.of(
      new KeyRange[] { pt("m"), pt("q") });
    Optional<KeySpace> merged = big.unionIfMergeable(small);
    assertTrue(merged.isPresent());
    assertEquals(big, merged.get());
  }

  @Test
  public void unionMergesWhenEqualOnNminus1AndOverlapping() {
    // Same dim0, overlapping dim1.
    KeySpace x = KeySpace.of(
      new KeyRange[] { pt("k"), range("1", true, "5", false) });
    KeySpace y = KeySpace.of(
      new KeyRange[] { pt("k"), range("3", true, "9", false) });
    Optional<KeySpace> merged = x.unionIfMergeable(y);
    assertTrue(merged.isPresent());
    assertEquals(range("1", true, "9", false), merged.get().get(1));
  }

  @Test
  public void unionIsNoOpWhenTwoDimsDiffer() {
    KeySpace x = KeySpace.of(
      new KeyRange[] { pt("a"), pt("1") });
    KeySpace y = KeySpace.of(
      new KeyRange[] { pt("b"), pt("2") });
    assertFalse(x.unionIfMergeable(y).isPresent());
  }

  @Test
  public void unionIsNoOpWhenDiffDimDisjoint() {
    // Same dim0, disjoint non-adjacent dim1.
    KeySpace x = KeySpace.of(
      new KeyRange[] { pt("k"), range("1", true, "3", false) });
    KeySpace y = KeySpace.of(
      new KeyRange[] { pt("k"), range("7", true, "9", false) });
    assertFalse(x.unionIfMergeable(y).isPresent());
  }

  @Test
  public void unionMergesAdjacentDisjointWithComplementaryInclusivity() {
    // [1,5) ∪ [5,9) covers [1,9) because upper of first is exclusive and lower of second is
    // inclusive on the same byte value.
    KeySpace x = KeySpace.of(
      new KeyRange[] { pt("k"), range("1", true, "5", false) });
    KeySpace y = KeySpace.of(
      new KeyRange[] { pt("k"), range("5", true, "9", false) });
    Optional<KeySpace> merged = x.unionIfMergeable(y);
    assertTrue(merged.isPresent());
    assertEquals(range("1", true, "9", false), merged.get().get(1));
  }

  /**
   * Regression for the inverted-singleton disjoint-merging bug seen in
   * SkipScanQueryIT.testOrWithMixedOrderPKs. Distinct inverted (DESC) single-key
   * byte sequences like {@code \xCD} (= '2' DESC) and {@code \xCD\xCC} (= '23' DESC)
   * must NOT merge into a range — {@code KeyRange.intersect} has a bug where
   * intersecting two inverted singletons of different byte widths returns a non-
   * empty "backward" range instead of EMPTY_RANGE, which would cause
   * {@code unionIfMergeable} to proceed to union. The explicit
   * "two distinct single-keys with different bytes are disjoint" check in
   * {@link KeySpace#unionIfMergeable} defends against this.
   */
  @Test
  public void unionOfInvertedSingletonsOfDifferentBytesDoesNotMerge() {
    byte[] cdBytes = new byte[] { (byte) 0xCD };
    byte[] cdccBytes = new byte[] { (byte) 0xCD, (byte) 0xCC };
    // Construct inverted (DESC) singleton KeyRanges.
    KeyRange invCd = KeyRange.getKeyRange(cdBytes, true, cdBytes, true, true);
    KeyRange invCdcc = KeyRange.getKeyRange(cdccBytes, true, cdccBytes, true, true);
    assertTrue("inverted single-byte must be single-key", invCd.isSingleKey());
    assertTrue("inverted two-byte must be single-key", invCdcc.isSingleKey());
    KeySpace ksCd = KeySpace.of(new KeyRange[] { invCd, KeyRange.EVERYTHING_RANGE });
    KeySpace ksCdcc = KeySpace.of(new KeyRange[] { invCdcc, KeyRange.EVERYTHING_RANGE });
    Optional<KeySpace> merged = ksCd.unionIfMergeable(ksCdcc);
    assertFalse(
      "Distinct inverted singletons must not merge even when KeyRange.intersect returns"
        + " a non-empty backward range for their inverted-byte comparison",
      merged.isPresent());
  }

  /**
   * Companion to {@link #unionOfInvertedSingletonsOfDifferentBytesDoesNotMerge}: two
   * non-inverted (ASC) singletons with different bytes also don't merge. This is the
   * analogous case without the inversion bug, asserted to document the expected
   * behavior.
   */
  @Test
  public void unionOfNonInvertedSingletonsOfDifferentBytesDoesNotMerge() {
    KeySpace ksA = KeySpace.of(new KeyRange[] { pt("a"), KeyRange.EVERYTHING_RANGE });
    KeySpace ksB = KeySpace.of(new KeyRange[] { pt("b"), KeyRange.EVERYTHING_RANGE });
    assertFalse("Distinct ASC singletons must not merge", ksA.unionIfMergeable(ksB).isPresent());
  }
}
