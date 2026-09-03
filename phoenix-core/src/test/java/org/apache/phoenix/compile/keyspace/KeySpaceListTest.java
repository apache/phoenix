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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.junit.Test;

/**
 * Algebra-level tests for {@link KeySpaceList}. Verifies AND/OR closure over lists of
 * key spaces and the merge-to-fixpoint invariant.
 */
public class KeySpaceListTest {

  private static KeyRange pt(String v) {
    byte[] b = Bytes.toBytes(v);
    return KeyRange.getKeyRange(b, true, b, true);
  }

  private static KeySpace ks2(KeyRange d0, KeyRange d1) {
    return KeySpace.of(new KeyRange[] { d0, d1 });
  }

  @Test
  public void unsatisfiableAbsorbsAnd() {
    KeySpaceList unsat = KeySpaceList.unsatisfiable(2);
    KeySpaceList any = KeySpaceList.of(ks2(pt("a"), pt("b")));
    assertTrue(unsat.and(any).isUnsatisfiable());
    assertTrue(any.and(unsat).isUnsatisfiable());
  }

  @Test
  public void everythingIsIdentityForAnd() {
    KeySpaceList all = KeySpaceList.everything(2);
    KeySpaceList some = KeySpaceList.of(ks2(pt("a"), pt("b")));
    assertEquals(some, all.and(some));
    assertEquals(some, some.and(all));
  }

  @Test
  public void everythingAbsorbsOr() {
    KeySpaceList all = KeySpaceList.everything(2);
    KeySpaceList some = KeySpaceList.of(ks2(pt("a"), pt("b")));
    assertTrue(all.or(some).isEverything());
    assertTrue(some.or(all).isEverything());
  }

  @Test
  public void andDistributesOverOr() {
    // (A OR B) AND (C OR D) = (A∧C) OR (A∧D) OR (B∧C) OR (B∧D) after merges.
    // A: pk1=1, B: pk1=2; C: pk2=x, D: pk2=y.
    KeySpaceList left = KeySpaceList.of(
      KeySpace.single(0, pt("1"), 2),
      KeySpace.single(0, pt("2"), 2));
    KeySpaceList right = KeySpaceList.of(
      KeySpace.single(1, pt("x"), 2),
      KeySpace.single(1, pt("y"), 2));
    KeySpaceList result = left.and(right);
    assertEquals(4, result.size());
    assertTrue(result.spaces().contains(ks2(pt("1"), pt("x"))));
    assertTrue(result.spaces().contains(ks2(pt("1"), pt("y"))));
    assertTrue(result.spaces().contains(ks2(pt("2"), pt("x"))));
    assertTrue(result.spaces().contains(ks2(pt("2"), pt("y"))));
  }

  @Test
  public void orConcatenatesThenMergesContainment() {
    KeySpaceList big = KeySpaceList.of(
      KeySpace.single(0, KeyRange.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("z"), false), 2));
    KeySpaceList small = KeySpaceList.of(KeySpace.single(0, pt("m"), 2));
    KeySpaceList unioned = big.or(small);
    assertEquals(1, unioned.size());
    assertEquals(big.spaces().get(0), unioned.spaces().get(0));
  }

  @Test
  public void orMergesAdjacentRangesInSameDim() {
    KeySpaceList a = KeySpaceList.of(
      ks2(KeyRange.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("5"), false), pt("k")));
    KeySpaceList b = KeySpaceList.of(
      ks2(KeyRange.getKeyRange(Bytes.toBytes("5"), true, Bytes.toBytes("9"), false), pt("k")));
    KeySpaceList unioned = a.or(b);
    assertEquals(1, unioned.size());
    KeyRange mergedDim0 = unioned.spaces().get(0).get(0);
    assertEquals(KeyRange.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("9"), false), mergedDim0);
  }

  @Test
  public void orKeepsNonMergeableSpacesSeparate() {
    KeySpaceList a = KeySpaceList.of(ks2(pt("a"), pt("1")));
    KeySpaceList b = KeySpaceList.of(ks2(pt("b"), pt("2")));
    KeySpaceList unioned = a.or(b);
    assertEquals(2, unioned.size());
  }

  @Test
  public void andDropsEmptyCrossProducts() {
    KeySpaceList a = KeySpaceList.of(
      KeySpace.single(0, pt("1"), 2),
      KeySpace.single(0, pt("2"), 2));
    KeySpaceList b = KeySpaceList.of(KeySpace.single(0, pt("2"), 2));
    KeySpaceList result = a.and(b);
    // Only pk1=2 survives after intersection.
    assertEquals(1, result.size());
    assertEquals(pt("2"), result.spaces().get(0).get(0));
  }

  @Test
  public void emptySpacesAreFilteredOut() {
    KeySpaceList list = KeySpaceList.of(
      KeySpace.single(0, pt("a"), 2),
      KeySpace.empty(2),
      KeySpace.single(0, pt("b"), 2));
    assertFalse(list.isUnsatisfiable());
    assertEquals(2, list.size());
  }

  @Test
  public void orOfOverlappingRangesCoveringEverythingCollapses() {
    // `pk1 >= 7 OR pk1 < 9` — the two overlap on [7, 9), so their union is the full
    // 1D number line. The list should collapse to EVERYTHING (a single all-dims-
    // EVERYTHING KeySpace) so downstream extraction can recognize the tautology and
    // drop the predicate from the residual filter.
    KeyRange geSeven =
      KeyRange.getKeyRange(Bytes.toBytes("7"), true, KeyRange.UNBOUND, false);
    KeyRange ltNine = KeyRange.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes("9"), false);
    KeySpaceList a = KeySpaceList.of(KeySpace.single(0, geSeven, 2));
    KeySpaceList b = KeySpaceList.of(KeySpace.single(0, ltNine, 2));
    KeySpaceList unioned = a.or(b);
    assertTrue("(pk1 >= 7) OR (pk1 < 9) should simplify to everything; got: " + unioned,
      unioned.isEverything());
  }

  /**
   * Regression for SkipScanQueryIT.testOrWithMixedOrderPKs: an OR chain of distinct
   * inverted (DESC-encoded) single-key values must NOT collapse adjacent byte prefixes
   * into a range. E.g., '2' DESC = {@code \xCD} and '23' DESC = {@code \xCD\xCC} are
   * distinct points; their OR must yield 2 spaces, not 1 merged range.
   * <p>
   * {@link KeySpaceList#mergeSingleDim} previously used {@link KeyRange#coalesce} which
   * inherits a bug in {@code KeyRange.intersect} for inverted singletons — it now uses
   * {@link KeySpace#unionIfMergeable} which correctly rejects the merge.
   */
  @Test
  public void orOfDistinctInvertedSingletonsDoesNotOverMerge() {
    byte[] cd = new byte[] { (byte) 0xCD };       // '2' DESC
    byte[] cdcc = new byte[] { (byte) 0xCD, (byte) 0xCC }; // '23' DESC
    KeyRange invCd = KeyRange.getKeyRange(cd, true, cd, true, true);
    KeyRange invCdcc = KeyRange.getKeyRange(cdcc, true, cdcc, true, true);
    KeySpaceList a = KeySpaceList.of(KeySpace.single(0, invCd, 2));
    KeySpaceList b = KeySpaceList.of(KeySpace.single(0, invCdcc, 2));
    KeySpaceList unioned = a.or(b);
    assertEquals(
      "Distinct inverted singletons should OR into 2 separate spaces, not 1 merged range",
      2, unioned.size());
  }

  /**
   * A 10-way OR chain on inverted (DESC) singletons should preserve all 10 points
   * after the merge-fixpoint pass. Mirrors the shape of SkipScanQueryIT's
   * {@code testOrWithMixedOrderPKs} where COL1 VARCHAR DESC has distinct values
   * {@code '1','2','3','4','5','6','8','12','17','23'} encoded as inverted singletons.
   */
  @Test
  public void orOfTenInvertedSingletonsPreservesAllPoints() {
    byte[][] values = new byte[][] {
      { (byte) 0xCE },               // '1'
      { (byte) 0xCD },               // '2'
      { (byte) 0xCC },               // '3'
      { (byte) 0xCB },               // '4'
      { (byte) 0xCA },               // '5'
      { (byte) 0xC9 },               // '6'
      { (byte) 0xC7 },               // '8'
      { (byte) 0xCE, (byte) 0xCD },  // '12'
      { (byte) 0xCE, (byte) 0xC8 },  // '17'
      { (byte) 0xCD, (byte) 0xCC },  // '23'
    };
    java.util.List<KeySpaceList> branches = new java.util.ArrayList<>();
    for (byte[] v : values) {
      KeyRange inv = KeyRange.getKeyRange(v, true, v, true, true);
      branches.add(KeySpaceList.of(KeySpace.single(0, inv, 2)));
    }
    KeySpaceList full = KeySpaceList.orAll(2, branches);
    assertEquals("All 10 distinct inverted singletons must remain after OR merge",
      10, full.size());
  }
}
