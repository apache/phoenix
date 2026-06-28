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

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.keyspace.KeyRangeExtractor.Result;
import org.apache.phoenix.query.KeyRange;
import org.junit.Test;

public class KeyRangeExtractorTest {

  private static KeyRange pt(String v) {
    byte[] b = Bytes.toBytes(v);
    return KeyRange.getKeyRange(b, true, b, true);
  }

  private static KeyRange range(String lo, boolean loInc, String hi, boolean hiInc) {
    return KeyRange.getKeyRange(Bytes.toBytes(lo), loInc, Bytes.toBytes(hi), hiInc);
  }

  private static KeySpace ks(int n, KeyRange... dims) {
    KeyRange[] full = new KeyRange[n];
    for (int i = 0; i < n; i++) {
      full[i] = (i < dims.length) ? dims[i] : KeyRange.EVERYTHING_RANGE;
    }
    return KeySpace.of(full);
  }

  @Test
  public void unsatisfiableYieldsNothing() {
    Result r = KeyRangeExtractor.extract(KeySpaceList.unsatisfiable(3), 3, 50);
    assertTrue(r.isNothing());
  }

  @Test
  public void everythingYieldsEverything() {
    Result r = KeyRangeExtractor.extract(KeySpaceList.everything(3), 3, 50);
    assertTrue(r.isEverything());
  }

  @Test
  public void singleSpaceSingleDimProducesOneSlot() {
    KeySpaceList list = KeySpaceList.of(ks(3, pt("a")));
    Result r = KeyRangeExtractor.extract(list, 3, 50);
    assertEquals(1, r.ranges.size());
    assertEquals(1, r.ranges.get(0).size());
    assertEquals(pt("a"), r.ranges.get(0).get(0));
    assertFalse(r.useSkipScan);
  }

  @Test
  public void singleSpaceTwoLeadingDimsProducesTwoSlots() {
    KeySpaceList list = KeySpaceList.of(ks(3, pt("a"), pt("b")));
    Result r = KeyRangeExtractor.extract(list, 3, 50);
    assertEquals(2, r.ranges.size());
    assertEquals(pt("a"), r.ranges.get(0).get(0));
    assertEquals(pt("b"), r.ranges.get(1).get(0));
  }

  @Test
  public void gapDimStopsAtLeadingEverything() {
    // dim 0 = 'a', dim 1 = EVERYTHING, dim 2 = 'c' — only dim 0 makes it into the scan.
    KeyRange[] dims = { pt("a"), KeyRange.EVERYTHING_RANGE, pt("c") };
    KeySpace ks = KeySpace.of(dims);
    KeySpaceList list = KeySpaceList.of(ks);
    Result r = KeyRangeExtractor.extract(list, 3, 50);
    assertEquals(1, r.ranges.size());
    assertEquals(pt("a"), r.ranges.get(0).get(0));
  }

  @Test
  public void twoSpacesSameLeadingDimEmitMultipleRangesAndForceSkipScan() {
    KeySpaceList list = KeySpaceList.of(ks(3, pt("a")), ks(3, pt("b")));
    Result r = KeyRangeExtractor.extract(list, 3, 50);
    assertEquals(1, r.ranges.size());
    List<KeyRange> slot0 = r.ranges.get(0);
    assertEquals(2, slot0.size());
    assertTrue(slot0.contains(pt("a")));
    assertTrue(slot0.contains(pt("b")));
    assertTrue(r.useSkipScan);
  }

  @Test
  public void adjacentRangesAreCoalesced() {
    // [1,5) and [5,9) should coalesce to [1,9).
    KeySpaceList list = KeySpaceList.of(
      ks(2, range("1", true, "5", false)),
      ks(2, range("5", true, "9", false)));
    Result r = KeyRangeExtractor.extract(list, 2, 50);
    assertEquals(1, r.ranges.size());
    List<KeyRange> slot0 = r.ranges.get(0);
    assertEquals(1, slot0.size());
    assertEquals(range("1", true, "9", false), slot0.get(0));
    assertFalse(r.useSkipScan);
  }

  @Test
  public void cartesianBoundTruncatesTrailingSlots() {
    // Build a key space list whose spaces all share slot 0 so they don't trigger the
    // multi-dim divergence bail-out, but whose slot 1 has multiple ranges that push the
    // running product past the bound. The extractor emits slots up to and including the
    // one that tripped the bound, then stops.
    KeySpaceList list = KeySpaceList.of(
      ks(3, pt("a"), pt("1")),
      ks(3, pt("a"), pt("2")),
      ks(3, pt("a"), pt("3")));
    Result r = KeyRangeExtractor.extract(list, 3, 1);
    // With bound=1 the trip happens at slot 1 (1 * 3 = 3 > 1), so slots 0 and 1 are emitted
    // and further trailing slots (none here) are truncated.
    assertEquals(2, r.ranges.size());
    assertEquals(1, r.ranges.get(0).size());
    assertEquals(3, r.ranges.get(1).size());
  }

  @Test
  public void multiDimDivergenceProducesPerSlotProjection() {
    // Two spaces that differ on both dim 0 and dim 1. Per the per-slot emission rule,
    // the extractor projects each slot independently: slot 0 = {a, b}, slot 1 = {1, 2}.
    // The resulting cartesian is 4 compound keys (the 2-of-them over-approximation — the
    // residual filter rejects mismatched pairs at scan time).
    KeySpaceList list = KeySpaceList.of(
      ks(2, pt("a"), pt("1")),
      ks(2, pt("b"), pt("2")));
    Result r = KeyRangeExtractor.extract(list, 2, 50);
    assertEquals(2, r.ranges.size());
    assertEquals(2, r.ranges.get(0).size());
    assertEquals(2, r.ranges.get(1).size());
  }

  @Test
  public void multiSpaceTrailingRangesPreservedUnderRelaxedBound() {
    KeySpaceList list = KeySpaceList.of(
      ks(3, pt("a"), pt("1")),
      ks(3, pt("a"), pt("2")));
    Result r = KeyRangeExtractor.extract(list, 3, 50);
    // Slot 0: {a}, slot 1: {1, 2}. OR within each slot.
    assertEquals(2, r.ranges.size());
    assertEquals(1, r.ranges.get(0).size());
    assertEquals(2, r.ranges.get(1).size());
  }

  /**
   * Build a minimal {@link org.apache.phoenix.schema.RowKeySchema} with {@code n} fixed-
   * width ASC fields of {@code fieldLen} bytes each. Used to exercise the schema-based
   * compound-emission path.
   */
  private static org.apache.phoenix.schema.RowKeySchema fixedWidthSchema(int n, int fieldLen) {
    org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder b =
      new org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder(n);
    for (int i = 0; i < n; i++) {
      b.addField(new org.apache.phoenix.schema.PDatum() {
        @Override public boolean isNullable() { return false; }
        @Override public org.apache.phoenix.schema.types.PDataType getDataType() {
          return org.apache.phoenix.schema.types.PChar.INSTANCE;
        }
        @Override public Integer getMaxLength() { return fieldLen; }
        @Override public Integer getScale() { return null; }
        @Override public org.apache.phoenix.schema.SortOrder getSortOrder() {
          return org.apache.phoenix.schema.SortOrder.ASC;
        }
      }, false, org.apache.phoenix.schema.SortOrder.ASC);
    }
    return b.build();
  }

  /**
   * Regression for SkipScanQueryIT.testPreSplitCompositeFixedKey: compound emission is
   * UNSAFE when any space has a non-single-key dim followed by another constrained dim
   * in the compound window. For a query like {@code key_1 in [000,200) AND key_2 in
   * [aabb,aadd)} the byte interval {@code [000aabb, 200aadd)} is lex-wider than the
   * conjunction (rows like {@code (100, aaaa)} lie in the compound but don't match
   * key_2's range). The extractor must route to {@code emitV1Projection} which emits
   * per-column slots with a SkipScanFilter enforcing both constraints per-row.
   */
  @Test
  public void compoundIsUnsafeWhenLeadingRangeFollowedByTrailingRange() {
    // Simulate `key_1 in [000, 200) AND key_2 in [aabb, aadd)` on a 2-col fixed-width PK.
    org.apache.phoenix.schema.RowKeySchema schema = fixedWidthSchema(2, 3);
    KeyRange key1Range = range("000", true, "200", false);
    KeyRange key2Range = KeyRange.getKeyRange(Bytes.toBytes("aab"), true,
      Bytes.toBytes("aad"), false);
    KeySpaceList list = KeySpaceList.of(KeySpace.of(new KeyRange[] { key1Range, key2Range }));
    Result r = KeyRangeExtractor.extract(list, 2, 50000, 0, schema);
    // Must fall back to per-column projection (2 slots), with useSkipScan = true so the
    // downstream SkipScanFilter rejects rows where key_2 is out of range.
    assertEquals("Range+range compound must fall back to per-column projection",
      2, r.ranges.size());
    assertTrue("Per-column projection for range+range must force useSkipScan",
      r.useSkipScan);
  }

  /**
   * Regression for SkipScanQueryIT.testNullInfiniteLoop: {@code COL1 in [A,B] AND
   * COL2 = v} must emit a SkipScanFilter. The compound byte interval
   * {@code [A+v, B+v]} includes rows with any COL2 value in the middle of COL1's
   * range, so without a filter, rows with COL2 != v slip through. V2 previously
   * emitted no filter; this test asserts a filter is now required.
   */
  @Test
  public void compoundIsUnsafeWhenLeadingRangeFollowedByTrailingPinned() {
    org.apache.phoenix.schema.RowKeySchema schema = fixedWidthSchema(2, 3);
    KeyRange col1Range = range("100", true, "200", true);
    KeyRange col2Pinned = pt("aaa");
    KeySpaceList list = KeySpaceList.of(KeySpace.of(new KeyRange[] { col1Range, col2Pinned }));
    Result r = KeyRangeExtractor.extract(list, 2, 50000, 0, schema);
    // My implementation splits trailing-pinned when the compound has an unbounded side;
    // for a fully bounded range + pinned, the compound gate routes to V1 projection.
    // Either way, useSkipScan should be true because a SkipScanFilter is required to
    // enforce per-row COL2 = v.
    assertTrue("range+pinned must emit SkipScanFilter", r.useSkipScan);
  }

  /**
   * Regression for SkipScanQueryIT.testOrWithMixedOrderPKs: when compound emission runs
   * for an all-single-key shape on a DESC var-length leading PK with trailing EVERYTHING
   * dims, the trailing DESC separator byte from {@code ScanUtil.getMinKey/getMaxKey}
   * must be stripped. Otherwise the emitted point bytes are 2 bytes ({@code \xCD\xFF})
   * when the stored rows have PK bytes {@code \xCD\xFF\x??} — the SkipScanFilter
   * compares against the full row bytes and the extra trailing separator causes the
   * filter to miss all matching rows (scan returns 0 rows).
   * <p>
   * This test doesn't assert the stripping directly (that's a byte-level detail of
   * compound emission); the integration test
   * {@code SkipScanQueryIT.testOrWithMixedOrderPKs} verifies the end-to-end result.
   * Here we verify the structural precondition: the extractor emits a single compound
   * slot with multiple point ranges when given an all-single-key OR chain.
   */
  @Test
  public void allSingleKeyCompoundOnLeadingDimEmitsPointKeyRanges() {
    org.apache.phoenix.schema.RowKeySchema schema = fixedWidthSchema(2, 1);
    // 3 distinct single-key points on dim 0.
    KeyRange p1 = KeyRange.getKeyRange(new byte[] { (byte) 0xC7 });
    KeyRange p2 = KeyRange.getKeyRange(new byte[] { (byte) 0xC9 });
    KeyRange p3 = KeyRange.getKeyRange(new byte[] { (byte) 0xCA });
    KeySpaceList list = KeySpaceList.of(
      KeySpace.single(0, p1, 2),
      KeySpace.single(0, p2, 2),
      KeySpace.single(0, p3, 2));
    Result r = KeyRangeExtractor.extract(list, 2, 50000, 0, schema);
    // Expect: 1 slot with 3 point ranges (compound emission on all-single-key),
    // useSkipScan=true because coalesced.size() > 1.
    assertEquals("Single compound slot expected for all-single-key points",
      1, r.ranges.size());
    assertEquals("Three distinct points must be preserved", 3, r.ranges.get(0).size());
    assertTrue("Multiple points in one slot must force useSkipScan", r.useSkipScan);
    for (KeyRange kr : r.ranges.get(0)) {
      assertTrue("Each emitted range must be a single-key point", kr.isSingleKey());
    }
  }

  /**
   * Regression for the trailing-pinned + unbounded-range shape. Pattern:
   * {@code a='aaa' AND b >= 'bbb' AND c='ccc' AND d='ddd'} on a 4-col fixed-width PK.
   * <p>
   * The compound emission path splits the trailing pinned dims (c, d) off the compound
   * window because b's range has an unbounded upper side. The resulting compound
   * [aaabbb, ∞) is lex-wider than the conjunction — rows with leading bytes inside the
   * compound but whose c/d don't equal the pinned values slip through unless a
   * SkipScanFilter enforces per-row equality. Because the coalesced compound has only
   * one range, {@code coalesced.size() > 1} is false; the extractor must still force
   * {@code useSkipScan = true} when trailing pinned slots were split off.
   */
  @Test
  public void unboundedRangeWithTrailingPinnedForcesSkipScan() {
    org.apache.phoenix.schema.RowKeySchema schema = fixedWidthSchema(4, 3);
    KeyRange a = pt("aaa");
    KeyRange b = KeyRange.getKeyRange(Bytes.toBytes("bbb"), true, KeyRange.UNBOUND, false);
    KeyRange c = pt("ccc");
    KeyRange d = pt("ddd");
    KeySpaceList list = KeySpaceList.of(KeySpace.of(new KeyRange[] { a, b, c, d }));
    Result r = KeyRangeExtractor.extract(list, 4, 50000, 0, schema);
    // Expect the extractor to emit the compound plus 2 trailing pinned slots (or fall
    // back to V1 projection). Either way, useSkipScan must be true so the filter
    // rejects rows whose c/d don't match the pinned values.
    assertTrue(
      "Unbounded range with trailing pinned dims must force SkipScanFilter to enforce "
        + "per-row equality on split-off pinned slots",
      r.useSkipScan);
  }

  /**
   * Regression for VarBinaryEncoded1IT: when the emitV1Projection path has three
   * constrained slots where the middle slot is a non-point range AND the trailing
   * slot is IS_NOT_NULL_RANGE, the start/stop rows alone can't enforce the trailing
   * IS_NOT_NULL — rows with a null PK3 sneak through. Must force
   * {@code useSkipScan = true} so {@link org.apache.phoenix.filter.SkipScanFilter}
   * rejects them per row.
   * <p>
   * Pattern: {@code PK1 = v AND PK2 BETWEEN a AND b AND PK3 IS NOT NULL} on a
   * 3-col VARBINARY_ENCODED PK.
   */
  @Test
  public void pinnedPlusRangePlusIsNotNullForcesSkipScan() {
    org.apache.phoenix.schema.RowKeySchema schema = fixedWidthSchema(3, 4);
    KeyRange pk1Pinned = pt("aaaa");
    KeyRange pk2Between = range("bbbb", true, "cccc", true);
    KeyRange pk3NotNull = KeyRange.IS_NOT_NULL_RANGE;
    KeySpaceList list =
      KeySpaceList.of(KeySpace.of(new KeyRange[] { pk1Pinned, pk2Between, pk3NotNull }));
    Result r = KeyRangeExtractor.extract(list, 3, 50000, 0, schema);
    assertEquals("Three slots expected (pinned + range + IS_NOT_NULL)", 3, r.ranges.size());
    assertTrue(
      "pinned + range + IS_NOT_NULL must force SkipScanFilter to enforce per-row PK3 non-null",
      r.useSkipScan);
  }

  /**
   * Build a 2-field schema: (ASC BIGINT fixed 8 bytes, DESC DECIMAL variable-width).
   * Mirrors SortOrderIT.testSkipScanCompare's t_null_DECIMAL_DESC shape.
   */
  private static org.apache.phoenix.schema.RowKeySchema mixedCmpSchema() {
    org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder b =
      new org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder(2);
    b.addField(new org.apache.phoenix.schema.PDatum() {
      @Override public boolean isNullable() { return false; }
      @Override public org.apache.phoenix.schema.types.PDataType getDataType() {
        return org.apache.phoenix.schema.types.PLong.INSTANCE;
      }
      @Override public Integer getMaxLength() { return null; }
      @Override public Integer getScale() { return null; }
      @Override public org.apache.phoenix.schema.SortOrder getSortOrder() {
        return org.apache.phoenix.schema.SortOrder.ASC;
      }
    }, false, org.apache.phoenix.schema.SortOrder.ASC);
    b.addField(new org.apache.phoenix.schema.PDatum() {
      @Override public boolean isNullable() { return true; }
      @Override public org.apache.phoenix.schema.types.PDataType getDataType() {
        return org.apache.phoenix.schema.types.PDecimal.INSTANCE;
      }
      @Override public Integer getMaxLength() { return null; }
      @Override public Integer getScale() { return null; }
      @Override public org.apache.phoenix.schema.SortOrder getSortOrder() {
        return org.apache.phoenix.schema.SortOrder.DESC;
      }
    }, true, org.apache.phoenix.schema.SortOrder.DESC);
    return b.build();
  }

  /**
   * Regression for SortOrderIT.testSkipScanCompare: when the compound window spans
   * fields whose {@link org.apache.phoenix.util.ScanUtil#getComparator} differs
   * (e.g. ASC fixed-width leading + DESC variable-width trailing), the extractor must
   * fall back to per-column projection. Otherwise the single compound slot is walked by
   * {@link org.apache.phoenix.filter.SkipScanFilter} using only the leading field's
   * comparator, which silently mismatches DESC-variable-width bytes in the trailing
   * dim and misses rows whose trailing DESC values have a different byte length than
   * the slot's upper-bound bytes.
   */
  @Test
  public void compoundWithMixedComparatorsFallsBackToPerSlot() {
    org.apache.phoenix.schema.RowKeySchema schema = mixedCmpSchema();
    // k1 IN (2, 4): two spaces differing on leading dim; each has a shared trailing
    // range k2 > 1.0 (represented as an inverted DESC range on dim 1).
    byte[] k1_2 = org.apache.phoenix.schema.types.PLong.INSTANCE.toBytes(2L,
      org.apache.phoenix.schema.SortOrder.ASC);
    byte[] k1_4 = org.apache.phoenix.schema.types.PLong.INSTANCE.toBytes(4L,
      org.apache.phoenix.schema.SortOrder.ASC);
    KeyRange k1Eq2 = KeyRange.getKeyRange(k1_2, true, k1_2, true);
    KeyRange k1Eq4 = KeyRange.getKeyRange(k1_4, true, k1_4, true);
    byte[] k2_10_desc = org.apache.phoenix.schema.types.PDecimal.INSTANCE.toBytes(
      new java.math.BigDecimal("1.0"), org.apache.phoenix.schema.SortOrder.DESC);
    // k2 > 1.0 on DESC: scan range is [UNBOUND, inverted-1.0) in byte terms.
    KeyRange k2RangeDesc = KeyRange.getKeyRange(KeyRange.UNBOUND, false, k2_10_desc, false);
    KeySpaceList list = KeySpaceList.of(
      KeySpace.of(new KeyRange[] { k1Eq2, k2RangeDesc }),
      KeySpace.of(new KeyRange[] { k1Eq4, k2RangeDesc }));
    Result r = KeyRangeExtractor.extract(list, 2, 50000, 0, schema);
    assertEquals("Expected per-column projection (2 slots) due to mixed comparators",
      2, r.ranges.size());
    assertEquals("Leading slot must union the IN-list points", 2, r.ranges.get(0).size());
    assertEquals("Trailing slot must preserve the DESC range", 1, r.ranges.get(1).size());
  }
}
