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
package org.apache.phoenix.compile.keyspace.scan;

import static org.junit.Assert.assertArrayEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;

/**
 * Differential validation of {@link CompoundByteEncoder} for DESC field shapes against
 * V1's {@link ScanUtil#getMinKey} / {@link ScanUtil#getMaxKey}.
 * <p>
 * Pins down V1's byte output for DESC-column queries so the encoder can match, shape by
 * shape. The parity harness previously excluded DESC fields because
 * {@link WhereOptimizerV2EncoderParityTest} surfaced a divergence on
 * {@code testDescDecimalRange}: V1 appends a trailing {@code 0xff} DESC separator for an
 * UNBOUND-lower range on a fixed-width DESC column, which the encoder doesn't.
 * <p>
 * These tests are the proof obligations the encoder must satisfy before the DESC
 * exclusion can be lifted.
 */
public class CompoundByteEncoderDescDifferentialTest {

  /**
   * Point lookup on an ASC fixed-width leading column followed by an exclusive-upper range
   * on a DESC fixed-width trailing column. Mirrors the shape from
   * {@code testDescDecimalRange}'s {@code k1=1 AND k2>1.0} branch.
   */
  @Test
  public void pointOnAscPlusExclusiveUpperRangeOnDescFixed() {
    RowKeySchema sch = schema(
      field(PLong.INSTANCE, 8, SortOrder.ASC),
      field(PDecimal.INSTANCE, null, SortOrder.DESC));
    byte[] one = PLong.INSTANCE.toBytes(1L);
    // DESC-encoded "k2 > 1.0" means lower bound is UNBOUND, upper is exclusive "DESC(1.0)".
    // PDecimal stored DESC: invert the bytes of 1.0.
    byte[] dec1Raw = PDecimal.INSTANCE.toBytes(BigDecimal.valueOf(1.0));
    byte[] dec1Inv = invert(dec1Raw);

    KeyRange aEqOne = KeyRange.getKeyRange(one, true, one, true);
    KeyRange bDescLt = KeyRange.getKeyRange(KeyRange.UNBOUND, false, dec1Inv, false);
    KeySpace space = space(2, aEqOne, bDescLt);
    assertAgree(sch, space);
  }

  /**
   * Single pinned DESC leading column.
   */
  @Test
  public void pinnedDescLeadingColumn() {
    RowKeySchema sch = schema(
      field(PInteger.INSTANCE, 4, SortOrder.DESC),
      field(PLong.INSTANCE, 8, SortOrder.ASC));
    byte[] fiveAsc = PInteger.INSTANCE.toBytes(5);
    byte[] fiveDesc = invert(fiveAsc);
    KeyRange point = KeyRange.getKeyRange(fiveDesc, true, fiveDesc, true);
    KeySpace space = space(2, point);
    assertAgree(sch, space);
  }

  /**
   * Reproduces the exact shape the parity harness surfaced as a divergence on
   * {@code WhereOptimizerTest.testDescDecimalRange}: point lookup on a fixed-width ASC
   * leading column + an exclusive-upper range with UNBOUND lower on a DESC variable-width
   * DECIMAL trailing column, where the DESC upper is encoded as a single-byte inverted
   * value.
   * <p>
   * V1 appends a trailing {@code 0xff} DESC separator on the LOWER output; the encoder
   * (in its pre-fix state) would not. The live KeySpace as captured from the parity check:
   * <pre>
   * KeySpace[\x80\x00\x00\x00\x00\x00\x00\x01, (* - &gt;\xFD)]
   * </pre>
   */
  @Test
  public void liveShapeAscPointPlusDescVarWidthExclusiveUpper() {
    RowKeySchema sch = schema(
      field(PLong.INSTANCE, 8, SortOrder.ASC),
      field(PDecimal.INSTANCE, null, SortOrder.DESC));
    byte[] k1One = PLong.INSTANCE.toBytes(1L);
    // Matches the KeySpace captured from the live failure: single-byte upper 0xFD.
    byte[] fd = new byte[] { (byte) 0xFD };
    KeyRange aEqOne = KeyRange.getKeyRange(k1One, true, k1One, true);
    KeyRange bDescLt = KeyRange.getKeyRange(KeyRange.UNBOUND, false, fd, false);
    KeySpace space = space(2, aEqOne, bDescLt);
    assertAgree(sch, space);
  }

  /**
   * Multi-space variant of the live-shape case: {@code k1 IN (1, 2) AND k2 > 1.0} with
   * {@code k2} DESC var-width. Two spaces — one per k1 value. Exercises the list-level
   * encoder path against per-space V1 byte-lex-min/max reference.
   */
  @Test
  public void liveShapeMultiSpaceAscInPlusDescVarWidthExclusiveUpper() {
    RowKeySchema sch = schema(
      field(PLong.INSTANCE, 8, SortOrder.ASC),
      field(PDecimal.INSTANCE, null, SortOrder.DESC));
    byte[] k1One = PLong.INSTANCE.toBytes(1L);
    byte[] k1Two = PLong.INSTANCE.toBytes(2L);
    byte[] fd = new byte[] { (byte) 0xFD };
    KeyRange aEqOne = KeyRange.getKeyRange(k1One, true, k1One, true);
    KeyRange aEqTwo = KeyRange.getKeyRange(k1Two, true, k1Two, true);
    KeyRange bDescLt = KeyRange.getKeyRange(KeyRange.UNBOUND, false, fd, false);
    KeySpace b1 = space(2, aEqOne, bDescLt);
    KeySpace b2 = space(2, aEqTwo, bDescLt);
    org.apache.phoenix.compile.keyspace.KeySpaceList list =
      org.apache.phoenix.compile.keyspace.KeySpaceList.of(b1, b2);
    byte[] refLower = null;
    byte[] refUpper = null;
    for (KeySpace s : list.spaces()) {
      List<List<KeyRange>> slots = toSlots(s);
      int[] slotSpan = new int[slots.size()];
      byte[] lo = ScanUtil.getMinKey(sch, slots, slotSpan);
      byte[] hi = ScanUtil.getMaxKey(sch, slots, slotSpan);
      if (lo == KeyRange.UNBOUND || lo.length == 0) {
        refLower = KeyRange.UNBOUND;
      } else if (refLower == null || (refLower != KeyRange.UNBOUND
        && org.apache.hadoop.hbase.util.Bytes.compareTo(lo, refLower) < 0)) {
        refLower = lo;
      }
      if (hi == KeyRange.UNBOUND || hi.length == 0) {
        refUpper = KeyRange.UNBOUND;
      } else if (refUpper == null || (refUpper != KeyRange.UNBOUND
        && org.apache.hadoop.hbase.util.Bytes.compareTo(hi, refUpper) > 0)) {
        refUpper = hi;
      }
    }
    byte[] encLower = CompoundByteEncoder.encodeListLower(sch, list, 0);
    byte[] encUpper = CompoundByteEncoder.encodeListUpper(sch, list, 0);
    assertArrayEquals("list lower bytes must match per-space min of V1 getMinKey",
      refLower, encLower);
    assertArrayEquals("list upper bytes must match per-space max of V1 getMaxKey",
      refUpper, encUpper);
  }

  /**
   * DESC range with both bounds specified (inclusive lower, exclusive upper).
   */
  @Test
  public void boundedRangeOnDescLeading() {
    RowKeySchema sch = schema(
      field(PLong.INSTANCE, 8, SortOrder.DESC),
      field(PInteger.INSTANCE, 4, SortOrder.ASC));
    byte[] tenDesc = invert(PLong.INSTANCE.toBytes(10L));
    byte[] fiveDesc = invert(PLong.INSTANCE.toBytes(5L));
    // DESC-ordered range: "lower" bytes are the larger original value.
    KeyRange r = KeyRange.getKeyRange(tenDesc, true, fiveDesc, false);
    KeySpace space = space(2, r);
    assertAgree(sch, space);
  }

  // ------- helpers -------

  private static byte[] invert(byte[] bytes) {
    byte[] out = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      out[i] = (byte) (~bytes[i]);
    }
    return out;
  }

  private static void assertAgree(RowKeySchema schema, KeySpace space) {
    List<List<KeyRange>> slots = toSlots(space);
    int[] slotSpan = new int[slots.size()];
    byte[] v1Lower = ScanUtil.getMinKey(schema, slots, slotSpan);
    byte[] v1Upper = ScanUtil.getMaxKey(schema, slots, slotSpan);
    byte[] v2Lower = CompoundByteEncoder.encodeLower(schema, space, 0);
    byte[] v2Upper = CompoundByteEncoder.encodeUpper(schema, space, 0);
    assertArrayEquals("lower bytes must match V1", v1Lower, v2Lower);
    assertArrayEquals("upper bytes must match V1", v1Upper, v2Upper);
  }

  private static List<List<KeyRange>> toSlots(KeySpace space) {
    int lastConstrained = -1;
    for (int d = 0; d < space.nDims(); d++) {
      if (space.get(d) != KeyRange.EVERYTHING_RANGE) {
        lastConstrained = d;
      }
    }
    List<List<KeyRange>> out = new ArrayList<>();
    for (int d = 0; d <= lastConstrained; d++) {
      out.add(Collections.singletonList(space.get(d)));
    }
    return out;
  }

  private static RowKeySchema schema(FieldDatum... fields) {
    RowKeySchema.RowKeySchemaBuilder b = new RowKeySchema.RowKeySchemaBuilder(fields.length);
    for (FieldDatum f : fields) {
      b.addField(f.datum, false, f.datum.getSortOrder());
    }
    return b.build();
  }

  private static final class FieldDatum {
    final PDatum datum;
    FieldDatum(PDatum datum) { this.datum = datum; }
  }

  private static FieldDatum field(PDataType type, Integer maxLen, SortOrder order) {
    return new FieldDatum(new PDatum() {
      @Override public boolean isNullable() { return false; }
      @Override public PDataType getDataType() { return type; }
      @Override public Integer getMaxLength() { return maxLen; }
      @Override public Integer getScale() { return null; }
      @Override public SortOrder getSortOrder() { return order; }
    });
  }

  private static KeySpace space(int n, KeyRange... dims) {
    KeyRange[] all = new KeyRange[n];
    for (int i = 0; i < n; i++) {
      all[i] = (i < dims.length) ? dims[i] : KeyRange.EVERYTHING_RANGE;
    }
    return KeySpace.of(all);
  }
}
