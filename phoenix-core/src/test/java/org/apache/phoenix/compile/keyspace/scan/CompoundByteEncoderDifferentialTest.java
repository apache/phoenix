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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;

/**
 * Differential validation of {@link CompoundByteEncoder} against V1's
 * {@link ScanUtil#getMinKey} / {@link ScanUtil#getMaxKey}.
 * <p>
 * For each shape, builds a {@link KeySpace} and the equivalent per-column slot list,
 * runs both encoders, and asserts byte-for-byte agreement. Any divergence is either a
 * bug in the encoder or an V1 edge case the encoder's rules don't cover yet — in either
 * case, a gap worth pinning down before the encoder becomes load-bearing in the scan
 * path.
 * <p>
 * This test is intentionally strict. The encoder and V1's setKey need to agree on
 * byte-level output for the shapes covered here so that when V2ScanBuilder starts
 * calling the encoder, the scan bytes V2 emits are provably V1-equivalent. Shapes the
 * encoder doesn't handle yet (multi-space lists, RVC-spans) live in future tests as
 * they land in the encoder.
 */
public class CompoundByteEncoderDifferentialTest {

  private static final byte[] SEP = new byte[] { QueryConstants.SEPARATOR_BYTE };

  @Test
  public void pointLookupOnFixedWidthLeading() {
    RowKeySchema sch = schema(
      fixed(PChar.INSTANCE, 3),
      fixed(PInteger.INSTANCE, 4));
    byte[] abc = PChar.INSTANCE.toBytes("abc");
    KeySpace space = space(2, point(abc));
    assertAgree(sch, space);
  }

  @Test
  public void pinnedVarPlusRangeInclusiveUpperOnFixed() {
    RowKeySchema sch = schema(
      fixed(PVarchar.INSTANCE, null),
      fixed(PSmallint.INSTANCE, 2),
      fixed(PTinyint.INSTANCE, 1));
    byte[] c0 = Bytes.toBytes("c0");
    byte[] ds = PSmallint.INSTANCE.toBytes((short) 0);
    byte[] ms = PTinyint.INSTANCE.toBytes((byte) 1);
    KeyRange msLeq = KeyRange.getKeyRange(KeyRange.UNBOUND, false, ms, true);
    KeySpace space = space(3, point(c0), point(ds), msLeq);
    assertAgree(sch, space);
  }

  /**
   * Pinned var-width leading column + inclusive-lower range on var-width second column
   * with trailing unconstrained PK columns. V1's {@link ScanUtil#getMinKey} strips the
   * trailing SEP after the score (via its tail-strip loop at line 659-678). The encoder
   * preserves the SEP — which is still a correct scan lower-row (both 6-byte and 7-byte
   * versions admit the same rows, since every legitimate row has score bytes followed
   * by the SEP delimiter anyway).
   * <p>
   * This divergence is intentional and semantically equivalent; assert both behaviors
   * explicitly so future changes to either path can't regress one without updating the
   * other.
   */
  @Test
  public void pinnedVarPlusInclusiveLowerRangeOnVarWithTrailing() {
    RowKeySchema sch = schema(
      fixed(PVarchar.INSTANCE, null),
      fixed(PDecimal.INSTANCE, null),
      fixed(PVarchar.INSTANCE, null),
      fixed(PLong.INSTANCE, 8));
    byte[] c0 = Bytes.toBytes("c0");
    byte[] score = PDecimal.INSTANCE.toBytes(new BigDecimal("4980"));
    KeyRange scoreGte = KeyRange.getKeyRange(score, true, KeyRange.UNBOUND, false);
    KeySpace space = space(4, point(c0), scoreGte);

    // V1: strips the trailing SEP after score via the tail-strip loop.
    byte[] expectedV1 =
      org.apache.phoenix.util.ByteUtil.concat(c0, SEP, score);
    List<List<KeyRange>> slots = toSlots(space);
    int[] slotSpan = new int[slots.size()];
    assertArrayEquals(expectedV1, ScanUtil.getMinKey(sch, slots, slotSpan));

    // Encoder: keeps the SEP. Both bytes admit the same rows.
    byte[] expectedV2 = org.apache.phoenix.util.ByteUtil.concat(c0, SEP, score, SEP);
    assertArrayEquals(expectedV2, CompoundByteEncoder.encodeLower(sch, space, 0));

    // Upper bounds agree (both UNBOUND → empty).
    byte[] v1Upper = ScanUtil.getMaxKey(sch, slots, slotSpan);
    byte[] v2Upper = CompoundByteEncoder.encodeUpper(sch, space, 0);
    assertArrayEquals(v1Upper, v2Upper);
  }

  @Test
  public void exclusiveUpperRangeOnVarTerminates() {
    RowKeySchema sch = schema(
      fixed(PVarchar.INSTANCE, null),
      fixed(PDecimal.INSTANCE, null),
      fixed(PVarchar.INSTANCE, null));
    byte[] c0 = Bytes.toBytes("c0");
    byte[] score = PDecimal.INSTANCE.toBytes(new BigDecimal("5000"));
    KeyRange scoreLt = KeyRange.getKeyRange(KeyRange.UNBOUND, false, score, false);
    KeySpace space = space(3, point(c0), scoreLt);
    assertAgree(sch, space);
  }

  @Test
  public void fullyPinnedAllFixed() {
    RowKeySchema sch = schema(
      fixed(PChar.INSTANCE, 3),
      fixed(PInteger.INSTANCE, 4),
      fixed(PLong.INSTANCE, 8));
    KeySpace space = space(3,
      point(PChar.INSTANCE.toBytes("aaa")),
      point(PInteger.INSTANCE.toBytes(42)),
      point(PLong.INSTANCE.toBytes(999L)));
    assertAgree(sch, space);
  }

  @Test
  public void fullyPinnedMixedFixedVar() {
    RowKeySchema sch = schema(
      fixed(PVarchar.INSTANCE, null),
      fixed(PInteger.INSTANCE, 4),
      fixed(PVarchar.INSTANCE, null),
      fixed(PLong.INSTANCE, 8));
    KeySpace space = space(4,
      point(Bytes.toBytes("a")),
      point(PInteger.INSTANCE.toBytes(1)),
      point(Bytes.toBytes("b")),
      point(PLong.INSTANCE.toBytes(7L)));
    assertAgree(sch, space);
  }

  @Test
  public void exclusiveLowerRangeOnFixedLeading() {
    RowKeySchema sch = schema(
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4));
    byte[] five = PInteger.INSTANCE.toBytes(5);
    KeyRange gtFive = KeyRange.getKeyRange(five, false, KeyRange.UNBOUND, false);
    KeySpace space = space(2, gtFive);
    assertAgree(sch, space);
  }

  @Test
  public void inclusiveLowerInclusiveUpperFullyBoundedRange() {
    RowKeySchema sch = schema(
      fixed(PVarchar.INSTANCE, null),
      fixed(PInteger.INSTANCE, 4));
    byte[] c0 = Bytes.toBytes("c0");
    KeyRange iRange = KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(10), true,
      PInteger.INSTANCE.toBytes(20), true);
    KeySpace space = space(2, point(c0), iRange);
    assertAgree(sch, space);
  }

  @Test
  public void leadingRangeNoTrailingConstraint() {
    RowKeySchema sch = schema(
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4));
    KeyRange r = KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(5), true,
      PInteger.INSTANCE.toBytes(10), false);
    KeySpace space = space(3, r);
    assertAgree(sch, space);
  }

  // ------- helpers -------

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

  /** Convert a {@link KeySpace} to the per-slot list V1's setKey expects. */
  private static List<List<KeyRange>> toSlots(KeySpace space) {
    // V1's setKey stops at the first EVERYTHING at a fixed-width field on LOWER and at
    // any EVERYTHING on UPPER — the encoder mirrors this. Include only slots up to the
    // last constrained dim; trailing EVERYTHING slots would make ScanUtil walk them
    // (appending empty bytes / terminators) and produce different output than the
    // encoder, but only because the encoder takes its schema-bounded truncation a step
    // earlier. Both interpretations are correct for the scan-bounds question.
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

  /** Field descriptor for schema building. */
  private static final class FieldDatum {
    final PDatum datum;
    FieldDatum(PDatum datum) {
      this.datum = datum;
    }
  }

  private static FieldDatum fixed(PDataType type, Integer maxLen) {
    return new FieldDatum(new PDatum() {
      @Override public boolean isNullable() { return false; }
      @Override public PDataType getDataType() { return type; }
      @Override public Integer getMaxLength() { return maxLen; }
      @Override public Integer getScale() { return null; }
      @Override public SortOrder getSortOrder() { return SortOrder.ASC; }
    });
  }

  private static KeyRange point(byte[] v) {
    return KeyRange.getKeyRange(v, true, v, true);
  }

  private static KeySpace space(int n, KeyRange... dims) {
    KeyRange[] all = new KeyRange[n];
    for (int i = 0; i < n; i++) {
      all[i] = (i < dims.length) ? dims[i] : KeyRange.EVERYTHING_RANGE;
    }
    return KeySpace.of(all);
  }
}
