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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.compile.keyspace.KeySpaceList;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;

/**
 * Differential validation of {@link CompoundByteEncoder#encodeListLower} /
 * {@link CompoundByteEncoder#encodeListUpper} against a V1-equivalent reference
 * computation: the byte-lex-min of per-space {@code ScanUtil.getMinKey} outputs for the
 * lower bound, and the byte-lex-max of per-space {@code ScanUtil.getMaxKey} outputs for
 * the upper bound.
 * <p>
 * The target shape class here is OR-of-AND expansion of RVC-inequality — the class of
 * queries the single-space encoder can't express on its own. Each {@link KeySpaceList}
 * carries multiple {@link KeySpace}s representing one lex branch of the expansion. The
 * encoder's list-level output is the bounding envelope of the union; the reference
 * computation is the same thing computed by exercising V1's per-space getMinKey/getMaxKey
 * through {@link ScanUtil}.
 * <p>
 * This mirrors the single-space differential test's methodology — if any shape here
 * diverges, it's either an encoder bug or a V1 edge case the encoder doesn't cover yet.
 */
public class CompoundByteEncoderListDifferentialTest {

  private static final byte[] SEP = new byte[] { QueryConstants.SEPARATOR_BYTE };

  /**
   * RVC inequality {@code (a, b) > (1, 5)} expanded to {@code a > 1 OR (a = 1 AND b > 5)}.
   * Two spaces: one with {@code a > 1}, one with {@code a = 1 AND b > 5}.
   */
  @Test
  public void rvcGreaterThanExpanded() {
    RowKeySchema sch = schema(
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4));
    byte[] one = PInteger.INSTANCE.toBytes(1);
    byte[] five = PInteger.INSTANCE.toBytes(5);

    KeyRange aGtOne = KeyRange.getKeyRange(one, false, KeyRange.UNBOUND, false);
    KeySpace branch1 = space(2, aGtOne, KeyRange.EVERYTHING_RANGE);

    KeyRange aEqOne = KeyRange.getKeyRange(one, true, one, true);
    KeyRange bGtFive = KeyRange.getKeyRange(five, false, KeyRange.UNBOUND, false);
    KeySpace branch2 = space(2, aEqOne, bGtFive);

    KeySpaceList list = KeySpaceList.of(branch1, branch2);
    assertListAgree(sch, list);
  }

  /**
   * RVC inequality {@code (a, b) >= (1, 5)} expanded to {@code a > 1 OR (a = 1 AND b >= 5)}.
   */
  @Test
  public void rvcGreaterOrEqualExpanded() {
    RowKeySchema sch = schema(
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4));
    byte[] one = PInteger.INSTANCE.toBytes(1);
    byte[] five = PInteger.INSTANCE.toBytes(5);

    KeyRange aGtOne = KeyRange.getKeyRange(one, false, KeyRange.UNBOUND, false);
    KeySpace branch1 = space(2, aGtOne, KeyRange.EVERYTHING_RANGE);

    KeyRange aEqOne = KeyRange.getKeyRange(one, true, one, true);
    KeyRange bGteFive = KeyRange.getKeyRange(five, true, KeyRange.UNBOUND, false);
    KeySpace branch2 = space(2, aEqOne, bGteFive);

    KeySpaceList list = KeySpaceList.of(branch1, branch2);
    assertListAgree(sch, list);
  }

  /**
   * RVC inequality {@code (a, b, c) < (5, 7, 3)} expanded to
   * {@code a < 5 OR (a = 5 AND b < 7) OR (a = 5 AND b = 7 AND c < 3)}.
   */
  @Test
  public void rvcLessThanThreeTupleExpanded() {
    RowKeySchema sch = schema(
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4));
    byte[] five = PInteger.INSTANCE.toBytes(5);
    byte[] seven = PInteger.INSTANCE.toBytes(7);
    byte[] three = PInteger.INSTANCE.toBytes(3);

    KeyRange aLtFive = KeyRange.getKeyRange(KeyRange.UNBOUND, false, five, false);
    KeySpace branch1 = space(3, aLtFive, KeyRange.EVERYTHING_RANGE, KeyRange.EVERYTHING_RANGE);

    KeyRange aEqFive = KeyRange.getKeyRange(five, true, five, true);
    KeyRange bLtSeven = KeyRange.getKeyRange(KeyRange.UNBOUND, false, seven, false);
    KeySpace branch2 = space(3, aEqFive, bLtSeven, KeyRange.EVERYTHING_RANGE);

    KeyRange bEqSeven = KeyRange.getKeyRange(seven, true, seven, true);
    KeyRange cLtThree = KeyRange.getKeyRange(KeyRange.UNBOUND, false, three, false);
    KeySpace branch3 = space(3, aEqFive, bEqSeven, cLtThree);

    KeySpaceList list = KeySpaceList.of(branch1, branch2, branch3);
    assertListAgree(sch, list);
  }

  /**
   * Pinned leading column + RVC inequality on trailing columns:
   * {@code category = 'c0' AND (score, pk, sk) > (5000, 'pk_0', 7)} expanded to
   * {@code category = 'c0' AND (score > 5000 OR (score = 5000 AND (pk, sk) > ('pk_0', 7)))}.
   * Mirrors {@code testRVCScanBoundaries1}'s first case.
   */
  @Test
  public void pinnedLeadingPlusRvcGreaterThan() {
    RowKeySchema sch = schema(
      fixed(PVarchar.INSTANCE, null),
      fixed(PInteger.INSTANCE, 4),
      fixed(PVarchar.INSTANCE, null),
      fixed(PLong.INSTANCE, 8));
    byte[] c0 = Bytes.toBytes("c0");
    byte[] i5000 = PInteger.INSTANCE.toBytes(5000);
    byte[] pk0 = Bytes.toBytes("pk_0");
    byte[] l7 = PLong.INSTANCE.toBytes(7L);

    KeyRange catEqC0 = KeyRange.getKeyRange(c0, true, c0, true);
    KeyRange scoreGt5000 = KeyRange.getKeyRange(i5000, false, KeyRange.UNBOUND, false);
    KeySpace b1 = space(4, catEqC0, scoreGt5000, KeyRange.EVERYTHING_RANGE, KeyRange.EVERYTHING_RANGE);

    KeyRange scoreEq5000 = KeyRange.getKeyRange(i5000, true, i5000, true);
    KeyRange pkGtPk0 = KeyRange.getKeyRange(pk0, false, KeyRange.UNBOUND, false);
    KeySpace b2 = space(4, catEqC0, scoreEq5000, pkGtPk0, KeyRange.EVERYTHING_RANGE);

    KeyRange pkEqPk0 = KeyRange.getKeyRange(pk0, true, pk0, true);
    KeyRange skGt7 = KeyRange.getKeyRange(l7, false, KeyRange.UNBOUND, false);
    KeySpace b3 = space(4, catEqC0, scoreEq5000, pkEqPk0, skGt7);

    KeySpaceList list = KeySpaceList.of(b1, b2, b3);
    assertListAgree(sch, list);
  }

  /**
   * OR of two equalities on leading column: {@code a = 1 OR a = 3}. Simple multi-space
   * sanity — the envelope is {@code [1, nextKey(3))}.
   */
  @Test
  public void disjointEqualitiesOnLeadingColumn() {
    RowKeySchema sch = schema(
      fixed(PInteger.INSTANCE, 4),
      fixed(PInteger.INSTANCE, 4));
    byte[] one = PInteger.INSTANCE.toBytes(1);
    byte[] three = PInteger.INSTANCE.toBytes(3);

    KeyRange aEqOne = KeyRange.getKeyRange(one, true, one, true);
    KeyRange aEqThree = KeyRange.getKeyRange(three, true, three, true);
    KeySpace b1 = space(2, aEqOne, KeyRange.EVERYTHING_RANGE);
    KeySpace b2 = space(2, aEqThree, KeyRange.EVERYTHING_RANGE);

    KeySpaceList list = KeySpaceList.of(b1, b2);
    assertListAgree(sch, list);
  }

  /**
   * OR of a fully-pinned point with a range: {@code (a=1 AND b=2) OR (a=5 AND b>=10)}.
   */
  @Test
  public void pointOrRange() {
    RowKeySchema sch = schema(
      fixed(PChar.INSTANCE, 3),
      fixed(PInteger.INSTANCE, 4));
    byte[] aaa = PChar.INSTANCE.toBytes("aaa");
    byte[] bbb = PChar.INSTANCE.toBytes("bbb");
    byte[] two = PInteger.INSTANCE.toBytes(2);
    byte[] ten = PInteger.INSTANCE.toBytes(10);

    KeyRange aEqA = KeyRange.getKeyRange(aaa, true, aaa, true);
    KeyRange aEqB = KeyRange.getKeyRange(bbb, true, bbb, true);
    KeyRange bEq2 = KeyRange.getKeyRange(two, true, two, true);
    KeyRange bGte10 = KeyRange.getKeyRange(ten, true, KeyRange.UNBOUND, false);

    KeySpace b1 = space(2, aEqA, bEq2);
    KeySpace b2 = space(2, aEqB, bGte10);

    KeySpaceList list = KeySpaceList.of(b1, b2);
    assertListAgree(sch, list);
  }

  // ------- helpers -------

  /**
   * Reference: per-space V1 encoding via {@code ScanUtil.getMinKey}/{@code getMaxKey},
   * then byte-lex-min/max across the list. Encoder output must match.
   */
  private static void assertListAgree(RowKeySchema schema, KeySpaceList list) {
    byte[] refLower = null;
    byte[] refUpper = null;
    for (KeySpace s : list.spaces()) {
      List<List<KeyRange>> slots = toSlots(s);
      int[] slotSpan = new int[slots.size()];
      byte[] lo = ScanUtil.getMinKey(schema, slots, slotSpan);
      byte[] hi = ScanUtil.getMaxKey(schema, slots, slotSpan);
      if (lo == KeyRange.UNBOUND || lo.length == 0) {
        refLower = KeyRange.UNBOUND;
      } else if (refLower != null && refLower != KeyRange.UNBOUND) {
        if (Bytes.compareTo(lo, refLower) < 0) refLower = lo;
      } else if (refLower == null) {
        refLower = lo;
      }
      if (hi == KeyRange.UNBOUND || hi.length == 0) {
        refUpper = KeyRange.UNBOUND;
      } else if (refUpper != null && refUpper != KeyRange.UNBOUND) {
        if (Bytes.compareTo(hi, refUpper) > 0) refUpper = hi;
      } else if (refUpper == null) {
        refUpper = hi;
      }
    }
    byte[] encLower = CompoundByteEncoder.encodeListLower(schema, list, 0);
    byte[] encUpper = CompoundByteEncoder.encodeListUpper(schema, list, 0);
    assertArrayEquals("list lower bytes must match per-space min of V1 getMinKey",
      refLower, encLower);
    assertArrayEquals("list upper bytes must match per-space max of V1 getMaxKey",
      refUpper, encUpper);
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

  private static FieldDatum fixed(PDataType type, Integer maxLen) {
    return new FieldDatum(new PDatum() {
      @Override public boolean isNullable() { return false; }
      @Override public PDataType getDataType() { return type; }
      @Override public Integer getMaxLength() { return maxLen; }
      @Override public Integer getScale() { return null; }
      @Override public SortOrder getSortOrder() { return SortOrder.ASC; }
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
