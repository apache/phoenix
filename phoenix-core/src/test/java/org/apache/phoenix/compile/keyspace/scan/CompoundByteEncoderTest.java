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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.keyspace.KeySpace;
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
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

/**
 * Golden tests for {@link CompoundByteEncoder}. Each test constructs a {@link KeySpace}
 * against a hand-built {@link RowKeySchema} and asserts the encoder's lower/upper byte
 * output matches the V1-equivalent shape produced by {@code ScanUtil.setKey}.
 * <p>
 * These serve two purposes:
 * <ol>
 * <li>Reference documentation — each test is a small worked example of what V1's byte
 *     encoding rules produce for a specific shape.</li>
 * <li>Regression pin — when V2 shifts to calling {@link CompoundByteEncoder} from its
 *     scan-construction path, these tests are the oracle for correctness.</li>
 * </ol>
 */
public class CompoundByteEncoderTest {

  private static final byte[] SEP = new byte[] { QueryConstants.SEPARATOR_BYTE };

  private static RowKeySchema schema(Field... fields) {
    RowKeySchema.RowKeySchemaBuilder b = new RowKeySchema.RowKeySchemaBuilder(fields.length);
    for (Field f : fields) {
      b.addField(f.datum, f.nullable, f.datum.getSortOrder());
    }
    return b.build();
  }

  /** Field descriptor for schema building. */
  private static final class Field {
    final PDatum datum;
    final boolean nullable;

    Field(PDatum datum, boolean nullable) {
      this.datum = datum;
      this.nullable = nullable;
    }
  }

  private static Field field(PDataType type, Integer maxLen, SortOrder order, boolean nullable) {
    return new Field(new PDatum() {
      @Override public boolean isNullable() { return nullable; }
      @Override public PDataType getDataType() { return type; }
      @Override public Integer getMaxLength() { return maxLen; }
      @Override public Integer getScale() { return null; }
      @Override public SortOrder getSortOrder() { return order; }
    }, nullable);
  }

  private static KeyRange point(byte[] v) {
    return KeyRange.getKeyRange(v, true, v, true);
  }

  private static KeySpace space(int nDims, KeyRange... dims) {
    KeyRange[] all = new KeyRange[nDims];
    for (int i = 0; i < nDims; i++) {
      all[i] = (i < dims.length) ? dims[i] : KeyRange.EVERYTHING_RANGE;
    }
    return KeySpace.of(all);
  }

  /**
   * Two pinned var-width leading columns + inclusive-upper range on a fixed-width
   * trailing column, with an unconstrained trailing PK column.
   * <p>
   * Shape: {@code cat='c0' AND journey='j0' AND datasource=0 AND match_status <= 1} on
   * PK {@code (cat VARCHAR, journey VARCHAR, datasource SMALLINT, match_status TINYINT, extra VARCHAR)}.
   * Lower row: {@code c0·SEP·j0·SEP·\x80\x00·\x81} (byte of match_status=1).
   * Upper row: {@code nextKey(c0·SEP·j0·SEP·\x80\x00·\x81)} — the inclusive-upper bump
   *   converts `<= 1` to byte-exclusive form.
   */
  @Test
  public void pinnedPrefixPlusInclusiveUpperRangeOnFixedWidthTail() {
    RowKeySchema sch = schema(
      field(PVarchar.INSTANCE, null, SortOrder.ASC, false),
      field(PVarchar.INSTANCE, null, SortOrder.ASC, false),
      field(org.apache.phoenix.schema.types.PSmallint.INSTANCE, null, SortOrder.ASC, false),
      field(org.apache.phoenix.schema.types.PTinyint.INSTANCE, null, SortOrder.ASC, false),
      field(PVarchar.INSTANCE, null, SortOrder.ASC, true));

    byte[] c0 = Bytes.toBytes("c0");
    byte[] j0 = Bytes.toBytes("j0");
    byte[] ds0 = org.apache.phoenix.schema.types.PSmallint.INSTANCE.toBytes((short) 0);
    byte[] ms1 = org.apache.phoenix.schema.types.PTinyint.INSTANCE.toBytes((byte) 1);
    KeyRange msLeq1 = KeyRange.getKeyRange(KeyRange.UNBOUND, false, ms1, true);

    KeySpace space = space(5, point(c0), point(j0), point(ds0), msLeq1, KeyRange.EVERYTHING_RANGE);

    // Lower: match_status has UNBOUND lower on a fixed-width column, which terminates
    // encoding (no point in extending a lower bound past an unconstrained fixed-width
    // boundary — it wouldn't filter). Matches V1's setKey behavior at line 529.
    byte[] expectedLower = ByteUtil.concat(c0, SEP, j0, SEP, ds0);
    assertArrayEquals(expectedLower, CompoundByteEncoder.encodeLower(sch, space, 0));

    // Upper is inclusive → the column byte is included, then nextKey bump converts
    // `<=1` (inclusive byte-exclusive) to the HBase scan stopRow form.
    byte[] expectedUpper = ByteUtil.nextKey(ByteUtil.concat(c0, SEP, j0, SEP, ds0, ms1));
    assertArrayEquals(expectedUpper, CompoundByteEncoder.encodeUpper(sch, space, 0));
  }

  /**
   * Inclusive-upper range on a var-width PK column followed by unconstrained PK columns.
   * V1's scan stop includes a trailing SEP after the var-width column because the tail
   * has trailing PK columns; the inclusive-upper bump then applies at the SEP.
   * <p>
   * Shape: {@code cat='c0' AND score >= 4980}, PK {@code (cat VARCHAR, score DECIMAL, pk VARCHAR, sk BIGINT)}.
   * Lower row: {@code c0·SEP·score_bytes·SEP}.
   */
  @Test
  public void inclusiveLowerRangeOnVarWidthWithTrailingColumns() {
    RowKeySchema sch = schema(
      field(PVarchar.INSTANCE, null, SortOrder.ASC, false),
      field(org.apache.phoenix.schema.types.PDecimal.INSTANCE, null, SortOrder.ASC, false),
      field(PVarchar.INSTANCE, null, SortOrder.ASC, false),
      field(PLong.INSTANCE, null, SortOrder.ASC, false));

    byte[] c0 = Bytes.toBytes("c0");
    byte[] score4980 =
      org.apache.phoenix.schema.types.PDecimal.INSTANCE.toBytes(new java.math.BigDecimal("4980"));
    KeyRange scoreGte = KeyRange.getKeyRange(score4980, true, KeyRange.UNBOUND, false);

    KeySpace space = space(4, point(c0), scoreGte);

    // Lower: cat · SEP · score_bytes · SEP (trailing SEP because var-width followed by
    // more PK columns).
    byte[] expectedLower = ByteUtil.concat(c0, SEP, score4980, SEP);
    assertArrayEquals(expectedLower, CompoundByteEncoder.encodeLower(sch, space, 0));
  }

  /**
   * Single pinned leading column on a fixed-width CHAR PK.
   */
  @Test
  public void pointLookupOnFixedWidthLeadingColumn() {
    RowKeySchema sch = schema(
      field(PChar.INSTANCE, 3, SortOrder.ASC, false),
      field(PInteger.INSTANCE, null, SortOrder.ASC, false));

    byte[] abc = PChar.INSTANCE.toBytes("abc");
    KeySpace space = space(2, point(abc));

    // For a point range (single-key, inclusive both sides) on the leading column, lower
    // row is just the column bytes. Upper is nextKey of the same.
    assertArrayEquals(abc, CompoundByteEncoder.encodeLower(sch, space, 0));
    assertArrayEquals(ByteUtil.nextKey(abc), CompoundByteEncoder.encodeUpper(sch, space, 0));
  }

  /**
   * All-everything (nothing constrained) returns {@link KeyRange#UNBOUND}.
   */
  @Test
  public void allEverythingReturnsUnbound() {
    RowKeySchema sch = schema(
      field(PChar.INSTANCE, 3, SortOrder.ASC, false),
      field(PChar.INSTANCE, 3, SortOrder.ASC, false));
    KeySpace space = space(2);
    assertArrayEquals(KeyRange.UNBOUND, CompoundByteEncoder.encodeLower(sch, space, 0));
    assertArrayEquals(KeyRange.UNBOUND, CompoundByteEncoder.encodeUpper(sch, space, 0));
  }

  /**
   * Range with exclusive upper on a var-width column followed by unconstrained PK
   * columns. Exclusive-upper stops encoding — no trailing SEP, no bump.
   */
  @Test
  public void exclusiveUpperRangeStopsAtTheBoundary() {
    RowKeySchema sch = schema(
      field(PVarchar.INSTANCE, null, SortOrder.ASC, false),
      field(org.apache.phoenix.schema.types.PDecimal.INSTANCE, null, SortOrder.ASC, false),
      field(PVarchar.INSTANCE, null, SortOrder.ASC, false));

    byte[] c0 = Bytes.toBytes("c0");
    byte[] score5000 =
      org.apache.phoenix.schema.types.PDecimal.INSTANCE.toBytes(new java.math.BigDecimal("5000"));
    KeyRange scoreLt = KeyRange.getKeyRange(KeyRange.UNBOUND, false, score5000, false);

    KeySpace space = space(3, point(c0), scoreLt);

    // Upper: cat · SEP · score_bytes (no trailing SEP, no bump — exclusive upper).
    byte[] expectedUpper = ByteUtil.concat(c0, SEP, score5000);
    assertArrayEquals(expectedUpper, CompoundByteEncoder.encodeUpper(sch, space, 0));
  }
}
