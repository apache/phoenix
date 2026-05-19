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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.compile.keyspace.KeySpaceList;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

/**
 * Validates the encoder reproduces the exact scan bytes V1 produces for
 * {@code QueryCompilerTest.testRVCScanBoundaries1} case 3:
 * <pre>
 *   WHERE category = 'category_0'
 *     AND score &gt;= 4980
 *     AND (score, pk, sk) &lt; (5010, 'pk_10', 5010)
 * </pre>
 * on schema {@code (category VARCHAR, score DECIMAL, pk VARCHAR, sk BIGINT)}.
 * <p>
 * V1 expects:
 * <ul>
 * <li>startRow: {@code cat0 · SEP · dec4980 · SEP} (15 bytes)</li>
 * <li>stopRow:  {@code cat0 · SEP · dec5010 · SEP · pk10 · SEP · long5010} (28 bytes)</li>
 * </ul>
 * <p>
 * This is the shape the current V2 scan-construction path fails on: the leading SEP after
 * score is stripped. The encoder's multi-space path preserves it (because dim index 1 is
 * followed by more PK columns, the separator rule appends SEP). This test verifies the
 * encoder produces the V1-expected bytes for this exact logical KeySpaceList —
 * demonstrating that making the encoder load-bearing fixes the test.
 */
public class RVCScanBoundariesEncoderTest {

  private static final byte[] SEP = new byte[] { QueryConstants.SEPARATOR_BYTE };

  @Test
  public void testRVCScanBoundaries1Case3() {
    RowKeySchema sch = schema(
      fixed(PVarchar.INSTANCE, null),
      fixed(PDecimal.INSTANCE, null),
      fixed(PVarchar.INSTANCE, null),
      fixed(PLong.INSTANCE, 8));

    byte[] cat0 = Bytes.toBytes("category_0");
    byte[] dec4980 = PDecimal.INSTANCE.toBytes(new BigDecimal("4980"));
    byte[] dec5010 = PDecimal.INSTANCE.toBytes(new BigDecimal("5010"));
    byte[] pk10 = Bytes.toBytes("pk_10");
    byte[] long5010 = PLong.INSTANCE.toBytes(5010L);

    KeyRange catEqC0 = KeyRange.getKeyRange(cat0, true, cat0, true);

    // RVC (score, pk, sk) < (5010, 'pk_10', 5010) combined with score >= 4980 expands to
    // three branches. Branches 2 and 3 pin score=5010 (>= 4980 holds trivially).
    //
    // Branch 1: category=c0 AND 4980 <= score < 5010
    KeyRange scoreRange4980To5010 = KeyRange.getKeyRange(dec4980, true, dec5010, false);
    KeySpace b1 = space(4, catEqC0, scoreRange4980To5010,
      KeyRange.EVERYTHING_RANGE, KeyRange.EVERYTHING_RANGE);

    // Branch 2: category=c0 AND score=5010 AND pk < 'pk_10'
    KeyRange scoreEq5010 = KeyRange.getKeyRange(dec5010, true, dec5010, true);
    KeyRange pkLtPk10 = KeyRange.getKeyRange(KeyRange.UNBOUND, false, pk10, false);
    KeySpace b2 = space(4, catEqC0, scoreEq5010, pkLtPk10, KeyRange.EVERYTHING_RANGE);

    // Branch 3: category=c0 AND score=5010 AND pk='pk_10' AND sk < 5010
    KeyRange pkEqPk10 = KeyRange.getKeyRange(pk10, true, pk10, true);
    KeyRange skLt5010 = KeyRange.getKeyRange(KeyRange.UNBOUND, false, long5010, false);
    KeySpace b3 = space(4, catEqC0, scoreEq5010, pkEqPk10, skLt5010);

    KeySpaceList list = KeySpaceList.of(b1, b2, b3);

    byte[] encLower = CompoundByteEncoder.encodeListLower(sch, list, 0);
    byte[] encUpper = CompoundByteEncoder.encodeListUpper(sch, list, 0);

    byte[] expectedLower = ByteUtil.concat(cat0, SEP, dec4980, SEP);
    byte[] expectedUpper = ByteUtil.concat(cat0, SEP, dec5010, SEP, pk10, SEP, long5010);

    assertArrayEquals("encoder list lower must match V1's testRVCScanBoundaries1 case 3 startRow",
      expectedLower, encLower);
    assertArrayEquals("encoder list upper must match V1's testRVCScanBoundaries1 case 3 stopRow",
      expectedUpper, encUpper);
  }

  // ------- helpers -------

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
