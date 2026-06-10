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
package org.apache.phoenix.filter;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.junit.Test;

/**
 * Unit tests for {@link CDCVersionFilter}, which keeps, per column, the cells within the per-row
 * change-timestamp band {@code [min, max]} plus the first cell below {@code min} (the pre-image),
 * and always keeps DeleteFamily markers.
 */
public class CDCVersionFilterTest {

  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] CF1 = Bytes.toBytes("cf");
  private static final byte[] CF2 = Bytes.toBytes("cf2");
  private static final byte[] CQ1 = Bytes.toBytes("cq1");
  private static final byte[] CQ2 = Bytes.toBytes("cq2");
  private static final byte[] VAL = Bytes.toBytes("v");

  private static final byte[] CF = CF1;

  private Cell put(byte[] row, byte[] cf, byte[] cq, long ts) {
    return new KeyValue(row, cf, cq, ts, KeyValue.Type.Put, VAL);
  }

  private Cell deleteColumn(byte[] row, byte[] cf, byte[] cq, long ts) {
    return new KeyValue(row, cf, cq, ts, KeyValue.Type.DeleteColumn);
  }

  private Cell pointDelete(byte[] row, byte[] cf, byte[] cq, long ts) {
    return new KeyValue(row, cf, cq, ts, KeyValue.Type.Delete);
  }

  private Cell deleteFamily(byte[] row, byte[] cf, long ts) {
    return new KeyValue(row, cf, null, ts, KeyValue.Type.DeleteFamily);
  }

  private static long[] band(long min, long max) {
    return new long[] { min, max };
  }

  private CDCVersionFilter createFilter(Map<ImmutableBytesPtr, long[]> rangeMap) {
    return new CDCVersionFilter(rangeMap);
  }

  private Map<ImmutableBytesPtr, long[]> singleRow(byte[] row, long min, long max) {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(row), band(min, max));
    return map;
  }

  @Test
  public void testSingleChangeKeepsChangeAndPreImage() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100))); // change
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80))); // redundant
  }

  @Test
  public void testBandIncludesAllInRangeVersions() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 60, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 70)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 60))); // == min
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 50))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 40)));
  }

  @Test
  public void testCellsAboveMaxAreSkipped() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 50, 80));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 80))); // == max
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 60)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 50))); // == min
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 40))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 30)));
  }

  @Test
  public void testAllCellsAboveMaxAreSkipped() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 50, 50));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testColumnWithNoInBandCellKeepsOnlyPreImage() throws IOException {
    // The column has no cell at/inside the band; the first cell below min is its pre-image.
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 70)));
  }

  @Test
  public void testDeleteFamilyAlwaysIncluded() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 50)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 10)));
  }

  @Test
  public void testDeleteFamilyDoesNotAffectColumnState() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testDeleteColumnInBandIncluded() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteColumn(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testDeleteColumnAsPreImage() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteColumn(ROW1, CF, CQ1, 90))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testPointDeleteInBandAndAsPreImage() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(pointDelete(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(pointDelete(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testMultipleColumnsResetState() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    // Column CQ1
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
    // Column CQ2 — per-column pre-image state should reset.
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ2, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ2, 85)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ2, 70)));
  }

  @Test
  public void testMultipleColumnFamiliesResetState() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    // CF1:CQ1
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF1, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF1, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF1, CQ1, 80)));
    // CF2:CQ1 — same qualifier, different family, state should reset.
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF2, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF2, CQ1, 85)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF2, CQ1, 70)));
  }

  @Test
  public void testMultipleRows() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), band(100, 100));
    map.put(new ImmutableBytesPtr(ROW2), band(50, 80));
    CDCVersionFilter filter = createFilter(map);
    // ROW1
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
    // ROW2 — the row change is detected automatically via the row key.
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 60)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 50))); // == min
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 40))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW2, CF, CQ1, 30)));
  }

  @Test
  public void testRowNotInMapIncludesEverything() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 90)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 80)));
  }

  @Test
  public void testNullRangeMap() throws IOException {
    CDCVersionFilter filter = new CDCVersionFilter(null);
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
  }

  @Test
  public void testEmptyRangeMap() throws IOException {
    CDCVersionFilter filter = createFilter(new HashMap<>());
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
  }

  @Test
  public void testResetClearsState() throws IOException {
    CDCVersionFilter filter = createFilter(singleRow(ROW1, 100, 100));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    filter.reset();
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testSerializationRoundTrip() throws IOException, DeserializationException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), band(60, 100));
    map.put(new ImmutableBytesPtr(ROW2), band(80, 80));
    CDCVersionFilter original = createFilter(map);

    byte[] serialized = original.toByteArray();
    CDCVersionFilter deserialized = CDCVersionFilter.parseFrom(serialized);

    // ROW1 band [60, 100]
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 60))); // == min
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 50))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, deserialized.filterCell(put(ROW1, CF, CQ1, 40)));

    // ROW2 band [80, 80]
    deserialized.reset();
    assertEquals(ReturnCode.SKIP, deserialized.filterCell(put(ROW2, CF, CQ1, 100))); // above max
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW2, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW2, CF, CQ1, 70))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, deserialized.filterCell(put(ROW2, CF, CQ1, 60)));
  }

  // parseFrom is a public, class-name-addressable deserialization entry point; a malformed payload
  // with a negative wire-supplied length must be rejected rather than throwing
  // NegativeArraySizeException.

  @Test(expected = DeserializationException.class)
  public void testParseFromRejectsNegativeRowCount() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bos)) {
      out.writeInt(-1); // numRows
    }
    CDCVersionFilter.parseFrom(bos.toByteArray());
  }

  @Test(expected = DeserializationException.class)
  public void testParseFromRejectsNegativeRowKeyLength() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bos)) {
      out.writeInt(1); // numRows
      out.writeInt(-3); // keyLen negative
    }
    CDCVersionFilter.parseFrom(bos.toByteArray());
  }
}
