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

public class CDCVersionFilterTest {

  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] CF1 = Bytes.toBytes("cf");
  private static final byte[] CF2 = Bytes.toBytes("cf2");
  private static final byte[] CQ1 = Bytes.toBytes("cq1");
  private static final byte[] CQ2 = Bytes.toBytes("cq2");
  private static final byte[] VAL = Bytes.toBytes("v");

  // Convenience alias so existing tests don't need to change
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

  private CDCVersionFilter createFilter(Map<ImmutableBytesPtr, long[]> timestampMap) {
    return new CDCVersionFilter(timestampMap);
  }

  @Test
  public void testSingleChangeTimestamp() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    // ts=100 is the change, ts=90 is the pre-image, ts=80 should be skipped
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testMultipleChangeTimestamps() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 60 });
    CDCVersionFilter filter = createFilter(map);

    // Column CQ1: cells at ts 100, 90, 80, 70, 60, 50, 40
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90))); // pre for 100
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 80)));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 70)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 60))); // change
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 50))); // pre for 60
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 40)));
  }

  @Test
  public void testNoGapBetweenTimestamps() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 80, 60 });
    CDCVersionFilter filter = createFilter(map);

    // Column with cells exactly at change timestamps plus pre-images
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90))); // pre for 100
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 80))); // change
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 70))); // pre for 80
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 60))); // change
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 50))); // pre for 60
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 40)));
  }

  @Test
  public void testMissingChangeTimestamp() throws IOException {
    // Column has no cell at ts=60 but needs pre-image below it
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 60 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 70))); // pre for 100
    // No cell at ts=60; ts=55 should be included as pre-image for ts=60
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 55)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 40)));
  }

  @Test
  public void testDeleteFamilyAlwaysIncluded() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 50)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 10)));
  }

  @Test
  public void testDeleteColumnAtChangeTimestamp() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteColumn(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90))); // pre-image
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testMultipleColumns() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    // Column CQ1
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));

    // Column CQ2 — state should reset
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ2, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ2, 85)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ2, 70)));
  }

  @Test
  public void testMultipleRows() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    map.put(new ImmutableBytesPtr(ROW2), new long[] { 80, 50 });
    CDCVersionFilter filter = createFilter(map);

    // ROW1
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));

    // ROW2 — different change timestamps
    filter.reset();
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 70))); // pre for 80
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW2, CF, CQ1, 60)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 50))); // change
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 40))); // pre for 50
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW2, CF, CQ1, 30)));
  }

  @Test
  public void testRowNotInMap() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    // ROW2 is not in the map, should include everything
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 90)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW2, CF, CQ1, 80)));
  }

  @Test
  public void testCellAboveAllChangeTimestamps() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 80 });
    CDCVersionFilter filter = createFilter(map);

    // Cell at ts=100 is above the change timestamp 80; should be skipped.
    // But we still need a pre-image for ts=80.
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 70)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 60)));
  }

  @Test
  public void testSerializationRoundTrip() throws IOException, DeserializationException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 60 });
    map.put(new ImmutableBytesPtr(ROW2), new long[] { 80 });
    CDCVersionFilter original = createFilter(map);

    byte[] serialized = original.toByteArray();
    CDCVersionFilter deserialized = CDCVersionFilter.parseFrom(serialized);

    // Verify deserialized filter behaves the same as original
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.SKIP, deserialized.filterCell(put(ROW1, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 60)));
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW1, CF, CQ1, 50)));
    assertEquals(ReturnCode.NEXT_COL, deserialized.filterCell(put(ROW1, CF, CQ1, 40)));

    deserialized.reset();
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW2, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, deserialized.filterCell(put(ROW2, CF, CQ1, 70)));
    assertEquals(ReturnCode.NEXT_COL, deserialized.filterCell(put(ROW2, CF, CQ1, 60)));
  }

  @Test
  public void testMixedDeleteAndPutTypes() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    // DeleteFamily at ts=100 — always included
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 100)));
    // DeleteColumn for CQ1 at ts=100 — matches change timestamp
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteColumn(ROW1, CF, CQ1, 100)));
    // Put for CQ1 at ts=90 — pre-image
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    // Put for CQ1 at ts=80 — not needed
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testPreImageServedByChangeTimestamp() throws IOException {
    // Change timestamps are adjacent: pre-image for 100 is the cell at 80
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 80 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    // Cell at 80 serves as both pre-image for 100 AND change at 80
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 80)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 70))); // pre for 80
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 60)));
  }

  @Test
  public void testSparseColumnPreImageForSkippedTimestamp() throws IOException {
    // ts=100 and ts=60 are change timestamps, but column only has cells at ts=100, 55, 40
    // Cell at 55 must be included as pre-image for both ts=100 and ts=60
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 60 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    // ts=55: pre-image for ts=100, and also covers ts=60 (no cell at 60 for this column)
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 55)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 40)));
  }

  @Test
  public void testNullTimestampMap() throws IOException {
    CDCVersionFilter filter = new CDCVersionFilter(null);
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
  }

  @Test
  public void testEmptyTimestampMap() throws IOException {
    CDCVersionFilter filter = createFilter(new HashMap<>());
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
  }

  @Test
  public void testColumnWithNoCellAtAnyChangeTimestamp() throws IOException {
    // change timestamps = [100, 60], column has cells only at [95, 55]
    // 95 should be pre-image for 100, 55 should be pre-image for 60
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 60 });
    CDCVersionFilter filter = createFilter(map);

    // ts=95: while loop advances past ts=100 (100 > 95), needPreImage=true.
    // No match (95 != 60). needPreImage=true -> INCLUDE as pre-image for 100.
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 95)));
    // ts=55: while loop advances past ts=60 (60 > 55), needPreImage=true.
    // tsIdx >= len. needPreImage=true -> INCLUDE as pre-image for 60.
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 55)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 40)));
  }

  @Test
  public void testDeleteColumnAsPreImage() throws IOException {
    // DeleteColumn at ts=90 serves as pre-image for change ts=100
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteColumn(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testAllCellsAboveAllChangeTimestamps() throws IOException {
    // change timestamps = [50], column cells at [100, 90, 80] — all above 50, no match
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 50 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.SKIP, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testPointDeleteAtChangeTimestamp() throws IOException {
    // Point delete (Type.Delete) at change timestamp
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(pointDelete(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testPointDeleteAsPreImage() throws IOException {
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    // Point delete serving as pre-image
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(pointDelete(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }

  @Test
  public void testDeleteColumnBetweenChangeTimestampsNotNeeded() throws IOException {
    // DeleteColumn at ts=75 is between change ts 100 and 60, not at either,
    // and not a pre-image candidate (pre-image for 100 already served by ts=90)
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100, 60 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90))); // pre for 100
    assertEquals(ReturnCode.SKIP, filter.filterCell(deleteColumn(ROW1, CF, CQ1, 75)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 60)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 50)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 40)));
  }

  @Test
  public void testMultipleColumnFamilies() throws IOException {
    // Same qualifier name in different families — state should reset on family change
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    // CF1:CQ1
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF1, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF1, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF1, CQ1, 80)));

    // CF2:CQ1 — same qualifier but different family, state should reset
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF2, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF2, CQ1, 85)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF2, CQ1, 70)));
  }

  @Test
  public void testSingleCellAtChangeTimestamp() throws IOException {
    // Only one cell for this column, exactly at the change timestamp
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    // Next column starts immediately — no pre-image available, that's fine
  }

  @Test
  public void testDeleteFamilyDoesNotAffectColumnState() throws IOException {
    // DeleteFamily markers should not interfere with per-column version tracking
    Map<ImmutableBytesPtr, long[]> map = new HashMap<>();
    map.put(new ImmutableBytesPtr(ROW1), new long[] { 100 });
    CDCVersionFilter filter = createFilter(map);

    // DeleteFamily at ts=100
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(deleteFamily(ROW1, CF, 100)));
    // Then normal column cells — should still track correctly
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 100)));
    assertEquals(ReturnCode.INCLUDE, filter.filterCell(put(ROW1, CF, CQ1, 90)));
    assertEquals(ReturnCode.NEXT_COL, filter.filterCell(put(ROW1, CF, CQ1, 80)));
  }
}
