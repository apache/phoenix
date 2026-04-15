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
package org.apache.phoenix.mapreduce;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for PhoenixSyncTableMapper.
 */
public class PhoenixSyncTableMapperTest {

  private PhoenixSyncTableMapper mapper;

  @Before
  public void setup() throws Exception {
    mapper = new PhoenixSyncTableMapper();
  }

  /**
   * Helper method to create a PhoenixSyncTableCheckpointOutputRow with start and end keys.
   */
  private PhoenixSyncTableCheckpointOutputRow createChunk(byte[] startKey, byte[] endKey) {
    return new PhoenixSyncTableCheckpointOutputRow.Builder().setStartRowKey(startKey)
      .setEndRowKey(endKey).build();
  }

  /**
   * Helper method to verify a gap/range matches expected values.
   */
  private void assertGap(KeyRange gap, byte[] expectedStart, byte[] expectedEnd) {
    assertArrayEquals("Gap start key mismatch", expectedStart, gap.getLowerRange());
    assertArrayEquals("Gap end key mismatch", expectedEnd, gap.getUpperRange());
  }

  @Test
  public void testNullProcessedChunks() {
    // Scenario: No processed chunks (null), entire region should be unprocessed
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");

    List<KeyRange> result = mapper.calculateUnprocessedRanges(regionStart, regionEnd, null);

    assertEquals("Should have 1 unprocessed range when no chunks processed", 1, result.size());
    assertGap(result.get(0), regionStart, regionEnd);
  }

  @Test
  public void testEmptyProcessedChunks() {
    // Scenario: Empty processed chunks list, entire region should be unprocessed
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 1 unprocessed range when chunks list is empty", 1, result.size());
    assertGap(result.get(0), regionStart, regionEnd);
  }

  @Test
  public void testFullyCoveredRegion() {
    // Scenario: Entire region covered by processed chunks, no gaps
    // Region: [a, z)
    // Chunks: [a, d], [d, g], [g, z]
    // Expected: No unprocessed ranges
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("d")));
    processedChunks.add(createChunk(Bytes.toBytes("d"), Bytes.toBytes("g")));
    processedChunks.add(createChunk(Bytes.toBytes("g"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have no unprocessed ranges when region fully covered", 0, result.size());
  }

  @Test
  public void testGapAtStart() {
    // Scenario: Gap at the start of the region
    // Region: [a, z)
    // Chunks: [e, z]
    // Expected: [a, e)
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("e"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 1 unprocessed range at start", 1, result.size());
    assertGap(result.get(0), Bytes.toBytes("a"), Bytes.toBytes("e"));
  }

  @Test
  public void testGapAtEnd() {
    // Scenario: Gap at the end of the region
    // Region: [a, z)
    // Chunks: [a, e]
    // Expected: [e, z)
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("e")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 1 unprocessed range at end", 1, result.size());
    assertGap(result.get(0), Bytes.toBytes("e"), Bytes.toBytes("z"));
  }

  @Test
  public void testGapInMiddle() {
    // Scenario: Gap in the middle between two chunks
    // Region: [a, z)
    // Chunks: [a, d], [g, z]
    // Expected: [d, g)
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("d")));
    processedChunks.add(createChunk(Bytes.toBytes("g"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 1 unprocessed range in middle", 1, result.size());
    assertGap(result.get(0), Bytes.toBytes("d"), Bytes.toBytes("g"));
  }

  @Test
  public void testMultipleGaps() {
    // Scenario: Multiple gaps throughout the region
    // Region: [a, z)
    // Chunks: [b, d], [f, h], [j, l]
    // Expected: [a, b), [d, f), [h, j), [l, z)
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("b"), Bytes.toBytes("d")));
    processedChunks.add(createChunk(Bytes.toBytes("f"), Bytes.toBytes("h")));
    processedChunks.add(createChunk(Bytes.toBytes("j"), Bytes.toBytes("l")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 4 unprocessed ranges", 4, result.size());
    assertGap(result.get(0), Bytes.toBytes("a"), Bytes.toBytes("b"));
    assertGap(result.get(1), Bytes.toBytes("d"), Bytes.toBytes("f"));
    assertGap(result.get(2), Bytes.toBytes("h"), Bytes.toBytes("j"));
    assertGap(result.get(3), Bytes.toBytes("l"), Bytes.toBytes("z"));
  }

  @Test
  public void testChunkStartsBeforeRegion() {
    // Scenario: First chunk starts before mapper region (clipping needed)
    // Region: [d, z)
    // Chunks: [a, g], [g, z]
    // Expected: No gaps (chunk clipped to [d, g])
    byte[] regionStart = Bytes.toBytes("d");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("g")));
    processedChunks.add(createChunk(Bytes.toBytes("g"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have no unprocessed ranges after clipping", 0, result.size());
  }

  @Test
  public void testChunkEndsAfterRegion() {
    // Scenario: Last chunk ends after mapper region (clipping needed)
    // Region: [a, m)
    // Chunks: [a, g], [g, z]
    // Expected: No gaps (last chunk clipped to [g, m))
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("m");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("g")));
    processedChunks.add(createChunk(Bytes.toBytes("g"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have no unprocessed ranges after clipping", 0, result.size());
  }

  @Test
  public void testChunkBothSidesOutsideRegion() {
    // Scenario: Chunk starts before and ends after region
    // Region: [d, m)
    // Chunks: [a, z]
    // Expected: No gaps (chunk covers entire region)
    byte[] regionStart = Bytes.toBytes("d");
    byte[] regionEnd = Bytes.toBytes("m");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have no unprocessed ranges when chunk covers entire region", 0,
      result.size());
  }

  @Test
  public void testFirstRegionWithEmptyStartKey() {
    // Scenario: First region of table with empty start key []
    // Region: [[], d)
    // Chunks: [a, d]
    // Expected: [[], a) gap at start, no gap at end since chunk ends at region boundary
    byte[] regionStart = HConstants.EMPTY_START_ROW;
    byte[] regionEnd = Bytes.toBytes("d");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("d")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 1 unprocessed range at start", 1, result.size());
    assertGap(result.get(0), HConstants.EMPTY_START_ROW, Bytes.toBytes("a"));
  }

  @Test
  public void testFirstRegionWithGapAtStart() {
    // Scenario: First region with gap at start
    // Region: [[], d)
    // Chunks: [b, d]
    // Expected: [[], b)
    byte[] regionStart = HConstants.EMPTY_START_ROW;
    byte[] regionEnd = Bytes.toBytes("d");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("b"), Bytes.toBytes("d")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 1 unprocessed range at start of first region", 1, result.size());
    assertGap(result.get(0), HConstants.EMPTY_START_ROW, Bytes.toBytes("b"));
  }

  @Test
  public void testLastRegionWithEmptyEndKey() {
    // Scenario: Last region of table with empty end key []
    // Region: [v, [])
    // Chunks: [v, z]
    // Expected: [z, [])
    byte[] regionStart = Bytes.toBytes("v");
    byte[] regionEnd = HConstants.EMPTY_END_ROW;
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("v"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 1 unprocessed range at end of last region", 1, result.size());
    assertGap(result.get(0), Bytes.toBytes("z"), HConstants.EMPTY_END_ROW);
  }

  @Test
  public void testLastRegionFullyCovered() {
    // Scenario: Last region fully covered
    // Region: [v, [])
    // Chunks: [v, x], [x, [])
    // Expected: No gaps (but will add [[], []) due to isEndRegionOfTable logic)
    byte[] regionStart = Bytes.toBytes("v");
    byte[] regionEnd = HConstants.EMPTY_END_ROW;
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("v"), Bytes.toBytes("x")));
    processedChunks.add(createChunk(Bytes.toBytes("x"), HConstants.EMPTY_END_ROW));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    // Due to isEndRegionOfTable check, we always add remaining range
    assertEquals("Last region should have 1 range added", 1, result.size());
    assertGap(result.get(0), HConstants.EMPTY_END_ROW, HConstants.EMPTY_END_ROW);
  }

  @Test
  public void testSingleRegionTable() {
    // Scenario: Single region table (entire table)
    // Region: [[], [])
    // Chunks: [a, m]
    // Expected: [[], a), [m, [])
    byte[] regionStart = HConstants.EMPTY_START_ROW;
    byte[] regionEnd = HConstants.EMPTY_END_ROW;
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("m")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 2 unprocessed ranges for single region table", 2, result.size());
    assertGap(result.get(0), HConstants.EMPTY_START_ROW, Bytes.toBytes("a"));
    assertGap(result.get(1), Bytes.toBytes("m"), HConstants.EMPTY_END_ROW);
  }

  @Test
  public void testSingleChunkInMiddleOfRegion() {
    // Scenario: Single chunk in middle of region
    // Region: [a, z)
    // Chunks: [e, m]
    // Expected: [a, e), [m, z)
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("e"), Bytes.toBytes("m")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 2 unprocessed ranges", 2, result.size());
    assertGap(result.get(0), Bytes.toBytes("a"), Bytes.toBytes("e"));
    assertGap(result.get(1), Bytes.toBytes("m"), Bytes.toBytes("z"));
  }

  @Test
  public void testAdjacentChunksWithNoGaps() {
    // Scenario: Perfectly adjacent chunks with no gaps
    // Region: [a, z)
    // Chunks: [a, c], [c, f], [f, j], [j, z]
    // Expected: No gaps
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("c")));
    processedChunks.add(createChunk(Bytes.toBytes("c"), Bytes.toBytes("f")));
    processedChunks.add(createChunk(Bytes.toBytes("f"), Bytes.toBytes("j")));
    processedChunks.add(createChunk(Bytes.toBytes("j"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have no unprocessed ranges for perfectly adjacent chunks", 0,
      result.size());
  }

  @Test
  public void testSmallGapsBetweenManyChunks() {
    // Scenario: Many chunks with small gaps between them
    // Region: [a, z)
    // Chunks: [a, b], [c, d], [e, f], [g, h]
    // Expected: [b, c), [d, e), [f, g), [h, z)
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("b")));
    processedChunks.add(createChunk(Bytes.toBytes("c"), Bytes.toBytes("d")));
    processedChunks.add(createChunk(Bytes.toBytes("e"), Bytes.toBytes("f")));
    processedChunks.add(createChunk(Bytes.toBytes("g"), Bytes.toBytes("h")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 4 unprocessed ranges", 4, result.size());
    assertGap(result.get(0), Bytes.toBytes("b"), Bytes.toBytes("c"));
    assertGap(result.get(1), Bytes.toBytes("d"), Bytes.toBytes("e"));
    assertGap(result.get(2), Bytes.toBytes("f"), Bytes.toBytes("g"));
    assertGap(result.get(3), Bytes.toBytes("h"), Bytes.toBytes("z"));
  }

  @Test
  public void testChunkExactlyMatchesRegion() {
    // Scenario: Single chunk exactly matches region boundaries
    // Region: [a, z)
    // Chunks: [a, z]
    // Expected: No gaps
    byte[] regionStart = Bytes.toBytes("a");
    byte[] regionEnd = Bytes.toBytes("z");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("z")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have no unprocessed ranges when chunk matches region", 0, result.size());
  }

  @Test
  public void testRegionBoundaryChangeScenario() {
    // Scenario: Region boundaries changed after split/merge
    // New Region: [d, r)
    // Old processed chunks: [a, g], [j, m], [s, v]
    // First chunk [a, g] starts before region, clipped to [d, g]
    // Second chunk [j, m] is within region
    // Third chunk [s, v] starts after region end 'r', effectiveStart becomes 's'
    // scanPos is updated to 's', making the final gap [m, s)
    // Expected: [g, j), [m, s)
    byte[] regionStart = Bytes.toBytes("d");
    byte[] regionEnd = Bytes.toBytes("r");
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("g")));
    processedChunks.add(createChunk(Bytes.toBytes("j"), Bytes.toBytes("m")));
    processedChunks.add(createChunk(Bytes.toBytes("s"), Bytes.toBytes("v")));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 2 unprocessed ranges after region boundary change", 2, result.size());
    assertGap(result.get(0), Bytes.toBytes("g"), Bytes.toBytes("j"));
    assertGap(result.get(1), Bytes.toBytes("m"), Bytes.toBytes("s"));
  }

  @Test
  public void testComplexMultiByteKeys() {
    // Scenario: Using multi-byte keys (realistic scenario)
    // Region: [\x01\x00, \x05\x00)
    // Chunks: [\x01\x00, \x02\x00], [\x03\x00, \x04\x00]
    // Expected: [\x02\x00, \x03\x00), [\x04\x00, \x05\x00)
    byte[] regionStart = new byte[] { 0x01, 0x00 };
    byte[] regionEnd = new byte[] { 0x05, 0x00 };
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks = new ArrayList<>();
    processedChunks.add(createChunk(new byte[] { 0x01, 0x00 }, new byte[] { 0x02, 0x00 }));
    processedChunks.add(createChunk(new byte[] { 0x03, 0x00 }, new byte[] { 0x04, 0x00 }));

    List<KeyRange> result =
      mapper.calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    assertEquals("Should have 2 unprocessed ranges with multi-byte keys", 2, result.size());
    assertGap(result.get(0), new byte[] { 0x02, 0x00 }, new byte[] { 0x03, 0x00 });
    assertGap(result.get(1), new byte[] { 0x04, 0x00 }, new byte[] { 0x05, 0x00 });
  }

  // Tests for shouldStartKeyBeInclusive method

  @Test
  public void testShouldStartKeyBeInclusiveWithNullMapperStart() {
    // Null mapper region start should return true (first region)
    assertTrue(mapper.shouldStartKeyBeInclusive(null, new ArrayList<>()));
  }

  @Test
  public void testShouldStartKeyBeInclusiveWithEmptyMapperStart() {
    // Empty mapper region start should return true (first region)
    assertTrue(mapper.shouldStartKeyBeInclusive(HConstants.EMPTY_START_ROW, new ArrayList<>()));
  }

  @Test
  public void testShouldStartKeyBeInclusiveWithNullChunks() {
    // Null processed chunks should return true
    assertTrue(mapper.shouldStartKeyBeInclusive(Bytes.toBytes("a"), null));
  }

  @Test
  public void testShouldStartKeyBeInclusiveWithEmptyChunks() {
    // Empty processed chunks should return true
    assertTrue(mapper.shouldStartKeyBeInclusive(Bytes.toBytes("a"), new ArrayList<>()));
  }

  @Test
  public void testShouldStartKeyBeInclusiveWhenFirstChunkAfterMapperStart() {
    // Mapper: [a, ...) Chunks: [c, ...]
    // First chunk starts AFTER mapper start -> return true (gap at beginning)
    byte[] mapperStart = Bytes.toBytes("a");
    List<PhoenixSyncTableCheckpointOutputRow> chunks = new ArrayList<>();
    chunks.add(createChunk(Bytes.toBytes("c"), Bytes.toBytes("f")));
    assertTrue(mapper.shouldStartKeyBeInclusive(mapperStart, chunks));
  }

  @Test
  public void testShouldStartKeyBeInclusiveWhenFirstChunkAtMapperStart() {
    // Mapper: [a, ...) Chunks: [a, ...]
    // First chunk starts AT mapper start -> return false (no gap)
    byte[] mapperStart = Bytes.toBytes("a");
    List<PhoenixSyncTableCheckpointOutputRow> chunks = new ArrayList<>();
    chunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("f")));
    assertFalse(mapper.shouldStartKeyBeInclusive(mapperStart, chunks));
  }

  @Test
  public void testShouldStartKeyBeInclusiveWhenFirstChunkBeforeMapperStart() {
    // Mapper: [d, ...) Chunks: [a, ...]
    // First chunk starts BEFORE mapper start -> return false (no gap, chunk overlaps start)
    byte[] mapperStart = Bytes.toBytes("d");
    List<PhoenixSyncTableCheckpointOutputRow> chunks = new ArrayList<>();
    chunks.add(createChunk(Bytes.toBytes("a"), Bytes.toBytes("g")));
    assertFalse(mapper.shouldStartKeyBeInclusive(mapperStart, chunks));
  }
}
