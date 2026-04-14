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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.phoenix.query.KeyRange;
import org.junit.Test;

/**
 * Unit tests for PhoenixSyncTableInputFormat. Tests various scenarios of filtering completed splits
 */
public class PhoenixSyncTableInputFormatTest {

  private PhoenixSyncTableInputFormat inputFormat = new PhoenixSyncTableInputFormat();

  /**
   * Helper method to create a PhoenixInputSplit with given key range boundaries.
   */
  private PhoenixInputSplit createSplit(byte[] start, byte[] end) {
    Scan scan = new Scan();
    scan.withStartRow(start, true);
    scan.withStopRow(end, false);
    return new PhoenixInputSplit(Collections.singletonList(scan));
  }

  /**
   * Helper method to create a KeyRange with given boundaries.
   */
  private KeyRange createKeyRange(byte[] start, byte[] end) {
    return KeyRange.getKeyRange(start, true, end, false);
  }

  @Test
  public void testNoCompletedRegions() {
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    allSplits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    allSplits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<KeyRange> completedRegions = new ArrayList<>();

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("All splits should be unprocessed when no completed regions", 3, result.size());
    assertEquals(allSplits, result);
  }

  @Test
  public void testAllRegionsCompleted() {
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    allSplits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    allSplits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("d")));
    completedRegions.add(createKeyRange(Bytes.toBytes("d"), Bytes.toBytes("g")));
    completedRegions.add(createKeyRange(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("No splits should be unprocessed when all regions completed", 0, result.size());
  }

  @Test
  public void testPartiallyCompletedRegions() {
    // Scenario: Some regions completed, some pending
    // Splits: [a,d), [d,g), [g,j)
    // Completed: [a,d), [g,j)
    // Expected unprocessed: [d,g)
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    allSplits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    allSplits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("d")));
    completedRegions.add(createKeyRange(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("Only middle split should be unprocessed", 1, result.size());
    PhoenixInputSplit unprocessed = (PhoenixInputSplit) result.get(0);
    assertTrue("Should be [d,g) split",
      Bytes.equals(Bytes.toBytes("d"), unprocessed.getKeyRange().getLowerRange()));
    assertTrue("Should be [d,g) split",
      Bytes.equals(Bytes.toBytes("g"), unprocessed.getKeyRange().getUpperRange()));
  }

  @Test
  public void testSplitExtendsAcrossCompleted() {
    // Scenario: Split extends beyond completed region on both sides
    // Split: [a,k)
    // Completed: [c,g)
    // Expected unprocessed: [a,k) (completed is inside split, not fully contained)
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("k")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("c"), Bytes.toBytes("g")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("Split should be unprocessed when completed is inside split", 1, result.size());
  }

  @Test
  public void testExactMatchSplitAndCompleted() {
    // Scenario: Split exactly matches completed region
    // Split: [a,d)
    // Completed: [a,d)
    // Expected unprocessed: none
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("d")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("Split should be filtered out when it exactly matches completed region", 0,
      result.size());
  }

  @Test
  public void testLastRegionWithEmptyEndRow() {
    // Scenario: Last region with empty end row that partially overlaps with a middle split
    // Splits: [a,d), [d,g), [g,[])
    // Completed: [f,[]) - fully contains [g,[]) and partially overlaps [d,g)
    // Expected unprocessed: [a,d), [d,g) - partial overlap means split is NOT filtered
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    allSplits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    allSplits.add(createSplit(Bytes.toBytes("g"), HConstants.EMPTY_END_ROW));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("f"), HConstants.EMPTY_END_ROW));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals(
      "First two splits should be unprocessed (partial overlap keeps split), last should be filtered",
      2, result.size());
    PhoenixInputSplit first = (PhoenixInputSplit) result.get(0);
    PhoenixInputSplit second = (PhoenixInputSplit) result.get(1);
    assertTrue("First should be [a,d) split",
      Bytes.equals(Bytes.toBytes("a"), first.getKeyRange().getLowerRange()));
    assertTrue("Second should be [d,g) split",
      Bytes.equals(Bytes.toBytes("d"), second.getKeyRange().getLowerRange()));
  }

  @Test
  public void testCompletedRegionCoversMultipleSplits() {
    // Scenario: One completed region covers multiple splits
    // Splits: [a,c), [c,e), [e,g)
    // Completed: [a,g)
    // Expected unprocessed: none
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("c")));
    allSplits.add(createSplit(Bytes.toBytes("c"), Bytes.toBytes("e")));
    allSplits.add(createSplit(Bytes.toBytes("e"), Bytes.toBytes("g")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("g")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("All splits should be filtered when covered by one large completed region", 0,
      result.size());
  }

  @Test
  public void testInterleavedCompletedAndUnprocessed() {
    // Scenario: Completed and unprocessed regions interleaved
    // Splits: [a,c), [c,e), [e,g), [g,i), [i,k)
    // Completed: [a,c), [e,g), [i,k)
    // Expected unprocessed: [c,e), [g,i)
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("c")));
    allSplits.add(createSplit(Bytes.toBytes("c"), Bytes.toBytes("e")));
    allSplits.add(createSplit(Bytes.toBytes("e"), Bytes.toBytes("g")));
    allSplits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("i")));
    allSplits.add(createSplit(Bytes.toBytes("i"), Bytes.toBytes("k")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("c")));
    completedRegions.add(createKeyRange(Bytes.toBytes("e"), Bytes.toBytes("g")));
    completedRegions.add(createKeyRange(Bytes.toBytes("i"), Bytes.toBytes("k")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("Should have 2 unprocessed splits", 2, result.size());
    PhoenixInputSplit split1 = (PhoenixInputSplit) result.get(0);
    PhoenixInputSplit split2 = (PhoenixInputSplit) result.get(1);

    assertTrue("First unprocessed should be [c,e)",
      Bytes.equals(Bytes.toBytes("c"), split1.getKeyRange().getLowerRange()));
    assertTrue("Second unprocessed should be [g,i)",
      Bytes.equals(Bytes.toBytes("g"), split2.getKeyRange().getLowerRange()));
  }

  @Test
  public void testEmptyStartRow() {
    // Scenario: First region with empty start row
    // Splits: [[],c), [c,f)
    // Completed: [[],c)
    // Expected unprocessed: [c,f)
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(HConstants.EMPTY_START_ROW, Bytes.toBytes("c")));
    allSplits.add(createSplit(Bytes.toBytes("c"), Bytes.toBytes("f")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(HConstants.EMPTY_START_ROW, Bytes.toBytes("c")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("Second split should be unprocessed", 1, result.size());
    PhoenixInputSplit unprocessed = (PhoenixInputSplit) result.get(0);
    assertTrue("Should be [c,f) split",
      Bytes.equals(Bytes.toBytes("c"), unprocessed.getKeyRange().getLowerRange()));
  }

  @Test
  public void testUnsortedInputSplits() {
    // Scenario: Verify that input splits are sorted before processing
    // Splits (unsorted): [g,j), [a,d), [d,g)
    // Completed: [a,d)
    // Expected unprocessed: [d,g), [g,j) (after sorting)
    List<InputSplit> allSplits = new ArrayList<>();
    allSplits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j"))); // out of order
    allSplits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    allSplits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));

    List<KeyRange> completedRegions = new ArrayList<>();
    completedRegions.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("d")));

    List<InputSplit> result = inputFormat.filterCompletedSplits(allSplits, completedRegions);

    assertEquals("Should have 2 unprocessed splits after sorting", 2, result.size());
    // Verify sorted order
    PhoenixInputSplit split1 = (PhoenixInputSplit) result.get(0);
    PhoenixInputSplit split2 = (PhoenixInputSplit) result.get(1);

    assertTrue("First should be [d,g)",
      Bytes.compareTo(split1.getKeyRange().getLowerRange(), split2.getKeyRange().getLowerRange())
          < 0);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testCreateRecordReaderReturnsNoOpReader() {
    RecordReader reader = inputFormat.createRecordReader(null, null);
    assertNotNull("createRecordReader should never return null", reader);
    assertTrue("Should return a PhoenixNoOpSingleRecordReader",
      reader instanceof PhoenixNoOpSingleRecordReader);
  }
}
