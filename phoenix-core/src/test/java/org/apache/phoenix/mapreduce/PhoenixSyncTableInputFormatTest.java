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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for PhoenixSyncTableInputFormat. Tests various scenarios of filtering completed splits
 * and split coalescing functionality.
 */
public class PhoenixSyncTableInputFormatTest {

  private PhoenixSyncTableInputFormat inputFormat = new PhoenixSyncTableInputFormat();

  private ConnectionQueryServices mockQueryServices;
  private byte[] physicalTableName = Bytes.toBytes("TEST_TABLE");

  @Before
  public void setup() throws Exception {
    mockQueryServices = mock(ConnectionQueryServices.class);
  }

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

  @Test
  public void testCoalesceSplitsWithSingleServer() throws Exception {
    // Create 3 PhoenixInputSplits all on same server
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));

    // Create mock region location - all splits on server1
    HRegionLocation mockRegion = createMockRegionLocation("server1:16020", Bytes.toBytes("a"));

    // Mock ConnectionQueryServices: all splits → server1
    when(mockQueryServices.getTableRegionLocation(any(byte[].class), any(byte[].class)))
      .thenReturn(mockRegion);

    // Call coalesceSplits()
    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    // Verify: 1 coalesced split (all on same server)
    assertEquals("Should have 1 coalesced split (all on same server)", 1, result.size());

    // Verify: Split is coalesced and contains 3 KeyRanges
    PhoenixInputSplit coalescedSplit = (PhoenixInputSplit) result.get(0);

    assertTrue("Split should be coalesced", coalescedSplit.isCoalesced());
    assertEquals("Split should have 3 KeyRanges", 3, coalescedSplit.getKeyRanges().size());

    // Verify: KeyRanges are sorted
    List<KeyRange> keyRanges = coalescedSplit.getKeyRanges();
    assertTrue("First KeyRange should start with 'a'",
      Bytes.equals(Bytes.toBytes("a"), keyRanges.get(0).getLowerRange()));
    assertTrue("Second KeyRange should start with 'd'",
      Bytes.equals(Bytes.toBytes("d"), keyRanges.get(1).getLowerRange()));
    assertTrue("Third KeyRange should start with 'g'",
      Bytes.equals(Bytes.toBytes("g"), keyRanges.get(2).getLowerRange()));
  }

  @Test
  public void testCoalesceSplitsWithMultipleServers() throws Exception {
    // Create 6 PhoenixInputSplits
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("c")));
    splits.add(createSplit(Bytes.toBytes("c"), Bytes.toBytes("e")));
    splits.add(createSplit(Bytes.toBytes("e"), Bytes.toBytes("g")));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("i")));
    splits.add(createSplit(Bytes.toBytes("i"), Bytes.toBytes("k")));
    splits.add(createSplit(Bytes.toBytes("k"), Bytes.toBytes("m")));

    // Create mock region locations BEFORE stubbing to avoid nested stubbing issues
    HRegionLocation mockRegionA = createMockRegionLocation("server1:16020", Bytes.toBytes("a"));
    HRegionLocation mockRegionC = createMockRegionLocation("server1:16020", Bytes.toBytes("c"));
    HRegionLocation mockRegionE = createMockRegionLocation("server1:16020", Bytes.toBytes("e"));
    HRegionLocation mockRegionG = createMockRegionLocation("server2:16020", Bytes.toBytes("g"));
    HRegionLocation mockRegionI = createMockRegionLocation("server2:16020", Bytes.toBytes("i"));
    HRegionLocation mockRegionK = createMockRegionLocation("server2:16020", Bytes.toBytes("k"));

    // Mock ConnectionQueryServices: first 3 splits → server1, last 3 splits → server2
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("a")))
      .thenReturn(mockRegionA);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("c")))
      .thenReturn(mockRegionC);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("e")))
      .thenReturn(mockRegionE);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("g")))
      .thenReturn(mockRegionG);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("i")))
      .thenReturn(mockRegionI);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("k")))
      .thenReturn(mockRegionK);

    // Call coalesceSplits()
    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    // Verify: 2 coalesced splits (one per server)
    assertEquals("Should have 2 coalesced splits (one per server)", 2, result.size());

    // Verify: Each split is coalesced and contains 3 KeyRanges
    PhoenixInputSplit split1 = (PhoenixInputSplit) result.get(0);
    PhoenixInputSplit split2 = (PhoenixInputSplit) result.get(1);

    assertTrue("Split 1 should be coalesced", split1.isCoalesced());
    assertTrue("Split 2 should be coalesced", split2.isCoalesced());

    assertEquals("Split 1 should have 3 KeyRanges", 3, split1.getKeyRanges().size());
    assertEquals("Split 2 should have 3 KeyRanges", 3, split2.getKeyRanges().size());

    // Verify: Splits are sorted by start key within each server group
    List<KeyRange> keyRanges1 = split1.getKeyRanges();
    List<KeyRange> keyRanges2 = split2.getKeyRanges();

    // Check that KeyRanges are sorted (each should be less than next)
    for (int i = 0; i < keyRanges1.size() - 1; i++) {
      assertTrue("KeyRanges in split 1 should be sorted",
        Bytes.compareTo(keyRanges1.get(i).getLowerRange(), keyRanges1.get(i + 1).getLowerRange())
            < 0);
    }
    for (int i = 0; i < keyRanges2.size() - 1; i++) {
      assertTrue("KeyRanges in split 2 should be sorted",
        Bytes.compareTo(keyRanges2.get(i).getLowerRange(), keyRanges2.get(i + 1).getLowerRange())
            < 0);
    }
  }

  @Test
  public void testCoalesceSplitsWithEmptyList() throws Exception {
    // Test edge case: empty input list
    List<InputSplit> splits = new ArrayList<>();

    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    assertEquals("Should return empty list for empty input", 0, result.size());
  }

  @Test
  public void testCoalesceSplitsWithSingleSplit() throws Exception {
    // Test edge case: single split (no coalescing needed)
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));

    HRegionLocation mockRegion = createMockRegionLocation("server1:16020", Bytes.toBytes("a"));
    when(mockQueryServices.getTableRegionLocation(any(byte[].class), any(byte[].class)))
      .thenReturn(mockRegion);

    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    assertEquals("Should return 1 split", 1, result.size());
    PhoenixInputSplit resultSplit = (PhoenixInputSplit) result.get(0);
    assertFalse("Single split should not be marked as coalesced", resultSplit.isCoalesced());
    assertEquals("Should have 1 KeyRange", 1, resultSplit.getKeyRanges().size());
  }

  @Test
  public void testCoalesceSplitsWithNullRegionLocationFallsBackToUnknownServer() throws Exception {
    // Null location (e.g. region in transition) — split should be placed in UNKNOWN_SERVER bucket.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));

    when(mockQueryServices.getTableRegionLocation(any(byte[].class), any(byte[].class)))
      .thenReturn(null);

    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    assertEquals("Should return 1 split assigned to UNKNOWN_SERVER bucket", 1, result.size());
    PhoenixInputSplit resultSplit = (PhoenixInputSplit) result.get(0);
    // The split is coalesced under UNKNOWN_SERVER — location reflects that server name.
    assertEquals("Split location should be UNKNOWN_SERVER",
      PhoenixSyncTableInputFormat.UNKNOWN_SERVER, resultSplit.getLocations()[0]);
  }

  @Test
  public void testCoalesceSplitsWithNullServerNameFallsBackToUnknownServer() throws Exception {
    // Location present but serverName is null (e.g. region in transition) — same fallback.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));

    HRegionLocation nullServerLocation = mock(HRegionLocation.class);
    when(nullServerLocation.getServerName()).thenReturn(null);
    when(mockQueryServices.getTableRegionLocation(any(byte[].class), any(byte[].class)))
      .thenReturn(nullServerLocation);

    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    assertEquals("Should return 1 split assigned to UNKNOWN_SERVER bucket", 1, result.size());
    PhoenixInputSplit resultSplit = (PhoenixInputSplit) result.get(0);
    assertEquals("Split location should be UNKNOWN_SERVER",
      PhoenixSyncTableInputFormat.UNKNOWN_SERVER, resultSplit.getLocations()[0]);
  }

  @Test
  public void testCoalesceSplitsMultipleNullLocationsCoalescedIntoOneUnknownSplit()
    throws Exception {
    // Multiple splits with unavailable region location — all grouped into one UNKNOWN_SERVER split.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));

    when(mockQueryServices.getTableRegionLocation(any(byte[].class), any(byte[].class)))
      .thenReturn(null);

    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    assertEquals("All RIT splits should be coalesced into one UNKNOWN_SERVER split", 1,
      result.size());
    PhoenixInputSplit coalescedSplit = (PhoenixInputSplit) result.get(0);
    assertTrue("UNKNOWN_SERVER split should be marked as coalesced", coalescedSplit.isCoalesced());
    assertEquals("Should contain all 3 KeyRanges", 3, coalescedSplit.getKeyRanges().size());
  }

  @Test
  public void testCoalesceSplitsMixedValidAndNullLocations() throws Exception {
    // Scenario: 2 splits on server1, 2 splits with null location (RIT).
    // Expected: 2 output splits — 1 coalesced on server1, 1 coalesced on UNKNOWN_SERVER.
    // This is the most realistic production scenario: a region split mid-job affects only a
    // subset of regions while the rest are healthy.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));
    splits.add(createSplit(Bytes.toBytes("j"), Bytes.toBytes("m")));

    HRegionLocation mockRegion = createMockRegionLocation("server1:16020", Bytes.toBytes("a"));
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("a")))
      .thenReturn(mockRegion);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("d")))
      .thenReturn(mockRegion);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("g")))
      .thenReturn(null);
    when(mockQueryServices.getTableRegionLocation(physicalTableName, Bytes.toBytes("j")))
      .thenReturn(null);

    List<InputSplit> result =
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);

    assertEquals("Should produce 2 splits: server1 bucket + UNKNOWN_SERVER bucket", 2,
      result.size());

    // Collect split locations
    List<String> locations = new ArrayList<>();
    for (InputSplit s : result) {
      locations.add(((PhoenixInputSplit) s).getLocations()[0]);
    }

    assertTrue("Should have a server1 split", locations.contains("server1:16020"));
    assertTrue("Should have an UNKNOWN_SERVER split",
      locations.contains(PhoenixSyncTableInputFormat.UNKNOWN_SERVER));

    // Verify key range counts: server1 gets 2, UNKNOWN_SERVER gets 2
    for (InputSplit s : result) {
      PhoenixInputSplit ps = (PhoenixInputSplit) s;
      String loc = ps.getLocations()[0];
      if ("server1:16020".equals(loc)) {
        assertEquals("server1 bucket should have 2 KeyRanges", 2, ps.getKeyRanges().size());
      } else {
        assertEquals("UNKNOWN_SERVER bucket should have 2 KeyRanges", 2, ps.getKeyRanges().size());
      }
    }
  }

  @Test
  public void testCoalesceSplitsFailureThrowsForGenuineClusterError() throws Exception {
    // A real cluster error (SQLException) should still propagate — not be swallowed.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));

    SQLException simulatedFailure =
      new SQLException("Simulated RegionServer communication failure");
    when(mockQueryServices.getTableRegionLocation(any(byte[].class), any(byte[].class)))
      .thenThrow(simulatedFailure);

    try {
      inputFormat.coalesceSplits(splits, mockQueryServices, physicalTableName);
      fail("Expected SQLException to be thrown for a genuine cluster error");
    } catch (SQLException e) {
      assertTrue("Exception message should be preserved",
        e.getMessage().contains("Simulated RegionServer communication failure"));
    }
  }

  /**
   * Helper method to create a mock HRegionLocation with the given server address and start key.
   */
  private HRegionLocation createMockRegionLocation(String serverAddress, byte[] startKey) {
    HRegionLocation mockRegionLocation = mock(HRegionLocation.class);
    ServerName mockServerName = mock(ServerName.class);
    // Create a real Address object instead of mocking it, since toString() is final
    // Parse the serverAddress string to extract hostname and port
    String[] parts = serverAddress.split(":");
    String hostname = parts[0];
    int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 16020;
    org.apache.hadoop.hbase.net.Address address =
      org.apache.hadoop.hbase.net.Address.fromParts(hostname, port);

    when(mockServerName.getAddress()).thenReturn(address);
    when(mockRegionLocation.getServerName()).thenReturn(mockServerName);
    return mockRegionLocation;
  }
}
