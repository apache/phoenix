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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.EXPECTED_UPPER_REGION_KEY;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.LOCAL_INDEX;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;

/**
 * Tests the server-side boundary check and the boundary rewrite for local index scans. Each scan is
 * prepared as the client prepares it, including the client-side swap for reversed scans.
 */
public class LocalIndexScanBoundaryTest {

  private static final byte[] EMPTY = HConstants.EMPTY_BYTE_ARRAY;
  private static final byte[] E = Bytes.toBytes("e");
  private static final byte[] I = Bytes.toBytes("i");
  private static final byte[] O = Bytes.toBytes("o");
  private static final byte[] LOWER_SUFFIX = new byte[] { 0, 1 };
  private static final byte[] UPPER_SUFFIX = new byte[] { 0, 2 };

  private static Region region(byte[] startKey, byte[] endKey) {
    Region region = mock(Region.class);
    when(region.getRegionInfo()).thenReturn(RegionInfoBuilder.newBuilder(TableName.valueOf("T"))
      .setStartKey(startKey).setEndKey(endKey).build());
    return region;
  }

  private static Scan clientScan(byte[] regionStartKey, byte[] regionEndKey, boolean reversed) {
    Scan scan = new Scan();
    scan.setAttribute(LOCAL_INDEX, PDataType.TRUE_BYTES);
    ScanUtil.setLocalIndexAttributes(scan, 0, regionStartKey, regionEndKey, LOWER_SUFFIX,
      UPPER_SUFFIX);
    if (reversed) {
      ScanUtil.setReversed(scan);
      ScanUtil.setupReverseScan(scan);
    }
    return scan;
  }

  private static byte[] concat(byte[] prefix, byte[] suffix) {
    return Bytes.add(prefix, suffix);
  }

  private static void assertStale(Scan scan, Region region) {
    try {
      BaseScannerRegionObserver.throwIfScanOutOfRegion(scan, region);
      fail("Expected a stale region boundary failure");
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getCause() instanceof StaleRegionBoundaryCacheException);
    }
  }

  private static void assertScanRows(Scan scan, byte[] prefix, boolean reversed) {
    if (reversed) {
      assertTrue(scan.isReversed());
      assertArrayEquals(concat(prefix, UPPER_SUFFIX), scan.getStartRow());
      assertArrayEquals(concat(prefix, LOWER_SUFFIX), scan.getStopRow());
      assertFalse(scan.includeStartRow());
      assertTrue(scan.includeStopRow());
    } else {
      assertFalse(scan.isReversed());
      assertArrayEquals(concat(prefix, LOWER_SUFFIX), scan.getStartRow());
      assertArrayEquals(concat(prefix, UPPER_SUFFIX), scan.getStopRow());
      assertTrue(scan.includeStartRow());
      assertFalse(scan.includeStopRow());
    }
  }

  @Test
  public void testMatchingRegionPassesInBothDirections() throws Exception {
    for (boolean reversed : new boolean[] { false, true }) {
      // First region: the prefix is zero bytes with the length of the region end key.
      Scan scan = clientScan(EMPTY, E, reversed);
      BaseScannerRegionObserver.throwIfScanOutOfRegion(scan, region(EMPTY, E));
      assertScanRows(scan, new byte[E.length], reversed);

      scan = clientScan(E, I, reversed);
      BaseScannerRegionObserver.throwIfScanOutOfRegion(scan, region(E, I));
      assertScanRows(scan, E, reversed);

      // Last region: the end key is open.
      scan = clientScan(O, EMPTY, reversed);
      BaseScannerRegionObserver.throwIfScanOutOfRegion(scan, region(O, EMPTY));
      assertScanRows(scan, O, reversed);
    }
  }

  @Test
  public void testSplitIsDetectedInBothDirections() {
    for (boolean reversed : new boolean[] { false, true }) {
      // The client still sees region [e, o), but the region split at i.
      assertStale(clientScan(E, O, reversed), region(E, I));
      assertStale(clientScan(E, O, reversed), region(I, O));
      // The last region split at o.
      assertStale(clientScan(I, EMPTY, reversed), region(I, O));
      assertStale(clientScan(I, EMPTY, reversed), region(O, EMPTY));
    }
  }

  @Test
  public void testMergeIsDetectedInBothDirections() {
    for (boolean reversed : new boolean[] { false, true }) {
      // The client still sees region [e, i), but regions [e, i) and [i, o) merged.
      assertStale(clientScan(E, I, reversed), region(E, O));
    }
  }

  @Test
  public void testForwardScanWithoutExpectedUpperRegionKey() throws Exception {
    // Older clients do not set the attribute. The forward scan falls back to the stop row.
    Scan scan = clientScan(E, I, false);
    scan.setAttribute(EXPECTED_UPPER_REGION_KEY, null);
    BaseScannerRegionObserver.throwIfScanOutOfRegion(scan, region(E, I));
    assertScanRows(scan, E, false);
    scan = clientScan(E, I, false);
    scan.setAttribute(EXPECTED_UPPER_REGION_KEY, null);
    assertStale(scan, region(E, O));
  }
}
