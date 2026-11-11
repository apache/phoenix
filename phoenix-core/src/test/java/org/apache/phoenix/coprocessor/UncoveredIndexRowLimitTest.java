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

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.CHECK_VERIFY_COLUMN;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.NON_AGGREGATE_QUERY;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.UNCOVERED_GLOBAL_INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests which scanner gets the row limit of an index scan. The global index checker and the scan
 * observer both run their hooks for a checked index scan. The checker runs first.
 */
public class UncoveredIndexRowLimitTest {
  private static final long LIMIT = 2;

  private final RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
  @SuppressWarnings("unchecked")
  private final ObserverContext<RegionCoprocessorEnvironment> context = mock(ObserverContext.class);
  private final Scan scan = new Scan();

  @Before
  public void setUp() {
    when(context.getEnvironment()).thenReturn(env);
    when(env.getRegion()).thenReturn(mock(Region.class));
    scan.setAttribute(NON_AGGREGATE_QUERY, Bytes.toBytes(true));
    scan.setAttribute(SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
    scan.setFilter(new PageFilter(LIMIT));
  }

  /**
   * A server merge of a covered index can drop a verified index row. Thus the uncovered index
   * scanner must get the row limit and apply it after the merge.
   */
  @Test
  public void testServerMergeGivesRowLimitToUncoveredScanner() throws Exception {
    scan.setAttribute(CHECK_VERIFY_COLUMN, Bytes.toBytes(true));
    scan.setAttribute(UNCOVERED_GLOBAL_INDEX, Bytes.toBytes(true));
    openScanner();
    assertNull(scan.getFilter());
    assertEquals(LIMIT, getUncoveredScannerRowLimit());
  }

  @Test
  public void testCheckedScanKeepsRowLimitInChecker() throws Exception {
    scan.setAttribute(CHECK_VERIFY_COLUMN, Bytes.toBytes(true));
    openScanner();
    assertNull(scan.getFilter());
    assertNull(scan.getAttribute(UncoveredGlobalIndexRegionScanner.PAGE_SIZE_ATTRIB));
  }

  @Test
  public void testUncoveredIndexScanGivesRowLimitToUncoveredScanner() throws Exception {
    scan.setAttribute(UNCOVERED_GLOBAL_INDEX, Bytes.toBytes(true));
    openScanner();
    assertNull(scan.getFilter());
    assertEquals(LIMIT, getUncoveredScannerRowLimit());
  }

  private void openScanner() throws Exception {
    new GlobalIndexChecker().preScannerOpen(context, scan);
    new ScanRegionObserver().preScannerOpen(context, scan);
  }

  private long getUncoveredScannerRowLimit() {
    byte[] limit = scan.getAttribute(UncoveredGlobalIndexRegionScanner.PAGE_SIZE_ATTRIB);
    return limit == null ? Long.MAX_VALUE : Bytes.toLong(limit);
  }
}
