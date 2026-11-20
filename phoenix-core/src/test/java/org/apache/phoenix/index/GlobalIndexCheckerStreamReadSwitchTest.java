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
package org.apache.phoenix.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.PhoenixScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.filter.PagingFilter;
import org.junit.Test;

/**
 * Checks when the index checker switches a scanner that it opens to stream read, and where the
 * stream read scanner starts.
 */
public class GlobalIndexCheckerStreamReadSwitchTest {
  private static final long PAGE_SIZE_MS = Long.MAX_VALUE;
  private static final byte[] START_ROW = Bytes.toBytes("b");
  private static final byte[] ROW = Bytes.toBytes("c");
  private static final byte[] NEXT_ROW = Bytes.toBytes("d");

  private static Scan newScan() {
    return new Scan().withStartRow(START_ROW, true).setFilter(new PagingFilter(null, PAGE_SIZE_MS));
  }

  private static List<Cell> row(byte[] rowKey) {
    return Collections.singletonList(
      new KeyValue(rowKey, Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("v")));
  }

  private static PhoenixScannerContext returnImmediatelyContext() {
    PhoenixScannerContext context = new PhoenixScannerContext(false);
    PhoenixScannerContext.setReturnImmediately(context);
    return context;
  }

  @Test
  public void testRpcScannerDoesNotSwitch() throws Exception {
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(newScan());
    streamReadSwitch.resultReturned(row(ROW));
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertFalse(streamReadSwitch.isPending());
  }

  @Test
  public void testReplacementScannerSwitchesAfterLastRow() throws Exception {
    Scan scan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(scan);
    streamReadSwitch.scannerOpened(new Scan(scan));
    PhoenixScannerContext context = new PhoenixScannerContext(false);
    streamReadSwitch.resultReturned(row(START_ROW));
    streamReadSwitch.checkPreadLimit(false, context, PAGE_SIZE_MS, null);
    assertFalse(streamReadSwitch.isPending());
    PhoenixScannerContext.setReturnImmediately(context);
    streamReadSwitch.resultReturned(row(ROW));
    streamReadSwitch.checkPreadLimit(false, context, PAGE_SIZE_MS, null);
    assertTrue(streamReadSwitch.isPending());
    assertArrayEquals(ROW, streamReadSwitch.getRestartRow());
    assertFalse(streamReadSwitch.isRestartRowInclusive());
  }

  @Test
  public void testReplacementScannerWithoutRowSwitchesAtScanStart() throws Exception {
    Scan scan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(scan);
    streamReadSwitch.resultReturned(row(ROW));
    streamReadSwitch.scannerOpened(new Scan(scan));
    streamReadSwitch.resultReturned(Collections.emptyList());
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertTrue(streamReadSwitch.isPending());
    assertArrayEquals(START_ROW, streamReadSwitch.getRestartRow());
    assertTrue(streamReadSwitch.isRestartRowInclusive());
    streamReadSwitch.scannerOpened(new Scan(scan).withStartRow(START_ROW, false));
    assertFalse(streamReadSwitch.isRestartRowInclusive());
  }

  @Test
  public void testFlagSetBeforeCallDoesNotSwitch() throws Exception {
    Scan scan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(scan);
    streamReadSwitch.scannerOpened(new Scan(scan));
    streamReadSwitch.checkPreadLimit(true, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertFalse(streamReadSwitch.isPending());
  }

  @Test
  public void testPagingDoesNotSwitch() throws Exception {
    Scan scan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(scan);
    streamReadSwitch.scannerOpened(new Scan(scan));
    // A negative page size makes the page time out at once.
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), -1, null);
    assertFalse(streamReadSwitch.isPending());
    // A zero page size stops the paging filter at the first row.
    PagingFilter pagingFilter = new PagingFilter(null, 0);
    pagingFilter.filterRowKey(row(ROW).get(0));
    pagingFilter.hasFilterRow();
    assertTrue(pagingFilter.isStopped());
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, pagingFilter);
    assertFalse(streamReadSwitch.isPending());
  }

  @Test
  public void testNonDefaultReadTypeDoesNotSwitch() throws Exception {
    for (Scan.ReadType readType : new Scan.ReadType[] { Scan.ReadType.PREAD,
      Scan.ReadType.STREAM }) {
      Scan scan = newScan().setReadType(readType);
      GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
        new GlobalIndexChecker.StreamReadSwitch(scan);
      streamReadSwitch.scannerOpened(new Scan(scan));
      streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
      assertFalse(streamReadSwitch.isPending());
    }
  }

  @Test
  public void testScanWithoutPagingFilterDoesNotSwitch() throws Exception {
    Scan scan = new Scan().withStartRow(START_ROW, true);
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(scan);
    streamReadSwitch.scannerOpened(new Scan(scan));
    streamReadSwitch.resultReturned(row(ROW));
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertFalse(streamReadSwitch.isPending());
  }

  @Test
  public void testStreamScannerDoesNotSwitch() throws Exception {
    Scan indexScan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(indexScan);
    streamReadSwitch.scannerOpened(indexScan);
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertTrue(streamReadSwitch.isPending());
    streamReadSwitch.scannerOpened(streamReadSwitch.createStreamScan(indexScan, 42));
    assertFalse(streamReadSwitch.isPending());
    assertTrue(streamReadSwitch.isSwitched());
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertFalse(streamReadSwitch.isPending());
  }

  @Test
  public void testStreamScanStartsAfterLastRow() throws Exception {
    Scan indexScan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(indexScan);
    streamReadSwitch.scannerOpened(indexScan);
    streamReadSwitch.resultReturned(row(ROW));
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    Scan streamScan = streamReadSwitch.createStreamScan(indexScan, 42);
    assertEquals(Scan.ReadType.STREAM, streamScan.getReadType());
    assertArrayEquals(ROW, streamScan.getStartRow());
    assertFalse(streamScan.includeStartRow());
    assertEquals(42, PackagePrivateFieldAccessor.getMvccReadPoint(streamScan));
    // The index scan is the template for later repair scanners, so it must not change.
    assertEquals(Scan.ReadType.DEFAULT, indexScan.getReadType());
    assertArrayEquals(START_ROW, indexScan.getStartRow());
    assertTrue(indexScan.includeStartRow());
  }

  @Test
  public void testRepairAfterSwitchStartsWithPread() throws Exception {
    Scan indexScan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(indexScan);
    // A repair scanner passes the pread limit and changes to stream read.
    streamReadSwitch.scannerOpened(indexScan);
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertTrue(streamReadSwitch.isPending());
    streamReadSwitch.scannerOpened(streamReadSwitch.createStreamScan(indexScan, 42));
    assertTrue(streamReadSwitch.isSwitched());
    // A later repair opens a scanner with the index scan, which still uses the default read type.
    indexScan.withStartRow(ROW, false);
    assertEquals(Scan.ReadType.DEFAULT, indexScan.getReadType());
    streamReadSwitch.scannerOpened(indexScan);
    assertFalse(streamReadSwitch.isSwitched());
    assertFalse(streamReadSwitch.isPending());
    // The repair scanner can switch on its own when it passes the pread limit.
    streamReadSwitch.resultReturned(row(NEXT_ROW));
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertTrue(streamReadSwitch.isPending());
    Scan streamScan = streamReadSwitch.createStreamScan(indexScan, 43);
    assertEquals(Scan.ReadType.STREAM, streamScan.getReadType());
    assertArrayEquals(NEXT_ROW, streamScan.getStartRow());
    assertFalse(streamScan.includeStartRow());
  }

  @Test
  public void testRepairReopenClearsPendingSwitch() throws Exception {
    Scan indexScan = newScan();
    GlobalIndexChecker.StreamReadSwitch streamReadSwitch =
      new GlobalIndexChecker.StreamReadSwitch(indexScan);
    streamReadSwitch.scannerOpened(indexScan);
    streamReadSwitch.checkPreadLimit(false, returnImmediatelyContext(), PAGE_SIZE_MS, null);
    assertTrue(streamReadSwitch.isPending());
    // A repair replaces the scanner before the switch. The new scanner starts with pread.
    streamReadSwitch.scannerOpened(indexScan.withStartRow(ROW, true));
    assertFalse(streamReadSwitch.isPending());
    assertFalse(streamReadSwitch.isSwitched());
  }
}
