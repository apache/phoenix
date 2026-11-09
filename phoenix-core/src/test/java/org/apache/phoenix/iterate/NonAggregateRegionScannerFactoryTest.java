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
package org.apache.phoenix.iterate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.PhoenixScannerContext;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.memory.GlobalMemoryManager;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

/**
 * Tests for the region scanner that emits the cached topN rows.
 */
public class NonAggregateRegionScannerFactoryTest {

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final String REGION_NAME = "topn-region";

  private OrderedResultIterator iterator;
  private RegionScannerResultIterator delegate;
  private MemoryManager.MemoryChunk chunk;
  private Region region;

  @Before
  public void setUp() {
    iterator = mock(OrderedResultIterator.class);
    delegate = mock(RegionScannerResultIterator.class);
    when(iterator.getDelegate()).thenReturn(delegate);
    chunk = mock(MemoryManager.MemoryChunk.class);
    RegionInfo regionInfo = mock(RegionInfo.class);
    when(regionInfo.getRegionNameAsString()).thenReturn(REGION_NAME);
    region = mock(Region.class);
    when(region.getRegionInfo()).thenReturn(regionInfo);
  }

  private static Tuple dummyTuple(String row) {
    List<Cell> cells = new ArrayList<>(1);
    ScanUtil.getDummyResult(Bytes.toBytes(row), cells);
    return new ResultTuple(Result.create(cells));
  }

  private static Tuple realTuple(String row) {
    Cell cell = new KeyValue(Bytes.toBytes(row), FAMILY, QUALIFIER, Bytes.toBytes(row));
    return new ResultTuple(Result.create(Collections.singletonList(cell)));
  }

  /**
   * Return an open-time scanner context. The pread limit of a store scanner sets the return
   * immediately flag on this context during the topN build.
   */
  private static PhoenixScannerContext openContext(boolean returnImmediately) {
    PhoenixScannerContext sc = new PhoenixScannerContext(false);
    if (returnImmediately) {
      PhoenixScannerContext.setReturnImmediately(sc);
    }
    return sc;
  }

  private RegionScanner newScanner(Tuple firstTuple, PhoenixScannerContext sc) {
    return NonAggregateRegionScannerFactory.newTopNScanner(mock(RegionScanner.class), iterator,
      firstTuple, sc, chunk, region);
  }

  private static void assertRow(String row, List<Cell> results) {
    assertEquals(1, results.size());
    assertFalse(ScanUtil.isDummy(results));
    assertArrayEquals(Bytes.toBytes(row), CellUtil.cloneRow(results.get(0)));
  }

  private static void assertDummy(String row, List<Cell> results) {
    assertTrue(ScanUtil.isDummy(results));
    assertArrayEquals(Bytes.toBytes(row), CellUtil.cloneRow(results.get(0)));
  }

  @Test
  public void testTopNScannerPullsBeforeEmittingHeldDummy() throws Exception {
    RegionScanner scanner = newScanner(dummyTuple("d1"), openContext(true));
    List<Cell> results = new ArrayList<>();
    // The open RPC sends a heartbeat and does no more work.
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());
    verify(iterator, never()).next();

    // The next RPC completes the build and returns the rows.
    when(iterator.next()).thenReturn(realTuple("r1"), realTuple("r2"), null);
    PhoenixScannerContext rpcContext = new PhoenixScannerContext(false);
    assertTrue(scanner.next(results, rpcContext));
    assertRow("r1", results);
    InOrder order = inOrder(delegate, iterator);
    order.verify(delegate).setRegionScannerContext(rpcContext);
    order.verify(iterator, times(2)).next();

    results.clear();
    assertFalse(scanner.next(results, rpcContext));
    assertRow("r2", results);
    verify(iterator, times(3)).next();
  }

  @Test
  public void testTopNScannerEmitsDummyOnlyWhenPullReturnsDummy() throws Exception {
    RegionScanner scanner = newScanner(dummyTuple("d1"), openContext(true));
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());

    // The iterator pages out again, so the scanner sends the new dummy and pulls no more.
    when(iterator.next()).thenReturn(dummyTuple("d2"));
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertDummy("d2", results);
    verify(iterator, times(1)).next();

    // The next RPC pulls again and gets the rows.
    when(iterator.next()).thenReturn(realTuple("r1"), (Tuple) null);
    results.clear();
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r1", results);
    verify(iterator, times(3)).next();
  }

  @Test
  public void testTopNScannerEndsWithoutDummyWhenPullReturnsNoRows() throws Exception {
    RegionScanner scanner = newScanner(dummyTuple("d1"), openContext(true));
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());

    when(iterator.next()).thenReturn(null);
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());
    assertTrue(scanner.isFilterDone());
  }

  @Test
  public void testTopNScannerKeepsOpenHeartbeatForCompleteBuild() throws Exception {
    RegionScanner scanner = newScanner(realTuple("r1"), openContext(true));
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());
    verify(iterator, never()).next();

    when(iterator.next()).thenReturn(null);
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r1", results);
  }

  @Test
  public void testTopNScannerFirstCallEmitsHeldDummyWithoutPullFirst() throws Exception {
    // Without the return immediately flag the first call sends the held tuple as before.
    RegionScanner scanner = newScanner(dummyTuple("d1"), openContext(false));
    when(iterator.next()).thenReturn(realTuple("r1"), (Tuple) null);
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertDummy("d1", results);
    verify(iterator, times(1)).next();

    results.clear();
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r1", results);
  }

  @Test
  public void testTopNScannerPullsFirstAfterFirstCallSendsDummy() throws Exception {
    // The first call has no heartbeat. It sends the held dummy and holds the next dummy.
    RegionScanner scanner = newScanner(dummyTuple("d1"), openContext(false));
    when(iterator.next()).thenReturn(dummyTuple("d2"), realTuple("r1"), realTuple("r2"), null);
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertDummy("d1", results);
    verify(iterator, times(1)).next();

    // The second call does not send the held dummy. It pulls first and sends the first row.
    results.clear();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r1", results);
    verify(iterator, times(3)).next();

    results.clear();
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r2", results);
    verify(iterator, times(4)).next();
  }

  @Test
  public void testTopNScannerKeepsHeldDummyWhenPullFails() throws Exception {
    RegionScanner scanner = newScanner(dummyTuple("d1"), openContext(true));
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());

    // A failed pull sends no row and names the region in the error.
    doThrow(new SQLException("pull failed")).when(iterator).next();
    try {
      scanner.next(results, new PhoenixScannerContext(false));
      fail("Expected an IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(REGION_NAME));
    }
    assertTrue(results.isEmpty());
    assertFalse(scanner.isFilterDone());

    // The scanner still holds the dummy, so the next call pulls again.
    doReturn(realTuple("r1"), (Tuple) null).when(iterator).next();
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r1", results);
    verify(iterator, times(3)).next();
  }

  @Test
  public void testTopNScannerPullsFirstWithoutScannerContext() throws Exception {
    RegionScanner scanner = newScanner(dummyTuple("d1"), openContext(true));
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());

    // A call with no scanner context clears the delegate context before the pull.
    when(iterator.next()).thenReturn(realTuple("r1"), (Tuple) null);
    assertFalse(scanner.next(results));
    assertRow("r1", results);
    InOrder order = inOrder(delegate, iterator);
    order.verify(delegate).setRegionScannerContext(null);
    order.verify(iterator, times(2)).next();
  }

  @Test
  public void testTopNScannerResizesChunkWhenBuildCompletesAfterOpen() throws Exception {
    // The build pages out at open, so the chunk keeps the estimated size.
    MemoryManager memoryManager = new GlobalMemoryManager(1024 * 1024);
    MemoryManager.MemoryChunk sizedChunk = memoryManager.allocate(1024);
    when(iterator.getByteSize()).thenReturn(0L);
    when(iterator.next()).thenReturn(dummyTuple("d1"));
    Tuple firstTuple = NonAggregateRegionScannerFactory.getFirstTopNTuple(iterator, sizedChunk);
    assertEquals(1024, sizedChunk.getSize());
    RegionScanner scanner = NonAggregateRegionScannerFactory.newTopNScanner(
      mock(RegionScanner.class), iterator, firstTuple, openContext(true), sizedChunk, region);
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertEquals(1024, sizedChunk.getSize());

    // The pull pages out again, so the build is still not complete.
    when(iterator.next()).thenReturn(dummyTuple("d2"));
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertDummy("d2", results);
    assertEquals(1024, sizedChunk.getSize());

    // The build completes. The chunk then holds the size of the cached rows.
    when(iterator.getByteSize()).thenReturn(4096L);
    when(iterator.next()).thenReturn(realTuple("r1"), realTuple("r2"), null);
    results.clear();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r1", results);
    assertEquals(4096, sizedChunk.getSize());
    assertEquals(1024 * 1024 - 4096, memoryManager.getAvailableMemory());

    results.clear();
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r2", results);
    assertEquals(4096, sizedChunk.getSize());
    scanner.close();
    assertEquals(1024 * 1024, memoryManager.getAvailableMemory());
  }

  @Test
  public void testTopNScannerResizesChunkWhenBuildEndsWithNoRows() throws Exception {
    MemoryManager memoryManager = new GlobalMemoryManager(1024 * 1024);
    MemoryManager.MemoryChunk sizedChunk = memoryManager.allocate(1024);
    when(iterator.next()).thenReturn(dummyTuple("d1"));
    Tuple firstTuple = NonAggregateRegionScannerFactory.getFirstTopNTuple(iterator, sizedChunk);
    RegionScanner scanner = NonAggregateRegionScannerFactory.newTopNScanner(
      mock(RegionScanner.class), iterator, firstTuple, openContext(true), sizedChunk, region);
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));

    // The build completes with no row for the client. The chunk still gets the real size.
    when(iterator.getByteSize()).thenReturn(512L);
    when(iterator.next()).thenReturn(null);
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertTrue(results.isEmpty());
    assertEquals(512, sizedChunk.getSize());
    scanner.close();
    assertEquals(1024 * 1024, memoryManager.getAvailableMemory());
  }

  @Test
  public void testTopNScannerFailsWhenChunkResizeExceedsMemory() throws Exception {
    MemoryManager memoryManager = new GlobalMemoryManager(2048);
    MemoryManager.MemoryChunk sizedChunk = memoryManager.allocate(1024);
    when(iterator.next()).thenReturn(dummyTuple("d1"));
    Tuple firstTuple = NonAggregateRegionScannerFactory.getFirstTopNTuple(iterator, sizedChunk);
    RegionScanner scanner = NonAggregateRegionScannerFactory.newTopNScanner(
      mock(RegionScanner.class), iterator, firstTuple, openContext(true), sizedChunk, region);
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));

    // The cached rows need more memory than the pool has, so the call fails.
    when(iterator.getByteSize()).thenReturn(4096L);
    when(iterator.next()).thenReturn(realTuple("r1"), (Tuple) null);
    try {
      scanner.next(results, new PhoenixScannerContext(false));
      fail("Expected an IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(REGION_NAME));
    }
    assertTrue(results.isEmpty());
    assertEquals(1024, sizedChunk.getSize());
    scanner.close();
    assertEquals(2048, memoryManager.getAvailableMemory());
  }

  @Test
  public void testTopNScannerKeepsChunkSizeWhenFirstTupleIsReal() throws Exception {
    // The build completes at open, so the open sizes the chunk and the scanner does not resize it.
    MemoryManager memoryManager = new GlobalMemoryManager(1024 * 1024);
    MemoryManager.MemoryChunk sizedChunk = memoryManager.allocate(1024);
    when(iterator.getByteSize()).thenReturn(2048L);
    when(iterator.next()).thenReturn(realTuple("r1"));
    Tuple firstTuple = NonAggregateRegionScannerFactory.getFirstTopNTuple(iterator, sizedChunk);
    assertEquals(2048, sizedChunk.getSize());
    RegionScanner scanner = NonAggregateRegionScannerFactory.newTopNScanner(
      mock(RegionScanner.class), iterator, firstTuple, openContext(false), sizedChunk, region);
    when(iterator.getByteSize()).thenReturn(4096L);
    when(iterator.next()).thenReturn(realTuple("r2"), (Tuple) null);
    List<Cell> results = new ArrayList<>();
    assertTrue(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r1", results);
    results.clear();
    assertFalse(scanner.next(results, new PhoenixScannerContext(false)));
    assertRow("r2", results);
    assertEquals(2048, sizedChunk.getSize());
    scanner.close();
    assertEquals(1024 * 1024, memoryManager.getAvailableMemory());
  }

  @Test
  public void testTopNScannerClosesChunk() throws Exception {
    RegionScanner scanner = newScanner(realTuple("r1"), openContext(false));
    scanner.close();
    verify(chunk).close();
    verify(iterator).close();
  }
}
