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
package org.apache.phoenix.hbase.index.hnsw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.MemStoreSize;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Verifies the shared rules of the segment stores, the listing scan and the payload read, with a
 * read-only {@link HnswSegmentStore.TableStore}. The tests also verify the flush wait of a
 * {@link HnswSegmentStore.RegionStore} write.
 */
public class HnswSegmentStoreTest {
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] KEY = Bytes.toBytes("HNSW_INDEX");
  private static final HnswSegment.Descriptor DESCRIPTOR =
    new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 100L, Bytes.toBytes("f"), 10, 90L,
      HnswSegment.definitionDigest("definition"));
  private static final byte[] PAYLOAD_QUALIFIER =
    HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.PAYLOAD_TAG);

  /**
   * The listing scans only the cells of the index in {@code V#}, and gets MOB references as stored.
   * It decodes the segments of all rows. A filter on the server removes the payload cells, which
   * hold the full payload until the {@code V#} flush.
   */
  @Test
  public void testList() throws Exception {
    HnswSegment.Descriptor other = new HnswSegment.Descriptor(KEY, Bytes.toBytes("f"), 200L,
      new byte[0], 3, 200L, DESCRIPTOR.definition);
    List<Cell> first = new ArrayList<>(HnswSegmentTest.cells(DESCRIPTOR.anchor, DESCRIPTOR, true));
    List<Cell> second = new ArrayList<>(HnswSegmentTest.cells(other.anchor, other, true));
    Table table = mock(Table.class);
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator())
      .thenReturn(Arrays.asList(Result.create(first), Result.create(second)).iterator());
    ArgumentCaptor<Scan> scan = ArgumentCaptor.forClass(Scan.class);
    when(table.getScanner(scan.capture())).thenReturn(scanner);

    List<HnswSegment.Descriptor> listed = new HnswSegmentStore.TableStore(table).list(KEY);
    assertEquals(2, listed.size());
    HnswSegmentTest.assertDescriptor(DESCRIPTOR, listed.get(0));
    HnswSegmentTest.assertDescriptor(other, listed.get(1));
    assertArrayEquals(HnswSegmentTest.WRITER, listed.get(0).writer);
    Scan s = scan.getValue();
    assertEquals(1, s.getFamilyMap().size());
    assertTrue(s.getFamilyMap().containsKey(FAMILY));
    assertTrue(MobUtils.isRawMobScan(s));
    Filter filter = s.getFilter();
    for (Cell cell : first) {
      byte tag =
        cell.getQualifierArray()[cell.getQualifierOffset() + cell.getQualifierLength() - 1];
      assertEquals(Character.toString((char) tag), tag != HnswSegment.PAYLOAD_TAG,
        filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    }
    // The filter also removes the cells of another index whose key starts with the key of this
    // index
    for (Cell cell : HnswSegmentTest.cells(DESCRIPTOR.anchor,
      new HnswSegment.Descriptor(Bytes.toBytes("HNSW_INDEX2"), DESCRIPTOR.startKey, 100L,
        DESCRIPTOR.endKey, 10, 90L, DESCRIPTOR.definition),
      true)) {
      assertFalse(filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    }
  }

  /**
   * A payload read gets the payload cell from the anchor row. It asks HBase for an empty value if
   * the MOB file of the reference is missing or corrupt.
   */
  @Test
  public void testReadPayload() throws Exception {
    Table table = mock(Table.class);
    ArgumentCaptor<Get> get = ArgumentCaptor.forClass(Get.class);
    when(table.get(get.capture())).thenReturn(Result.create(Arrays
      .asList(new KeyValue(DESCRIPTOR.anchor, FAMILY, PAYLOAD_QUALIFIER, Bytes.toBytes("bytes")))));
    Cell cell = new HnswSegmentStore.TableStore(table).readPayload(DESCRIPTOR);
    assertEquals("bytes",
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    Get g = get.getValue();
    assertArrayEquals(Bytes.toBytes("b\0"), g.getRow());
    assertTrue(g.getFamilyMap().get(FAMILY).contains(PAYLOAD_QUALIFIER));
    assertEquals(1, g.getFamilyMap().get(FAMILY).size());
    assertTrue(Bytes.toBoolean(g.getAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS)));
  }

  /**
   * A missing payload cell means that the region retired the segment after the listing. HBase
   * returns an empty value for a reference whose MOB file is missing or corrupt. The store returns
   * this empty value. The open of the segment then fails as for a corrupt segment, so the region
   * rebuilds.
   */
  @Test
  public void testLostPayload() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
    Table table = mock(Table.class);
    when(table.get(any(Get.class))).thenReturn(Result.EMPTY_RESULT);
    HnswSegmentStore store = new HnswSegmentStore.TableStore(table);
    try {
      HnswSegment.open(store, DESCRIPTOR, allocator, "COSINE");
      fail("a missing payload must not load");
    } catch (HnswSegment.NotFoundException expected) {
    }

    table = mock(Table.class);
    when(table.get(any(Get.class))).thenReturn(Result.create(
      Arrays.asList(new KeyValue(DESCRIPTOR.anchor, FAMILY, PAYLOAD_QUALIFIER, new byte[0]))));
    store = new HnswSegmentStore.TableStore(table);
    assertEquals(0, store.readPayload(DESCRIPTOR).getValueLength());
    try {
      HnswSegment.open(store, DESCRIPTOR, allocator, "COSINE");
      fail("an empty payload must not load");
    } catch (HnswSegment.NotFoundException e) {
      fail("an empty payload is corrupt, not retired");
    } catch (DoNotRetryIOException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().startsWith("Corrupt HNSW segment"));
    }
    assertEquals(0, allocator.getAllocatedBytes());
  }

  /**
   * A write asks the flush requester to flush {@code V#} until the {@code V#} memstore is empty.
   * The store repeats a refused request after a delay.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testWriteRepeatsRefusedFlush() throws Exception {
    MockRegion r = new MockRegion();
    long start = System.nanoTime();
    // The requester refuses the first request, as it does when another flush of the region is in
    // its queue
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        FlushLifeCycleTracker tracker = i.getArgument(2);
        if (r.requests.incrementAndGet() == 1) {
          tracker.notExecuted("another flush is queued");
          return false;
        }
        r.flushed.set(true);
        tracker.afterExecution();
        return true;
      });
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null);
    assertEquals(2, r.requests.get());
    assertTrue(r.flushed.get());
    assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(100));
    ArgumentCaptor<List<byte[]>> families = ArgumentCaptor.forClass(List.class);
    verify(r.requester, times(2)).requestFlush(eq(r.region), families.capture(),
      any(FlushLifeCycleTracker.class));
    for (List<byte[]> f : families.getAllValues()) {
      assertEquals(1, f.size());
      assertArrayEquals(FAMILY, f.get(0));
    }
  }

  /**
   * The requester can delay a request that it accepted, as it does while a store of the region has
   * too many files. A new request would replace the delayed request and start its delay again. Thus
   * the write waits for the answer to its request and makes no other request.
   */
  @Test(timeout = 30_000)
  public void testWriteWaitsForDelayedFlush() throws Exception {
    MockRegion r = new MockRegion();
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        r.requests.incrementAndGet();
        FlushLifeCycleTracker tracker = i.getArgument(2);
        // The flush runs after five checks of the memstore
        new Thread(() -> {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          r.flushed.set(true);
          tracker.afterExecution();
        }).start();
        return true;
      });
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null);
    assertEquals(1, r.requests.get());
    assertTrue(r.flushed.get());
  }

  /**
   * A flush for memory pressure, or the request of another caller, can remove a request from the
   * queue of the requester without an answer. The write asks again when its request has no answer
   * after twice the blocking wait time of HBase.
   */
  @Test(timeout = 30_000)
  public void testWriteRepeatsUnansweredFlush() throws Exception {
    MockRegion r = new MockRegion();
    r.conf.setInt("hbase.hstore.blockingWaitTime", 100);
    long start = System.nanoTime();
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        // The first request gets no answer
        if (r.requests.incrementAndGet() > 1) {
          r.flushed.set(true);
          ((FlushLifeCycleTracker) i.getArgument(2)).afterExecution();
        }
        return true;
      });
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null);
    assertEquals(2, r.requests.get());
    assertTrue(r.flushed.get());
    assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(200));
  }

  /** The flush wait of a write ends when the region closes. */
  @Test
  public void testWriteFlushEndsWhenRegionCloses() throws Exception {
    MockRegion r = new MockRegion();
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        r.requests.incrementAndGet();
        ((FlushLifeCycleTracker) i.getArgument(2)).notExecuted("another flush is queued");
        return false;
      });
    when(r.region.isClosing()).thenAnswer(i -> r.requests.get() >= 2);
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null);
    assertEquals(2, r.requests.get());
    assertFalse(r.flushed.get());
  }

  /**
   * A write records the encoded name of its region in the W cell, at the segment time. A listing of
   * the written cells returns this name as the writer of the segment. A retirement deletes all
   * cells of the write, also the W cell.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testWriteRecordsWriter() throws Exception {
    MockRegion r = new MockRegion();
    r.flushed.set(true);
    ArgumentCaptor<List<Mutation>> mutations = ArgumentCaptor.forClass(List.class);
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(r.env);
    store.write(DESCRIPTOR, Bytes.toBytes("payload"));
    store.retire(Collections.singletonList(DESCRIPTOR));
    verify(r.region, times(2)).mutateRowsWithLocks(mutations.capture(), anyCollection(), anyLong(),
      anyLong());
    Put put = (Put) mutations.getAllValues().get(0).get(0);
    byte[] writer = r.region.getRegionInfo().getEncodedNameAsBytes();
    List<Cell> w =
      put.get(FAMILY, HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.WRITER_TAG));
    assertEquals(1, w.size());
    assertArrayEquals(writer, CellUtil.cloneValue(w.get(0)));
    assertEquals(DESCRIPTOR.time, w.get(0).getTimestamp());

    HnswSegment.Listing listing = new HnswSegment.Listing();
    for (Cell cell : put.getFamilyCellMap().get(FAMILY)) {
      listing.add(cell);
    }
    List<HnswSegment.Descriptor> listed = listing.descriptors();
    assertEquals(1, listed.size());
    HnswSegmentTest.assertDescriptor(DESCRIPTOR, listed.get(0));
    assertArrayEquals(writer, listed.get(0).writer);

    Delete delete = (Delete) mutations.getAllValues().get(1).get(0);
    List<String> deleted = new ArrayList<>();
    for (Cell marker : delete.getFamilyCellMap().get(FAMILY)) {
      assertEquals(Cell.Type.DeleteColumn, marker.getType());
      assertEquals(DESCRIPTOR.time, marker.getTimestamp());
      deleted.add(Bytes.toStringBinary(CellUtil.cloneQualifier(marker)));
    }
    for (Cell cell : put.getFamilyCellMap().get(FAMILY)) {
      assertTrue(deleted.contains(Bytes.toStringBinary(CellUtil.cloneQualifier(cell))));
    }
  }

  // A mock region whose V# memstore holds data until the test marks it as flushed
  private static final class MockRegion {
    final RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    final HRegion region = mock(HRegion.class);
    final FlushRequester requester = mock(FlushRequester.class);
    final Configuration conf = HBaseConfiguration.create();
    final AtomicBoolean flushed = new AtomicBoolean();
    final AtomicInteger requests = new AtomicInteger();

    MockRegion() {
      RegionServerServices services = mock(RegionServerServices.class);
      HStore store = mock(HStore.class);
      MemStoreSize full = mock(MemStoreSize.class);
      when(full.getDataSize()).thenReturn(1024L);
      MemStoreSize empty = mock(MemStoreSize.class);
      when(env.getRegion()).thenReturn(region);
      when(env.getConfiguration()).thenReturn(conf);
      when(region.getRegionInfo())
        .thenReturn(RegionInfoBuilder.newBuilder(TableName.valueOf("T")).build());
      when(env.getOnlineRegions()).thenReturn(services);
      when(services.getFlushRequester()).thenReturn(requester);
      when(region.getStore(FAMILY)).thenReturn(store);
      when(store.getFlushableSize()).thenAnswer(i -> flushed.get() ? empty : full);
    }
  }
}
