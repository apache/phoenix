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
import static org.junit.Assert.assertNull;
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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
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
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Verifies the shared rules of the segment stores, the listing scan and the payload read, with a
 * read-only {@link HnswSegmentStore.TableStore}. The tests also verify that a base write of a
 * {@link HnswSegmentStore.RegionStore} waits for the flush, and that a delta write does not wait.
 */
public class HnswSegmentStoreTest {
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] KEY = Bytes.toBytes("HNSW_INDEX");
  private static final HnswSegment.Descriptor DESCRIPTOR =
    new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 100L, Bytes.toBytes("f"), 10, null, 90L,
      HnswSegment.definitionDigest("definition"));
  private static final byte[] PAYLOAD_QUALIFIER =
    HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.PAYLOAD_TAG);

  /**
   * The listing scans only the cells of the index in {@code V#}, and gets MOB references as stored.
   * It decodes the segments of all rows. A filter on the server removes the payload cells and the
   * tombstones cells. Until the {@code V#} flush, these cells hold their full values, which can be
   * large. The filter keeps the base time of a delta.
   */
  @Test
  public void testList() throws Exception {
    HnswSegment.Descriptor other = new HnswSegment.Descriptor(KEY, Bytes.toBytes("f"), 200L,
      new byte[0], 3, null, 200L, DESCRIPTOR.definition);
    HnswSegment.Descriptor delta = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L,
      Bytes.toBytes("f"), 2, 100L, 150L, DESCRIPTOR.definition);
    List<Cell> first = new ArrayList<>(HnswSegmentTest.cells(DESCRIPTOR.anchor, DESCRIPTOR, true));
    first.addAll(HnswSegmentTest.cells(delta.anchor, delta, true));
    first.add(new KeyValue(delta.anchor, FAMILY,
      HnswSegment.qualifier(delta.identity, HnswSegment.TOMBSTONES_TAG),
      HnswSegment.encodeKeys(new byte[][] { Bytes.toBytes("c") })));
    List<Cell> second = new ArrayList<>(HnswSegmentTest.cells(other.anchor, other, true));
    Table table = mock(Table.class);
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator())
      .thenReturn(Arrays.asList(Result.create(first), Result.create(second)).iterator());
    ArgumentCaptor<Scan> scan = ArgumentCaptor.forClass(Scan.class);
    when(table.getScanner(scan.capture())).thenReturn(scanner);

    List<HnswSegment.Descriptor> listed = new HnswSegmentStore.TableStore(table).list(KEY);
    assertEquals(3, listed.size());
    HnswSegmentTest.assertDescriptor(DESCRIPTOR, listed.get(0));
    HnswSegmentTest.assertDescriptor(delta, listed.get(1));
    HnswSegmentTest.assertDescriptor(other, listed.get(2));
    for (HnswSegment.Descriptor d : listed) {
      assertArrayEquals(HnswSegmentTest.WRITER, d.writer);
    }
    Scan s = scan.getValue();
    assertEquals(1, s.getFamilyMap().size());
    assertTrue(s.getFamilyMap().containsKey(FAMILY));
    assertTrue(MobUtils.isRawMobScan(s));
    Filter filter = s.getFilter();
    for (Cell cell : first) {
      byte tag =
        cell.getQualifierArray()[cell.getQualifierOffset() + cell.getQualifierLength() - 1];
      assertEquals(Character.toString((char) tag),
        tag != HnswSegment.PAYLOAD_TAG && tag != HnswSegment.TOMBSTONES_TAG,
        filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    }
    // The filter also removes the cells of another index whose key starts with the key of this
    // index
    for (Cell cell : HnswSegmentTest.cells(
      DESCRIPTOR.anchor, new HnswSegment.Descriptor(Bytes.toBytes("HNSW_INDEX2"),
        DESCRIPTOR.startKey, 100L, DESCRIPTOR.endKey, 10, null, 90L, DESCRIPTOR.definition),
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
   * A tombstones read gets the tombstones cell and the count cell of a delta from the anchor row.
   * It asks HBase for an empty value if the MOB file of the reference is missing or corrupt. A
   * delta without a tombstones cell records no deleted rows. A missing count cell means that the
   * region retired the segment after the listing.
   */
  @Test
  public void testReadTombstones() throws Exception {
    HnswSegment.Descriptor delta = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L,
      Bytes.toBytes("f"), 0, 100L, 150L, DESCRIPTOR.definition);
    byte[] tombstones = HnswSegment.qualifier(delta.identity, HnswSegment.TOMBSTONES_TAG);
    byte[] count = HnswSegment.qualifier(delta.identity, HnswSegment.COUNT_TAG);
    KeyValue countCell = new KeyValue(delta.anchor, FAMILY, count, Bytes.toBytes(0));
    Table table = mock(Table.class);
    ArgumentCaptor<Get> get = ArgumentCaptor.forClass(Get.class);
    when(table.get(get.capture())).thenReturn(Result.create(Arrays.asList(countCell,
      new KeyValue(delta.anchor, FAMILY, tombstones, Bytes.toBytes("bytes")))));
    HnswSegmentStore store = new HnswSegmentStore.TableStore(table);
    Cell cell = store.readTombstones(delta);
    assertEquals("bytes",
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    Get g = get.getValue();
    assertArrayEquals(Bytes.toBytes("b\0"), g.getRow());
    assertEquals(2, g.getFamilyMap().get(FAMILY).size());
    assertTrue(g.getFamilyMap().get(FAMILY).contains(tombstones));
    assertTrue(g.getFamilyMap().get(FAMILY).contains(count));
    assertTrue(Bytes.toBoolean(g.getAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS)));

    when(table.get(any(Get.class))).thenReturn(Result.create(Arrays.asList(countCell)));
    assertNull(store.readTombstones(delta));
    when(table.get(any(Get.class))).thenReturn(Result.EMPTY_RESULT);
    try {
      store.readTombstones(delta);
      fail("the tombstones of a retired delta must not be read");
    } catch (HnswSegment.NotFoundException expected) {
    }
  }

  /**
   * The write of a delta adds its base time and its tombstones to the cells of a segment. The write
   * of a base adds neither. All cells have the segment time as their timestamp.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testWriteDelta() throws Exception {
    MockRegion r = new MockRegion();
    r.flushed.set(true);
    ArgumentCaptor<List<Mutation>> mutations = ArgumentCaptor.forClass(List.class);
    HnswSegment.Descriptor delta = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L,
      Bytes.toBytes("f"), 1, 100L, 150L, DESCRIPTOR.definition);
    byte[][] tombstones = { Bytes.toBytes("c"), Bytes.toBytes("d") };
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(r.env);
    store.write(delta, Bytes.toBytes("payload"), tombstones);
    store.write(DESCRIPTOR, Bytes.toBytes("payload"), new byte[0][]);
    verify(r.region, times(2)).mutateRowsWithLocks(mutations.capture(), anyCollection(), anyLong(),
      anyLong());
    Put deltaPut = (Put) mutations.getAllValues().get(0).get(0);
    Put basePut = (Put) mutations.getAllValues().get(1).get(0);
    assertEquals(Durability.SKIP_WAL, deltaPut.getDurability());
    assertEquals(HnswSegment.TAGS.length, deltaPut.getFamilyCellMap().get(FAMILY).size());
    assertEquals(HnswSegment.TAGS.length - 2, basePut.getFamilyCellMap().get(FAMILY).size());
    for (Cell cell : deltaPut.getFamilyCellMap().get(FAMILY)) {
      assertEquals(delta.time, cell.getTimestamp());
    }
    assertEquals(100L, Bytes.toLong(CellUtil.cloneValue(deltaPut
      .get(FAMILY, HnswSegment.qualifier(delta.identity, HnswSegment.BASE_TIME_TAG)).get(0))));
    assertArrayEquals(HnswSegment.encodeKeys(tombstones), CellUtil.cloneValue(deltaPut
      .get(FAMILY, HnswSegment.qualifier(delta.identity, HnswSegment.TOMBSTONES_TAG)).get(0)));
    assertTrue(
      basePut.get(FAMILY, HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.BASE_TIME_TAG))
        .isEmpty());
    assertTrue(
      basePut.get(FAMILY, HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.TOMBSTONES_TAG))
        .isEmpty());
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
   * A base write asks the flush requester to flush {@code V#} until the {@code V#} memstore is
   * empty. The store repeats a refused request after a delay.
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
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null, new byte[0][]);
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
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null, new byte[0][]);
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
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null, new byte[0][]);
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
    new HnswSegmentStore.RegionStore(r.env).write(DESCRIPTOR, null, new byte[0][]);
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
    store.write(DESCRIPTOR, Bytes.toBytes("payload"), new byte[0][]);
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

  /**
   * A delta write asks for a flush of {@code V#} and does not wait for it. One thread writes the
   * deltas of all regions of the server. While a store of a region has too many files, the
   * requester delays the flush of the region. A wait would then delay the deltas of other regions.
   */
  @Test(timeout = 10_000)
  @SuppressWarnings("unchecked")
  public void testDeltaWriteDoesNotWaitForFlush() throws Exception {
    MockRegion r = new MockRegion();
    // The requester accepts the request and delays the flush past the end of the test
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        r.requests.incrementAndGet();
        return true;
      });
    HnswSegment.Descriptor delta = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L,
      Bytes.toBytes("f"), 1, 100L, 150L, DESCRIPTOR.definition);
    new HnswSegmentStore.RegionStore(r.env).write(delta, Bytes.toBytes("payload"),
      new byte[][] { Bytes.toBytes("c") });
    verify(r.region).mutateRowsWithLocks(anyList(), anyCollection(), anyLong(), anyLong());
    assertEquals(1, r.requests.get());
    assertFalse(r.flushed.get());
    ArgumentCaptor<List<byte[]>> families = ArgumentCaptor.forClass(List.class);
    verify(r.requester).requestFlush(eq(r.region), families.capture(),
      any(FlushLifeCycleTracker.class));
    assertEquals(1, families.getValue().size());
    assertArrayEquals(FAMILY, families.getValue().get(0));
  }

  /**
   * A listing also returns the row and the identity of each segment that has small cells but is not
   * well-formed. The test uses a segment without a W cell, and a segment outside the anchor row of
   * its key range.
   */
  @Test
  public void testListMalformed() throws Exception {
    HnswSegment.Descriptor noWriter = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 101L,
      Bytes.toBytes("f"), 1, null, 101L, DESCRIPTOR.definition);
    HnswSegment.Descriptor offAnchor = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 102L,
      Bytes.toBytes("f"), 1, null, 102L, DESCRIPTOR.definition);
    List<Cell> cells = new ArrayList<>(HnswSegmentTest.cells(DESCRIPTOR.anchor, DESCRIPTOR, true));
    for (Cell cell : HnswSegmentTest.cells(noWriter.anchor, noWriter, true)) {
      if (
        !Bytes.equals(CellUtil.cloneQualifier(cell),
          HnswSegment.qualifier(noWriter.identity, HnswSegment.WRITER_TAG))
      ) {
        cells.add(cell);
      }
    }
    cells.sort(CellComparator.getInstance());
    byte[] wrongRow = Bytes.toBytes("c");
    List<Cell> other = new ArrayList<>(HnswSegmentTest.cells(wrongRow, offAnchor, true));
    Table table = mock(Table.class);
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator())
      .thenReturn(Arrays.asList(Result.create(cells), Result.create(other)).iterator());
    when(table.getScanner(any(Scan.class))).thenReturn(scanner);
    List<byte[][]> malformed = new ArrayList<>();
    List<HnswSegment.Descriptor> listed =
      new HnswSegmentStore.TableStore(table).list(KEY, malformed);
    assertEquals(1, listed.size());
    HnswSegmentTest.assertDescriptor(DESCRIPTOR, listed.get(0));
    assertEquals(2, malformed.size());
    assertArrayEquals(noWriter.anchor, malformed.get(0)[0]);
    assertArrayEquals(noWriter.identity, malformed.get(0)[1]);
    assertArrayEquals(wrongRow, malformed.get(1)[0]);
    assertArrayEquals(offAnchor.identity, malformed.get(1)[1]);
  }

  /**
   * A listing of all cells of {@code V#} reads the MOB references as stored, and gets the length of
   * each value in place of the value. Thus it never transfers a payload, also when the payload is
   * in the memstore.
   */
  @Test
  public void testListCells() throws Exception {
    Table table = mock(Table.class);
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator()).thenReturn(Collections.<Result> emptyList().iterator());
    ArgumentCaptor<Scan> scan = ArgumentCaptor.forClass(Scan.class);
    when(table.getScanner(scan.capture())).thenReturn(scanner);
    List<Cell> cells = new ArrayList<>();
    new HnswSegmentStore.TableStore(table).listCells(cells::add);
    assertTrue(cells.isEmpty());
    Scan s = scan.getValue();
    assertEquals(1, s.getFamilyMap().size());
    assertTrue(s.getFamilyMap().containsKey(FAMILY));
    assertNull(s.getFamilyMap().get(FAMILY));
    assertTrue(MobUtils.isRawMobScan(s));
    Cell payload = new KeyValue(DESCRIPTOR.anchor, FAMILY, PAYLOAD_QUALIFIER, new byte[5000]);
    Filter filter = s.getFilter();
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(payload));
    Cell length = filter.transformCell(payload);
    assertEquals(Bytes.SIZEOF_INT, length.getValueLength());
    assertEquals(5000, Bytes.toInt(length.getValueArray(), length.getValueOffset()));
  }

  /**
   * The size of a payload comes from the length of the stored value. The store reads a short value:
   * a MOB reference holds the length of the payload, and a short payload ends with the magic. A
   * long value is a payload, also in the memstore, and the store does not read it.
   */
  @Test
  public void testPayloadSize() throws Exception {
    Table table = mock(Table.class);
    HnswSegmentStore store = new HnswSegmentStore.TableStore(table);
    List<Get> gets = new ArrayList<>();
    byte[][] stored = new byte[1][];
    when(table.get(any(Get.class))).thenAnswer(i -> {
      Get get = i.getArgument(0);
      gets.add(get);
      assertTrue(MobUtils.isRawMobScan(new Scan(get)));
      if (stored[0] == null) {
        return Result.EMPTY_RESULT;
      }
      Cell cell = new KeyValue(DESCRIPTOR.anchor, FAMILY, PAYLOAD_QUALIFIER, stored[0]);
      if (get.getFilter() != null) {
        cell = get.getFilter().transformCell(cell);
      }
      return Result.create(Collections.singletonList(cell));
    });
    assertEquals(-1, store.payloadSize(DESCRIPTOR));

    stored[0] = new byte[2_000_000];
    gets.clear();
    assertEquals(2_000_000, store.payloadSize(DESCRIPTOR));
    assertEquals(1, gets.size());

    stored[0] = Bytes.add(Bytes.toBytes(70_000), Bytes.toBytes("d41d8cd98f00b204_0123456789"));
    gets.clear();
    assertEquals(70_000, store.payloadSize(DESCRIPTOR));
    assertEquals(2, gets.size());
    assertArrayEquals(DESCRIPTOR.anchor, gets.get(1).getRow());

    stored[0] = Bytes.add(new byte[100], Bytes.toBytes(HnswSegment.MAGIC));
    assertEquals(104, store.payloadSize(DESCRIPTOR));
  }

  /**
   * The retirement of malformed cells uses one scan of the cells of the index. It deletes each cell
   * that is not in a well-formed segment. Examples are a segment without a W cell, a payload
   * without small cells, and a cell whose qualifier does not decode. It keeps the well-formed
   * segments of every definition with their payloads, and the cells of other indexes. The deletes
   * skip the WAL.
   */
  @Test
  public void testRetireMalformed() throws Exception {
    SegmentCells cells = new SegmentCells();
    Region region = mock(Region.class);
    when(region.getRegionInfo())
      .thenReturn(RegionInfoBuilder.newBuilder(TableName.valueOf("T")).build());
    cells.stubOnly(region);
    RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    when(env.getRegion()).thenReturn(region);
    List<Durability> durability = new ArrayList<>();
    cells.setHook(m -> durability.add(m.getDurability()));

    cells.put(DESCRIPTOR, Bytes.toBytes("payload"), HnswSegmentTest.WRITER);
    HnswSegment.Descriptor noWriter = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 101L,
      Bytes.toBytes("f"), 1, null, 101L, DESCRIPTOR.definition);
    cells.put(noWriter, Bytes.toBytes("payload"), HnswSegmentTest.WRITER);
    cells.remove(noWriter, HnswSegment.WRITER_TAG);
    HnswSegment.Descriptor payloadOnly = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 102L,
      Bytes.toBytes("f"), 1, null, 102L, DESCRIPTOR.definition);
    cells.put(payloadOnly.anchor,
      HnswSegment.qualifier(payloadOnly.identity, HnswSegment.PAYLOAD_TAG), 102L,
      Bytes.toBytes("payload"));
    cells.put(DESCRIPTOR.anchor, Bytes.add(HnswSegment.prefix(KEY), Bytes.toBytes("junk")), 5L,
      Bytes.toBytes(1));
    HnswSegment.Descriptor other = new HnswSegment.Descriptor(Bytes.toBytes("OTHER"),
      Bytes.toBytes("b"), 103L, Bytes.toBytes("f"), 1, null, 103L, DESCRIPTOR.definition);
    cells.put(other, null, HnswSegmentTest.WRITER);
    // A well-formed segment of another definition of the index stays. A manager that does not yet
    // know a re-created index must not delete the segments of the new definition
    HnswSegment.Descriptor otherDefinition = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"),
      104L, Bytes.toBytes("f"), 1, null, 104L, HnswSegment.definitionDigest("other"));
    cells.put(otherDefinition, Bytes.toBytes("payload"), HnswSegmentTest.WRITER);
    int kept = 2 * (HnswSegment.TAGS.length - 2) + 5;

    assertEquals(HnswSegment.TAGS.length - 3 + 1 + 1,
      new HnswSegmentStore.RegionStore(env).retireMalformed(KEY));
    assertEquals(kept, cells.size());
    List<HnswSegment.Descriptor> listed = cells.list(KEY);
    assertEquals(listed.toString(), 2, listed.size());
    for (HnswSegment.Descriptor d : listed) {
      HnswSegmentTest.assertDescriptor(d.time == DESCRIPTOR.time ? DESCRIPTOR : otherDefinition, d);
    }
    assertEquals(1, cells.list(Bytes.toBytes("OTHER")).size());
    assertEquals(Collections.singletonList(Durability.SKIP_WAL), durability);
    assertEquals(0, new HnswSegmentStore.RegionStore(env).retireMalformed(KEY));
    assertEquals(1, durability.size());
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
