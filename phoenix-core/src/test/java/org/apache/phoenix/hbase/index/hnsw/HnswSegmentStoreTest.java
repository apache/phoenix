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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
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
 * Verifies the shared rules of the segment stores, the listing scan and the chunk reads, with a
 * read-only {@link HnswSegmentStore.TableStore}. The tests also verify the chunked write of a
 * {@link HnswSegmentStore.RegionStore}: the commit Put, the memstore gate, the failure cleanup, and
 * the removal of malformed cells. A base write waits for its final flush, and a delta write does
 * not.
 */
public class HnswSegmentStoreTest {
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] KEY = Bytes.toBytes("HNSW_INDEX");
  // The chunk size of the write tests
  private static final int CHUNK = 16;
  // A payload of three chunks: two full chunks and a tail of eight bytes
  private static final byte[] PAYLOAD = payload(40);
  private static final HnswSegment.Layout LAYOUT = new HnswSegment.Layout(CHUNK, 40, 30, 0);
  private static final HnswSegment.Descriptor DESCRIPTOR =
    new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 100L, Bytes.toBytes("f"), 10, null, 90L,
      HnswSegment.definitionDigest("definition"), LAYOUT);
  // A base with no vectors, which has no chunks
  private static final HnswSegment.Descriptor EMPTY = new HnswSegment.Descriptor(KEY,
    Bytes.toBytes("b"), 100L, Bytes.toBytes("f"), 0, null, 90L, DESCRIPTOR.definition);
  private static final byte[] PAYLOAD_QUALIFIER =
    HnswSegment.qualifier(DESCRIPTOR.identity, 0, HnswSegment.PAYLOAD_TAG);

  // Returns a payload of the given length whose bytes differ from chunk to chunk
  private static byte[] payload(int length) {
    byte[] payload = new byte[length];
    for (int i = 0; i < length; i++) {
      payload[i] = (byte) i;
    }
    return payload;
  }

  /**
   * The listing scans only the cells of the index in {@code V#}, and gets MOB references as stored.
   * It decodes the segments of all rows. A filter on the server removes the payload chunks and the
   * tombstones chunks. Until the {@code V#} flush, these cells hold their full values, which can be
   * large. The filter keeps the base time of a delta and the L cell.
   */
  @Test
  public void testList() throws Exception {
    HnswSegment.Descriptor other = new HnswSegment.Descriptor(KEY, Bytes.toBytes("f"), 200L,
      new byte[0], 3, null, 200L, DESCRIPTOR.definition);
    HnswSegment.Descriptor delta = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L,
      Bytes.toBytes("f"), 2, 100L, 150L, DESCRIPTOR.definition);
    List<Cell> first = new ArrayList<>(HnswSegmentTest.cells(DESCRIPTOR.anchor, DESCRIPTOR, true));
    first.addAll(HnswSegmentTest.cells(delta.anchor, delta, true));
    // Chunk cells whose chunk index holds the byte of a small tag
    for (int chunk : new int[] { 1, HnswSegment.END_KEY_TAG, HnswSegment.LAYOUT_TAG }) {
      first.add(new KeyValue(delta.anchor, FAMILY,
        HnswSegment.qualifier(delta.identity, chunk, HnswSegment.TOMBSTONES_TAG),
        HnswSegment.encodeKeys(new byte[][] { Bytes.toBytes("c") })));
      first.add(new KeyValue(DESCRIPTOR.anchor, FAMILY,
        HnswSegment.qualifier(DESCRIPTOR.identity, chunk, HnswSegment.PAYLOAD_TAG), PAYLOAD));
    }
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
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      HnswSegment.Qualifier q = HnswSegment.parseQualifier(qualifier, 0, qualifier.length);
      assertEquals(Bytes.toStringBinary(qualifier), q.chunk < 0,
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

  // Returns a table store over the cells. It records the gets of the store
  private static HnswSegmentStore tableStore(SegmentCells cells, List<Get> gets) throws Exception {
    return tableStore(cells, gets, HnswSegmentStore.BATCH_BYTES);
  }

  // Returns a table store over the cells that reads groups of up to the given bytes. It records
  // the gets of the store
  private static HnswSegmentStore tableStore(SegmentCells cells, List<Get> gets, int batchBytes)
    throws Exception {
    Table table = mock(Table.class);
    when(table.get(any(Get.class))).thenAnswer(i -> {
      Get get = i.getArgument(0);
      gets.add(get);
      return cells.get(get);
    });
    return new HnswSegmentStore.TableStore(table, batchBytes);
  }

  /**
   * A chunk read gets the chunk cell and the L cell from the anchor row in one get. It asks HBase
   * for an empty value if the MOB file of the reference is missing or corrupt. A payload copy reads
   * all chunks in order. The three chunks fit in one group, so the copy makes one get. The payload
   * size comes from the layout, without a read.
   */
  @Test
  public void testReadChunk() throws Exception {
    SegmentCells cells = new SegmentCells();
    cells.put(DESCRIPTOR, PAYLOAD, HnswSegmentTest.WRITER);
    List<Get> gets = new ArrayList<>();
    HnswSegmentStore store = tableStore(cells, gets);
    assertEquals(40, store.payloadSize(DESCRIPTOR));
    assertEquals(0, store.payloadSize(EMPTY));
    assertTrue(gets.isEmpty());
    Cell cell = store.readChunk(DESCRIPTOR, HnswSegment.PAYLOAD_TAG, 2);
    assertArrayEquals(Arrays.copyOfRange(PAYLOAD, 32, 40), CellUtil.cloneValue(cell));
    Get g = gets.get(0);
    assertArrayEquals(Bytes.toBytes("b\0"), g.getRow());
    assertEquals(2, g.getFamilyMap().get(FAMILY).size());
    assertTrue(g.getFamilyMap().get(FAMILY)
      .contains(HnswSegment.qualifier(DESCRIPTOR.identity, 2, HnswSegment.PAYLOAD_TAG)));
    assertTrue(g.getFamilyMap().get(FAMILY)
      .contains(HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.LAYOUT_TAG)));
    assertTrue(Bytes.toBoolean(g.getAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS)));

    gets.clear();
    ByteArrayOutputStream copy = new ByteArrayOutputStream();
    store.copyPayload(DESCRIPTOR, copy);
    assertArrayEquals(PAYLOAD, copy.toByteArray());
    assertEquals(1, gets.size());
    assertEquals(4, gets.get(0).getFamilyMap().get(FAMILY).size());
    try {
      store.readChunk(DESCRIPTOR, HnswSegment.PAYLOAD_TAG, 3);
      fail("a chunk outside the layout must not be read");
    } catch (IllegalArgumentException expected) {
    }
  }

  /**
   * An absent L cell means that the region retired the segment after the listing. If the L cell is
   * present, a chunk read fails as for a corrupt segment in three cases. The chunk is absent, HBase
   * gives the empty value of a lost MOB file, or the chunk has the wrong length. It also fails if
   * the L cell does not hold the layout of the listing. The open of the segment then fails, so the
   * region rebuilds.
   */
  @Test
  public void testLostPayload() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(Long.MAX_VALUE);
    byte[] layout = HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.LAYOUT_TAG);
    SegmentCells cells = new SegmentCells();
    HnswSegmentStore store = tableStore(cells, new ArrayList<>());
    try {
      HnswSegment.open(store, DESCRIPTOR, allocator, "COSINE");
      fail("a retired segment must not load");
    } catch (HnswSegment.NotFoundException expected) {
    }
    Object[][] cases = { { new byte[0], HnswSegment.CorruptSegmentException.Kind.UNRESOLVED },
      { new byte[CHUNK - 1], HnswSegment.CorruptSegmentException.Kind.SHORT },
      { new byte[CHUNK + 1], HnswSegment.CorruptSegmentException.Kind.SHORT },
      { null, HnswSegment.CorruptSegmentException.Kind.MISSING } };
    for (Object[] c : cases) {
      cells.clear();
      cells.put(DESCRIPTOR, PAYLOAD, HnswSegmentTest.WRITER);
      if (c[0] == null) {
        cells.remove(DESCRIPTOR.anchor, PAYLOAD_QUALIFIER);
      } else {
        cells.put(DESCRIPTOR.anchor, PAYLOAD_QUALIFIER, DESCRIPTOR.time, (byte[]) c[0]);
      }
      assertCorrupt(store, c[1]);
      try {
        HnswSegment.open(store, DESCRIPTOR, allocator, "COSINE");
        fail(c[1] + " must not load");
      } catch (HnswSegment.CorruptSegmentException expected) {
        assertEquals(c[1], expected.getKind());
      }
    }
    cells.clear();
    cells.put(DESCRIPTOR, PAYLOAD, HnswSegmentTest.WRITER);
    cells.put(DESCRIPTOR.anchor, layout, DESCRIPTOR.time,
      new HnswSegment.Layout(CHUNK, 48, 30, 0).encode());
    assertCorrupt(store, HnswSegment.CorruptSegmentException.Kind.INVALID);
    cells.put(DESCRIPTOR.anchor, layout, DESCRIPTOR.time, new byte[3]);
    assertCorrupt(store, HnswSegment.CorruptSegmentException.Kind.INVALID);
    assertEquals(0, allocator.getAllocatedBytes());
  }

  // Asserts that the read of the first payload chunk of DESCRIPTOR fails as corrupt
  private static void assertCorrupt(HnswSegmentStore store, Object kind) throws Exception {
    try {
      store.readChunk(DESCRIPTOR, HnswSegment.PAYLOAD_TAG, 0);
      fail(kind + " must not be read");
    } catch (HnswSegment.NotFoundException e) {
      fail(kind + " is corrupt, not retired");
    } catch (HnswSegment.CorruptSegmentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("Corrupt HNSW segment"));
      assertEquals(kind, e.getKind());
      assertEquals(HnswSegment.PAYLOAD_TAG, e.getTag());
      assertEquals(0, e.getChunk());
    }
  }

  // Returns a base with a payload of the given length, in chunks of CHUNK bytes
  private static HnswSegment.Descriptor base(int payloadLength) {
    return new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 100L, Bytes.toBytes("f"), 10, null,
      90L, DESCRIPTOR.definition,
      new HnswSegment.Layout(CHUNK, payloadLength, payloadLength - 8, 0));
  }

  // Returns the chunk indices of each get, in order. Each get must also read the L cell, and must
  // ask for an empty value for a lost MOB file
  private static List<List<Integer>> groups(List<Get> gets, HnswSegment.Descriptor d) {
    byte[] layout = HnswSegment.qualifier(d.identity, HnswSegment.LAYOUT_TAG);
    List<List<Integer>> groups = new ArrayList<>();
    for (Get get : gets) {
      assertTrue(get.getFamilyMap().get(FAMILY).contains(layout));
      assertTrue(Bytes.toBoolean(get.getAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS)));
      groups.add(chunkIndices(get.getFamilyMap().get(FAMILY)));
    }
    return groups;
  }

  // Returns the sorted chunk indices of the qualifiers
  private static List<Integer> chunkIndices(Iterable<byte[]> qualifiers) {
    List<Integer> chunks = new ArrayList<>();
    for (byte[] qualifier : qualifiers) {
      HnswSegment.Qualifier q = HnswSegment.parseQualifier(qualifier, 0, qualifier.length);
      if (q.chunk >= 0) {
        chunks.add(q.chunk);
      }
    }
    Collections.sort(chunks);
    return chunks;
  }

  private static List<Integer> range(int first, int end) {
    List<Integer> range = new ArrayList<>();
    for (int i = first; i < end; i++) {
      range.add(i);
    }
    return range;
  }

  /**
   * A read gets consecutive chunks in groups. A group holds at most the batch size of chunks, and
   * at least one chunk. Each get also reads the L cell. The read gives the chunks in order, with
   * the bytes of the payload. A range reads only its own chunks, and an empty range reads nothing.
   */
  @Test
  public void testReadChunksInGroups() throws Exception {
    // Seven chunks: six full chunks and a tail of five bytes
    byte[] payload = payload(6 * CHUNK + 5);
    HnswSegment.Descriptor d = base(payload.length);
    SegmentCells cells = new SegmentCells();
    cells.put(d, payload, HnswSegmentTest.WRITER);
    Object[][] cases = { { 1, Arrays.asList(1, 1, 1, 1, 1, 1, 1) },
      { CHUNK, Arrays.asList(1, 1, 1, 1, 1, 1, 1) }, { 2 * CHUNK, Arrays.asList(2, 2, 2, 1) },
      { 3 * CHUNK - 1, Arrays.asList(2, 2, 2, 1) }, { 4 * CHUNK, Arrays.asList(4, 3) },
      { 7 * CHUNK, Arrays.asList(7) }, { HnswSegmentStore.BATCH_BYTES, Arrays.asList(7) } };
    for (Object[] c : cases) {
      List<Get> gets = new ArrayList<>();
      HnswSegmentStore store = tableStore(cells, gets, (Integer) c[0]);
      ByteArrayOutputStream copy = new ByteArrayOutputStream();
      store.copyPayload(d, copy);
      assertArrayEquals(payload, copy.toByteArray());
      List<Integer> sizes = new ArrayList<>();
      int next = 0;
      for (List<Integer> group : groups(gets, d)) {
        assertEquals(range(next, next + group.size()), group);
        next += group.size();
        sizes.add(group.size());
      }
      assertEquals("batch of " + c[0] + " bytes", c[1], sizes);
    }

    List<Get> gets = new ArrayList<>();
    HnswSegmentStore store = tableStore(cells, gets, 2 * CHUNK);
    List<Integer> seen = new ArrayList<>();
    store.readChunks(d, HnswSegment.PAYLOAD_TAG, 3, 7, (i, cell) -> {
      seen.add(i);
      assertArrayEquals(
        Arrays.copyOfRange(payload, i * CHUNK, Math.min(payload.length, (i + 1) * CHUNK)),
        CellUtil.cloneValue(cell));
    });
    assertEquals(range(3, 7), seen);
    assertEquals(Arrays.asList(range(3, 5), range(5, 7)), groups(gets, d));
    gets.clear();
    store.readChunks(d, HnswSegment.PAYLOAD_TAG, 7, 7, (i, cell) -> fail("no chunk to read"));
    assertTrue(gets.isEmpty());
    for (int[] bad : new int[][] { { -1, 1 }, { 2, 1 }, { 0, 8 } }) {
      try {
        store.readChunks(d, HnswSegment.PAYLOAD_TAG, bad[0], bad[1], (i, cell) -> {
        });
        fail("chunks [" + bad[0] + ", " + bad[1] + ") are not in the stream");
      } catch (IllegalArgumentException expected) {
      }
    }
    assertTrue(gets.isEmpty());
  }

  /**
   * A grouped read checks each chunk of a later group as a single read does. An absent, unresolved
   * or short chunk fails with its own index, after the read gives the chunks before it. A changed L
   * cell fails with the first chunk of its group, and an absent L cell means a retirement.
   */
  @Test
  public void testReadChunksFailsInLaterGroup() throws Exception {
    byte[] payload = payload(6 * CHUNK + 5);
    HnswSegment.Descriptor d = base(payload.length);
    byte[] fifth = HnswSegment.qualifier(d.identity, 5, HnswSegment.PAYLOAD_TAG);
    byte[] layout = HnswSegment.qualifier(d.identity, HnswSegment.LAYOUT_TAG);
    Object[][] cases = { { null, HnswSegment.CorruptSegmentException.Kind.MISSING },
      { new byte[0], HnswSegment.CorruptSegmentException.Kind.UNRESOLVED },
      { new byte[CHUNK - 1], HnswSegment.CorruptSegmentException.Kind.SHORT } };
    for (Object[] c : cases) {
      SegmentCells cells = new SegmentCells();
      cells.put(d, payload, HnswSegmentTest.WRITER);
      if (c[0] == null) {
        cells.remove(d.anchor, fifth);
      } else {
        cells.put(d.anchor, fifth, d.time, (byte[]) c[0]);
      }
      List<Integer> seen = new ArrayList<>();
      try {
        tableStore(cells, new ArrayList<>(), 2 * CHUNK).readChunks(d, HnswSegment.PAYLOAD_TAG, 0, 7,
          (i, cell) -> seen.add(i));
        fail(c[1] + " must not be read");
      } catch (HnswSegment.CorruptSegmentException e) {
        assertEquals(c[1], e.getKind());
        assertEquals(HnswSegment.PAYLOAD_TAG, e.getTag());
        assertEquals(5, e.getChunk());
      }
      assertEquals(range(0, 5), seen);
    }

    // The L cell changes, or the region retires the segment, after the first group
    for (boolean retire : new boolean[] { false, true }) {
      SegmentCells cells = new SegmentCells();
      cells.put(d, payload, HnswSegmentTest.WRITER);
      List<Get> gets = new ArrayList<>();
      HnswSegmentStore store = tableStore(cells, gets, 2 * CHUNK);
      List<Integer> seen = new ArrayList<>();
      try {
        store.readChunks(d, HnswSegment.PAYLOAD_TAG, 0, 7, (i, cell) -> {
          seen.add(i);
          if (i == 1) {
            if (retire) {
              cells.remove(d.anchor, layout);
            } else {
              cells.put(d.anchor, layout, d.time,
                new HnswSegment.Layout(CHUNK, payload.length, 0, 0).encode());
            }
          }
        });
        fail("a changed L cell must stop the read");
      } catch (HnswSegment.NotFoundException e) {
        assertTrue(retire);
      } catch (HnswSegment.CorruptSegmentException e) {
        assertFalse(retire);
        assertEquals(HnswSegment.CorruptSegmentException.Kind.INVALID, e.getKind());
        assertEquals(2, e.getChunk());
      }
      assertEquals(range(0, 2), seen);
      assertEquals(2, gets.size());
    }
  }

  /**
   * A write puts the chunks except the last of each stream in Puts of consecutive chunks. A Put
   * holds at most the batch size of chunks, and at least one chunk. Each Put skips the WAL, and
   * each chunk is its own array. The commit Put comes last and holds the last chunk. A payload of
   * one chunk is one Put. The written payload reads back with the same bytes.
   */
  @Test
  public void testWriteBatchesChunks() throws Exception {
    // The payload length, the batch size or null for the default, and the chunks of each Put. The
    // commit Put is last
    Object[][] cases = {
      { 6 * CHUNK + 5, 2 * CHUNK,
        Arrays.asList(range(0, 2), range(2, 4), range(4, 6), range(6, 7)) },
      { 6 * CHUNK + 5, 3 * CHUNK - 1,
        Arrays.asList(range(0, 2), range(2, 4), range(4, 6), range(6, 7)) },
      { 6 * CHUNK + 5, 3 * CHUNK, Arrays.asList(range(0, 3), range(3, 6), range(6, 7)) },
      { 6 * CHUNK + 5, 4 * CHUNK, Arrays.asList(range(0, 4), range(4, 6), range(6, 7)) },
      { 7 * CHUNK, 3 * CHUNK, Arrays.asList(range(0, 3), range(3, 6), range(6, 7)) },
      { 6 * CHUNK + 5, 1,
        Arrays.asList(range(0, 1), range(1, 2), range(2, 3), range(3, 4), range(4, 5), range(5, 6),
          range(6, 7)) },
      { 6 * CHUNK + 5, null, Arrays.asList(range(0, 6), range(6, 7)) },
      { CHUNK - 3, 2 * CHUNK, Arrays.asList(range(0, 1)) } };
    for (Object[] c : cases) {
      byte[] payload = payload((Integer) c[0]);
      HnswSegment.Descriptor d = base(payload.length);
      SegmentCells cells = new SegmentCells();
      HnswSegmentStore.RegionStore store = regionStore(cells, (Integer) c[1]);
      byte[] layout = HnswSegment.qualifier(d.identity, HnswSegment.LAYOUT_TAG);
      List<List<Integer>> puts = new ArrayList<>();
      List<Boolean> commits = new ArrayList<>();
      List<byte[]> values = new ArrayList<>();
      cells.setHook(m -> {
        assertEquals(Durability.SKIP_WAL, m.getDurability());
        assertTrue(cells.list(KEY).isEmpty());
        List<byte[]> qualifiers = new ArrayList<>();
        for (Cell cell : m.getFamilyCellMap().get(FAMILY)) {
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          qualifiers.add(qualifier);
          if (HnswSegment.parseQualifier(qualifier, 0, qualifier.length).chunk >= 0) {
            assertEquals(cell.getValueLength(), cell.getValueArray().length);
            values.add(cell.getValueArray());
          }
        }
        puts.add(chunkIndices(qualifiers));
        commits.add(m.has(FAMILY, layout));
      });
      store.write(d, new ByteArrayInputStream(payload), null, null);
      String name = c[0] + " bytes in batches of " + c[1];
      assertEquals(name, c[2], puts);
      for (int i = 0; i < commits.size(); i++) {
        assertEquals(name, i == commits.size() - 1, commits.get(i));
      }
      assertEquals(d.layout.chunks(HnswSegment.PAYLOAD_TAG), values.size());
      for (int i = 0; i < values.size(); i++) {
        for (int j = 0; j < i; j++) {
          assertFalse(values.get(i) == values.get(j));
        }
      }
      cells.clearHook();
      ByteArrayOutputStream copy = new ByteArrayOutputStream();
      store.copyPayload(d, copy);
      assertArrayEquals(name, payload, copy.toByteArray());
    }
  }

  /**
   * The memstore gate admits each Put with the bytes of all of its chunks. A Put of two chunks fits
   * in the window only if both chunks fit. An abort stops the write before its next Put of chunks.
   * A busy region refuses a Put of chunks, and the write repeats the same Put.
   */
  @Test(timeout = 30_000)
  @SuppressWarnings("unchecked")
  public void testWriteGatesEachBatch() throws Exception {
    long window = 64L * 1024 * 1024;
    // Five chunks: the first two Puts each hold two chunks, and the commit holds the tail
    assertEquals(Arrays.asList(0, 1, 2), gatedRequests(window - 2 * CHUNK, window));
    assertEquals(Arrays.asList(1, 2, 3), gatedRequests(window - 2 * CHUNK + 1, window));

    MockRegion r = new MockRegion();
    r.flushed.set(true);
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        ((FlushLifeCycleTracker) i.getArgument(2)).afterExecution();
        return true;
      });
    List<Mutation> applied = new ArrayList<>();
    doAnswer(i -> {
      List<Mutation> m = i.getArgument(0);
      applied.add(m.get(0));
      if (applied.size() == 2) {
        throw new RegionTooBusyException("busy");
      }
      return null;
    }).when(r.region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(), anyLong());
    HnswSegment.Descriptor d = base(4 * CHUNK + 8);
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(r.env, CHUNK, 2 * CHUNK);
    store.write(d, new ByteArrayInputStream(payload(4 * CHUNK + 8)), null, null);
    assertEquals(4, applied.size());
    assertTrue(applied.get(1) == applied.get(2));
    assertEquals(range(2, 4), chunkIndices(qualifiers(applied.get(2))));

    applied.clear();
    try {
      store.write(d, new ByteArrayInputStream(payload(4 * CHUNK + 8)), null,
        () -> applied.size() == 1);
      fail("the write must stop");
    } catch (HnswSegmentStore.AbortedWriteException expected) {
    }
    assertEquals(1, applied.size());
    assertEquals(range(0, 2), chunkIndices(qualifiers(applied.get(0))));
  }

  // Returns the qualifiers of the V# cells of a mutation
  private static List<byte[]> qualifiers(Mutation m) {
    List<byte[]> qualifiers = new ArrayList<>();
    for (Cell cell : m.getFamilyCellMap().get(FAMILY)) {
      qualifiers.add(CellUtil.cloneQualifier(cell));
    }
    return qualifiers;
  }

  // Writes a delta of five chunks in Puts of two chunks, with the memstore at the given size. Each
  // flush writes the cells that the memstore held, and other writers fill the window again. Each
  // Put adds two chunks to the memstore. Returns the flush requests before each Put
  private static List<Integer> gatedRequests(long memstore, long window) throws Exception {
    MockRegion r = new MockRegion();
    r.memstore.set(memstore);
    List<Integer> requestsAtPut = new ArrayList<>();
    doAnswer(i -> {
      requestsAtPut.add(r.requests.get());
      r.memstore.addAndGet(2 * CHUNK);
      return null;
    }).when(r.region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(), anyLong());
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        r.requests.incrementAndGet();
        FlushLifeCycleTracker tracker = i.getArgument(2);
        r.flushedCells.addAndGet(r.memstore.getAndSet(window));
        tracker.afterExecution();
        return true;
      });
    HnswSegment.Descriptor delta =
      new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L, Bytes.toBytes("f"), 1, 100L, 150L,
        DESCRIPTOR.definition, new HnswSegment.Layout(CHUNK, 4 * CHUNK + 8, 4 * CHUNK, 0));
    new HnswSegmentStore.RegionStore(r.env, CHUNK, 2 * CHUNK).write(delta,
      new ByteArrayInputStream(payload(4 * CHUNK + 8)), null, null);
    return requestsAtPut.subList(0, 3);
  }

  /**
   * The write of a delta adds its base time and its tombstones to the cells of a segment. The write
   * of a base adds neither. All cells have the segment time as their timestamp, and all Puts skip
   * the WAL. The commit Put holds the small cells and the last chunk of each stream.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testWriteDelta() throws Exception {
    MockRegion r = new MockRegion();
    r.flushed.set(true);
    ArgumentCaptor<List<Mutation>> mutations = ArgumentCaptor.forClass(List.class);
    byte[][] tombstones = { Bytes.toBytes("c"), Bytes.toBytes("d") };
    HnswSegment.Descriptor delta = delta(tombstones);
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(r.env, CHUNK, CHUNK);
    store.write(delta, new ByteArrayInputStream(PAYLOAD), new HnswSegment.KeyStream(tombstones),
      null);
    store.write(DESCRIPTOR, new ByteArrayInputStream(PAYLOAD), null, null);
    verify(r.region, times(6)).mutateRowsWithLocks(mutations.capture(), anyCollection(), anyLong(),
      anyLong());
    Put deltaPut = (Put) mutations.getAllValues().get(2).get(0);
    Put basePut = (Put) mutations.getAllValues().get(5).get(0);
    for (List<Mutation> m : mutations.getAllValues()) {
      assertEquals(1, m.size());
      assertEquals(Durability.SKIP_WAL, m.get(0).getDurability());
    }
    // The small cells, the payload tail and the tombstones tail
    assertEquals(HnswSegment.SMALL_TAGS.length + 2, deltaPut.getFamilyCellMap().get(FAMILY).size());
    assertEquals(HnswSegment.SMALL_TAGS.length, basePut.getFamilyCellMap().get(FAMILY).size());
    for (Cell cell : deltaPut.getFamilyCellMap().get(FAMILY)) {
      assertEquals(delta.time, cell.getTimestamp());
    }
    assertEquals(100L, Bytes.toLong(CellUtil.cloneValue(deltaPut
      .get(FAMILY, HnswSegment.qualifier(delta.identity, HnswSegment.BASE_TIME_TAG)).get(0))));
    assertArrayEquals(HnswSegment.encodeKeys(tombstones), CellUtil.cloneValue(deltaPut
      .get(FAMILY, HnswSegment.qualifier(delta.identity, 0, HnswSegment.TOMBSTONES_TAG)).get(0)));
    assertArrayEquals(delta.layout.encode(), CellUtil.cloneValue(
      deltaPut.get(FAMILY, HnswSegment.qualifier(delta.identity, HnswSegment.LAYOUT_TAG)).get(0)));
    assertTrue(
      basePut.get(FAMILY, HnswSegment.qualifier(DESCRIPTOR.identity, HnswSegment.BASE_TIME_TAG))
        .isEmpty());
    assertTrue(
      basePut.get(FAMILY, HnswSegment.qualifier(DESCRIPTOR.identity, 0, HnswSegment.TOMBSTONES_TAG))
        .isEmpty());
  }

  // Returns a delta with the payload and the given tombstones, in chunks of CHUNK bytes
  private static HnswSegment.Descriptor delta(byte[][] tombstones) {
    return new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L, Bytes.toBytes("f"), 1, 100L,
      150L, DESCRIPTOR.definition,
      new HnswSegment.Layout(CHUNK, 40, 30, HnswSegment.KeyStream.length(tombstones)));
  }

  // Returns a delta with the payload of DESCRIPTOR and no tombstones
  private static HnswSegment.Descriptor plainDelta() {
    return new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L, Bytes.toBytes("f"), 1, 100L,
      150L, DESCRIPTOR.definition, LAYOUT);
  }

  // Returns a store over the cells, in a region that holds no data rows. It writes one chunk in
  // each Put
  private static HnswSegmentStore.RegionStore regionStore(SegmentCells cells) throws Exception {
    return regionStore(cells, CHUNK);
  }

  // Returns a store over the cells, in a region that holds no data rows. It groups chunks up to
  // the given bytes, or up to the default batch size for null
  private static HnswSegmentStore.RegionStore regionStore(SegmentCells cells, Integer batchBytes)
    throws Exception {
    Region region = mock(Region.class);
    when(region.getRegionInfo())
      .thenReturn(RegionInfoBuilder.newBuilder(TableName.valueOf("T")).build());
    cells.stubOnly(region);
    RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    when(env.getRegion()).thenReturn(region);
    when(env.getConfiguration()).thenReturn(HBaseConfiguration.create());
    return batchBytes == null
      ? new HnswSegmentStore.RegionStore(env, CHUNK)
      : new HnswSegmentStore.RegionStore(env, CHUNK, batchBytes);
  }

  /**
   * A write puts each chunk except the last of each stream before the commit Put. A listing finds
   * the segment only after the commit Put. Each chunk is its own array of the chunk length. The
   * written segment reads back with the same payload and tombstones.
   */
  @Test
  public void testWriteStreamsChunksBeforeCommit() throws Exception {
    SegmentCells cells = new SegmentCells();
    HnswSegmentStore.RegionStore store = regionStore(cells);
    List<Integer> listed = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();
    cells.setHook(m -> {
      listed.add(cells.list(KEY).size());
      for (Cell cell : m.getFamilyCellMap().get(FAMILY)) {
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        if (HnswSegment.parseQualifier(qualifier, 0, qualifier.length).chunk >= 0) {
          assertEquals(0, cell.getValueOffset());
          assertEquals(cell.getValueLength(), cell.getValueArray().length);
          values.add(cell.getValueArray());
        }
      }
    });
    byte[][] tombstones = new byte[10][];
    for (int i = 0; i < tombstones.length; i++) {
      tombstones[i] = Bytes.toBytes("row" + i);
    }
    HnswSegment.Descriptor delta = delta(tombstones);
    assertEquals(6, delta.layout.chunks(HnswSegment.TOMBSTONES_TAG));
    store.write(delta, new ByteArrayInputStream(PAYLOAD), new HnswSegment.KeyStream(tombstones),
      null);
    // Two payload chunks and five tombstones chunks, then the commit Put
    assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0), listed);
    assertEquals(9, values.size());
    for (int i = 0; i < values.size(); i++) {
      for (int j = 0; j < i; j++) {
        assertFalse(values.get(i) == values.get(j));
      }
    }
    List<HnswSegment.Descriptor> segments = store.list(KEY);
    assertEquals(1, segments.size());
    assertEquals(delta.layout, segments.get(0).layout);
    ByteArrayOutputStream copy = new ByteArrayOutputStream();
    store.copyPayload(segments.get(0), copy);
    assertArrayEquals(PAYLOAD, copy.toByteArray());
    HnswSegment.KeyDecoder decoder = new HnswSegment.KeyDecoder(delta.layout.tombstonesLength,
      delta.identity, HnswSegment.TOMBSTONES_TAG);
    for (int i = 0; i < delta.layout.chunks(HnswSegment.TOMBSTONES_TAG); i++) {
      Cell cell = store.readChunk(segments.get(0), HnswSegment.TOMBSTONES_TAG, i);
      decoder.feed(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }
    byte[][] decoded = decoder.finish();
    assertEquals(tombstones.length, decoded.length);
    for (int i = 0; i < tombstones.length; i++) {
      assertArrayEquals(tombstones[i], decoded[i]);
    }
  }

  /**
   * A write fails if a stream does not have the length of the layout. A busy region refuses a Put,
   * and the write repeats the same Put after a flush request. A base write that fails before its
   * commit deletes its chunks with a retirement. A failed delta keeps its cells. A write in a
   * region that closes leaves its chunks to the removal of malformed cells.
   */
  @Test
  public void testWriteFailureRetires() throws Exception {
    MockRegion r = new MockRegion();
    r.flushed.set(true);
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        r.requests.incrementAndGet();
        ((FlushLifeCycleTracker) i.getArgument(2)).afterExecution();
        return true;
      });
    List<Mutation> applied = new ArrayList<>();
    AtomicInteger calls = new AtomicInteger();
    doAnswer(i -> {
      List<Mutation> m = i.getArgument(0);
      applied.add(m.get(0));
      int call = calls.incrementAndGet();
      if (call == 1) {
        throw new RegionTooBusyException("busy");
      }
      if (call == 4) {
        throw new IOException("the commit fails");
      }
      return null;
    }).when(r.region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(), anyLong());
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(r.env, CHUNK, CHUNK);
    try {
      store.write(DESCRIPTOR, new ByteArrayInputStream(PAYLOAD), null, null);
      fail("the write must fail");
    } catch (IOException e) {
      assertEquals("the commit fails", e.getMessage());
    }
    assertEquals(1, r.requests.get());
    assertEquals(5, applied.size());
    assertTrue(applied.get(0) == applied.get(1));
    assertTrue(applied.get(4) instanceof Delete);
    List<String> deleted = new ArrayList<>();
    for (Cell marker : applied.get(4).getFamilyCellMap().get(FAMILY)) {
      deleted.add(Bytes.toStringBinary(CellUtil.cloneQualifier(marker)));
    }
    for (Mutation m : applied.subList(1, 4)) {
      for (Cell cell : m.getFamilyCellMap().get(FAMILY)) {
        assertTrue(deleted.contains(Bytes.toStringBinary(CellUtil.cloneQualifier(cell))));
      }
    }

    // A short stream fails before the commit
    applied.clear();
    calls.set(10);
    try {
      store.write(DESCRIPTOR, new ByteArrayInputStream(PAYLOAD, 0, 39), null, null);
      fail("a short stream must fail");
    } catch (IllegalStateException expected) {
    }
    assertEquals(3, applied.size());
    assertTrue(applied.get(2) instanceof Delete);

    // A failed delta keeps its cells. Its retry has the same identity and time, and a delete marker
    // at that time would hide the cells of the retry
    applied.clear();
    calls.set(1);
    try {
      store.write(plainDelta(), new ByteArrayInputStream(PAYLOAD), null, null);
      fail("the write must fail");
    } catch (IOException e) {
      assertEquals("the commit fails", e.getMessage());
    }
    assertEquals(3, applied.size());
    for (Mutation m : applied) {
      assertTrue(m instanceof Put);
    }

    // A region that closes keeps the chunks
    applied.clear();
    calls.set(2);
    when(r.region.isClosing()).thenReturn(true);
    try {
      store.write(DESCRIPTOR, new ByteArrayInputStream(PAYLOAD), null, null);
      fail("the write must fail");
    } catch (IOException expected) {
    }
    assertEquals(2, applied.size());
    assertTrue(applied.get(1) instanceof Put);
  }

  /**
   * An abort stops a write before its next Put, and the write does not delete its chunks. The
   * caller retires the segment after it closes.
   */
  @Test
  public void testWriteAbort() throws Exception {
    MockRegion r = new MockRegion();
    r.flushed.set(true);
    AtomicInteger puts = new AtomicInteger();
    doAnswer(i -> {
      puts.incrementAndGet();
      return null;
    }).when(r.region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(), anyLong());
    try {
      new HnswSegmentStore.RegionStore(r.env, CHUNK, CHUNK).write(DESCRIPTOR,
        new ByteArrayInputStream(PAYLOAD), null, () -> puts.get() == 1);
      fail("the write must stop");
    } catch (HnswSegmentStore.AbortedWriteException expected) {
    }
    assertEquals(1, puts.get());
  }

  /**
   * A write whose abort gives true while the region closes stops as an abort. This holds when the
   * region closes during a memstore wait, and when a Put fails because the region closes. The write
   * deletes no cells.
   */
  @Test(timeout = 30_000)
  @SuppressWarnings("unchecked")
  public void testWriteAbortWhenRegionCloses() throws Exception {
    MockRegion r = new MockRegion();
    r.memstore.set(64L * 1024 * 1024);
    AtomicBoolean closed = new AtomicBoolean();
    when(r.region.isClosing()).thenAnswer(i -> closed.get());
    List<Mutation> applied = new ArrayList<>();
    doAnswer(i -> {
      applied.add(((List<Mutation>) i.getArgument(0)).get(0));
      return null;
    }).when(r.region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(), anyLong());
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        closed.set(true);
        ((FlushLifeCycleTracker) i.getArgument(2)).notExecuted("the region closes");
        return true;
      });
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(r.env, CHUNK, CHUNK);
    try {
      store.write(DESCRIPTOR, new ByteArrayInputStream(PAYLOAD), null, r.region::isClosing);
      fail("the write must stop");
    } catch (HnswSegmentStore.AbortedWriteException expected) {
    }
    assertTrue(applied.isEmpty());

    // The region closes after the memstore check, so the Put fails
    MockRegion closing = new MockRegion();
    closing.memstore.set(0);
    AtomicBoolean closingNow = new AtomicBoolean();
    when(closing.region.isClosing()).thenAnswer(i -> closingNow.get());
    AtomicInteger puts = new AtomicInteger();
    doAnswer(i -> {
      puts.incrementAndGet();
      closingNow.set(true);
      throw new NotServingRegionException("the region is closing");
    }).when(closing.region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(),
      anyLong());
    try {
      new HnswSegmentStore.RegionStore(closing.env, CHUNK, CHUNK).write(DESCRIPTOR,
        new ByteArrayInputStream(PAYLOAD), null, closing.region::isClosing);
      fail("the write must stop");
    } catch (HnswSegmentStore.AbortedWriteException e) {
      assertTrue(e.getCause() instanceof NotServingRegionException);
    }
    assertEquals(1, puts.get());
  }

  /**
   * Before a Put that would pass the memstore window, a write asks for a flush of {@code V#}. It
   * waits until the store flushed the cells that the memstore held, and then makes the Put. A Put
   * that fits in the window does not wait. Other writers fill the memstore again during each flush,
   * so the memstore is never empty and never has space. The write does not wait for these cells. A
   * delta obeys the window as a base does.
   */
  @Test(timeout = 30_000)
  @SuppressWarnings("unchecked")
  public void testWriteGateFlushes() throws Exception {
    MockRegion r = new MockRegion();
    long window = 64L * 1024 * 1024;
    r.memstore.set(window - CHUNK);
    List<Integer> requestsAtPut = new ArrayList<>();
    doAnswer(i -> {
      requestsAtPut.add(r.requests.get());
      r.memstore.addAndGet(CHUNK);
      return null;
    }).when(r.region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(), anyLong());
    when(r.requester.requestFlush(eq(r.region), anyList(), any(FlushLifeCycleTracker.class)))
      .thenAnswer(i -> {
        r.requests.incrementAndGet();
        FlushLifeCycleTracker tracker = i.getArgument(2);
        new Thread(() -> {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          // The flush writes the cells that the memstore held. Other writers fill the window
          // again, so the flushable size of V# stays above zero
          r.flushedCells.addAndGet(r.memstore.getAndSet(window));
          tracker.afterExecution();
        }).start();
        return true;
      });
    new HnswSegmentStore.RegionStore(r.env, CHUNK, CHUNK).write(plainDelta(),
      new ByteArrayInputStream(PAYLOAD), null, null);
    // The first chunk fits. The second chunk and the commit each wait for one flush
    assertEquals(Arrays.asList(0, 1, 2), requestsAtPut);
    assertTrue(r.flushedCells.get() >= 2 * window);
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
    new HnswSegmentStore.RegionStore(r.env).write(EMPTY, null, null, null);
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
    new HnswSegmentStore.RegionStore(r.env).write(EMPTY, null, null, null);
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
    new HnswSegmentStore.RegionStore(r.env).write(EMPTY, null, null, null);
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
    new HnswSegmentStore.RegionStore(r.env).write(EMPTY, null, null, null);
    assertEquals(2, r.requests.get());
    assertFalse(r.flushed.get());
  }

  /**
   * A write records the encoded name of its region in the W cell, at the segment time. A listing of
   * the cells of the commit Put returns this name as the writer of the segment. A retirement
   * deletes all cells of the write, including the W cell and all chunks.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testWriteRecordsWriter() throws Exception {
    MockRegion r = new MockRegion();
    r.flushed.set(true);
    ArgumentCaptor<List<Mutation>> mutations = ArgumentCaptor.forClass(List.class);
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(r.env, CHUNK, CHUNK);
    store.write(DESCRIPTOR, new ByteArrayInputStream(PAYLOAD), null, null);
    store.retire(Collections.singletonList(DESCRIPTOR));
    verify(r.region, times(4)).mutateRowsWithLocks(mutations.capture(), anyCollection(), anyLong(),
      anyLong());
    Put put = (Put) mutations.getAllValues().get(2).get(0);
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

    Delete delete = (Delete) mutations.getAllValues().get(3).get(0);
    List<String> deleted = new ArrayList<>();
    for (Cell marker : delete.getFamilyCellMap().get(FAMILY)) {
      assertEquals(Cell.Type.DeleteColumn, marker.getType());
      assertEquals(DESCRIPTOR.time, marker.getTimestamp());
      deleted.add(Bytes.toStringBinary(CellUtil.cloneQualifier(marker)));
    }
    for (List<Mutation> m : mutations.getAllValues().subList(0, 3)) {
      for (Cell cell : m.get(0).getFamilyCellMap().get(FAMILY)) {
        assertTrue(deleted.contains(Bytes.toStringBinary(CellUtil.cloneQualifier(cell))));
      }
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
    byte[][] tombstones = { Bytes.toBytes("c") };
    new HnswSegmentStore.RegionStore(r.env, CHUNK, CHUNK).write(delta(tombstones),
      new ByteArrayInputStream(PAYLOAD), new HnswSegment.KeyStream(tombstones), null);
    verify(r.region, times(3)).mutateRowsWithLocks(anyList(), anyCollection(), anyLong(),
      anyLong());
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
   * The retirement of malformed cells uses one scan of the cells of the index. It deletes each cell
   * that is not in a well-formed segment. Examples are a segment without a W cell, a payload chunk
   * without small cells, and a cell whose qualifier does not decode. It also deletes a chunk of a
   * well-formed segment above the chunk count of its layout. It keeps the well-formed segments of
   * every definition with their chunks, and the cells of other indexes. The deletes skip the WAL.
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

    // Six small cells and three chunks
    cells.put(DESCRIPTOR, PAYLOAD, HnswSegmentTest.WRITER);
    // A chunk that a failed write with the same identity left
    cells.put(DESCRIPTOR.anchor,
      HnswSegment.qualifier(DESCRIPTOR.identity, 3, HnswSegment.PAYLOAD_TAG), DESCRIPTOR.time,
      PAYLOAD);
    HnswSegment.Descriptor noWriter = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 101L,
      Bytes.toBytes("f"), 1, null, 101L, DESCRIPTOR.definition, LAYOUT);
    cells.put(noWriter, PAYLOAD, HnswSegmentTest.WRITER);
    cells.remove(noWriter, HnswSegment.WRITER_TAG);
    HnswSegment.Descriptor payloadOnly = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 102L,
      Bytes.toBytes("f"), 1, null, 102L, DESCRIPTOR.definition, LAYOUT);
    cells.put(payloadOnly.anchor,
      HnswSegment.qualifier(payloadOnly.identity, 0, HnswSegment.PAYLOAD_TAG), 102L, PAYLOAD);
    cells.put(DESCRIPTOR.anchor, Bytes.add(HnswSegment.prefix(KEY), Bytes.toBytes("junk")), 5L,
      Bytes.toBytes(1));
    HnswSegment.Descriptor other = new HnswSegment.Descriptor(Bytes.toBytes("OTHER"),
      Bytes.toBytes("b"), 103L, Bytes.toBytes("f"), 1, null, 103L, DESCRIPTOR.definition, LAYOUT);
    cells.put(other, PAYLOAD, HnswSegmentTest.WRITER);
    // A well-formed segment of another definition of the index stays. A manager that does not yet
    // know a re-created index must not delete the segments of the new definition
    HnswSegment.Descriptor otherDefinition = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"),
      104L, Bytes.toBytes("f"), 1, null, 104L, HnswSegment.definitionDigest("other"), LAYOUT);
    cells.put(otherDefinition, PAYLOAD, HnswSegmentTest.WRITER);
    int kept = 3 * 9;

    // The extra chunk, the cells of the segment without a W cell, the payload chunk, and the junk
    assertEquals(1 + 8 + 1 + 1,
      new HnswSegmentStore.RegionStore(env).retireMalformed(KEY, otherDefinition.time));
    assertEquals(kept, cells.size());
    List<HnswSegment.Descriptor> listed = cells.list(KEY);
    assertEquals(listed.toString(), 2, listed.size());
    for (HnswSegment.Descriptor d : listed) {
      HnswSegmentTest.assertDescriptor(d.time == DESCRIPTOR.time ? DESCRIPTOR : otherDefinition, d);
    }
    assertEquals(3, cells.chunks(DESCRIPTOR, HnswSegment.PAYLOAD_TAG).size());
    assertEquals(1, cells.list(Bytes.toBytes("OTHER")).size());
    assertEquals(Collections.singletonList(Durability.SKIP_WAL), durability);
    assertEquals(0,
      new HnswSegmentStore.RegionStore(env).retireMalformed(KEY, otherDefinition.time));
    assertEquals(1, durability.size());
  }

  /**
   * A write streams its chunks before its commit Put, so a scan can see the chunks of a write in
   * progress without small cells. The retirement of malformed cells keeps the cells of a segment
   * that is not well-formed if its time is after the given time. Thus it does not delete the chunks
   * of a write in progress, which has a later time than the newest base.
   */
  @Test
  public void testRetireMalformedKeepsNewerChunks() throws Exception {
    SegmentCells cells = new SegmentCells();
    Region region = mock(Region.class);
    when(region.getRegionInfo())
      .thenReturn(RegionInfoBuilder.newBuilder(TableName.valueOf("T")).build());
    cells.stubOnly(region);
    RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    when(env.getRegion()).thenReturn(region);
    cells.put(DESCRIPTOR, PAYLOAD, HnswSegmentTest.WRITER);
    HnswSegment.Descriptor failed = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 150L,
      Bytes.toBytes("f"), 1, null, 150L, DESCRIPTOR.definition, LAYOUT);
    cells.putChunks(failed.anchor, failed.identity, HnswSegment.PAYLOAD_TAG, failed.time,
      Arrays.copyOf(PAYLOAD, CHUNK), CHUNK);
    HnswSegment.Descriptor writing = new HnswSegment.Descriptor(KEY, Bytes.toBytes("b"), 200L,
      Bytes.toBytes("f"), 1, null, 200L, DESCRIPTOR.definition, LAYOUT);
    cells.putChunks(writing.anchor, writing.identity, HnswSegment.PAYLOAD_TAG, writing.time,
      Arrays.copyOf(PAYLOAD, 2 * CHUNK), CHUNK);
    HnswSegmentStore.RegionStore store = new HnswSegmentStore.RegionStore(env);

    assertEquals(1, store.retireMalformed(KEY, failed.time));
    assertTrue(cells.chunks(failed, HnswSegment.PAYLOAD_TAG).isEmpty());
    assertEquals(2, cells.chunks(writing, HnswSegment.PAYLOAD_TAG).size());
    assertEquals(3, cells.chunks(DESCRIPTOR, HnswSegment.PAYLOAD_TAG).size());
    assertEquals(2, store.retireMalformed(KEY, writing.time));
    assertTrue(cells.chunks(writing, HnswSegment.PAYLOAD_TAG).isEmpty());
    assertEquals(1, cells.list(KEY).size());
  }

  // A mock region whose V# memstore holds data until the test marks it as flushed. The memstore
  // size of the gate and the flushed cells size are separate counters
  private static final class MockRegion {
    final RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    final HRegion region = mock(HRegion.class);
    final FlushRequester requester = mock(FlushRequester.class);
    final Configuration conf = HBaseConfiguration.create();
    final AtomicBoolean flushed = new AtomicBoolean();
    final AtomicInteger requests = new AtomicInteger();
    final AtomicLong memstore = new AtomicLong(1024);
    final AtomicLong flushedCells = new AtomicLong();

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
      MemStoreSize current = mock(MemStoreSize.class);
      when(current.getDataSize()).thenAnswer(i -> memstore.get());
      when(store.getMemStoreSize()).thenReturn(current);
      when(store.getSnapshotSize()).thenReturn(empty);
      when(store.getFlushedCellsSize()).thenAnswer(i -> flushedCells.get());
    }
  }
}
