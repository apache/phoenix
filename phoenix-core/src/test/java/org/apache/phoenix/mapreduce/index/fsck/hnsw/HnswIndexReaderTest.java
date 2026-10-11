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
package org.apache.phoenix.mapreduce.index.fsck.hnsw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.VectorIndexType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests how {@link HnswIndexReader} classifies the cells of the hidden family {@code V#} of the
 * data table for the checks of an index.
 */
public class HnswIndexReaderTest {
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] KEY = Bytes.toBytes("IDX");
  private static final byte[] DEFINITION = HnswSegment.definitionDigest("definition");
  private static final byte[] WRITER = Bytes.toBytes("0123456789abcdef0123456789abcdef");
  private static final HnswSegment.Descriptor SEGMENT = new HnswSegment.Descriptor(KEY,
    Bytes.toBytes("m"), 100L, HConstants.EMPTY_END_ROW, 10, null, 100L, DEFINITION);

  private HnswIndexContext context;
  private Table dataTable;
  private HnswIndexReader reader;
  // The cells of V# at the time of the listing, and at the time of the scan of all keys
  private List<Cell> listed;
  private List<Cell> current;
  private final List<Scan> scans = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    context = mock(HnswIndexContext.class);
    when(context.getIndexKey()).thenReturn(KEY);
    when(context.getDefinition()).thenReturn(DEFINITION);
    when(context.getDataPhysicalName()).thenReturn(TableName.valueOf("T"));
    PhoenixConnection pconn = mock(PhoenixConnection.class);
    when(pconn.getTableNoCache(anyString())).thenAnswer(i -> {
      throw new TableNotFoundException(i.getArgument(0));
    });
    when(context.getConnection()).thenReturn(pconn);
    dataTable = mock(Table.class);
    when(dataTable.getScanner(any(Scan.class))).thenAnswer(invocation -> {
      Scan scan = invocation.getArgument(0);
      scans.add(scan);
      return scanner(scan, isListing(scan) ? listed : current);
    });
    Connection connection = mock(Connection.class);
    when(connection.getTable(any(TableName.class))).thenReturn(dataTable);
    reader = new HnswIndexReader(context, connection);
  }

  // The listing of an index filters by the key and the tags. The scan of all keys gets the lengths
  // of the values, and the scan of changed rows has no filter
  private static boolean isListing(Scan scan) {
    return scan.getFilter() != null && !(scan.getFilter() instanceof KeyOnlyFilter);
  }

  /** Returns a scanner that applies the filter of the scan to the cells, as a region does. */
  private static ResultScanner scanner(Scan scan, List<Cell> cells) throws Exception {
    List<Cell> sorted = new ArrayList<>(cells);
    sorted.sort(CellComparator.getInstance());
    List<Result> results = new ArrayList<>();
    List<Cell> row = new ArrayList<>();
    Filter filter = scan.getFilter();
    for (Cell cell : sorted) {
      if (!row.isEmpty() && !CellUtil.matchingRows(row.get(0), cell)) {
        results.add(Result.create(row));
        row = new ArrayList<>();
      }
      if (filter != null) {
        filter.reset();
      }
      Filter.ReturnCode rc = filter == null ? Filter.ReturnCode.INCLUDE : filter.filterCell(cell);
      if (rc == Filter.ReturnCode.INCLUDE || rc == Filter.ReturnCode.INCLUDE_AND_NEXT_COL) {
        row.add(filter == null ? cell : filter.transformCell(cell));
      }
    }
    if (!row.isEmpty()) {
      results.add(Result.create(row));
    }
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator()).thenReturn(results.iterator());
    return scanner;
  }

  private static List<Cell> cells(HnswSegment.Descriptor d, byte[] row, boolean writer) {
    List<Cell> cells = new ArrayList<>();
    cells.add(cell(row, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG), d.endKey));
    cells.add(
      cell(row, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG), Bytes.toBytes(d.count)));
    cells.add(cell(row, HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG),
      Bytes.toBytes(d.replayFrom)));
    cells
      .add(cell(row, HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG), d.definition));
    if (writer) {
      cells.add(cell(row, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG), WRITER));
    }
    if (d.isDelta()) {
      cells.add(cell(row, HnswSegment.qualifier(d.identity, HnswSegment.BASE_TIME_TAG),
        Bytes.toBytes(d.baseTime)));
    }
    cells
      .add(cell(row, HnswSegment.qualifier(d.identity, HnswSegment.PAYLOAD_TAG), new byte[2000]));
    return cells;
  }

  private static Cell cell(byte[] row, byte[] qualifier, byte[] value) {
    return new KeyValue(row, FAMILY, qualifier, 1L, value);
  }

  private static Set<String> names(List<byte[][]> cells, int part) {
    Set<String> names = new HashSet<>();
    for (byte[][] cell : cells) {
      names.add(Bytes.toStringBinary(cell[part]));
    }
    return names;
  }

  /**
   * One listing of the index and one later scan of all keys classify the cells. A segment that the
   * server writes between the two scans is not malformed. A segment without a W cell, a payload
   * without small cells, and a qualifier of the index that does not decode are malformed. The
   * reader reports two other groups: cells that name no index key, and cells of an index key that
   * the catalog does not have. Neither scan returns a payload.
   */
  @Test
  public void testSegmentWrittenDuringCheckIsNotMalformed() throws Exception {
    HnswSegment.Descriptor written = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 200L,
      HConstants.EMPTY_END_ROW, 10, null, 200L, DEFINITION);
    HnswSegment.Descriptor noWriter = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 300L,
      HConstants.EMPTY_END_ROW, 10, null, 300L, DEFINITION);
    HnswSegment.Descriptor payloadOnly = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 400L,
      HConstants.EMPTY_END_ROW, 10, null, 400L, DEFINITION);
    byte[] junk = Bytes.add(HnswSegment.prefix(KEY), Bytes.toBytes("junk"));
    byte[] gone = Bytes.add(Bytes.toBytes("GONE"), new byte[] { 0 }, Bytes.toBytes("x"));
    listed = new ArrayList<>(cells(SEGMENT, SEGMENT.anchor, true));
    listed.addAll(cells(noWriter, noWriter.anchor, false));
    listed.add(cell(payloadOnly.anchor,
      HnswSegment.qualifier(payloadOnly.identity, HnswSegment.PAYLOAD_TAG), new byte[2000]));
    listed.add(cell(SEGMENT.anchor, junk, Bytes.toBytes(1)));
    listed.add(cell(Bytes.toBytes("a"), Bytes.toBytes("stray"), Bytes.toBytes(1)));
    listed.add(cell(Bytes.toBytes("a"), gone, Bytes.toBytes(1)));
    current = new ArrayList<>(listed);
    current.addAll(cells(written, written.anchor, true));

    HnswIndexReader.Catalog catalog = reader.readCatalog();
    assertEquals(1, catalog.segments.size());
    assertArrayEquals(SEGMENT.identity, catalog.segments.get(0).identity);
    assertEquals(
      new HashSet<>(Arrays.asList(Bytes.toStringBinary(noWriter.identity),
        Bytes.toStringBinary(payloadOnly.identity), Bytes.toStringBinary(junk))),
      names(catalog.malformed, 1));
    assertEquals(Collections.singleton(Bytes.toStringBinary(SEGMENT.anchor)),
      names(catalog.malformed, 0));
    assertEquals(Collections.singleton("GONE"), catalog.strayKeys.keySet());
    assertArrayEquals(Bytes.toBytes("a"), catalog.strayKeys.get("GONE").get(0));
    assertEquals(1, catalog.keyless.size());
    assertArrayEquals(Bytes.toBytes("stray"), catalog.keyless.get(0)[1]);

    assertEquals(2, scans.size());
    for (Scan scan : scans) {
      assertTrue(MobUtils.isRawMobScan(scan));
      assertEquals(Collections.singleton(new ImmutableBytesPtr(FAMILY)),
        toPtrs(scan.getFamilyMap().keySet()));
    }
  }

  private static Set<ImmutableBytesPtr> toPtrs(Set<byte[]> families) {
    Set<ImmutableBytesPtr> ptrs = new HashSet<>();
    for (byte[] family : families) {
      ptrs.add(new ImmutableBytesPtr(family));
    }
    return ptrs;
  }

  /**
   * A purge request carries an index key as a string, and the region uses the part after the last
   * '#'. A key that is not valid UTF-8, or that holds '#', changes in this conversion, so no purge
   * removes its cells. The reader reports these cells with the cells that name no index key, and
   * repair does not purge them. Two such keys that decode to the same string stay apart.
   */
  @Test
  public void testKeyThatNoPurgeNamesIsKeyless() throws Exception {
    byte[] hash = Bytes.add(Bytes.toBytes("A#B"), new byte[] { 0 }, Bytes.toBytes("x"));
    byte[] invalid = new byte[] { (byte) 0xC3, 0x28, 0, 'x' };
    byte[] other = new byte[] { (byte) 0xFF, 0x28, 0, 'x' };
    listed = new ArrayList<>(cells(SEGMENT, SEGMENT.anchor, true));
    listed.add(cell(Bytes.toBytes("a"), hash, Bytes.toBytes(1)));
    listed.add(cell(Bytes.toBytes("a"), invalid, Bytes.toBytes(1)));
    listed.add(cell(Bytes.toBytes("a"), other, Bytes.toBytes(1)));
    current = listed;
    HnswIndexReader.Catalog catalog = reader.readCatalog();
    assertEquals(1, catalog.segments.size());
    assertTrue(catalog.strayKeys.toString(), catalog.strayKeys.isEmpty());
    assertEquals(new HashSet<>(Arrays.asList(Bytes.toStringBinary(hash),
      Bytes.toStringBinary(invalid), Bytes.toStringBinary(other))), names(catalog.keyless, 1));
  }

  /** A delta is well-formed if its B cell is a long, and malformed otherwise. */
  @Test
  public void testDeltaRows() throws Exception {
    HnswSegment.Descriptor delta = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 150L,
      HConstants.EMPTY_END_ROW, 1, 100L, 150L, DEFINITION);
    HnswSegment.Descriptor bad = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 160L,
      HConstants.EMPTY_END_ROW, 1, 100L, 160L, DEFINITION);
    listed = new ArrayList<>(cells(delta, delta.anchor, true));
    for (Cell cell : cells(bad, bad.anchor, true)) {
      boolean baseTime = Bytes.equals(CellUtil.cloneQualifier(cell),
        HnswSegment.qualifier(bad.identity, HnswSegment.BASE_TIME_TAG));
      listed
        .add(baseTime ? cell(bad.anchor, CellUtil.cloneQualifier(cell), Bytes.toBytes(100)) : cell);
    }
    current = listed;
    HnswIndexReader.Catalog catalog = reader.readCatalog();
    assertEquals(1, catalog.segments.size());
    assertTrue(catalog.segments.get(0).isDelta());
    assertEquals(Collections.singleton(Bytes.toStringBinary(bad.identity)),
      names(catalog.malformed, 1));
  }

  /**
   * A segment that the region retires between the listing and the scan of all keys keeps its
   * classification from the listing. The cells of an HNSW index of the table are not stray.
   */
  @Test
  public void testRetiredSegmentAndLiveIndexKey() throws Exception {
    PhoenixConnection pconn = context.getConnection();
    PTable live = mock(PTable.class);
    when(live.getType()).thenReturn(PTableType.INDEX);
    when(live.getIndexType()).thenReturn(PTable.IndexType.VECTOR_LOCAL);
    when(live.getVectorIndexType()).thenReturn(VectorIndexType.HNSW);
    when(live.getParentName()).thenReturn(PNameFactory.newName("T"));
    PTable data = mock(PTable.class);
    when(data.getPhysicalName()).thenReturn(PNameFactory.newName("T"));
    doReturn(live).when(pconn).getTableNoCache("LIVE");
    doReturn(data).when(pconn).getTableNoCache("T");
    HnswSegment.Descriptor other = new HnswSegment.Descriptor(Bytes.toBytes("LIVE"),
      Bytes.toBytes("m"), 100L, HConstants.EMPTY_END_ROW, 10, null, 100L, DEFINITION);
    listed = new ArrayList<>(cells(SEGMENT, SEGMENT.anchor, true));
    current = new ArrayList<>(cells(other, other.anchor, false));
    HnswIndexReader.Catalog catalog = reader.readCatalog();
    assertEquals(1, catalog.segments.size());
    assertTrue(catalog.malformed.isEmpty());
    assertTrue(catalog.strayKeys.isEmpty());
    assertTrue(catalog.keyless.isEmpty());
  }

  /**
   * A region holds the segments whose anchor row is in its key range, and it uses only those of the
   * definition of the index. After a split, the lower daughter holds the stack of the parent.
   */
  @Test
  public void testHeldAndFenced() {
    HnswSegment.Descriptor parent = new HnswSegment.Descriptor(KEY, HConstants.EMPTY_START_ROW,
      100L, HConstants.EMPTY_END_ROW, 10, null, 100L, DEFINITION);
    HnswSegment.Descriptor stale = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 50L,
      HConstants.EMPTY_END_ROW, 10, null, 50L, HnswSegment.definitionDigest("stale"));
    List<HnswSegment.Descriptor> all = Arrays.asList(parent, SEGMENT, stale);
    byte[] m = Bytes.toBytes("m");
    assertEquals(Collections.singletonList(parent),
      HnswIndexReader.held(all, HConstants.EMPTY_START_ROW, m));
    assertEquals(Arrays.asList(SEGMENT, stale),
      HnswIndexReader.held(all, m, HConstants.EMPTY_END_ROW));
    assertEquals(Collections.singletonList(SEGMENT),
      reader.fenced(HnswIndexReader.held(all, m, HConstants.EMPTY_END_ROW)));
  }

  /**
   * The tools count a row as held in memory when region replay reads it again. Both use a raw scan
   * for cells and delete markers at or after the replay start, in the families of the indexed
   * columns.
   */
  @Test
  public void testChangedSinceScansAsReplay() throws Exception {
    byte[] vectorFamily = Bytes.toBytes("V");
    IndexMaintainer maintainer = mock(IndexMaintainer.class);
    when(maintainer.getAllColumnsForDataTable())
      .thenReturn(Collections.singleton(new ColumnReference(vectorFamily, Bytes.toBytes("VEC"))));
    when(context.getMaintainer()).thenReturn(maintainer);
    byte[] row = Bytes.toBytes("r");
    current = Collections
      .singletonList(new KeyValue(row, vectorFamily, Bytes.toBytes("VEC"), 50L, Bytes.toBytes(1)));
    listed = current;
    Set<ImmutableBytesPtr> changed =
      reader.changedSince(Bytes.toBytes("a"), Bytes.toBytes("z"), 42);
    assertEquals(Collections.singleton(new ImmutableBytesPtr(row)), changed);
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(dataTable).getScanner(captor.capture());
    Scan scan = captor.getValue();
    assertTrue(scan.isRaw());
    assertFalse(scan.getCacheBlocks());
    assertEquals(42, scan.getTimeRange().getMin());
    assertEquals(1, scan.getFamilyMap().size());
    assertTrue(scan.getFamilyMap().containsKey(vectorFamily));
    assertTrue(Bytes.equals(Bytes.toBytes("a"), scan.getStartRow()));
    assertTrue(Bytes.equals(Bytes.toBytes("z"), scan.getStopRow()));
  }
}
