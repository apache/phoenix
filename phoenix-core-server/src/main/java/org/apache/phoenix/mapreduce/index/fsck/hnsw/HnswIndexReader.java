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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.HnswSegmentStore;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.index.vector.CentroidManager;

/**
 * Reads the segments of an HNSW index and the vectors of its data table for the tools. The segments
 * are cells of the hidden family {@code V#} of the data table. The reader reads them only through
 * {@link HnswSegmentStore.TableStore}. It decodes segments with {@link HnswSegment}, and gets the
 * vector of a data row with {@link org.apache.phoenix.index.IndexMaintainer} as the region server
 * does. Thus the tools and the server agree on the segments and on the vector of each row. A close
 * of the reader closes its HBase connection.
 */
public class HnswIndexReader implements AutoCloseable {
  /** The number of data rows that one multi-get reads. */
  private static final int GET_BATCH = 1000;

  private final HnswIndexContext context;
  private final Connection connection;
  private final Table dataTable;
  private final HnswSegmentStore store;

  public HnswIndexReader(HnswIndexContext context) throws IOException {
    this(context, ConnectionFactory
      .createConnection(context.getConnection().getQueryServices().getConfiguration()));
  }

  HnswIndexReader(HnswIndexContext context, Connection connection) throws IOException {
    this.context = context;
    this.connection = connection;
    this.dataTable = connection.getTable(context.getDataPhysicalName());
    this.store = new HnswSegmentStore.TableStore(dataTable);
  }

  public HnswIndexContext getContext() {
    return context;
  }

  /**
   * Returns the well-formed segments of the index for all definitions, in order of start key and
   * then time.
   */
  public List<HnswSegment.Descriptor> listSegments() throws IOException {
    return sort(store.list(context.getIndexKey()));
  }

  private static List<HnswSegment.Descriptor> sort(List<HnswSegment.Descriptor> segments) {
    segments.sort((a, b) -> {
      int c = Bytes.compareTo(a.startKey, b.startKey);
      return c != 0 ? c : Long.compare(a.time, b.time);
    });
    return segments;
  }

  /**
   * Returns the segments that the region of the key range {@code [start, end)} holds. These are the
   * segments whose anchor row is in the range. A region discovers only the segments that it holds.
   */
  public static List<HnswSegment.Descriptor> held(List<HnswSegment.Descriptor> all, byte[] start,
    byte[] end) {
    List<HnswSegment.Descriptor> held = new ArrayList<>();
    for (HnswSegment.Descriptor d : all) {
      if (
        Bytes.compareTo(d.anchor, start) >= 0
          && (end.length == 0 || Bytes.compareTo(d.anchor, end) < 0)
      ) {
        held.add(d);
      }
    }
    return held;
  }

  /**
   * Returns true if the base segments cover the region of the key range {@code [start, end)}. A
   * region without an anchor row holds no rows, so it is covered, as in IndexTool. A region that is
   * not covered reads its rows without the index until a rebuild writes a base that covers it.
   */
  public static boolean covered(byte[] start, byte[] end, List<HnswSegment.Descriptor> segments) {
    return HnswSegment.anchor(start, end) == null || HnswSegment.covered(start, end, segments);
  }

  /**
   * Returns the segments that have the current definition of the index. A region does not use the
   * segments of another definition.
   */
  public List<HnswSegment.Descriptor> fenced(List<HnswSegment.Descriptor> segments) {
    byte[] definition = context.getDefinition();
    List<HnswSegment.Descriptor> fenced = new ArrayList<>();
    for (HnswSegment.Descriptor d : segments) {
      if (Bytes.equals(d.definition, definition)) {
        fenced.add(d);
      }
    }
    return fenced;
  }

  /** Returns the default replica regions of the data table, in start key order. */
  public List<RegionInfo> listRegions() throws IOException {
    List<RegionInfo> regions = new ArrayList<>();
    try (RegionLocator locator = connection.getRegionLocator(context.getDataPhysicalName())) {
      for (HRegionLocation location : locator.getAllRegionLocations()) {
        if (RegionReplicaUtil.isDefaultReplica(location.getRegion())) {
          regions.add(location.getRegion());
        }
      }
    }
    regions.sort((a, b) -> Bytes.compareTo(a.getStartKey(), b.getStartKey()));
    return regions;
  }

  /** Returns the region that holds the row, or null if no region holds it. */
  public static RegionInfo regionOf(List<RegionInfo> regions, byte[] row) {
    for (RegionInfo region : regions) {
      if (region.containsRow(row)) {
        return region;
      }
    }
    return null;
  }

  /**
   * The cells of {@code V#} that the checks of the index classify. One listing of the index finds
   * its segments. Then one key-only scan of all cells finds the other cells.
   */
  public static final class Catalog {
    /** The well-formed segments of the index, for all definitions. */
    public final List<HnswSegment.Descriptor> segments = new ArrayList<>();
    /**
     * The row and the identity of each segment of the index that is not well-formed. Also the row
     * and the qualifier of each cell of the index that does not decode.
     */
    public final List<byte[][]> malformed = new ArrayList<>();
    /** The rows of the cells of each other index key that names no HNSW index of the table. */
    public final Map<String, List<byte[]>> strayKeys = new TreeMap<>();
    /**
     * The row and the qualifier of each cell that no purge request can name. Such a cell has no
     * index key, or its key changes when a purge request carries it as a string.
     */
    public final List<byte[][]> keyless = new ArrayList<>();
  }

  /**
   * Reads the cells of {@code V#} for the checks of the index. First, the listing of the index
   * reads the segments. Then a key-only scan of all cells finds the cells that the listing did not
   * decode. A segment write puts its chunks first, and then one commit Put adds the small cells. A
   * retirement is atomic. Thus, if the later scan finds small cells of a segment that the listing
   * did not see, a write added the segment after the listing. The method does not classify such a
   * segment. An identity with only chunk cells is a write in progress if it is newer than each
   * listed segment of its anchor row. The method does not classify it either. An older identity
   * with only chunk cells is a failed write, so it is malformed. A chunk of a listed segment that
   * its layout does not count is malformed too. The scans never read a MOB file. They never
   * transfer a chunk, even when the chunk is in the memstore. The method classifies each cell when
   * the scan gives it, and keeps only the cells that the checks report.
   */
  public Catalog readCatalog() throws IOException, SQLException {
    byte[] indexKey = context.getIndexKey();
    Catalog catalog = new Catalog();
    List<byte[][]> rejected = new ArrayList<>();
    catalog.segments.addAll(sort(store.list(indexKey, rejected)));
    catalog.malformed.addAll(rejected);
    // The layout of each well-formed segment, and the newest well-formed segment of each row
    Map<ImmutableBytesPtr, HnswSegment.Layout> layouts = new HashMap<>();
    Map<ImmutableBytesPtr, Long> newest = new HashMap<>();
    for (HnswSegment.Descriptor d : catalog.segments) {
      layouts.put(HnswSegment.Listing.key(d.anchor, d.identity), d.layout);
      newest.merge(new ImmutableBytesPtr(d.anchor), d.time, Math::max);
    }
    Set<ImmutableBytesPtr> listed = new HashSet<>(layouts.keySet());
    for (byte[][] r : rejected) {
      listed.add(HnswSegment.Listing.key(r[0], r[1]));
    }
    // The segments of the index that the listing did not see, with the time of the identity. A
    // write added the subset with small cells after the listing
    Map<ImmutableBytesPtr, byte[][]> unlisted = new LinkedHashMap<>();
    Map<ImmutableBytesPtr, Long> unlistedTimes = new HashMap<>();
    Set<ImmutableBytesPtr> small = new HashSet<>();
    Map<String, Boolean> live = new HashMap<>();
    try {
      store.listCells(cell -> {
        byte[] row = CellUtil.cloneRow(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        int key = HnswSegment.indexKeyLength(qualifier, 0, qualifier.length);
        if (key == 0 || key == qualifier.length) {
          catalog.keyless.add(new byte[][] { row, qualifier });
          return;
        }
        if (!Bytes.equals(qualifier, 0, key, indexKey, 0, indexKey.length)) {
          String other = Bytes.toString(qualifier, 0, key);
          // A purge request carries the key as a string, and the region uses the part after the
          // last '#'. A purge cannot name a key that changes in this conversion
          byte[] request = Bytes.toBytes(CentroidManager.getCentroidIndexName(other));
          if (!Bytes.equals(request, 0, request.length, qualifier, 0, key)) {
            catalog.keyless.add(new byte[][] { row, qualifier });
            return;
          }
          Boolean isIndex = live.get(other);
          if (isIndex == null) {
            try {
              isIndex = HnswIndexManager.isHnswIndexOf(context.getConnection(), other,
                context.getDataPhysicalName());
            } catch (SQLException e) {
              throw new UncheckedIOException(new IOException(e));
            }
            live.put(other, isIndex);
          }
          if (!isIndex) {
            catalog.strayKeys.computeIfAbsent(other, k -> new ArrayList<>()).add(row);
          }
          return;
        }
        HnswSegment.Qualifier q = HnswSegment.parseQualifier(qualifier, 0, qualifier.length);
        if (q == null) {
          catalog.malformed.add(new byte[][] { row, qualifier });
          return;
        }
        ImmutableBytesPtr segment = HnswSegment.Listing.key(row, q.identity());
        if (listed.contains(segment)) {
          HnswSegment.Layout layout = layouts.get(segment);
          if (layout != null && q.chunk >= 0 && q.chunk >= layout.chunks(q.tag)) {
            catalog.malformed.add(new byte[][] { row, qualifier });
          }
          return;
        }
        unlisted.putIfAbsent(segment, new byte[][] { row, q.identity() });
        unlistedTimes.putIfAbsent(segment, q.time);
        if (q.chunk < 0) {
          small.add(segment);
        }
      });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
    for (Map.Entry<ImmutableBytesPtr, byte[][]> e : unlisted.entrySet()) {
      if (small.contains(e.getKey())) {
        continue;
      }
      Long rowNewest = newest.get(new ImmutableBytesPtr(e.getValue()[0]));
      if (rowNewest == null || unlistedTimes.get(e.getKey()) > rowNewest) {
        // The identity belongs to a write in progress
        continue;
      }
      catalog.malformed.add(e.getValue());
    }
    return catalog;
  }

  /** Returns true if a data row has a vector. The scan stops at the first such row. */
  public boolean hasVectors() throws IOException {
    try (ResultScanner scanner =
      dataTable.getScanner(vectorScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW))) {
      for (Result r : scanner) {
        if (vectorOf(r) != null) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns the size of the payload of a segment, from its layout. The method reads nothing.
   */
  public long payloadSize(HnswSegment.Descriptor d) {
    return store.payloadSize(d);
  }

  /**
   * Writes the payload of a segment to the stream, one group of chunks at a time. The heap holds
   * the chunks of at most one group, so the size of the payload has no limit.
   * @throws HnswSegment.NotFoundException       if a region retired the segment
   * @throws HnswSegment.CorruptSegmentException if a chunk is absent, unresolved or of the wrong
   *                                             length
   */
  public void copyPayload(HnswSegment.Descriptor d, OutputStream out) throws IOException {
    store.copyPayload(d, out);
  }

  /**
   * Reads a range of the stream with the given chunk tag. The method reads only the chunks that
   * hold the range, in groups of consecutive chunks.
   * @throws IllegalArgumentException            if the range is not in the stream
   * @throws HnswSegment.NotFoundException       if a region retired the segment
   * @throws HnswSegment.CorruptSegmentException if a chunk is absent, unresolved or of the wrong
   *                                             length
   */
  public byte[] readRange(HnswSegment.Descriptor d, byte tag, long offset, int length)
    throws IOException {
    HnswSegment.Layout layout = d.layout;
    if (offset < 0 || length < 0 || offset + length > layout.length(tag)) {
      throw new IllegalArgumentException(
        "Range " + offset + "+" + length + " of a stream of " + layout.length(tag) + " bytes");
    }
    byte[] range = new byte[length];
    if (length == 0) {
      return range;
    }
    int first = (int) (offset / layout.chunkSize);
    int last = (int) ((offset + length - 1) / layout.chunkSize);
    store.readChunks(d, tag, first, last + 1, (chunk, cell) -> {
      // The overlap of this chunk with the range, as offsets in the stream
      long start = Math.max(offset, (long) chunk * layout.chunkSize);
      long end = Math.min(offset + length, (long) chunk * layout.chunkSize + cell.getValueLength());
      int within = (int) (start - (long) chunk * layout.chunkSize);
      System.arraycopy(cell.getValueArray(), cell.getValueOffset() + within, range,
        (int) (start - offset), (int) (end - start));
    });
    return range;
  }

  /**
   * Opens a segment with the client off-heap allocator of this JVM. The caller closes the segment.
   * A graph larger than the off-heap budget does not fail the open. The open then reads only the
   * key mapping, and {@link HnswSegment#graph()} throws {@link HnswSegment.TooLargeException}.
   * @throws HnswSegment.NotFoundException       if a region retired the segment
   * @throws HnswSegment.CorruptSegmentException if a chunk is bad or the bytes do not decode
   * @throws PayloadTooLargeException            if the heap part exceeds the limit of this JVM
   * @throws IOException                         if the read fails
   */
  public HnswSegment open(HnswSegment.Descriptor d) throws IOException {
    checkSize(d);
    return HnswSegment.open(store, d,
      HnswOffheapAllocator.getForClient(connection.getConfiguration()), context.getMetric());
  }

  // Refuses a segment if its heap part is more than a quarter of the maximum heap. An open decodes
  // the key mapping and the tombstones into the heap. The graph goes off-heap, so the check does
  // not count it. The check reads no cell
  static void checkSize(HnswSegment.Descriptor d) throws IOException {
    long size = d.layout.mappingLength() + d.layout.tombstonesLength;
    long limit = Runtime.getRuntime().maxMemory() / 4;
    if (size > limit) {
      throw new PayloadTooLargeException(
        "The key mapping and tombstones of segment " + Bytes.toStringBinary(d.identity) + " have "
          + size + " bytes, more than the " + limit + " this JVM reads");
    }
  }

  /**
   * Signals that the key mapping and the tombstones of a segment are too large for the heap of this
   * JVM. It shows a limit of the tool, not a fault of the segment.
   */
  public static final class PayloadTooLargeException extends IOException {
    private static final long serialVersionUID = 1L;

    PayloadTooLargeException(String message) {
      super(message);
    }
  }

  /**
   * Returns a scan of the data rows in {@code [start, end)} that reads only the columns from which
   * the index gets its vectors. The scan does not fill the block cache.
   */
  public Scan vectorScan(byte[] start, byte[] end) {
    Scan scan = new Scan().withStartRow(start).withStopRow(end).setCacheBlocks(false);
    for (ColumnReference ref : context.getMaintainer().getAllColumnsForDataTable()) {
      scan.addColumn(ref.getFamily(), ref.getQualifier());
    }
    return scan;
  }

  /** Returns the indexed vector of a data row, or null if the row has no vector. */
  public float[] vectorOf(Result row) throws IOException {
    return HnswIndexManager.vectorOf(context.getMaintainer(), row);
  }

  /**
   * Returns the vectors of the given data rows, by row key. The map has no entry for a row that
   * does not exist or has no vector.
   */
  public Map<ImmutableBytesPtr, float[]> getVectors(Collection<byte[]> keys) throws IOException {
    Map<ImmutableBytesPtr, float[]> vectors = new HashMap<>();
    List<Get> batch = new ArrayList<>(GET_BATCH);
    for (byte[] key : keys) {
      Get get = new Get(key);
      for (ColumnReference ref : context.getMaintainer().getAllColumnsForDataTable()) {
        get.addColumn(ref.getFamily(), ref.getQualifier());
      }
      batch.add(get);
      if (batch.size() == GET_BATCH) {
        readBatch(batch, vectors);
      }
    }
    readBatch(batch, vectors);
    return vectors;
  }

  private void readBatch(List<Get> batch, Map<ImmutableBytesPtr, float[]> vectors)
    throws IOException {
    if (batch.isEmpty()) {
      return;
    }
    for (Result r : dataTable.get(batch)) {
      float[] vector = vectorOf(r);
      if (vector != null) {
        vectors.put(new ImmutableBytesPtr(r.getRow()), vector);
      }
    }
    batch.clear();
  }

  /**
   * Returns the keys of the data rows in {@code [start, end)} with a change or a delete at or after
   * {@code since}. It uses the scan of the replay of a region that opens. When {@code since} is the
   * replay start of a region, these are the rows that the region can hold only in memory.
   */
  public Set<ImmutableBytesPtr> changedSince(byte[] start, byte[] end, long since)
    throws IOException {
    Set<ImmutableBytesPtr> changed = new HashSet<>();
    Scan scan = HnswIndexManager.changedRowsScan(context.getMaintainer(), since).withStartRow(start)
      .withStopRow(end);
    try (ResultScanner scanner = dataTable.getScanner(scan)) {
      for (Result r : scanner) {
        changed.add(new ImmutableBytesPtr(r.getRow()));
      }
    }
    return changed;
  }

  @Override
  public void close() throws IOException {
    try {
      dataTable.close();
    } finally {
      connection.close();
    }
  }
}
