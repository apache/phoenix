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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.IndexUtil;

/**
 * Provides access to HNSW index segment metadata, raw segment payloads, and underlying data table
 * vectors. Uses {@link org.apache.phoenix.index.IndexMaintainer} and {@link HnswSegment} to ensure
 * tooling evaluates rows consistently with server-side query and rebuild paths.
 */
public class HnswIndexReader implements AutoCloseable {
  /** Batch size for multi-row vector lookups. */
  private static final int GET_BATCH = 1000;
  // Segment payloads not ending with TRAILER_MAGIC represent HBase MOB cell references
  private static final byte[] TRAILER_MAGIC = Bytes.toBytes(0x484E5357);

  private final HnswIndexContext context;
  private final Connection connection;
  private final Table indexTable;
  private final Table dataTable;

  public HnswIndexReader(HnswIndexContext context) throws IOException {
    this.context = context;
    this.connection = ConnectionFactory
      .createConnection(context.getConnection().getQueryServices().getConfiguration());
    this.indexTable = connection.getTable(context.getIndexPhysicalName());
    this.dataTable = connection.getTable(context.getDataPhysicalName());
  }

  public HnswIndexContext getContext() {
    return context;
  }

  /** Lists all segment descriptors ordered by start key and timestamp. */
  public List<HnswSegment.Descriptor> listSegments() throws IOException {
    List<HnswSegment.Descriptor> segments = HnswSegment.list(indexTable, context.getFamily());
    segments.sort((a, b) -> {
      int c = Bytes.compareTo(a.startKey, b.startKey);
      return c != 0 ? c : Long.compare(a.time, b.time);
    });
    return segments;
  }

  /** Returns default-replica regions for the data table sorted by start key. */
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

  /**
   * Scans for index table rows that do not conform to valid segment descriptor schema. Only row
   * keys and column qualifiers are populated.
   */
  public List<Result> listStrayRows() throws IOException {
    Set<ImmutableBytesPtr> segments = new HashSet<>();
    for (HnswSegment.Descriptor d : HnswSegment.list(indexTable, context.getFamily())) {
      segments.add(new ImmutableBytesPtr(d.rowKey));
    }
    List<Result> stray = new ArrayList<>();
    Scan scan = new Scan().setFilter(new KeyOnlyFilter());
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    try (ResultScanner scanner = indexTable.getScanner(scan)) {
      for (Result r : scanner) {
        if (!segments.contains(new ImmutableBytesPtr(r.getRow()))) {
          stray.add(r);
        }
      }
    }
    return stray;
  }

  /** Returns true if at least one data table row contains vector data. */
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
   * Returns payload sizes mapped by segment row key, inspecting MOB references or inline cell
   * values without reading external MOB files.
   */
  public Map<ImmutableBytesPtr, Integer> payloadSizes() throws IOException {
    Map<ImmutableBytesPtr, Integer> sizes = new HashMap<>();
    Scan scan = new Scan().addColumn(context.getFamily(), HnswSegment.PAYLOAD_QUALIFIER);
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    try (ResultScanner scanner = indexTable.getScanner(scan)) {
      for (Result r : scanner) {
        Cell cell = r.getColumnLatestCell(context.getFamily(), HnswSegment.PAYLOAD_QUALIFIER);
        if (cell != null) {
          sizes.put(new ImmutableBytesPtr(r.getRow()), payloadSize(cell));
        }
      }
    }
    return sizes;
  }

  // Inline payloads terminate with TRAILER_MAGIC; MOB references encode value length followed by
  // filename
  private static int payloadSize(Cell cell) {
    int length = cell.getValueLength();
    if (
      length >= TRAILER_MAGIC.length
        && Bytes.equals(cell.getValueArray(), cell.getValueOffset() + length - TRAILER_MAGIC.length,
          TRAILER_MAGIC.length, TRAILER_MAGIC, 0, TRAILER_MAGIC.length)
    ) {
      return length;
    }
    return length >= Bytes.SIZEOF_INT
      ? Bytes.toInt(cell.getValueArray(), cell.getValueOffset())
      : length;
  }

  /**
   * Reads the full segment payload bytes. Returns null if the payload cell is absent, or an empty
   * byte array if the referenced MOB cell cannot be resolved.
   * @throws IOException if reading fails or payload exceeds heap limits
   */
  public byte[] readPayload(HnswSegment.Descriptor d) throws IOException {
    checkSize(d);
    Get get = new Get(d.rowKey).addColumn(context.getFamily(), HnswSegment.PAYLOAD_QUALIFIER);
    get.setAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS, Bytes.toBytes(true));
    Cell cell =
      indexTable.get(get).getColumnLatestCell(context.getFamily(), HnswSegment.PAYLOAD_QUALIFIER);
    return cell == null ? null : CellUtil.cloneValue(cell);
  }

  /**
   * Loads and opens an {@link HnswSegment} instance. Caller is responsible for closing.
   * @throws IOException if the segment payload is missing, corrupt, or exceeds memory limits
   */
  public HnswSegment open(HnswSegment.Descriptor d) throws IOException {
    checkSize(d);
    return HnswSegment.open(connection, context.getIndexPhysicalName(), context.getFamily(),
      d.rowKey, HnswOffheapAllocator.get(connection.getConfiguration()), context.getMetric());
  }

  // Guard against memory exhaustion if payload exceeds 25% of maximum JVM heap
  private void checkSize(HnswSegment.Descriptor d) throws IOException {
    Get get = new Get(d.rowKey).addColumn(context.getFamily(), HnswSegment.PAYLOAD_QUALIFIER);
    get.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    Cell cell =
      indexTable.get(get).getColumnLatestCell(context.getFamily(), HnswSegment.PAYLOAD_QUALIFIER);
    long limit = Runtime.getRuntime().maxMemory() / 4;
    if (cell != null && payloadSize(cell) > limit) {
      throw new IOException("The payload of segment " + Bytes.toStringBinary(d.rowKey) + " has "
        + payloadSize(cell) + " bytes, more than the " + limit + " this JVM reads");
    }
  }

  /** Callback receiver for row keys and corresponding float vectors. */
  public interface VectorConsumer {
    void accept(byte[] key, float[] vector) throws IOException;
  }

  /**
   * Scans data rows within {@code [start, end)} containing vector data, projecting only columns
   * required by the index maintainer.
   */
  public void scanVectors(byte[] start, byte[] end, VectorConsumer consumer) throws IOException {
    try (ResultScanner scanner = dataTable.getScanner(vectorScan(start, end))) {
      for (Result r : scanner) {
        float[] vector = vectorOf(r);
        if (vector != null) {
          consumer.accept(r.getRow(), vector);
        }
      }
    }
  }

  /** Creates a scan covering {@code [start, end)} projecting index vector source columns. */
  public Scan vectorScan(byte[] start, byte[] end) {
    Scan scan = new Scan().withStartRow(start).withStopRow(end).setCacheBlocks(false);
    for (ColumnReference ref : context.getMaintainer().getAllColumnsForDataTable()) {
      scan.addColumn(ref.getFamily(), ref.getQualifier());
    }
    return scan;
  }

  /** Extracts the float vector from a data table result row, or null if unpopulated. */
  public float[] vectorOf(Result row) throws IOException {
    if (row.isEmpty()) {
      return null;
    }
    Put put = new Put(row.getRow());
    for (Cell cell : row.rawCells()) {
      put.add(cell);
    }
    return context.getMaintainer().getVectorAsFloats(new IndexUtil.SimpleValueGetter(put),
      HConstants.LATEST_TIMESTAMP);
  }

  /** Batch-fetches vectors for specified data row keys. */
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
   * Identifies data rows in {@code [start, end)} modified or deleted at or after {@code since}.
   * During region startup, these mutations are replayed into memory rather than served from
   * segments.
   */
  public Set<ImmutableBytesPtr> changedSince(byte[] start, byte[] end, long since)
    throws IOException {
    Set<ImmutableBytesPtr> changed = new HashSet<>();
    Scan scan = new Scan().withStartRow(start).withStopRow(end).setRaw(true)
      .setFilter(new KeyOnlyFilter()).setCacheBlocks(false);
    scan.setTimeRange(since, HConstants.LATEST_TIMESTAMP);
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
      indexTable.close();
      dataTable.close();
    } finally {
      connection.close();
    }
  }
}
