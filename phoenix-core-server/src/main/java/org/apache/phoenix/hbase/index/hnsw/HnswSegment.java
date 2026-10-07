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

import io.github.jbellis.jvector.disk.ByteBufferReader;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskSequentialGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.disk.feature.NVQ;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.NVQuantization;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PTable;

/**
 * Represents an immutable HNSW graph segment stored as a MOB cell in the index table. Each segment
 * contains a serialized JVector {@link OnDiskGraphIndex} alongside dense mappings from graph node
 * ordinals to data table primary keys.
 * <p>
 * Payload layout: {@code [graph][mapping][mapping length (int)][MAGIC (int)]}, where the mapping is
 * {@code [count (int)] ([key length (int)][key])*} stored in ordinal order. Graph node ordinals are
 * dense and sorted lexicographically by row key to enable mapping row key ranges directly to
 * contiguous ordinal intervals. Graph serialization is performed sequentially using an
 * {@link HnswBufferWriter} with graph headers appended.
 * <p>
 * Base segments are produced by full rebuilds. Delta segments are produced by flushes and stacked
 * onto a base segment, recording updated rows, tombstones for deletions, and the base segment
 * timestamp.
 * <p>
 * The graph stores quantized codes only (NVQ for SQ8, fused PQ with NVQ for PQ, as JVector needs a
 * vector feature and reranks PQ candidates with it) or, without quantization, the full vectors. A
 * rebuild never reads vectors back from a segment; it rescans the base table.
 * <p>
 * The graph is materialized into a direct buffer from {@link HnswOffheapAllocator}. When the
 * allocator evicts it, the graph reference is dropped and the next search re-reads the cell.
 * Eviction takes no lock, so it cannot deadlock with a reload on another segment, and in-flight
 * searches keep their reference until they finish.
 */
public final class HnswSegment {
  /** Column qualifier used for segment payloads in the index table. */
  public static final byte[] PAYLOAD_QUALIFIER = Bytes.toBytes("P");
  /** Column qualifier storing the segment region end key. */
  public static final byte[] END_KEY_QUALIFIER = Bytes.toBytes("E");
  /** Column qualifier storing the number of indexed vectors. */
  public static final byte[] COUNT_QUALIFIER = Bytes.toBytes("N");
  /** Column qualifier storing the base segment timestamp for delta segments. */
  public static final byte[] BASE_TIME_QUALIFIER = Bytes.toBytes("B");
  /** Column qualifier storing tombstones for deleted row keys in delta segments. */
  public static final byte[] TOMBSTONES_QUALIFIER = Bytes.toBytes("T");
  private static final int MAGIC = 0x484E5357; // "HNSW"
  private static final float NEIGHBOR_OVERFLOW = 1.2f;
  // Fixed byte overhead for JVector graph index headers excluding dynamic feature headers
  private static final int GRAPH_HEADER_BYTES = 4096;

  private final Connection connection;
  private final TableName indexTable;
  private final byte[] family;
  private final byte[] rowKey;
  private final HnswOffheapAllocator allocator;
  private final VectorSimilarityFunction similarity;
  private final byte[][] keys;
  private final Map<ImmutableBytesPtr, Integer> ordinals;
  private final byte[][] tombstones;
  private volatile OnDiskGraphIndex graph;

  private HnswSegment(Connection connection, TableName indexTable, byte[] family, byte[] rowKey,
    HnswOffheapAllocator allocator, VectorSimilarityFunction similarity, byte[][] keys,
    byte[][] tombstones) {
    this.connection = connection;
    this.indexTable = indexTable;
    this.family = family;
    this.rowKey = rowKey;
    this.allocator = allocator;
    this.similarity = similarity;
    this.keys = keys;
    this.ordinals = new HashMap<>(keys.length * 2);
    for (int i = 0; i < keys.length; i++) {
      ordinals.put(new ImmutableBytesPtr(keys[i]), i);
    }
    this.tombstones = tombstones;
  }

  /**
   * Metadata describing a persisted segment, including its row key, covered region key range,
   * creation timestamp, and vector count.
   */
  public static final class Descriptor {
    public final byte[] rowKey;
    public final byte[] startKey;
    public final byte[] endKey;
    public final long time;
    public final int count;
    /** Base segment timestamp for delta segments, or null for base segments. */
    public final Long baseTime;

    public Descriptor(byte[] rowKey, byte[] endKey, int count, Long baseTime) {
      this.rowKey = rowKey;
      this.startKey = Arrays.copyOf(rowKey, rowKey.length - Bytes.SIZEOF_LONG);
      this.endKey = endKey;
      this.time = Bytes.toLong(rowKey, rowKey.length - Bytes.SIZEOF_LONG);
      this.count = count;
      this.baseTime = baseTime;
    }

    /** Returns true if this descriptor represents a delta segment. */
    public boolean isDelta() {
      return baseTime != null;
    }

    /** Returns true if the segment covers the exact key range {@code [start, end)}. */
    public boolean covers(byte[] start, byte[] end) {
      return Bytes.equals(startKey, start) && Bytes.equals(endKey, end);
    }

    /**
     * Determines whether the key range of this segment intersects the given boundary range.
     * @param start start key of the range (inclusive)
     * @param end   end key of the range (exclusive, or empty byte array if unbounded)
     * @return true if the ranges intersect
     */
    public boolean overlaps(byte[] start, byte[] end) {
      return (end.length == 0 || Bytes.compareTo(startKey, end) < 0)
        && (endKey.length == 0 || Bytes.compareTo(start, endKey) < 0);
    }

    /**
     * Determines whether this segment's key range is fully covered by the given segments.
     * @param segments list of candidate segment descriptors
     * @return true if completely covered
     */
    public boolean coveredBy(List<Descriptor> segments) {
      return covered(startKey, endKey, segments);
    }

    @Override
    public String toString() {
      return "Descriptor{" + "startKey=" + Bytes.toStringBinary(startKey) + ", endKey="
        + Bytes.toStringBinary(endKey) + ", time=" + time + ", count=" + count
        + (isDelta() ? ", baseTime=" + baseTime : "") + '}';
    }
  }

  /**
   * Evaluates whether the key range {@code [from, to)} is fully spanned by the collective ranges of
   * the specified segments. Empty start or end keys indicate unbounded intervals. Delta segments
   * never cover or hide older segments.
   * @param from     start key of the target range (inclusive)
   * @param to       end key of the target range (exclusive, or empty byte array if unbounded)
   * @param segments collection of segment descriptors to evaluate
   * @return true if the range is fully covered
   */
  public static boolean covered(byte[] from, byte[] to, List<Descriptor> segments) {
    List<Descriptor> sorted = new ArrayList<>();
    for (Descriptor d : segments) {
      if (!d.isDelta()) {
        sorted.add(d);
      }
    }
    sorted.sort((a, b) -> Bytes.compareTo(a.startKey, b.startKey));
    byte[] reached = from;
    for (Descriptor d : sorted) {
      if (Bytes.compareTo(d.startKey, reached) > 0) {
        break;
      }
      if (d.endKey.length == 0) {
        return true;
      }
      if (Bytes.compareTo(d.endKey, reached) > 0) {
        reached = d.endKey;
      }
      if (to.length > 0 && Bytes.compareTo(reached, to) >= 0) {
        return true;
      }
    }
    return false;
  }

  /** Writes a base segment record to the index table. */
  public static Descriptor write(Table table, byte[] family, byte[] startKey, byte[] endKey,
    long time, byte[] payload, int count) throws IOException {
    return write(table, family, startKey, endKey, time, payload, count, null, new byte[0][]);
  }

  /**
   * Writes a segment record to the index table. When {@code baseTime} is non-null, the record is
   * written as a delta segment with optional deletion tombstones.
   */
  public static Descriptor write(Table table, byte[] family, byte[] startKey, byte[] endKey,
    long time, byte[] payload, int count, Long baseTime, byte[][] tombstones) throws IOException {
    byte[] rowKey = Bytes.add(startKey, Bytes.toBytes(time));
    Put put = new Put(rowKey).addColumn(family, END_KEY_QUALIFIER, endKey).addColumn(family,
      COUNT_QUALIFIER, Bytes.toBytes(count));
    if (baseTime != null) {
      put.addColumn(family, BASE_TIME_QUALIFIER, Bytes.toBytes(baseTime));
    }
    if (payload != null) {
      put.addColumn(family, PAYLOAD_QUALIFIER, payload);
    }
    if (tombstones.length > 0) {
      put.addColumn(family, TOMBSTONES_QUALIFIER, encodeKeys(tombstones));
    }
    table.put(put);
    return new Descriptor(rowKey, endKey, count, baseTime);
  }

  /**
   * Scans and returns descriptors for all segments in the index table.
   * @param table  index table
   * @param family column family
   * @return list of segment descriptors
   * @throws IOException if scanning fails
   */
  public static List<Descriptor> list(Table table, byte[] family) throws IOException {
    List<Descriptor> segments = new ArrayList<>();
    Scan scan = new Scan().addColumn(family, END_KEY_QUALIFIER).addColumn(family, COUNT_QUALIFIER)
      .addColumn(family, BASE_TIME_QUALIFIER);
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result r : scanner) {
        byte[] end = r.getValue(family, END_KEY_QUALIFIER);
        byte[] count = r.getValue(family, COUNT_QUALIFIER);
        if (end != null && count != null) {
          byte[] base = r.getValue(family, BASE_TIME_QUALIFIER);
          Long baseTime =
            base != null && base.length == Bytes.SIZEOF_LONG ? Bytes.toLong(base) : null;
          segments.add(new Descriptor(r.getRow(), end, Bytes.toInt(count), baseTime));
        }
      }
    }
    return segments;
  }

  /** Maps a vector distance metric name to the corresponding JVector similarity function. */
  public static VectorSimilarityFunction similarityFunction(String metric) {
    switch (metric) {
      case "L2":
        return VectorSimilarityFunction.EUCLIDEAN;
      case "INNER_PRODUCT":
        return VectorSimilarityFunction.DOT_PRODUCT;
      case "COSINE":
        return VectorSimilarityFunction.COSINE;
      default:
        throw new IllegalArgumentException("Unsupported vector distance metric " + metric);
    }
  }

  /**
   * Constructs an immutable HNSW graph segment from vector data and primary keys, returning the
   * serialized segment payload. Input rows are sorted lexicographically so that graph ordinals
   * align strictly with primary key order.
   * @param vi      vector index metadata
   * @param vectors vector dataset
   * @param keys    corresponding data table row keys
   * @return serialized segment bytes
   * @throws IOException if serialization fails
   */
  public static byte[] build(PTable.VectorIndex vi, RandomAccessVectorValues vectors, byte[][] keys)
    throws IOException {
    if (vectors.size() != keys.length) {
      throw new IllegalArgumentException(vectors.size() + " vectors but " + keys.length + " keys");
    }
    Integer[] order = new Integer[keys.length];
    for (int i = 0; i < order.length; i++) {
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> Bytes.compareTo(keys[a], keys[b]));
    List<VectorFloat<?>> sortedVectors = new ArrayList<>(order.length);
    byte[][] sortedKeys = new byte[order.length][];
    for (int i = 0; i < order.length; i++) {
      VectorFloat<?> v = vectors.getVector(order[i]);
      sortedVectors.add(vectors.isValueShared() ? v.copy() : v);
      sortedKeys[i] = keys[order[i]];
    }
    RandomAccessVectorValues sorted =
      new ListRandomAccessVectorValues(sortedVectors, vectors.dimension());
    VectorSimilarityFunction similarity = similarityFunction(vi.getDistanceMetric());
    try (GraphIndexBuilder builder = new GraphIndexBuilder(sorted, similarity, vi.getHnswM(),
      vi.getHnswEfConstruction(), NEIGHBOR_OVERFLOW, vi.getHnswAlpha().floatValue(), true, true)) {
      ImmutableGraphIndex graph = builder.build(sorted);
      byte[] mapping = encodeKeys(sortedKeys);
      HnswBufferWriter out = writeGraph(graph, sorted, vi, mapping.length + 2 * Bytes.SIZEOF_INT);
      out.write(mapping);
      out.writeInt(mapping.length);
      out.writeInt(MAGIC);
      return out.toByteArray();
    }
  }

  // Serializes the graph index into direct memory pre-sized to accommodate the graph and trailer
  private static HnswBufferWriter writeGraph(ImmutableGraphIndex graph,
    RandomAccessVectorValues vectors, PTable.VectorIndex vi, int trailerBytes) throws IOException {
    String quantization = vi.getQuantizationType();
    // Datasets below the minimum centroid training threshold fall back to unquantized vectors
    if ("PQ".equals(quantization) && vectors.size() < 256) {
      quantization = "NONE";
    }
    List<Feature> features = new ArrayList<>(2);
    Map<FeatureId, IntFunction<Feature.State>> states = new EnumMap<>(FeatureId.class);
    try (ImmutableGraphIndex.View view = graph.getView()) {
      if ("NONE".equals(quantization)) {
        features.add(new InlineVectors(vectors.dimension()));
        states.put(FeatureId.INLINE_VECTORS,
          node -> new InlineVectors.State(vectors.getVector(node)));
      } else {
        // Generate scalar quantization (NVQ) for compact vector encoding and initial candidate
        // scoring
        NVQuantization nvq = NVQuantization.compute(vectors, 1);
        features.add(new NVQ(nvq));
        states.put(FeatureId.NVQ_VECTORS,
          node -> new NVQ.State(nvq.encode(vectors.getVector(node))));
        if ("PQ".equals(quantization)) {
          ProductQuantization pq =
            ProductQuantization.compute(vectors, vi.getPqSegments(), 256, true);
          PQVectors codes = (PQVectors) pq.encodeAll(vectors);
          features.add(new FusedPQ(graph.maxDegree(), pq));
          states.put(FeatureId.FUSED_PQ, node -> new FusedPQ.State(view, codes, node));
        }
      }
      HnswBufferWriter out = new HnswBufferWriter(estimateSize(graph, features, trailerBytes));
      OnDiskSequentialGraphIndexWriter.Builder writer =
        new OnDiskSequentialGraphIndexWriter.Builder(graph, out);
      for (Feature feature : features) {
        writer.with(feature);
      }
      try (OnDiskSequentialGraphIndexWriter w = writer.build()) {
        w.write(states);
      }
      return out;
    }
  }

  /** Thrown when a segment row, or the payload of a segment that holds vectors, is absent. */
  public static final class NotFoundException extends IOException {
    private static final long serialVersionUID = 1L;

    NotFoundException(byte[] rowKey, TableName indexTable) {
      super("HNSW segment " + Bytes.toStringBinary(rowKey) + " not found in " + indexTable);
    }
  }

  /**
   * Computes an upper bound estimate of serialized segment byte size to minimize buffer
   * reallocation during serialization. Accounts for duplicate graph headers in the header and
   * footer, base layer nodes with inline features and full adjacency lists, upper layer index
   * hierarchies, fused feature codes, and trailer metadata.
   */
  static int estimateSize(ImmutableGraphIndex graph, List<Feature> features, int trailerBytes) {
    long headers = GRAPH_HEADER_BYTES;
    long inline = 0;
    for (Feature feature : features) {
      headers += feature.headerSize();
      inline += feature.featureSize();
    }
    long base =
      graph.size(0) * (2L * Integer.BYTES + inline + (long) Integer.BYTES * graph.getDegree(0));
    long upper = 0;
    for (int level = 1; level <= graph.getMaxLevel(); level++) {
      upper +=
        graph.size(level) * (2L * Integer.BYTES + (long) Integer.BYTES * graph.getDegree(level));
    }
    return (int) Math.min(Integer.MAX_VALUE - 8,
      2 * headers + base + base / 16 + upper + trailerBytes);
  }

  /**
   * Opens and materializes an HNSW segment stored in the specified index table.
   * @param connection HBase connection
   * @param indexTable index table name
   * @param family     column family name
   * @param rowKey     segment row key
   * @param allocator  off-heap memory allocator
   * @param metric     distance metric name
   * @return materialized HNSW segment
   * @throws IOException if reading or opening the segment fails
   */
  public static HnswSegment open(Connection connection, TableName indexTable, byte[] family,
    byte[] rowKey, HnswOffheapAllocator allocator, String metric) throws IOException {
    try (Table table = connection.getTable(indexTable)) {
      Result result = table.get(new Get(rowKey).addColumn(family, PAYLOAD_QUALIFIER)
        .addColumn(family, TOMBSTONES_QUALIFIER).addColumn(family, COUNT_QUALIFIER));
      Cell payloadCell = result.getColumnLatestCell(family, PAYLOAD_QUALIFIER);
      Cell tombstoneCell = result.getColumnLatestCell(family, TOMBSTONES_QUALIFIER);
      Cell countCell = result.getColumnLatestCell(family, COUNT_QUALIFIER);
      if (payloadCell == null && tombstoneCell == null && countCell == null) {
        throw new NotFoundException(rowKey, indexTable);
      }
      byte[][] tombstones = tombstoneCell != null
        ? decodeKeys(tombstoneCell.getValueArray(), tombstoneCell.getValueOffset(),
          tombstoneCell.getValueLength())
        : new byte[0][];
      if (payloadCell == null) {
        return new HnswSegment(connection, indexTable, family, rowKey, allocator,
          similarityFunction(metric), new byte[0][], tombstones);
      }
      int graphLength = graphLength(payloadCell, rowKey);
      HnswSegment segment = new HnswSegment(connection, indexTable, family, rowKey, allocator,
        similarityFunction(metric),
        decodeKeys(payloadCell.getValueArray(), payloadCell.getValueOffset() + graphLength,
          payloadCell.getValueLength() - graphLength - 8),
        tombstones);
      segment.materialize(payloadCell, graphLength);
      return segment;
    }
  }

  private static Cell readPayload(Connection connection, TableName indexTable, byte[] family,
    byte[] rowKey) throws IOException {
    try (Table table = connection.getTable(indexTable)) {
      Result result = table.get(new Get(rowKey).addColumn(family, PAYLOAD_QUALIFIER));
      Cell cell = result.getColumnLatestCell(family, PAYLOAD_QUALIFIER);
      if (cell == null) {
        throw new NotFoundException(rowKey, indexTable);
      }
      return cell;
    }
  }

  private static int graphLength(Cell cell, byte[] rowKey) throws IOException {
    int length = cell.getValueLength();
    int end = cell.getValueOffset() + length;
    if (length < 8 || Bytes.toInt(cell.getValueArray(), end - 4) != MAGIC) {
      throw new IOException("Corrupt HNSW segment " + Bytes.toStringBinary(rowKey));
    }
    return length - 8 - Bytes.toInt(cell.getValueArray(), end - 8);
  }

  // Materializes the on-disk graph structure into an off-heap buffer
  private void materialize(Cell cell, int graphLength) throws IOException {
    ByteBuffer buffer = allocator.allocate(this, graphLength, this::evict);
    buffer.put(cell.getValueArray(), cell.getValueOffset(), graphLength).flip();
    ByteBuffer graphBytes = buffer.asReadOnlyBuffer();
    graph = OnDiskGraphIndex.load(() -> new ByteBufferReader(graphBytes.duplicate()));
  }

  // [count (int)] ([key length (int)][key])*, the payload's key mapping and a delta's tombstones
  private static byte[] encodeKeys(byte[][] keys) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(keys.length);
    for (byte[] key : keys) {
      out.writeInt(key.length);
      out.write(key);
    }
    out.flush();
    return bytes.toByteArray();
  }

  private static byte[][] decodeKeys(byte[] bytes, int offset, int length) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, length));
    byte[][] keys = new byte[in.readInt()][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = new byte[in.readInt()];
      in.readFully(keys[i]);
    }
    return keys;
  }

  private void evict() {
    graph = null;
  }

  private OnDiskGraphIndex graph() throws IOException {
    if (keys.length == 0) {
      return null;
    }
    OnDiskGraphIndex g = graph;
    if (g == null) {
      synchronized (this) {
        g = graph;
        if (g == null) {
          Cell cell = readPayload(connection, indexTable, family, rowKey);
          materialize(cell, graphLength(cell, rowKey));
          g = graph;
        }
      }
    }
    allocator.touch(this);
    return g;
  }

  /** Returns the index table row key for this segment. */
  public byte[] getRowKey() {
    return rowKey;
  }

  /** Returns the number of vectors in this segment. */
  public int size() {
    return keys.length;
  }

  /** Returns the data table primary key for the specified vector ordinal. */
  public byte[] getKey(int ordinal) {
    return keys[ordinal];
  }

  /** Returns deleted row keys recorded as tombstones in this segment. */
  public byte[][] getTombstones() {
    return tombstones;
  }

  /**
   * Locates the lowest ordinal whose row key is greater than or equal to the specified key. Because
   * ordinals follow sorted row key order, {@code [ceiling(start), ceiling(stop))} defines the
   * ordinal slice covering the range {@code [start, stop)}.
   */
  public int ceiling(byte[] key) {
    return ceiling(keys, key);
  }

  static int ceiling(byte[][] keys, byte[] key) {
    int low = 0;
    int high = keys.length;
    while (low < high) {
      int mid = (low + high) >>> 1;
      if (Bytes.compareTo(keys[mid], key) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  /** Returns the vector ordinal corresponding to the specified row key, or -1 if not present. */
  public int ordinalOf(ImmutableBytesPtr key) {
    Integer ordinal = ordinals.get(key);
    return ordinal != null ? ordinal : -1;
  }

  /**
   * Searches the segment for approximate nearest neighbors matching the query vector.
   * @param query    query vector
   * @param topK     maximum number of results to return
   * @param efSearch search queue expansion factor
   * @param accept   filter bitset determining eligible vector ordinals
   * @return search result containing neighbor ordinals and similarity scores
   * @throws IOException if graph search fails
   */
  public SearchResult search(VectorFloat<?> query, int topK, int efSearch, Bits accept)
    throws IOException {
    if (keys.length == 0) {
      return new SearchResult(new SearchResult.NodeScore[0], 0, 0, 0, 0, 0.0f);
    }
    OnDiskGraphIndex g = graph();
    try (GraphSearcher searcher = new GraphSearcher(g)) {
      // Use the searcher view to evaluate scores and load quantized neighbor codes during traversal
      OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();
      SearchScoreProvider ssp;
      if (g.getFeatureSet().contains(FeatureId.FUSED_PQ)) {
        ssp = new DefaultSearchScoreProvider(view.approximateScoreFunctionFor(query, similarity),
          view.rerankerFor(query, similarity));
      } else if (g.getFeatureSet().contains(FeatureId.NVQ_VECTORS)) {
        ssp = new DefaultSearchScoreProvider(view.rerankerFor(query, similarity));
      } else {
        ssp = DefaultSearchScoreProvider.exact(query, similarity, view);
      }
      return searcher.search(ssp, topK, Math.max(efSearch, topK), 0.0f, 0.0f, accept);
    }
  }

  /** Releases off-heap memory allocated for this segment. */
  public void close() {
    allocator.release(this);
    graph = null;
  }
}
