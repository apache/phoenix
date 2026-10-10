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

import io.github.jbellis.jvector.disk.ByteBufferIndexWriter;
import io.github.jbellis.jvector.disk.ByteBufferReader;
import io.github.jbellis.jvector.disk.IndexWriter;
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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PTable;

/**
 * An immutable HNSW graph segment. The cells of a segment are in the hidden MOB family {@code V#}
 * of the data table. A segment holds a serialized JVector {@link OnDiskGraphIndex} and a dense map
 * from graph node ordinals to data table row keys.
 * <p>
 * Payload layout: {@code [graph][mapping][mapping length (int)][MAGIC (int)]}. The mapping is
 * {@code [count (int)] ([key length (int)][key])*} in ordinal order. The ordinals are dense and
 * follow row key order. As a result, each row key range maps to one contiguous interval of
 * ordinals. JVector writes the graph into a heap array of the exact payload size.
 * <p>
 * The cells of a segment are in one row of {@code V#}, the {@link #anchor anchor row} of the key
 * range of the segment. The qualifier of each cell is
 * {@code [index key][0x00][start key length (int)][start key][time (long)][tag (byte)]}. The index
 * key is the index name without view prefixes. The qualifier without its tag is the identity of the
 * segment, and the identity names the segment everywhere. The anchor row follows from the key range
 * of the segment, so it is not part of the identity. After a merge, a region can hold segments at
 * the anchor rows of its predecessors. The payload cell is large and goes to a MOB file. The other
 * cells are small and stay in the store files. One small cell holds the encoded name of the region
 * that wrote the segment.
 * <p>
 * Full rebuilds make base segments.
 * <p>
 * Without quantization, the graph stores the full vectors. With quantization, it stores only
 * quantized codes: NVQ for SQ8, and fused PQ with NVQ for PQ. JVector reranks PQ candidates with a
 * vector feature, so a PQ segment also stores NVQ codes. A PQ segment adds the PQ codes of the
 * neighbors of each node, so it is larger than an SQ8 segment. PQ makes the graph traversal faster,
 * but it does not decrease memory. All codecs encode float32 vectors, and DOUBLE vectors become
 * float32. A rebuild does not read vectors from a segment. It scans the base table again.
 * <p>
 * A segment loads its graph into a direct buffer from {@link HnswOffheapAllocator}. When the
 * allocator evicts the buffer, the segment drops its graph, and the next search reads the cell
 * again. Eviction takes no lock, so it cannot deadlock with a reload of another segment. Each
 * search holds a reference to the buffer. The buffer becomes free when the segment is evicted or
 * closed and no search reads it.
 */
public final class HnswSegment {
  /** The tag of the payload cell of a segment. */
  public static final byte PAYLOAD_TAG = 'P';
  /** The tag of the cell that holds the end key of the segment range. */
  public static final byte END_KEY_TAG = 'E';
  /** The tag of the cell that holds the number of vectors in the segment. */
  public static final byte COUNT_TAG = 'N';
  /**
   * The tag of the cell that holds the replay start of the segment, {@link Descriptor#replayFrom}.
   */
  public static final byte REPLAY_FROM_TAG = 'R';
  /**
   * The tag of the cell that holds the digest of the index definition, {@link #definitionDigest}.
   */
  public static final byte DEFINITION_TAG = 'D';
  /**
   * The tag of the cell that holds the encoded name of the region that wrote the segment,
   * {@link Descriptor#writer}.
   */
  public static final byte WRITER_TAG = 'W';
  /** The tags of the cells of a segment. */
  static final byte[] TAGS =
    { PAYLOAD_TAG, END_KEY_TAG, COUNT_TAG, REPLAY_FROM_TAG, DEFINITION_TAG, WRITER_TAG };
  /** The length of a definition digest, which is a SHA-256 digest. */
  public static final int DEFINITION_LENGTH = 32;
  /** The last int of each segment payload, the ASCII text "HNSW". */
  public static final int MAGIC = 0x484E5357;
  /** The JVector neighbor overflow of all HNSW graphs that Phoenix builds. */
  static final float NEIGHBOR_OVERFLOW = 1.2f;
  // The largest array that the JVM can allocate. This is also the largest segment payload.
  private static final int MAX_PAYLOAD_BYTES = Integer.MAX_VALUE - 8;

  private final HnswSegmentStore store;
  private final Descriptor descriptor;
  private final HnswOffheapAllocator allocator;
  private final VectorSimilarityFunction similarity;
  private final byte[][] keys;
  // The loaded graph, or null after eviction or close
  private final AtomicReference<Resident> resident = new AtomicReference<>();
  // The graph that graph() returned. The segment keeps it until close().
  private volatile Resident pinned;
  // The failure to load a graph that is larger than the off-heap budget. A reload fails the same
  // way, and does not read the payload again.
  private volatile TooLargeException tooLarge;
  private volatile boolean closed;

  // A graph and the off-heap block that holds it
  private static final class Resident {
    final HnswOffheapAllocator.Block block;
    final OnDiskGraphIndex graph;

    Resident(HnswOffheapAllocator.Block block, OnDiskGraphIndex graph) {
      this.block = block;
      this.graph = graph;
    }
  }

  private HnswSegment(HnswSegmentStore store, Descriptor descriptor, HnswOffheapAllocator allocator,
    VectorSimilarityFunction similarity, byte[][] keys) {
    this.store = store;
    this.descriptor = descriptor;
    this.allocator = allocator;
    this.similarity = similarity;
    this.keys = keys;
  }

  /**
   * The metadata of a stored segment: its identity, key range, time, vector count, replay start,
   * anchor row, index definition digest and writer region.
   */
  public static final class Descriptor {
    /** The identity of the segment, {@link #identity(byte[], byte[], long)}. */
    public final byte[] identity;
    /** The key of the index, the index name without view prefixes. */
    public final byte[] indexKey;
    public final byte[] startKey;
    public final byte[] endKey;
    public final long time;
    public final int count;
    /**
     * The time from which a region replays the row changes that the segment can miss. It is the
     * segment time, or an earlier start of a write batch that changed the graph after the segment
     * time.
     */
    public final long replayFrom;
    /** The row of {@code V#} that holds the cells of the segment, the anchor of its key range. */
    public final byte[] anchor;
    /** The digest of the definition of the index that wrote the segment. */
    public final byte[] definition;
    /**
     * The encoded name of the region that wrote the segment. A listing reads it from the W cell. It
     * is null in the descriptor of a new segment, because the region store writes the name of its
     * own region.
     */
    public final byte[] writer;

    /**
     * Creates the descriptor of a new segment of an index that covers {@code [startKey, endKey)}.
     * @throws IllegalArgumentException if the key range has no anchor row
     */
    public Descriptor(byte[] indexKey, byte[] startKey, long time, byte[] endKey, int count,
      long replayFrom, byte[] definition) {
      this(indexKey, startKey, time, endKey, count, replayFrom, definition, null);
    }

    // Creates the descriptor of a listed segment, with the region that wrote it
    private Descriptor(byte[] indexKey, byte[] startKey, long time, byte[] endKey, int count,
      long replayFrom, byte[] definition, byte[] writer) {
      this.anchor = anchor(startKey, endKey);
      if (anchor == null) {
        throw new IllegalArgumentException("The key range [" + Bytes.toStringBinary(startKey) + ", "
          + Bytes.toStringBinary(endKey) + ") has no anchor row");
      }
      this.identity = identity(indexKey, startKey, time);
      this.indexKey = indexKey;
      this.startKey = startKey;
      this.endKey = endKey;
      this.time = time;
      this.count = count;
      this.replayFrom = replayFrom;
      this.definition = definition;
      this.writer = writer;
    }

    /** Returns true if the segment covers the exact key range {@code [start, end)}. */
    public boolean covers(byte[] start, byte[] end) {
      return Bytes.equals(startKey, start) && Bytes.equals(endKey, end);
    }

    /**
     * Returns true if a part of the given range is in the key range of this segment.
     * @param start the start key of the range, inclusive
     * @param end   the end key of the range, exclusive. An empty array has no upper limit.
     * @return true if the two ranges overlap
     */
    public boolean overlaps(byte[] start, byte[] end) {
      return (end.length == 0 || Bytes.compareTo(startKey, end) < 0)
        && (endKey.length == 0 || Bytes.compareTo(start, endKey) < 0);
    }

    /**
     * Returns true if the given segments together cover all of the key range of this segment.
     * @param segments the segments that can cover the range
     * @return true if the segments cover all of the range
     */
    public boolean coveredBy(List<Descriptor> segments) {
      return covered(startKey, endKey, segments);
    }

    @Override
    public String toString() {
      return "Descriptor{" + "indexKey=" + Bytes.toStringBinary(indexKey) + ", startKey="
        + Bytes.toStringBinary(startKey) + ", endKey=" + Bytes.toStringBinary(endKey) + ", time="
        + time + ", count=" + count + ", replayFrom=" + replayFrom + ", writer="
        + Bytes.toStringBinary(writer) + '}';
    }
  }

  /**
   * Returns true if the given segments together cover all of the key range {@code [from, to)}. An
   * empty start key or end key has no limit on that side.
   * @param from     the start key of the range, inclusive
   * @param to       the end key of the range, exclusive. An empty array has no upper limit.
   * @param segments the segments that can cover the range
   * @return true if the segments cover all of the range
   */
  public static boolean covered(byte[] from, byte[] to, List<Descriptor> segments) {
    List<Descriptor> sorted = new ArrayList<>(segments);
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

  /**
   * Returns the anchor row of the key range {@code [start, end)}. A region of that range writes its
   * segment cells to this row of {@code V#}. The anchor is {@code start || 0x00}, or {@code start}
   * if {@code end} is {@code start || 0x00}. The anchor is always in the range. The range
   * {@code [empty, 0x00)} holds no rows and has no anchor, so this method returns null for it.
   */
  public static byte[] anchor(byte[] start, byte[] end) {
    byte[] anchor = Arrays.copyOf(start, start.length + 1);
    if (!Bytes.equals(anchor, end)) {
      return anchor;
    }
    return start.length > 0 ? start : null;
  }

  /** Returns the prefix of the qualifiers of the segment cells of an index. */
  public static byte[] prefix(byte[] indexKey) {
    return Arrays.copyOf(indexKey, indexKey.length + 1);
  }

  /**
   * Returns the identity of a segment, the qualifier of its cells without the tag:
   * {@code [index key][0x00][start key length (int)][start key][time (long)]}.
   */
  public static byte[] identity(byte[] indexKey, byte[] startKey, long time) {
    return Bytes.add(prefix(indexKey), Bytes.add(Bytes.toBytes(startKey.length), startKey),
      Bytes.toBytes(time));
  }

  /** Returns the qualifier of the cell of a segment with the given tag. */
  public static byte[] qualifier(byte[] identity, byte tag) {
    byte[] qualifier = Arrays.copyOf(identity, identity.length + 1);
    qualifier[identity.length] = tag;
    return qualifier;
  }

  /** The parts of a segment cell qualifier. */
  public static final class Qualifier {
    public final byte[] indexKey;
    public final byte[] startKey;
    public final long time;
    public final byte tag;

    private Qualifier(byte[] indexKey, byte[] startKey, long time, byte tag) {
      this.indexKey = indexKey;
      this.startKey = startKey;
      this.time = time;
      this.tag = tag;
    }

    /** Returns the identity of the segment of the cell. */
    public byte[] identity() {
      return HnswSegment.identity(indexKey, startKey, time);
    }
  }

  /**
   * Decodes a segment cell qualifier, or returns null for a malformed qualifier. A qualifier is
   * malformed if the index key is empty or has no 0x00 after it. It is also malformed if the start
   * key length does not agree with the qualifier length, or if the tag is not a known tag.
   */
  public static Qualifier parseQualifier(byte[] bytes, int offset, int length) {
    int end = offset + length;
    int zero = offset;
    while (zero < end && bytes[zero] != 0) {
      zero++;
    }
    int start = zero + 1 + Bytes.SIZEOF_INT;
    if (zero == offset || start > end) {
      return null;
    }
    int startLength = Bytes.toInt(bytes, zero + 1);
    if (startLength < 0 || startLength != end - start - Bytes.SIZEOF_LONG - 1) {
      return null;
    }
    byte tag = bytes[end - 1];
    if (!isTag(tag)) {
      return null;
    }
    return new Qualifier(Arrays.copyOfRange(bytes, offset, zero),
      Arrays.copyOfRange(bytes, start, start + startLength),
      Bytes.toLong(bytes, start + startLength), tag);
  }

  private static boolean isTag(byte tag) {
    for (byte t : TAGS) {
      if (t == tag) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the SHA-256 digest of an index definition. Each segment stores this digest. If a user
   * drops an index and creates it again with a different definition, the new index ignores the
   * earlier segments.
   */
  public static byte[] definitionDigest(String definition) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(Bytes.toBytes(definition));
    } catch (NoSuchAlgorithmException e) {
      // Every Java platform implements SHA-256
      throw new IllegalStateException(e);
    }
  }

  /**
   * Collects the segment cells of a scan of {@code V#} and decodes the well-formed segments. A
   * well-formed segment has qualifiers that decode, an E cell, an int N, a long R, a digest D and a
   * W cell. Its cells must also be in the anchor row of its key range. The listing ignores the
   * payload cell. It copies the data that it keeps, so the scanner can release the cells.
   */
  static final class Listing {
    private final Map<ImmutableBytesPtr, Cells> segments = new LinkedHashMap<>();

    // The cells of one segment in one row
    private static final class Cells {
      final byte[] row;
      final Qualifier qualifier;
      byte[] endKey;
      byte[] count;
      byte[] replayFrom;
      byte[] definition;
      byte[] writer;

      Cells(byte[] row, Qualifier qualifier) {
        this.row = row;
        this.qualifier = qualifier;
      }
    }

    void add(Cell cell) {
      Qualifier q = parseQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
      if (q == null || q.tag == PAYLOAD_TAG) {
        return;
      }
      byte[] row = CellUtil.cloneRow(cell);
      byte[] identity = q.identity();
      Cells cells = segments.computeIfAbsent(
        new ImmutableBytesPtr(Bytes.add(Bytes.toBytes(row.length), row, identity)),
        k -> new Cells(row, q));
      byte[] value = CellUtil.cloneValue(cell);
      switch (q.tag) {
        case END_KEY_TAG:
          cells.endKey = value;
          break;
        case COUNT_TAG:
          cells.count = value;
          break;
        case REPLAY_FROM_TAG:
          cells.replayFrom = value;
          break;
        case DEFINITION_TAG:
          cells.definition = value;
          break;
        case WRITER_TAG:
          cells.writer = value;
          break;
        default:
          break;
      }
    }

    List<Descriptor> descriptors() {
      List<Descriptor> descriptors = new ArrayList<>(segments.size());
      for (Cells c : segments.values()) {
        if (
          c.endKey != null && c.count != null && c.count.length == Bytes.SIZEOF_INT
            && c.replayFrom != null && c.replayFrom.length == Bytes.SIZEOF_LONG
            && c.definition != null && c.definition.length == DEFINITION_LENGTH && c.writer != null
            && Bytes.equals(c.row, anchor(c.qualifier.startKey, c.endKey))
        ) {
          descriptors
            .add(new Descriptor(c.qualifier.indexKey, c.qualifier.startKey, c.qualifier.time,
              c.endKey, Bytes.toInt(c.count), Bytes.toLong(c.replayFrom), c.definition, c.writer));
        }
      }
      return descriptors;
    }
  }

  /**
   * Returns the JVector similarity function of a vector distance metric. HNSW supports only L2 and
   * COSINE.
   */
  public static VectorSimilarityFunction similarityFunction(String metric) {
    switch (metric) {
      case "L2":
        return VectorSimilarityFunction.EUCLIDEAN;
      case "COSINE":
        return VectorSimilarityFunction.COSINE;
      default:
        throw new IllegalArgumentException("Unsupported vector distance metric " + metric);
    }
  }

  /**
   * Builds an HNSW graph from vectors and their row keys, and returns the serialized segment
   * payload. The build sorts the rows by row key, so the graph ordinals follow row key order.
   * @param vi      the vector index metadata
   * @param vectors the vectors
   * @param keys    the data table row keys of the vectors, in the same order
   * @return the segment payload
   * @throws IOException if the serialization fails
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
    try (GraphIndexBuilder builder =
      graphBuilder(vi, sorted, similarityFunction(vi.getDistanceMetric()), true)) {
      return serialize(builder.build(sorted), sorted, vi, encodeKeys(sortedKeys));
    }
  }

  /**
   * Creates a graph builder with the M, ef_construction and alpha of the index. The JVector
   * constructor takes the neighbor overflow before alpha.
   * @param refineFinalGraph true if one pass builds the graph, false if the graph takes inserts one
   *                         at a time
   */
  public static GraphIndexBuilder graphBuilder(PTable.VectorIndex vi,
    RandomAccessVectorValues vectors, VectorSimilarityFunction similarity,
    boolean refineFinalGraph) {
    return new GraphIndexBuilder(vectors, similarity, vi.getHnswM(), vi.getHnswEfConstruction(),
      NEIGHBOR_OVERFLOW, vi.getHnswAlpha().floatValue(), true, refineFinalGraph);
  }

  /**
   * Trains the PQ codebooks. Global centering keeps Euclidean distances, but it changes cosine
   * similarity. JVector scores the centered vectors, so only an L2 index uses centering.
   */
  static ProductQuantization trainPq(RandomAccessVectorValues vectors, PTable.VectorIndex vi) {
    return ProductQuantization.compute(vectors, vi.getPqSegments(), 256,
      similarityFunction(vi.getDistanceMetric()) == VectorSimilarityFunction.EUCLIDEAN);
  }

  // Serializes the graph, then adds the key mapping and the trailer. JVector writes the graph two
  // times: first to measure it, then into an array of the exact payload size.
  private static byte[] serialize(ImmutableGraphIndex graph, RandomAccessVectorValues vectors,
    PTable.VectorIndex vi, byte[] mapping) throws IOException {
    String quantization = vi.getQuantizationType();
    // PQ needs 256 vectors to train its centroids. A smaller segment stores the full vectors.
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
        // SQ8 and PQ both store NVQ codes, a compact vector feature. An SQ8 search scores with
        // these codes, and a PQ search reranks with them.
        NVQuantization nvq = NVQuantization.compute(vectors, 1);
        features.add(new NVQ(nvq));
        states.put(FeatureId.NVQ_VECTORS,
          node -> new NVQ.State(nvq.encode(vectors.getVector(node))));
        if ("PQ".equals(quantization)) {
          ProductQuantization pq = trainPq(vectors, vi);
          PQVectors codes = (PQVectors) pq.encodeAll(vectors);
          features.add(new FusedPQ(graph.maxDegree(), pq));
          states.put(FeatureId.FUSED_PQ, node -> new FusedPQ.State(view, codes, node));
        }
      }
      SizeCounter counter = new SizeCounter();
      writeGraph(graph, features, states, counter);
      long size = counter.position() + mapping.length + 2L * Bytes.SIZEOF_INT;
      if (size > MAX_PAYLOAD_BYTES) {
        throw new TooLargeException("HNSW segment of " + size + " bytes exceeds the limit of "
          + MAX_PAYLOAD_BYTES + " bytes");
      }
      byte[] payload = new byte[(int) size];
      ByteBufferIndexWriter out = new ByteBufferIndexWriter(ByteBuffer.wrap(payload));
      writeGraph(graph, features, states, out);
      out.write(mapping);
      out.writeInt(mapping.length);
      out.writeInt(MAGIC);
      if (out.position() != size) {
        throw new IllegalStateException(
          "HNSW segment of " + out.position() + " bytes was measured as " + size + " bytes");
      }
      return payload;
    }
  }

  private static void writeGraph(ImmutableGraphIndex graph, List<Feature> features,
    Map<FeatureId, IntFunction<Feature.State>> states, IndexWriter out) throws IOException {
    OnDiskSequentialGraphIndexWriter.Builder writer =
      new OnDiskSequentialGraphIndexWriter.Builder(graph, out);
    for (Feature feature : features) {
      writer.with(feature);
    }
    try (OnDiskSequentialGraphIndexWriter w = writer.build()) {
      w.write(states);
    }
  }

  // Counts the bytes that JVector writes. The count stops at Integer.MAX_VALUE.
  private static final class SizeCounter extends DataOutputStream implements IndexWriter {
    SizeCounter() {
      super(OutputStream.nullOutputStream());
    }

    @Override
    public long position() {
      return written;
    }
  }

  /**
   * Signals that the payload cell of a segment with vectors is missing, as after the retirement of
   * the segment. A search of a closed segment also throws this exception. A payload cell whose MOB
   * file is missing or corrupt is present. HBase returns an empty value for it, and the segment
   * fails as a corrupt segment.
   */
  public static final class NotFoundException extends IOException {
    private static final long serialVersionUID = 1L;

    NotFoundException(byte[] identity, Object store) {
      super("HNSW segment " + Bytes.toStringBinary(identity) + " not found in " + store);
    }
  }

  /**
   * Signals that a segment is too large to build or to load. A retry fails the same way. The index
   * needs a larger budget or smaller regions.
   */
  public static final class TooLargeException extends DoNotRetryIOException {
    private static final long serialVersionUID = 1L;

    TooLargeException(String message) {
      super(message);
    }
  }

  /**
   * Opens a segment from a segment store and loads its graph off-heap. If the graph is larger than
   * the off-heap budget, the segment does not load it. A search of that segment then fails with
   * {@link TooLargeException}, but the row keys are available.
   * @param store      the segment store
   * @param descriptor the segment descriptor
   * @param allocator  the off-heap memory allocator
   * @param metric     the distance metric name
   * @return the open segment
   * @throws NotFoundException if the payload of a segment with vectors is missing
   * @throws IOException       if the read or the load of the segment fails
   */
  public static HnswSegment open(HnswSegmentStore store, Descriptor descriptor,
    HnswOffheapAllocator allocator, String metric) throws IOException {
    if (descriptor.count == 0) {
      return new HnswSegment(store, descriptor, allocator, similarityFunction(metric),
        new byte[0][]);
    }
    Cell payloadCell = store.readPayload(descriptor);
    HnswSegment segment = create(store, descriptor, allocator, metric, payloadCell.getValueArray(),
      payloadCell.getValueOffset(), payloadCell.getValueLength());
    try {
      segment.load(payloadCell.getValueArray(), payloadCell.getValueOffset(),
        graphLength(payloadCell.getValueArray(), payloadCell.getValueOffset(),
          payloadCell.getValueLength(), descriptor.identity)).block.release();
    } catch (TooLargeException e) {
      // Writes need only the row keys. A search fails the same way, and does not read the payload
      segment.tooLarge = e;
    }
    return segment;
  }

  /**
   * Returns a segment that this server writes, loaded from the payload in memory. It does not read
   * the segment store. A null payload gives a segment with no vectors. A reload after eviction
   * reads the payload from {@code store}, so do not search the segment until the store holds its
   * cells.
   */
  public static HnswSegment of(HnswSegmentStore store, Descriptor descriptor,
    HnswOffheapAllocator allocator, String metric, byte[] payload) throws IOException {
    if (payload == null) {
      return new HnswSegment(store, descriptor, allocator, similarityFunction(metric),
        new byte[0][]);
    }
    HnswSegment segment = create(store, descriptor, allocator, metric, payload, 0, payload.length);
    segment.load(payload, 0, graphLength(payload, 0, payload.length, descriptor.identity)).block
      .release();
    return segment;
  }

  // Returns the segment of a payload, with its row keys and without its graph
  private static HnswSegment create(HnswSegmentStore store, Descriptor descriptor,
    HnswOffheapAllocator allocator, String metric, byte[] bytes, int offset, int length)
    throws IOException {
    int graphLength = graphLength(bytes, offset, length, descriptor.identity);
    byte[][] keys =
      decodeKeys(bytes, offset + graphLength, length - graphLength - 8, descriptor.identity);
    return new HnswSegment(store, descriptor, allocator, similarityFunction(metric), keys);
  }

  // Checks the payload framing and returns the length of the graph. HBase returns an empty payload
  // for a MOB reference whose file is lost. This check rejects it, so it never loads as a graph.
  private static int graphLength(byte[] bytes, int offset, int length, byte[] identity)
    throws IOException {
    int end = offset + length;
    int mapping = length < 8 ? -1 : Bytes.toInt(bytes, end - 8);
    if (length < 8 || Bytes.toInt(bytes, end - 4) != MAGIC || mapping < 0 || mapping > length - 8) {
      // The failure is permanent, because a new read of the payload fails the same way
      throw new DoNotRetryIOException("Corrupt HNSW segment " + Bytes.toStringBinary(identity));
    }
    return length - 8 - mapping;
  }

  // Copies the graph into an off-heap block and makes it resident. The caller releases the
  // returned reference.
  private Resident load(byte[] bytes, int offset, int graphLength) throws IOException {
    HnswOffheapAllocator.Block block = allocator.allocate(graphLength);
    Resident r;
    try {
      ByteBuffer buffer = block.buffer();
      buffer.put(bytes, offset, graphLength).flip();
      ByteBuffer graphBytes = buffer.asReadOnlyBuffer();
      r = new Resident(block,
        OnDiskGraphIndex.load(() -> new ByteBufferReader(graphBytes.duplicate())));
    } catch (RuntimeException e) {
      block.release();
      // JVector throws a RuntimeException for a graph that it cannot read. The failure is
      // permanent, because a new read of the payload fails the same way.
      throw new DoNotRetryIOException(
        "Corrupt HNSW segment " + Bytes.toStringBinary(descriptor.identity), e);
    } catch (Throwable t) {
      block.release();
      throw t;
    }
    resident.set(r);
    allocator.commit(this, block, () -> resident.compareAndSet(r, null));
    if (closed) {
      // close() can run between the publish and the commit, and then not see the resident block
      resident.compareAndSet(r, null);
      allocator.release(this);
    }
    return r;
  }

  // Encodes the key mapping of a payload: [count (int)] ([key length (int)][key])*
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

  // Decodes the key mapping of a payload. The failure for a corrupt payload is permanent, because
  // a new read fails the same way.
  private static byte[][] decodeKeys(byte[] bytes, int offset, int length, byte[] identity)
    throws DoNotRetryIOException {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, length));
      int count = in.readInt();
      // Each key uses at least the bytes of its length. As a result, a corrupt count or length
      // cannot allocate more memory than the cell holds.
      if (count < 0 || count > in.available() / Bytes.SIZEOF_INT) {
        throw new IOException("Key count " + count);
      }
      byte[][] keys = new byte[count][];
      for (int i = 0; i < keys.length; i++) {
        int keyLength = in.readInt();
        if (keyLength < 0 || keyLength > in.available()) {
          throw new IOException("Key length " + keyLength);
        }
        keys[i] = new byte[keyLength];
        in.readFully(keys[i]);
      }
      return keys;
    } catch (IOException | RuntimeException e) {
      throw new DoNotRetryIOException("Corrupt HNSW segment " + Bytes.toStringBinary(identity), e);
    }
  }

  // Returns the resident graph with a reference that the caller releases. If eviction dropped the
  // graph, this method reads the payload again.
  private Resident acquire() throws IOException {
    Resident r = resident.get();
    if (r == null || !r.block.retain()) {
      synchronized (this) {
        if (closed) {
          throw new NotFoundException(descriptor.identity, store);
        }
        r = resident.get();
        if (r == null || !r.block.retain()) {
          if (tooLarge != null) {
            throw new TooLargeException(tooLarge.getMessage());
          }
          Cell cell = store.readPayload(descriptor);
          try {
            r = load(cell.getValueArray(), cell.getValueOffset(), graphLength(cell.getValueArray(),
              cell.getValueOffset(), cell.getValueLength(), descriptor.identity));
          } catch (TooLargeException e) {
            tooLarge = e;
            throw e;
          }
        }
      }
    }
    allocator.touch(this);
    return r;
  }

  /**
   * Returns the graph of the segment, or null if the segment has no vectors. If eviction dropped
   * the graph, this method reads the payload again. The segment keeps the returned graph in memory
   * until {@link #close()}. Do not use the graph after close(). Do not call this method at the same
   * time as close().
   */
  public synchronized OnDiskGraphIndex graph() throws IOException {
    if (keys.length == 0) {
      return null;
    }
    if (pinned == null) {
      pinned = acquire();
    }
    return pinned.graph;
  }

  /** Returns the number of vectors in this segment. */
  public int size() {
    return keys.length;
  }

  /** Returns the data table row key of a vector ordinal. */
  public byte[] getKey(int ordinal) {
    return keys[ordinal];
  }

  /**
   * Returns the lowest ordinal whose row key is equal to or greater than the given key. The
   * ordinals follow row key order, so {@code [ceiling(start), ceiling(stop))} are the ordinals of
   * the key range {@code [start, stop)}.
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

  /** Returns the vector ordinal of a row key, or -1 if the segment does not hold the key. */
  public int ordinalOf(ImmutableBytesPtr key) {
    // The ordinals follow row key order, so a binary search finds the key
    int low = 0;
    int high = keys.length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int c = Bytes.compareTo(keys[mid], 0, keys[mid].length, key.get(), key.getOffset(),
        key.getLength());
      if (c < 0) {
        low = mid + 1;
      } else if (c > 0) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -1;
  }

  /**
   * Finds the approximate nearest neighbors of the query vector in the segment. The beam width is
   * at least {@code topK}, because JVector refuses a rerank count below the result count.
   * @param query    the query vector
   * @param topK     the maximum number of results
   * @param efSearch the beam width of the search
   * @param accept   the ordinals that the search can return
   * @return the neighbor ordinals and their similarity scores
   * @throws IOException if the graph search fails
   */
  public SearchResult search(VectorFloat<?> query, int topK, int efSearch, Bits accept)
    throws IOException {
    if (keys.length == 0) {
      return new SearchResult(new SearchResult.NodeScore[0], 0, 0, 0, 0, 0.0f);
    }
    Resident r = acquire();
    OnDiskGraphIndex g = r.graph;
    try (GraphSearcher searcher = new GraphSearcher(g)) {
      // The score functions use the view of the searcher. Fused PQ reads the codes of the
      // neighbors from this view in the traversal.
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
    } finally {
      r.block.release();
    }
  }

  /**
   * Releases the off-heap memory of this segment. The memory becomes free when the searches that
   * read it end. A later search throws {@link NotFoundException}.
   */
  public void close() {
    closed = true;
    resident.set(null);
    allocator.release(this);
    Resident p = pinned;
    if (p != null) {
      pinned = null;
      p.block.release();
    }
  }
}
