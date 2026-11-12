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
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
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
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * An immutable HNSW graph segment. The cells of a segment are in the hidden MOB family {@code V#}
 * of the data table. A segment holds a serialized JVector {@link OnDiskGraphIndex} and a dense map
 * from graph node ordinals to data table row keys.
 * <p>
 * Payload layout: {@code [graph][mapping][mapping length (int)][MAGIC (int)]}. The mapping is
 * {@code [count (int)] ([key length (int)][key])*} in ordinal order. The ordinals are dense and
 * follow row key order. As a result, each row key range maps to one contiguous interval of
 * ordinals. JVector writes the graph directly into an off-heap block. No heap array holds the full
 * payload.
 * <p>
 * The cells of a segment are in one row of {@code V#}, the {@link #anchor anchor row} of the key
 * range of the segment. A small cell has the qualifier
 * {@code [index key][0x00][start key length (int)][start key][time (long)][tag (byte)]}. The index
 * key is the index name without view prefixes. The qualifier without its tag is the identity of the
 * segment, and the identity names the segment everywhere. The anchor row follows from the key range
 * of the segment, so it is not part of the identity. After a merge, a region can hold segments at
 * the anchor rows of its predecessors. One small cell holds the encoded name of the region that
 * wrote the segment.
 * <p>
 * The payload and the tombstones are byte streams. Each stream is a sequence of chunk cells, and
 * chunk {@code i} holds the stream bytes {@code [i * chunkSize, (i + 1) * chunkSize)}. A chunk cell
 * has the qualifier {@code [identity][chunk index (int)][tag (byte)]}. The tag stays the last byte,
 * so a filter on the last byte can keep out all chunk cells. A big-endian chunk index sorts the
 * chunks in stream order. The layout cell {@link #LAYOUT_TAG L} records the chunk size and the
 * length of each stream, and the reads check each chunk against it. Large chunks go to MOB files.
 * The small cells stay in the store files. A chunk is much smaller than a full payload. Thus no
 * read, write or compaction of a segment holds a full payload in one heap array.
 * <p>
 * Full rebuilds make base segments. Flushes make delta segments, and each delta segment stacks on
 * one base segment. A delta segment holds the rows that changed, a tombstone for each deleted row,
 * and the time of its base segment.
 * <p>
 * Without quantization, the graph stores the full vectors. With quantization, it stores only
 * quantized codes: NVQ for SQ8, and fused PQ with NVQ for PQ. JVector reranks PQ candidates with a
 * vector feature, so a PQ segment also stores NVQ codes. A PQ segment adds the PQ codes of the
 * neighbors of each node, so it is larger than an SQ8 segment. PQ makes the graph traversal faster,
 * but it does not decrease memory. All codecs encode float32 vectors, and DOUBLE vectors become
 * float32. A rebuild does not read vectors from a segment. It scans the base table again.
 * <p>
 * A segment loads its graph into a direct buffer from {@link HnswOffheapAllocator}. When the
 * allocator evicts the buffer, the segment drops its graph, and the next search reads the graph
 * chunks again. Eviction takes no lock, so it cannot deadlock with a reload of another segment.
 * Each search holds a reference to the buffer. The buffer becomes free when the segment is evicted
 * or closed and no search reads it.
 */
public final class HnswSegment {
  /** The tag of the payload chunk cells of a segment. */
  public static final byte PAYLOAD_TAG = 'P';
  /** The tag of the cell that holds the end key of the segment range. */
  public static final byte END_KEY_TAG = 'E';
  /** The tag of the cell that holds the number of vectors in the segment. */
  public static final byte COUNT_TAG = 'N';
  /** The tag of the cell that holds the base segment time of a delta segment. */
  public static final byte BASE_TIME_TAG = 'B';
  /**
   * The tag of the chunk cells that hold the deleted row keys of a delta segment. After many
   * deletes, the stream is large, so its chunks can go to MOB files.
   */
  public static final byte TOMBSTONES_TAG = 'T';
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
  /** The tag of the cell that holds the {@link Layout} of the chunk streams of the segment. */
  public static final byte LAYOUT_TAG = 'L';
  /** The tags of the small cells of a segment. Each small cell has one qualifier. */
  public static final byte[] SMALL_TAGS = { END_KEY_TAG, COUNT_TAG, BASE_TIME_TAG, REPLAY_FROM_TAG,
    DEFINITION_TAG, WRITER_TAG, LAYOUT_TAG };
  /** The tags of the chunk streams of a segment. Each chunk of a stream has its own qualifier. */
  public static final byte[] CHUNK_TAGS = { PAYLOAD_TAG, TOMBSTONES_TAG };
  /**
   * The chunk size of new segments, 255 KiB. A chunk cell, with its key and its value, stays at or
   * below the default largest MSLAB allocation, 256 KiB. Thus the memstore copies the cell into its
   * MSLAB pool, and makes no separate array for the cell. The row and the qualifier of each chunk
   * cell hold the start key of the region. If the start key has more than about 450 bytes, a chunk
   * cell is too large for MSLAB. Such a cell costs one more copy in the young generation.
   * <p>
   * With G1 regions of 2 MiB or more, each array that holds chunks on the write, flush and read
   * paths stays below the G1 humongous threshold. The largest flush buffer holds about two chunk
   * cells. With G1 regions of 1 MiB, the threshold is 512 KiB, and the flush buffer stays below it
   * only for short start keys.
   */
  public static final int CHUNK_SIZE = 255 * 1024;
  /**
   * The largest chunk size that a layout accepts, 8 MiB minus 64 KiB. A layout records its own
   * chunk size, so the validity of a layout does not depend on the chunk size of new segments. The
   * bound stops a corrupt layout cell from naming a very large chunk.
   */
  public static final int MAX_CHUNK_SIZE = 8 * 1024 * 1024 - 64 * 1024;
  /** The length of a definition digest, which is a SHA-256 digest. */
  public static final int DEFINITION_LENGTH = 32;
  /** The last int of each segment payload, the ASCII text "HNSW". */
  public static final int MAGIC = 0x484E5357;
  /** The JVector neighbor overflow of all HNSW graphs that Phoenix builds. */
  static final float NEIGHBOR_OVERFLOW = 1.2f;
  /** The largest direct buffer that the JVM can allocate. This is also the largest graph. */
  public static final int MAX_GRAPH_BYTES = Integer.MAX_VALUE - 8;
  // The most chunks in one stream. The limit keeps a corrupt layout cell from naming a very large
  // number of qualifiers. At the default chunk size, it allows streams of more than 16 GB. This is
  // more than MAX_GRAPH_BYTES.
  static final int MAX_CHUNKS = 1 << 16;
  // The length of the payload trailer: the mapping length and the magic
  private static final int TRAILER_LENGTH = 2 * Bytes.SIZEOF_INT;

  private final HnswSegmentStore store;
  private final Descriptor descriptor;
  private final HnswOffheapAllocator allocator;
  private final VectorSimilarityFunction similarity;
  private final byte[][] keys;
  private final byte[][] tombstones;
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
    VectorSimilarityFunction similarity, byte[][] keys, byte[][] tombstones) {
    this.store = store;
    this.descriptor = descriptor;
    this.allocator = allocator;
    this.similarity = similarity;
    this.keys = keys;
    this.tombstones = tombstones;
  }

  /**
   * The metadata of a stored segment: its identity, key range, time, vector count, base segment
   * time, replay start, anchor row, index definition digest and writer region.
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
    /** The time of the base segment of a delta segment, or null for a base segment. */
    public final Long baseTime;
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
    /** The chunk size and the stream lengths of the segment, from its L cell. */
    public final Layout layout;

    /**
     * Creates the descriptor of a new segment with no payload and no tombstones, of an index that
     * covers {@code [startKey, endKey)}.
     * @throws IllegalArgumentException if the key range has no anchor row
     */
    public Descriptor(byte[] indexKey, byte[] startKey, long time, byte[] endKey, int count,
      Long baseTime, long replayFrom, byte[] definition) {
      this(indexKey, startKey, time, endKey, count, baseTime, replayFrom, definition, Layout.EMPTY);
    }

    /**
     * Creates the descriptor of a new segment of an index that covers {@code [startKey, endKey)},
     * with the layout of its chunk streams.
     * @throws IllegalArgumentException if the key range has no anchor row
     */
    public Descriptor(byte[] indexKey, byte[] startKey, long time, byte[] endKey, int count,
      Long baseTime, long replayFrom, byte[] definition, Layout layout) {
      this(indexKey, startKey, time, endKey, count, baseTime, replayFrom, definition, layout, null);
    }

    // Creates the descriptor of a listed segment, with the region that wrote it
    private Descriptor(byte[] indexKey, byte[] startKey, long time, byte[] endKey, int count,
      Long baseTime, long replayFrom, byte[] definition, Layout layout, byte[] writer) {
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
      this.baseTime = baseTime;
      this.replayFrom = replayFrom;
      this.definition = definition;
      this.layout = layout;
      this.writer = writer;
    }

    /** Returns true for a delta segment. */
    public boolean isDelta() {
      return baseTime != null;
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
        + time + ", count=" + count + (isDelta() ? ", baseTime=" + baseTime : "") + ", replayFrom="
        + replayFrom + ", writer=" + Bytes.toStringBinary(writer) + ", layout=" + layout + '}';
    }
  }

  /**
   * The layout of the chunk streams of a segment. The L cell holds it in this form:
   * {@code [format (byte)][chunk size (int)][payload length (long)][graph length (long)]
   * [tombstones length (long)]}. A stream of length n has {@code ceil(n / chunkSize)} chunks. Each
   * chunk holds {@code chunkSize} bytes, and the last chunk holds the remainder. The graph is the
   * first part of the payload. A segment stores its own chunk size, so the readers do not depend on
   * the chunk size of the server that reads it.
   */
  public static final class Layout {
    /** The format of the layout cell and of the chunk streams. */
    public static final byte FORMAT = 1;
    /** The length of the encoded layout. */
    public static final int ENCODED_LENGTH = 1 + Bytes.SIZEOF_INT + 3 * Bytes.SIZEOF_LONG;
    /** The layout of a segment with no payload and no tombstones. */
    public static final Layout EMPTY = new Layout(CHUNK_SIZE, 0, 0, 0);

    public final int chunkSize;
    public final long payloadLength;
    /** The length of the graph, the first part of the payload. */
    public final long graphLength;
    public final long tombstonesLength;

    /**
     * Creates a layout.
     * @throws IllegalArgumentException if the lengths do not make a valid layout
     */
    public Layout(int chunkSize, long payloadLength, long graphLength, long tombstonesLength) {
      String error = check(chunkSize, payloadLength, graphLength, tombstonesLength);
      if (error != null) {
        throw new IllegalArgumentException(error);
      }
      this.chunkSize = chunkSize;
      this.payloadLength = payloadLength;
      this.graphLength = graphLength;
      this.tombstonesLength = tombstonesLength;
    }

    // Returns the reason that the values are not a valid layout, or null for a valid layout. The
    // bounds protect the readers: the graph must fit in one direct buffer, and the chunk count of a
    // stream must stay low
    private static String check(int chunkSize, long payloadLength, long graphLength,
      long tombstonesLength) {
      if (chunkSize <= 0 || chunkSize > MAX_CHUNK_SIZE) {
        return "Chunk size " + chunkSize;
      }
      if (payloadLength < 0 || graphLength < 0 || tombstonesLength < 0) {
        return "Negative length";
      }
      if (graphLength > MAX_GRAPH_BYTES) {
        return "Graph length " + graphLength + " exceeds " + MAX_GRAPH_BYTES;
      }
      if (payloadLength > 0 ? graphLength > payloadLength - TRAILER_LENGTH : graphLength != 0) {
        return "Graph length " + graphLength + " in a payload of " + payloadLength + " bytes";
      }
      if (
        chunks(payloadLength, chunkSize) > MAX_CHUNKS
          || chunks(tombstonesLength, chunkSize) > MAX_CHUNKS
      ) {
        return "More than " + MAX_CHUNKS + " chunks of " + chunkSize + " bytes";
      }
      return null;
    }

    /** Returns the number of chunks of the given size that a stream of the given length needs. */
    public static long chunks(long length, int chunkSize) {
      return length / chunkSize + (length % chunkSize == 0 ? 0 : 1);
    }

    /** Decodes a layout cell value. Returns null if the value is not a valid layout. */
    public static Layout decode(byte[] bytes) {
      if (bytes == null || bytes.length != ENCODED_LENGTH || bytes[0] != FORMAT) {
        return null;
      }
      int chunkSize = Bytes.toInt(bytes, 1);
      long payloadLength = Bytes.toLong(bytes, 1 + Bytes.SIZEOF_INT);
      long graphLength = Bytes.toLong(bytes, 1 + Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG);
      long tombstonesLength = Bytes.toLong(bytes, 1 + Bytes.SIZEOF_INT + 2 * Bytes.SIZEOF_LONG);
      if (check(chunkSize, payloadLength, graphLength, tombstonesLength) != null) {
        return null;
      }
      return new Layout(chunkSize, payloadLength, graphLength, tombstonesLength);
    }

    /** Returns the value of the layout cell. */
    public byte[] encode() {
      byte[] bytes = new byte[ENCODED_LENGTH];
      bytes[0] = FORMAT;
      int offset = Bytes.putInt(bytes, 1, chunkSize);
      offset = Bytes.putLong(bytes, offset, payloadLength);
      offset = Bytes.putLong(bytes, offset, graphLength);
      Bytes.putLong(bytes, offset, tombstonesLength);
      return bytes;
    }

    /** Returns the length of the stream with the given chunk tag. */
    public long length(byte tag) {
      switch (tag) {
        case PAYLOAD_TAG:
          return payloadLength;
        case TOMBSTONES_TAG:
          return tombstonesLength;
        default:
          throw new IllegalArgumentException("Tag " + (char) tag + " has no chunks");
      }
    }

    /** Returns the number of chunks of the stream with the given chunk tag. */
    public int chunks(byte tag) {
      return (int) chunks(length(tag), chunkSize);
    }

    /** Returns the length of a chunk of the stream with the given chunk tag. */
    public int chunkLength(byte tag, int chunk) {
      long start = (long) chunk * chunkSize;
      long length = length(tag);
      if (chunk < 0 || start >= length) {
        throw new IllegalArgumentException(
          "Chunk " + chunk + " of a stream of " + length + " bytes");
      }
      return (int) Math.min(chunkSize, length - start);
    }

    /** Returns the length of the key mapping in the payload, or 0 if there is no payload. */
    public long mappingLength() {
      return payloadLength > 0 ? payloadLength - graphLength - TRAILER_LENGTH : 0;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Layout)) {
        return false;
      }
      Layout l = (Layout) o;
      return chunkSize == l.chunkSize && payloadLength == l.payloadLength
        && graphLength == l.graphLength && tombstonesLength == l.tombstonesLength;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(encode());
    }

    @Override
    public String toString() {
      return "Layout{chunkSize=" + chunkSize + ", payloadLength=" + payloadLength + ", graphLength="
        + graphLength + ", tombstonesLength=" + tombstonesLength + '}';
    }
  }

  /**
   * Returns true if the given base segments together cover all of the key range {@code [from, to)}.
   * An empty start key or end key has no limit on that side. A delta segment holds only the changes
   * of a flush, so it never covers a range or hides an older segment. This method ignores deltas.
   * @param from     the start key of the range, inclusive
   * @param to       the end key of the range, exclusive. An empty array has no upper limit.
   * @param segments the segments that can cover the range
   * @return true if the segments cover all of the range
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

  /**
   * Returns the qualifier of the small cell of a segment with the given tag.
   * @throws IllegalArgumentException if the tag is a chunk tag
   */
  public static byte[] qualifier(byte[] identity, byte tag) {
    if (isChunkTag(tag)) {
      throw new IllegalArgumentException("Tag " + (char) tag + " needs a chunk index");
    }
    byte[] qualifier = Arrays.copyOf(identity, identity.length + 1);
    qualifier[identity.length] = tag;
    return qualifier;
  }

  /** Returns the qualifier of a chunk cell of a segment, with the given index and chunk tag. */
  public static byte[] qualifier(byte[] identity, int chunk, byte tag) {
    if (!isChunkTag(tag) || chunk < 0) {
      throw new IllegalArgumentException("Tag " + (char) tag + " with chunk index " + chunk);
    }
    byte[] qualifier = Arrays.copyOf(identity, identity.length + Bytes.SIZEOF_INT + 1);
    Bytes.putInt(qualifier, identity.length, chunk);
    qualifier[qualifier.length - 1] = tag;
    return qualifier;
  }

  /** The parts of a segment cell qualifier. */
  public static final class Qualifier {
    public final byte[] indexKey;
    public final byte[] startKey;
    public final long time;
    /** The chunk index of a chunk cell, or -1 for a small cell. */
    public final int chunk;
    public final byte tag;

    private Qualifier(byte[] indexKey, byte[] startKey, long time, int chunk, byte tag) {
      this.indexKey = indexKey;
      this.startKey = startKey;
      this.time = time;
      this.chunk = chunk;
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
   * key length does not agree with the qualifier length, or if the tag is not a known tag. The tag
   * decides the form: a chunk tag always has a chunk index before it, and a small tag never has
   * one. A negative chunk index is malformed.
   */
  public static Qualifier parseQualifier(byte[] bytes, int offset, int length) {
    int end = offset + length;
    int zero = offset + indexKeyLength(bytes, offset, length);
    int start = zero + 1 + Bytes.SIZEOF_INT;
    if (zero == offset || start >= end) {
      return null;
    }
    byte tag = bytes[end - 1];
    boolean chunked = isChunkTag(tag);
    if (!chunked && !isSmallTag(tag)) {
      return null;
    }
    int suffix = (chunked ? Bytes.SIZEOF_INT : 0) + 1;
    int startLength = Bytes.toInt(bytes, zero + 1);
    if (startLength < 0 || startLength != end - start - Bytes.SIZEOF_LONG - suffix) {
      return null;
    }
    int chunk = chunked ? Bytes.toInt(bytes, end - suffix) : -1;
    if (chunked && chunk < 0) {
      return null;
    }
    return new Qualifier(Arrays.copyOfRange(bytes, offset, zero),
      Arrays.copyOfRange(bytes, start, start + startLength),
      Bytes.toLong(bytes, start + startLength), chunk, tag);
  }

  /**
   * Returns the length of the index key in a segment cell qualifier. The index key is the bytes
   * before the first 0x00. If the qualifier has no 0x00, the index key is the full qualifier.
   */
  public static int indexKeyLength(byte[] bytes, int offset, int length) {
    int key = 0;
    while (key < length && bytes[offset + key] != 0) {
      key++;
    }
    return key;
  }

  /** Returns true for the tag of a small cell. */
  public static boolean isSmallTag(byte tag) {
    return contains(SMALL_TAGS, tag);
  }

  /** Returns true for the tag of a chunk stream. */
  public static boolean isChunkTag(byte tag) {
    return contains(CHUNK_TAGS, tag);
  }

  private static boolean contains(byte[] tags, byte tag) {
    for (byte t : tags) {
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
   * Collects the segment cells of a scan of {@code V#} and decodes the well-formed segments. The
   * qualifiers of a well-formed segment decode. The segment has an E cell, an int N, a long R, a
   * digest D, a W cell and a valid L cell. A B cell is optional, and it must be a long. The N count
   * is positive exactly when the layout has a payload. A base has no tombstones. The cells must
   * also be in the anchor row of the key range. The listing ignores the chunk cells. It copies the
   * data that it keeps, so the scanner can release the cells.
   */
  public static final class Listing {
    private final Map<ImmutableBytesPtr, Cells> segments = new LinkedHashMap<>();

    // The cells of one segment in one row
    private static final class Cells {
      final byte[] row;
      final Qualifier qualifier;
      byte[] endKey;
      byte[] count;
      byte[] baseTime;
      byte[] replayFrom;
      byte[] definition;
      byte[] writer;
      byte[] layout;

      Cells(byte[] row, Qualifier qualifier) {
        this.row = row;
        this.qualifier = qualifier;
      }
    }

    void add(Cell cell) {
      Qualifier q = parseQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
      if (q == null || q.chunk >= 0) {
        return;
      }
      byte[] row = CellUtil.cloneRow(cell);
      Cells cells = segments.computeIfAbsent(key(row, q.identity()), k -> new Cells(row, q));
      byte[] value = CellUtil.cloneValue(cell);
      switch (q.tag) {
        case END_KEY_TAG:
          cells.endKey = value;
          break;
        case COUNT_TAG:
          cells.count = value;
          break;
        case BASE_TIME_TAG:
          cells.baseTime = value;
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
        case LAYOUT_TAG:
          cells.layout = value;
          break;
        default:
          break;
      }
    }

    /** Returns the key that identifies the cells of one segment in one row. */
    public static ImmutableBytesPtr key(byte[] row, byte[] identity) {
      return new ImmutableBytesPtr(Bytes.add(Bytes.toBytes(row.length), row, identity));
    }

    List<Descriptor> descriptors() {
      List<Descriptor> descriptors = new ArrayList<>(segments.size());
      for (Cells c : segments.values()) {
        Descriptor d = descriptor(c);
        if (d != null) {
          descriptors.add(d);
        }
      }
      return descriptors;
    }

    /**
     * Returns the row and the identity of each segment that the listing collected but that is not
     * well-formed. The descriptors of the listing do not include these segments.
     */
    List<byte[][]> malformed() {
      List<byte[][]> malformed = new ArrayList<>();
      for (Cells c : segments.values()) {
        if (descriptor(c) == null) {
          malformed.add(new byte[][] { c.row, c.qualifier.identity() });
        }
      }
      return malformed;
    }

    // Returns the descriptor of a well-formed segment, or null if the segment is not well-formed
    private static Descriptor descriptor(Cells c) {
      if (
        c.endKey == null || c.count == null || c.count.length != Bytes.SIZEOF_INT
          || c.baseTime != null && c.baseTime.length != Bytes.SIZEOF_LONG || c.replayFrom == null
          || c.replayFrom.length != Bytes.SIZEOF_LONG || c.definition == null
          || c.definition.length != DEFINITION_LENGTH || c.writer == null
          || !Bytes.equals(c.row, anchor(c.qualifier.startKey, c.endKey))
      ) {
        return null;
      }
      Layout layout = Layout.decode(c.layout);
      int count = Bytes.toInt(c.count);
      if (
        layout == null || count < 0 || (count > 0) != (layout.payloadLength > 0)
          || c.baseTime == null && layout.tombstonesLength > 0
      ) {
        return null;
      }
      return new Descriptor(c.qualifier.indexKey, c.qualifier.startKey, c.qualifier.time, c.endKey,
        count, c.baseTime != null ? Bytes.toLong(c.baseTime) : null, Bytes.toLong(c.replayFrom),
        c.definition, layout, c.writer);
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
   * Returns the vector distance that a JVector similarity score gives. The distance has the units
   * of the Phoenix distance function of the metric: Euclidean distance for L2, and cosine distance
   * for COSINE. JVector scores EUCLIDEAN as 1 / (1 + d^2), and COSINE as (1 + cosine) / 2.
   */
  public static double distance(VectorSimilarityFunction similarity, float score) {
    switch (similarity) {
      case EUCLIDEAN:
        return Math.sqrt(Math.max(0.0, 1.0 / score - 1.0));
      case COSINE:
        return 2.0 * (1.0 - score);
      default:
        throw new IllegalArgumentException("Unsupported vector similarity " + similarity);
    }
  }

  // The score slack of nearestDistance. It is much more than the rounding of the float sums of a
  // score, and much less than the distance margin of a normal bound
  static final float SCORE_SLACK = 0x1p-16f;

  /**
   * Returns a lower estimate of the vector distance that a JVector similarity score gives. JVector
   * calculates the score in float arithmetic. A score near 1 keeps few bits of a small distance,
   * and the float sums add rounding. Thus the method uses a score that is nearer to the query by
   * {@code SCORE_SLACK}. A small distance bound then does not stop a search because of rounding.
   */
  public static double nearestDistance(VectorSimilarityFunction similarity, float score) {
    return distance(similarity, Math.min(1f, score + SCORE_SLACK));
  }

  /**
   * Builds an HNSW graph from vectors and their row keys, and writes the graph into an off-heap
   * block. The build sorts the rows by row key, so the graph ordinals follow row key order. The
   * result holds the graph and the sorted keys, and streams the segment payload. The vectors and
   * the graph builder are not reachable after the return.
   * @param vi        the vector index metadata
   * @param vectors   the vectors
   * @param keys      the row keys of the vectors in the data table, in the same order
   * @param allocator the off-heap memory allocator of the graph
   * @return the built segment, which the caller closes
   * @throws TooLargeException if the graph is larger than a direct buffer or the off-heap budget
   * @throws IOException       if the serialization fails, or if the allocator cannot make space now
   */
  public static Built build(PTable.VectorIndex vi, RandomAccessVectorValues vectors, byte[][] keys,
    HnswOffheapAllocator allocator) throws IOException {
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
    Built built = null;
    try (GraphIndexBuilder builder =
      graphBuilder(vi, sorted, similarityFunction(vi.getDistanceMetric()), true)) {
      built = serialize(builder.build(sorted), sorted, vi, sortedKeys, allocator);
      return built;
    } catch (Throwable t) {
      // The close of the builder can fail after the result exists. The caller then gets no
      // result, so release its block here
      if (built != null) {
        built.close();
      }
      throw t;
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

  // Serializes the graph into an off-heap block. JVector writes the graph two times: first to
  // measure it, then into a direct buffer of the exact graph size. The key mapping and the trailer
  // are not encoded here. The payload stream of the result produces them from the sorted keys.
  private static Built serialize(ImmutableGraphIndex graph, RandomAccessVectorValues vectors,
    PTable.VectorIndex vi, byte[][] sortedKeys, HnswOffheapAllocator allocator) throws IOException {
    String quantization = vi.getQuantizationType();
    // PQ needs 256 vectors to train its centroids. A smaller segment stores the full vectors.
    if ("PQ".equals(quantization) && vectors.size() < 256) {
      quantization = "NONE";
    }
    List<Feature> features = new ArrayList<>(2);
    Map<FeatureId, IntFunction<Feature.State>> states = new EnumMap<>(FeatureId.class);
    Built built = null;
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
      long graphLength = counter.position();
      if (graphLength > MAX_GRAPH_BYTES) {
        throw new TooLargeException("HNSW graph of " + graphLength + " bytes exceeds the limit of "
          + MAX_GRAPH_BYTES + " bytes");
      }
      long mappingLength = KeyStream.length(sortedKeys);
      if (mappingLength > Integer.MAX_VALUE) {
        // The trailer records the mapping length as an int
        throw new TooLargeException("HNSW key mapping of " + mappingLength
          + " bytes exceeds the limit of " + Integer.MAX_VALUE + " bytes");
      }
      HnswOffheapAllocator.Block block = allocator.allocate((int) graphLength);
      try {
        ByteBufferIndexWriter out = new ByteBufferIndexWriter(block.buffer());
        writeGraph(graph, features, states, out);
        if (out.position() != graphLength) {
          throw new IllegalStateException(
            "HNSW graph of " + out.position() + " bytes was measured as " + graphLength + " bytes");
        }
        // The buffer has the exact graph size, so the full buffer is the graph
        block.buffer().clear();
        built = new Built(block, (int) graphLength, sortedKeys, mappingLength);
      } catch (Throwable t) {
        block.release();
        throw t;
      }
      return built;
    } catch (Throwable t) {
      // The close of the view can fail after the result exists. The caller then gets no result,
      // so release its block here
      if (built != null) {
        built.close();
      }
      throw t;
    }
  }

  private static void writeGraph(ImmutableGraphIndex graph, List<Feature> features,
    Map<FeatureId, IntFunction<Feature.State>> states, IndexWriter out) throws IOException {
    writeGraph(graph, features, states, out, ordinalMapper(graph));
  }

  /**
   * Returns the identity ordinal mapper if the graph has dense ordinals, or null for the JVector
   * default. A graph has dense ordinals when its ordinal bound is equal to its node count. The
   * default mapper renumbers the ordinals through a map of boxed integers. For dense ordinals, this
   * renumbering changes no ordinal, but it makes much garbage.
   */
  static OrdinalMapper ordinalMapper(ImmutableGraphIndex graph) {
    int size = graph.size();
    // The argument of the identity mapper is the largest ordinal
    return graph.getIdUpperBound() == size ? new OrdinalMapper.IdentityMapper(size - 1) : null;
  }

  // Writes the graph with the given ordinal mapper. A null mapper selects the JVector default
  static void writeGraph(ImmutableGraphIndex graph, List<Feature> features,
    Map<FeatureId, IntFunction<Feature.State>> states, IndexWriter out, OrdinalMapper mapper)
    throws IOException {
    OnDiskSequentialGraphIndexWriter.Builder writer =
      new OnDiskSequentialGraphIndexWriter.Builder(graph, out);
    for (Feature feature : features) {
      writer.with(feature);
    }
    if (mapper != null) {
      writer.withMapper(mapper);
    }
    try (OnDiskSequentialGraphIndexWriter w = writer.build()) {
      w.write(states);
    }
  }

  /**
   * The result of a {@link #build}: the graph in an off-heap block, and the sorted row keys. It
   * streams the segment payload for the write, and gives the graph and the keys to {@link #of}
   * without a copy. It holds one reference to the block until {@link #close()}. The budget of the
   * allocator counts the block until its last release. The block thus stays in the budget while the
   * write streams it, even if the allocator evicts the installed segment in that time.
   */
  public static final class Built implements Closeable {
    private final HnswOffheapAllocator.Block block;
    private final int graphLength;
    private final byte[][] keys;
    private final long mappingLength;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Built(HnswOffheapAllocator.Block block, int graphLength, byte[][] keys,
      long mappingLength) {
      this.block = block;
      this.graphLength = graphLength;
      this.keys = keys;
      this.mappingLength = mappingLength;
    }

    /** Returns the length of the payload. */
    public long length() {
      return graphLength + mappingLength + TRAILER_LENGTH;
    }

    /** Returns the length of the graph, the first part of the payload. */
    public int graphLength() {
      return graphLength;
    }

    /** Returns the number of vectors. */
    public int size() {
      return keys.length;
    }

    /**
     * Returns a new sequential stream of the payload bytes. The stream reads the graph from the
     * off-heap block, and encodes the key mapping and the trailer as it goes. Do not read it after
     * {@link #close()}.
     */
    public InputStream payload() {
      byte[] trailer = new byte[TRAILER_LENGTH];
      Bytes.putInt(trailer, Bytes.putInt(trailer, 0, (int) mappingLength), MAGIC);
      return new SequenceInputStream(
        Collections.enumeration(Arrays.asList(new BufferStream(block.buffer().asReadOnlyBuffer()),
          new KeyStream(keys), new ByteArrayInputStream(trailer))));
    }

    /** Releases the reference of this result to the off-heap block. */
    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        block.release();
      }
    }
  }

  // A sequential stream of the remaining bytes of a buffer
  private static final class BufferStream extends InputStream {
    private final ByteBuffer buffer;

    BufferStream(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public int read() {
      return buffer.hasRemaining() ? buffer.get() & 0xFF : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) {
        return 0;
      }
      if (!buffer.hasRemaining()) {
        return -1;
      }
      int n = Math.min(len, buffer.remaining());
      buffer.get(b, off, n);
      return n;
    }
  }

  /**
   * A sequential stream of encoded row keys, {@code [count (int)] ([key length (int)][key])*}. The
   * key mapping of a payload and the tombstones of a delta segment share this format. The stream
   * encodes one length header at a time, so it never holds a copy of the keys.
   */
  public static final class KeyStream extends InputStream {
    private final byte[][] keys;
    private final byte[] header = new byte[Bytes.SIZEOF_INT];
    // The position in the header. The header is complete at its length
    private int headerPosition;
    // The key of the header and of the key bytes, or -1 for the count header
    private int key = -1;
    // The position in the bytes of the current key
    private int keyPosition;

    public KeyStream(byte[][] keys) {
      this.keys = keys;
      Bytes.putInt(header, 0, keys.length);
    }

    /** Returns the length of the encoded keys. */
    public static long length(byte[][] keys) {
      long length = Bytes.SIZEOF_INT;
      for (byte[] k : keys) {
        length += Bytes.SIZEOF_INT + k.length;
      }
      return length;
    }

    @Override
    public int read() {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) {
        return 0;
      }
      int n = 0;
      while (n < len) {
        if (headerPosition < header.length) {
          int copy = Math.min(header.length - headerPosition, len - n);
          System.arraycopy(header, headerPosition, b, off + n, copy);
          headerPosition += copy;
          n += copy;
        } else if (key >= 0 && keyPosition < keys[key].length) {
          int copy = Math.min(keys[key].length - keyPosition, len - n);
          System.arraycopy(keys[key], keyPosition, b, off + n, copy);
          keyPosition += copy;
          n += copy;
        } else if (key + 1 < keys.length) {
          key++;
          keyPosition = 0;
          headerPosition = 0;
          Bytes.putInt(header, 0, keys[key].length);
        } else {
          break;
        }
      }
      return n == 0 ? -1 : n;
    }
  }

  // Decodes encoded row keys as they arrive in parts. The checks bound each allocation by the bytes
  // that remain. As a result, a corrupt count or key length cannot allocate more memory than the
  // stream holds.
  static final class KeyDecoder {
    private final byte[] identity;
    private final byte tag;
    private final long length;
    private final byte[] header = new byte[Bytes.SIZEOF_INT];
    private long consumed;
    private int headerPosition;
    private byte[][] keys;
    // The next key to decode
    private int key;
    // The key that receives bytes, or null while a header receives bytes
    private byte[] current;
    private int currentPosition;

    KeyDecoder(long length, byte[] identity, byte tag) {
      this.length = length;
      this.identity = identity;
      this.tag = tag;
    }

    // Takes the next part of the encoded keys
    void feed(byte[] b, int off, int len) throws CorruptSegmentException {
      if (consumed + len > length) {
        throw corrupt("more than " + length + " bytes of keys");
      }
      int end = off + len;
      while (off < end) {
        if (current != null) {
          int copy = Math.min(current.length - currentPosition, end - off);
          System.arraycopy(b, off, current, currentPosition, copy);
          currentPosition += copy;
          off += copy;
          consumed += copy;
          if (currentPosition == current.length) {
            keys[key++] = current;
            current = null;
          }
          continue;
        }
        if (keys != null && key == keys.length) {
          throw corrupt("bytes after " + keys.length + " keys");
        }
        int copy = Math.min(header.length - headerPosition, end - off);
        System.arraycopy(b, off, header, headerPosition, copy);
        headerPosition += copy;
        off += copy;
        consumed += copy;
        if (headerPosition < header.length) {
          continue;
        }
        headerPosition = 0;
        int value = Bytes.toInt(header);
        long remaining = length - consumed;
        if (keys == null) {
          // Each key uses at least the bytes of its length
          if (value < 0 || value > remaining / Bytes.SIZEOF_INT) {
            throw corrupt("key count " + value);
          }
          keys = new byte[value][];
        } else {
          if (value < 0 || value > remaining) {
            throw corrupt("key length " + value);
          }
          current = new byte[value];
          currentPosition = 0;
          if (value == 0) {
            keys[key++] = current;
            current = null;
          }
        }
      }
    }

    // Returns the keys. All bytes must have arrived, and they must hold exactly the keys.
    byte[][] finish() throws CorruptSegmentException {
      if (consumed != length || keys == null || key != keys.length || current != null) {
        throw corrupt("incomplete keys");
      }
      return keys;
    }

    private CorruptSegmentException corrupt(String detail) {
      return new CorruptSegmentException(identity, CorruptSegmentException.Kind.INVALID, tag, -1,
        detail, null);
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
   * Signals that a segment is missing, as after its retirement. A read of a chunk finds the L cell
   * absent. A search of a closed segment also throws this exception.
   */
  public static final class NotFoundException extends IOException {
    private static final long serialVersionUID = 1L;

    NotFoundException(byte[] identity, Object store) {
      super("HNSW segment " + Bytes.toStringBinary(identity) + " not found in " + store);
    }
  }

  /**
   * Signals that a listed segment cannot be read. The L cell is present, so no region retired the
   * segment. A retry fails the same way. The region leaves the segment out and rebuilds. The
   * message starts with "Corrupt HNSW segment" and the identity of the segment.
   */
  public static final class CorruptSegmentException extends DoNotRetryIOException {
    private static final long serialVersionUID = 1L;

    /** The kinds of corruption. */
    public enum Kind {
      /** A chunk cell is absent. */
      MISSING,
      /**
       * A chunk cell refers to a MOB file that is missing or corrupt. HBase gives an empty value.
       */
      UNRESOLVED,
      /** A chunk cell has a length that does not agree with the layout. */
      SHORT,
      /** The bytes of the segment do not decode. */
      INVALID
    }

    private final Kind kind;
    private final byte tag;
    private final int chunk;

    CorruptSegmentException(byte[] identity, Kind kind, byte tag, int chunk, String detail,
      Throwable cause) {
      super("Corrupt HNSW segment " + Bytes.toStringBinary(identity) + ": " + detail, cause);
      this.kind = kind;
      this.tag = tag;
      this.chunk = chunk;
    }

    public Kind getKind() {
      return kind;
    }

    /** Returns the tag of the stream that failed. */
    public byte getTag() {
      return tag;
    }

    /** Returns the index of the chunk that failed, or -1 if no single chunk failed. */
    public int getChunk() {
      return chunk;
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
   * Opens a segment from a segment store and loads its graph off-heap. The open reads the chunks in
   * order. It copies the graph bytes into a direct buffer, and decodes the key mapping as the bytes
   * arrive. If the graph is larger than the off-heap budget, the open reads only the chunks of the
   * key mapping. A search of that segment then fails with {@link TooLargeException}, but the row
   * keys are available. The open of a delta segment also reads its tombstones.
   * @param store      the segment store
   * @param descriptor the segment descriptor
   * @param allocator  the off-heap memory allocator
   * @param metric     the distance metric name
   * @return the open segment
   * @throws NotFoundException       if a region retired the segment
   * @throws CorruptSegmentException if a chunk is absent, unresolved or of the wrong length, or if
   *                                 the bytes do not decode
   * @throws IOException             if the read or the load of the segment fails
   */
  public static HnswSegment open(HnswSegmentStore store, Descriptor descriptor,
    HnswOffheapAllocator allocator, String metric) throws IOException {
    Layout layout = descriptor.layout;
    byte[][] tombstones = new byte[0][];
    if (layout.tombstonesLength > 0) {
      KeyDecoder decoder =
        new KeyDecoder(layout.tombstonesLength, descriptor.identity, TOMBSTONES_TAG);
      store.readChunks(descriptor, TOMBSTONES_TAG, 0, layout.chunks(TOMBSTONES_TAG), (i,
        cell) -> decoder.feed(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      tombstones = decoder.finish();
    }
    if (descriptor.count == 0) {
      return new HnswSegment(store, descriptor, allocator, similarityFunction(metric),
        new byte[0][], tombstones);
    }
    long loadStart = EnvironmentEdgeManager.currentTimeMillis();
    HnswOffheapAllocator.Block block = null;
    TooLargeException tooLarge = null;
    try {
      block = allocator.allocate((int) layout.graphLength);
    } catch (TooLargeException e) {
      // Writes need only the row keys. A search fails the same way, and does not read the payload
      tooLarge = e;
    }
    try {
      KeyDecoder mapping = new KeyDecoder(layout.mappingLength(), descriptor.identity, PAYLOAD_TAG);
      byte[] trailer = new byte[TRAILER_LENGTH];
      int first = block != null ? 0 : (int) (layout.graphLength / layout.chunkSize);
      readPayload(store, descriptor, first, layout.chunks(PAYLOAD_TAG),
        block != null ? block.buffer() : null, mapping, trailer);
      checkTrailer(descriptor, trailer);
      HnswSegment segment = new HnswSegment(store, descriptor, allocator,
        similarityFunction(metric), mapping.finish(), tombstones);
      if (block == null) {
        segment.tooLarge = tooLarge;
        return segment;
      }
      HnswOffheapAllocator.Block graph = block;
      // The install takes the reference of the caller, and releases it on a failure
      block = null;
      graph.buffer().flip();
      segment.install(graph, loadStart).block.release();
      return segment;
    } finally {
      if (block != null) {
        block.release();
      }
    }
  }

  // Reads the payload chunks [first, last) in order. The bytes below the graph length go into the
  // graph buffer, if there is one. The bytes of the key mapping go to the decoder, and the trailer
  // bytes go into the trailer array, if there is one. The store reads the chunks in groups. Each
  // group is unreachable after its copy
  private static void readPayload(HnswSegmentStore store, Descriptor d, int first, int last,
    ByteBuffer graph, KeyDecoder mapping, byte[] trailer) throws IOException {
    Layout layout = d.layout;
    long graphEnd = layout.graphLength;
    long mappingEnd = graphEnd + layout.mappingLength();
    store.readChunks(d, PAYLOAD_TAG, first, last, (i, cell) -> {
      byte[] bytes = cell.getValueArray();
      int offset = cell.getValueOffset();
      long start = (long) i * layout.chunkSize;
      long end = start + cell.getValueLength();
      if (graph != null && start < graphEnd) {
        graph.put(bytes, offset, (int) (Math.min(end, graphEnd) - start));
      }
      if (mapping != null && end > graphEnd && start < mappingEnd) {
        long from = Math.max(start, graphEnd);
        mapping.feed(bytes, offset + (int) (from - start),
          (int) (Math.min(end, mappingEnd) - from));
      }
      if (trailer != null && end > mappingEnd) {
        long from = Math.max(start, mappingEnd);
        System.arraycopy(bytes, offset + (int) (from - start), trailer, (int) (from - mappingEnd),
          (int) (end - from));
      }
    });
  }

  // Checks that the trailer agrees with the layout. The trailer gives the mapping length and ends
  // the payload with the magic.
  private static void checkTrailer(Descriptor d, byte[] trailer) throws CorruptSegmentException {
    int mapping = Bytes.toInt(trailer, 0);
    if (Bytes.toInt(trailer, Bytes.SIZEOF_INT) != MAGIC || mapping != d.layout.mappingLength()) {
      throw new CorruptSegmentException(d.identity, CorruptSegmentException.Kind.INVALID,
        PAYLOAD_TAG, -1, "the payload trailer does not agree with the layout", null);
    }
  }

  /**
   * Returns a segment that this server writes, loaded from the result of {@link #build}. It does
   * not read the segment store. The segment uses the keys of the result, and takes its own
   * reference to the graph block. The result stays usable for the write, and the caller still
   * closes it. A null result gives a segment with no vectors. A reload after eviction reads the
   * payload from {@code store}, so do not search the segment until the store holds its cells.
   */
  public static HnswSegment of(HnswSegmentStore store, Descriptor descriptor,
    HnswOffheapAllocator allocator, String metric, Built built, byte[][] tombstones)
    throws IOException {
    if (built == null) {
      return new HnswSegment(store, descriptor, allocator, similarityFunction(metric),
        new byte[0][], tombstones);
    }
    if (built.closed.get() || !built.block.retain()) {
      throw new IllegalStateException("The built HNSW segment is closed");
    }
    long loadStart = EnvironmentEdgeManager.currentTimeMillis();
    HnswSegment segment = new HnswSegment(store, descriptor, allocator, similarityFunction(metric),
      built.keys, tombstones);
    segment.install(built.block, loadStart).block.release();
    return segment;
  }

  // Loads the graph of a filled block and makes it resident. The buffer of the block holds exactly
  // the graph. The method takes the reference of the caller and returns it in the result, so the
  // caller releases it. On a failure, the method releases it. The load time starts at loadStart, so
  // it includes the read of the payload.
  private Resident install(HnswOffheapAllocator.Block block, long loadStart) throws IOException {
    Resident r;
    try {
      ByteBuffer graphBytes = block.buffer().asReadOnlyBuffer();
      r = new Resident(block,
        OnDiskGraphIndex.load(() -> new ByteBufferReader(graphBytes.duplicate())));
    } catch (RuntimeException e) {
      block.release();
      // JVector throws a RuntimeException for a graph that it cannot read. The failure is
      // permanent, because a new read of the payload fails the same way.
      throw new CorruptSegmentException(descriptor.identity, CorruptSegmentException.Kind.INVALID,
        PAYLOAD_TAG, -1, "the graph does not load", e);
    } catch (Throwable t) {
      block.release();
      throw t;
    }
    resident.set(r);
    allocator.commit(this, block, () -> resident.compareAndSet(r, null), loadStart);
    if (closed) {
      // close() can run between the publish and the commit, and then not see the resident block
      resident.compareAndSet(r, null);
      allocator.release(this);
    }
    return r;
  }

  // Encodes row keys in the format of KeyStream, as one array
  @VisibleForTesting
  static byte[] encodeKeys(byte[][] keys) throws IOException {
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

  // Returns the resident graph with a reference that the caller releases. If eviction dropped the
  // graph, this method reads the graph chunks again. It does not read the key mapping.
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
          long loadStart = EnvironmentEdgeManager.currentTimeMillis();
          Layout layout = descriptor.layout;
          HnswOffheapAllocator.Block block;
          try {
            block = allocator.allocate((int) layout.graphLength);
          } catch (TooLargeException e) {
            tooLarge = e;
            throw e;
          }
          try {
            readPayload(store, descriptor, 0,
              (int) Layout.chunks(layout.graphLength, layout.chunkSize), block.buffer(), null,
              null);
            block.buffer().flip();
          } catch (Throwable t) {
            block.release();
            throw t;
          }
          r = install(block, loadStart);
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

  /** Returns the deleted row keys that this segment records. Only a delta segment records them. */
  public byte[][] getTombstones() {
    return tombstones;
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
