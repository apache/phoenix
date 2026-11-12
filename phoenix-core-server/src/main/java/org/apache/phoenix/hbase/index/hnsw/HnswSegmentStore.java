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

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Reads and writes the cells of HNSW segments in the hidden family {@code V#} of a data table.
 * {@link HnswSegment} describes the cell layout. A listing reads only the small cells of the
 * segments. It never reads a chunk cell or a MOB file. A chunk read gets a group of consecutive
 * chunks with resolved MOB references, and the L cell of the segment, in one get. A group holds at
 * most {@link #BATCH_BYTES} of chunks, and at least one chunk. If the L cell is absent, the region
 * retired the segment after the listing, and the read throws {@link HnswSegment.NotFoundException}.
 * If the L cell is present and a chunk is absent, unresolved or of the wrong length, the read
 * throws {@link HnswSegment.CorruptSegmentException}. HBase gives an empty value for a chunk whose
 * MOB file is missing or corrupt.
 * <p>
 * {@link RegionStore} serves the RegionServer. Its reads and writes are calls on the region, and
 * make no RPC. {@link TableStore} serves client tools, and only reads.
 */
public abstract class HnswSegmentStore {
  private static final Logger LOG = LoggerFactory.getLogger(HnswSegmentStore.class);
  /** The hidden column family of the segment cells. */
  static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  /**
   * The most chunk bytes in one Put of a write or in one get of a read, 8 MiB. Groups of chunks
   * need fewer region operations than single chunks. Each chunk keeps its own array, so a region
   * operation on a group makes no large array.
   */
  public static final int BATCH_BYTES = 8 * 1024 * 1024;

  // The most chunk bytes in one Put or one get of this store
  final int batchBytes;

  HnswSegmentStore() {
    this(BATCH_BYTES);
  }

  // Creates a store with the given group size in bytes. Tests use small groups
  HnswSegmentStore(int batchBytes) {
    if (batchBytes <= 0) {
      throw new IllegalArgumentException("Batch size " + batchBytes);
    }
    this.batchBytes = batchBytes;
  }

  /**
   * Receives the chunk cells of a read, in chunk order. The cell is valid only during the call.
   */
  @FunctionalInterface
  public interface ChunkConsumer {
    void accept(int chunk, Cell cell) throws IOException;
  }

  /**
   * Returns the descriptors of the well-formed segments of an index in this store. The listing
   * reads the small cells of the index as stored. It never returns a chunk cell, and never reads a
   * MOB file.
   */
  public List<HnswSegment.Descriptor> list(byte[] indexKey) throws IOException {
    return list(indexKey, new ArrayList<>());
  }

  /**
   * Lists the segments of an index as {@link #list(byte[])} does. The method also adds to
   * {@code malformed} the row and the identity of each segment that has small cells but is not
   * well-formed. The listing does not read chunk cells, so it does not find chunks without small
   * cells.
   */
  public List<HnswSegment.Descriptor> list(byte[] indexKey, List<byte[][]> malformed)
    throws IOException {
    HnswSegment.Listing listing = new HnswSegment.Listing();
    scan(listScan(indexKey), listing::add);
    malformed.addAll(listing.malformed());
    return listing.descriptors();
  }

  /**
   * Gives each cell of {@code V#}, for all indexes, to the consumer. Each cell has the length of
   * its value in place of the value. The scan never reads a MOB file. It never transfers a payload,
   * also when the payload is in the memstore. The method keeps no cell, so its memory use does not
   * increase with the number of cells.
   */
  public void listCells(Consumer<Cell> consumer) throws IOException {
    Scan scan = new Scan().addFamily(FAMILY).setFilter(new KeyOnlyFilter(true));
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    scan(scan, consumer);
  }

  /**
   * Returns the keys of the indexes that have cells in {@code V#}. A key is an index name without
   * view prefixes. The listing does not read payloads, and it ignores cells without a key.
   */
  public Set<String> indexKeys() throws IOException {
    Set<String> keys = new HashSet<>();
    listCells(cell -> {
      int key = HnswSegment.indexKeyLength(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
      if (key > 0 && key < cell.getQualifierLength()) {
        keys.add(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), key));
      }
    });
    return keys;
  }

  /**
   * Returns the length of the payload of a segment, from its layout. The method reads nothing.
   */
  public long payloadSize(HnswSegment.Descriptor d) {
    return d.layout.payloadLength;
  }

  /**
   * Returns one chunk cell of a segment, with the MOB reference resolved. The read is the same as
   * {@link #readChunks} for one chunk.
   * @param d     the segment
   * @param tag   the chunk tag of the stream
   * @param chunk the chunk index
   * @return the chunk cell, with the length that the layout gives
   * @throws IllegalArgumentException            if the stream has no such chunk
   * @throws HnswSegment.NotFoundException       if the L cell is absent
   * @throws HnswSegment.CorruptSegmentException if the chunk is absent, empty or of the wrong
   *                                             length, or if the L cell changed
   */
  public Cell readChunk(HnswSegment.Descriptor d, byte tag, int chunk) throws IOException {
    d.layout.chunkLength(tag, chunk);
    Cell[] cell = new Cell[1];
    readChunks(d, tag, chunk, chunk + 1, (i, c) -> cell[0] = c);
    return cell[0];
  }

  /**
   * Reads the chunks [from, to) of a stream in order, and gives each chunk to the consumer. One get
   * reads a group of consecutive chunks and the L cell of the segment. One get of one row is atomic
   * against a retirement, so an absent L cell always means a retired segment. The read checks the L
   * cell of each group. It checks each chunk before it gives the chunk to the consumer. The result
   * of a group is unreachable after the consumer receives its chunks. Thus the heap holds the
   * chunks of at most one group. The read sets {@link MobConstants#EMPTY_VALUE_ON_MOBCELL_MISS}.
   * With this attribute, HBase gives an empty value for a reference whose MOB file is missing or
   * corrupt. This occurs on the region path and on the RPC path.
   * @param d        the segment
   * @param tag      the chunk tag of the stream
   * @param from     the first chunk
   * @param to       the chunk after the last chunk
   * @param consumer receives each chunk cell, with the length that the layout gives
   * @throws IllegalArgumentException            if the range is not in the stream
   * @throws HnswSegment.NotFoundException       if the L cell is absent
   * @throws HnswSegment.CorruptSegmentException if a chunk is absent, empty or of the wrong length,
   *                                             or if the L cell changed
   */
  public void readChunks(HnswSegment.Descriptor d, byte tag, int from, int to,
    ChunkConsumer consumer) throws IOException {
    int chunks = d.layout.chunks(tag);
    if (from < 0 || from > to || to > chunks) {
      throw new IllegalArgumentException(
        "Chunks [" + from + ", " + to + ") of a stream of " + chunks + " chunks");
    }
    int group = Math.max(1, batchBytes / d.layout.chunkSize);
    for (int first = from; first < to; first += group) {
      readGroup(d, tag, first, Math.min(to, first + group), consumer);
    }
  }

  // Reads the chunks [first, end) and the L cell in one get, checks them, and gives the chunks to
  // the consumer in order. The result is local to this method, so the group is unreachable after
  // the method returns
  private void readGroup(HnswSegment.Descriptor d, byte tag, int first, int end,
    ChunkConsumer consumer) throws IOException {
    byte[] layout = HnswSegment.qualifier(d.identity, HnswSegment.LAYOUT_TAG);
    byte[][] qualifiers = new byte[end - first][];
    Get get = new Get(d.anchor);
    for (int i = first; i < end; i++) {
      qualifiers[i - first] = HnswSegment.qualifier(d.identity, i, tag);
      get.addColumn(FAMILY, qualifiers[i - first]);
    }
    get.addColumn(FAMILY, layout);
    get.setAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS, Bytes.toBytes(true));
    Result result = get(get);
    Cell layoutCell = result.getColumnLatestCell(FAMILY, layout);
    if (layoutCell == null) {
      throw new HnswSegment.NotFoundException(d.identity, this);
    }
    if (!d.layout.equals(HnswSegment.Layout.decode(CellUtil.cloneValue(layoutCell)))) {
      throw corrupt(d, HnswSegment.CorruptSegmentException.Kind.INVALID, tag, first,
        "the layout cell changed");
    }
    for (int chunk = first; chunk < end; chunk++) {
      int expected = d.layout.chunkLength(tag, chunk);
      Cell cell = result.getColumnLatestCell(FAMILY, qualifiers[chunk - first]);
      if (cell == null) {
        throw corrupt(d, HnswSegment.CorruptSegmentException.Kind.MISSING, tag, chunk,
          "chunk " + chunk + " of " + (char) tag + " is absent");
      }
      if (cell.getValueLength() == 0) {
        throw corrupt(d, HnswSegment.CorruptSegmentException.Kind.UNRESOLVED, tag, chunk,
          "chunk " + chunk + " of " + (char) tag + " refers to a MOB file that does not exist");
      }
      if (cell.getValueLength() != expected) {
        throw corrupt(d, HnswSegment.CorruptSegmentException.Kind.SHORT, tag, chunk,
          "chunk " + chunk + " of " + (char) tag + " has " + cell.getValueLength() + " bytes, not "
            + expected);
      }
      consumer.accept(chunk, cell);
    }
  }

  /**
   * Writes the payload of a segment to the stream, one group of chunks at a time. The heap holds
   * the chunks of at most one group.
   * @throws HnswSegment.NotFoundException       if a region retired the segment
   * @throws HnswSegment.CorruptSegmentException if a chunk cannot be read
   */
  public void copyPayload(HnswSegment.Descriptor d, OutputStream out) throws IOException {
    readChunks(d, HnswSegment.PAYLOAD_TAG, 0, d.layout.chunks(HnswSegment.PAYLOAD_TAG),
      (i, cell) -> out.write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
  }

  private static HnswSegment.CorruptSegmentException corrupt(HnswSegment.Descriptor d,
    HnswSegment.CorruptSegmentException.Kind kind, byte tag, int chunk, String detail) {
    return new HnswSegment.CorruptSegmentException(d.identity, kind, tag, chunk, detail, null);
  }

  // Returns a scan of all cells of an index. The scan returns MOB reference cells as stored, and
  // reads no MOB file.
  static Scan indexScan(byte[] indexKey) {
    Scan scan =
      new Scan().addFamily(FAMILY).setFilter(new ColumnPrefixFilter(HnswSegment.prefix(indexKey)));
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    return scan;
  }

  // Returns a scan of the cells of an index that a listing decodes. A filter on the server keeps
  // only the qualifiers that end with a small tag. It removes the chunk cells, which can be large.
  // MOB_SCAN_RAW has no effect on a cell in the memstore, because the memstore holds the full
  // value until the flush.
  static Scan listScan(byte[] indexKey) {
    RegexStringComparator tags =
      new RegexStringComparator("[" + Bytes.toString(HnswSegment.SMALL_TAGS) + "]\\z");
    // ISO-8859-1 maps each byte to one char, so the pattern matches the last byte of the qualifier
    tags.setCharset(StandardCharsets.ISO_8859_1);
    Scan scan = indexScan(indexKey);
    return scan.setFilter(
      new FilterList(scan.getFilter(), new QualifierFilter(CompareOperator.EQUAL, tags)));
  }

  // Gives each cell of the scan to the consumer. The consumer copies the data that it keeps.
  abstract void scan(Scan scan, Consumer<Cell> consumer) throws IOException;

  abstract Result get(Get get) throws IOException;

  /**
   * The segment store of one region. It reads and writes the {@code V#} cells of the region with
   * region operations.
   * <p>
   * A segment write streams the chunks of the payload and of the tombstones. The chunks except the
   * last of each stream go in Puts of consecutive chunks. A Put holds at most {@link #BATCH_BYTES}
   * of chunks, and at least one chunk. Then one commit Put holds the small cells, the L cell and
   * the last chunk of each stream. A listing reads only the small cells, so it sees a segment only
   * after all of its chunks are in the region. A small segment is one Put.
   * <p>
   * Segment writes and retirements skip the WAL, because segments are derived state. If a crash
   * loses a segment write, the result is the same as a graph flush that did not occur. The region
   * then replays the data rows from the replay start of the remaining segments. One thread writes
   * the Puts of a segment in order, and a memstore flush captures all of {@code V#} at one time.
   * Thus a flush that holds a commit Put also holds all earlier chunks that are not yet in store
   * files. A retirement always comes after the write of the segment that replaces the retired
   * segments. As a result, a crash never keeps a retirement without its replacement. A purge uses
   * the WAL.
   * <p>
   * The memstore gate keeps the {@code V#} memstore below a window during a write. The window is
   * the larger of 64 MiB and one eighth of the payload, in whole chunks. Before a Put that would
   * pass the window, the write asks for a flush of {@code V#}. It then waits until the flush of the
   * cells that the memstore held at that time ends. It does not wait for an empty memstore, because
   * other writers can keep the memstore busy. Thus the memstore holds a small part of a large
   * payload, and a base write makes about eight extra store files and MOB files.
   * <p>
   * The timestamp of each segment cell is the segment time. A retirement or a purge deletes each
   * cell at its own timestamp. As a result, the delete masks the cell also when the clock of this
   * server is behind the clock of the writer.
   * <p>
   * The mutations take the row locks of the anchor rows. For this reason, the caller must not hold
   * a lock that a data write holds while it waits for row locks.
   */
  public static class RegionStore extends HnswSegmentStore {
    // The time to wait before the store repeats a flush request or checks the memstore again
    private static final long FLUSH_RETRY_MS = 100;
    // The HBase setting for the longest delay of a queued flush while a store of the region has
    // too many files, and its HBase default
    private static final String BLOCKING_WAIT_TIME = "hbase.hstore.blockingWaitTime";
    private static final long DEFAULT_BLOCKING_WAIT_TIME = 90_000;
    // The smallest memstore window of a write
    private static final long MIN_WINDOW = 64L * 1024 * 1024;
    // The part of the payload that the memstore window allows
    private static final int WINDOW_DIVISOR = 8;

    private final RegionCoprocessorEnvironment env;
    private final Region region;
    private final int chunkSize;

    public RegionStore(RegionCoprocessorEnvironment env) {
      this(env, HnswSegment.CHUNK_SIZE);
    }

    /** Creates a store that writes chunks of the given size. Tests use small chunks. */
    @VisibleForTesting
    public RegionStore(RegionCoprocessorEnvironment env, int chunkSize) {
      this(env, chunkSize, BATCH_BYTES);
    }

    /**
     * Creates a store that writes chunks of the given size, and groups chunks up to the given
     * number of bytes. Tests use small chunks and small groups.
     */
    @VisibleForTesting
    public RegionStore(RegionCoprocessorEnvironment env, int chunkSize, int batchBytes) {
      super(batchBytes);
      this.env = env;
      this.region = env.getRegion();
      this.chunkSize = chunkSize;
    }

    @Override
    void scan(Scan scan, Consumer<Cell> consumer) throws IOException {
      try (RegionScanner scanner = region.getScanner(scan)) {
        List<Cell> cells = new ArrayList<>();
        boolean more;
        do {
          cells.clear();
          more = scanner.next(cells);
          for (Cell cell : cells) {
            consumer.accept(cell);
          }
        } while (more);
      }
    }

    @Override
    Result get(Get get) throws IOException {
      return region.get(get);
    }

    /**
     * Returns the layout of a new segment with the chunk size of this store.
     * @throws HnswSegment.TooLargeException if a stream needs too many chunks
     */
    public HnswSegment.Layout layout(long payloadLength, long graphLength, long tombstonesLength)
      throws HnswSegment.TooLargeException {
      if (
        HnswSegment.Layout.chunks(payloadLength, chunkSize) > HnswSegment.MAX_CHUNKS
          || HnswSegment.Layout.chunks(tombstonesLength, chunkSize) > HnswSegment.MAX_CHUNKS
      ) {
        throw new HnswSegment.TooLargeException("HNSW segment with a payload of " + payloadLength
          + " bytes and tombstones of " + tombstonesLength + " bytes needs more than "
          + HnswSegment.MAX_CHUNKS + " chunks of " + chunkSize + " bytes");
      }
      return new HnswSegment.Layout(chunkSize, payloadLength, graphLength, tombstonesLength);
    }

    /**
     * Writes the cells of a segment to its anchor row without the WAL. The streams give the bytes
     * of the payload and of the tombstones, with the lengths of the layout of the descriptor. A
     * null stream is an empty stream. The write streams the chunks, and the commit Put comes last.
     * The W cell holds the encoded name of this region, for a base and for a delta. A delta segment
     * also records its base time.
     * <p>
     * A base write that fails before the commit Put deletes its chunks, unless the region closes. A
     * failed delta keeps its cells. An abort is not a failure. It throws
     * {@link AbortedWriteException}, and the caller deletes the chunks. A failure while the abort
     * gives true is also an abort. If a Put meets a busy region, the write asks for a flush and
     * repeats the same Put. A repeated chunk Put is safe, because it has the same row, the same
     * qualifiers and the same timestamp.
     * <p>
     * After the commit, the write asks for a flush of {@code V#}, so the chunks move to MOB files.
     * A base write waits for the flush. A delta write asks once and does not wait. One thread
     * writes the deltas of all regions of the server. HBase can delay the flush of a region for a
     * long time, for example while a store of the region has too many files. A wait would then
     * delay the deltas of all regions. No retirement depends on a delta. If a crash loses a delta
     * before its flush, the region replays from the replay start of the segments that remain. The
     * loss of a base has the same result. HBase does not do the requested flush while another flush
     * of the region is queued or runs. A later flush of {@code V#} then writes the delta, for
     * example the flush of the next base write.
     * @param d          the segment, with its layout
     * @param payload    the payload bytes, or null
     * @param tombstones the encoded tombstones, or null
     * @param abort      gives true when the write must stop. The write checks it before each Put.
     * @return the time in milliseconds that the write waited for the final flush. It is 0 for a
     *         delta.
     * @throws AbortedWriteException if {@code abort} gave true before the commit
     */
    public long write(HnswSegment.Descriptor d, InputStream payload, InputStream tombstones,
      BooleanSupplier abort) throws IOException {
      HnswSegment.Layout layout = d.layout;
      long window = window(layout);
      try {
        Put commit = new Put(d.anchor, d.time)
          .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG), d.endKey)
          .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG),
            Bytes.toBytes(d.count))
          .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG),
            Bytes.toBytes(d.replayFrom))
          .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG),
            d.definition)
          .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG),
            region.getRegionInfo().getEncodedNameAsBytes())
          .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.LAYOUT_TAG),
            layout.encode());
        if (d.isDelta()) {
          commit.addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.BASE_TIME_TAG),
            Bytes.toBytes(d.baseTime));
        }
        long tails = stream(d, HnswSegment.PAYLOAD_TAG, payload, commit, window, abort)
          + stream(d, HnswSegment.TOMBSTONES_TAG, tombstones, commit, window, abort);
        checkAbort(d, abort);
        gate(tails, window, d, abort);
        commit.setDurability(Durability.SKIP_WAL);
        put(commit, d, abort);
      } catch (AbortedWriteException e) {
        throw e;
      } catch (IOException | RuntimeException | Error e) {
        // A Put fails while the region closes. If the caller stops the write at that time, the
        // failure is an abort
        if (!(e instanceof Error) && abort != null && abort.getAsBoolean()) {
          throw new AbortedWriteException("The write of HNSW segment "
            + Bytes.toStringBinary(d.identity) + " to " + this + " was aborted", e);
        }
        deletePartialWrite(d, e);
        throw e;
      }
      if (!d.isDelta()) {
        long start = EnvironmentEdgeManager.currentTimeMillis();
        flush();
        return EnvironmentEdgeManager.currentTimeMillis() - start;
      }
      if (region.getStore(FAMILY).getFlushableSize().getDataSize() > 0) {
        requestFlush(Collections.singletonList(FAMILY));
      }
      return 0;
    }

    // Writes the chunks of a stream except the last in Puts of consecutive chunks. The last chunk
    // goes into the commit Put. A Put holds at most batchBytes of chunks, and at least one chunk.
    // The write checks the abort and the memstore gate before each Put. Each chunk is a new array
    // of the exact chunk length. The write never changes or uses a chunk array again, so a Put
    // stays valid after the region applies it. Returns the length of the last chunk.
    private long stream(HnswSegment.Descriptor d, byte tag, InputStream in, Put commit, long window,
      BooleanSupplier abort) throws IOException {
      int chunks = d.layout.chunks(tag);
      if (chunks == 0) {
        return 0;
      }
      if (in == null) {
        throw new IllegalArgumentException(
          "No stream for " + d.layout.length(tag) + " bytes of " + (char) tag);
      }
      int group = Math.max(1, batchBytes / d.layout.chunkSize);
      for (int first = 0; first < chunks - 1; first += group) {
        int end = Math.min(chunks - 1, first + group);
        checkAbort(d, abort);
        long bytes = 0;
        for (int i = first; i < end; i++) {
          bytes += d.layout.chunkLength(tag, i);
        }
        gate(bytes, window, d, abort);
        Put put = new Put(d.anchor, d.time);
        for (int i = first; i < end; i++) {
          put.add(chunkCell(d, tag, i, read(in, d, tag, i)));
        }
        put.setDurability(Durability.SKIP_WAL);
        put(put, d, abort);
      }
      byte[] last = read(in, d, tag, chunks - 1);
      if (in.read() != -1) {
        throw new IllegalStateException(
          "The stream of " + (char) tag + " is longer than " + d.layout.length(tag) + " bytes");
      }
      commit.add(chunkCell(d, tag, chunks - 1, last));
      return last.length;
    }

    // Reads one chunk of a stream into a new array
    private static byte[] read(InputStream in, HnswSegment.Descriptor d, byte tag, int chunk)
      throws IOException {
      byte[] bytes = new byte[d.layout.chunkLength(tag, chunk)];
      int n = 0;
      while (n < bytes.length) {
        int r = in.read(bytes, n, bytes.length - n);
        if (r < 0) {
          throw new IllegalStateException(
            "The stream of " + (char) tag + " is shorter than " + d.layout.length(tag) + " bytes");
        }
        n += r;
      }
      return bytes;
    }

    // Returns a chunk cell. The cell refers to the chunk array and does not copy it, so the array
    // must not change after the Put.
    private static Cell chunkCell(HnswSegment.Descriptor d, byte tag, int chunk, byte[] value) {
      return CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(d.anchor)
        .setFamily(FAMILY).setQualifier(HnswSegment.qualifier(d.identity, chunk, tag))
        .setTimestamp(d.time).setType(Cell.Type.Put).setValue(value).build();
    }

    // Returns the memstore window of a write: the larger of MIN_WINDOW and one eighth of the
    // payload, rounded up to whole chunks. A larger payload thus makes a bounded number of extra
    // store files, and stays below the store file limit of a region
    private static long window(HnswSegment.Layout layout) {
      long window =
        Math.max(MIN_WINDOW, HnswSegment.Layout.chunks(layout.payloadLength, WINDOW_DIVISOR));
      return HnswSegment.Layout.chunks(window, layout.chunkSize) * layout.chunkSize;
    }

    // Waits until the V# memstore has space for the given bytes in the window. If it has no space,
    // the method asks for a flush, and waits until the store flushed as many bytes as the memstore
    // held. The flush of the cells that the memstore held then ended. Other writers can add cells
    // during the wait, so the method does not wait for an empty memstore. The bound is soft: a
    // write that races with the check can add one more Put.
    private void gate(long bytes, long window, HnswSegment.Descriptor d, BooleanSupplier abort)
      throws IOException {
      Store store = region.getStore(FAMILY);
      long held = memStoreSize(store);
      if (held + bytes <= window) {
        return;
      }
      // Read the flushed bytes after the memstore size. A flush between the two reads then makes
      // the wait longer, never shorter
      long flushed = store.getFlushedCellsSize();
      awaitFlush(() -> store.getFlushedCellsSize() - flushed >= held
        || memStoreSize(store) + bytes <= window, d, abort);
    }

    // Returns the data size of the V# memstore: the active cells and the snapshot of a flush
    private static long memStoreSize(Store store) {
      return store.getMemStoreSize().getDataSize() + store.getSnapshotSize().getDataSize();
    }

    // Applies one Put. A busy region refuses the Put while its memstore is above the blocking
    // limit. The method then asks for a flush of V# and repeats the Put, for up to twice the
    // blocking wait time of a queued flush.
    private void put(Put put, HnswSegment.Descriptor d, BooleanSupplier abort) throws IOException {
      long start = System.nanoTime();
      for (;;) {
        try {
          mutate(Collections.singletonList(put));
          return;
        } catch (RegionTooBusyException e) {
          long limit = TimeUnit.MILLISECONDS.toNanos(
            2 * env.getConfiguration().getLong(BLOCKING_WAIT_TIME, DEFAULT_BLOCKING_WAIT_TIME));
          if (System.nanoTime() - start > limit || region.isClosing() || region.isClosed()) {
            throw e;
          }
          checkAbort(d, abort);
          requestFlush(Collections.singletonList(FAMILY));
          try {
            Thread.sleep(FLUSH_RETRY_MS);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while writing HNSW segment "
              + Bytes.toStringBinary(d.identity) + " to " + this);
          }
        }
      }
    }

    private void checkAbort(HnswSegment.Descriptor d, BooleanSupplier abort)
      throws AbortedWriteException {
      if (abort != null && abort.getAsBoolean()) {
        throw new AbortedWriteException("The write of HNSW segment "
          + Bytes.toStringBinary(d.identity) + " to " + this + " was aborted");
      }
    }

    // Deletes the cells of a base write that failed before its commit, unless the region closes.
    // The delete is a best effort. The method logs a delete failure and adds it to the write
    // failure. The removal of malformed cells later deletes the chunks that stay. A failed delta
    // keeps its cells, because its retry uses the same identity and time. A delete marker at that
    // time hides the cells of the retry until a major compaction. The retry writes the same cells
    // again
    private void deletePartialWrite(HnswSegment.Descriptor d, Throwable failure) {
      if (d.isDelta() || region.isClosing() || region.isClosed()) {
        return;
      }
      try {
        retire(Collections.singletonList(d));
      } catch (IOException | RuntimeException e) {
        LOG.warn("Cannot delete the cells of the failed write of HNSW segment {} in {}",
          Bytes.toStringBinary(d.identity), this, e);
        failure.addSuppressed(e);
      }
    }

    /**
     * Deletes all cells of the given segments without the WAL: the small cells, and the chunks that
     * the layout of each segment names. One atomic mutation covers all of their anchor rows. This
     * region must hold the segments.
     */
    public void retire(List<HnswSegment.Descriptor> segments) throws IOException {
      Map<byte[], Delete> deletes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (HnswSegment.Descriptor d : segments) {
        Delete delete = deletes.computeIfAbsent(d.anchor, Delete::new);
        for (byte tag : HnswSegment.SMALL_TAGS) {
          delete.addColumns(FAMILY, HnswSegment.qualifier(d.identity, tag), d.time);
        }
        for (byte tag : HnswSegment.CHUNK_TAGS) {
          for (int i = 0; i < d.layout.chunks(tag); i++) {
            delete.addColumns(FAMILY, HnswSegment.qualifier(d.identity, i, tag), d.time);
          }
        }
        delete.setDurability(Durability.SKIP_WAL);
      }
      mutate(new ArrayList<>(deletes.values()));
    }

    /**
     * Deletes all cells of an index in this region, in all anchor rows, with the WAL. The purge
     * also deletes the malformed cells of the index.
     * @return the number of cells deleted
     */
    public int purge(byte[] indexKey) throws IOException {
      Map<byte[], Delete> deletes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      int[] cells = new int[1];
      scan(indexScan(indexKey), cell -> {
        deletes.computeIfAbsent(CellUtil.cloneRow(cell), Delete::new).addColumns(FAMILY,
          CellUtil.cloneQualifier(cell), cell.getTimestamp());
        cells[0]++;
      });
      mutate(new ArrayList<>(deletes.values()));
      return cells[0];
    }

    /**
     * Deletes the cells of an index in this region that no well-formed segment owns. The deletes
     * cover all anchor rows and skip the WAL. The listing of the index skips these cells, so
     * {@link #retire(List)} cannot name them. One scan finds the cells and the well-formed
     * segments. The method deletes three kinds of cells:
     * <ul>
     * <li>cells whose qualifier does not decode</li>
     * <li>chunks of a well-formed segment at or above the chunk count of its layout. An earlier
     * failed write with the same identity can leave such chunks.</li>
     * <li>all cells of a segment that is not well-formed, if the segment time is at or before
     * {@code keepAfter}</li>
     * </ul>
     * A write streams its chunks before its commit Put. Thus the scan can see the chunks of a write
     * in progress without its small cells. The caller gives the time of its newest base as
     * {@code keepAfter}. A write that is still in progress has a later time, so the method keeps
     * its chunks. The method keeps the well-formed segments of other definitions. A manager that
     * does not yet know a re-created index must not delete the segments of the new definition.
     * @param indexKey  the index key
     * @param keepAfter the time after which the method keeps the cells of segments that are not
     *                  well-formed
     * @return the number of cells deleted
     */
    public int retireMalformed(byte[] indexKey, long keepAfter) throws IOException {
      HnswSegment.Listing listing = new HnswSegment.Listing();
      List<Cell> cells = new ArrayList<>();
      scan(indexScan(indexKey), cell -> {
        listing.add(cell);
        cells.add(new KeyValue(CellUtil.cloneRow(cell), FAMILY, CellUtil.cloneQualifier(cell),
          cell.getTimestamp(), KeyValue.Type.Put));
      });
      Map<ImmutableBytesPtr, HnswSegment.Descriptor> wellFormed = new HashMap<>();
      for (HnswSegment.Descriptor d : listing.descriptors()) {
        wellFormed.put(HnswSegment.Listing.key(d.anchor, d.identity), d);
      }
      Map<byte[], Delete> deletes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      int deleted = 0;
      for (Cell cell : cells) {
        byte[] row = CellUtil.cloneRow(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        HnswSegment.Qualifier q = HnswSegment.parseQualifier(qualifier, 0, qualifier.length);
        boolean delete;
        if (q == null) {
          delete = true;
        } else {
          HnswSegment.Descriptor d = wellFormed.get(HnswSegment.Listing.key(row, q.identity()));
          delete =
            d != null ? q.chunk >= 0 && q.chunk >= d.layout.chunks(q.tag) : q.time <= keepAfter;
        }
        if (delete) {
          Delete del = deletes.computeIfAbsent(row, Delete::new);
          del.addColumns(FAMILY, qualifier, cell.getTimestamp());
          del.setDurability(Durability.SKIP_WAL);
          deleted++;
        }
      }
      mutate(new ArrayList<>(deletes.values()));
      return deleted;
    }

    // Applies the mutations as one atomic operation under the row locks of their rows
    private void mutate(List<Mutation> mutations) throws IOException {
      if (mutations.isEmpty()) {
        return;
      }
      List<byte[]> rows = new ArrayList<>(mutations.size());
      for (Mutation m : mutations) {
        rows.add(m.getRow());
      }
      rows.sort(Bytes.BYTES_COMPARATOR);
      region.mutateRowsWithLocks(mutations, rows, HConstants.NO_NONCE, HConstants.NO_NONCE);
    }

    // Asks for a flush of V#, and waits until the V# memstore is empty
    private void flush() throws IOException {
      Store store = region.getStore(FAMILY);
      awaitFlush(() -> store.getFlushableSize().getDataSize() == 0, null, null);
    }

    // Asks the flush requester of the RegionServer to flush only V#, and waits until the condition
    // holds. A request stays in the queue of the requester until its flush runs. While a store of
    // the region has too many files, the requester delays the flush for up to its blocking wait
    // time. A new request replaces a delayed request and starts the delay again. Thus the store
    // asks again only in three cases. First, the requester refused the request, because another
    // flush of the region is in its queue. Second, the flush ran, and the condition does not hold
    // after it. This occurs when V# has new cells, or when another flush of the region held V#.
    // Third, the request has no answer after twice the blocking wait time. After each answer, the
    // store waits a short time before it asks again, so that a busy region does not get a flood of
    // requests. A condition that holds at the start adds no wait. A flush for memory pressure, or
    // the request of another caller, can remove a request from the queue without an answer. A
    // region that closes flushes its memstore, or loses it in an abort. For this reason, the wait
    // ends when the region closes. During a segment write, the wait also ends on an abort.
    private void awaitFlush(BooleanSupplier done, HnswSegment.Descriptor d, BooleanSupplier abort)
      throws IOException {
      List<byte[]> families = Collections.singletonList(FAMILY);
      long unanswered = TimeUnit.MILLISECONDS.toNanos(
        2 * env.getConfiguration().getLong(BLOCKING_WAIT_TIME, DEFAULT_BLOCKING_WAIT_TIME));
      CompletableFuture<Boolean> flushed = null;
      long requested = 0;
      while (!done.getAsBoolean() && !region.isClosing() && !region.isClosed()) {
        if (d != null) {
          checkAbort(d, abort);
        }
        if (flushed == null || System.nanoTime() - requested > unanswered) {
          flushed = requestFlush(families);
          requested = System.nanoTime();
        }
        try {
          flushed.get(FLUSH_RETRY_MS, TimeUnit.MILLISECONDS);
          flushed = null;
          if (!done.getAsBoolean()) {
            Thread.sleep(FLUSH_RETRY_MS);
          }
        } catch (TimeoutException e) {
          // The request is in the queue, or its flush runs now. Check the memstore again.
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Interrupted while flushing "
            + QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY + " of " + this);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        }
      }
      // A wait that ends because the region closes is an abort, if the caller stops the write
      if (d != null) {
        checkAbort(d, abort);
      }
    }

    // Asks the flush requester to flush V#. The result is true when the flush ran, and false when
    // the requester refused the request
    private CompletableFuture<Boolean> requestFlush(List<byte[]> families) {
      CompletableFuture<Boolean> flushed = new CompletableFuture<>();
      ((RegionServerServices) env.getOnlineRegions()).getFlushRequester()
        .requestFlush((HRegion) region, families, new FlushLifeCycleTracker() {
          @Override
          public void notExecuted(String reason) {
            flushed.complete(false);
          }

          @Override
          public void afterExecution() {
            flushed.complete(true);
          }
        });
      return flushed;
    }

    @Override
    public String toString() {
      return "region " + region.getRegionInfo().getEncodedName();
    }
  }

  /**
   * Signals that a segment write stopped before its commit, because its caller asked for it. The
   * region does not list the segment. The caller deletes the chunks that the write left.
   */
  public static final class AbortedWriteException extends IOException {
    private static final long serialVersionUID = 1L;

    AbortedWriteException(String message) {
      super(message);
    }

    AbortedWriteException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * The read-only segment store of a data table, for client tools. It reads {@code V#} with
   * ordinary data table scans and gets. Tools never write segment cells.
   */
  public static class TableStore extends HnswSegmentStore {
    private final Table table;

    /** Creates a store over the data table. The caller closes the table. */
    public TableStore(Table table) {
      this(table, BATCH_BYTES);
    }

    // Creates a store over the data table that reads groups of up to the given number of bytes.
    // Tests use small groups
    @VisibleForTesting
    TableStore(Table table, int batchBytes) {
      super(batchBytes);
      this.table = table;
    }

    @Override
    void scan(Scan scan, Consumer<Cell> consumer) throws IOException {
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          for (Cell cell : r.rawCells()) {
            consumer.accept(cell);
          }
        }
      }
    }

    @Override
    Result get(Get get) throws IOException {
      return table.get(get);
    }

    @Override
    public String toString() {
      return "table " + table.getName();
    }
  }
}
