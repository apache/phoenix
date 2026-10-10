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
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.phoenix.query.QueryConstants;

/**
 * Reads and writes the cells of HNSW segments in the hidden family {@code V#} of a data table.
 * {@link HnswSegment} describes the cell layout. A listing reads only the small cells of the
 * segments. It never reads a payload cell or a MOB file. A payload read resolves the MOB reference.
 * If the payload cell is missing, the region retired the segment after the listing, and the read
 * throws {@link HnswSegment.NotFoundException}. A payload whose MOB file is missing or corrupt
 * reads as an empty value. The payload framing check of {@link HnswSegment} rejects this value as a
 * corrupt segment.
 * <p>
 * {@link RegionStore} serves the RegionServer. Its reads and writes are calls on the region, and
 * make no RPC. {@link TableStore} serves client tools, and only reads.
 */
public abstract class HnswSegmentStore {
  /** The hidden column family of the segment cells. */
  static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;

  /**
   * Returns the descriptors of the well-formed segments of an index in this store. The listing
   * reads the small cells of the index as stored. It never returns a payload cell, and never reads
   * a MOB file.
   */
  public List<HnswSegment.Descriptor> list(byte[] indexKey) throws IOException {
    HnswSegment.Listing listing = new HnswSegment.Listing();
    scan(listScan(indexKey), listing::add);
    return listing.descriptors();
  }

  /**
   * Returns the payload cell of a segment, with the MOB reference resolved. The read sets
   * {@link MobConstants#EMPTY_VALUE_ON_MOBCELL_MISS}. With this attribute, HBase returns an empty
   * value for a reference whose MOB file is missing or corrupt. This occurs on the region path and
   * on the RPC path. The payload framing check rejects the empty value as a corrupt segment. The
   * region then replaces the segment with a rebuild.
   * @throws HnswSegment.NotFoundException if the payload cell is missing, as after the retirement
   *                                       of the segment
   */
  public Cell readPayload(HnswSegment.Descriptor d) throws IOException {
    byte[] qualifier = HnswSegment.qualifier(d.identity, HnswSegment.PAYLOAD_TAG);
    Get get = new Get(d.anchor).addColumn(FAMILY, qualifier);
    get.setAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS, Bytes.toBytes(true));
    Cell cell = get(get).getColumnLatestCell(FAMILY, qualifier);
    if (cell == null) {
      throw new HnswSegment.NotFoundException(d.identity, this);
    }
    return cell;
  }

  // Returns a scan of all cells of an index. The scan returns MOB reference cells as stored, and
  // reads no MOB file.
  static Scan indexScan(byte[] indexKey) {
    Scan scan =
      new Scan().addFamily(FAMILY).setFilter(new ColumnPrefixFilter(HnswSegment.prefix(indexKey)));
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    return scan;
  }

  // Returns a scan of the cells of an index that a listing decodes. A filter on the server removes
  // the payload cells. MOB_SCAN_RAW has no effect on a payload in the memstore, because the
  // memstore holds the full value until the flush.
  static Scan listScan(byte[] indexKey) {
    RegexStringComparator tags = new RegexStringComparator("["
      + Bytes.toString(new byte[] { HnswSegment.END_KEY_TAG, HnswSegment.COUNT_TAG,
        HnswSegment.REPLAY_FROM_TAG, HnswSegment.DEFINITION_TAG, HnswSegment.WRITER_TAG })
      + "]\\z");
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
   * Segment writes and retirements skip the WAL, because segments are derived state. If a crash
   * loses a segment write, the result is the same as a graph flush that did not occur. The region
   * then replays the data rows from the replay start of the remaining segments. A memstore flush
   * captures all of {@code V#} at one time. A retirement always comes after the write of the
   * segment that replaces the retired segments. As a result, a crash never keeps a retirement
   * without its replacement. A purge uses the WAL.
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

    private final RegionCoprocessorEnvironment env;
    private final Region region;

    public RegionStore(RegionCoprocessorEnvironment env) {
      this.env = env;
      this.region = env.getRegion();
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
     * Writes the cells of a segment to its anchor row without the WAL. Then it flushes {@code V#},
     * so the payload moves to a MOB file. A null payload writes a segment with no vectors. The W
     * cell holds the encoded name of this region.
     */
    public void write(HnswSegment.Descriptor d, byte[] payload) throws IOException {
      Put put = new Put(d.anchor, d.time)
        .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG), d.endKey)
        .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG),
          Bytes.toBytes(d.count))
        .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG),
          Bytes.toBytes(d.replayFrom))
        .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG),
          d.definition)
        .addColumn(FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG),
          region.getRegionInfo().getEncodedNameAsBytes());
      if (payload != null) {
        // The cell refers to the payload and does not copy it
        put.add(
          CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(d.anchor).setFamily(FAMILY)
            .setQualifier(HnswSegment.qualifier(d.identity, HnswSegment.PAYLOAD_TAG))
            .setTimestamp(d.time).setType(Cell.Type.Put).setValue(payload).build());
      }
      put.setDurability(Durability.SKIP_WAL);
      mutate(Collections.singletonList(put));
      flush();
    }

    /**
     * Deletes all cells of the given segments without the WAL. One atomic mutation covers all of
     * their anchor rows. This region must hold the segments.
     */
    public void retire(List<HnswSegment.Descriptor> segments) throws IOException {
      Map<byte[], Delete> deletes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (HnswSegment.Descriptor d : segments) {
        Delete delete = deletes.computeIfAbsent(d.anchor, Delete::new);
        for (byte tag : HnswSegment.TAGS) {
          delete.addColumns(FAMILY, HnswSegment.qualifier(d.identity, tag), d.time);
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

    // Asks the flush requester of the RegionServer to flush only V#, and waits until the V#
    // memstore is empty. A request stays in the queue of the requester until its flush runs. While
    // a store of the region has too many files, the requester delays the flush for up to its
    // blocking wait time. A new request replaces a delayed request and starts the delay again.
    // Thus the store asks again only in three cases. First, the requester refused the request,
    // because another flush of the region is in its queue. Second, the flush ran, and V# has new
    // cells after it. Third, the request has no answer after twice the blocking wait time. A flush
    // for memory pressure, or the request of another caller, can remove a request from the queue
    // without an answer. A region that closes flushes its memstore, or loses it in an abort. For
    // this reason, the wait ends when the region closes.
    private void flush() throws IOException {
      Store store = region.getStore(FAMILY);
      List<byte[]> families = Collections.singletonList(FAMILY);
      long unanswered = TimeUnit.MILLISECONDS.toNanos(
        2 * env.getConfiguration().getLong(BLOCKING_WAIT_TIME, DEFAULT_BLOCKING_WAIT_TIME));
      CompletableFuture<Boolean> flushed = null;
      long requested = 0;
      while (
        store.getFlushableSize().getDataSize() > 0 && !region.isClosing() && !region.isClosed()
      ) {
        if (flushed == null || System.nanoTime() - requested > unanswered) {
          flushed = requestFlush(families);
          requested = System.nanoTime();
        }
        try {
          if (!flushed.get(FLUSH_RETRY_MS, TimeUnit.MILLISECONDS)) {
            Thread.sleep(FLUSH_RETRY_MS);
          }
          flushed = null;
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
   * The read-only segment store of a data table, for client tools. It reads {@code V#} with
   * ordinary data table scans and gets. Tools never write segment cells.
   */
  public static class TableStore extends HnswSegmentStore {
    private final Table table;

    /** Creates a store over the data table. The caller closes the table. */
    public TableStore(Table table) {
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
