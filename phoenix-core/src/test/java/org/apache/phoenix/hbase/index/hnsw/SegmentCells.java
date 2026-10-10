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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.MemStoreSize;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The cells of the hidden family {@code V#} of mocked regions, for unit tests of the users of
 * {@link HnswSegmentStore.RegionStore}. One instance can serve several regions. Each region reads
 * only the rows of its own key range, as a region of a split or a merge reads its own anchor rows.
 * The memstore of {@code V#} is always empty, so a segment write makes no flush request.
 */
public final class SegmentCells {
  /**
   * Runs before the cells apply a mutation of {@code V#}. The hook can block the mutation, or make
   * it fail.
   */
  public interface Hook {
    void before(Mutation mutation) throws IOException;
  }

  // The cells by row, then by qualifier
  private final NavigableMap<byte[], NavigableMap<byte[], Cell>> rows =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
  private volatile Hook hook = m -> {
  };

  /** Sets the hook of later mutations. */
  public void setHook(Hook hook) {
    this.hook = hook;
  }

  /** Removes the hook. */
  public void clearHook() {
    this.hook = m -> {
    };
  }

  /** Returns true if the scan reads {@code V#}. */
  public static boolean isSegmentScan(Scan scan) {
    return scan.getFamilyMap().containsKey(HnswSegmentStore.FAMILY);
  }

  /** Returns true if the get reads {@code V#}. */
  public static boolean isSegmentGet(Get get) {
    return get.getFamilyMap().containsKey(HnswSegmentStore.FAMILY);
  }

  /**
   * Stubs the mutations of the region and its {@code V#} store. The caller dispatches region scans
   * and gets of {@code V#} to {@link #scanner} and {@link #get}.
   */
  public void stub(Region region) throws IOException {
    doAnswer(inv -> {
      Collection<Mutation> mutations = inv.getArgument(0);
      for (Mutation m : mutations) {
        hook.before(m);
      }
      for (Mutation m : mutations) {
        apply(m);
      }
      return null;
    }).when(region).mutateRowsWithLocks(anyCollection(), anyCollection(), anyLong(), anyLong());
    MemStoreSize empty = mock(MemStoreSize.class);
    Store store = mock(Store.class);
    when(store.getFlushableSize()).thenReturn(empty);
    when(region.getStore(HnswSegmentStore.FAMILY)).thenReturn(store);
  }

  /**
   * Stubs a region that has no data rows: its mutations, and its scans and gets, which find only
   * the cells of {@code V#}.
   */
  public void stubOnly(Region region) throws IOException {
    stub(region);
    when(region.getScanner(any(Scan.class))).thenAnswer(inv -> {
      Scan scan = inv.getArgument(0);
      return isSegmentScan(scan)
        ? scanner(scan, region.getRegionInfo())
        : scanner(new ArrayList<>());
    });
    when(region.get(any(Get.class))).thenAnswer(inv -> {
      Get get = inv.getArgument(0);
      return isSegmentGet(get) ? get(get) : Result.create(new ArrayList<>());
    });
  }

  private void apply(Mutation m) {
    byte[] row = m.getRow();
    List<Cell> cells = m.getFamilyCellMap().get(HnswSegmentStore.FAMILY);
    if (cells == null) {
      return;
    }
    if (m instanceof Put) {
      NavigableMap<byte[], Cell> stored =
        rows.computeIfAbsent(row, k -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
      for (Cell cell : cells) {
        stored.put(CellUtil.cloneQualifier(cell), new KeyValue(row, HnswSegmentStore.FAMILY,
          CellUtil.cloneQualifier(cell), cell.getTimestamp(), CellUtil.cloneValue(cell)));
      }
    } else if (m instanceof Delete) {
      NavigableMap<byte[], Cell> stored = rows.get(row);
      if (stored == null) {
        return;
      }
      for (Cell marker : cells) {
        byte[] qualifier = CellUtil.cloneQualifier(marker);
        Cell cell = stored.get(qualifier);
        if (cell != null && cell.getTimestamp() <= marker.getTimestamp()) {
          stored.remove(qualifier);
        }
      }
      if (stored.isEmpty()) {
        rows.remove(row);
      }
    }
  }

  /** Returns a region scanner over the cells of the region that the scan and its filter select. */
  public RegionScanner scanner(Scan scan, RegionInfo region) throws IOException {
    List<List<Cell>> results = new ArrayList<>();
    Filter filter = scan.getFilter();
    for (Map.Entry<byte[], NavigableMap<byte[], Cell>> row : rows.entrySet()) {
      if (!region.containsRow(row.getKey())) {
        continue;
      }
      if (filter != null) {
        filter.reset();
      }
      List<Cell> selected = new ArrayList<>();
      for (Cell cell : row.getValue().values()) {
        Filter.ReturnCode rc = filter == null ? Filter.ReturnCode.INCLUDE : filter.filterCell(cell);
        if (
          rc == Filter.ReturnCode.INCLUDE || rc == Filter.ReturnCode.INCLUDE_AND_NEXT_COL
            || rc == Filter.ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW
        ) {
          selected.add(cell);
        }
      }
      if (!selected.isEmpty()) {
        results.add(selected);
      }
    }
    return scanner(results);
  }

  // Returns a region scanner that returns the given rows
  private static RegionScanner scanner(List<List<Cell>> results) throws IOException {
    Iterator<List<Cell>> it = results.iterator();
    RegionScanner scanner = mock(RegionScanner.class);
    when(scanner.next(anyList())).thenAnswer(inv -> {
      if (it.hasNext()) {
        List<Cell> out = inv.getArgument(0);
        out.addAll(it.next());
      }
      return it.hasNext();
    });
    return scanner;
  }

  /** Returns the cells of the get. */
  public Result get(Get get) {
    NavigableMap<byte[], Cell> stored = rows.get(get.getRow());
    List<Cell> cells = new ArrayList<>();
    if (stored != null) {
      for (byte[] qualifier : get.getFamilyMap().get(HnswSegmentStore.FAMILY)) {
        Cell cell = stored.get(qualifier);
        if (cell != null) {
          cells.add(cell);
        }
      }
    }
    return Result.create(cells);
  }

  /** Returns the segments of an index that the cells hold, in all rows. */
  public List<HnswSegment.Descriptor> list(byte[] indexKey) {
    HnswSegment.Listing listing = new HnswSegment.Listing();
    byte[] prefix = HnswSegment.prefix(indexKey);
    for (NavigableMap<byte[], Cell> row : rows.values()) {
      for (Cell cell : row.values()) {
        if (Bytes.startsWith(CellUtil.cloneQualifier(cell), prefix)) {
          listing.add(cell);
        }
      }
    }
    return listing.descriptors();
  }

  /** Returns the number of cells. */
  public int size() {
    int size = 0;
    for (NavigableMap<byte[], Cell> row : rows.values()) {
      size += row.size();
    }
    return size;
  }

  /** Deletes every cell. */
  public void clear() {
    rows.clear();
  }

  /**
   * Writes the cells of a segment at its anchor row. A null payload writes no payload cell. The W
   * cell holds the encoded name of the writer region.
   */
  public void put(HnswSegment.Descriptor d, byte[] payload, byte[] writer) {
    put(d.anchor, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG), d.time, d.endKey);
    put(d.anchor, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG), d.time,
      Bytes.toBytes(d.count));
    put(d.anchor, HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG), d.time,
      Bytes.toBytes(d.replayFrom));
    put(d.anchor, HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG), d.time,
      d.definition);
    put(d.anchor, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG), d.time, writer);
    if (payload != null) {
      put(d.anchor, HnswSegment.qualifier(d.identity, HnswSegment.PAYLOAD_TAG), d.time, payload);
    }
  }

  /** Writes a cell, as a client that bypasses the region store can. */
  public void put(byte[] row, byte[] qualifier, long time, byte[] value) {
    rows.computeIfAbsent(row, k -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR))
      .put(qualifier, new KeyValue(row, HnswSegmentStore.FAMILY, qualifier, time, value));
  }

  /** Deletes the cell of a segment that has the given tag. */
  public void remove(HnswSegment.Descriptor d, byte tag) {
    NavigableMap<byte[], Cell> stored = rows.get(d.anchor);
    if (stored != null) {
      stored.remove(HnswSegment.qualifier(d.identity, tag));
    }
  }
}
