/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.covered.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;

import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;

/**
 * Wrapper around a lazily instantiated, local HTable.
 * <p>
 * Previously, we had used various row and batch caches. However, this ends up being very
 * complicated when attempting manage updating and invalidating the cache with no real gain as any
 * row accessed multiple times will likely be in HBase's block cache, invalidating any extra caching
 * we are doing here. In the end, its simpler and about as efficient to just get the current state
 * of the row from HBase and let HBase manage caching the row from disk on its own.
 */
public class LocalTable implements LocalHBaseState {

  private RegionCoprocessorEnvironment env;
    private Map<ImmutableBytesPtr, Result> results = null;

  public LocalTable(RegionCoprocessorEnvironment env) {
    this.env = env;
  }

  public void scanCurrentRowStates(Set<ImmutableBytesPtr> rows, Collection<? extends ColumnReference> columns, long ts) throws IOException {
      if (results == null) {
          results = new ConcurrentHashMap<>();
      }
      Scan s = IndexManagementUtil.newLocalStateScan(Collections.singletonList(columns));
      List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();
      for (ImmutableBytesPtr row : rows) {
          ranges.add(new MultiRowRangeFilter.RowRange(row.get(), true, row.get(), true));
      }
      s.setFilter(new MultiRowRangeFilter(ranges));
      s.setTimeRange(0, ts);
      Region region = this.env.getRegion();
      try (RegionScanner scanner = region.getScanner(s)) {
          boolean more;
          do {
              List<Cell> kvs = new ArrayList<Cell>(1);
              more = scanner.next(kvs);
              if (kvs.isEmpty()) {
                  return;
              }
              Result r = Result.create(kvs);
              Cell cell = kvs.get(0);
              byte[] rowKey = new byte[cell.getRowLength()];
              System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowKey, 0, cell.getRowLength());
              results.put(new ImmutableBytesPtr(rowKey), r);
          } while (more);
      }
  }

  public void removeRowStates(Set<ImmutableBytesPtr> rows) {
      for (ImmutableBytesPtr row : rows) {
          results.remove(row);
      }
  }

  @Override
  public Result getCurrentRowState(Mutation m, Collection<? extends ColumnReference> columns, boolean ignoreNewerMutations)
      throws IOException {
      byte[] row = m.getRow();
      Result r;
      if (results != null) {
          if (m.getAttribute(BaseScannerRegionObserver.REPLAY_WRITES) == null) {
              r = results.get(new ImmutableBytesPtr(row));
              if (r == null) {
                  List<Cell> kvs = new ArrayList<Cell>(1);
                  r = Result.create(kvs);
              }
              return r;
          }
      }

    // need to use a scan here so we can get raw state, which Get doesn't provide.
    Scan s = IndexManagementUtil.newLocalStateScan(Collections.singletonList(columns));
    s.setStartRow(row);
    s.setStopRow(row);
    if (ignoreNewerMutations) {
        // Provides a means of client indicating that newer cells should not be considered,
        // enabling mutations to be replayed to partially rebuild the index when a write fails.
        // When replaying mutations we want the oldest timestamp (as anything newer we be replayed)
        long ts = getOldestTimestamp(m.getFamilyCellMap().values());
        s.setTimeRange(0,ts);
    }
    Region region = this.env.getRegion();
    try (RegionScanner scanner = region.getScanner(s)) {
      List<Cell> kvs = new ArrayList<Cell>(1);
      boolean more = scanner.next(kvs);
      assert !more : "Got more than one result when scanning"
          + " a single row in the primary table!";

      r = Result.create(kvs);
      return r;
    }
  }

    // Returns the smallest timestamp in the given cell lists.
    // It is assumed that the lists have cells ordered from largest to smallest timestamp
    protected long getOldestTimestamp(Collection<List<Cell>> cellLists) {
        Ordering<List<Cell>> cellListOrdering = new Ordering<List<Cell>>() {
            @Override
            public int compare(List<Cell> left, List<Cell> right) {
                // compare the last element of each list, since that is the smallest in that list
                return Longs.compare(Iterables.getLast(left).getTimestamp(),
                    Iterables.getLast(right).getTimestamp());
            }
        };
        List<Cell> minList = cellListOrdering.min(cellLists);
        return Iterables.getLast(minList).getTimestamp();
    }
}
