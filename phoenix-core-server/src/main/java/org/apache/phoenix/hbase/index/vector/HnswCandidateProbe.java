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
package org.apache.phoenix.hbase.index.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.ServerScanUtil;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Restricts base table scans during HNSW nearest neighbor vector search to candidate rows returned
 * by the region's graph indexes, coordinating index traversal with query predicate evaluation.
 * <p>
 * <ul>
 * <li><b>Primary Key Predicates:</b> Scan range boundaries and {@link SkipScanFilter} key lists are
 * evaluated during graph traversal against primary keys stored in segment trailers. Graph nodes
 * failing key predicates serve as routing vertices during search.</li>
 * <li><b>Post-Filter Probing:</b> Predicates referencing non-key columns require table row data. An
 * internal probe scanner executes the client filter against candidate keys using standard scanner
 * lifecycle hooks, ensuring proper Phoenix TTL masking and column projection.</li>
 * <li><b>Adaptive Candidate Expansion:</b> If probed candidates yield fewer passing rows than the
 * query target (LIMIT + OFFSET), candidate search iteratively widens using the observed selectivity
 * pass rate. When the required candidate count encompasses the entire indexed key space in the scan
 * range, candidate restrictions are removed, allowing the base scan to perform an exact filtered
 * evaluation over the remaining rows.</li>
 * </ul>
 */
public final class HnswCandidateProbe {
  private static final byte[] ZERO = new byte[1];

  private HnswCandidateProbe() {
  }

  /**
   * Constructs a candidate restriction filter for the data table scan based on graph index search
   * results and predicate evaluation. Returns null when the scan should execute unrestricted across
   * its entire key range.
   * @param env        region coprocessor environment
   * @param manager    region HNSW index manager
   * @param scan       client scan containing query predicates
   * @param query      query vector
   * @param candidates initial candidate count to retrieve from the graph
   * @param target     target number of passing rows required (limit + offset)
   * @return filter restricting base scan to passing candidate rows, or null for full scan
   * @throws IOException on search or probe failure
   */
  public static Filter candidateFilter(RegionCoprocessorEnvironment env, HnswIndexManager manager,
    Scan scan, float[] query, int candidates, int target) throws IOException {
    byte[] start = scan.getStartRow();
    byte[] stop = scan.getStopRow();
    Filter filter = scan.getFilter();
    if (filter instanceof PagingFilter) {
      filter = ((PagingFilter) filter).getDelegateFilter();
    }
    if (filter == null) {
      return rows(manager.search(query, candidates, candidates, start, stop, null));
    }
    Predicate<byte[]> keyFilter = keyFilter(filter);
    int bound = manager.count(start, stop);
    Set<ImmutableBytesPtr> probed = new HashSet<>();
    List<byte[]> passed = new ArrayList<>();
    int count = candidates;
    while (count < bound) {
      List<byte[]> keys = manager.search(query, count, count, start, stop, keyFilter);
      List<byte[]> fresh = new ArrayList<>(keys.size());
      for (byte[] key : keys) {
        if (probed.add(new ImmutableBytesPtr(key))) {
          fresh.add(key);
        }
      }
      passed.addAll(probe(env, scan, filter, fresh));
      if (passed.size() >= target || keys.size() < count) {
        return rows(passed);
      }
      // Estimate candidate requirement based on observed filter selectivity.
      // Fall back to full scan range if zero candidates passed.
      count = passed.isEmpty()
        ? bound
        : (int) Math.min(bound, (long) Math.ceil(target * (double) probed.size() / passed.size()));
    }
    return null;
  }

  // Extracts row key predicate logic from a SkipScanFilter when present in the filter hierarchy
  private static Predicate<byte[]> keyFilter(Filter filter) throws IOException {
    SkipScanFilter skipScan = null;
    if (filter instanceof SkipScanFilter) {
      skipScan = (SkipScanFilter) filter;
    } else if (
      filter instanceof FilterList
        && ((FilterList) filter).getOperator() == FilterList.Operator.MUST_PASS_ALL
    ) {
      for (Filter f : ((FilterList) filter).getFilters()) {
        if (f instanceof SkipScanFilter) {
          skipScan = (SkipScanFilter) f;
        }
      }
    }
    if (skipScan == null) {
      return null;
    }
    // SkipScanFilter maintains internal positioning state.
    // Test point intersections using an isolated copy across [key, key\0).
    SkipScanFilter keys = (SkipScanFilter) copy(skipScan);
    return key -> keys.hasIntersect(key, Bytes.add(key, ZERO));
  }

  // Probes candidate keys by scanning base table rows against client filters
  private static List<byte[]> probe(RegionCoprocessorEnvironment env, Scan scan, Filter filter,
    List<byte[]> keys) throws IOException {
    List<byte[]> passed = new ArrayList<>();
    if (keys.isEmpty()) {
      return passed;
    }
    Scan probe = new Scan(scan);
    probe.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, null);
    probe.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY, null);
    // Isolate client filter state for each probe iteration
    probe.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, copy(filter),
      new MultiRowRangeFilter(ranges(keys))));
    // Standard scanner initialization ensures Phoenix TTL masking and visibility rules apply
    try (RegionScanner scanner = ServerScanUtil.openRegionScanner(env, env.getRegion(), probe)) {
      List<Cell> cells = new ArrayList<>();
      boolean more;
      do {
        more = scanner.next(cells);
        if (!cells.isEmpty()) {
          passed.add(CellUtil.cloneRow(cells.get(0)));
          cells.clear();
        }
      } while (more);
    }
    return passed;
  }

  private static Filter copy(Filter filter) throws IOException {
    return ProtobufUtil.toFilter(ProtobufUtil.toFilter(filter));
  }

  private static List<RowRange> ranges(List<byte[]> keys) {
    List<RowRange> ranges = new ArrayList<>(keys.size());
    for (byte[] key : keys) {
      ranges.add(new RowRange(key, true, key, true));
    }
    return ranges;
  }

  // Constructs a MultiRowRangeFilter targeting candidate keys, or PageFilter(0) if empty
  private static Filter rows(List<byte[]> keys) {
    return keys.isEmpty() ? new PageFilter(0) : new MultiRowRangeFilter(ranges(keys));
  }
}
