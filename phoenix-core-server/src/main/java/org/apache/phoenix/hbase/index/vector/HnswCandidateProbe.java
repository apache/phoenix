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

import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_FOREVER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.ServerScanUtil;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource.HnswMetric;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.CompiledTTLExpression;
import org.apache.phoenix.schema.TTLExpressionFactory;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Restricts the data table scan of an HNSW nearest neighbor search to the candidate rows that the
 * graph indexes of the region return.
 * <p>
 * <ul>
 * <li><b>Key predicates:</b> The graph search applies the key range of the scan and the keys of a
 * {@link SkipScanFilter} to the row keys that the graph holds. A node that fails these predicates
 * is not a candidate, but the search still follows its links. Thus the graph stays connected.</li>
 * <li><b>Probe:</b> A filter on non-key columns needs the data of each row. Thus the probe reads
 * the candidate rows with a region-local scanner and the client filter. This scanner adds only the
 * Phoenix TTL mask. It calls no region observer hooks, and it does not use paging. On a table with
 * a TTL, a search without a filter also probes its candidates. The reason is that the scan masks
 * expired rows that the graph still holds.</li>
 * <li><b>Widening:</b> If fewer candidates pass than the target (LIMIT + OFFSET), the search asks
 * the graph for more candidates. It calculates the new count from the pass rate. While no candidate
 * passes, it multiplies the count by {@code GROWTH}. The probe removes the candidate restriction
 * when the count is more than {@code MAX_CANDIDATES}, or reaches 1 / {@code UNRESTRICTED_SHARE} of
 * the indexed keys of the scan range. The scan then reads every row of its range and applies the
 * filter to each row.</li>
 * <li><b>Uncovered regions:</b> The base segments of a region can leave a part of its range without
 * coverage, for example in the upper daughter of a split before its first build. The graphs then do
 * not hold all the rows of the region. Thus the scan reads its range without restriction until a
 * base segment covers the region.</li>
 * </ul>
 */
public final class HnswCandidateProbe {
  private static final byte[] ZERO = new byte[1];
  // A search that needs more candidates than this reads its range without restriction. The client
  // sets the candidate count, so this limit bounds the heap that a search and its filter use
  static final int MAX_CANDIDATES = 10_000;
  // A probe that needs at least 1 / UNRESTRICTED_SHARE of the indexed keys of its range reads the
  // range without restriction. A read of every row costs about as much as that many seeks
  static final int UNRESTRICTED_SHARE = 10;
  // If no candidate passes a probe, the next search asks for GROWTH times as many candidates
  static final int GROWTH = 4;

  private HnswCandidateProbe() {
  }

  /**
   * Makes the candidate filter of a data table scan from the HNSW search attribute of the scan. The
   * attribute holds the candidate count, the target row count and the query vector. Returns null if
   * the scan must read its full key range without restriction.
   * @param env     the coprocessor environment of the region
   * @param manager the HNSW index manager of the region
   * @param scan    the client scan, which holds the search attribute and the query predicates
   * @return a filter that limits the scan to the candidate rows that pass, or null for a full scan
   * @throws DoNotRetryIOException if the search attribute is missing or malformed
   * @throws IOException           if the search or the probe fails
   */
  public static Filter candidateFilter(RegionCoprocessorEnvironment env, HnswIndexManager manager,
    Scan scan) throws IOException {
    byte[] search = scan.getAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY);
    int dimension = manager.dimension();
    if (search == null || search.length != Bytes.SIZEOF_INT * (2 + dimension)) {
      throw new DoNotRetryIOException(
        "HNSW search of region " + env.getRegionInfo().getRegionNameAsString()
          + " has no search parameters for dimension " + dimension);
    }
    int candidates = Bytes.toInt(search, 0);
    int target = Bytes.toInt(search, Bytes.SIZEOF_INT);
    if (candidates <= 0 || target < 0) {
      throw new DoNotRetryIOException(
        "HNSW search of region " + env.getRegionInfo().getRegionNameAsString() + " asks for "
          + candidates + " candidates and " + target + " rows");
    }
    float[] query = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      query[i] = Bytes.toFloat(search, Bytes.SIZEOF_INT * (2 + i));
    }
    return candidateFilter(env, manager, scan, query, candidates, target);
  }

  /**
   * Makes the candidate filter of a data table scan from a graph search and a probe of its
   * candidates. Returns null if the scan must read its full key range without restriction.
   * @param env        the coprocessor environment of the region
   * @param manager    the HNSW index manager of the region
   * @param scan       the client scan, which holds the query predicates
   * @param query      the query vector
   * @param candidates the candidate count of the first graph search
   * @param target     the number of rows that must pass (LIMIT + OFFSET)
   * @return a filter that limits the scan to the candidate rows that pass, or null for a full scan
   * @throws IOException if the search or the probe fails
   */
  static Filter candidateFilter(RegionCoprocessorEnvironment env, HnswIndexManager manager,
    Scan scan, float[] query, int candidates, int target) throws IOException {
    long start = EnvironmentEdgeManager.currentTimeMillis();
    try {
      return restrict(env, manager, scan, query, candidates, target);
    } finally {
      manager.metrics().update(HnswMetric.SEARCHES, 1);
      manager.metrics().update(HnswMetric.SEARCH_TIME,
        EnvironmentEdgeManager.currentTimeMillis() - start);
    }
  }

  // Makes the candidate filter, as candidateFilter describes. Each scan without candidate
  // restriction adds 1 to exactly one reason counter. The manager counts the region state, and
  // this method counts the candidate limits
  private static Filter restrict(RegionCoprocessorEnvironment env, HnswIndexManager manager,
    Scan scan, float[] query, int candidates, int target) throws IOException {
    if (candidates > MAX_CANDIDATES) {
      manager.metrics().update(HnswMetric.UNRESTRICTED_LIMIT, 1);
      return null;
    }
    byte[] start = scan.getStartRow();
    byte[] stop = scan.getStopRow();
    Filter filter = scan.getFilter();
    if (filter instanceof PagingFilter) {
      filter = ((PagingFilter) filter).getDelegateFilter();
    }
    if (filter == null && !masksExpiredRows(env, scan)) {
      // Without a filter or a TTL mask, the scan returns every candidate, so a probe is not
      // necessary. If the range holds no more indexed keys than the candidate count, the search
      // returns all of them. The search returns null for a state whose bases do not cover the
      // region, because its graphs do not hold all the rows
      HnswIndexManager.Candidates found = manager.candidates(query, candidates, start, stop, null);
      return found == null ? null : rows(found.keys);
    }
    // The filter or the TTL mask can remove candidates. Probe the candidates until enough pass
    Predicate<byte[]> keyFilter = filter == null ? null : keyFilter(filter);
    Set<ImmutableBytesPtr> probed = new HashSet<>();
    List<byte[]> passed = new ArrayList<>();
    int count = candidates;
    while (true) {
      // Each search gets the coverage, the candidates and the count of indexed keys of the range
      // from one state. That count is the limit of the widening. A count from an earlier state can
      // be too low, for example from an uncovered state before the cutover of its first base. The
      // probe would then stop with too few rows
      HnswIndexManager.Candidates found = manager.candidates(query, count, start, stop, keyFilter);
      if (found == null) {
        return null;
      }
      List<byte[]> keys = found.keys;
      int bound = found.count;
      List<byte[]> fresh = new ArrayList<>(keys.size());
      for (byte[] key : keys) {
        if (probed.add(new ImmutableBytesPtr(key))) {
          fresh.add(key);
        }
      }
      if (!fresh.isEmpty()) {
        manager.metrics().update(HnswMetric.PROBE_ROUNDS, 1);
        manager.metrics().update(HnswMetric.PROBE_ROWS_READ, fresh.size());
      }
      passed.addAll(probe(env, scan, filter, fresh));
      // Stop when enough candidates pass, or when the search found every indexed key of the range
      if (passed.size() >= target || keys.size() < count || count >= bound) {
        return rows(passed);
      }
      // Calculate the candidate count that gives the target from the pass rate of all probes. If
      // no candidate passed, multiply the count by GROWTH. Read the range without restriction if
      // the new count is too large
      long next = passed.isEmpty()
        ? (long) count * GROWTH
        : (long) Math.ceil(target * (double) probed.size() / passed.size());
      if (next * UNRESTRICTED_SHARE >= bound || next > MAX_CANDIDATES) {
        manager.metrics().update(HnswMetric.UNRESTRICTED_LIMIT, 1);
        return null;
      }
      count = (int) next;
    }
  }

  // Returns true if the scan can mask a row of the graph as expired. TTLRegionScanner makes the
  // same decision: it masks with the TTL of the scan, or else with the TTL of the table
  // descriptor. A relaxed TTL does not mask. Without Phoenix compaction, HBase masks with the TTL
  // of the table descriptor. The hidden family V# has no TTL. It is the first family if all the
  // data families sort after it, and then the TTL comes from the next family
  private static boolean masksExpiredRows(RegionCoprocessorEnvironment env, Scan scan)
    throws IOException {
    boolean phoenixMasks = ScanUtil.isPhoenixCompactionEnabled(env.getConfiguration());
    CompiledTTLExpression ttl = phoenixMasks ? ScanUtil.getTTLExpression(scan) : null;
    if (ttl == null || ttl.equals(TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR)) {
      ColumnFamilyDescriptor[] families = env.getRegion().getTableDescriptor().getColumnFamilies();
      ColumnFamilyDescriptor family = families.length > 1 && Bytes.equals(families[0].getName(),
        QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES) ? families[1] : families[0];
      ttl = TTLExpressionFactory.create(family.getTimeToLive());
    }
    return !ttl.equals(TTL_EXPRESSION_FOREVER) && (!phoenixMasks || ScanUtil.isStrictTTL(scan));
  }

  // Returns a row key predicate from the SkipScanFilter of the client filter, or null if there is
  // none. The method uses the SkipScanFilter only if it is the filter or a member of a
  // MUST_PASS_ALL list. Only then must each result row pass it, so the graph search can apply it
  // to each node
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
    // A SkipScanFilter keeps position state, so the predicate tests a private copy. The range
    // [key, key\0) holds only the given key
    SkipScanFilter keys = (SkipScanFilter) copy(skipScan);
    return key -> keys.hasIntersect(key, Bytes.add(key, ZERO));
  }

  // Returns the candidate keys whose rows the region holds and that pass the client filter and the
  // Phoenix TTL mask
  private static List<byte[]> probe(RegionCoprocessorEnvironment env, Scan scan, Filter filter,
    List<byte[]> keys) throws IOException {
    List<byte[]> passed = new ArrayList<>();
    if (keys.isEmpty()) {
      return passed;
    }
    Scan probe = new Scan(scan);
    probe.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, null);
    probe.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY, null);
    // A filter keeps state across rows, so each probe uses a new copy of the client filter
    Filter candidates = new MultiRowRangeFilter(ranges(keys));
    probe.setFilter(filter == null
      ? candidates
      : new FilterList(FilterList.Operator.MUST_PASS_ALL, copy(filter), candidates));
    // The probe needs only the rows that pass the client filter and the Phoenix TTL mask. Thus the
    // scanner adds only the TTL mask to the region scanner. It calls no region observer hooks, and
    // it does not use paging
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

  // Returns a filter that limits the scan to the given keys. With no keys, the filter ends the scan
  // immediately. TTLRegionScanner takes a top-level PageFilter out of the scan as a row limit, so
  // the end filter wraps its PageFilter in a WhileMatchFilter
  static Filter rows(List<byte[]> keys) {
    return keys.isEmpty()
      ? new WhileMatchFilter(new PageFilter(0))
      : new MultiRowRangeFilter(ranges(keys));
  }
}
