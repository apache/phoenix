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
import java.util.List;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.CompiledTTLExpression;
import org.apache.phoenix.schema.TTLExpressionFactory;
import org.apache.phoenix.util.ScanUtil;

/**
 * Restricts the data table scan of an HNSW nearest neighbor search to the candidate rows that the
 * graph indexes of the region return. The scan reads only the candidates in its key range.
 * <p>
 * In three cases, the scan reads its full key range without restriction. First, on a table with a
 * TTL, the scan masks the expired rows that the graph still holds. Expired candidates can then take
 * the places of live rows. Second, a search can ask for more than {@code MAX_CANDIDATES}
 * candidates. Third, the base segments of a region can leave a part of its range without coverage,
 * for example in the upper daughter of a split before its first build. Then the graphs do not hold
 * all the rows of the region until a covering base lands.
 */
public final class HnswCandidateProbe {
  // A search that needs more candidates than this reads its range without restriction. The client
  // sets the candidate count, so this limit bounds the heap that a search and its filter use
  static final int MAX_CANDIDATES = 10_000;

  private HnswCandidateProbe() {
  }

  /**
   * Makes the candidate filter of a data table scan from the HNSW search attribute of the scan. The
   * attribute holds the candidate count and the query vector. Returns null if the scan must read
   * its full key range without restriction.
   * @param env     the coprocessor environment of the region
   * @param manager the HNSW index manager of the region
   * @param scan    the client scan, which holds the search attribute
   * @return a filter that limits the scan to the candidate rows, or null for a full scan
   * @throws DoNotRetryIOException if the search attribute is missing or malformed
   * @throws IOException           if the search fails
   */
  public static Filter candidateFilter(RegionCoprocessorEnvironment env, HnswIndexManager manager,
    Scan scan) throws IOException {
    byte[] search = scan.getAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY);
    int dimension = manager.dimension();
    if (search == null || search.length != Bytes.SIZEOF_INT * (1 + dimension)) {
      throw new DoNotRetryIOException(
        "HNSW search of region " + env.getRegionInfo().getRegionNameAsString()
          + " has no search parameters for dimension " + dimension);
    }
    int candidates = Bytes.toInt(search, 0);
    if (candidates <= 0) {
      throw new DoNotRetryIOException("HNSW search of region "
        + env.getRegionInfo().getRegionNameAsString() + " asks for " + candidates + " candidates");
    }
    float[] query = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      query[i] = Bytes.toFloat(search, Bytes.SIZEOF_INT * (1 + i));
    }
    return candidateFilter(env, manager, scan, query, candidates);
  }

  /**
   * Makes the candidate filter of a data table scan from a graph search. The filter holds only the
   * candidates in the key range of the scan. Returns null if the scan must read its full key range
   * without restriction.
   * @param env        the coprocessor environment of the region
   * @param manager    the HNSW index manager of the region
   * @param scan       the client scan
   * @param query      the query vector
   * @param candidates the candidate count of the graph search
   * @return a filter that limits the scan to the candidate rows, or null for a full scan
   * @throws IOException if the search fails
   */
  static Filter candidateFilter(RegionCoprocessorEnvironment env, HnswIndexManager manager,
    Scan scan, float[] query, int candidates) throws IOException {
    // Read the full range for too many candidates, or if TTL masking can drop candidates. The
    // search returns no candidates from a state whose bases do not cover the region, because its
    // graphs do not hold all the rows. Thus the coverage and the candidates come from one state
    List<byte[]> found = candidates > MAX_CANDIDATES || masksExpiredRows(env, scan)
      ? null
      : manager.candidates(query, candidates);
    if (found == null) {
      return null;
    }
    // Keep only the candidates in the key range of the scan
    List<byte[]> keys = new ArrayList<>();
    for (byte[] key : found) {
      if (
        Bytes.compareTo(key, scan.getStartRow()) >= 0
          && (scan.getStopRow().length == 0 || Bytes.compareTo(key, scan.getStopRow()) < 0)
      ) {
        keys.add(key);
      }
    }
    return rows(keys);
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
