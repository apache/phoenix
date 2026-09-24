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
package org.apache.phoenix.execute;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

public class VectorIndexScanPlanTest extends BaseConnectionlessQueryTest {

  @Test
  public void testDefaultProbeCount() {
    // sqrt(1024) = 32
    assertEquals(32, VectorIndexScanPlan.resolveProbeCount(null, null, null, 1024));
    // sqrt(16) = 4
    assertEquals(4, VectorIndexScanPlan.resolveProbeCount(null, null, null, 16));
    // sqrt(64) = 8
    assertEquals(8, VectorIndexScanPlan.resolveProbeCount(null, null, null, 64));
    // sqrt(100) = 10
    assertEquals(10, VectorIndexScanPlan.resolveProbeCount(null, null, null, 100));
    // sqrt(50) = 7.071 -> 7
    assertEquals(7, VectorIndexScanPlan.resolveProbeCount(null, null, null, 50));
    // 0 centroids -> 0
    assertEquals(0, VectorIndexScanPlan.resolveProbeCount(null, null, null, 0));
    // 1 centroid -> 1
    assertEquals(1, VectorIndexScanPlan.resolveProbeCount(null, null, null, 1));
  }

  @Test
  public void testProbeSelectionCorrectness() {
    // 16 2D centroids at (0,0), (1,1), ..., (15,15)
    float[][] centroids = new float[16][2];
    for (int i = 0; i < 16; i++) {
      centroids[i] = new float[] { (float) i, (float) i };
    }
    CachedCentroids cached = new CachedCentroids("TEST_IDX", 1L, centroids);

    // Query vector close to (3.1, 3.1)
    float[] query = new float[] { 3.1f, 3.1f };
    // Distances:
    // (3,3): 0.1^2 + 0.1^2 = 0.02
    // (4,4): 0.9^2 + 0.9^2 = 1.62
    // (2,2): 1.1^2 + 1.1^2 = 2.42
    // (5,5): 1.9^2 + 1.9^2 = 7.22
    List<Integer> probes = VectorIndexScanPlan.selectProbes(cached, query, "L2", 4);
    assertEquals(Arrays.asList(3, 4, 2, 5), probes);

    // Probe selection with no query vector selects first P centroids
    List<Integer> defaultProbes = VectorIndexScanPlan.selectProbes(cached, null, "L2", 3);
    assertEquals(Arrays.asList(0, 1, 2), defaultProbes);
  }

  @Test
  public void testCustomProbeHint() {
    // /*+ VECTOR_PROBE_COUNT 5 */
    HintNode hintNode1 = new HintNode("/*+ VECTOR_PROBE_COUNT 5 */");
    assertTrue(hintNode1.hasHint(Hint.VECTOR_PROBE_COUNT));
    assertEquals(5, VectorIndexScanPlan.resolveProbeCount(null, hintNode1, null, 1024));

    // /*+ VECTOR_PROBE_COUNT(5) */
    HintNode hintNode2 = new HintNode("/*+ VECTOR_PROBE_COUNT(5) */");
    assertTrue(hintNode2.hasHint(Hint.VECTOR_PROBE_COUNT));
    assertEquals(5, VectorIndexScanPlan.resolveProbeCount(null, hintNode2, null, 1024));

    // /*+ VECTOR_PROBE_COUNT = 5 */
    HintNode hintNode3 = new HintNode("/*+ VECTOR_PROBE_COUNT = 5 */");
    assertTrue(hintNode3.hasHint(Hint.VECTOR_PROBE_COUNT));
    assertEquals(5, VectorIndexScanPlan.resolveProbeCount(null, hintNode3, null, 1024));

    // Programmatic creation: HintNode.create(..., Hint.VECTOR_PROBE_COUNT, "5")
    HintNode hintNode4 = HintNode.create(HintNode.EMPTY_HINT_NODE, Hint.VECTOR_PROBE_COUNT, "5");
    assertTrue(hintNode4.hasHint(Hint.VECTOR_PROBE_COUNT));
    assertEquals(5, VectorIndexScanPlan.resolveProbeCount(null, hintNode4, null, 1024));

    // Combined hint: /*+ INDEX(T IDX) VECTOR_PROBE_COUNT 12 */
    HintNode combinedHint = new HintNode("/*+ INDEX(T IDX) VECTOR_PROBE_COUNT 12 */");
    assertTrue(combinedHint.hasHint(Hint.INDEX));
    assertTrue(combinedHint.hasHint(Hint.VECTOR_PROBE_COUNT));
    assertEquals(12, VectorIndexScanPlan.resolveProbeCount(null, combinedHint, null, 1024));
  }

  @Test
  public void testSessionPropertyProbeCount() throws SQLException {
    Properties props1 = new Properties();
    props1.setProperty(QueryServices.VECTOR_PROBE_COUNT_ATTRIB, "7");
    try (Connection conn = DriverManager.getConnection(getUrl(), props1)) {
      PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
      assertEquals(7, VectorIndexScanPlan.resolveProbeCount(null, null, pConn, 1024));
    }

    Properties props2 = new Properties();
    props2.setProperty("VECTOR_PROBE_COUNT", "9");
    try (Connection conn = DriverManager.getConnection(getUrl(), props2)) {
      PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
      assertEquals(9, VectorIndexScanPlan.resolveProbeCount(null, null, pConn, 1024));
    }

    // Clamp behavior: requested 500 when centroid count is only 16
    assertEquals(16, VectorIndexScanPlan.resolveProbeCount(500, null, null, 16));
  }

  @Test
  public void testProbeCountPrecedence() throws SQLException {
    HintNode hint5 = new HintNode("/*+ VECTOR_PROBE_COUNT 5 */");
    HintNode hintAbc = new HintNode("/*+ VECTOR_PROBE_COUNT(abc) */");
    HintNode hint0 = new HintNode("/*+ VECTOR_PROBE_COUNT(0) */");

    Properties props7 = new Properties();
    props7.setProperty(QueryServices.VECTOR_PROBE_COUNT_ATTRIB, "7");

    Properties propsNeg2 = new Properties();
    propsNeg2.setProperty(QueryServices.VECTOR_PROBE_COUNT_ATTRIB, "-2");

    try (Connection conn7 = DriverManager.getConnection(getUrl(), props7);
      Connection connNeg2 = DriverManager.getConnection(getUrl(), propsNeg2)) {
      PhoenixConnection pConn7 = conn7.unwrap(PhoenixConnection.class);
      PhoenixConnection pConnNeg2 = connNeg2.unwrap(PhoenixConnection.class);

      // Explicit parameter takes precedence over hint
      assertEquals(3, VectorIndexScanPlan.resolveProbeCount(3, hint5, pConn7, 100));

      // Hint takes precedence over client connection property
      assertEquals(5, VectorIndexScanPlan.resolveProbeCount(null, hint5, pConn7, 100));

      // Non-numeric hint falls back to client connection property
      assertEquals(7, VectorIndexScanPlan.resolveProbeCount(null, hintAbc, pConn7, 100));

      // Non-positive hint falls back to default heuristic
      assertEquals(10, VectorIndexScanPlan.resolveProbeCount(null, hint0, null, 100));

      // Invalid connection property falls back to default heuristic
      assertEquals(10, VectorIndexScanPlan.resolveProbeCount(null, null, pConnNeg2, 100));
    }
  }

  @Test
  public void testResolveOversampleFactor() throws SQLException {
    // Default oversample factor when neither hint nor client property is configured
    assertEquals(3.0, VectorIndexScanPlan.resolveOversampleFactor(null, null, null), 1e-6);

    // Hint overrides default factor
    HintNode hint5 = new HintNode("/*+ OVERSAMPLE(5.0) */");
    assertEquals(5.0, VectorIndexScanPlan.resolveOversampleFactor(null, hint5, null), 1e-6);

    // Hint syntax without parentheses
    HintNode hint5Plain = new HintNode("/*+ OVERSAMPLE 5.0 */");
    assertEquals(5.0, VectorIndexScanPlan.resolveOversampleFactor(null, hint5Plain, null), 1e-6);

    // Explicit parameter takes precedence over hint
    assertEquals(4.0, VectorIndexScanPlan.resolveOversampleFactor(4.0, hint5, null), 1e-6);

    // Non-numeric hint falls back to default
    HintNode hintAbc = new HintNode("/*+ OVERSAMPLE(abc) */");
    assertEquals(3.0, VectorIndexScanPlan.resolveOversampleFactor(null, hintAbc, null), 1e-6);

    // Oversample factor below 1.0 is rejected and falls back to default
    HintNode hintSub1 = new HintNode("/*+ OVERSAMPLE(0.5) */");
    assertEquals(3.0, VectorIndexScanPlan.resolveOversampleFactor(null, hintSub1, null), 1e-6);

    // Session and connection property resolution
    Properties props4 = new Properties();
    props4.setProperty(QueryServices.VECTOR_OVERSAMPLE_FACTOR_ATTRIB, "4.5");
    try (Connection conn4 = DriverManager.getConnection(getUrl(), props4)) {
      PhoenixConnection pConn4 = conn4.unwrap(PhoenixConnection.class);
      assertEquals(4.5, VectorIndexScanPlan.resolveOversampleFactor(null, null, pConn4), 1e-6);
      // Hint overrides property
      assertEquals(5.0, VectorIndexScanPlan.resolveOversampleFactor(null, hint5, pConn4), 1e-6);
      // Explicit overrides hint and property
      assertEquals(2.5, VectorIndexScanPlan.resolveOversampleFactor(2.5, hint5, pConn4), 1e-6);
    }
  }

  @Test
  public void testResolveMaxProbeLimit() throws SQLException {
    // Default maximum probe limit when unconfigured
    assertEquals(Integer.MAX_VALUE, VectorIndexScanPlan.resolveMaxProbeLimit(null, null, null));

    // Query hint overrides default
    HintNode hint3 = new HintNode("/*+ MAX_PROBE_LIMIT(3) */");
    assertTrue(hint3.hasHint(Hint.MAX_PROBE_LIMIT));
    assertEquals(3, VectorIndexScanPlan.resolveMaxProbeLimit(null, hint3, null));

    HintNode hint3Eq = new HintNode("/*+ MAX_PROBE_LIMIT = 3 */");
    assertTrue(hint3Eq.hasHint(Hint.MAX_PROBE_LIMIT));
    assertEquals(3, VectorIndexScanPlan.resolveMaxProbeLimit(null, hint3Eq, null));

    HintNode hint3Plain = new HintNode("/*+ MAX_PROBE_LIMIT 3 */");
    assertTrue(hint3Plain.hasHint(Hint.MAX_PROBE_LIMIT));
    assertEquals(3, VectorIndexScanPlan.resolveMaxProbeLimit(null, hint3Plain, null));

    // Explicit parameter takes precedence over hint
    assertEquals(2, VectorIndexScanPlan.resolveMaxProbeLimit(2, hint3, null));

    // Invalid query hint falls back to default
    HintNode hintInvalid = new HintNode("/*+ MAX_PROBE_LIMIT(abc) */");
    assertEquals(Integer.MAX_VALUE,
      VectorIndexScanPlan.resolveMaxProbeLimit(null, hintInvalid, null));

    // Session and connection property resolution
    Properties props1 = new Properties();
    props1.setProperty(QueryServices.VECTOR_MAX_PROBE_LIMIT_ATTRIB, "4");
    try (Connection conn = DriverManager.getConnection(getUrl(), props1)) {
      PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
      assertEquals(4, VectorIndexScanPlan.resolveMaxProbeLimit(null, null, pConn));
      // Hint overrides property
      assertEquals(3, VectorIndexScanPlan.resolveMaxProbeLimit(null, hint3, pConn));
      // Explicit parameter overrides hint and property
      assertEquals(1, VectorIndexScanPlan.resolveMaxProbeLimit(1, hint3, pConn));
    }

    Properties props2 = new Properties();
    props2.setProperty("max_probe_limit", "5");
    try (Connection conn = DriverManager.getConnection(getUrl(), props2)) {
      PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
      assertEquals(5, VectorIndexScanPlan.resolveMaxProbeLimit(null, null, pConn));
    }

    Properties props3 = new Properties();
    props3.setProperty("MAX_PROBE_LIMIT", "6");
    try (Connection conn = DriverManager.getConnection(getUrl(), props3)) {
      PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
      assertEquals(6, VectorIndexScanPlan.resolveMaxProbeLimit(null, null, pConn));
    }

    Properties props4 = new Properties();
    props4.setProperty("VECTOR_MAX_PROBE_LIMIT", "7");
    try (Connection conn = DriverManager.getConnection(getUrl(), props4)) {
      PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
      assertEquals(7, VectorIndexScanPlan.resolveMaxProbeLimit(null, null, pConn));
    }
  }

  @Test
  public void testRemoveFilter() {
    SkipScanFilter centroidFilter = new SkipScanFilter();
    Filter other = new org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter();

    assertNull(VectorIndexScanPlan.removeFilter(null, centroidFilter));
    assertSame(other, VectorIndexScanPlan.removeFilter(other, null));
    assertNull(VectorIndexScanPlan.removeFilter(centroidFilter, centroidFilter));
    assertSame(other, VectorIndexScanPlan.removeFilter(other, centroidFilter));

    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, centroidFilter, other);
    assertSame(other, VectorIndexScanPlan.removeFilter(list, centroidFilter));

    // Filter matching identity is removed when multiple SkipScanFilter instances exist
    SkipScanFilter whereFilter = new SkipScanFilter();
    FilterList twoSkipScans =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, centroidFilter, whereFilter, other);
    Filter stripped = VectorIndexScanPlan.removeFilter(twoSkipScans, centroidFilter);
    assertTrue("Expected remaining filters to remain a FilterList", stripped instanceof FilterList);
    List<Filter> remaining = ((FilterList) stripped).getFilters();
    assertEquals(2, remaining.size());
    assertSame(whereFilter, remaining.get(0));
    assertSame(other, remaining.get(1));

    // Removal within nested filter lists
    FilterList nested = new FilterList(FilterList.Operator.MUST_PASS_ALL, centroidFilter,
      new FilterList(FilterList.Operator.MUST_PASS_ONE, whereFilter, other));
    assertTrue(VectorIndexScanPlan.removeFilter(nested, centroidFilter) instanceof FilterList);
    FilterList noTarget = new FilterList(FilterList.Operator.MUST_PASS_ALL, whereFilter, other);
    assertSame(noTarget, VectorIndexScanPlan.removeFilter(noTarget, centroidFilter));
  }

  @Test
  public void testKeyRangeConstructionNonSaltedNonTenant() {
    List<Integer> centroidIds = Arrays.asList(5, 12, 27);
    List<KeyRange> ranges = VectorIndexScanPlan.buildCentroidKeyRanges(centroidIds, null, null);

    assertEquals(3, ranges.size());

    // Centroid 5: start key 5 inclusive, end key 6 exclusive
    KeyRange r0 = ranges.get(0);
    assertTrue(r0.isLowerInclusive());
    assertFalse(r0.isUpperInclusive());
    assertArrayEquals(new byte[] { (byte) 0x80, 0, 0, 5 }, r0.getLowerRange());
    assertArrayEquals(new byte[] { (byte) 0x80, 0, 0, 6 }, r0.getUpperRange());

    // Centroid 12: start key 12 inclusive, end key 13 exclusive
    KeyRange r1 = ranges.get(1);
    assertTrue(r1.isLowerInclusive());
    assertFalse(r1.isUpperInclusive());
    assertArrayEquals(new byte[] { (byte) 0x80, 0, 0, 12 }, r1.getLowerRange());
    assertArrayEquals(new byte[] { (byte) 0x80, 0, 0, 13 }, r1.getUpperRange());

    // Centroid 27: start key 27 inclusive, end key 28 exclusive
    KeyRange r2 = ranges.get(2);
    assertTrue(r2.isLowerInclusive());
    assertFalse(r2.isUpperInclusive());
    assertArrayEquals(new byte[] { (byte) 0x80, 0, 0, 27 }, r2.getLowerRange());
    assertArrayEquals(new byte[] { (byte) 0x80, 0, 0, 28 }, r2.getUpperRange());

    ScanRanges scanRanges = ScanRanges.createCentroidScanRanges(ranges);
    assertNotNull(scanRanges);
    assertTrue(scanRanges.useSkipScanFilter());
    assertNotNull(scanRanges.getSkipScanFilter());
    assertFalse(scanRanges.isPointLookup());
    assertEquals(1, scanRanges.getRanges().size());
    assertEquals(3, scanRanges.getRanges().get(0).size());
  }

  @Test
  public void testKeyRangeConstructionSalted() {
    List<Integer> centroidIds = Arrays.asList(5, 12, 27);
    List<KeyRange> ranges = VectorIndexScanPlan.buildCentroidKeyRanges(centroidIds, 4, null);

    // 3 centroids * 4 salt buckets = 12 key ranges
    assertEquals(12, ranges.size());

    Set<Byte> seenBuckets = new HashSet<>();
    Set<Integer> seenCentroids = new HashSet<>();

    for (KeyRange r : ranges) {
      assertTrue(r.isLowerInclusive());
      assertFalse(r.isUpperInclusive());
      assertEquals(5, r.getLowerRange().length); // 1 salt byte + 4 int bytes
      assertEquals(5, r.getUpperRange().length);

      byte bucket = r.getLowerRange()[0];
      assertEquals(bucket, r.getUpperRange()[0]);
      seenBuckets.add(bucket);

      byte[] startCidBytes = Arrays.copyOfRange(r.getLowerRange(), 1, 5);
      byte[] endCidBytes = Arrays.copyOfRange(r.getUpperRange(), 1, 5);

      int startCid = (Integer) PInteger.INSTANCE.toObject(startCidBytes);
      int endCid = (Integer) PInteger.INSTANCE.toObject(endCidBytes);

      assertEquals(startCid + 1, endCid);
      seenCentroids.add(startCid);
    }

    assertEquals(4, seenBuckets.size());
    assertTrue(seenBuckets.contains((byte) 0));
    assertTrue(seenBuckets.contains((byte) 1));
    assertTrue(seenBuckets.contains((byte) 2));
    assertTrue(seenBuckets.contains((byte) 3));

    assertEquals(new HashSet<>(centroidIds), seenCentroids);

    // Verify key range byte prefix and bounds for salt bucket 2 and centroid 12
    KeyRange b2c12 = ranges.get(7);
    assertArrayEquals(new byte[] { 0x02, (byte) 0x80, 0, 0, 12 }, b2c12.getLowerRange());
    assertArrayEquals(new byte[] { 0x02, (byte) 0x80, 0, 0, 13 }, b2c12.getUpperRange());

    // Verify ScanRanges creation for salted table
    ScanRanges scanRanges = ScanRanges.createCentroidScanRanges(ranges);
    assertNotNull(scanRanges);
    assertTrue(scanRanges.useSkipScanFilter());
    assertNotNull(scanRanges.getSkipScanFilter());
    assertFalse(scanRanges.isPointLookup());
    assertEquals(1, scanRanges.getRanges().size());
    assertEquals(12, scanRanges.getRanges().get(0).size());
  }

  @Test
  public void testKeyRangeConstructionTenantScoped() {
    byte[] tenantPrefix = ByteUtil.concat(Bytes.toBytes("T1"), QueryConstants.SEPARATOR_BYTE_ARRAY);
    List<Integer> centroidIds = Arrays.asList(5, 12, 27);
    List<KeyRange> ranges =
      VectorIndexScanPlan.buildCentroidKeyRanges(centroidIds, null, tenantPrefix);

    assertEquals(3, ranges.size());

    // Verify prefix and key range boundaries for tenant-scoped centroid range
    assertArrayEquals(new byte[] { 'T', '1', 0, (byte) 0x80, 0, 0, 5 },
      ranges.get(0).getLowerRange());
    assertArrayEquals(new byte[] { 'T', '1', 0, (byte) 0x80, 0, 0, 6 },
      ranges.get(0).getUpperRange());

    for (int i = 0; i < ranges.size(); i++) {
      KeyRange r = ranges.get(i);
      assertTrue(r.isLowerInclusive());
      assertFalse(r.isUpperInclusive());

      int expectedCid = centroidIds.get(i);
      assertEquals(tenantPrefix.length + 4, r.getLowerRange().length);
      assertEquals(tenantPrefix.length + 4, r.getUpperRange().length);

      // Verify tenant ID prefix
      byte[] lowerPrefix = Arrays.copyOfRange(r.getLowerRange(), 0, tenantPrefix.length);
      byte[] upperPrefix = Arrays.copyOfRange(r.getUpperRange(), 0, tenantPrefix.length);
      assertArrayEquals(tenantPrefix, lowerPrefix);
      assertArrayEquals(tenantPrefix, upperPrefix);

      // Verify centroid ID bytes
      byte[] lowerCidBytes =
        Arrays.copyOfRange(r.getLowerRange(), tenantPrefix.length, tenantPrefix.length + 4);
      byte[] upperCidBytes =
        Arrays.copyOfRange(r.getUpperRange(), tenantPrefix.length, tenantPrefix.length + 4);

      int lowerCid = (Integer) PInteger.INSTANCE.toObject(lowerCidBytes);
      int upperCid = (Integer) PInteger.INSTANCE.toObject(upperCidBytes);

      assertEquals(expectedCid, lowerCid);
      assertEquals(expectedCid + 1, upperCid);
    }

    // Verify ScanRanges creation for tenant-scoped ranges
    ScanRanges scanRanges = ScanRanges.createCentroidScanRanges(ranges);
    assertNotNull(scanRanges);
    assertTrue(scanRanges.useSkipScanFilter());
    assertNotNull(scanRanges.getSkipScanFilter());
    assertFalse(scanRanges.isPointLookup());
    assertEquals(1, scanRanges.getRanges().size());
    assertEquals(3, scanRanges.getRanges().get(0).size());
  }

  @Test
  public void testKeyRangesAreSortedForSkipScan() {
    List<Integer> centroidIds = Arrays.asList(27, 5, 12);
    List<KeyRange> ranges = VectorIndexScanPlan.buildCentroidKeyRanges(centroidIds, 2, null);

    assertEquals(6, ranges.size());

    List<KeyRange> expectedSorted = new ArrayList<>(ranges);
    Collections.sort(expectedSorted, KeyRange.COMPARATOR);
    assertEquals("Key ranges must be strictly sorted by KeyRange.COMPARATOR for SkipScanFilter",
      expectedSorted, ranges);
  }

  @Test
  public void testSkipScanFilterSeekingPostingLists() {
    List<Integer> centroidIds = Arrays.asList(5, 12, 27);
    List<KeyRange> ranges = VectorIndexScanPlan.buildCentroidKeyRanges(centroidIds, null, null);
    ScanRanges scanRanges = ScanRanges.createCentroidScanRanges(ranges);
    SkipScanFilter skipper = scanRanges.getSkipScanFilter();

    assertNotNull(skipper);
    assertFalse(skipper.filterAllRemaining());

    // Keys preceding the first matched centroid seek directly to the next probed centroid
    byte[] rowCentroid2 = ByteUtil.concat(PInteger.INSTANCE.toBytes(2), Bytes.toBytes("row0"));
    Cell kv2 = KeyValueUtil.createFirstOnRow(rowCentroid2);
    assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterCell(kv2));
    assertEquals(KeyValueUtil.createFirstOnRow(PInteger.INSTANCE.toBytes(5)),
      skipper.getNextCellHint(kv2));

    // Rows within the current centroid posting list are included
    byte[] rowCentroid5_1 = ByteUtil.concat(PInteger.INSTANCE.toBytes(5), Bytes.toBytes("row1"));
    Cell kv5_1 = KeyValueUtil.createFirstOnRow(rowCentroid5_1);
    assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, skipper.filterCell(kv5_1));

    byte[] rowCentroid5_2 = ByteUtil.concat(PInteger.INSTANCE.toBytes(5), Bytes.toBytes("row2"));
    Cell kv5_2 = KeyValueUtil.createFirstOnRow(rowCentroid5_2);
    assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, skipper.filterCell(kv5_2));

    // Gaps between probed centroids generate seek hints to the next matched centroid range
    byte[] rowCentroid8 = ByteUtil.concat(PInteger.INSTANCE.toBytes(8), Bytes.toBytes("row8"));
    Cell kv8 = KeyValueUtil.createFirstOnRow(rowCentroid8);
    assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterCell(kv8));
    assertEquals(KeyValueUtil.createFirstOnRow(PInteger.INSTANCE.toBytes(12)),
      skipper.getNextCellHint(kv8));

    // Rows in subsequent centroid posting lists are included
    byte[] rowCentroid12 = ByteUtil.concat(PInteger.INSTANCE.toBytes(12), Bytes.toBytes("row12"));
    Cell kv12 = KeyValueUtil.createFirstOnRow(rowCentroid12);
    assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, skipper.filterCell(kv12));

    // Gaps between non-consecutive probed centroids seek to the next matched range
    byte[] rowCentroid20 = ByteUtil.concat(PInteger.INSTANCE.toBytes(20), Bytes.toBytes("row20"));
    Cell kv20 = KeyValueUtil.createFirstOnRow(rowCentroid20);
    assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterCell(kv20));
    assertEquals(KeyValueUtil.createFirstOnRow(PInteger.INSTANCE.toBytes(27)),
      skipper.getNextCellHint(kv20));

    // Rows in the final centroid posting list are included
    byte[] rowCentroid27 = ByteUtil.concat(PInteger.INSTANCE.toBytes(27), Bytes.toBytes("row27"));
    Cell kv27 = KeyValueUtil.createFirstOnRow(rowCentroid27);
    assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, skipper.filterCell(kv27));

    // Keys past the final probed centroid return NEXT_ROW and terminate iteration
    byte[] rowCentroid30 = ByteUtil.concat(PInteger.INSTANCE.toBytes(30), Bytes.toBytes("row30"));
    Cell kv30 = KeyValueUtil.createFirstOnRow(rowCentroid30);
    assertEquals(ReturnCode.NEXT_ROW, skipper.filterCell(kv30));
    assertTrue(skipper.filterAllRemaining());
  }

  @Test
  public void testPlanConstructionWithExplicitCentroids() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = new PhoenixStatement(conn);
      SelectStatement select =
        new org.apache.phoenix.parse.SQLParser("SELECT * FROM ATABLE").parseQuery();
      ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn);
      StatementContext context2 =
        new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
      StatementContext context8 =
        new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));

      // 16 2D centroids
      float[][] centroids = new float[16][2];
      for (int i = 0; i < 16; i++) {
        centroids[i] = new float[] { (float) i, (float) i };
      }
      CachedCentroids cached = new CachedCentroids("TEST_IDX", 1L, centroids);
      float[] query = new float[] { 2.0f, 2.0f };

      TableRef tableRef = resolver.getTables().get(0);

      VectorIndexScanPlan plan2 = new VectorIndexScanPlan(context2, select, tableRef,
        RowProjector.EMPTY_PROJECTOR, 10, 0, OrderBy.EMPTY_ORDER_BY, null, false, null,
        org.apache.phoenix.thirdparty.com.google.common.base.Optional.absent(), cached, query, "L2",
        2);

      VectorIndexScanPlan plan8 = new VectorIndexScanPlan(context8, select, tableRef,
        RowProjector.EMPTY_PROJECTOR, 10, 0, OrderBy.EMPTY_ORDER_BY, null, false, null,
        org.apache.phoenix.thirdparty.com.google.common.base.Optional.absent(), cached, query, "L2",
        8);

      assertEquals(2, plan2.getProbeCount());
      assertEquals(2, plan2.getProbeCentroids().size());
      assertEquals(Integer.valueOf(2), plan2.getProbeCentroids().get(0));
      assertEquals(2, plan2.getKeyRanges().size());

      assertEquals(8, plan8.getProbeCount());
      assertEquals(8, plan8.getProbeCentroids().size());
      assertEquals(8, plan8.getKeyRanges().size());

      // Verify ScanRanges was set on context2
      assertNotNull(context2.getScanRanges());
      assertTrue(context2.getScanRanges().useSkipScanFilter());
      assertNotNull(context2.getScanRanges().getSkipScanFilter());

      // Verify SkipScanFilter is attached to context scan
      assertNotNull(context2.getScan().getFilter());
      assertTrue("Scan filter should be SkipScanFilter",
        context2.getScan().getFilter() instanceof SkipScanFilter);

      // Verify ExplainPlan includes probe info
      ExplainPlan explain = plan2.getExplainPlan();
      assertNotNull(explain);
      assertTrue(explain.getPlanSteps().get(0).contains("CLIENT PROBING 2 OF 16 CENTROIDS"));

      // Cost model check: if base cost is known, cost8 should be 4x cost2
      Cost cost2 = plan2.getCost();
      Cost cost8 = plan8.getCost();
      assertNotNull(cost2);
      assertNotNull(cost8);
      if (cost2.isUnknown()) {
        assertTrue(cost8.isUnknown());
      } else {
        assertEquals(4.0, cost8.getCpu() / cost2.getCpu(), 0.001);
      }
    }
  }

  @Test
  public void testNoQueryVectorScansWholeIndex() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = new PhoenixStatement(conn);
      SelectStatement select =
        new org.apache.phoenix.parse.SQLParser("SELECT * FROM ATABLE").parseQuery();
      ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn);
      StatementContext context =
        new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));

      float[][] centroids = new float[16][2];
      for (int i = 0; i < 16; i++) {
        centroids[i] = new float[] { (float) i, (float) i };
      }
      CachedCentroids cached = new CachedCentroids("TEST_IDX", 1L, centroids);
      TableRef tableRef = resolver.getTables().get(0);

      VectorIndexScanPlan plan = new VectorIndexScanPlan(context, select, tableRef,
        RowProjector.EMPTY_PROJECTOR, 10, 0, OrderBy.EMPTY_ORDER_BY, null, false, null,
        org.apache.phoenix.thirdparty.com.google.common.base.Optional.absent(), cached,
        (float[]) null, "L2", 5);

      assertFalse("Plan should not be probing when query vector is null", plan.isProbing());
      assertTrue("Key ranges should be empty when not probing", plan.getKeyRanges().isEmpty());

      // context.getScanRanges() unchanged (not a skip scan)
      assertFalse("Context should not have skip scan filter",
        context.getScanRanges().useSkipScanFilter());

      // getExplainPlan() has no CLIENT PROBING step
      ExplainPlan explain = plan.getExplainPlan();
      assertNotNull(explain);
      for (String step : explain.getPlanSteps()) {
        assertFalse("Explain plan should not contain CLIENT PROBING",
          step.contains("CLIENT PROBING"));
      }
    }
  }

  @Test
  public void testExtractQueryVectorAndMetricFromOrderBy() throws Exception {
    String tableName = "T_EXTRACT_TEST";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (pk INTEGER NOT NULL PRIMARY KEY, v VECTOR(FLOAT, 3))");
      }

      String[] functions =
        new String[] { "L2_DISTANCE", "L2_DISTANCE_SQUARED", "COSINE_DISTANCE", "INNER_PRODUCT" };
      String[] expectedMetrics = new String[] { "L2", "L2", "COSINE", "INNER_PRODUCT" };
      Float[] boxedVec = new Float[] { 1.5f, 2.5f, 3.5f };
      float[] expectedVec = new float[] { 1.5f, 2.5f, 3.5f };

      for (int i = 0; i < functions.length; i++) {
        String fn = functions[i];
        String expectedMetric = expectedMetrics[i];

        // Function evaluation with column vector first and parameter query vector
        String sql1 = "SELECT pk FROM " + tableName + " ORDER BY " + fn + "(v, ?) LIMIT 5";
        try (PreparedStatement ps = conn.prepareStatement(sql1)) {
          ps.setArray(1, conn.createArrayOf("FLOAT", boxedVec));
          PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
          org.apache.phoenix.compile.QueryPlan qPlan = pps.optimizeQuery();
          OrderBy orderBy = qPlan.getOrderBy();
          assertNotNull(orderBy);
          Pair<float[], String> pair =
            VectorIndexScanPlan.extractQueryVectorAndMetric(orderBy, null);
          assertNotNull("Failed to extract query vector for fn " + fn + "(v, ?)", pair);
          assertArrayEquals(expectedVec, pair.getFirst(), 1e-6f);
          assertEquals(expectedMetric, pair.getSecond());
        }

        // Swapped arguments: query vector first and column vector second
        String sql2 = "SELECT pk FROM " + tableName + " ORDER BY " + fn + "(?, v) LIMIT 5";
        try (PreparedStatement ps = conn.prepareStatement(sql2)) {
          ps.setArray(1, conn.createArrayOf("FLOAT", boxedVec));
          PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
          org.apache.phoenix.compile.QueryPlan qPlan = pps.optimizeQuery();
          OrderBy orderBy = qPlan.getOrderBy();
          assertNotNull(orderBy);
          Pair<float[], String> pair =
            VectorIndexScanPlan.extractQueryVectorAndMetric(orderBy, null);
          assertNotNull("Failed to extract query vector for fn " + fn + "(?, v)", pair);
          assertArrayEquals(expectedVec, pair.getFirst(), 1e-6f);
          assertEquals(expectedMetric, pair.getSecond());
        }

        // Literal array query vector
        String sql3 =
          "SELECT pk FROM " + tableName + " ORDER BY " + fn + "(v, ARRAY[1.5, 2.5, 3.5]) LIMIT 5";
        try (PreparedStatement ps = conn.prepareStatement(sql3)) {
          PhoenixPreparedStatement pps = ps.unwrap(PhoenixPreparedStatement.class);
          org.apache.phoenix.compile.QueryPlan qPlan = pps.optimizeQuery();
          OrderBy orderBy = qPlan.getOrderBy();
          assertNotNull(orderBy);
          Pair<float[], String> pair =
            VectorIndexScanPlan.extractQueryVectorAndMetric(orderBy, null);
          assertNotNull("Failed to extract query vector for literal in " + fn, pair);
          assertArrayEquals(expectedVec, pair.getFirst(), 1e-6f);
          assertEquals(expectedMetric, pair.getSecond());
        }
      }
    }
  }

  @Test
  public void testToFloatArrayConversions() throws SQLException {
    // Primitive float array
    float[] fa = new float[] { 1.0f, 2.0f, 3.0f };
    assertArrayEquals(fa, VectorIndexScanPlan.toFloatArray(fa), 1e-6f);

    // Boxed Float array with null element mapped to 0.0f
    Float[] boxedF = new Float[] { 1.0f, null, 3.0f };
    assertArrayEquals(new float[] { 1.0f, 0.0f, 3.0f }, VectorIndexScanPlan.toFloatArray(boxedF),
      1e-6f);

    // Primitive double array
    double[] da = new double[] { 1.1, 2.2, 3.3 };
    assertArrayEquals(new float[] { 1.1f, 2.2f, 3.3f }, VectorIndexScanPlan.toFloatArray(da),
      1e-6f);

    // Boxed Double array with null element mapped to 0.0f
    Double[] boxedD = new Double[] { 1.1, null, 3.3 };
    assertArrayEquals(new float[] { 1.1f, 0.0f, 3.3f }, VectorIndexScanPlan.toFloatArray(boxedD),
      1e-6f);

    // Mixed Number array converted to float array
    Number[] numbers = new Number[] { 1, 2L, 3.5f, null };
    assertArrayEquals(new float[] { 1.0f, 2.0f, 3.5f, 0.0f },
      VectorIndexScanPlan.toFloatArray(numbers), 1e-6f);

    // JDBC SQL Array wrapping float elements
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Array sqlArray = conn.createArrayOf("FLOAT", new Float[] { 4.0f, 5.0f });
      assertArrayEquals(new float[] { 4.0f, 5.0f }, VectorIndexScanPlan.toFloatArray(sqlArray),
        1e-6f);
    }

    // Unsupported object types return null
    assertNull(VectorIndexScanPlan.toFloatArray("unsupported string"));
    assertNull(VectorIndexScanPlan.toFloatArray(12345));
    assertNull(VectorIndexScanPlan.toFloatArray(null));
  }
}
