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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.MaterializedResultIterator;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.DelegateTable;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

/**
 * Unit tests for {@link VectorIndexScanPlan}: posting list key ranges with salt and tenant
 * prefixes, the conditions that cause a full index scan, query vector extraction, probe hints, and
 * the interleave of generation rankings.
 */
public class VectorIndexScanPlanTest extends BaseConnectionlessQueryTest {

  private static final String INDEX_OPTIONS =
    " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 32, sample_size = 100) ASYNC";

  private static StatementContext indexContext(Connection conn, String indexName)
    throws SQLException {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    PTable index = pconn.getTable(indexName);
    return new StatementContext(new PhoenixStatement(pconn),
      FromCompiler.getResolver(new TableRef(index)));
  }

  private static PTable index(Connection conn, String indexName) throws SQLException {
    return conn.unwrap(PhoenixConnection.class).getTable(indexName);
  }

  private static ReturnCode filter(SkipScanFilter filter, byte[] first, byte[]... rest) {
    Cell cell = KeyValueUtil.createFirstOnRow(ByteUtil.concat(first, rest));
    return filter.filterCell(cell);
  }

  private static byte[] centroid(int id) {
    return PInteger.INSTANCE.toBytes(id);
  }

  @Test
  public void testPostingListRangesSeekBetweenProbedCentroids() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_PL (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_PL ON T_PL" + INDEX_OPTIONS);
      ScanRanges ranges = VectorIndexScanPlan.getPostingListRanges(indexContext(conn, "I_PL"),
        index(conn, "I_PL"), new int[] { 27, 5, 12 });
      assertNotNull(ranges);
      SkipScanFilter skip = ranges.getSkipScanFilter();
      byte[] row = Bytes.toBytes("r");
      assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, filter(skip, centroid(2), row));
      assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, filter(skip, centroid(5), row));
      assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, filter(skip, centroid(8), row));
      assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, filter(skip, centroid(12), row));
      assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, filter(skip, centroid(27), row));
      assertEquals(ReturnCode.NEXT_ROW, filter(skip, centroid(30), row));
    }
  }

  @Test
  public void testPostingListRangesSpanEverySaltBucket() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE T_PLS (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4)) SALT_BUCKETS=4");
      conn.createStatement().execute("CREATE VECTOR INDEX I_PLS ON T_PLS" + INDEX_OPTIONS);
      PTable index = index(conn, "I_PLS");
      assertEquals(Integer.valueOf(4), index.getBucketNum());
      ScanRanges ranges = VectorIndexScanPlan.getPostingListRanges(indexContext(conn, "I_PLS"),
        index, new int[] { 7 });
      assertNotNull(ranges);
      byte[] row = Bytes.toBytes("r");
      // The skip scan filter must seek forward to the probed centroid in each salt bucket
      SkipScanFilter skip = ranges.getSkipScanFilter();
      assertEquals(ReturnCode.SEEK_NEXT_USING_HINT,
        filter(skip, new byte[] { 0 }, centroid(6), row));
      for (int bucket = 0; bucket < 4; bucket++) {
        assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL,
          filter(skip, new byte[] { (byte) bucket }, centroid(7), row));
      }
    }
  }

  /** Posting list key ranges must include the padding of a fixed-width CHAR tenant ID. */
  @Test
  public void testPostingListRangesUnderPaddedCharTenant() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_PLT (TENANT_ID CHAR(15) NOT NULL, ID VARCHAR NOT NULL, "
          + "V VECTOR(FLOAT, 4) CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true");
      conn.createStatement().execute("CREATE VECTOR INDEX I_PLT ON T_PLT" + INDEX_OPTIONS);
    }
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "T1");
    try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
      ScanRanges ranges = VectorIndexScanPlan.getPostingListRanges(
        indexContext(tenantConn, "I_PLT"), index(tenantConn, "I_PLT"), new int[] { 3 });
      assertNotNull(ranges);
      byte[] padded = Bytes.toBytes(String.format("%-15s", "T1"));
      byte[] row = Bytes.toBytes("r");
      SkipScanFilter skip = ranges.getSkipScanFilter();
      assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, filter(skip, padded, centroid(2), row));
      assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, filter(skip, padded, centroid(3), row));
    }
  }

  /**
   * A multi-tenant index with no tenant context gets no posting list ranges. The plan then scans
   * the whole index.
   */
  @Test
  public void testMultiTenantIndexWithoutTenantScansWholeIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_PLG (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL, "
          + "V VECTOR(FLOAT, 4) CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true");
      conn.createStatement().execute("CREATE VECTOR INDEX I_PLG ON T_PLG" + INDEX_OPTIONS);
      assertNull(VectorIndexScanPlan.getPostingListRanges(indexContext(conn, "I_PLG"),
        index(conn, "I_PLG"), new int[] { 1 }));
    }
  }

  /** The empty scan range of a WHERE clause that is always false must not be replaced. */
  @Test
  public void testDegenerateWhereIsNotReplaced() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_PLD (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_PLD ON T_PLD" + INDEX_OPTIONS);
      StatementContext context = indexContext(conn, "I_PLD");
      context.setScanRanges(ScanRanges.NOTHING);
      assertNull(
        VectorIndexScanPlan.getPostingListRanges(context, index(conn, "I_PLD"), new int[] { 1 }));
    }
  }

  @Test
  public void testQueryVectorFromOrderBy() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_QV (PK INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 3), "
          + "W VECTOR(DOUBLE, 3))");
      for (String sql : new String[] { "SELECT PK FROM T_QV ORDER BY L2_DISTANCE(V, ?) LIMIT 5",
        "SELECT PK FROM T_QV ORDER BY COSINE_DISTANCE(?, V) LIMIT 5",
        "SELECT PK FROM T_QV ORDER BY INNER_PRODUCT(W, ?) LIMIT 5" }) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1.5f, 2.5f, 3.5f }));
          float[] vector = VectorIndexScanPlan
            .getQueryVector(ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getOrderBy());
          assertArrayEquals(sql, new float[] { 1.5f, 2.5f, 3.5f }, vector, 1e-6f);
        }
      }
      try (
        PreparedStatement ps = conn.prepareStatement("SELECT PK FROM T_QV ORDER BY PK LIMIT 5")) {
        assertNull(VectorIndexScanPlan
          .getQueryVector(ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getOrderBy()));
      }
    }
  }

  @Test
  public void testProbeHints() {
    HintNode hint = new HintNode("/*+ VECTOR_PROBE_COUNT(5) MAX_PROBE_LIMIT(3) */");
    assertEquals(5, VectorIndexScanPlan.getHintInt(hint, Hint.VECTOR_PROBE_COUNT, 0));
    assertEquals(3, VectorIndexScanPlan.getHintInt(hint, Hint.MAX_PROBE_LIMIT, 8));
    HintNode none = new HintNode("/*+ INDEX(T I) */");
    assertEquals(7, VectorIndexScanPlan.getHintInt(none, Hint.VECTOR_PROBE_COUNT, 7));
    HintNode invalid = new HintNode("/*+ VECTOR_PROBE_COUNT(abc) */");
    assertEquals(7, VectorIndexScanPlan.getHintInt(invalid, Hint.VECTOR_PROBE_COUNT, 7));
    // For a repeated hint the first value applies, as the INDEX hint uses its first index
    HintNode repeated = new HintNode("/*+ VECTOR_PROBE_COUNT(4) VECTOR_PROBE_COUNT(8) */");
    assertEquals(4, VectorIndexScanPlan.getHintInt(repeated, Hint.VECTOR_PROBE_COUNT, 7));
    HintNode bare = new HintNode("/*+ VECTOR_PROBE_COUNT */");
    assertEquals(7, VectorIndexScanPlan.getHintInt(bare, Hint.VECTOR_PROBE_COUNT, 7));
  }

  @Test
  public void testInterleaveAlternatesGenerationRankings() {
    // Each prefix of length 2n holds the n nearest centroids of each generation. The longer
    // ranking puts its extra centroids at the end.
    assertArrayEquals(new int[] { 0, 4, 1, 5, 2, 6 },
      VectorIndexScanPlan.interleave(new int[] { 0, 1, 2 }, new int[] { 4, 5, 6 }));
    assertArrayEquals(new int[] { 3, 9, 1, 8, 7 },
      VectorIndexScanPlan.interleave(new int[] { 3, 1 }, new int[] { 9, 8, 7 }));
    assertArrayEquals(new int[] { 2, 0 },
      VectorIndexScanPlan.interleave(new int[] { 2, 0 }, new int[0]));
  }

  private static Tuple indexRow(byte[] first, byte[]... rest) {
    return new SingleKeyValueTuple(KeyValueUtil.createFirstOnRow(ByteUtil.concat(first, rest)));
  }

  private static ImmutableBytesPtr dataRowKey(PTable index, byte[] first, byte[]... rest) {
    return VectorIndexScanPlan.getDataRowKey(indexRow(first, rest), index,
      VectorIndexScanPlan.getCentroidPosition(index));
  }

  private static byte[] tenant(String id) {
    return ByteUtil.concat(Bytes.toBytes(id), QueryConstants.SEPARATOR_BYTE_ARRAY);
  }

  /**
   * Verifies that a row read under both generations during a migration gives one data row key. Rows
   * of different tenants with the same primary key give different keys.
   */
  @Test
  public void testDataRowKeyIgnoresGenerationPrefix() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE T_DK (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4)) SALT_BUCKETS=4");
      conn.createStatement().execute("CREATE VECTOR INDEX I_DK ON T_DK" + INDEX_OPTIONS);
      PTable salted = index(conn, "I_DK");
      byte[] row1 = Bytes.toBytes("row1");
      ImmutableBytesPtr active = dataRowKey(salted, new byte[] { 1 }, centroid(3), row1);
      assertEquals(active, dataRowKey(salted, new byte[] { 2 }, centroid(17), row1));
      assertNotEquals(active,
        dataRowKey(salted, new byte[] { 1 }, centroid(3), Bytes.toBytes("row2")));

      conn.createStatement()
        .execute("CREATE TABLE T_DKT (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL, "
          + "V VECTOR(FLOAT, 4) CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true, "
          + "SALT_BUCKETS=4");
      conn.createStatement().execute("CREATE VECTOR INDEX I_DKT ON T_DKT" + INDEX_OPTIONS);
      PTable tenant = index(conn, "I_DKT");
      byte[] salt = new byte[] { 0 };
      ImmutableBytesPtr key = dataRowKey(tenant, salt, tenant("T1"), centroid(3), row1);
      assertEquals(key, dataRowKey(tenant, new byte[] { 3 }, tenant("T1"), centroid(17), row1));
      assertNotEquals(key, dataRowKey(tenant, salt, tenant("T2"), centroid(17), row1));
      assertNotEquals(key, dataRowKey(tenant, salt, tenant("T2"), centroid(3), row1));
      assertNotEquals(key, dataRowKey(tenant, salt, tenant("T1"), centroid(3), Bytes.toBytes("r")));
    }
  }

  private static ResultIterators regions(List<Tuple>... regions) {
    List<PeekingResultIterator> iterators = new ArrayList<>();
    for (List<Tuple> region : regions) {
      iterators.add(new MaterializedResultIterator(region));
    }
    return new ResultIterators() {
      @Override
      public int size() {
        return iterators.size();
      }

      @Override
      public List<KeyRange> getSplits() {
        return Collections.emptyList();
      }

      @Override
      public List<List<Scan>> getScans() {
        return Collections.emptyList();
      }

      @Override
      public void explain(List<String> planSteps) {
      }

      @Override
      public List<PeekingResultIterator> getIterators() {
        return iterators;
      }

      @Override
      public void explain(List<String> planSteps,
        ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
      }

      @Override
      public void close() {
      }
    };
  }

  /**
   * Verifies that the next distinct row from the regions replaces a duplicate copy of a data row
   * that the merge drops during a migration. The merge must not stop at the limit first.
   */
  @Test
  public void testDuplicateRowLeavesRoomForNextDistinctRow() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_DD (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_DD ON T_DD" + INDEX_OPTIONS);
      PTable index = index(conn, "I_DD");
      int centroidPosition = VectorIndexScanPlan.getCentroidPosition(index);
      // Order rows by data primary key in place of their distance to the query vector
      RowKeyColumnExpression id =
        new RowKeyColumnExpression(index.getPKColumns().get(centroidPosition + 1),
          new RowKeyValueAccessor(index.getPKColumns(), centroidPosition + 1));
      List<OrderByExpression> orderBy = Collections
        .singletonList(OrderByExpression.createByCheckIfExpressionSortOrderDesc(id, true, true));
      byte[] a = Bytes.toBytes("a");
      Tuple aActive = indexRow(centroid(3), a);
      Tuple b = indexRow(centroid(17), Bytes.toBytes("b"));
      Tuple c = indexRow(centroid(3), Bytes.toBytes("c"));
      Tuple d = indexRow(centroid(17), Bytes.toBytes("d"));
      ResultIterators batch =
        regions(Arrays.asList(aActive, c), Arrays.asList(indexRow(centroid(17), a), b, d));
      List<Tuple> found = new ArrayList<>();
      VectorIndexScanPlan.drain(batch, 3, orderBy, new HashSet<>(), index, centroidPosition, found);
      assertEquals(Arrays.asList(aActive, b, c), found);

      // Without a building generation, the merge stops at the limit
      found.clear();
      VectorIndexScanPlan.drain(regions(Arrays.asList(aActive, c), Arrays.asList(b)), 2, orderBy,
        null, index, centroidPosition, found);
      assertEquals(Arrays.asList(aActive, b), found);
    }
  }

  private static int topN(Scan scan) throws Exception {
    return WritableUtils.readVInt(new DataInputStream(
      new ByteArrayInputStream(scan.getAttribute(BaseScannerRegionObserverConstants.TOPN))));
  }

  /**
   * Verifies that during a migration each region returns twice the rows that the client keeps.
   * Otherwise the two index rows of one data row can use two slots of a region, and the query
   * returns fewer rows than its limit.
   */
  @Test
  public void testServerTopNDoubledWhileBuildingGenerationIsProbed() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_TN (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 2))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_TN ON T_TN" + INDEX_OPTIONS);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable index = index(conn, "I_TN");
      VectorCentroidCache cache =
        VectorCentroidCache.getInstance(pconn.getQueryServices().getConfiguration());
      cache.put("I_TN", 1L, new CachedCentroids(
        Arrays.asList(new float[] { 0, 0 }, new float[] { 10, 10 }), DistanceMetric.L2));
      cache.put("I_TN", 2L, new CachedCentroids(
        Arrays.asList(new float[] { 1, 1 }, new float[] { 9, 9 }), DistanceMetric.L2, 2));
      String sql = "SELECT ID FROM T_TN ORDER BY L2_DISTANCE(V, ?) LIMIT 3 OFFSET 2";
      for (Long building : new Long[] { null, 2L }) {
        pconn.addTable(new DelegateTable(index) {
          @Override
          public PIndexState getIndexState() {
            return PIndexState.ACTIVE;
          }

          @Override
          public Long getVectorCentroidGeneration() {
            return 1L;
          }

          @Override
          public Long getVectorBuildingGeneration() {
            return building;
          }
        }, index.getTimeStamp());
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.5f, 0.5f }));
          VectorIndexScanPlan plan =
            (VectorIndexScanPlan) ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
          assertEquals(building == null ? 2 : 4, plan.getCentroidCount());
          assertEquals(building == null ? 5 : 10, topN(plan.getContext().getScan()));
        }
      }
    }
  }
}
