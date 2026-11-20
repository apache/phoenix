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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.MaterializedResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.ResultIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionlessQueryServicesImpl;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.DelegateTable;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsInfoBuilder;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
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

  /** Returns a scan grouper that reports the given number of index regions. */
  private static ParallelScanGrouper regionGrouper(int regions) {
    return new DefaultParallelScanGrouper() {
      @Override
      public List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName) {
        List<HRegionLocation> locations = new ArrayList<>(regions);
        for (int i = 0; i < regions; i++) {
          locations.add(
            new HRegionLocation(RegionInfoBuilder.newBuilder(TableName.valueOf(tableName)).build(),
              ServerName.valueOf("localhost", 1, 1)));
        }
        return locations;
      }
    };
  }

  /**
   * Returns a scan grouper that reports index regions with the given split keys. The regions cover
   * the full key range.
   */
  private static ParallelScanGrouper splitGrouper(byte[]... splits) {
    return new DefaultParallelScanGrouper() {
      @Override
      public List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName) {
        List<HRegionLocation> locations = new ArrayList<>(splits.length + 1);
        for (int i = 0; i <= splits.length; i++) {
          locations.add(new HRegionLocation(
            RegionInfoBuilder.newBuilder(TableName.valueOf(tableName))
              .setStartKey(i == 0 ? ByteUtil.EMPTY_BYTE_ARRAY : splits[i - 1])
              .setEndKey(i == splits.length ? ByteUtil.EMPTY_BYTE_ARRAY : splits[i]).build(),
            ServerName.valueOf("localhost", 1, 1)));
        }
        return locations;
      }
    };
  }

  /**
   * Makes a table with a vector index of eight trained centroids, and returns a connection with the
   * given read type bound in bytes. A null bound keeps the default bound. If {@code indexBytes} is
   * not null, the statistics cache gets guideposts with that total size for the index.
   */
  private static Connection trainedIndex(String table, String index, Long bound, Long indexBytes)
    throws Exception {
    return trainedIndex(table, index, bound == null ? null : Long.toString(bound), indexBytes, "",
      0);
  }

  /**
   * Makes a trained index as above, with the read type bound as text. The table gets the given
   * options, and the centroid IDs start at {@code firstId}.
   */
  private static Connection trainedIndex(String table, String index, String bound, Long indexBytes,
    String tableOptions, int firstId) throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    if (bound != null) {
      props.setProperty(QueryServices.VECTOR_PREAD_MAX_BYTES_PER_REGION_ATTRIB, bound);
    }
    Connection conn = DriverManager.getConnection(getUrl(), props);
    conn.createStatement().execute("CREATE TABLE " + table
      + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 2))" + tableOptions);
    conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table + INDEX_OPTIONS);
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      vectors.add(new float[] { i, i });
    }
    VectorCentroidCache.getInstance(pconn.getQueryServices().getConfiguration()).put(index, 1L,
      new CachedCentroids(vectors, DistanceMetric.L2, firstId));
    PTable indexTable = index(conn, index);
    pconn.addTable(new DelegateTable(indexTable) {
      @Override
      public PIndexState getIndexState() {
        return PIndexState.ACTIVE;
      }

      @Override
      public Long getVectorCentroidGeneration() {
        return 1L;
      }
    }, indexTable.getTimeStamp());
    if (indexBytes != null) {
      putIndexStats(conn, index, guideposts(indexBytes));
    }
    return conn;
  }

  /** Returns two guideposts with the given total bytes. */
  private static GuidePostsInfo guideposts(long bytes) {
    GuidePostsInfoBuilder builder = new GuidePostsInfoBuilder();
    builder.trackGuidePost(new ImmutableBytesWritable(centroid(2)), bytes / 2, 1, 0);
    builder.trackGuidePost(new ImmutableBytesWritable(centroid(6)), bytes - bytes / 2, 1, 0);
    return builder.build();
  }

  /** Puts the given statistics into the statistics cache for each column family of the index. */
  private static void putIndexStats(Connection conn, String indexName, GuidePostsInfo info)
    throws SQLException {
    PTable index = index(conn, indexName);
    List<byte[]> families = new ArrayList<>();
    for (PColumnFamily family : index.getColumnFamilies()) {
      families.add(family.getName().getBytes());
    }
    if (families.isEmpty()) {
      families.add(SchemaUtil.getEmptyColumnFamily(index));
    }
    for (byte[] family : families) {
      putFamilyStats(conn, index, family, info);
    }
  }

  /** Puts the given statistics into the statistics cache for one column family of the index. */
  private static void putFamilyStats(Connection conn, PTable index, byte[] family,
    GuidePostsInfo info) throws SQLException {
    // A connection with its own properties wraps the shared query services in a delegate
    ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
    while (services instanceof DelegateConnectionQueryServices) {
      services = ((DelegateConnectionQueryServices) services).getDelegate();
    }
    ((ConnectionlessQueryServicesImpl) services)
      .addTableStats(new GuidePostsKey(index.getPhysicalName().getBytes(), family), info);
  }

  /** Compiles a vector query on the trained index with the given probe count and batch limit. */
  private static VectorIndexScanPlan plan(Connection conn, String table, int probes, int batches,
    String hints) throws Exception {
    PreparedStatement ps =
      conn.prepareStatement("SELECT /*+ VECTOR_PROBE_COUNT(" + probes + ") MAX_PROBE_LIMIT("
        + batches + ") " + hints + " */ ID FROM " + table + " ORDER BY L2_DISTANCE(V, ?) LIMIT 3");
    ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.5f, 0.5f }));
    VectorIndexScanPlan plan =
      (VectorIndexScanPlan) ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
    assertEquals(probes, plan.getProbeCount());
    assertEquals(batches > 1 && probes < 8, plan.isAdaptive());
    return plan;
  }

  /** Returns the posting list ranges of a later batch, as the plan makes them. */
  private static ScanRanges batchRanges(Connection conn, VectorIndexScanPlan plan, int[] ids)
    throws SQLException {
    PTable index = plan.getTableRef().getTable();
    ScanRanges ranges = VectorIndexScanPlan
      .getPostingListRanges(indexContext(conn, index.getName().getString()), index, ids);
    assertNotNull(ranges);
    return ranges;
  }

  /**
   * Makes the iterator of the plan over the given number of regions, and closes it. Returns the
   * read type of the statement scan. Each region scan of the first batch must have the same read
   * type.
   */
  private static Scan.ReadType firstBatchReadType(VectorIndexScanPlan plan, int regions)
    throws Exception {
    return firstBatchReadType(plan, regionGrouper(regions));
  }

  /** Makes the iterator of the plan with the given scan grouper, as above. */
  private static Scan.ReadType firstBatchReadType(VectorIndexScanPlan plan,
    ParallelScanGrouper grouper) throws Exception {
    ResultIterator iterator = plan.iterator(grouper);
    iterator.close();
    Scan.ReadType readType = plan.getContext().getScan().getReadType();
    assertFalse(plan.getScans().isEmpty());
    for (List<Scan> scans : plan.getScans()) {
      for (Scan scan : scans) {
        assertEquals(readType, scan.getReadType());
      }
    }
    return readType;
  }

  /** Returns a scan grouper that fails on a lookup of the index regions for the read type. */
  private static ParallelScanGrouper noRegionLookup() {
    return new DefaultParallelScanGrouper() {
      @Override
      public List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName) {
        throw new AssertionError("unexpected region lookup");
      }
    };
  }

  private static final long MB = 1024L * 1024L;

  @Test
  public void testBoundedReadTypeRule() {
    // A region scan reads at least one posting list, also if the batch is narrower than the
    // region count
    assertEquals(Scan.ReadType.PREAD, VectorIndexScanPlan.boundedReadType(1, 4, 100L, 100));
    assertEquals(Scan.ReadType.DEFAULT, VectorIndexScanPlan.boundedReadType(1, 4, 101L, 100));
    // The lists of a batch spread over the regions
    assertEquals(Scan.ReadType.PREAD, VectorIndexScanPlan.boundedReadType(64, 4, MB, 16 * MB));
    assertEquals(Scan.ReadType.DEFAULT, VectorIndexScanPlan.boundedReadType(65, 4, MB, 16 * MB));
    assertEquals(Scan.ReadType.PREAD, VectorIndexScanPlan.boundedReadType(3, 2, 100L, 150));
    // A large posting list is above the default bound for any width and region count
    assertEquals(Scan.ReadType.DEFAULT,
      VectorIndexScanPlan.boundedReadType(1, 12, 30 * MB, 16 * MB));
    // No estimate, or a bound of 0 or less, gives the default read type
    assertEquals(Scan.ReadType.DEFAULT, VectorIndexScanPlan.boundedReadType(1, 4, null, 16 * MB));
    assertEquals(Scan.ReadType.DEFAULT, VectorIndexScanPlan.boundedReadType(1, 4, 1L, 0));
    assertEquals(Scan.ReadType.DEFAULT, VectorIndexScanPlan.boundedReadType(1, 4, 1L, -1));
  }

  /** The index size comes from the guideposts. An empty guidepost or no statistics give none. */
  @Test
  public void testIndexBytesFromStatistics() throws Exception {
    try (Connection conn = trainedIndex("T_PRS", "I_PRS", null, null)) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      assertNull(VectorIndexScanPlan.getIndexBytes(pconn, index(conn, "I_PRS")));
      putIndexStats(conn, "I_PRS", guideposts(801));
      assertEquals(Long.valueOf(801),
        VectorIndexScanPlan.getIndexBytes(pconn, index(conn, "I_PRS")));
      putIndexStats(conn, "I_PRS", GuidePostsInfo.createEmptyGuidePost(300 * MB, 0));
      assertNull(VectorIndexScanPlan.getIndexBytes(pconn, index(conn, "I_PRS")));
    }
  }

  /**
   * The first batch uses PREAD under the default bound, on the adaptive and the non-adaptive path.
   * Eight lists of 512 KB give 1 MB for a region scan of two lists.
   */
  @Test
  public void testPreadUnderBoundOnBothPaths() throws Exception {
    try (Connection conn = trainedIndex("T_PR", "I_PR", null, 4 * MB)) {
      for (int batches : new int[] { 1, 4 }) {
        assertEquals(Scan.ReadType.PREAD,
          firstBatchReadType(plan(conn, "T_PR", 2, batches, ""), 1));
      }
    }
  }

  /** The first batch keeps the default read type above the bound, on both paths. */
  @Test
  public void testDefaultReadTypeAboveBound() throws Exception {
    // Lists of 100 bytes and a bound of 200 bytes
    try (Connection conn = trainedIndex("T_PRA", "I_PRA", 200L, 800L)) {
      for (int batches : new int[] { 1, 4 }) {
        assertEquals(Scan.ReadType.DEFAULT,
          firstBatchReadType(plan(conn, "T_PRA", 6, batches, ""), 2));
        // More regions put fewer posting lists in each region scan
        assertEquals(Scan.ReadType.PREAD,
          firstBatchReadType(plan(conn, "T_PRA", 6, batches, ""), 3));
      }
    }
  }

  /**
   * A posting list above the bound gives the default read type without a region lookup. A probe of
   * all lists gives the mean region size, which the bound compares as for any batch.
   */
  @Test
  public void testLargePostingListsKeepDefaultReadType() throws Exception {
    try (Connection conn = trainedIndex("T_PRL", "I_PRL", null, 8 * 30 * MB)) {
      for (int batches : new int[] { 1, 4 }) {
        VectorIndexScanPlan plan = plan(conn, "T_PRL", 1, batches, "");
        assertEquals(1,
          plan.getBoundedReadTypeRegions(noRegionLookup(), plan.getContext().getScan()));
        assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, 12));
      }
    }
    try (Connection conn = trainedIndex("T_PRP", "I_PRP", null, 8 * 4 * MB)) {
      assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan(conn, "T_PRP", 8, 1, ""), 1));
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan(conn, "T_PRP", 8, 1, ""), 2));
    }
  }

  /**
   * Without an estimate, all batches keep the default read type and the plan does not look up the
   * regions. An empty guidepost is not an estimate, because it holds the guidepost width.
   */
  @Test
  public void testNoEstimateKeepsDefaultReadType() throws Exception {
    try (Connection conn = trainedIndex("T_PRX", "I_PRX", null, null)) {
      for (GuidePostsInfo stats : new GuidePostsInfo[] { null,
        GuidePostsInfo.createEmptyGuidePost(300 * MB, 0) }) {
        if (stats != null) {
          putIndexStats(conn, "I_PRX", stats);
        }
        for (int batches : new int[] { 1, 4 }) {
          VectorIndexScanPlan plan = plan(conn, "T_PRX", 1, batches, "");
          Scan scan = plan.getContext().getScan();
          assertEquals(1, plan.getBoundedReadTypeRegions(noRegionLookup(), scan));
          assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, 8));
          ScanRanges ranges = batchRanges(conn, plan, new int[] { 3 });
          assertEquals(Scan.ReadType.DEFAULT,
            plan.newBatchScan(scan, ranges, 1, 8, null).getReadType());
        }
      }
    }
  }

  /** A repeat iteration replaces the PREAD of an earlier iteration if the estimate goes away. */
  @Test
  public void testOwnPreadReplacedWhenEstimateGoesAway() throws Exception {
    try (Connection conn = trainedIndex("T_PRV", "I_PRV", null, 4 * MB)) {
      VectorIndexScanPlan plan = plan(conn, "T_PRV", 2, 4, "");
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan, 1));
      putIndexStats(conn, "I_PRV", GuidePostsInfo.NO_GUIDEPOST);
      assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, 1));
    }
  }

  /**
   * Each later batch gets the read type for its own width. The copy of a scan with PREAD from the
   * first batch must change to the default read type above the bound.
   */
  @Test
  public void testPreadBoundAppliesToEachLaterBatch() throws Exception {
    // Lists of 100 bytes and a bound of 200 bytes
    try (Connection conn = trainedIndex("T_PRB", "I_PRB", 200L, 800L)) {
      VectorIndexScanPlan plan = plan(conn, "T_PRB", 2, 4, "");
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan, 1));
      Scan scan = plan.getContext().getScan();
      ScanRanges ranges = batchRanges(conn, plan, new int[] { 3, 4, 5, 6 });
      assertEquals(Scan.ReadType.DEFAULT,
        plan.newBatchScan(scan, ranges, 4, 1, 100L).getReadType());
      assertEquals(Scan.ReadType.PREAD, plan.newBatchScan(scan, ranges, 4, 2, 100L).getReadType());
      // The rule does not round down the posting lists per region scan
      assertEquals(Scan.ReadType.DEFAULT,
        plan.newBatchScan(scan, ranges, 5, 2, 100L).getReadType());
      assertEquals(Scan.ReadType.PREAD, scan.getReadType());
      // A later batch scan of a scan with the default read type can get PREAD
      scan.setReadType(Scan.ReadType.DEFAULT);
      assertEquals(Scan.ReadType.PREAD, plan.newBatchScan(scan, ranges, 2, 1, 100L).getReadType());
      // Without an estimate, a later batch keeps the default read type
      assertEquals(Scan.ReadType.DEFAULT,
        plan.newBatchScan(scan, ranges, 1, 1, null).getReadType());
    }
  }

  /** A bound of 0 turns off PREAD for all batches, also with an estimate. */
  @Test
  public void testPreadBoundDisabledByConfig() throws Exception {
    try (Connection conn = trainedIndex("T_PRD", "I_PRD", 0L, 800L)) {
      for (int batches : new int[] { 1, 4 }) {
        assertEquals(Scan.ReadType.DEFAULT,
          firstBatchReadType(plan(conn, "T_PRD", 1, batches, ""), 8));
      }
      VectorIndexScanPlan plan = plan(conn, "T_PRD", 1, 4, "");
      assertEquals(0,
        plan.getBoundedReadTypeRegions(noRegionLookup(), plan.getContext().getScan()));
      ScanRanges ranges = batchRanges(conn, plan, new int[] { 3 });
      assertEquals(Scan.ReadType.DEFAULT,
        plan.newBatchScan(plan.getContext().getScan(), ranges, 1, 8, 100L).getReadType());
    }
  }

  /** The bound does not change a read type that the caller set. */
  @Test
  public void testExplicitReadTypeIsKept() throws Exception {
    // Lists of 100 bytes and a bound of 100 bytes
    try (Connection conn = trainedIndex("T_PRE", "I_PRE", 100L, 800L)) {
      for (int batches : new int[] { 1, 4 }) {
        // The SMALL hint asks for PREAD, also above the bound
        assertEquals(Scan.ReadType.PREAD,
          firstBatchReadType(plan(conn, "T_PRE", 6, batches, "SMALL"), 1));
        // A stream read stays a stream read, also under the bound
        VectorIndexScanPlan plan = plan(conn, "T_PRE", 1, batches, "");
        Scan scan = plan.getContext().getScan();
        scan.setReadType(Scan.ReadType.STREAM);
        assertEquals(Scan.ReadType.STREAM, firstBatchReadType(plan, 8));
        ScanRanges ranges = batchRanges(conn, plan, new int[] { 3 });
        assertEquals(Scan.ReadType.STREAM,
          plan.newBatchScan(scan, ranges, 1, 0, 100L).getReadType());
      }
    }
  }

  /**
   * A repeat iteration of a plan, as for an EXPLAIN before the query, does not take the PREAD of
   * the earlier iteration as a caller choice.
   */
  @Test
  public void testRepeatIterationChecksBoundAgain() throws Exception {
    try (Connection conn = trainedIndex("T_PRR", "I_PRR", 200L, 800L)) {
      VectorIndexScanPlan plan = plan(conn, "T_PRR", 6, 4, "");
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan, 3));
      assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, 2));
      assertTrue(plan.isAdaptive());
    }
  }

  /**
   * The bound applies only to the statement scan with the default read type or with the read type
   * that the plan set. Otherwise the plan keeps the read type of the scan for all batches.
   */
  @Test
  public void testBoundedReadTypeRegionsKeepsOtherReadTypes() throws Exception {
    try (Connection conn = trainedIndex("T_PRK", "I_PRK", 200L, 800L)) {
      VectorIndexScanPlan plan = plan(conn, "T_PRK", 6, 4, "");
      Scan scan = plan.getContext().getScan();
      assertEquals(3, plan.getBoundedReadTypeRegions(regionGrouper(3), scan));
      // A region scan copy, as for a retry, keeps its read type
      assertEquals(0, plan.getBoundedReadTypeRegions(regionGrouper(3), ScanUtil.newScan(scan)));
      for (Scan.ReadType readType : new Scan.ReadType[] { Scan.ReadType.PREAD,
        Scan.ReadType.STREAM }) {
        scan.setReadType(readType);
        assertEquals(0, plan.getBoundedReadTypeRegions(regionGrouper(3), scan));
      }
      // A caller read type on the scan of an earlier iteration is still a caller choice
      scan.setReadType(Scan.ReadType.DEFAULT);
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan, 3));
      scan.setReadType(Scan.ReadType.STREAM);
      assertEquals(0, plan.getBoundedReadTypeRegions(regionGrouper(3), scan));
      assertEquals(Scan.ReadType.STREAM, firstBatchReadType(plan, 2));
    }
    try (Connection conn = trainedIndex("T_PRZ", "I_PRZ", 0L, 800L)) {
      VectorIndexScanPlan plan = plan(conn, "T_PRZ", 6, 4, "");
      assertEquals(0,
        plan.getBoundedReadTypeRegions(regionGrouper(3), plan.getContext().getScan()));
    }
  }

  /** If the widest batch fits in the bound, the plan uses PREAD without a region lookup. */
  @Test
  public void testNarrowBatchesSkipRegionLookup() throws Exception {
    // Lists of 100 bytes and a bound of 400 bytes
    try (Connection conn = trainedIndex("T_PRN", "I_PRN", 400L, 800L)) {
      // One batch of two lists, and an adaptive budget of 2 x 2 = 4 lists
      for (int batches : new int[] { 1, 2 }) {
        VectorIndexScanPlan plan = plan(conn, "T_PRN", 2, batches, "");
        assertEquals(1,
          plan.getBoundedReadTypeRegions(noRegionLookup(), plan.getContext().getScan()));
      }
      // An adaptive budget of 8 lists, which is all the centroids, is wider than the bound
      VectorIndexScanPlan plan = plan(conn, "T_PRN", 3, 4, "");
      assertEquals(5,
        plan.getBoundedReadTypeRegions(regionGrouper(5), plan.getContext().getScan()));
    }
  }

  /** A plan that probes no posting lists keeps the default read type. */
  @Test
  public void testUntrainedIndexKeepsDefaultReadType() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_PRU (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 2))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_PRU ON T_PRU" + INDEX_OPTIONS);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = index(conn, "I_PRU");
      pconn.addTable(new DelegateTable(indexTable) {
        @Override
        public PIndexState getIndexState() {
          return PIndexState.ACTIVE;
        }
      }, indexTable.getTimeStamp());
      putIndexStats(conn, "I_PRU", guideposts(800));
      PreparedStatement ps =
        conn.prepareStatement("SELECT ID FROM T_PRU ORDER BY L2_DISTANCE(V, ?) LIMIT 3");
      ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.5f, 0.5f }));
      VectorIndexScanPlan plan =
        (VectorIndexScanPlan) ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
      assertEquals(0, plan.getProbeCount());
      assertEquals(0,
        plan.getBoundedReadTypeRegions(regionGrouper(1), plan.getContext().getScan()));
      assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, 1));
    }
  }

  /** The estimate of one posting list rounds up, so that it is not low. */
  @Test
  public void testListEstimateRoundsUp() throws Exception {
    // 801 bytes over 8 lists give lists of 101 bytes, which is above a bound of 100 bytes
    for (long indexBytes : new long[] { 800, 801 }) {
      try (Connection conn =
        trainedIndex("T_PRC" + indexBytes, "I_PRC" + indexBytes, 100L, indexBytes)) {
        VectorIndexScanPlan plan = plan(conn, "T_PRC" + indexBytes, 1, 1, "");
        assertEquals(1,
          plan.getBoundedReadTypeRegions(noRegionLookup(), plan.getContext().getScan()));
        assertEquals(indexBytes == 800 ? Scan.ReadType.PREAD : Scan.ReadType.DEFAULT,
          firstBatchReadType(plan, 1));
      }
    }
  }

  /**
   * During a migration, the estimate divides the index size over the centroids of both generations.
   * The region count includes the regions of the building generation.
   */
  @Test
  public void testListEstimateCountsBothGenerations() throws Exception {
    // 1600 bytes over 16 lists give lists of 100 bytes. The bound is 100 bytes
    try (Connection conn = trainedIndex("T_PRG", "I_PRG", 100L, 1600L)) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      List<float[]> vectors = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        vectors.add(new float[] { i, i });
      }
      VectorCentroidCache.getInstance(pconn.getQueryServices().getConfiguration()).put("I_PRG", 2L,
        new CachedCentroids(vectors, DistanceMetric.L2, 8));
      PTable index = index(conn, "I_PRG");
      pconn.addTable(new DelegateTable(index) {
        @Override
        public Long getVectorBuildingGeneration() {
          return 2L;
        }
      }, index.getTimeStamp());
      PreparedStatement ps = conn.prepareStatement("SELECT /*+ VECTOR_PROBE_COUNT(1) "
        + "MAX_PROBE_LIMIT(1) */ ID FROM T_PRG ORDER BY L2_DISTANCE(V, ?) LIMIT 3");
      ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.5f, 0.5f }));
      VectorIndexScanPlan plan =
        (VectorIndexScanPlan) ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
      // One list of each generation
      assertEquals(2, plan.getProbeCount());
      assertEquals(16, plan.getCentroidCount());
      assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, 1));
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan, 2));
      // Regions of the building generation count, and a region after it does not
      assertEquals(2, plan.getBoundedReadTypeRegions(
        splitGrouper(centroid(0), centroid(8), centroid(16)), plan.getContext().getScan()));
    }
  }

  /** The region count does not include regions that hold only rows of an older generation. */
  @Test
  public void testRegionCountSkipsOlderGenerations() throws Exception {
    // Lists of 100 bytes and a bound of 200 bytes. The active centroid IDs are 8 to 15
    try (Connection conn = trainedIndex("T_PRO", "I_PRO", "200", 800L, "", 8)) {
      VectorIndexScanPlan plan = plan(conn, "T_PRO", 6, 4, "");
      ParallelScanGrouper grouper =
        splitGrouper(centroid(0), centroid(4), centroid(8), centroid(12), centroid(16));
      assertEquals(2, plan.getBoundedReadTypeRegions(grouper, plan.getContext().getScan()));
      // Six lists over two regions give 300 bytes for a region scan
      assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, grouper));
      // Six lists over three regions give 200 bytes for a region scan
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan,
        splitGrouper(centroid(4), centroid(8), centroid(10), centroid(12))));
    }
  }

  /**
   * A salted index has a part of each posting list in each salt bucket. Thus a region scan can read
   * less than one full list.
   */
  @Test
  public void testSaltedIndexSplitsListsOverBuckets() throws Exception {
    // Lists of 400 bytes, parts of 100 bytes in each of four buckets, and a bound of 200 bytes
    try (Connection conn = trainedIndex("T_PRT", "I_PRT", "200", 3200L, " SALT_BUCKETS=4", 0)) {
      assertEquals(Integer.valueOf(4), index(conn, "I_PRT").getBucketNum());
      VectorIndexScanPlan plan = plan(conn, "T_PRT", 1, 1, "");
      // Four parts over four regions give 100 bytes for a region scan
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan, 4));
      assertEquals(Scan.ReadType.PREAD, firstBatchReadType(plan, 2));
      // Four parts in one region give 400 bytes
      assertEquals(Scan.ReadType.DEFAULT, firstBatchReadType(plan, 1));
    }
  }

  /**
   * The empty column family must have guideposts. Another family adds the bytes of its guideposts,
   * and without guideposts it adds nothing.
   */
  @Test
  public void testIndexBytesOverColumnFamilies() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE T_PRF (ID VARCHAR NOT NULL PRIMARY KEY, "
        + "A.V VECTOR(FLOAT, 2), B.TAG VARCHAR)");
      conn.createStatement().execute("CREATE VECTOR INDEX I_PRF ON T_PRF (A.V) INCLUDE (B.TAG) "
        + "WITH (algorithm = 'IVF', metric = 'L2', lists = 32, sample_size = 100) ASYNC");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable index = index(conn, "I_PRF");
      byte[] empty = SchemaUtil.getEmptyColumnFamily(index);
      byte[] other = null;
      for (PColumnFamily family : index.getColumnFamilies()) {
        if (!Bytes.equals(family.getName().getBytes(), empty)) {
          other = family.getName().getBytes();
        }
      }
      assertNotNull(other);
      // The other family alone gives no estimate
      putFamilyStats(conn, index, other, guideposts(50));
      assertNull(VectorIndexScanPlan.getIndexBytes(pconn, index));
      putFamilyStats(conn, index, empty, guideposts(800));
      assertEquals(Long.valueOf(850), VectorIndexScanPlan.getIndexBytes(pconn, index));
      for (GuidePostsInfo stats : new GuidePostsInfo[] { GuidePostsInfo.NO_GUIDEPOST,
        GuidePostsInfo.createEmptyGuidePost(300 * MB, 0) }) {
        putFamilyStats(conn, index, other, stats);
        assertEquals(Long.valueOf(800), VectorIndexScanPlan.getIndexBytes(pconn, index));
      }
      putFamilyStats(conn, index, empty, GuidePostsInfo.createEmptyGuidePost(300 * MB, 0));
      assertNull(VectorIndexScanPlan.getIndexBytes(pconn, index));
    }
  }

  /** The bound accepts a byte size with a unit suffix. */
  @Test
  public void testPreadBoundAcceptsUnitSuffix() throws Exception {
    // Lists of 2000 bytes are over a bound of 1 KB and under a bound of 4 KB
    for (String bound : new String[] { "1k", "4k" }) {
      String table = "T_PRQ" + bound.charAt(0);
      try (Connection conn = trainedIndex(table, "I_PRQ" + bound.charAt(0), bound, 16000L, "", 0)) {
        assertEquals(bound.equals("1k") ? Scan.ReadType.DEFAULT : Scan.ReadType.PREAD,
          firstBatchReadType(plan(conn, table, 1, 1, ""), 1));
      }
    }
  }
}
