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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

/**
 * Unit tests for {@link VectorIndexScanPlan} posting list key range generation, salt bucket
 * partitioning, tenant prefix scoping, fallback full scan conditions, and probe hint parsing.
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
      // Forward seeking filter evaluation across ascending keys
      SkipScanFilter skip = ranges.getSkipScanFilter();
      assertEquals(ReturnCode.SEEK_NEXT_USING_HINT,
        filter(skip, new byte[] { 0 }, centroid(6), row));
      for (int bucket = 0; bucket < 4; bucket++) {
        assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL,
          filter(skip, new byte[] { (byte) bucket }, centroid(7), row));
      }
    }
  }

  /** Posting list key ranges under padded fixed width CHAR tenant identifiers. */
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

  /** Full index scan fallback when querying multitenant indexes without a tenant context. */
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

  /** Degenerate WHERE clause scan range preservation. */
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
  }

  @Test
  public void testHnswEfSearchHint() {
    HintNode hint = new HintNode("/*+ HNSW_EF_SEARCH(150) VECTOR_PROBE_COUNT(5) */");
    assertEquals(150, VectorIndexScanPlan.getHintInt(hint, Hint.HNSW_EF_SEARCH, 64));
    assertEquals(5, VectorIndexScanPlan.getHintInt(hint, Hint.VECTOR_PROBE_COUNT, 0));
    assertEquals(64,
      VectorIndexScanPlan.getHintInt(new HintNode("/*+ INDEX(T I) */"), Hint.HNSW_EF_SEARCH, 64));
  }

  @Test
  public void testInterleaveAlternatesGenerationRankings() {
    // Any prefix of 2n holds the n nearest of each generation, and a longer ranking keeps its tail
    assertArrayEquals(new int[] { 0, 4, 1, 5, 2, 6 },
      VectorIndexScanPlan.interleave(new int[] { 0, 1, 2 }, new int[] { 4, 5, 6 }));
    assertArrayEquals(new int[] { 3, 9, 1, 8, 7 },
      VectorIndexScanPlan.interleave(new int[] { 3, 1 }, new int[] { 9, 8, 7 }));
    assertArrayEquals(new int[] { 2, 0 },
      VectorIndexScanPlan.interleave(new int[] { 2, 0 }, new int[0]));
  }
}
