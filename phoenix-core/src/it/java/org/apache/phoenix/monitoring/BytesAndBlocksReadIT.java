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
package org.apache.phoenix.monitoring;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.CompactSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class BytesAndBlocksReadIT extends BaseTest {

  @BeforeClass
  public static void setup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(4);
    props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
    props.put(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, "0");
    props.put(CompactSplit.HBASE_REGION_SERVER_ENABLE_COMPACTION, "false");
    setUpTestDriver(new ReadOnlyProps(props));
  }

  @Test
  public void testSinglePointLookupQuery() throws Exception {
    String tableName = generateUniqueName();
    String sql = "SELECT * FROM " + tableName + " WHERE k1 = 1 AND k2 = 'a'";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "");
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
    }
  }

  @Test
  public void testMultiPointLookupQueryWithoutBloomFilter() throws Exception {
    String tableName = generateUniqueName();
    String sql = "SELECT * FROM " + tableName + " WHERE k1 IN (1, 2, 3) AND k2 IN ('a', 'b', 'c')";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "BLOOMFILTER='NONE'");
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
    }
  }

  @Test
  public void testMultiPointLookupQueryWithBloomFilter() throws Exception {
    String tableName = generateUniqueName();
    String sql = "SELECT * FROM " + tableName + " WHERE k1 IN (1, 2, 3) AND k2 IN ('a', 'b', 'c')";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName,
        "\"phoenix.bloomfilter.multikey.pointlookup\"=true");
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), true);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
    }
  }

  @Test
  public void testBytesReadInClientUpsertSelect() throws Exception {
    String sourceTableName = generateUniqueName();
    String targetTableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, sourceTableName, "");
      createTable(conn, targetTableName, "");
      assertOnReadsFromMemstore(sourceTableName,
        getMutationReadMetrics(conn, targetTableName, sourceTableName, 1));
      TestUtil.flush(utility, TableName.valueOf(sourceTableName));
      assertOnReadsFromFs(sourceTableName,
        getMutationReadMetrics(conn, targetTableName, sourceTableName, 2));
      assertOnReadsFromBlockcache(sourceTableName,
        getMutationReadMetrics(conn, targetTableName, sourceTableName, 3));
    }
  }

  @Test
  public void testAggregateQueryWithoutGroupBy() throws Exception {
    String tableName = generateUniqueName();
    String sql = "SELECT MAX(v1) FROM " + tableName + " WHERE k1 = 1";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'a2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'c', 'c1', 'c2')");
      conn.commit();
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
    }
  }

  @Test
  public void testAggregateQueryWithGroupBy() throws Exception {
    String tableName = generateUniqueName();
    String sql = "SELECT MAX(v1) FROM " + tableName + " WHERE v2 = 'v2' GROUP BY k1";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'c', 'c1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'd', 'd1', 'd2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'e', 'e1', 'v2')");
      conn.commit();
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
    }
  }

  @Test
  public void testAggregateQueryWithGroupByAndOrderBy() throws Exception {
    String tableName = generateUniqueName();
    String sql = "SELECT v2, MAX(v1) FROM " + tableName + " GROUP BY v2 ORDER BY v2";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'c', 'c1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'd', 'd1', 'd2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'e', 'e1', 'v2')");
      conn.commit();
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
    }
  }

  @Test
  public void testUnionAllQuery() throws Exception {
    String tableName1 = generateUniqueName();
    String tableName2 = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName1, "");
      Statement stmt = conn.createStatement();
      stmt.execute("UPSERT INTO " + tableName1 + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName1 + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName1 + " (k1, k2, v1, v2) VALUES (1, 'c', 'c1', 'v2')");
      conn.commit();
      createTable(conn, tableName2, "");
      stmt.execute("UPSERT INTO " + tableName2 + " (k1, k2, v1, v2) VALUES (3, 'd', 'd1', 'd2')");
      stmt.execute("UPSERT INTO " + tableName2 + " (k1, k2, v1, v2) VALUES (3, 'e', 'e1', 'v2')");
      conn.commit();
      String sql = "SELECT MAX(v1) FROM (SELECT k1, v1 FROM " + tableName1
        + " UNION ALL SELECT k1, v1 FROM " + tableName2 + ") GROUP BY k1 HAVING MAX(v1) > 'c1'";
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName1, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName1));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromFs(tableName1, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName1, getQueryReadMetrics(rs));
      rs = stmt.executeQuery(sql);
    }
  }

  private void createTable(Connection conn, String tableName, String ddlOptions) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName
        + " (k1 INTEGER NOT NULL, k2 varchar NOT NULL, v1 VARCHAR, v2 VARCHAR, CONSTRAINT PK PRIMARY KEY (k1, k2)) "
        + ddlOptions);
      conn.commit();
    }
  }

  private void createTableAndUpsertData(Connection conn, String tableName, String ddlOptions)
    throws Exception {
    createTable(conn, tableName, ddlOptions);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'v1', 'v2')");
      conn.commit();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (2, 'b', 'v1', 'v2')");
      conn.commit();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'c', 'v1', 'v2')");
      conn.commit();
    }
  }

  private void assertOnReadsFromMemstore(String tableName,
    Map<String, Map<MetricType, Long>> readMetrics) throws Exception {
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE) > 0);
    Assert.assertEquals(0, (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS));
    Assert.assertEquals(0,
      (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE));
  }

  private void assertOnReadsFromFs(String tableName, Map<String, Map<MetricType, Long>> readMetrics)
    throws Exception {
    assertOnReadsFromFs(tableName, readMetrics, false);
  }

  private void assertOnReadsFromFs(String tableName, Map<String, Map<MetricType, Long>> readMetrics,
    boolean isMultiPointBloomFilterEnabled) throws Exception {
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS) > 0);
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BLOCK_READ_OPS_COUNT) > 0);
    Assert.assertEquals(0,
      (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE));
    if (isMultiPointBloomFilterEnabled) {
      Assert.assertTrue(
        (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE) > 0);
    } else {
      Assert.assertEquals(0,
        (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE));
    }
  }

  private void assertOnReadsFromBlockcache(String tableName,
    Map<String, Map<MetricType, Long>> readMetrics) throws Exception {
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE) > 0);
    Assert.assertEquals(0, (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS));
    Assert.assertEquals(0,
      (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE));
    Assert.assertEquals(0, (long) readMetrics.get(tableName).get(MetricType.BLOCK_READ_OPS_COUNT));
  }

  private Map<String, Map<MetricType, Long>> getQueryReadMetrics(ResultSet rs) throws Exception {
    int rowCount = 0;
    while (rs.next()) {
      rowCount++;
    }
    Assert.assertTrue(rowCount > 0);
    Map<String, Map<MetricType, Long>> readMetrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
    System.out.println("Query readMetrics: " + readMetrics);
    return readMetrics;
  }

  private Map<String, Map<MetricType, Long>> getMutationReadMetrics(Connection conn,
    String targetTableName, String sourceTableName, int rowId) throws Exception {
    Statement stmt = conn.createStatement();
    stmt.execute("UPSERT INTO " + targetTableName + " (k1, k2, v1, v2) SELECT * FROM "
      + sourceTableName + " WHERE k1 = " + rowId);
    conn.commit();
    Map<String, Map<MetricType, Long>> readMetrics =
      PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(conn);
    System.out.println("Mutation readMetrics: " + readMetrics);
    PhoenixRuntime.resetMetrics(conn);
    return readMetrics;
  }
}
