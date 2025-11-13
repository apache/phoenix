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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

@Category(NeedsOwnMiniClusterTest.class)
public class SlowestScanMetricsIT extends BaseTest {
  @BeforeClass
  public static void setup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
    props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testSinglePointLookupQuery() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    String sql = "SELECT * FROM " + tableName + " WHERE k1 = 1 AND k2 = 'a'";
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      createTableAndUpsertData(conn, tableName, "");
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 1 as its single point lookup though topN is 2.
      assertEquals(1, slowestScanMetricsJsonArray.size());
      JsonArray groupArray = slowestScanMetricsJsonArray.get(0).getAsJsonArray();
      // Inner array has size 1 as it's a simple single point lookup and a query with subquery or
      // JOIN operation.
      assertEquals(1, groupArray.size());
      JsonObject groupJson = groupArray.get(0).getAsJsonObject();
      assertEquals(2, groupJson.size());
      assertEquals(tableName, groupJson.get("table").getAsString());
      JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
      assertEquals(1, regionsArray.size());
      JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
      assertNotNull(regionJson.get("region"));
      assertNotNull(regionJson.get("server"));
      // BROC is 2 as it's a single point lookup so, bloom filter block will also be read along with
      // data block.
      assertEquals(2, regionJson.get("broc").getAsLong());
    }
  }

  @Test
  public void testMultiplePointsLookupQuery() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    String sql = "SELECT * FROM " + tableName + " WHERE (k1, k2) IN ((1, 'a'), (2, 'b'), (3, 'c'))";
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      createTableAndUpsertData(conn, tableName, "SALT_BUCKETS=3");
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(3, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 2 as its a multi-point lookup query and topN is 2.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a simple single point lookup and a query with subquery or
        // JOIN operation.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        assertEquals(2, groupJson.size());
        assertEquals(tableName, groupJson.get("table").getAsString());
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 1 as it's a multi-point lookup query so, only data block will be read.
        assertEquals(1, regionJson.get("broc").getAsLong());
      }
    }
  }

  @Test
  public void testUnionAllQuery() throws Exception {
    int topN = 2;
    String tableName1 = generateUniqueName();
    String tableName2 = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName1, "");
      createTableAndUpsertData(conn, tableName2, "");
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT * FROM " + tableName1 + " WHERE k1 = 1 AND k2 = 'a'"
        + " UNION ALL SELECT * FROM " + tableName2 + " WHERE k1 = 1 AND k2 = 'a'";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(2, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 2 as its UNION of 2 single point lookup queries and topN is 2.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a union of 2 single point lookup queries and not a query
        // with subquery or JOIN operation. Two HBase tables are scanned and there is one inner
        // array for each table.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        assertEquals(2, groupJson.size());
        String tableName = groupJson.get("table").getAsString();
        assertTrue(tableName1.equals(tableName) || tableName2.equals(tableName));
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 2 as each query in UNION is a single point lookup so, bloom filter block will
        // also be read along with data block.
        assertEquals(2, regionJson.get("broc").getAsLong());
      }

      StatementContext stmtCtx = rs.unwrap(PhoenixResultSet.class).getContext();

      // Verify that existing read metrics are combined correctly.
      ReadMetricQueue readMetricsQueue = stmtCtx.getReadMetricsQueue();
      Map<String, Map<MetricType, Long>> expectedMetrics = readMetricsQueue.aggregate();
      Map<String, Map<MetricType, Long>> actualMetrics =
        PhoenixRuntime.getRequestReadMetricInfo(rs);
      assertEquals(expectedMetrics, actualMetrics);

      // Verify that overall query metrics are published correctly.
      Map<MetricType, Long> expectedOverallQueryMetrics =
        stmtCtx.getOverallQueryMetrics().publish();
      Map<MetricType, Long> actualOverallQueryMetrics =
        PhoenixRuntime.getOverAllReadRequestMetricInfo(rs);
      assertEquals(expectedOverallQueryMetrics, actualOverallQueryMetrics);
    }
  }

  @Test
  public void testQueryContainingSubquery() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    String subqueryTableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "");
      createTableAndUpsertData(conn, subqueryTableName, "");
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT * FROM " + tableName + " WHERE k1 IN (SELECT k1 FROM "
        + subqueryTableName + " WHERE k2 IN ('a', 'b'))";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(2, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 1 as it's a query containing subquery and both outer query and
      // subquery are scanning single regions.
      assertEquals(1, slowestScanMetricsJsonArray.size());
      JsonArray groupArray = slowestScanMetricsJsonArray.get(0).getAsJsonArray();
      // Inner array has size 2 as there is one region scan metrics for outer query and one for
      // subquery.
      assertEquals(2, groupArray.size());
      for (int i = 0; i < groupArray.size(); i++) {
        JsonObject groupJson = groupArray.get(i).getAsJsonObject();
        assertEquals(2, groupJson.size());
        String hbaseTableName = groupJson.get("table").getAsString();
        assertTrue(hbaseTableName.equals(tableName) || hbaseTableName.equals(subqueryTableName));
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 1 as both outer query and subquery are scanning multiple rows so, only data block
        // will be read.
        assertEquals(1, regionJson.get("broc").getAsLong());
      }
    }
  }

  @Test
  public void testQueryWithSelfJoin() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + tableName
        + " (k1 INTEGER NOT NULL, k2 varchar NOT NULL, v1 VARCHAR, v2 VARCHAR, CONSTRAINT PK PRIMARY KEY (k1, k2))");
      conn.commit();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'c', 'c1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'd', 'd1', 'd2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'e', 'e1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'f', 'f1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (5, 'g', 'g1', 'v2')");
      conn.commit();
      TestUtil.flush(getUtility(), TableName.valueOf(tableName));
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql =
        "SELECT a.k1 as k1, b.k2 as k2, b.v1 as v1, a.total_count as total_count FROM (SELECT k1, COUNT(*) as total_count FROM "
          + tableName + " WHERE k1 IN (1, 3) GROUP BY k1) a JOIN (SELECT k1, k2, v1 FROM "
          + tableName + " WHERE k1 IN (1, 3) AND k2 = 'a') b ON a.k1 = b.k1";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Same sized as inner array.
      long[] brocValues = new long[2];
      long[] brfcValues = new long[2];
      // Outer array has size 1 as it's a JOIN query and both left and right tables are scanning
      // single regions.
      assertEquals(1, slowestScanMetricsJsonArray.size());
      JsonArray groupArray = slowestScanMetricsJsonArray.get(0).getAsJsonArray();
      // Inner array has size 2 as there is one region scan metrics for left table and one for
      // right table.
      assertEquals(2, groupArray.size());
      for (int i = 0; i < groupArray.size(); i++) {
        JsonObject groupJson = groupArray.get(i).getAsJsonObject();
        assertEquals(2, groupJson.size());
        String hbaseTableName = groupJson.get("table").getAsString();
        assertEquals(tableName, hbaseTableName);
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        brocValues[i] = regionJson.get("broc").getAsLong();
        brfcValues[i] = regionJson.get("brfc").getAsLong();
      }

      // On scanning the left table, the data blocks get loaded into block cache. So, left table
      // reads data from file system and right table reads data from block cache. As both left and
      // right tables are scanning single regions, only data block will be read.
      assertEquals(0, brocValues[0]);
      assertEquals(1, brocValues[1]);
      assertTrue(brfcValues[0] > 0);
      assertEquals(0, brfcValues[1]);
    }
  }

  @Test
  public void testUnionAllAggregateQuery() throws Exception {
    int topN = 2;
    String tableName1 = generateUniqueName();
    String tableName2 = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName1, "");
      createTableAndUpsertData(conn, tableName2, "");
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT MAX(v1) FROM (SELECT k1, v1 FROM " + tableName1
        + " UNION ALL SELECT k1, v1 FROM " + tableName2 + ") GROUP BY k1 HAVING MAX(v1) > 'c1'";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 2 as its UNION of it's union of 2 queries each scanning single region
      // and topN is 2.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a union of 2 queries each scanning single region and not a
        // query with subquery or JOIN operation. Two HBase tables are scanned and there is one
        // inner array for each table.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        assertEquals(2, groupJson.size());
        String tableName = groupJson.get("table").getAsString();
        assertTrue(tableName1.equals(tableName) || tableName2.equals(tableName));
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 1 as each query in UNIONed query is doing full table scan so, only data block is
        // read.
        assertEquals(1, regionJson.get("broc").getAsLong());
      }

      StatementContext stmtCtx = rs.unwrap(PhoenixResultSet.class).getContext();
      StatementContext subStmtCtx = stmtCtx.getSubStatementContexts().iterator().next();

      // Verify that existing read metrics are combined correctly.
      ReadMetricQueue readMetricsQueue = subStmtCtx.getReadMetricsQueue();
      Map<String, Map<MetricType, Long>> expectedMetrics = readMetricsQueue.aggregate();
      Map<String, Map<MetricType, Long>> actualMetrics =
        PhoenixRuntime.getRequestReadMetricInfo(rs);
      assertEquals(expectedMetrics, actualMetrics);

      // Verify that overall query metrics are published correctly.
      Map<MetricType, Long> expectedOverallQueryMetrics =
        stmtCtx.getOverallQueryMetrics().publish();
      Map<MetricType, Long> actualOverallQueryMetrics =
        PhoenixRuntime.getOverAllReadRequestMetricInfo(rs);
      assertEquals(expectedOverallQueryMetrics, actualOverallQueryMetrics);
    }
  }

  @Test
  public void testConstantQuery() throws Exception {
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(2));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT 'This is a constant query'";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array contains empty inner array as its a constant query not needing to scan any
      // HBase table.
      assertEquals(1, slowestScanMetricsJsonArray.size());
      JsonArray groupArray = slowestScanMetricsJsonArray.get(0).getAsJsonArray();
      assertEquals(0, groupArray.size());
    }
  }

  @Test
  public void testSlowestScanMetricsDisabled() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "");
      Statement stmt = conn.createStatement();
      String sql = "SELECT * FROM " + tableName + " WHERE k1 = 1 AND k2 = 'a'";
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);
      // No slowest scan metrics are returned as slowest scan metrics are disabled.
      assertEquals(0, slowestScanMetricsJsonArray.size());
    }
  }

  @Test
  public void testHBaseScanMetricsByRegionDisabled() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "SALT_BUCKETS=3");
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql =
        "SELECT * FROM " + tableName + " WHERE (k1, k2) IN ((1, 'a'), (2, 'b'), (3, 'c'))";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(3, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 2 as its a multi-point lookup query and topN is 2 even though 3
      // parallel scans are generated.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a simple query and not a query with subquery or
        // JOIN operation.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        String hbaseTableName = groupJson.get("table").getAsString();
        assertEquals(tableName, hbaseTableName);
        // No region name and server are returned as HBase scan metrics by region are disabled.
        assertNull(groupJson.get("regions"));
        assertNull(groupJson.get("server"));
        assertNull(groupJson.get("region"));
        // BROC is 1 as it's a multi-point lookup query so, only data block is read.
        assertEquals(1, groupJson.get("broc").getAsLong());
      }
    }
  }

  @Test
  public void testAggregateQueryWithoutGroupBy() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "SALT_BUCKETS=3");
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT MAX(v1) FROM " + tableName;
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 2 as aggregate query did a full table scan and topN is 2.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a simple aggregate query and not a query with subquery or
        // JOIN operation.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        assertEquals(tableName, groupJson.get("table").getAsString());
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 1 as it's a full table scan so, only data block is read.
        assertEquals(1, regionJson.get("broc").getAsLong());
      }
    }
  }

  @Test
  public void testAggregateQueryWithGroupBy() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "BLOOMFILTER='NONE'");
      Statement stmt = conn.createStatement();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'c', 'c1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'd', 'd1', 'd2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'e', 'e1', 'v2')");
      conn.commit();
      TestUtil.flush(getUtility(), TableName.valueOf(tableName));
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT MAX(v1) FROM " + tableName + " WHERE v2 = 'v2' GROUP BY k1";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(2, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 1 as its scan of single region only.
      assertEquals(1, slowestScanMetricsJsonArray.size());
      JsonArray groupArray = slowestScanMetricsJsonArray.get(0).getAsJsonArray();
      // Inner array has size 1 as it's a simple aggregate query with group by and not a query with
      // subquery or JOIN operation.
      assertEquals(1, groupArray.size());
      for (int i = 0; i < groupArray.size(); i++) {
        JsonObject groupJson = groupArray.get(i).getAsJsonObject();
        assertEquals(tableName, groupJson.get("table").getAsString());
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // Though it's a full table scan but still broc is 2 as there are 2 HFiles per commit call
        // and one data block is read from each HFile.
        assertEquals(2, regionJson.get("broc").getAsLong());
      }
    }
  }

  @Test
  public void testAggregateQueryWithGroupByAndOrderBy() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "BLOOMFILTER='NONE'");
      Statement stmt = conn.createStatement();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'c', 'c1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'd', 'd1', 'd2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'e', 'e1', 'v2')");
      conn.commit();
      TestUtil.flush(getUtility(), TableName.valueOf(tableName));
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT v2, MAX(v1) FROM " + tableName + " GROUP BY v2 ORDER BY v2";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      // Read total 5 rows from 2 HFiles.
      assertEquals(5, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 1 as its scan of single region only.
      assertEquals(1, slowestScanMetricsJsonArray.size());
      JsonArray groupArray = slowestScanMetricsJsonArray.get(0).getAsJsonArray();
      // Inner array has size 1 as it's a simple aggregate query with group by and not a query with
      // subquery or JOIN operation.
      assertEquals(1, groupArray.size());
      for (int i = 0; i < groupArray.size(); i++) {
        JsonObject groupJson = groupArray.get(i).getAsJsonObject();
        assertEquals(tableName, groupJson.get("table").getAsString());
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // Though it's a full table scan but still broc is 2 as there are 2 HFiles per commit call
        // and one data block is read from each HFile.
        assertEquals(2, regionJson.get("broc").getAsLong());
      }
    }
  }

  @Test
  public void testQueryOnCoveredIndex() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "SALT_BUCKETS=3");
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + " (v1) INCLUDE (v2)");
      conn.commit();
      TestUtil.flush(getUtility(), TableName.valueOf(indexName));
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT v1, v2 FROM " + tableName + " WHERE v1 = 'a1'";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      int totalBroc = 0;
      // Outer array has size 2 as the base table is salted so, salt bucket no. of parallel scans
      // are generated and topN is 2.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a simple query and not a query with subquery or
        // JOIN operation.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        assertEquals(indexName, groupJson.get("table").getAsString());
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 1 as it's a simple query so, only data block is read.
        totalBroc += regionJson.get("broc").getAsLong();
      }
      // Total BROC is 1 as data is read from only one region.
      assertEquals(1, totalBroc);
    }
  }

  @Test
  public void testQueryOnUncoveredIndex() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "SALT_BUCKETS=3");
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (v1)");
      conn.commit();
      TestUtil.flush(getUtility(), TableName.valueOf(indexName));
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT v1, v2 FROM " + tableName + " WHERE v1 = 'a1'";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      int totalBroc = 0;
      // Outer array has size 2 as the base table is salted so, salt bucket no. of parallel scans
      // are generated and topN is 2.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a simple query and not a query with subquery or
        // JOIN operation.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        assertEquals(indexName, groupJson.get("table").getAsString());
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 1 as it's a simple query so, only data block is read.
        totalBroc += regionJson.get("broc").getAsLong();
      }
      // Total BROC is 2 as data is read from one region of index table and one region of data table
      // as index is uncovered.
      assertEquals(2, totalBroc);
    }
  }

  @Test
  public void testQueryOnView() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    String viewName = generateUniqueName();
    assertNotEquals(viewName, tableName);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " WHERE k1 = 1");
      conn.commit();
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT * FROM " + viewName;
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      // Outer array has size 1 as its scan of single region only.
      assertEquals(1, slowestScanMetricsJsonArray.size());
      JsonArray groupArray = slowestScanMetricsJsonArray.get(0).getAsJsonArray();
      // Inner array has size 1 as it's a simple query and not a query with subquery or
      // JOIN operation.
      assertEquals(1, groupArray.size());
      JsonObject groupJson = groupArray.get(0).getAsJsonObject();
      assertEquals(tableName, groupJson.get("table").getAsString());
      JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
      assertEquals(1, regionsArray.size());
      JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
      assertNotNull(regionJson.get("region"));
      assertNotNull(regionJson.get("server"));
      // BROC is 1 as it's not a single point lookup so, only data block is read.
      assertEquals(1, regionJson.get("broc").getAsLong());
    }
  }

  @Test
  public void testQueryOnViewIndex() throws Exception {
    int topN = 2;
    String tableName = generateUniqueName();
    String viewName = generateUniqueName();
    String viewIndexName = generateUniqueName();
    String viewIndexPhysicalName = MetaDataUtil.getViewIndexPhysicalName(tableName);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTableAndUpsertData(conn, tableName, "SALT_BUCKETS=3");
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " WHERE k1 = 1");
      conn.commit();
      stmt.execute("CREATE INDEX " + viewIndexName + " ON " + viewName + " (v1) INCLUDE (v2)");
      conn.commit();
      TestUtil.flush(getUtility(), TableName.valueOf(viewIndexPhysicalName));
    }
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(topN));
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT v2 FROM " + viewName + " WHERE v1 = 'a1'";
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(1, rowCount);
      JsonArray slowestScanMetricsJsonArray = getSlowestScanMetricsJsonArray(rs);

      int totalBroc = 0;
      // Outer array has size 2 as the base table is salted so, salt bucket no. of parallel scans
      // are generated and topN is 2.
      assertEquals(topN, slowestScanMetricsJsonArray.size());
      for (int i = 0; i < topN; i++) {
        JsonArray groupArray = slowestScanMetricsJsonArray.get(i).getAsJsonArray();
        // Inner array has size 1 as it's a simple query and not a query with subquery or
        // JOIN operation.
        assertEquals(1, groupArray.size());
        JsonObject groupJson = groupArray.get(0).getAsJsonObject();
        assertEquals(viewIndexPhysicalName, groupJson.get("table").getAsString());
        JsonArray regionsArray = groupJson.get("regions").getAsJsonArray();
        assertEquals(1, regionsArray.size());
        JsonObject regionJson = regionsArray.get(0).getAsJsonObject();
        assertNotNull(regionJson.get("region"));
        assertNotNull(regionJson.get("server"));
        // BROC is 1 as it's a simple query so, only data block is read.
        totalBroc += regionJson.get("broc").getAsLong();
      }
      // Total BROC is 1 as data is read from single region of view index table.
      assertEquals(1, totalBroc);
    }
  }

  private void createTableAndUpsertData(Connection conn, String tableName, String ddlOptions)
    throws Exception {
    String createTableSql = "CREATE TABLE " + tableName
      + " (k1 INTEGER NOT NULL, k2 varchar NOT NULL, v1 VARCHAR, v2 VARCHAR, CONSTRAINT PK PRIMARY KEY (k1, k2)) "
      + ddlOptions;
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(createTableSql);
      conn.commit();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'a2')");
      stmt.execute(
        "UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (2, 'b', 'b1000', 'b2000')");
      stmt.execute(
        "UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'c', 'c1000000', 'c2000000')");
      conn.commit();
    }
    TestUtil.flush(getUtility(), TableName.valueOf(tableName));
  }

  /**
   * Returns a JSON array of arrays of JSON maps. Each JSON map contains name of HBase table scanned
   * and list of scan metrics for each region scanned along with region and server name. The list of
   * scan metrics in JSON map will have more than one entry if region moved b/w consecutive RPC
   * calls.
   * @param rs ResultSet from which to get the slowest scan metrics
   * @return JSON array of arrays of JSON maps
   */
  public static JsonArray getSlowestScanMetricsJsonArray(ResultSet rs) throws Exception {
    List<List<ScanMetricsGroup>> slowestScanMetrics = PhoenixRuntime.getTopNSlowestScanMetrics(rs);
    JsonArray jsonArray = new JsonArray();
    for (List<ScanMetricsGroup> group : slowestScanMetrics) {
      JsonArray groupArray = new JsonArray();
      for (ScanMetricsGroup scanMetricsGroup : group) {
        groupArray.add(scanMetricsGroup.toJson());
      }
      jsonArray.add(groupArray);
    }
    System.out.println("Slowest scan metrics JSON array: " + jsonArray);
    return jsonArray;
  }
}
