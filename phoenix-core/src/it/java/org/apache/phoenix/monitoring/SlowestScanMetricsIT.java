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
import static org.junit.Assert.assertNotNull;
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
   * @throws Exception
   */
  private JsonArray getSlowestScanMetricsJsonArray(ResultSet rs) throws Exception {
    List<List<ScanMetricsGroup>> slowestScanMetrics =
      PhoenixRuntime.getTopNSlowestScanMetrics(rs);
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
