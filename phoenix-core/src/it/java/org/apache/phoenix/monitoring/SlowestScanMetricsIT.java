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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

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
    String tableName = generateUniqueName();
    String sql = "SELECT * FROM " + tableName + " WHERE k1 = 1 AND k2 = 'a'";
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, "1");
    props.setProperty(QueryServices.SCAN_METRICS_BY_REGION_ENABLED, "true");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      createTableAndUpsertData(conn, tableName, "");
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {

      }
      List<List<ScanMetricsGroup>> slowestScanReadMetrics =
        PhoenixRuntime.getTopNSlowestScanReadMetrics(rs);
      System.out.println("Slowest scan read metrics: " + slowestScanReadMetrics);
    }
  }

  @Test
  public void testMultiplePointsLookupQuery() throws Exception {
    String tableName = generateUniqueName();
    String sql = "SELECT * FROM " + tableName + " WHERE (k1, k2) IN ((1, 'a'), (2, 'b'), (3, 'c'))";
    Properties props = new Properties();
    props.setProperty(QueryServices.SLOWEST_SCAN_METRICS_COUNT, "3");
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
      List<List<ScanMetricsGroup>> slowestScanReadMetrics =
        PhoenixRuntime.getTopNSlowestScanReadMetrics(rs);
      Map<String, Map<MetricType, Long>> readMetrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
      System.out.println("Slowest scan read metrics: " + slowestScanReadMetrics);
      System.out.println("Read metrics: " + readMetrics);
    }
  }

  private void createTableAndUpsertData(Connection conn, String tableName, String ddlOptions)
    throws SQLException {
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
  }
}
