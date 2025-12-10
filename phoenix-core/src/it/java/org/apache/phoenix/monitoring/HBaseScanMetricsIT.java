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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.CompactSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class HBaseScanMetricsIT extends BaseTest {

  @BeforeClass
  public static void setup() throws Exception {
    Assume.assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.6.3") > 0);
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
      // 1 Data Block + 1 Bloom Block
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), 2);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
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
      // 1 Data Block
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), 1);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
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
      // 1 Data Block + 1 Bloom Block
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), 2, true);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
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
      // 1 Data Block from source table
      assertOnReadsFromFs(sourceTableName,
        getMutationReadMetrics(conn, targetTableName, sourceTableName, 2), 1);
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
      // 1 Data Block from table
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), 1);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
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
      // 1 Data Block from table
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), 1);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
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
      // 1 Data Block from table
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), 1);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
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
      Map<String, Map<MetricType, Long>> readMetrics = getQueryReadMetrics(rs);
      assertOnReadsFromMemstore(tableName1, readMetrics);
      assertOnReadsFromMemstore(tableName2, readMetrics);
      TestUtil.flush(utility, TableName.valueOf(tableName1));
      TestUtil.flush(utility, TableName.valueOf(tableName2));
      rs = stmt.executeQuery(sql);
      // 1 Data block per table in UNION ALL
      readMetrics = getQueryReadMetrics(rs);
      assertOnReadsFromFs(tableName1, readMetrics, 1);
      assertOnReadsFromFs(tableName2, readMetrics, 1);
      rs = stmt.executeQuery(sql);
      readMetrics = getQueryReadMetrics(rs);
      assertOnReadsFromBlockcache(tableName1, readMetrics);
      assertOnReadsFromBlockcache(tableName2, readMetrics);
    }
  }

  @Test
  public void testJoinQuery() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'c', 'c1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'd', 'd1', 'd2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'e', 'e1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'f', 'f1', 'v2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (5, 'g', 'g1', 'v2')");
      conn.commit();
      String sql =
        "SELECT a.k1 as k1, b.k2 as k2, b.v1 as v1, a.total_count as total_count FROM (SELECT k1, COUNT(*) as total_count FROM "
          + tableName + " WHERE k1 IN (1, 3) GROUP BY k1) a JOIN (SELECT k1, k2, v1 FROM "
          + tableName + " WHERE k1 IN (1, 3) AND k2 = 'a') b ON a.k1 = b.k1";
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(tableName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      rs = stmt.executeQuery(sql);
      // 1 Data Block for left table and data block for right table is read from block cache
      assertOnReadsFromFs(tableName, getQueryReadMetrics(rs), 1, true);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(tableName, getQueryReadMetrics(rs));
    }
  }

  @Test
  public void testQueryOnUncoveredIndex() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (v1)");
      conn.commit();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'a2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (2, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'c', 'c1', 'c2')");
      conn.commit();
      String sql = "SELECT k1, k2, v1, v2 FROM " + tableName + " WHERE v1 = 'b1'";
      ExplainPlan explainPlan =
        stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql).getExplainPlan();
      ExplainPlanAttributes planAttributes = explainPlan.getPlanStepsAsAttributes();
      String tableNameFromExplainPlan = planAttributes.getTableName();
      Assert.assertEquals(indexName, tableNameFromExplainPlan);
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(indexName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      TestUtil.flush(utility, TableName.valueOf(indexName));
      rs = stmt.executeQuery(sql);
      // 1 Data block from index table and 1 data block from data table as index is uncovered
      assertOnReadsFromFs(indexName, getQueryReadMetrics(rs), 2);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(indexName, getQueryReadMetrics(rs));
    }
  }

  @Test
  public void testQueryOnCoveredIndexWithoutReadRepair() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + " (v1) INCLUDE (v2)");
      conn.commit();
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'a2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (2, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'c', 'c1', 'c2')");
      conn.commit();
      String sql = "SELECT k1, k2, v1, v2 FROM " + tableName + " WHERE v1 = 'b1'";
      ExplainPlan explainPlan =
        stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql).getExplainPlan();
      ExplainPlanAttributes planAttributes = explainPlan.getPlanStepsAsAttributes();
      String tableNameFromExplainPlan = planAttributes.getTableName();
      Assert.assertEquals(indexName, tableNameFromExplainPlan);
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(indexName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      TestUtil.flush(utility, TableName.valueOf(indexName));
      rs = stmt.executeQuery(sql);
      // 1 Data Block from index table
      assertOnReadsFromFs(indexName, getQueryReadMetrics(rs), 1);
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(indexName, getQueryReadMetrics(rs));
    }
  }

  @Test
  public void testQueryOnCoveredIndexWithReadRepair() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + " (v1) INCLUDE (v2)");
      conn.commit();
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (1, 'a', 'a1', 'a2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (2, 'b', 'b1', 'b2')");
      stmt.execute("UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (3, 'c', 'c1', 'c2')");
      conn.commit();
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
      TestUtil.dumpTable(conn, TableName.valueOf(indexName));
      String sql = "SELECT k1, k2, v1, v2 FROM " + tableName + " WHERE v1 = 'b1'";
      ResultSet rs = stmt.executeQuery(sql);
      assertOnReadsFromMemstore(indexName, getQueryReadMetrics(rs));
      TestUtil.flush(utility, TableName.valueOf(tableName));
      TestUtil.flush(utility, TableName.valueOf(indexName));
      sql = "SELECT k1, k2, v1, v2 FROM " + tableName + " WHERE v1 = 'c1'";
      rs = stmt.executeQuery(sql);
      // 1 data block of index table from GlobalIndexScanner, 1 bloom block of data table while
      // doing read repair, 2 times same data block of data table while doing read repair as read
      // repair opens region scanner thrice and second time its done with caching to block cache
      // disabled and third time its done with caching to block cache enabled. The newly repaired
      // column qualifier will be in memstore of index table and
      // GlobalIndexScanner verifies if row got repaired correctly so, read will even happen from
      // memstore.
      assertOnReadsFromFs(indexName, getQueryReadMetrics(rs), 4, true, true);
      sql = "SELECT k1, k2, v1, v2 FROM " + tableName + " WHERE v1 = 'a1'";
      rs = stmt.executeQuery(sql);
      assertOnReadsFromBlockcache(indexName, getQueryReadMetrics(rs), true);
    } finally {
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
    }
  }

  @Test
  public void testScanRpcQueueWaitTime() throws Exception {
    int handlerCount =
      Integer.parseInt(utility.getConfiguration().get(HConstants.REGION_SERVER_HANDLER_COUNT));
    int threadPoolSize = 6 * handlerCount;
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);
    String tableName = generateUniqueName();
    int numRows = 10000;
    CountDownLatch latch = new CountDownLatch(threadPoolSize);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTable(conn, tableName, "");
      Statement stmt = conn.createStatement();
      for (int i = 1; i <= numRows; i++) {
        stmt.execute(
          "UPSERT INTO " + tableName + " (k1, k2, v1, v2) VALUES (" + i + ", 'a', 'v1', 'v2')");
        if (i % 100 == 0) {
          conn.commit();
        }
      }
      conn.commit();
      TestUtil.flush(utility, TableName.valueOf(tableName));
      AtomicLong scanRpcQueueWaitTime = new AtomicLong(0);
      AtomicLong rpcScanProcessingTime = new AtomicLong(0);
      for (int i = 0; i < threadPoolSize; i++) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              Statement stmt = conn.createStatement();
              stmt.setFetchSize(2);
              ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
              int rowsRead = 0;
              while (rs.next()) {
                rowsRead++;
              }
              Assert.assertEquals(numRows, rowsRead);
              Map<String, Map<MetricType, Long>> readMetrics =
                PhoenixRuntime.getRequestReadMetricInfo(rs);
              scanRpcQueueWaitTime
                .addAndGet(readMetrics.get(tableName).get(MetricType.RPC_SCAN_QUEUE_WAIT_TIME));
              rpcScanProcessingTime
                .addAndGet(readMetrics.get(tableName).get(MetricType.RPC_SCAN_PROCESSING_TIME));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            } finally {
              latch.countDown();
            }
          }
        });
      }
      latch.await();
      Assert.assertTrue(scanRpcQueueWaitTime.get() > 0);
      Assert.assertTrue(rpcScanProcessingTime.get() > 0);
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
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

  private void assertOnReadsFromFs(String tableName, Map<String, Map<MetricType, Long>> readMetrics,
    long expectedBlocksReadOps) throws Exception {
    assertOnReadsFromFs(tableName, readMetrics, expectedBlocksReadOps, false);
  }

  private void assertOnReadsFromFs(String tableName, Map<String, Map<MetricType, Long>> readMetrics,
    long expectedBlocksReadOps, boolean isReadFromBlockCacheExpected) throws Exception {
    assertOnReadsFromFs(tableName, readMetrics, expectedBlocksReadOps, isReadFromBlockCacheExpected,
      false);
  }

  private void assertOnReadsFromFs(String tableName, Map<String, Map<MetricType, Long>> readMetrics,
    long expectedBlocksReadOps, boolean isReadFromBlockCacheExpected,
    boolean isReadFromMemstoreExpected) throws Exception {
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS) > 0);
    Assert.assertEquals(expectedBlocksReadOps,
      (long) readMetrics.get(tableName).get(MetricType.BLOCK_READ_OPS_COUNT));
    if (isReadFromMemstoreExpected) {
      Assert
        .assertTrue((long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE) > 0);
    } else {
      Assert.assertEquals(0,
        (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE));
    }
    if (isReadFromBlockCacheExpected) {
      Assert.assertTrue(
        (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE) > 0);
    } else {
      Assert.assertEquals(0,
        (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE));
    }
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.RPC_SCAN_PROCESSING_TIME) > 0);
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.FS_READ_TIME) > 0);
  }

  private void assertOnReadsFromBlockcache(String tableName,
    Map<String, Map<MetricType, Long>> readMetrics) throws Exception {
    assertOnReadsFromBlockcache(tableName, readMetrics, false);
  }

  private void assertOnReadsFromBlockcache(String tableName,
    Map<String, Map<MetricType, Long>> readMetrics, boolean isReadFromMemstoreExpected)
    throws Exception {
    Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE) > 0);
    Assert.assertEquals(0, (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS));
    Assert.assertEquals(0, (long) readMetrics.get(tableName).get(MetricType.BLOCK_READ_OPS_COUNT));
    if (isReadFromMemstoreExpected) {
      Assert
        .assertTrue((long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE) > 0);
    } else {
      Assert.assertEquals(0,
        (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE));
    }
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
