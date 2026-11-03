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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.query.explain.ExplainPlanTestUtil.assertPlan;
import static org.apache.phoenix.query.explain.ExplainPlanTestUtil.getExplainAttributes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Checks that a filtered scan on a global index uses a constant number of scan RPCs when the scan
 * reads more bytes than the pread limit of the store scanner. HBase switches a long scan from pread
 * to stream read only on the scanner that the RPC layer opens. If the index scan runs on a
 * replacement scanner, each index row after the pread limit ends the RPC. The checker changes the
 * scan filter before the scanner opens, so the class also checks a query that reads that filter.
 * Read repair opens new scanners, and these scanners must switch to stream read on a long scan.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class GlobalIndexCheckerScanRpcIT extends ParallelStatsDisabledIT {
  private static final int ROW_COUNT = 2000;
  private static final int MATCH_COUNT = 5;
  private static final int LIMIT = 1500;
  private static final int MAX_EXPECTED_RPCS = 10;
  private static final String PADDING = Strings.repeat("x", 200);
  private static final int UNVERIFIED_COUNT = 3;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
    // A small pread limit lets a short index scan pass it many times over.
    props.put(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, Long.toString(4096));
    props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @After
  public void resetIndexFailures() {
    IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
  }

  private static String v1(int id) {
    return String.format("v1_%06d_%s", id, PADDING);
  }

  private static void createAndLoadTable(Connection conn, String tableName) throws Exception {
    conn.createStatement().execute("CREATE TABLE " + tableName
      + " (ID INTEGER NOT NULL PRIMARY KEY, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR)");
    try (PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?)")) {
      for (int i = 0; i < ROW_COUNT; i++) {
        // The matching rows sort last in each index, so the scan reads all other rows first.
        String match = i >= ROW_COUNT - MATCH_COUNT ? "match" : "other";
        stmt.setInt(1, i);
        stmt.setString(2, v1(i));
        stmt.setString(3, match);
        stmt.setString(4, match);
        stmt.executeUpdate();
        if (i % 500 == 0) {
          conn.commit();
        }
      }
      conn.commit();
    }
  }

  private static void createCoveredIndex(Connection conn, String tableName, String indexName)
    throws Exception {
    conn.createStatement()
      .execute("CREATE INDEX " + indexName + " ON " + tableName + " (V1) INCLUDE (V2)");
    TestUtil.flush(getUtility(), TableName.valueOf(tableName));
    TestUtil.flush(getUtility(), TableName.valueOf(indexName));
  }

  /**
   * Runs the upserts with failed post index updates. Each upsert leaves an unverified index row.
   */
  private static void upsertWithUnverifiedIndexRows(Connection conn, String... upserts)
    throws Exception {
    IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
    try {
      for (String upsert : upserts) {
        conn.createStatement().execute(upsert);
      }
      conn.commit();
    } finally {
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
    }
  }

  private static void assertRpcCount(ResultSet rs) throws Exception {
    long rpcs = getRpcCount(rs);
    assertTrue("Expected at most " + MAX_EXPECTED_RPCS + " scan RPCs but got " + rpcs,
      rpcs <= MAX_EXPECTED_RPCS);
  }

  private static long getRpcCount(ResultSet rs) throws Exception {
    Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
    long rpcs = 0;
    for (Map<MetricType, Long> tableMetrics : metrics.values()) {
      Long value = tableMetrics.get(MetricType.COUNT_RPC_CALLS);
      if (value != null) {
        rpcs += value;
      }
    }
    return rpcs;
  }

  private static void assertConstantRpcCount(Connection conn, String query, String indexName)
    throws Exception {
    assertPlan(conn, query).table(indexName);
    try (ResultSet rs = conn.createStatement().executeQuery(query)) {
      int rows = 0;
      while (rs.next()) {
        rows++;
      }
      assertEquals(MATCH_COUNT, rows);
      assertRpcCount(rs);
    }
  }

  @Test
  public void testCoveredIndexFilteredScanRpcCount() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndLoadTable(conn, tableName);
      createCoveredIndex(conn, tableName, indexName);
      // A filter on a covered column makes the index checker wrap the scan filter.
      String query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ V1, V2 FROM "
        + tableName + " WHERE V2 = 'match'";
      assertConstantRpcCount(conn, query, indexName);
    }
  }

  @Test
  public void testCoveredIndexLimitScanRpcCount() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndLoadTable(conn, tableName);
      createCoveredIndex(conn, tableName, indexName);
      // A key range with a limit and no other condition sends a page filter with the index scan.
      assertLimitScan(conn, tableName, indexName);
    }
  }

  @Test
  public void testUncoveredIndexLimitScanRpcCount() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndLoadTable(conn, tableName);
      conn.createStatement()
        .execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (V1)");
      TestUtil.flush(getUtility(), TableName.valueOf(tableName));
      TestUtil.flush(getUtility(), TableName.valueOf(indexName));
      // The index checker does not process uncovered index scans. This test guards the same scan
      // shape on the uncovered index scanner.
      assertLimitScan(conn, tableName, indexName);
    }
  }

  @Test
  public void testCoveredIndexLimitScanAfterReadRepairRpcCount() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndLoadTable(conn, tableName);
      createCoveredIndex(conn, tableName, indexName);
      int firstRow = ROW_COUNT - LIMIT - MATCH_COUNT;
      // The first index row of the range is unverified, and its data row has a new V1 value.
      // Read repair skips this index row. The next index row is unverified, but its data row
      // keeps its V1 value. Read repair keeps this index row. Each repair opens a new scanner
      // for the rest of the range.
      upsertWithUnverifiedIndexRows(conn,
        "UPSERT INTO " + tableName + " (ID, V1) VALUES (" + firstRow + ", 'a_moved')",
        "UPSERT INTO " + tableName + " (ID, V2) VALUES (" + (firstRow + 1) + ", 'other')");
      String query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ ID, V2 FROM "
        + tableName + " WHERE V1 >= '" + String.format("v1_%06d", firstRow) + "' LIMIT " + LIMIT;
      assertPlan(conn, query).table(indexName);
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        for (int i = firstRow + 1; i <= firstRow + LIMIT; i++) {
          assertTrue(rs.next());
          assertEquals(i, rs.getInt(1));
          assertEquals(i >= ROW_COUNT - MATCH_COUNT ? "match" : "other", rs.getString(2));
        }
        assertFalse(rs.next());
        assertRpcCount(rs);
      }
    }
  }

  @Test
  public void testCoveredIndexReverseLimitScanAfterReadRepairRpcCount() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndLoadTable(conn, tableName);
      createCoveredIndex(conn, tableName, indexName);
      int lastRow = LIMIT + MATCH_COUNT - 1;
      // A reverse scan reads the range from its last row. The repairs at that end open new
      // reverse scanners for the rest of the range. The query plan uses a reverse scan only if
      // the null order of the key column agrees with it, so the query uses NULLS LAST.
      upsertWithUnverifiedIndexRows(conn,
        "UPSERT INTO " + tableName + " (ID, V1) VALUES (" + lastRow + ", 'z_moved')",
        "UPSERT INTO " + tableName + " (ID, V2) VALUES (" + (lastRow - 1) + ", 'other')");
      String query =
        "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ ID, V2 FROM " + tableName
          + " WHERE V1 <= '" + v1(lastRow) + "' ORDER BY V1 DESC NULLS LAST LIMIT " + LIMIT;
      assertPlan(conn, query).table(indexName).clientSortedBy("REVERSE");
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        for (int i = lastRow - 1; i >= lastRow - LIMIT; i--) {
          assertTrue(rs.next());
          assertEquals(i, rs.getInt(1));
          assertEquals("other", rs.getString(2));
        }
        assertFalse(rs.next());
        assertRpcCount(rs);
      }
    }
  }

  @Test
  public void testCoveredIndexFilteredScanAfterReadRepairRpcCount() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndLoadTable(conn, tableName);
      createCoveredIndex(conn, tableName, indexName);
      // The first index rows become unverified, and their covered value does not match the
      // filter. The unverified row filter passes each one to read repair. After a repair, the
      // filter rejects the repaired row, and the new scanner returns the next unverified row.
      // The checker then rewinds to that row. Thus the scan opens several scanners at the start.
      String[] upserts = new String[UNVERIFIED_COUNT];
      for (int i = 0; i < UNVERIFIED_COUNT; i++) {
        upserts[i] = "UPSERT INTO " + tableName + " (ID, V2) VALUES (" + i + ", 'other')";
      }
      upsertWithUnverifiedIndexRows(conn, upserts);
      String query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ ID, V2 FROM "
        + tableName + " WHERE V2 = 'match'";
      assertPlan(conn, query).table(indexName);
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        for (int i = ROW_COUNT - MATCH_COUNT; i < ROW_COUNT; i++) {
          assertTrue(rs.next());
          assertEquals(i, rs.getInt(1));
          assertEquals("match", rs.getString(2));
        }
        assertFalse(rs.next());
        assertRpcCount(rs);
      }
    }
  }

  @Test
  public void testCoveredIndexServerMergeDistinctScan() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (ID1 VARCHAR NOT NULL,"
          + " ID2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR"
          + " CONSTRAINT PK PRIMARY KEY (ID1, ID2))");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (V1) INCLUDE (V3)");
      String[][] rows = { { "a1", "a1", "other" }, { "a1", "a2", "other" }, { "a1", "a3", "match" },
        { "a2", "a1", "other" }, { "a3", "a1", "other" }, { "a3", "a2", "match" },
        { "a4", "a1", "match" } };
      try (PreparedStatement stmt =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, 'v1', ?, 'v3')")) {
        for (String[] row : rows) {
          stmt.setString(1, row[0]);
          stmt.setString(2, row[1]);
          stmt.setString(3, row[2]);
          stmt.executeUpdate();
        }
      }
      conn.commit();
      // The index does not cover V2, so the region server merges the index rows with the data
      // rows. The checker wraps the distinct prefix filter of this covered index scan. The merge
      // scanner must still find the distinct prefix filter, because the merge can reject the
      // first index row of a prefix.
      String query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ DISTINCT ID1 FROM "
        + tableName + " WHERE V1 = 'v1' AND V2 = 'match'";
      ExplainPlanAttributes attributes = getExplainAttributes(conn, query);
      assertPlan(attributes).table(indexName);
      assertNotNull(attributes.getServerMergeColumns());
      assertNotNull(attributes.getServerDistinctFilter());
      List<String> ids = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
      assertEquals(Arrays.asList("a1", "a3", "a4"), ids);
    }
  }

  private static void assertLimitScan(Connection conn, String tableName, String indexName)
    throws Exception {
    int firstRow = ROW_COUNT - LIMIT - MATCH_COUNT;
    String query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ ID, V2 FROM "
      + tableName + " WHERE V1 >= '" + String.format("v1_%06d", firstRow) + "' LIMIT " + LIMIT;
    assertPlan(conn, query).table(indexName);
    try (ResultSet rs = conn.createStatement().executeQuery(query)) {
      for (int i = firstRow; i < firstRow + LIMIT; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
        assertEquals("other", rs.getString(2));
      }
      assertFalse(rs.next());
      assertRpcCount(rs);
    }
  }
}
