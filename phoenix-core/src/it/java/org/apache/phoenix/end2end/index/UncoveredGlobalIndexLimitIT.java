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

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.SERVER_PAGE_SIZE_MS;
import static org.apache.phoenix.query.explain.ExplainPlanTestUtil.assertPlan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Regression tests for PHOENIX-8015: LIMIT counts verified uncovered-index rows. */
@Category(NeedsOwnMiniClusterTest.class)
public class UncoveredGlobalIndexLimitIT extends BaseTest {
  private static final long AGE_THRESHOLD_MS = 60_000;
  // Keep stale entries live in HBase and in Phoenix while testing the independent repair threshold.
  private static final int TTL_SECONDS = 3600;
  private static final int ROW_COUNT = 20;
  private static final int LIMIT = 5;
  // Force multiple index batches before and after the first valid row.
  private static final long INDEX_PAGE_SIZE_IN_ROWS = 3;
  private static final long UPDATED_TIME_BASE = 1_700_000_000_000L;
  private final ManualEnvironmentEdge clock = new ManualEnvironmentEdge();

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
      Long.toString(AGE_THRESHOLD_MS));
    props.put(QueryServices.PHOENIX_COMPACTION_ENABLED, Boolean.toString(true));
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    props.put(QueryServices.INDEX_PAGE_SIZE_IN_ROWS, Long.toString(INDEX_PAGE_SIZE_IN_ROWS));
    // BaseTest defaults to a dummy result after every row; exercise a complete server batch here.
    props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(60_000));
    props.put("hbase.coprocessor.region.classes", LimitTestObserver.class.getName());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    // BaseTest overwrites the age threshold with zero during mini-cluster configuration.
    // Restore it before creating user tables so their coprocessors inherit the intended value.
    getUtility().getConfiguration()
      .setLong(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, AGE_THRESHOLD_MS);
    getUtility().getHBaseCluster().getRegionServerThreads().forEach(thread -> {
      thread.getRegionServer().getConfiguration().setLong(
        QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, AGE_THRESHOLD_MS);
      assertEquals(AGE_THRESHOLD_MS, thread.getRegionServer().getConfiguration()
        .getLong(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, -1));
    });
  }

  @After
  public void resetClock() throws Exception {
    LimitTestObserver.indexTable = null;
    LimitTestObserver.dataTable = null;
    LimitTestObserver.limitedTable = null;
    LimitTestObserver.lookupTable = null;
    EnvironmentEdgeManager.reset();
    assertFalse("refCount leaked", isAnyStoreRefCountLeaked());
  }

  @Test
  public void testLimitWithLeadingStaleRowsAndTTLMasking() throws Exception {
    assertLimitWithLeadingStaleRows(true);
  }

  @Test
  public void testLimitWithLeadingStaleRowsWithoutTTLMasking() throws Exception {
    assertLimitWithLeadingStaleRows(false);
  }

  private void assertLimitWithLeadingStaleRows(boolean finiteTTL) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Fixture fixture = createFixture(conn, finiteTTL, 1000);
      String query = fixture.indexQuery("") + " LIMIT " + LIMIT;
      assertPlan(conn, query).table(fixture.indexTable).scanType("FULL SCAN")
        .serverRowLimit((long) LIMIT);
      assertRows(conn, query, LIMIT);
      assertStaleEntriesPresent(fixture);
    }
  }

  @Test
  public void testLimitControlsWithLeadingStaleRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Use the same FOREVER-TTL configuration as the reproduced PageFilter failure.
      Fixture fixture = createFixture(conn, false, 1000);
      String baseQuery = "SELECT /*+ NO_INDEX */ ID, PAYLOAD, UPDATED_TIME FROM "
        + fixture.dataTable + " ORDER BY UPDATED_TIME ASC LIMIT " + LIMIT;
      assertPlan(conn, baseQuery).table(fixture.dataTable);
      assertRows(conn, baseQuery, LIMIT);

      String boundedQuery =
        fixture.indexQuery(" WHERE UPDATED_TIME > TO_TIMESTAMP('2000-01-01')") + " LIMIT " + LIMIT;
      assertPlan(conn, boundedQuery).table(fixture.indexTable).scanType("RANGE SCAN");
      assertRows(conn, boundedQuery, LIMIT);

      // PAYLOAD comes from the base row, so this exercises INDEX_FILTER / INDEX_LIMIT.
      String filteredQuery = fixture.indexQuery(" WHERE PAYLOAD IS NOT NULL") + " LIMIT " + LIMIT;
      assertPlan(conn, filteredQuery).table(fixture.indexTable);
      assertRows(conn, filteredQuery, LIMIT);

      assertRows(conn, fixture.indexQuery(""), ROW_COUNT);
      // All controls leave the young stale entries in place for the same reason as the LIMIT IT.
      assertStaleEntriesPresent(fixture);
    }
  }

  @Test
  public void testLimitWithPartiallyStaleLeadingRows() throws Exception {
    for (boolean finiteTTL : new boolean[] { false, true }) {
      try (Connection conn = DriverManager.getConnection(getUrl())) {
        Fixture fixture = createFixture(conn, finiteTTL, 1000, 3);
        assertRows(conn, fixture.indexQuery("") + " LIMIT " + LIMIT, LIMIT);
        assertStaleEntriesPresent(fixture);
      }
      EnvironmentEdgeManager.reset();
    }
  }

  @Test
  public void testLimitLargerThanValidRowCount() throws Exception {
    for (boolean finiteTTL : new boolean[] { false, true }) {
      try (Connection conn = DriverManager.getConnection(getUrl())) {
        Fixture fixture = createFixture(conn, finiteTTL, 1000);
        assertRows(conn, fixture.indexQuery("") + " LIMIT " + (ROW_COUNT * 3), ROW_COUNT);
        assertStaleEntriesPresent(fixture);
      }
      EnvironmentEdgeManager.reset();
    }
  }

  @Test
  public void testLimitWithOffset() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Fixture fixture = createFixture(conn, false, 1000);
      assertRows(conn, fixture.indexQuery("") + " LIMIT " + LIMIT + " OFFSET 3", LIMIT, 3);
      assertStaleEntriesPresent(fixture);
    }
  }

  @Test
  public void testLimitAcrossDummyPages() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Fixture fixture = createFixture(conn, false, 1000);
      LimitTestObserver.dummyResults.set(0);
      LimitTestObserver.indexTable = fixture.indexTable;
      LimitTestObserver.dataTable = fixture.dataTable;
      assertRows(conn, fixture.indexQuery("") + " LIMIT " + LIMIT, LIMIT);
      assertTrue("The query must return actual PagingFilter dummy results",
        LimitTestObserver.dummyResults.get() > 0);
      assertStaleEntriesPresent(fixture);
    }
  }

  @Test
  public void testSmallLimitCapsDataLookupsAtDefaultBatchSize() throws Exception {
    // Use the production default rather than the small batches used by the other regressions.
    getUtility().getHBaseCluster().getRegionServerThreads()
      .forEach(thread -> thread.getRegionServer().getConfiguration().setLong(
        QueryServices.INDEX_PAGE_SIZE_IN_ROWS,
        QueryServicesOptions.DEFAULT_INDEX_PAGE_SIZE_IN_ROWS));
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      for (int staleRowCount : new int[] { 0, 3 }) {
        try {
          Fixture fixture = createFixture(conn, false, 1000, staleRowCount);
          LimitTestObserver.lookupRows.set(0);
          LimitTestObserver.lookupTable = fixture.dataTable;
          String query = fixture.indexQuery("") + " LIMIT 1";
          assertPlan(conn, query).table(fixture.indexTable).serverRowLimit(1L);
          assertRows(conn, query, 1);
          assertEquals("LIMIT 1 must fetch only the stale prefix and the first valid candidate",
            staleRowCount + 1, LimitTestObserver.lookupRows.get());
          assertStaleEntriesPresent(fixture);
        } finally {
          EnvironmentEdgeManager.reset();
        }
      }
    } finally {
      getUtility().getHBaseCluster().getRegionServerThreads()
        .forEach(thread -> thread.getRegionServer().getConfiguration()
          .setLong(QueryServices.INDEX_PAGE_SIZE_IN_ROWS, INDEX_PAGE_SIZE_IN_ROWS));
    }
  }

  @Test
  public void testServerLimitCountsOnlyVerifiedRows() throws Exception {
    for (boolean finiteTTL : new boolean[] { false, true }) {
      try (Connection conn = DriverManager.getConnection(getUrl())) {
        Fixture fixture = createFixture(conn, finiteTTL, 1000);
        LimitTestObserver.returnedRows.set(0);
        LimitTestObserver.limitedTable = fixture.indexTable;
        assertRows(conn, fixture.indexQuery("") + " LIMIT " + LIMIT, LIMIT);
        // Client-side LIMIT can hide an unbounded server scan. Count rows actually sent by HBase.
        assertEquals("Server LIMIT must count verified rows", LIMIT,
          LimitTestObserver.returnedRows.get());
        assertStaleEntriesPresent(fixture);
      } finally {
        LimitTestObserver.limitedTable = null;
        EnvironmentEdgeManager.reset();
      }
    }
  }

  private Fixture createFixture(Connection conn, boolean finiteTTL, long updateDelayMs)
    throws Exception {
    return createFixture(conn, finiteTTL, updateDelayMs, ROW_COUNT);
  }

  private Fixture createFixture(Connection conn, boolean finiteTTL, long updateDelayMs,
    int staleRowCount) throws Exception {
    String dataTable = generateUniqueName();
    String indexTable = generateUniqueName();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + dataTable
        + " (ID VARCHAR NOT NULL PRIMARY KEY, UPDATED_TIME TIMESTAMP, PAYLOAD VARCHAR)"
        + (finiteTTL ? " TTL=" + TTL_SECONDS + ", IS_STRICT_TTL=true" : ""));
      stmt.execute("CREATE UNCOVERED INDEX " + indexTable + " ON " + dataTable + " (UPDATED_TIME)");
    }
    clock.setValue(EnvironmentEdgeManager.currentTimeMillis() + 1);
    EnvironmentEdgeManager.injectEdge(clock);
    long insertTime = clock.currentTime();
    upsertRows(conn, dataTable, false, staleRowCount);
    List<Result> initialRows = readPhysicalRows(indexTable);
    assertEquals(ROW_COUNT, initialRows.size());
    // This unsalted index orders NULL-keyed entries before the non-NULL entries.
    List<Result> staleRows = new ArrayList<>(initialRows.subList(0, staleRowCount));
    assertEquals("Initial NULL-keyed index entries", staleRowCount, staleRows.size());
    for (Result stale : staleRows) {
      assertEquals(insertTime, stale.rawCells()[0].getTimestamp());
    }

    clock.incrementValue(updateDelayMs);
    // Supply the indexed column explicitly to take the write-optimized uncovered-index path.
    upsertRows(conn, dataTable, true, staleRowCount);
    Fixture fixture = new Fixture(dataTable, indexTable, staleRows);
    assertEquals("Both stale and current index entries must physically exist",
      ROW_COUNT + staleRowCount, readPhysicalRows(indexTable).size());
    assertStaleEntriesPresent(fixture);
    for (Result data : readPhysicalRows(dataTable)) {
      assertEquals("Base rows must have the recent update timestamp", clock.currentTime(),
        data.rawCells()[0].getTimestamp());
    }
    return fixture;
  }

  private void upsertRows(Connection conn, String dataTable, boolean nonNullTimestamp,
    int staleRowCount) throws Exception {
    try (PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO " + dataTable + " (ID, UPDATED_TIME, PAYLOAD) VALUES (?, ?, ?)")) {
      for (int i = 0; i < ROW_COUNT; i++) {
        stmt.setString(1, "tenant-" + i);
        if (nonNullTimestamp || i >= staleRowCount) {
          stmt.setTimestamp(2, new Timestamp(UPDATED_TIME_BASE + i * 1000L));
        } else {
          stmt.setNull(2, Types.TIMESTAMP);
        }
        stmt.setString(3, "payload-" + i);
        stmt.executeUpdate();
      }
      conn.commit();
    }
  }

  private List<Result> readPhysicalRows(String tableName) throws Exception {
    // No Phoenix scan attributes: inspect live HBase entries without uncovered verification.
    List<Result> rows = new ArrayList<>();
    try (Table table = getUtility().getConnection().getTable(TableName.valueOf(tableName));
      ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result result : scanner) {
        rows.add(result);
      }
    }
    return rows;
  }

  private void assertStaleEntriesPresent(Fixture fixture) throws Exception {
    try (
      Table index = getUtility().getConnection().getTable(TableName.valueOf(fixture.indexTable))) {
      for (Result stale : fixture.staleRows) {
        Result current = index.get(new Get(stale.getRow()));
        assertFalse("Fixture lost a stale index entry", current.isEmpty());
        assertEquals("Stale entry timestamp must not advance with the data row",
          stale.rawCells()[0].getTimestamp(), current.rawCells()[0].getTimestamp());
      }
    }
  }

  private void assertRows(Connection conn, String query, int expectedRows) throws Exception {
    assertRows(conn, query, expectedRows, 0);
  }

  private void assertRows(Connection conn, String query, int expectedRows, int firstRow)
    throws Exception {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        assertEquals("tenant-" + (firstRow + count), rs.getString(1));
        assertEquals("payload-" + (firstRow + count), rs.getString(2));
        assertEquals(new Timestamp(UPDATED_TIME_BASE + (firstRow + count) * 1000L),
          rs.getTimestamp(3));
        count++;
      }
      assertEquals("Valid rows returned by: " + query, expectedRows, count);
    }
  }

  public static class LimitTestObserver extends SimpleRegionObserver {
    private static volatile String indexTable;
    private static volatile String dataTable;
    private static final AtomicInteger dummyResults = new AtomicInteger();
    private static volatile String limitedTable;
    private static final AtomicInteger returnedRows = new AtomicInteger();
    private static volatile String lookupTable;
    private static final AtomicInteger lookupRows = new AtomicInteger();

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> context, Scan scan) {
      String table =
        context.getEnvironment().getRegion().getTableDescriptor().getTableName().getNameAsString();
      if (table.equals(indexTable) || table.equals(dataTable)) {
        scan.setAttribute(SERVER_PAGE_SIZE_MS, Bytes.toBytes(0L));
        if (scan.getFilter() instanceof PagingFilter) {
          scan
            .setFilter(new PagingFilter(((PagingFilter) scan.getFilter()).getDelegateFilter(), 0));
        }
      }
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> context,
      InternalScanner scanner, List<Result> results, int limit, boolean hasMore) {
      String table =
        context.getEnvironment().getRegion().getTableDescriptor().getTableName().getNameAsString();
      for (Result result : results) {
        if (table.equals(indexTable) || table.equals(dataTable)) {
          if (ScanUtil.isDummy(result)) {
            dummyResults.incrementAndGet();
          }
        }
        if (table.equals(lookupTable) && !result.isEmpty() && !ScanUtil.isDummy(result)) {
          lookupRows.incrementAndGet();
        }
      }
      if (table.equals(limitedTable)) {
        for (Result result : results) {
          if (!result.isEmpty() && !ScanUtil.isDummy(result)) {
            returnedRows.incrementAndGet();
          }
        }
      }
      return hasMore;
    }
  }

  private static class Fixture {
    final String dataTable;
    final String indexTable;
    final List<Result> staleRows;

    Fixture(String dataTable, String indexTable, List<Result> staleRows) {
      this.dataTable = dataTable;
      this.indexTable = indexTable;
      this.staleRows = staleRows;
    }

    String indexQuery(String predicate) {
      return "SELECT /*+ INDEX(" + dataTable + " " + indexTable
        + ") */ ID, PAYLOAD, UPDATED_TIME FROM " + dataTable + predicate
        + " ORDER BY UPDATED_TIME ASC";
    }
  }
}
