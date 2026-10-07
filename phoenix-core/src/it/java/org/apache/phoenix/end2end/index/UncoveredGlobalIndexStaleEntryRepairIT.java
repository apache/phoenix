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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Regression tests for PHOENIX-8016: age stale-entry cleanup by the index timestamp. */
@Category(NeedsOwnMiniClusterTest.class)
public class UncoveredGlobalIndexStaleEntryRepairIT extends BaseTest {
  private static final long AGE_THRESHOLD_MS = 60_000;
  // Keep stale entries live in HBase and in Phoenix while testing the independent repair threshold.
  private static final int TTL_SECONDS = 3600;
  private static final int ROW_COUNT = 20;
  private static final long UPDATED_TIME_BASE = 1_700_000_000_000L;
  private final ManualEnvironmentEdge clock = new ManualEnvironmentEdge();

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
      Long.toString(AGE_THRESHOLD_MS));
    props.put(QueryServices.PHOENIX_COMPACTION_ENABLED, Boolean.toString(true));
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    // BaseTest defaults to a dummy result after every row; exercise a complete server batch here.
    props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(60_000));
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
    EnvironmentEdgeManager.reset();
    assertFalse("refCount leaked", isAnyStoreRefCountLeaked());
  }

  @Test
  public void testOldStaleEntriesAreDeletedWhileDataRowsAreRecent() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Fixture fixture = createFixture(conn, AGE_THRESHOLD_MS * 2);
      // No LIMIT: isolate cleanup from premature scan termination in PHOENIX-8015.
      String query = fixture.indexQuery("");
      assertPlan(conn, query).table(fixture.indexTable).scanType("FULL SCAN");
      assertRows(conn, query, ROW_COUNT);
      try (Table index =
        getUtility().getConnection().getTable(TableName.valueOf(fixture.indexTable))) {
        for (Result stale : fixture.staleRows) {
          assertTrue("Old stale index entry survived read repair",
            index.get(new Get(stale.getRow())).isEmpty());
        }
      }
      assertEquals("Current index entries must survive cleanup", ROW_COUNT,
        readPhysicalRows(fixture.indexTable).size());
      assertRows(conn, "SELECT /*+ NO_INDEX */ ID, PAYLOAD, UPDATED_TIME FROM " + fixture.dataTable
        + " ORDER BY UPDATED_TIME ASC", ROW_COUNT);
    }
  }

  @Test
  public void testYoungStaleEntriesAreNotDeleted() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Fixture fixture = createFixture(conn, 1000);
      assertRows(conn, fixture.indexQuery(""), ROW_COUNT);
      assertStaleEntriesPresent(fixture);
    }
  }

  private Fixture createFixture(Connection conn, long updateDelayMs) throws Exception {
    String dataTable = generateUniqueName();
    String indexTable = generateUniqueName();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + dataTable
        + " (ID VARCHAR NOT NULL PRIMARY KEY, UPDATED_TIME TIMESTAMP, PAYLOAD VARCHAR)" + " TTL="
        + TTL_SECONDS + ", IS_STRICT_TTL=true");
      stmt.execute("CREATE UNCOVERED INDEX " + indexTable + " ON " + dataTable + " (UPDATED_TIME)");
    }
    clock.setValue(EnvironmentEdgeManager.currentTimeMillis() + 1);
    EnvironmentEdgeManager.injectEdge(clock);
    long insertTime = clock.currentTime();
    upsertRows(conn, dataTable, false);
    List<Result> staleRows = readPhysicalRows(indexTable);
    assertEquals("Initial NULL-keyed index entries", ROW_COUNT, staleRows.size());
    for (Result stale : staleRows) {
      assertEquals(insertTime, stale.rawCells()[0].getTimestamp());
    }

    clock.incrementValue(updateDelayMs);
    // Supply the indexed column explicitly to take the write-optimized uncovered-index path.
    upsertRows(conn, dataTable, true);
    Fixture fixture = new Fixture(dataTable, indexTable, staleRows);
    assertEquals("Both stale and current index entries must physically exist", ROW_COUNT * 2,
      readPhysicalRows(indexTable).size());
    assertStaleEntriesPresent(fixture);
    for (Result data : readPhysicalRows(dataTable)) {
      assertEquals("Base rows must have the recent update timestamp", clock.currentTime(),
        data.rawCells()[0].getTimestamp());
    }
    return fixture;
  }

  private void upsertRows(Connection conn, String dataTable, boolean nonNullTimestamp)
    throws Exception {
    try (PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO " + dataTable + " (ID, UPDATED_TIME, PAYLOAD) VALUES (?, ?, ?)")) {
      for (int i = 0; i < ROW_COUNT; i++) {
        stmt.setString(1, "tenant-" + i);
        if (nonNullTimestamp) {
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
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        assertEquals("tenant-" + count, rs.getString(1));
        assertEquals("payload-" + count, rs.getString(2));
        assertEquals(new Timestamp(UPDATED_TIME_BASE + count * 1000L), rs.getTimestamp(3));
        count++;
      }
      assertEquals("Valid rows returned by: " + query, expectedRows, count);
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
