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
package org.apache.phoenix.replication.reader;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY;
import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.CONSISTENCY_POINT_UNAVAILABLE;
import static org.apache.phoenix.util.TestUtil.assertRawRowCount;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Integration tests for the replication consistency point compaction guard. Verifies that
 * CompactionScanner retains delete markers with timestamps newer than the consistency point on
 * clusters where replication replay is enabled.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class CompactionReplicationGuardIT extends BaseTest {

  private static final int MAX_LOOKBACK_AGE = 15;
  private static final int ROWS_POPULATED = 2;
  private ManualEnvironmentEdge injectEdge;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(5);
    props.put(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
    props.put(QueryServices.PHOENIX_COMPACTION_ENABLED, Boolean.toString(true));
    props.put(ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED,
      Boolean.toString(true));
    props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void beforeTest() throws Exception {
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(System.currentTimeMillis());
    EnvironmentEdgeManager.injectEdge(injectEdge);
  }

  @After
  public synchronized void afterTest() throws Exception {
    ReplicationLogReplayService.resetInstanceForTesting();
    EnvironmentEdgeManager.reset();
    boolean refCountLeaked = isAnyStoreRefCountLeaked();
    assertFalse("refCount leaked", refCountLeaked);
  }

  /**
   * Test 1: Guard retains delete markers that maxLookback would have purged. The consistency point
   * is set BEFORE the delete timestamp, so the delete marker is newer than the consistency point
   * and must be retained even after maxLookback window passes.
   */
  @Test(timeout = 120000L)
  public void testGuardRetainsDeleteMarkersNewerThanConsistencyPoint() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      createTable(dataTableName);
      TableName dataTable = TableName.valueOf(dataTableName);
      populateTable(dataTableName);

      injectEdge.incrementValue(1);
      long beforeDeleteTime = EnvironmentEdgeManager.currentTimeMillis();

      // Delete a row
      conn.createStatement().execute("DELETE FROM " + dataTableName + " WHERE id = 'a'");
      conn.commit();
      injectEdge.incrementValue(1);

      // Set consistency point BEFORE the delete — meaning replay hasn't caught up to the delete
      long consistencyPoint = beforeDeleteTime - 1;
      injectMockConsistencyPoint(consistencyPoint);

      // Advance time past maxLookback window — without guard, marker would be purged
      injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000 + 1000);

      flush(dataTable);
      majorCompact(dataTable);

      // Delete marker should be retained because its timestamp > consistencyPoint
      assertRawRowCount(conn, dataTable, ROWS_POPULATED);
    }
  }

  /**
   * Test 2: Both maxLookback and guard allow purge. The consistency point has advanced past the
   * delete marker AND maxLookback window has passed — marker should be purged.
   */
  @Test(timeout = 120000L)
  public void testDeleteMarkersPurgedWhenOlderThanBothConsistencyPointAndMaxLookback()
    throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      createTable(dataTableName);
      TableName dataTable = TableName.valueOf(dataTableName);
      populateTable(dataTableName);

      injectEdge.incrementValue(1);

      // Delete a row
      conn.createStatement().execute("DELETE FROM " + dataTableName + " WHERE id = 'a'");
      conn.commit();
      injectEdge.incrementValue(1);

      // Advance time past maxLookback
      injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000 + 1000);

      // Set consistency point to current time — replay is fully caught up
      long consistencyPoint = EnvironmentEdgeManager.currentTimeMillis();
      injectMockConsistencyPoint(consistencyPoint);

      flush(dataTable);
      majorCompact(dataTable);

      // Delete marker should be purged — both guard and maxLookback agree
      assertRawRowCount(conn, dataTable, ROWS_POPULATED - 1);
    }
  }

  /**
   * Test 3: MaxLookback retains even when guard wouldn't. Consistency point has advanced past the
   * delete, but we're still within the maxLookback window — marker retained by maxLookback.
   */
  @Test(timeout = 120000L)
  public void testMaxLookbackRetainsEvenWhenGuardAllowsPurge() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      createTable(dataTableName);
      TableName dataTable = TableName.valueOf(dataTableName);
      populateTable(dataTableName);

      injectEdge.incrementValue(1);

      // Delete a row
      conn.createStatement().execute("DELETE FROM " + dataTableName + " WHERE id = 'a'");
      conn.commit();
      injectEdge.incrementValue(1);

      // Set consistency point to current time — guard would allow purge
      long consistencyPoint = EnvironmentEdgeManager.currentTimeMillis();
      injectMockConsistencyPoint(consistencyPoint);

      // Do NOT advance past maxLookback — still within the window
      injectEdge.incrementValue(1);

      flush(dataTable);
      majorCompact(dataTable);

      // Delete marker retained because still within maxLookback window
      assertRawRowCount(conn, dataTable, ROWS_POPULATED);
    }
  }

  /**
   * Test 4: Guard fallback when consistency point unavailable — retains all delete markers. When
   * the replay service throws an exception (e.g., not initialized), the guard falls back to
   * retaining all markers to avoid data loss.
   */
  @Test(timeout = 120000L)
  public void testGuardFallbackRetainsAllWhenConsistencyPointUnavailable() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      createTable(dataTableName);
      TableName dataTable = TableName.valueOf(dataTableName);
      populateTable(dataTableName);

      injectEdge.incrementValue(1);

      // Delete a row
      conn.createStatement().execute("DELETE FROM " + dataTableName + " WHERE id = 'a'");
      conn.commit();
      injectEdge.incrementValue(1);

      // Inject consistency point as UNAVAILABLE — simulating fallback when replay service fails
      ReplicationLogReplayService.setConsistencyPointForTesting(getUtility().getConfiguration(),
        CONSISTENCY_POINT_UNAVAILABLE);

      // Advance past maxLookback
      injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000 + 1000);

      flush(dataTable);
      majorCompact(dataTable);

      // Fallback retains all — delete marker NOT purged despite maxLookback passing
      assertRawRowCount(conn, dataTable, ROWS_POPULATED);
    }
  }

  /**
   * Test 5: Guard retains delete markers on a table with explicit TTL. The consistency point is set
   * BEFORE the delete, time advances past both TTL and maxLookback, and the guard still retains the
   * delete marker because its timestamp is newer than the consistency point.
   */
  @Test(timeout = 120000L)
  public void testGuardRetainsDeleteMarkersWithExplicitTTL() throws Exception {
    int ttlSeconds = 30;
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      createTableWithTTL(dataTableName, ttlSeconds);
      TableName dataTable = TableName.valueOf(dataTableName);
      populateTable(dataTableName);

      injectEdge.incrementValue(1);
      long beforeDeleteTime = EnvironmentEdgeManager.currentTimeMillis();

      // Delete a row
      conn.createStatement().execute("DELETE FROM " + dataTableName + " WHERE id = 'a'");
      conn.commit();
      injectEdge.incrementValue(1);

      // Set consistency point BEFORE the delete — replay hasn't caught up
      long consistencyPoint = beforeDeleteTime - 1;
      injectMockConsistencyPoint(consistencyPoint);

      // Advance past both TTL and maxLookback — without guard, marker would be purged
      injectEdge.incrementValue(ttlSeconds * 1000 + 1000);

      flush(dataTable);
      majorCompact(dataTable);

      // Delete marker retained — guard caps purge at consistencyPoint
      assertRawRowCount(conn, dataTable, ROWS_POPULATED);
    }
  }

  private void injectMockConsistencyPoint(long consistencyPoint) {
    ReplicationLogReplayService.setConsistencyPointForTesting(getUtility().getConfiguration(),
      consistencyPoint);
  }

  private void flush(TableName table) throws IOException {
    getUtility().getAdmin().flush(table);
  }

  private void majorCompact(TableName table) throws Exception {
    TestUtil.majorCompact(getUtility(), table);
  }

  private void createTable(String tableName) throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE " + tableName + " (id VARCHAR(10) NOT NULL PRIMARY KEY, val1 VARCHAR(10),"
          + " val2 VARCHAR(10), val3 VARCHAR(10))");
      conn.commit();
    }
  }

  private void createTableWithTTL(String tableName, int ttlSeconds) throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE " + tableName + " (id VARCHAR(10) NOT NULL PRIMARY KEY, val1 VARCHAR(10),"
          + " val2 VARCHAR(10), val3 VARCHAR(10)) TTL=" + ttlSeconds);
      conn.commit();
    }
  }

  private void populateTable(String tableName) throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES ('b', 'bc', 'bcd', 'bcde')");
      conn.commit();
    }
  }
}
