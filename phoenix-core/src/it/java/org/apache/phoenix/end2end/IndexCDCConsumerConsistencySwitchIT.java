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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_POLL_INTERVAL_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_RETRY_PAUSE_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_STARTUP_DELAY_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Tests that the eventual index consumer does not lose changes when ALTER INDEX ... CONSISTENCY
 * switches an index. The consumer reads only CDC events that are older than a large time buffer.
 * Thus the events of the EVENTUAL period are still pending when the index changes to STRONG.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexCDCConsumerConsistencySwitchIT extends ParallelStatsDisabledIT {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(IndexCDCConsumerConsistencySwitchIT.class);

  private static final long TIMESTAMP_BUFFER_MS = 15000L;
  private static final long INDEX_CATCH_UP_TIMEOUT_MS = 120000L;
  private static final int ROWS = 20;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(10);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Long.toString(1000000L));
    props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(2));
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1));
    props.put(QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB, Boolean.TRUE.toString());
    props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
    props.put(INDEX_CDC_CONSUMER_STARTUP_DELAY_MS, Long.toString(1000L));
    props.put(INDEX_CDC_CONSUMER_POLL_INTERVAL_MS, Long.toString(500L));
    props.put(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, Long.toString(100L));
    props.put("phoenix.index.cdc.consumer.timestamp.buffer.ms", Long.toString(TIMESTAMP_BUFFER_MS));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  private static Connection connect() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private static long count(Connection conn, String sql) throws Exception {
    try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static void createTable(Connection conn, String table) throws Exception {
    conn.createStatement()
      .execute("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, name VARCHAR, city VARCHAR)");
  }

  private static void upsertRows(Connection conn, String table, int from, int to) throws Exception {
    for (int id = from; id <= to; id++) {
      conn.createStatement()
        .execute("UPSERT INTO " + table + " VALUES (" + id + ", 'n" + id + "', 'c" + id + "')");
    }
    conn.commit();
  }

  /**
   * Closes and opens the regions of the table. A consumer starts only when its region opens, so
   * this starts a consumer that sees the current indexes.
   */
  private static void reopenTable(Connection conn, String table) throws Exception {
    try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      admin.disableTable(TableName.valueOf(table));
      admin.enableTable(TableName.valueOf(table));
    }
  }

  /**
   * Waits until the CDC stream of the table is ENABLED. CREATE INDEX ... CONSISTENCY=EVENTUAL does
   * not wait for the stream, and a split before the stream is ENABLED does not record the parent.
   */
  private static void waitForStreamEnabled(Connection conn, String table) throws Exception {
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + INDEX_CATCH_UP_TIMEOUT_MS;
    while (!isStreamEnabled(conn, table)) {
      assertTrue("CDC stream of " + table + " is not enabled",
        EnvironmentEdgeManager.currentTimeMillis() < deadline);
      Thread.sleep(500);
    }
  }

  private static boolean isStreamEnabled(Connection conn, String table) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
      "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME
        + " WHERE TABLE_NAME = ? AND " + PhoenixDatabaseMetaData.STREAM_STATUS + " = ?")) {
      ps.setString(1, table);
      ps.setString(2, CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1) > 0;
      }
    }
  }

  private static void alterConsistency(Connection conn, String table, String index,
    String consistency) throws Exception {
    conn.createStatement()
      .execute("ALTER INDEX " + index + " ON " + table + " CONSISTENCY=" + consistency);
  }

  /** Asserts that the consumer has not yet written index rows for the EVENTUAL writes. */
  private static void assertIndexBehind(Connection conn, String table, String index)
    throws Exception {
    long dataRows = count(conn, "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + table);
    long indexRows = count(conn, "SELECT COUNT(*) FROM " + index);
    assertTrue(
      "The index must lag the data before the switch: data=" + dataRows + " index=" + indexRows,
      indexRows < dataRows);
  }

  private static void waitForIndexRows(Connection conn, String table, String index)
    throws Exception {
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + INDEX_CATCH_UP_TIMEOUT_MS;
    long dataRows;
    long indexRows;
    while (true) {
      dataRows = count(conn, "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + table);
      indexRows = count(conn, "SELECT COUNT(*) FROM " + index);
      if (dataRows == indexRows || EnvironmentEdgeManager.currentTimeMillis() > deadline) {
        break;
      }
      Thread.sleep(1000);
    }
    LOGGER.info("{}: dataRows={} indexRows={}", table, dataRows, indexRows);
    assertEquals("index rows of " + index, dataRows, indexRows);
  }

  /**
   * The index changes to STRONG before the consumer applies the EVENTUAL writes. The running
   * consumer must still apply them. The region opens again after CREATE INDEX, so that a consumer
   * runs during the EVENTUAL period.
   */
  @Test
  public void testEventualToStrongAppliesPendingEvents() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = connect()) {
      createTable(conn, table);
      conn.createStatement().execute(
        "CREATE INDEX " + index + " ON " + table + " (name) INCLUDE (city) CONSISTENCY=EVENTUAL");
      reopenTable(conn, table);
      upsertRows(conn, table, 1, ROWS);
      assertIndexBehind(conn, table, index);
      alterConsistency(conn, table, index, "STRONG");
      upsertRows(conn, table, ROWS + 1, 2 * ROWS);
      waitForIndexRows(conn, table, index);
    }
  }

  /**
   * The index changes to STRONG, and then the region splits before the consumer applies the
   * EVENTUAL writes. The consumers of the daughter regions must start and replay the parent.
   */
  @Test
  public void testSplitAfterEventualToStrongReplaysParent() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = connect()) {
      createTable(conn, table);
      conn.createStatement().execute(
        "CREATE INDEX " + index + " ON " + table + " (name) INCLUDE (city) CONSISTENCY=EVENTUAL");
      waitForStreamEnabled(conn, table);
      upsertRows(conn, table, 1, ROWS);
      assertIndexBehind(conn, table, index);
      alterConsistency(conn, table, index, "STRONG");
      TestUtil.splitTable(conn, table, PInteger.INSTANCE.toBytes(ROWS / 2));
      upsertRows(conn, table, ROWS + 1, 2 * ROWS);
      waitForIndexRows(conn, table, index);
    }
  }

  /**
   * The index changes to EVENTUAL, and the region splits at once. The ALTER returns only after the
   * CDC stream is ENABLED, so the split records the parent. The consumers of the daughter regions
   * must replay the EVENTUAL writes of the parent.
   */
  @Test
  public void testSplitAfterStrongToEventualReplaysParent() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = connect()) {
      createTable(conn, table);
      conn.createStatement()
        .execute("CREATE INDEX " + index + " ON " + table + " (name) INCLUDE (city)");
      alterConsistency(conn, table, index, "EVENTUAL");
      assertTrue("The ALTER must return after the CDC stream is enabled",
        isStreamEnabled(conn, table));
      upsertRows(conn, table, 1, ROWS);
      assertIndexBehind(conn, table, index);
      TestUtil.splitTable(conn, table, PInteger.INSTANCE.toBytes(ROWS / 2));
      upsertRows(conn, table, ROWS + 1, 2 * ROWS);
      waitForIndexRows(conn, table, index);
    }
  }

  /**
   * The region server caches the table while the index is STRONG and the table has no CDC index.
   * After the switch to EVENTUAL, the region opens again. The new consumer must read the table from
   * SYSTEM.CATALOG, find the CDC index, and apply the EVENTUAL writes.
   */
  @Test
  public void testConsumerReadsFreshMetadataAfterRegionReopen() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = connect()) {
      createTable(conn, table);
      conn.createStatement()
        .execute("CREATE INDEX " + index + " ON " + table + " (name) INCLUDE (city)");
      upsertRows(conn, table, 1, ROWS);
      // Let the consumer of the region start, cache the table, and exit.
      Thread.sleep(5000);
      alterConsistency(conn, table, index, "EVENTUAL");
      reopenTable(conn, table);
      upsertRows(conn, table, ROWS + 1, 2 * ROWS);
      waitForIndexRows(conn, table, index);
    }
  }
}
