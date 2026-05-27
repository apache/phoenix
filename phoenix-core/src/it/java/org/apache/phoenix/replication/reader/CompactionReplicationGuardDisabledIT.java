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
import static org.apache.phoenix.util.TestUtil.assertRawRowCount;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.replication.reader.ReplicationLogReplayService;
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
 * Integration test verifying that the replication compaction guard does NOT interfere with normal
 * compaction when explicitly disabled via configuration.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class CompactionReplicationGuardDisabledIT extends BaseTest {

  private static final int MAX_LOOKBACK_AGE = 15;
  private static final int ROWS_POPULATED = 2;
  private ManualEnvironmentEdge injectEdge;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(4);
    props.put(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
    props.put(QueryServices.PHOENIX_COMPACTION_ENABLED, Boolean.toString(true));
    props.put(ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED,
      Boolean.toString(true));
    props.put(ReplicationLogReplayService.REPLICATION_COMPACTION_GUARD_ENABLED,
      Boolean.toString(false));
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
   * When guard is disabled, delete markers are purged normally by maxLookback even though the
   * consistency point would have protected them if the guard were enabled.
   */
  @Test(timeout = 120000L)
  public void testGuardDisabledDeleteMarkersPurgedByMaxLookback() throws Exception {
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

      // Set consistency point BEFORE delete — guard would retain if enabled
      long consistencyPoint = beforeDeleteTime - 1;
      ReplicationLogReplayService mockService = mock(ReplicationLogReplayService.class);
      when(mockService.getConsistencyPoint()).thenReturn(consistencyPoint);
      ReplicationLogReplayService.setInstanceForTesting(mockService);

      // Advance past maxLookback
      injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000 + 1000);

      flush(dataTable);
      majorCompact(dataTable);

      // Guard disabled — delete marker purged by maxLookback as normal
      assertRawRowCount(conn, dataTable, ROWS_POPULATED - 1);
    }
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
