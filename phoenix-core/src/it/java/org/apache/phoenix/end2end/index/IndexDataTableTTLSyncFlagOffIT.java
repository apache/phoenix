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

import static org.apache.phoenix.end2end.index.IndexDataTableTTLSyncIT.COVCOL_VALUE;
import static org.apache.phoenix.end2end.index.IndexDataTableTTLSyncIT.IDXCOL_VALUE;
import static org.apache.phoenix.end2end.index.IndexDataTableTTLSyncIT.maxTimestampForValue;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Confirms the {@code phoenix.index.ttl.column.resync.enabled} kill switch. The resync flag is read
 * once server-side at coprocessor {@code start()}, so it can only be exercised with its own mini
 * cluster — hence a separate class from {@link IndexDataTableTTLSyncIT}.
 * <p>
 * With the flag off, the column re-sync is skipped: a covcol-omitting touch does NOT re-inject
 * covcol into the data Put, so the data-side covcol keeps its original write timestamp. This is the
 * mirror of the on-by-default assertion in {@link IndexDataTableTTLSyncIT} and demonstrates that the
 * timestamp skew (which a later major compaction turns into data/index divergence) returns when an
 * operator opts out.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexDataTableTTLSyncFlagOffIT extends BaseTest {
  private ManualEnvironmentEdge injectEdge;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = IndexDataTableTTLSyncIT.baseServerProps();
    props.put(IndexRegionObserver.PHOENIX_INDEX_TTL_COLUMN_RESYNC_ENABLED, Boolean.toString(false));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void beforeTest() {
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
  }

  @After
  public synchronized void afterTest() throws Exception {
    boolean refCountLeaked = isAnyStoreRefCountLeaked();
    EnvironmentEdgeManager.reset();
    Assert.assertFalse("refCount leaked", refCountLeaked);
  }

  @Test
  public void testResyncDisabledLeavesCovColAtOriginalTimestamp() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR) TTL="
        + IndexDataTableTTLSyncIT.TTL + ", COLUMN_ENCODED_BYTES=0");
      conn.createStatement().execute(
        "CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();
      long originalCovTs =
        maxTimestampForValue(conn, TableName.valueOf(tableName), Bytes.toBytes("r1"), COVCOL_VALUE);

      injectEdge.incrementValue(1000);
      long touchTime = injectEdge.currentTime();
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();

      // Flag off: no re-injection, so the data covcol keeps its original timestamp (< touchTime).
      long dataCovTs =
        maxTimestampForValue(conn, TableName.valueOf(tableName), Bytes.toBytes("r1"), COVCOL_VALUE);
      assertTrue("with resync disabled, covcol must NOT be re-stamped; originalCovTs="
        + originalCovTs + " touchTime=" + touchTime + " dataCovTs=" + dataCovTs,
        dataCovTs == originalCovTs && dataCovTs < touchTime);
    }
  }

  /**
   * The uncovered-index counterpart to the covered case above: with the flag off, an
   * {@code idxcol}-omitting touch does NOT re-inject the uncovered index's indexed column, so the
   * data-side {@code idxcol} keeps its original write timestamp. This is the mirror of
   * {@link IndexDataTableTTLSyncIT#testUncoveredIndexIndexedColumnResync} and confirms the kill
   * switch governs the uncovered path too.
   */
  @Test
  public void testResyncDisabledLeavesUncoveredIdxColAtOriginalTimestamp() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, idxcol VARCHAR, touchcol VARCHAR) TTL="
        + IndexDataTableTTLSyncIT.TTL + ", COLUMN_ENCODED_BYTES=0");
      conn.createStatement()
        .execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (idxcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, touchcol) VALUES ('r1', '" + IDXCOL_VALUE + "', 'x0')");
      conn.commit();
      long originalIdxTs =
        maxTimestampForValue(conn, TableName.valueOf(tableName), Bytes.toBytes("r1"), IDXCOL_VALUE);

      injectEdge.incrementValue(1000);
      long touchTime = injectEdge.currentTime();
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();

      // Flag off: no re-injection, so the data idxcol keeps its original timestamp (< touchTime).
      long dataIdxTs =
        maxTimestampForValue(conn, TableName.valueOf(tableName), Bytes.toBytes("r1"), IDXCOL_VALUE);
      assertTrue("with resync disabled, uncovered idxcol must NOT be re-stamped; originalIdxTs="
        + originalIdxTs + " touchTime=" + touchTime + " dataIdxTs=" + dataIdxTs,
        dataIdxTs == originalIdxTs && dataIdxTs < touchTime);
    }
  }
}
