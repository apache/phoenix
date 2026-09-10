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

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Verifies that a major compaction of a SYSTEM table whose TTL is a CONDITIONAL expression
 * (SYSTEM.CDC_STREAM) physically purges expired rows, rather than only masking them at read time.
 * <p>
 * SYSTEM.CDC_STREAM is created with a conditional TTL that expires CLOSED partition rows
 * (PARTITION_END_TIME IS NOT NULL) once they age past the configured partition-expiry window. If
 * the compaction path routes SYSTEM tables unconditionally onto the column-family-descriptor TTL
 * (which is FOREVER for this table), the conditional expression is never compiled, so
 * postProcessForConditionalTTL never runs and the expired closed-partition rows survive on disk
 * forever. This IT drives a major compaction after advancing the clock past the expiry window and
 * asserts the closed-partition rows are physically gone.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class CDCStreamCompactionTTLIT extends CDCBaseIT {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(5);
    // Conditional-TTL rows are only PHYSICALLY purged at major compaction once they fall outside
    // the max-lookback retain window. Disable max-lookback (0) so the expired closed-partition
    // rows are eligible for physical removal at compaction, not merely masked at read time.
    props.put(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(0));
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    taskRegionEnvironment =
      getUtility().getRSForFirstRegionInTable(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
        .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME).get(0)
        .getCoprocessorHost().findCoprocessorEnvironment(TaskRegionObserver.class.getName());
  }

  @Test
  public void testMajorCompactionPhysicallyPurgesExpiredClosedPartitionRows() throws Exception {
    Connection conn = newConnection();
    String tableName = generateUniqueName();
    createTableAndEnableCDC(conn, tableName, true);
    // Splitting yields 3 SYSTEM.CDC_STREAM rows for this table: 1 CLOSED parent partition
    // (PARTITION_END_TIME not null) + 2 OPEN child partitions (PARTITION_END_TIME null).
    TestUtil.splitTable(conn, tableName, Bytes.toBytes("m"));

    String totalSql = "SELECT COUNT(*) FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'";
    // The conditional TTL expires exactly the rows with PARTITION_END_TIME IS NOT NULL.
    String closedSql = "SELECT COUNT(*) FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName
      + "' AND PARTITION_END_TIME IS NOT NULL";

    long total;
    long closed;
    try (ResultSet rs = conn.createStatement().executeQuery(totalSql)) {
      assertTrue(rs.next());
      total = rs.getLong(1);
    }
    try (ResultSet rs = conn.createStatement().executeQuery(closedSql)) {
      assertTrue(rs.next());
      closed = rs.getLong(1);
    }
    // A single split of a one-region table yields exactly 3 SYSTEM.CDC_STREAM rows:
    // 1 closed parent partition (PARTITION_END_TIME not null) + 2 open child partitions.
    assertEquals("split should yield exactly 3 partition rows", 3, total);
    assertEquals("exactly one closed (split-parent) partition row", 1, closed);

    // Resolve the physical HBase table backing SYSTEM.CDC_STREAM.
    TableName physicalCdcStreamTable = SchemaUtil.getPhysicalTableName(SYSTEM_CDC_STREAM_NAME,
      conn.unwrap(PhoenixConnection.class).getQueryServices().getProps());
    long rawBefore = TestUtil.getRawRowCount(conn, physicalCdcStreamTable);
    // Before advancing the clock, every partition row is still present on disk.
    assertEquals("baseline: all 3 partition rows present on disk before compaction", 3, rawBefore);

    try {
      // Advance the clock past the partition-expiry window so the closed-partition rows expire.
      ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
      long t = System.currentTimeMillis()
        + QueryServicesOptions.DEFAULT_PHOENIX_CDC_STREAM_PARTITION_EXPIRY_MIN_AGE_MS + 5000;
      t = (t / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(t);

      // Major compact SYSTEM.CDC_STREAM. With the conditional TTL compiled, the expired
      // closed-partition rows must be physically removed here.
      TestUtil.doMajorCompaction(conn, SYSTEM_CDC_STREAM_NAME);

      long rawAfter = TestUtil.getRawRowCount(conn, physicalCdcStreamTable);
      assertEquals("closed partition physically purged; only the 2 open partitions remain", 2,
        rawAfter);

      // The SQL (read-path) view must now agree with the physical purge: the closed parent
      // partition row is gone and only the 2 open child partitions remain visible.
      long totalAfter;
      long closedAfter;
      try (ResultSet rs = conn.createStatement().executeQuery(totalSql)) {
        assertTrue(rs.next());
        totalAfter = rs.getLong(1);
      }
      try (ResultSet rs = conn.createStatement().executeQuery(closedSql)) {
        assertTrue(rs.next());
        closedAfter = rs.getLong(1);
      }
      assertEquals("only the 2 open partitions remain visible via SQL after compaction", 2,
        totalAfter);
      assertEquals("no closed partition rows remain visible via SQL after compaction", 0,
        closedAfter);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }
}
