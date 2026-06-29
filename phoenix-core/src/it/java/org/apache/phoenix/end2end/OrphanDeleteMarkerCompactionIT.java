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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.CompactionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test to verify the lifecycle of orphaned delete markers (DeleteFamily markers
 * without corresponding Put cells) during compaction.
 *
 * Orphan delete markers follow the same lifecycle as normal deleted rows:
 * - Within max-lookback: retained
 * - Outside max-lookback: purged
 * - Outside TTL: purged
 *
 * The root cause of the bug is HBase's DropDeletesCompactionScanQueryMatcher.tryDropDelete()
 * which drops delete markers whose timestamp < earliestPutTs when KeepDeletedCells=TTL and
 * timeToPurgeDeletes is 0. The fix sets timeToPurgeDeletes to Long.MAX_VALUE so HBase
 * never purges delete markers before Phoenix CompactionScanner processes them.
 *
 * Users who need orphan delete markers retained beyond max-lookback (e.g., for replication
 * scenarios) can use the per-table max-lookback override to extend it up to TTL.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class OrphanDeleteMarkerCompactionIT extends BaseTest {
  private static final int MAX_LOOKBACK_AGE = 15;
  private ManualEnvironmentEdge injectEdge;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(4);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    props.put(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, "0");
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

  /**
   * Verifies that an orphaned delete marker within the max-lookback window survives major
   * compaction. Without the timeToPurgeDeletes fix, HBase's tryDropDelete() would drop it
   * because earliestPutTs (from a different row's HFile) > marker timestamp.
   */
  @Test(timeout = 120000L)
  public void testOrphanedDeleteMarkerRetainedWithinMaxLookback() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)"
        + " TTL=300");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.incrementValue(1);

      TableName hbaseTableName = TableName.valueOf(tableName);
      ConnectionQueryServices cqs =
        conn.unwrap(PhoenixConnection.class).getQueryServices();
      Table hTable = cqs.getTable(hbaseTableName.getName());

      // Write an orphaned DeleteFamily marker using raw HBase API.
      long oldDeleteTs = EnvironmentEdgeManager.currentTimeMillis();
      byte[] rowA = Bytes.toBytes("a");
      Delete delete = new Delete(rowA, oldDeleteTs);
      hTable.delete(delete);

      // Flush to persist the delete marker into its own HFile
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Advance time but stay WITHIN max-lookback window (advance 10s < 15s max-lookback)
      injectEdge.incrementValue(10 * 1000L);

      // Write a put for a DIFFERENT row — creates an HFile with earliestPutTs > marker ts,
      // which triggers the tryDropDelete bug without the timeToPurgeDeletes fix.
      stmt.execute("UPSERT INTO " + tableName + " VALUES ('b', 'v1', 'v2')");
      conn.commit();

      // Flush to create a second HFile
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      int deleteMarkersBefore = countDeleteMarkers(conn, hbaseTableName);
      assertTrue("Expected at least one delete marker before compaction, found "
        + deleteMarkersBefore, deleteMarkersBefore > 0);

      // Major compact
      TestUtil.majorCompact(getUtility(), hbaseTableName);
      injectEdge.incrementValue(1);

      // Assert: marker within max-lookback is retained
      int deleteMarkersAfter = countDeleteMarkers(conn, hbaseTableName);
      assertTrue("Orphaned delete marker within max-lookback should be retained after "
          + "major compaction. Before=" + deleteMarkersBefore + ", After=" + deleteMarkersAfter,
        deleteMarkersAfter > 0);

      // Verify correct query behavior
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 'a'");
      Assert.assertFalse("Deleted row should not be visible", rs.next());
      rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 'b'");
      Assert.assertTrue("Row 'b' should still be visible", rs.next());
      assertEquals("v1", rs.getString("val1"));

      hTable.close();
    }
  }

  /**
   * Verifies that an orphaned delete marker is purged once it ages past the max-lookback
   * window but is still within TTL — same lifecycle as a normal Phoenix deleted row.
   */
  @Test(timeout = 120000L)
  public void testOrphanedDeleteMarkerPurgedBetweenMaxLookbackAndTTL() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)"
        + " TTL=300");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.incrementValue(1);

      TableName hbaseTableName = TableName.valueOf(tableName);
      ConnectionQueryServices cqs =
        conn.unwrap(PhoenixConnection.class).getQueryServices();
      Table hTable = cqs.getTable(hbaseTableName.getName());

      // Write orphaned delete marker
      long oldDeleteTs = EnvironmentEdgeManager.currentTimeMillis();
      byte[] rowA = Bytes.toBytes("a");
      Delete delete = new Delete(rowA, oldDeleteTs);
      hTable.delete(delete);
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Write a put for a different row
      stmt.execute("UPSERT INTO " + tableName + " VALUES ('b', 'v1', 'v2')");
      conn.commit();
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Advance past max-lookback (20s > 15s) but still within TTL (300s)
      injectEdge.incrementValue((MAX_LOOKBACK_AGE + 5) * 1000L);

      // Major compact
      TestUtil.majorCompact(getUtility(), hbaseTableName);
      injectEdge.incrementValue(1);

      // Assert: marker outside max-lookback is purged (same as normal deleted row)
      int deleteMarkersAfter = countDeleteMarkers(conn, hbaseTableName);
      assertEquals("Orphaned delete marker outside max-lookback should be purged "
        + "(same as normal deleted row)", 0, deleteMarkersAfter);

      hTable.close();
    }
  }

  /**
   * Verifies that an orphaned delete marker is purged when it is outside TTL.
   */
  @Test(timeout = 120000L)
  public void testOrphanedDeleteMarkerPurgedOutsideTTL() throws Exception {
    int ttl = 20;
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)"
        + " TTL=" + ttl);
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.incrementValue(1);

      TableName hbaseTableName = TableName.valueOf(tableName);
      ConnectionQueryServices cqs =
        conn.unwrap(PhoenixConnection.class).getQueryServices();
      Table hTable = cqs.getTable(hbaseTableName.getName());

      // Write orphaned delete marker
      long oldDeleteTs = EnvironmentEdgeManager.currentTimeMillis();
      byte[] rowA = Bytes.toBytes("a");
      Delete delete = new Delete(rowA, oldDeleteTs);
      hTable.delete(delete);
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Write a newer put for a different row
      stmt.execute("UPSERT INTO " + tableName + " VALUES ('b', 'v1', 'v2')");
      conn.commit();
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Advance time past TTL + max-lookback
      injectEdge.incrementValue((ttl + MAX_LOOKBACK_AGE) * 1000L + 1000L);

      // Major compact
      TestUtil.majorCompact(getUtility(), hbaseTableName);
      injectEdge.incrementValue(1);

      // Assert: marker outside TTL is purged
      int deleteMarkersAfter = countDeleteMarkers(conn, hbaseTableName);
      assertEquals("Orphaned delete marker should be purged after TTL expires",
        0, deleteMarkersAfter);

      hTable.close();
    }
  }

  /**
   * Verifies that overriding max-lookback to match TTL retains the orphaned delete marker
   * in the max-lookback-to-TTL window. This is the escape hatch for replication scenarios
   * where users need markers retained longer than the global max-lookback.
   */
  @Test(timeout = 120000L)
  public void testMaxLookbackOverrideRetainsOrphanedDeleteMarkerUntilTTL() throws Exception {
    int ttl = 60;
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)"
        + " TTL=" + ttl);
      conn.commit();

      // Override max-lookback for this table to match TTL (60s instead of global 15s)
      CompactionScanner.overrideMaxLookback(tableName, "0", ttl * 1000L);

      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.incrementValue(1);

      TableName hbaseTableName = TableName.valueOf(tableName);
      ConnectionQueryServices cqs =
        conn.unwrap(PhoenixConnection.class).getQueryServices();
      Table hTable = cqs.getTable(hbaseTableName.getName());

      // Write orphaned delete marker
      long oldDeleteTs = EnvironmentEdgeManager.currentTimeMillis();
      byte[] rowA = Bytes.toBytes("a");
      Delete delete = new Delete(rowA, oldDeleteTs);
      hTable.delete(delete);
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Write a put for a different row
      stmt.execute("UPSERT INTO " + tableName + " VALUES ('b', 'v1', 'v2')");
      conn.commit();
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Advance 30s — past the global max-lookback (15s) but within the
      // per-table override (60s) and within TTL (60s)
      injectEdge.incrementValue(30 * 1000L);

      // Major compact
      TestUtil.majorCompact(getUtility(), hbaseTableName);
      injectEdge.incrementValue(1);

      // Assert: marker is retained because the per-table max-lookback is 60s
      int deleteMarkersAfter = countDeleteMarkers(conn, hbaseTableName);
      assertTrue("Orphaned delete marker should be retained when within per-table "
          + "max-lookback override. After=" + deleteMarkersAfter,
        deleteMarkersAfter > 0);

      // Now advance past the override (total ~31s more, so ~61s from marker creation)
      injectEdge.incrementValue(31 * 1000L);

      // Major compact again
      TestUtil.majorCompact(getUtility(), hbaseTableName);
      injectEdge.incrementValue(1);

      // Assert: marker is now purged — exceeded max-lookback override
      deleteMarkersAfter = countDeleteMarkers(conn, hbaseTableName);
      assertEquals("Orphaned delete marker should be purged after exceeding "
        + "max-lookback override", 0, deleteMarkersAfter);

      hTable.close();
    }
  }

  /**
   * Verifies minor compaction retains orphaned delete markers regardless of age
   * (minor compactions never expire cells).
   */
  @Test(timeout = 120000L)
  public void testOrphanedDeleteMarkerRetainedDuringMinorCompaction() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)"
        + " TTL=300");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.incrementValue(1);

      TableName hbaseTableName = TableName.valueOf(tableName);
      ConnectionQueryServices cqs =
        conn.unwrap(PhoenixConnection.class).getQueryServices();
      Table hTable = cqs.getTable(hbaseTableName.getName());

      // Write orphaned delete marker
      long oldDeleteTs = EnvironmentEdgeManager.currentTimeMillis();
      byte[] rowA = Bytes.toBytes("a");
      Delete delete = new Delete(rowA, oldDeleteTs);
      hTable.delete(delete);
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      // Write puts for different rows to create multiple HFiles
      injectEdge.incrementValue(5000);
      stmt.execute("UPSERT INTO " + tableName + " VALUES ('b', 'v1', 'v2')");
      conn.commit();
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      stmt.execute("UPSERT INTO " + tableName + " VALUES ('c', 'v3', 'v4')");
      conn.commit();
      flush(hbaseTableName);
      injectEdge.incrementValue(1);

      int deleteMarkersBefore = countDeleteMarkers(conn, hbaseTableName);
      assertTrue("Expected at least one delete marker before minor compaction",
        deleteMarkersBefore > 0);

      // Run minor compaction
      TestUtil.minorCompact(getUtility(), hbaseTableName);
      injectEdge.incrementValue(1);

      // Assert: minor compaction never expires cells
      int deleteMarkersAfter = countDeleteMarkers(conn, hbaseTableName);
      assertTrue("Orphaned delete marker should be retained after minor compaction. "
          + "Before=" + deleteMarkersBefore + ", After=" + deleteMarkersAfter,
        deleteMarkersAfter > 0);

      // Verify correct query behavior
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 'a'");
      Assert.assertFalse("Deleted row should not be visible", rs.next());
      rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 'b'");
      Assert.assertTrue("Row 'b' should still be visible", rs.next());

      hTable.close();
    }
  }

  private void flush(TableName table) throws IOException {
    Admin admin = getUtility().getAdmin();
    admin.flush(table);
  }

  private int countDeleteMarkers(Connection conn, TableName tableName)
    throws Exception {
    ConnectionQueryServices cqs =
      conn.unwrap(PhoenixConnection.class).getQueryServices();
    Table table = cqs.getTable(tableName.getName());
    Scan scan = new Scan();
    scan.setRaw(true);
    scan.readAllVersions();
    int deleteMarkerCount = 0;
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result;
      while ((result = scanner.next()) != null) {
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
          Cell cell = cellScanner.current();
          if (cell.getType() == Cell.Type.DeleteFamily
            || cell.getType() == Cell.Type.DeleteColumn
            || cell.getType() == Cell.Type.Delete) {
            deleteMarkerCount++;
          }
        }
      }
    }
    return deleteMarkerCount;
  }
}
