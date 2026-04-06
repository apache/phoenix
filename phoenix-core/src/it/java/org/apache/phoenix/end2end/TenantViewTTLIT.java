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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/**
 * Integration test verifying per-tenant TTL via global views on a multi-tenant table.
 *
 * The pattern under test:
 *   1. Create a MULTI_TENANT=true base table (no TTL on the table itself).
 *   2. Create global views with WHERE clause on the tenant-id PK column, each with a
 *      different TTL value.
 *   3. Verify read-time masking: expired rows are not visible when querying through the view.
 *   4. Verify compaction: expired rows are physically removed by major compaction.
 *   5. Verify tenant isolation: a short-TTL tenant's data expires independently of a
 *      long-TTL tenant's data.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class TenantViewTTLIT extends BaseTest {

  private ManualEnvironmentEdge injectEdge;

  @BeforeClass
  public static void doSetup() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(true));
    props.put(QueryServices.PHOENIX_COMPACTION_ENABLED, String.valueOf(true));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(0));
    props.put(QueryServices.PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT, String.valueOf(10));
    props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void beforeTest() {
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
  }

  @After
  public synchronized void afterTest() {
    EnvironmentEdgeManager.reset();
  }

  /**
   * Verifies that different tenants can have different TTLs via global views with WHERE
   * clauses on the tenant-id column, and that read masking works correctly for each.
   *
   * Tenant org1 gets TTL = 10 seconds, org2 gets TTL = 100 seconds.
   * After 20 seconds: org1's data should be masked, org2's data should still be visible.
   */
  @Test
  public void testReadMaskingWithDifferentTenantTTLs() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String view2Name = SchemaUtil.getTableName(schemaName, generateUniqueName());

    int ttlOrg1 = 10;
    int ttlOrg2 = 100;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      conn.createStatement().execute(String.format(
        "CREATE TABLE %s ("
          + "ORGID VARCHAR NOT NULL, "
          + "ID1 VARCHAR NOT NULL, "
          + "ID2 VARCHAR NOT NULL, "
          + "COL1 VARCHAR, "
          + "COL2 VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1, ID2)"
          + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
        fullTableName));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'org1' TTL = %d",
        view1Name, fullTableName, ttlOrg1));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'org2' TTL = %d",
        view2Name, fullTableName, ttlOrg2));

      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      upsertRow(conn, view1Name, "k1", "v1", "row1col1", "row1col2");
      upsertRow(conn, view1Name, "k2", "v2", "row2col1", "row2col2");
      upsertRow(conn, view2Name, "k1", "v1", "row3col1", "row3col2");
      upsertRow(conn, view2Name, "k2", "v2", "row4col1", "row4col2");

      assertViewRowCount(conn, view1Name, 2, "org1 should have 2 rows before TTL expiry");
      assertViewRowCount(conn, view2Name, 2, "org2 should have 2 rows before TTL expiry");

      injectEdge.incrementValue((ttlOrg1 * 2) * 1000L);

      assertViewRowCount(conn, view1Name, 0,
        "org1 rows should be masked after org1 TTL expiry");
      assertViewRowCount(conn, view2Name, 2,
        "org2 rows should still be visible (TTL not expired yet)");

      injectEdge.incrementValue((ttlOrg2 * 2) * 1000L);

      assertViewRowCount(conn, view2Name, 0,
        "org2 rows should be masked after org2 TTL expiry");
    }
  }

  /**
   * Verifies that major compaction physically removes expired rows per the view's TTL,
   * while preserving rows belonging to views whose TTL has not yet expired.
   */
  @Test
  public void testCompactionWithDifferentTenantTTLs() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String view2Name = SchemaUtil.getTableName(schemaName, generateUniqueName());

    int ttlOrg1 = 10;
    int ttlOrg2 = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      conn.createStatement().execute(String.format(
        "CREATE TABLE %s ("
          + "ORGID VARCHAR NOT NULL, "
          + "ID1 VARCHAR NOT NULL, "
          + "ID2 VARCHAR NOT NULL, "
          + "COL1 VARCHAR, "
          + "COL2 VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1, ID2)"
          + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
        fullTableName));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'org1' TTL = %d",
        view1Name, fullTableName, ttlOrg1));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'org2' TTL = %d",
        view2Name, fullTableName, ttlOrg2));

      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      upsertRow(conn, view1Name, "k1", "v1", "c1", "c2");
      upsertRow(conn, view1Name, "k2", "v2", "c3", "c4");
      upsertRow(conn, view2Name, "k1", "v1", "c5", "c6");
      upsertRow(conn, view2Name, "k2", "v2", "c7", "c8");
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 4,
      "All 4 rows should exist in HBase before compaction");

    injectEdge.setValue(afterInsertTime + (ttlOrg1 * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 2,
      "Only org2's 2 rows should remain after compacting past org1 TTL");

    injectEdge.setValue(afterInsertTime + (ttlOrg2 * 2 * 1000L));
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 0,
      "All rows should be removed after compacting past org2 TTL");
  }

  /**
   * Verifies that data not covered by any view (an "unassigned" tenant) is NOT expired
   * by compaction, since no view TTL applies to it.
   */
  @Test
  public void testUnassignedTenantDataNotExpiredByCompaction() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());

    int ttl = 10;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      conn.createStatement().execute(String.format(
        "CREATE TABLE %s ("
          + "ORGID VARCHAR NOT NULL, "
          + "ID1 VARCHAR NOT NULL, "
          + "COL1 VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
          + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
        fullTableName));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'org1' TTL = %d",
        viewName, fullTableName, ttl));

      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      upsertRowDirect(conn, fullTableName, "org1", "k1", "val1");
      upsertRowDirect(conn, fullTableName, "org2", "k1", "val2");
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();

    injectEdge.setValue(afterInsertTime + (ttl * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 1,
      "org2's row (no view, no TTL) should survive compaction; org1's row should be removed");
  }

  /**
   * Verifies read masking and compaction for three tenants with different TTLs to
   * confirm the mechanism scales beyond two tenants.
   */
  @Test
  public void testThreeTenantsDifferentTTLs() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String viewA = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String viewB = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String viewC = SchemaUtil.getTableName(schemaName, generateUniqueName());

    int ttlA = 10;
    int ttlB = 50;
    int ttlC = 200;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      conn.createStatement().execute(String.format(
        "CREATE TABLE %s ("
          + "ORGID VARCHAR NOT NULL, "
          + "ID1 VARCHAR NOT NULL, "
          + "COL1 VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
          + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
        fullTableName));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'tenA' TTL = %d",
        viewA, fullTableName, ttlA));
      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'tenB' TTL = %d",
        viewB, fullTableName, ttlB));
      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'tenC' TTL = %d",
        viewC, fullTableName, ttlC));

      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      upsertRowSimple(conn, viewA, "r1", "a1");
      upsertRowSimple(conn, viewB, "r1", "b1");
      upsertRowSimple(conn, viewC, "r1", "c1");

      assertViewRowCount(conn, viewA, 1, "tenA visible before TTL");
      assertViewRowCount(conn, viewB, 1, "tenB visible before TTL");
      assertViewRowCount(conn, viewC, 1, "tenC visible before TTL");

      injectEdge.incrementValue(ttlA * 2 * 1000L);
      assertViewRowCount(conn, viewA, 0, "tenA masked after its TTL");
      assertViewRowCount(conn, viewB, 1, "tenB still visible");
      assertViewRowCount(conn, viewC, 1, "tenC still visible");

      injectEdge.incrementValue(ttlB * 2 * 1000L);
      assertViewRowCount(conn, viewB, 0, "tenB masked after its TTL");
      assertViewRowCount(conn, viewC, 1, "tenC still visible");
    }

    long afterInsertTime = injectEdge.currentTime();

    injectEdge.setValue(afterInsertTime + (ttlC * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, 0, 0,
      "All rows should be compacted away after all TTLs expire");
  }

  /**
   * Verifies that compaction works correctly with a pre-split table where each region
   * has non-empty start/end keys. Four tenants span two regions; compaction should
   * correctly expire data per view regardless of which region the data resides in.
   */
  @Test
  public void testCompactionWithRegionSplitPrunesGlobalViews() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String viewA = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String viewB = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String viewC = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String viewD = SchemaUtil.getTableName(schemaName, generateUniqueName());

    int shortTTL = 10;
    int longTTL = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      // Pre-split between orgB and orgC so we get 2 regions:
      //   Region 1: [empty, 'orgC') -> orgA, orgB
      //   Region 2: ['orgC', empty) -> orgC, orgD
      conn.createStatement().execute(String.format(
        "CREATE TABLE %s ("
          + "ORGID VARCHAR NOT NULL, "
          + "ID1 VARCHAR NOT NULL, "
          + "COL1 VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
          + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'"
          + " SPLIT ON ('orgC')",
        fullTableName));

      TableName tn = TableName.valueOf(SchemaUtil.getTableName(schemaName, baseTableName));
      try (org.apache.hadoop.hbase.client.Connection hConn =
          ConnectionFactory.createConnection(getUtility().getConfiguration())) {
        List<RegionInfo> regions = hConn.getAdmin().getRegions(tn);
        assertEquals("Expected 2 regions after SPLIT ON", 2, regions.size());
      }

      // orgA and orgB get short TTL, orgC gets short TTL, orgD gets long TTL
      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'orgA' TTL = %d",
        viewA, fullTableName, shortTTL));
      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'orgB' TTL = %d",
        viewB, fullTableName, shortTTL));
      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'orgC' TTL = %d",
        viewC, fullTableName, shortTTL));
      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'orgD' TTL = %d",
        viewD, fullTableName, longTTL));

      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      upsertRowSimple(conn, viewA, "r1", "a1");
      upsertRowSimple(conn, viewB, "r1", "b1");
      upsertRowSimple(conn, viewC, "r1", "c1");
      upsertRowSimple(conn, viewD, "r1", "d1");
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 4,
      "All 4 rows should exist before compaction");

    injectEdge.setValue(afterInsertTime + (shortTTL * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    // orgA, orgB, orgC expired (shortTTL). orgD survives (longTTL).
    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 1,
      "Only orgD's row should survive; orgA/B/C expired across both regions");

    // Now advance past longTTL and compact again
    injectEdge.setValue(afterInsertTime + (longTTL * 2 * 1000L));
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 0,
      "All rows should be removed after all TTLs expire");
  }

  /**
   * Verifies that compaction correctly applies per-tenant TTL on a SALTED multi-tenant
   * table. Salting changes the row key layout to [salt_byte][tenant_id][pk_cols], which
   * exercises different offset calculations in both getTTLExpressionForRow() (isSalted
   * branch) and the GLOBAL_VIEWS post-filter (tenant-id extraction from salted region keys).
   */
  @Test
  public void testCompactionWithSaltedMultiTenantTable() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = SchemaUtil.getTableName(schemaName, generateUniqueName());
    String view2Name = SchemaUtil.getTableName(schemaName, generateUniqueName());

    int ttlOrg1 = 10;
    int ttlOrg2 = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      conn.createStatement().execute(String.format(
        "CREATE TABLE %s ("
          + "ORGID VARCHAR NOT NULL, "
          + "ID1 VARCHAR NOT NULL, "
          + "COL1 VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
          + ") MULTI_TENANT=true, SALT_BUCKETS=4, COLUMN_ENCODED_BYTES=0,"
          + " DEFAULT_COLUMN_FAMILY='0'",
        fullTableName));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'org1' TTL = %d",
        view1Name, fullTableName, ttlOrg1));

      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ORGID = 'org2' TTL = %d",
        view2Name, fullTableName, ttlOrg2));

      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      upsertRowSimple(conn, view1Name, "k1", "c1");
      upsertRowSimple(conn, view1Name, "k2", "c2");
      upsertRowSimple(conn, view2Name, "k1", "c3");
      upsertRowSimple(conn, view2Name, "k2", "c4");
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 4,
      "All 4 rows should exist in salted table before compaction");

    injectEdge.setValue(afterInsertTime + (ttlOrg1 * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 2,
      "Only org2's 2 rows should remain in salted table after org1 TTL");

    injectEdge.setValue(afterInsertTime + (ttlOrg2 * 2 * 1000L));
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 0,
      "All rows should be removed from salted table after all TTLs expire");
  }

  // ---- Helper methods ----

  private void upsertRow(Connection conn, String viewName,
      String id1, String id2, String col1, String col2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(String.format(
      "UPSERT INTO %s (ID1, ID2, COL1, COL2) VALUES (?, ?, ?, ?)", viewName));
    stmt.setString(1, id1);
    stmt.setString(2, id2);
    stmt.setString(3, col1);
    stmt.setString(4, col2);
    stmt.executeUpdate();
    conn.commit();
  }

  private void upsertRowDirect(Connection conn, String tableName,
      String orgId, String id1, String col1) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(String.format(
      "UPSERT INTO %s (ORGID, ID1, COL1) VALUES (?, ?, ?)", tableName));
    stmt.setString(1, orgId);
    stmt.setString(2, id1);
    stmt.setString(3, col1);
    stmt.executeUpdate();
    conn.commit();
  }

  private void upsertRowSimple(Connection conn, String viewName,
      String id1, String col1) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(String.format(
      "UPSERT INTO %s (ID1, COL1) VALUES (?, ?)", viewName));
    stmt.setString(1, id1);
    stmt.setString(2, col1);
    stmt.executeUpdate();
    conn.commit();
  }

  private void assertViewRowCount(Connection conn, String viewName,
      int expectedCount, String message) throws SQLException {
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT COUNT(*) FROM " + viewName);
    assertTrue(message + " (query returned no rows)", rs.next());
    assertEquals(message, expectedCount, rs.getInt(1));
    assertFalse(rs.next());
  }

  private void assertHBaseRowCount(String schemaName, String tableName,
      long minTimestamp, int expectedRows, String message)
      throws IOException, SQLException {
    byte[] hbaseTableNameBytes = SchemaUtil.getTableNameAsBytes(schemaName, tableName);
    try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
        .getTable(hbaseTableNameBytes)) {
      Scan allRows = new Scan();
      allRows.setTimeRange(minTimestamp, HConstants.LATEST_TIMESTAMP);
      ResultScanner scanner = tbl.getScanner(allRows);
      int numRows = 0;
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        numRows++;
      }
      assertEquals(message, expectedRows, numRows);
    }
  }

  private void flushAndMajorCompact(String schemaName, String tableName) throws Exception {
    String hbaseTableName = SchemaUtil.getTableName(schemaName, tableName);
    TableName tn = TableName.valueOf(hbaseTableName);
    try (org.apache.hadoop.hbase.client.Connection hConn =
        ConnectionFactory.createConnection(getUtility().getConfiguration())) {
      Admin admin = hConn.getAdmin();
      if (!admin.tableExists(tn)) {
        return;
      }
      admin.flush(tn);
      TestUtil.majorCompact(getUtility(), tn);
    }
  }

}
