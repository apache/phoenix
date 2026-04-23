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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.phoenix.exception.SQLExceptionCode;
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
 * Integration test verifying per-tenant TTL via tenant views on a multi-tenant table.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class TenantTTLIT extends BaseTest {

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
   * Verifies that different tenants can have different TTLs via global views with WHERE clauses on
   * the tenant-id column, and that read masking works correctly for each. Tenant org1 gets TTL = 10
   * seconds, org2 gets TTL = 100 seconds. After 20 seconds: org1's data should be masked, org2's
   * data should still be visible.
   */
  @Test
  public void testReadMaskingWithDifferentTenantTTLs() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = generateUniqueName();
    String view2Name = generateUniqueName();

    int ttlOrg1 = 10;
    int ttlOrg2 = 100;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement().execute(String.format(
        "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, ID2 VARCHAR NOT NULL, "
          + "COL1 VARCHAR, COL2 VARCHAR " + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1, ID2)"
          + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
        fullTableName));
    }

    createTenantViewWithTTL("org1", fullTableName, view1Name, ttlOrg1);
    createTenantViewWithTTL("org2", fullTableName, view2Name, ttlOrg2);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      upsertRow(t1, view1Name, "k1", "v1", "row1col1", "row1col2");
      upsertRow(t1, view1Name, "k2", "v2", "row2col1", "row2col2");
    }
    try (Connection t2 = getTenantConnection("org2")) {
      t2.setAutoCommit(true);
      upsertRow(t2, view2Name, "k1", "v1", "row3col1", "row3col2");
      upsertRow(t2, view2Name, "k2", "v2", "row4col1", "row4col2");
    }

    try (Connection t1 = getTenantConnection("org1"); Connection t2 = getTenantConnection("org2")) {
      assertViewRowCount(t1, view1Name, 2, "org1 should have 2 rows before TTL expiry");
      assertViewRowCount(t2, view2Name, 2, "org2 should have 2 rows before TTL expiry");

      injectEdge.incrementValue((ttlOrg1 * 2) * 1000L);

      assertViewRowCount(t1, view1Name, 0, "org1 rows should be masked after org1 TTL expiry");
      assertViewRowCount(t2, view2Name, 2,
        "org2 rows should still be visible (TTL not expired yet)");

      injectEdge.incrementValue((ttlOrg2 * 2) * 1000L);

      assertViewRowCount(t2, view2Name, 0, "org2 rows should be masked after org2 TTL expiry");
    }
  }

  /**
   * Verifies that major compaction physically removes expired rows per the view's TTL, while
   * preserving rows belonging to views whose TTL has not yet expired.
   */
  @Test
  public void testCompactionWithDifferentTenantTTLs() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = generateUniqueName();
    String view2Name = generateUniqueName();

    int ttlOrg1 = 10;
    int ttlOrg2 = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 INTEGER NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    createTenantViewWithTTL("org1", fullTableName, view1Name, ttlOrg1);
    createTenantViewWithTTL("org2", fullTableName, view2Name, ttlOrg2);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      upsertRowInt(t1, view1Name, 1, "c1");
      upsertRowInt(t1, view1Name, 2, "c2");
    }
    try (Connection t2 = getTenantConnection("org2")) {
      t2.setAutoCommit(true);
      upsertRowInt(t2, view2Name, 1, "c3");
      upsertRowInt(t2, view2Name, 2, "c4");
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
   * Verifies read masking and compaction for three tenants with different TTLs to confirm the
   * mechanism scales beyond two tenants.
   */
  @Test
  public void testThreeTenantsDifferentTTLs() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String viewA = generateUniqueName();
    String viewB = generateUniqueName();
    String viewC = generateUniqueName();

    int ttlA = 10;
    int ttlB = 50;
    int ttlC = 200;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, KP CHAR(3) NOT NULL, SEQ BIGINT NOT NULL, "
            + "COL1 VARCHAR " + "CONSTRAINT PK PRIMARY KEY (ORGID, KP, SEQ)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    createTenantViewWithTTL("tenA", fullTableName, viewA, ttlA);
    createTenantViewWithTTL("tenB", fullTableName, viewB, ttlB);
    createTenantViewWithTTL("tenC", fullTableName, viewC, ttlC);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection tA = getTenantConnection("tenA"); Connection tB = getTenantConnection("tenB");
      Connection tC = getTenantConnection("tenC")) {
      tA.setAutoCommit(true);
      tB.setAutoCommit(true);
      tC.setAutoCommit(true);
      upsertRowCharBigint(tA, viewA, "AAA", 1L, "a1");
      upsertRowCharBigint(tB, viewB, "BBB", 1L, "b1");
      upsertRowCharBigint(tC, viewC, "CCC", 1L, "c1");

      assertViewRowCount(tA, viewA, 1, "tenA visible before TTL");
      assertViewRowCount(tB, viewB, 1, "tenB visible before TTL");
      assertViewRowCount(tC, viewC, 1, "tenC visible before TTL");

      injectEdge.incrementValue(ttlA * 2 * 1000L);
      assertViewRowCount(tA, viewA, 0, "tenA masked after its TTL");
      assertViewRowCount(tB, viewB, 1, "tenB still visible");
      assertViewRowCount(tC, viewC, 1, "tenC still visible");

      injectEdge.incrementValue(ttlB * 2 * 1000L);
      assertViewRowCount(tB, viewB, 0, "tenB masked after its TTL");
      assertViewRowCount(tC, viewC, 1, "tenC still visible");
    }

    long afterInsertTime = injectEdge.currentTime();

    injectEdge.setValue(afterInsertTime + (ttlC * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, 0, 0,
      "All rows should be compacted away after all TTLs expire");
  }

  /**
   * Verifies that compaction works correctly with a pre-split table where each region has non-empty
   * start/end keys. Four tenants span two regions; compaction should correctly expire data per view
   * regardless of which region the data resides in.
   */
  @Test
  public void testCompactionWithSplitRegions() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String viewA = generateUniqueName();
    String viewB = generateUniqueName();
    String viewC = generateUniqueName();
    String viewD = generateUniqueName();

    int shortTTL = 10;
    int longTTL = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      // Pre-split between orgB and orgC so we get 2 regions:
      // Region 1: [empty, 'orgC') -> orgA, orgB
      // Region 2: ['orgC', empty) -> orgC, orgD
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'"
            + " SPLIT ON ('orgC')",
          fullTableName));
    }

    TableName tn = TableName.valueOf(SchemaUtil.getTableName(schemaName, baseTableName));
    try (org.apache.hadoop.hbase.client.Connection hConn =
      ConnectionFactory.createConnection(getUtility().getConfiguration())) {
      List<RegionInfo> regions = hConn.getAdmin().getRegions(tn);
      assertEquals("Expected 2 regions after SPLIT ON", 2, regions.size());
    }

    createTenantViewWithTTL("orgA", fullTableName, viewA, shortTTL);
    createTenantViewWithTTL("orgB", fullTableName, viewB, shortTTL);
    createTenantViewWithTTL("orgC", fullTableName, viewC, shortTTL);
    createTenantViewWithTTL("orgD", fullTableName, viewD, longTTL);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    String[] tenants = { "orgA", "orgB", "orgC", "orgD" };
    String[] views = { viewA, viewB, viewC, viewD };
    for (int i = 0; i < tenants.length; i++) {
      try (Connection tc = getTenantConnection(tenants[i])) {
        tc.setAutoCommit(true);
        upsertRowSimple(tc, views[i], "r1", tenants[i] + "_val");
      }
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
   * Verifies that compaction correctly applies per-tenant TTL on a SALTED multi-tenant table.
   * Salting changes the row key layout to [salt_byte][tenant_id][pk_cols], which exercises
   * different offset calculations in both getTTLExpressionForRow() (isSalted branch) and the
   * GLOBAL_VIEWS post-filter (tenant-id extraction from salted region keys).
   */
  @Test
  public void testCompactionWithSaltedMultiTenantTable() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = generateUniqueName();
    String view2Name = generateUniqueName();

    int ttlOrg1 = 10;
    int ttlOrg2 = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);

      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, SALT_BUCKETS=4, COLUMN_ENCODED_BYTES=0,"
            + " DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    createTenantViewWithTTL("org1", fullTableName, view1Name, ttlOrg1);
    createTenantViewWithTTL("org2", fullTableName, view2Name, ttlOrg2);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      upsertRowSimple(t1, view1Name, "k1", "c1");
      upsertRowSimple(t1, view1Name, "k2", "c2");
    }
    try (Connection t2 = getTenantConnection("org2")) {
      t2.setAutoCommit(true);
      upsertRowSimple(t2, view2Name, "k1", "c3");
      upsertRowSimple(t2, view2Name, "k2", "c4");
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

  /**
   * Verifies that tenant TTL compaction on the base table works correctly when a secondary index
   * exists on the base table.
   */
  @Test
  public void testCompactionWithTenantTableIndex() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String indexName = generateUniqueName();
    String view1Name = generateUniqueName();
    String view2Name = generateUniqueName();

    int ttlOrg1 = 10;
    int ttlOrg2 = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s ("
            + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR, COL2 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
      conn.createStatement().execute(
        String.format("CREATE INDEX %s ON %s(COL1) INCLUDE(COL2)", indexName, fullTableName));
    }

    createTenantViewWithTTL("org1", fullTableName, view1Name, ttlOrg1);
    createTenantViewWithTTL("org2", fullTableName, view2Name, ttlOrg2);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      upsertRowSimple(t1, view1Name, "k1", "v1");
      upsertRowSimple(t1, view1Name, "k2", "v2");
    }
    try (Connection t2 = getTenantConnection("org2")) {
      t2.setAutoCommit(true);
      upsertRowSimple(t2, view2Name, "k1", "v3");
      upsertRowSimple(t2, view2Name, "k2", "v4");
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 4,
      "Base table should have 4 rows before compaction");

    injectEdge.setValue(afterInsertTime + (ttlOrg1 * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 2,
      "Only org2's 2 rows should remain in base table after org1 TTL even with index present");

    injectEdge.setValue(afterInsertTime + (ttlOrg2 * 2 * 1000L));
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 0,
      "All rows should be removed from base table after all TTLs expire");
  }

  /**
   * Verifies that two different tenants can create tenant views with the SAME view name on the same
   * base table, with independent TTLs that don't interfere with each other.
   */
  @Test
  public void testSameViewNameAcrossDifferentTenants() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String sharedViewName = generateUniqueName();

    int ttlOrg1 = 10;
    int ttlOrg2 = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    // Both tenants create a view with the same name; scoped by tenant-id in SYSTEM.CATALOG.
    createTenantViewWithTTL("org1", fullTableName, sharedViewName, ttlOrg1);
    createTenantViewWithTTL("org2", fullTableName, sharedViewName, ttlOrg2);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      upsertRowSimple(t1, sharedViewName, "k1", "a1");
      upsertRowSimple(t1, sharedViewName, "k2", "a2");
    }
    try (Connection t2 = getTenantConnection("org2")) {
      t2.setAutoCommit(true);
      upsertRowSimple(t2, sharedViewName, "k1", "b1");
      upsertRowSimple(t2, sharedViewName, "k2", "b2");
    }

    try (Connection t1 = getTenantConnection("org1"); Connection t2 = getTenantConnection("org2")) {
      assertViewRowCount(t1, sharedViewName, 2, "org1 should see 2 rows");
      assertViewRowCount(t2, sharedViewName, 2, "org2 should see 2 rows");

      injectEdge.incrementValue((ttlOrg1 * 2) * 1000L);
      assertViewRowCount(t1, sharedViewName, 0, "org1 rows should be masked after its TTL");
      assertViewRowCount(t2, sharedViewName, 2, "org2 rows should still be visible");
    }

    long afterInsertTime = injectEdge.currentTime();
    flushAndMajorCompact(schemaName, baseTableName);
    assertHBaseRowCount(schemaName, baseTableName, 0, 2,
      "Only org2's rows should remain after compacting past org1 TTL");
  }

  /**
   * Exercises every branch of the tenant-view-conflict check in CreateTableCompiler:
   * <ol>
   * <li>Non-TTL view as first view for a tenant: allowed.</li>
   * <li>Another non-TTL view for the same tenant: allowed (both without TTL).</li>
   * <li>TTL view added to a tenant that already has non-TTL views: rejected (new has TTL, existing
   * siblings exist).</li>
   * <li>Different tenant on the same base creates a TTL view: allowed (per-tenant scope).</li>
   * <li>Same tenant tries to add a second TTL view: rejected (existing has TTL).</li>
   * <li>Same tenant tries to add a non-TTL view when a TTL view already exists: rejected (existing
   * has TTL).</li>
   * <li>Tenant view with WHERE clause is allowed when only non-TTL views exist.</li>
   * </ol>
   */
  @Test
  public void testTenantTTLViewCoexistenceRules() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    // Case 1: First non-TTL view for org1 -> allowed.
    String org1View1 = generateUniqueName();
    createTenantViewNoTTL("org1", fullTableName, org1View1);

    // Case 2: Another non-TTL view for org1 -> allowed (pattern (a): all non-TTL).
    String org1View2 = generateUniqueName();
    createTenantViewNoTTL("org1", fullTableName, org1View2);

    // Case 3: TTL view for org1 while non-TTL siblings exist -> rejected.
    String org1TTLView = generateUniqueName();
    assertCreateTenantViewFails("org1", fullTableName, org1TTLView, 10);

    // Case 4: org2 (different tenant) creates a TTL view on the same base -> allowed.
    String org2TTLView = generateUniqueName();
    createTenantViewWithTTL("org2", fullTableName, org2TTLView, 10);

    // Case 5: org2 tries a second TTL view -> rejected (existing has TTL).
    String org2TTLView2 = generateUniqueName();
    assertCreateTenantViewFails("org2", fullTableName, org2TTLView2, 20);

    // Case 6: org2 tries a non-TTL view -> rejected (existing has TTL).
    String org2NoTTLView = generateUniqueName();
    try (Connection conn = getTenantConnection("org2")) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format("CREATE VIEW %s AS SELECT * FROM %s", org2NoTTLView, fullTableName));
      fail("Expected TENANT_TTL_VIEW_CONFLICT when adding non-TTL view to tenant with TTL view");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.TENANT_TTL_VIEW_CONFLICT.getErrorCode(), e.getErrorCode());
    }

    // Case 7: org1 with only non-TTL siblings can still create a WHERE-scoped non-TTL view.
    String org1WhereView = generateUniqueName();
    try (Connection conn = getTenantConnection("org1")) {
      conn.setAutoCommit(true);
      conn.createStatement().execute(String.format(
        "CREATE VIEW %s AS SELECT * FROM %s WHERE ID1 = 'x'", org1WhereView, fullTableName));
    }
  }

  /**
   * Verifies the ALTER-path tenant TTL conflict check: altering a tenant view on a multi-tenant
   * base table to SET TTL is rejected if any sibling view exists for the same tenant.
   */
  @Test
  public void testAlterTTLOnTenantViewWithSiblingIsRejected() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = generateUniqueName();
    String view2Name = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    // Two non-TTL sibling views for org1 - allowed at create time.
    createTenantViewNoTTL("org1", fullTableName, view1Name);
    createTenantViewNoTTL("org1", fullTableName, view2Name);

    // Altering view1 to add TTL while view2 exists should be rejected.
    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      t1.createStatement().execute(String.format("ALTER VIEW %s SET TTL = 10", view1Name));
      fail("Expected TENANT_TTL_VIEW_CONFLICT when altering TTL while sibling view exists");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.TENANT_TTL_VIEW_CONFLICT.getErrorCode(), e.getErrorCode());
    }

    // Drop sibling then alter succeeds.
    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      t1.createStatement().execute(String.format("DROP VIEW %s", view2Name));
      t1.createStatement().execute(String.format("ALTER VIEW %s SET TTL = 10", view1Name));
    }
  }

  /**
   * Creates a tenant view without TTL, inserts rows, then alters the view to add TTL. Verifies read
   * masking and compaction honor the new TTL after the ALTER.
   */
  @Test
  public void testAlterTenantViewToAddTTL() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String viewName = generateUniqueName();

    int ttl = 10;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    createTenantViewNoTTL("org1", fullTableName, viewName);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      upsertRowSimple(t1, viewName, "k1", "v1");
      upsertRowSimple(t1, viewName, "k2", "v2");
      assertViewRowCount(t1, viewName, 2, "rows visible before TTL is set");
    }

    // Apply TTL after data was inserted.
    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      t1.createStatement().execute(String.format("ALTER VIEW %s SET TTL = %d", viewName, ttl));
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();
    injectEdge.setValue(afterInsertTime + (ttl * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);

    // Read masking after ALTER: rows should be masked.
    try (Connection t1 = getTenantConnection("org1")) {
      assertViewRowCount(t1, viewName, 0, "rows should be masked after ALTER TTL expiry");
    }

    // Compaction after ALTER: rows should be physically removed.
    flushAndMajorCompact(schemaName, baseTableName);
    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 0,
      "rows should be removed by compaction after ALTER TTL expiry");
  }

  /**
   * Creates a tenant view (no WHERE) with TTL on a multi-tenant base table, then creates child
   * views on the tenant view using WHERE on the next PK column. Verifies:
   * <ul>
   * <li>Setting TTL on the child view is rejected with TTL_ALREADY_DEFINED_IN_HIERARCHY.</li>
   * <li>Child views without TTL are accepted and inherit the parent tenant view's TTL for both read
   * masking and compaction.</li>
   * </ul>
   */
  @Test
  public void testChildViewsOfTenantTTLViewInheritTTL() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String tenantViewName = generateUniqueName();
    String childView1 = generateUniqueName();
    String childView2 = generateUniqueName();
    String childViewConflict = generateUniqueName();

    int parentTTL = 10;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement().execute(String.format(
        "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, ID2 VARCHAR NOT NULL, "
          + "COL1 VARCHAR " + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1, ID2)"
          + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
        fullTableName));
    }

    // Parent tenant view (no WHERE) with TTL.
    createTenantViewWithTTL("org1", fullTableName, tenantViewName, parentTTL);

    // Setting TTL on a child view should fail with hierarchy error.
    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      t1.createStatement()
        .execute(String.format("CREATE VIEW %s AS SELECT * FROM %s WHERE ID1 = 'a' TTL = 5",
          childViewConflict, tenantViewName));
      fail("Expected TTL_ALREADY_DEFINED_IN_HIERARCHY");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(),
        e.getErrorCode());
    }

    // Create two child views on the tenant view without TTL.
    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      t1.createStatement().execute(String
        .format("CREATE VIEW %s AS SELECT * FROM %s WHERE ID1 = 'a'", childView1, tenantViewName));
      t1.createStatement().execute(String
        .format("CREATE VIEW %s AS SELECT * FROM %s WHERE ID1 = 'b'", childView2, tenantViewName));
    }

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnection("org1")) {
      t1.setAutoCommit(true);
      upsertRowChildView(t1, childView1, "z1", "v1");
      upsertRowChildView(t1, childView1, "z2", "v2");
      upsertRowChildView(t1, childView2, "z1", "v3");
      upsertRowChildView(t1, childView2, "z2", "v4");
      assertViewRowCount(t1, childView1, 2, "childView1 visible before parent TTL");
      assertViewRowCount(t1, childView2, 2, "childView2 visible before parent TTL");
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();

    // Advance past parent's TTL: child views should be masked (inherited TTL).
    injectEdge.setValue(afterInsertTime + (parentTTL * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);

    try (Connection t1 = getTenantConnection("org1")) {
      assertViewRowCount(t1, childView1, 0,
        "childView1 should be masked using inherited parent TTL");
      assertViewRowCount(t1, childView2, 0,
        "childView2 should be masked using inherited parent TTL");
    }

    // Compaction should physically remove all rows (inherited TTL).
    flushAndMajorCompact(schemaName, baseTableName);
    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 0,
      "all rows should be removed from base table after parent TTL expiry");
  }

  /**
   * Verifies that tenant TTL works correctly when the connection has
   * PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED=true.
   */
  @Test
  public void testTenantTTLWithUpdatableViewRestrictionEnabled() throws Exception {
    String schemaName = generateUniqueName();
    String baseTableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
    String view1Name = generateUniqueName();
    String view2Name = generateUniqueName();

    int ttlOrg1 = 10;
    int ttlOrg2 = 1000;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format(
          "CREATE TABLE %s (" + "ORGID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, COL1 VARCHAR "
            + "CONSTRAINT PK PRIMARY KEY (ORGID, ID1)"
            + ") MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'",
          fullTableName));
    }

    Properties restrictProps = new Properties();
    restrictProps.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
    createTenantViewWithProps("org1", fullTableName, view1Name, ttlOrg1, restrictProps);
    createTenantViewWithProps("org2", fullTableName, view2Name, ttlOrg2, restrictProps);

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTime);

    try (Connection t1 = getTenantConnectionWithProps("org1", restrictProps)) {
      t1.setAutoCommit(true);
      upsertRowSimple(t1, view1Name, "k1", "v1");
      upsertRowSimple(t1, view1Name, "k2", "v2");
    }
    try (Connection t2 = getTenantConnectionWithProps("org2", restrictProps)) {
      t2.setAutoCommit(true);
      upsertRowSimple(t2, view2Name, "k1", "v3");
      upsertRowSimple(t2, view2Name, "k2", "v4");
    }

    long afterInsertTime = EnvironmentEdgeManager.currentTimeMillis();

    injectEdge.setValue(afterInsertTime + (ttlOrg1 * 2 * 1000L));
    EnvironmentEdgeManager.injectEdge(injectEdge);
    flushAndMajorCompact(schemaName, baseTableName);

    assertHBaseRowCount(schemaName, baseTableName, afterInsertTime, 2,
      "Only org2's 2 rows should remain after org1 TTL, even with view restriction enabled");
  }

  // ---- Helper methods ----

  private Connection getTenantConnection(String tenantId) throws SQLException {
    return DriverManager.getConnection(getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId);
  }

  private Connection getTenantConnectionWithProps(String tenantId, Properties props)
    throws SQLException {
    Properties merged = new Properties();
    merged.putAll(props);
    merged.setProperty(TENANT_ID_ATTRIB, tenantId);
    return DriverManager.getConnection(getUrl(), merged);
  }

  private void createTenantViewWithProps(String tenantId, String baseTable, String viewName,
    int ttlSeconds, Properties props) throws SQLException {
    try (Connection conn = getTenantConnectionWithProps(tenantId, props)) {
      conn.setAutoCommit(true);
      conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s TTL = %d",
        viewName, baseTable, ttlSeconds));
    }
  }

  private void createTenantViewWithTTL(String tenantId, String baseTable, String viewName,
    int ttlSeconds) throws SQLException {
    try (Connection conn = getTenantConnection(tenantId)) {
      conn.setAutoCommit(true);
      conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s TTL = %d",
        viewName, baseTable, ttlSeconds));
    }
  }

  private void upsertRow(Connection conn, String viewName, String id1, String id2, String col1,
    String col2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(
      String.format("UPSERT INTO %s (ID1, ID2, COL1, COL2) VALUES (?, ?, ?, ?)", viewName));
    stmt.setString(1, id1);
    stmt.setString(2, id2);
    stmt.setString(3, col1);
    stmt.setString(4, col2);
    stmt.executeUpdate();
    conn.commit();
  }

  private void upsertRowInt(Connection conn, String viewName, int id1, String col1)
    throws SQLException {
    PreparedStatement stmt =
      conn.prepareStatement(String.format("UPSERT INTO %s (ID1, COL1) VALUES (?, ?)", viewName));
    stmt.setInt(1, id1);
    stmt.setString(2, col1);
    stmt.executeUpdate();
    conn.commit();
  }

  private void upsertRowCharBigint(Connection conn, String viewName, String kp, long seq,
    String col1) throws SQLException {
    PreparedStatement stmt = conn
      .prepareStatement(String.format("UPSERT INTO %s (KP, SEQ, COL1) VALUES (?, ?, ?)", viewName));
    stmt.setString(1, kp);
    stmt.setLong(2, seq);
    stmt.setString(3, col1);
    stmt.executeUpdate();
    conn.commit();
  }

  private void upsertRowSimple(Connection conn, String viewName, String id1, String col1)
    throws SQLException {
    PreparedStatement stmt =
      conn.prepareStatement(String.format("UPSERT INTO %s (ID1, COL1) VALUES (?, ?)", viewName));
    stmt.setString(1, id1);
    stmt.setString(2, col1);
    stmt.executeUpdate();
    conn.commit();
  }

  private void assertViewRowCount(Connection conn, String viewName, int expectedCount,
    String message) throws SQLException {
    ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + viewName);
    assertTrue(message + " (query returned no rows)", rs.next());
    assertEquals(message, expectedCount, rs.getInt(1));
    assertFalse(rs.next());
  }

  private void assertHBaseRowCount(String schemaName, String tableName, long minTimestamp,
    int expectedRows, String message) throws IOException, SQLException {
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

  private void createTenantViewNoTTL(String tenantId, String baseTable, String viewName)
    throws SQLException {
    try (Connection conn = getTenantConnection(tenantId)) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName, baseTable));
    }
  }

  private void assertCreateTenantViewFails(String tenantId, String baseTable, String viewName,
    int ttlSeconds) throws SQLException {
    try (Connection conn = getTenantConnection(tenantId)) {
      conn.setAutoCommit(true);
      conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s TTL = %d",
        viewName, baseTable, ttlSeconds));
      fail("Expected TENANT_TTL_VIEW_CONFLICT for tenant=" + tenantId + ", view=" + viewName);
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.TENANT_TTL_VIEW_CONFLICT.getErrorCode(), e.getErrorCode());
    }
  }

  private void upsertRowChildView(Connection conn, String viewName, String id2, String col1)
    throws SQLException {
    PreparedStatement stmt =
      conn.prepareStatement(String.format("UPSERT INTO %s (ID2, COL1) VALUES (?, ?)", viewName));
    stmt.setString(1, id2);
    stmt.setString(2, col1);
    stmt.executeUpdate();
    conn.commit();
  }
}
