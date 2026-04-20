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

import static org.apache.phoenix.end2end.IndexToolIT.verifyIndexTable;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_BATCH_SIZE;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class MultiTenantEventualIndexIT extends ParallelStatsDisabledIT {

  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantEventualIndexIT.class);
  protected static final int MAX_LOOKBACK_AGE = 1000000;
  private static final long WAIT_MS = 25000;
  private static final int ROWS_PER_TENANT_PER_PHASE = 10;
  private static final String[] TENANT_PREFIXES = { "AA_", "BB_", "CC_", "DD_", "EE_", "FF_" };

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(12);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    props.put(INDEX_CDC_CONSUMER_BATCH_SIZE, Integer.toString(3500));
    props.put(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, Integer.toString(4000));
    props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(2));
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1));
    props.put(QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB, Boolean.TRUE.toString());
    props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
    props.put(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, Boolean.TRUE.toString());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  private Connection getTenantConnection(String tenantId) throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    return DriverManager.getConnection(getUrl(), props);
  }

  private Connection getGlobalConnection() throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private void waitForEventualConsistency() throws InterruptedException {
    Thread.sleep(WAIT_MS);
  }

  private String[] createTenants() {
    String[] tenants = new String[TENANT_PREFIXES.length];
    for (int i = 0; i < TENANT_PREFIXES.length; i++) {
      tenants[i] = TENANT_PREFIXES[i] + generateUniqueName();
    }
    return tenants;
  }

  private void insertRows(String[] tenants, String tableName, String phase, int startRow,
    int endRow) throws Exception {
    for (String tenant : tenants) {
      try (Connection conn = getTenantConnection(tenant)) {
        for (int j = startRow; j <= endRow; j++) {
          conn.createStatement().execute(
            String.format("UPSERT INTO %s(PK2, V1, V2) VALUES ('%s_r%d', '%s_v%d', '%s_d%d')",
              tableName, phase, j, phase, j, phase, j));
        }
        conn.commit();
      }
    }
  }

  private void updateRows(String[] tenants, String tableName, String phase, int startRow,
    int endRow, String suffix) throws Exception {
    for (String tenant : tenants) {
      try (Connection conn = getTenantConnection(tenant)) {
        for (int j = startRow; j <= endRow; j++) {
          conn.createStatement()
            .execute(String.format(
              "UPSERT INTO %s(PK2, V1, V2) VALUES ('%s_r%d', '%s_v%d_%s', '%s_d%d_%s')", tableName,
              phase, j, phase, j, suffix, phase, j, suffix));
        }
        conn.commit();
      }
    }
  }

  private void deleteRows(String[] tenants, String tableName, String phase, int startRow,
    int endRow) throws Exception {
    for (String tenant : tenants) {
      try (Connection conn = getTenantConnection(tenant)) {
        for (int j = startRow; j <= endRow; j++) {
          conn.createStatement()
            .execute(String.format("DELETE FROM %s WHERE PK2 = '%s_r%d'", tableName, phase, j));
        }
        conn.commit();
      }
    }
  }

  private void verifyRowCount(String[] tenants, String tableName, int expectedCount, String phase)
    throws Exception {
    for (String tenant : tenants) {
      try (Connection conn = getTenantConnection(tenant)) {
        ResultSet rs = conn.createStatement()
          .executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE V1 IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(phase + ": tenant " + tenant, expectedCount, rs.getInt(1));
      }
    }
  }

  private void verifyIndexLookup(String[] tenants, String tableName, String v1Value,
    String expectedPk2, String expectedV2) throws Exception {
    for (String tenant : tenants) {
      try (Connection conn = getTenantConnection(tenant)) {
        ResultSet rs = conn.createStatement()
          .executeQuery("SELECT PK2, V2 FROM " + tableName + " WHERE V1 = '" + v1Value + "'");
        assertTrue("Row with V1=" + v1Value + " not found for " + tenant, rs.next());
        assertEquals(expectedPk2, rs.getString(1));
        if (expectedV2 != null) {
          assertEquals(expectedV2, rs.getString(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  private void verifyNoResult(String[] tenants, String tableName, String v1Value, String message)
    throws Exception {
    for (String tenant : tenants) {
      try (Connection conn = getTenantConnection(tenant)) {
        ResultSet rs = conn.createStatement()
          .executeQuery("SELECT PK2 FROM " + tableName + " WHERE V1 = '" + v1Value + "'");
        assertFalse(message + " for " + tenant, rs.next());
      }
    }
  }

  private int getRegionCount(Connection conn, String tableName) throws Exception {
    List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);
    return regions.size();
  }

  @Test
  public void testBasicMultiTenantEventualIndex() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    String tenantA = "TENANT_A_" + generateUniqueName();
    String tenantB = "TENANT_B_" + generateUniqueName();

    try (Connection globalConn = getGlobalConnection()) {
      globalConn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (TENANT_ID VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR"
          + " CONSTRAINT pk PRIMARY KEY (TENANT_ID, PK2))"
          + " MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0");
      globalConn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName
        + "(V1) INCLUDE (V2) CONSISTENCY=EVENTUAL");
    }

    try (Connection connA = getTenantConnection(tenantA)) {
      connA.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r1', 'a1', 'x1')");
      connA.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r2', 'a2', 'x2')");
      connA.commit();
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      connB.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r1', 'b1', 'y1')");
      connB.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r2', 'b2', 'y2')");
      connB.commit();
    }

    waitForEventualConsistency();

    try (Connection connA = getTenantConnection(tenantA)) {
      ResultSet rs = connA.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'a1'");
      assertTrue(rs.next());
      assertEquals("r1", rs.getString(1));
      assertEquals("a1", rs.getString(2));
      assertEquals("x1", rs.getString(3));
      assertFalse(rs.next());

      rs = connA.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'a2'");
      assertTrue(rs.next());
      assertEquals("r2", rs.getString(1));
      assertEquals("a2", rs.getString(2));
      assertEquals("x2", rs.getString(3));
      assertFalse(rs.next());
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      ResultSet rs = connB.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'b1'");
      assertTrue(rs.next());
      assertEquals("r1", rs.getString(1));
      assertEquals("b1", rs.getString(2));
      assertEquals("y1", rs.getString(3));
      assertFalse(rs.next());
    }

    try (Connection globalConn = getGlobalConnection()) {
      long rowCount = verifyIndexTable(tableName, indexName, globalConn);
      assertEquals(4, rowCount);
    }
  }

  @Test
  public void testMultiTenantDeleteAndUpsert() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    String tenantA = "TENANT_A_" + generateUniqueName();
    String tenantB = "TENANT_B_" + generateUniqueName();

    try (Connection globalConn = getGlobalConnection()) {
      globalConn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (TENANT_ID VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR"
          + " CONSTRAINT pk PRIMARY KEY (TENANT_ID, PK2))"
          + " MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0");
      globalConn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName
        + "(V1) INCLUDE (V2) CONSISTENCY=EVENTUAL");
    }

    try (Connection connA = getTenantConnection(tenantA)) {
      connA.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r1', 'a1', 'x1')");
      connA.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r2', 'a2', 'x2')");
      connA.commit();
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      connB.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r1', 'b1', 'y1')");
      connB.commit();
    }

    waitForEventualConsistency();

    try (Connection connA = getTenantConnection(tenantA)) {
      connA.createStatement().execute("DELETE FROM " + tableName + " WHERE PK2 = 'r1'");
      connA.commit();
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      connB.createStatement().execute(
        "UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r1', 'b1_updated', 'y1_updated')");
      connB.commit();
    }

    waitForEventualConsistency();

    try (Connection connA = getTenantConnection(tenantA)) {
      ResultSet rs = connA.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'a1'");
      assertFalse("Deleted row should not be visible", rs.next());

      rs = connA.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'a2'");
      assertTrue(rs.next());
      assertEquals("r2", rs.getString(1));
      assertFalse(rs.next());
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      ResultSet rs = connB.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'b1_updated'");
      assertTrue(rs.next());
      assertEquals("r1", rs.getString(1));
      assertEquals("y1_updated", rs.getString(3));
      assertFalse(rs.next());
    }

    try (Connection globalConn = getGlobalConnection()) {
      long rowCount = verifyIndexTable(tableName, indexName, globalConn);
      assertEquals(2, rowCount);
    }
  }

  @Test
  public void testMultiTenantUncoveredEventualIndex() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    String tenantA = "TENANT_A_" + generateUniqueName();
    String tenantB = "TENANT_B_" + generateUniqueName();

    try (Connection globalConn = getGlobalConnection()) {
      globalConn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (TENANT_ID VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR"
          + " CONSTRAINT pk PRIMARY KEY (TENANT_ID, PK2))"
          + " MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0");
      globalConn.createStatement().execute(
        "CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + "(V1) CONSISTENCY=EVENTUAL");
    }

    try (Connection connA = getTenantConnection(tenantA)) {
      connA.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r1', 'a1', 'x1')");
      connA.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r2', 'a2', 'x2')");
      connA.commit();
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      connB.createStatement()
        .execute("UPSERT INTO " + tableName + "(PK2, V1, V2) VALUES ('r1', 'b1', 'y1')");
      connB.commit();
    }

    waitForEventualConsistency();

    try (Connection connA = getTenantConnection(tenantA)) {
      ResultSet rs = connA.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'a1'");
      assertTrue(rs.next());
      assertEquals("r1", rs.getString(1));
      assertEquals("a1", rs.getString(2));
      assertEquals("x1", rs.getString(3));
      assertFalse(rs.next());
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      ResultSet rs = connB.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'b1'");
      assertTrue(rs.next());
      assertEquals("r1", rs.getString(1));
      assertEquals("b1", rs.getString(2));
      assertEquals("y1", rs.getString(3));
      assertFalse(rs.next());
    }

    try (Connection connA = getTenantConnection(tenantA)) {
      connA.createStatement().execute("DELETE FROM " + tableName + " WHERE PK2 = 'r1'");
      connA.commit();
    }

    waitForEventualConsistency();

    try (Connection connA = getTenantConnection(tenantA)) {
      ResultSet rs = connA.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'a1'");
      assertFalse("Deleted row should not appear for tenant A", rs.next());
    }

    try (Connection connB = getTenantConnection(tenantB)) {
      ResultSet rs = connB.createStatement()
        .executeQuery("SELECT PK2, V1, V2 FROM " + tableName + " WHERE V1 = 'b1'");
      assertTrue("Tenant B row should still be visible", rs.next());
      assertEquals("r1", rs.getString(1));
      assertFalse(rs.next());
    }

    try (Connection globalConn = getGlobalConnection()) {
      long rowCount = verifyIndexTable(tableName, indexName, globalConn);
      assertEquals(2, rowCount);
    }
  }

  @Test
  public void testMultiTenantCoveredIndexWithSplits() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    String[] tenants = createTenants();

    try (Connection globalConn = getGlobalConnection()) {
      globalConn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (TENANT_ID VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR"
          + " CONSTRAINT pk PRIMARY KEY (TENANT_ID, PK2))"
          + " MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0");
      globalConn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName
        + "(V1) INCLUDE (V2) CONSISTENCY=EVENTUAL");
    }

    // Phase 1: Insert 10 rows per tenant (60 total), single region
    insertRows(tenants, tableName, "p1", 1, ROWS_PER_TENANT_PER_PHASE);
    waitForEventualConsistency();
    verifyRowCount(tenants, tableName, ROWS_PER_TENANT_PER_PHASE, "Phase 1");
    verifyIndexLookup(tenants, tableName, "p1_v5", "p1_r5", "p1_d5");

    try (Connection globalConn = getGlobalConnection()) {
      TestUtil.splitTable(globalConn, tableName, Bytes.toBytes("CC"));
      assertEquals("Region count after split 1", 2, getRegionCount(globalConn, tableName));
    }

    // Phase 2: Insert 10 more rows per tenant after first split
    insertRows(tenants, tableName, "p2", 1, ROWS_PER_TENANT_PER_PHASE);
    waitForEventualConsistency();
    verifyRowCount(tenants, tableName, 2 * ROWS_PER_TENANT_PER_PHASE, "Phase 2");
    verifyIndexLookup(tenants, tableName, "p1_v3", "p1_r3", "p1_d3");
    verifyIndexLookup(tenants, tableName, "p2_v7", "p2_r7", "p2_d7");

    try (Connection globalConn = getGlobalConnection()) {
      TestUtil.splitTable(globalConn, tableName, Bytes.toBytes("EE"));
      assertEquals("Region count after split 2", 3, getRegionCount(globalConn, tableName));
    }

    insertRows(tenants, tableName, "p3", 1, 5);
    updateRows(tenants, tableName, "p1", 1, 5, "upd");
    waitForEventualConsistency();
    // 10 (p1, 5 updated) + 10 (p2) + 5 (p3) = 25 unique PKs
    verifyRowCount(tenants, tableName, 2 * ROWS_PER_TENANT_PER_PHASE + 5, "Phase 3");

    verifyIndexLookup(tenants, tableName, "p1_v1_upd", "p1_r1", "p1_d1_upd");
    verifyIndexLookup(tenants, tableName, "p1_v5_upd", "p1_r5", "p1_d5_upd");
    verifyNoResult(tenants, tableName, "p1_v1", "Old p1_v1 should not exist");
    verifyNoResult(tenants, tableName, "p1_v5", "Old p1_v5 should not exist");
    verifyIndexLookup(tenants, tableName, "p1_v6", "p1_r6", "p1_d6");

    deleteRows(tenants, tableName, "p2", 1, 2);
    waitForEventualConsistency();
    verifyRowCount(tenants, tableName, 2 * ROWS_PER_TENANT_PER_PHASE + 3, "Phase 4");
    verifyNoResult(tenants, tableName, "p2_v1", "Deleted p2_r1 should not be visible via index");
    verifyNoResult(tenants, tableName, "p2_v2", "Deleted p2_r2 should not be visible via index");
    verifyIndexLookup(tenants, tableName, "p2_v3", "p2_r3", "p2_d3");

    try (Connection globalConn = getGlobalConnection()) {
      long rowCount = verifyIndexTable(tableName, indexName, globalConn);
      assertEquals(23 * tenants.length, rowCount);
    }
  }

  @Test
  public void testMultiTenantUncoveredIndexWithSplits() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    String[] tenants = createTenants();

    try (Connection globalConn = getGlobalConnection()) {
      globalConn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (TENANT_ID VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR"
          + " CONSTRAINT pk PRIMARY KEY (TENANT_ID, PK2))"
          + " MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0");
      globalConn.createStatement().execute(
        "CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + "(V1) CONSISTENCY=EVENTUAL");
    }

    insertRows(tenants, tableName, "p1", 1, ROWS_PER_TENANT_PER_PHASE);
    waitForEventualConsistency();
    verifyRowCount(tenants, tableName, ROWS_PER_TENANT_PER_PHASE, "Phase 1");
    verifyIndexLookup(tenants, tableName, "p1_v5", "p1_r5", "p1_d5");

    try (Connection globalConn = getGlobalConnection()) {
      TestUtil.splitTable(globalConn, tableName, Bytes.toBytes("CC"));
      assertEquals(2, getRegionCount(globalConn, tableName));
    }

    insertRows(tenants, tableName, "p2", 1, ROWS_PER_TENANT_PER_PHASE);
    waitForEventualConsistency();
    verifyRowCount(tenants, tableName, 2 * ROWS_PER_TENANT_PER_PHASE, "Phase 2");

    try (Connection globalConn = getGlobalConnection()) {
      TestUtil.splitTable(globalConn, tableName, Bytes.toBytes("EE"));
      assertEquals(3, getRegionCount(globalConn, tableName));
    }

    insertRows(tenants, tableName, "p3", 1, 5);
    updateRows(tenants, tableName, "p1", 1, 5, "upd");
    waitForEventualConsistency();
    verifyRowCount(tenants, tableName, 2 * ROWS_PER_TENANT_PER_PHASE + 5, "Phase 3");
    verifyIndexLookup(tenants, tableName, "p1_v3_upd", "p1_r3", "p1_d3_upd");
    verifyNoResult(tenants, tableName, "p1_v3", "Old value should not exist after update");

    deleteRows(tenants, tableName, "p2", 1, 2);
    waitForEventualConsistency();
    verifyRowCount(tenants, tableName, 2 * ROWS_PER_TENANT_PER_PHASE + 3, "Phase 4");
    verifyNoResult(tenants, tableName, "p2_v1", "Deleted row should not be visible");
    verifyIndexLookup(tenants, tableName, "p2_v5", "p2_r5", "p2_d5");

    try (Connection globalConn = getGlobalConnection()) {
      long rowCount = verifyIndexTable(tableName, indexName, globalConn);
      assertEquals(23 * tenants.length, rowCount);
    }
  }

  @Test(timeout = 1800000)
  @Ignore("too aggressive for jenkins builds")
  public void testConcurrentUpsertsWithTableSplits() throws Exception {
    int nThreads = 8;
    final int batchSize = 100;
    final int nRows = 777;
    final int nIndexValues = 23;
    final int nSplits = 3;
    final int totalUpserts = 10000;
    final String tableName = generateUniqueName();
    final String indexName1 = generateUniqueName();
    final String indexName2 = generateUniqueName();
    final String indexName3 = generateUniqueName();
    final String indexName4 = generateUniqueName();
    final String indexName5 = generateUniqueName();
    final String[] tenants = createTenants();

    try (Connection globalConn = getGlobalConnection()) {
      globalConn.createStatement()
        .execute("CREATE TABLE " + tableName
          + "(TENANT_ID VARCHAR NOT NULL, K2 INTEGER NOT NULL, V1 INTEGER, V2 INTEGER,"
          + " V3 INTEGER, V4 INTEGER," + " CONSTRAINT pk PRIMARY KEY (TENANT_ID, K2))"
          + " MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0");
      globalConn.createStatement().execute("CREATE INDEX " + indexName1 + " ON " + tableName
        + "(V1) INCLUDE (V2, V3) CONSISTENCY=EVENTUAL");
      globalConn.createStatement().execute(
        "CREATE UNCOVERED INDEX " + indexName2 + " ON " + tableName + "(V2) CONSISTENCY=EVENTUAL");
      globalConn.createStatement().execute("CREATE INDEX " + indexName3 + " ON " + tableName
        + "(V3) INCLUDE (V1, V2) CONSISTENCY=EVENTUAL");
      globalConn.createStatement().execute(
        "CREATE UNCOVERED INDEX " + indexName4 + " ON " + tableName + "(V4) CONSISTENCY=EVENTUAL");
      globalConn.createStatement().execute("CREATE INDEX " + indexName5 + " ON " + tableName
        + "(V1, V2) INCLUDE (V3, V4) CONSISTENCY=EVENTUAL");
    }

    final CountDownLatch doneSignal = new CountDownLatch(nThreads);
    Runnable[] runnables = new Runnable[nThreads];
    Thread.sleep(3000);
    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    for (int i = 0; i < nThreads; i++) {
      final String tenant = tenants[i % tenants.length];
      runnables[i] = () -> {
        try (Connection conn = getTenantConnection(tenant)) {
          ThreadLocalRandom rand = ThreadLocalRandom.current();
          for (int j = 0; j < totalUpserts; j++) {
            conn.createStatement()
              .execute("UPSERT INTO " + tableName + " VALUES (" + (j % nRows) + ", "
                + (rand.nextBoolean() ? null : (rand.nextInt() % nIndexValues)) + ", "
                + (rand.nextBoolean() ? null : rand.nextInt()) + ", "
                + (rand.nextBoolean() ? null : rand.nextInt()) + ", "
                + (rand.nextBoolean() ? null : rand.nextInt()) + ")");
            if ((j % batchSize) == 0) {
              conn.commit();
            }
          }
          conn.commit();
        } catch (SQLException e) {
          LOG.warn("Exception during concurrent upsert for tenant {}", tenant, e);
        } finally {
          doneSignal.countDown();
        }
      };
    }

    Thread splitThread = new Thread(() -> TestUtil.splitTable(getUrl(), tableName, nSplits, 8000));
    splitThread.start();
    for (int i = 0; i < nThreads; i++) {
      if (i >= (nThreads - 4)) {
        Thread.sleep(12000);
      }
      Thread t = new Thread(runnables[i]);
      t.start();
    }
    assertTrue("Ran out of time", doneSignal.await(350, TimeUnit.SECONDS));
    splitThread.join(10000);
    LOG.info("Total upsert time: {} ms", EnvironmentEdgeManager.currentTimeMillis() - startTime);

    int expectedTotal = tenants.length * nRows;
    List<String> allIndexes =
      new ArrayList<>(Arrays.asList(indexName1, indexName2, indexName3, indexName4, indexName5));
    Collections.shuffle(allIndexes, ThreadLocalRandom.current());
    LOG.info("Randomly selected indexes to verify: {}, {}", allIndexes.get(0), allIndexes.get(1));
    try (Connection globalConn = getGlobalConnection()) {
      Thread.sleep(500000);
      long rowCount = verifyIndexTable(tableName, allIndexes.get(0), globalConn, false);
      assertEquals("Index " + allIndexes.get(0), expectedTotal, rowCount);
      rowCount = verifyIndexTable(tableName, allIndexes.get(1), globalConn, false);
      assertEquals("Index " + allIndexes.get(1), expectedTotal, rowCount);
    }
  }

}
