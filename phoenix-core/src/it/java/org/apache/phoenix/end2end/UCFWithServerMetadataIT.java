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

import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_CONSUMER_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheResponse;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test GetTable rpc calls for the combination of UCF and useServerMetadata.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class UCFWithServerMetadataIT extends BaseTest {

  private final String updateCacheFrequency;
  private final boolean useServerMetadata;
  private final boolean singleRowUpdate;
  private static final AtomicLong getTableCallCount = new AtomicLong(0);
  private static final AtomicLong addServerCacheCallCount = new AtomicLong(0);

  public UCFWithServerMetadataIT(String updateCacheFrequency, boolean useServerMetadata,
    boolean singleRowUpdate) {
    this.updateCacheFrequency = updateCacheFrequency;
    this.useServerMetadata = useServerMetadata;
    this.singleRowUpdate = singleRowUpdate;
  }

  @Parameters(name = "UpdateCacheFrequency={0}, UseServerMetadata={1}, SingleRowUpdate={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { "60000", true, true }, { "60000", true, false },
      { "60000", false, true }, { "60000", false, false }, { "ALWAYS", true, true },
      { "ALWAYS", true, false }, { "ALWAYS", false, true }, { "ALWAYS", false, false } });
  }

  public static class TrackingMetaDataEndpointImpl extends MetaDataEndpointImpl {

    @Override
    public void getTable(RpcController controller, GetTableRequest request,
      RpcCallback<MetaDataResponse> done) {
      getTableCallCount.incrementAndGet();
      super.getTable(controller, request, done);
    }
  }

  public static class TrackingServerCachingEndpointImpl extends ServerCachingEndpointImpl {

    @Override
    public void addServerCache(RpcController controller, AddServerCacheRequest request,
      RpcCallback<AddServerCacheResponse> done) {
      addServerCacheCallCount.incrementAndGet();
      super.addServerCache(controller, request, done);
    }
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = new HashMap<>(1);
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    props.put(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, Boolean.toString(false));
    setUpTestDriver(new ReadOnlyProps(props));
  }

  @Before
  public void setUp() {
    getTableCallCount.set(0);
    addServerCacheCallCount.set(0);
  }

  @Test
  public void testUpdateCacheFrequency() throws Exception {
    String dataTableName = generateUniqueName();
    String coveredIndex1 = "CI1_" + generateUniqueName();
    String coveredIndex2 = "CI2_" + generateUniqueName();
    String uncoveredIndex1 = "UI1_" + generateUniqueName();
    String uncoveredIndex2 = "UI2_" + generateUniqueName();
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(QueryServices.INDEX_USE_SERVER_METADATA_ATTRIB,
      Boolean.toString(useServerMetadata));
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String createTableDDL =
        String.format("CREATE TABLE %s (id INTEGER PRIMARY KEY, name VARCHAR(50), "
          + "age INTEGER, city VARCHAR(50), salary INTEGER, department VARCHAR(50)"
          + ") UPDATE_CACHE_FREQUENCY=%s", dataTableName, updateCacheFrequency);
      conn.createStatement().execute(createTableDDL);
      attachCustomCoprocessor(conn, dataTableName);
      conn.createStatement().execute(String
        .format("CREATE INDEX %s ON %s (name) INCLUDE (age, city)", coveredIndex1, dataTableName));
      conn.createStatement().execute(String.format(
        "CREATE INDEX %s ON %s (city) INCLUDE (salary, department)", coveredIndex2, dataTableName));
      conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (age)", uncoveredIndex1, dataTableName));
      conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (salary)", uncoveredIndex2, dataTableName));
      String upsertSQL = String.format(
        "UPSERT INTO %s (id, name, age, city, salary, department) VALUES (?, ?, ?, ?, ?, ?)",
        dataTableName);
      int totalRows = 52;
      int batchSize = 8;
      PreparedStatement stmt = conn.prepareStatement(upsertSQL);
      long startGetTableCalls = getTableCallCount.get();
      for (int i = 1; i <= totalRows; i++) {
        stmt.setInt(1, i);
        stmt.setString(2, "Name" + i);
        stmt.setInt(3, 20 + (i % 40));
        stmt.setString(4, "City" + (i % 10));
        stmt.setInt(5, 30000 + (i * 1000));
        stmt.setString(6, "Dept" + (i % 5));
        stmt.executeUpdate();
        if (singleRowUpdate) {
          conn.commit();
        } else if (i % batchSize == 0) {
          conn.commit();
        }
      }
      if (!singleRowUpdate) {
        conn.commit();
      }
      long actualGetTableCalls = getTableCallCount.get() - startGetTableCalls;
      int expectedCalls;
      String caseKey = updateCacheFrequency + "_" + useServerMetadata + "_" + singleRowUpdate;
      switch (caseKey) {
        case "60000_true_true":
        case "60000_true_false":
          expectedCalls = 1;
          break;
        case "60000_false_true":
        case "60000_false_false":
          expectedCalls = 0;
          break;
        case "ALWAYS_true_true":
          expectedCalls = totalRows;
          break;
        case "ALWAYS_true_false":
          expectedCalls = (int) Math.ceil((double) totalRows / batchSize) * 2;
          break;
        case "ALWAYS_false_true":
          expectedCalls = totalRows;
          break;
        case "ALWAYS_false_false":
          expectedCalls = (int) Math.ceil((double) totalRows / batchSize);
          break;
        default:
          throw new IllegalArgumentException("Unexpected test case: " + caseKey);
      }
      assertEquals("Expected exact number of getTable() calls for case: " + caseKey, expectedCalls,
        actualGetTableCalls);
      long actualAddServerCacheCalls = addServerCacheCallCount.get();
      int expectedAddServerCacheCalls;
      switch (caseKey) {
        case "60000_false_false":
        case "ALWAYS_false_false":
          expectedAddServerCacheCalls = (int) Math.ceil((double) totalRows / batchSize);
          break;
        default:
          expectedAddServerCacheCalls = 0;
          break;
      }
      assertEquals("Expected exact number of addServerCache() calls for case: " + caseKey,
        expectedAddServerCacheCalls, actualAddServerCacheCalls);
    }
  }

  @Test
  public void testTenantViewIndexServerMetadata() throws Exception {
    String baseTableName = generateUniqueName();
    String tenant1 = generateUniqueName();
    String tenant2 = generateUniqueName();
    String viewName1 = "V1_" + generateUniqueName();
    String viewName2 = "V2_" + generateUniqueName();
    String coveredIndex1 = "TCI1_" + generateUniqueName();
    String uncoveredIndex1 = "TUI1_" + generateUniqueName();
    String coveredIndex2 = "TCI2_" + generateUniqueName();
    String viewIndexPhysical = MetaDataUtil.VIEW_INDEX_TABLE_PREFIX + baseTableName;

    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(QueryServices.INDEX_USE_SERVER_METADATA_ATTRIB,
      Boolean.toString(useServerMetadata));
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      createMultiTenantBaseTable(conn, baseTableName);
      attachCustomCoprocessor(conn, baseTableName);
    }

    try (Connection tenant1Conn = getTenantConnection(tenant1)) {
      tenant1Conn.createStatement()
        .execute(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName1, baseTableName));
      tenant1Conn.createStatement().execute(String
        .format("CREATE INDEX %s ON %s (name) INCLUDE (age, city)", coveredIndex1, viewName1));
      tenant1Conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (salary)", uncoveredIndex1, viewName1));

      PreparedStatement upsert = tenant1Conn.prepareStatement(String.format(
        "UPSERT INTO %s (id, name, age, city, salary, department) VALUES (?, ?, ?, ?, ?, ?)",
        viewName1));

      // Warm-up (row id=1) primes both the client and the server PTable caches so that, under a
      // large UPDATE_CACHE_FREQUENCY, the measured window observes no further getTable() RPCs.
      upsertTenantViewRow(upsert, 1);
      tenant1Conn.commit();
      assertCoveredIndexRow(tenant1Conn, viewName1, viewIndexPhysical, 1);
      assertUncoveredIndexRow(tenant1Conn, viewName1, viewIndexPhysical, 1);

      long startGetTableCalls = getTableCallCount.get();
      long startAddServerCacheCalls = addServerCacheCallCount.get();

      // Measured window: upsert rows 2..6 through the tenant connection, single-row or batched
      // consistent with the singleRowUpdate dimension (a batch of 5 exceeds the mutate-batch
      // threshold so the without-server-metadata batch case uses addServerCache()).
      for (int i = 2; i <= 6; i++) {
        upsertTenantViewRow(upsert, i);
        if (singleRowUpdate) {
          tenant1Conn.commit();
        }
      }
      if (!singleRowUpdate) {
        tenant1Conn.commit();
      }
      // Correctness: each view index must return exactly the right rows/columns for this tenant.
      assertCoveredIndexRow(tenant1Conn, viewName1, viewIndexPhysical, 3);
      assertUncoveredIndexRow(tenant1Conn, viewName1, viewIndexPhysical, 5);

      long getTableDelta = getTableCallCount.get() - startGetTableCalls;
      long addServerCacheDelta = addServerCacheCallCount.get() - startAddServerCacheCalls;

      // With server metadata, index writes must never trigger a client addServerCache() RPC. This
      // is the counting assertion the production change (global vs per-tenant connection) could
      // regress. Without server metadata, only batched writes above the threshold use it.
      if (useServerMetadata) {
        assertEquals("Tenant view index writes must not use addServerCache() with server metadata",
          0L, addServerCacheDelta);
      } else if (singleRowUpdate) {
        assertEquals("Single-row writes stay below the mutate-batch threshold", 0L,
          addServerCacheDelta);
      } else {
        assertTrue("Batched writes without server metadata should use addServerCache()",
          addServerCacheDelta >= 1);
      }

      // getTable() behavior on the tenant-view path. The server-metadata path now resolves tenant
      // views/indexes with a per-tenant-scoped reusable server connection, so metadata resolution
      // honors UPDATE_CACHE_FREQUENCY exactly as it does for non-tenant tables. With UCF=ALWAYS
      // every batch re-resolves the tenant view (on the client for the non-server path, on the
      // RegionServer for the server-metadata path), so the count grows. With a large UCF, once the
      // warm-up row has populated the shared server/client PTable caches, the measured window
      // re-resolves nothing and issues no getTable() RPCs. Exact counts under ALWAYS depend on the
      // number of batches, so we assert the robust direction invariant.
      if ("ALWAYS".equals(updateCacheFrequency)) {
        assertTrue("With UCF=ALWAYS the tenant view path must re-resolve metadata (getTable grows)",
          getTableDelta > 0);
      } else {
        assertEquals("With a large UCF the warm tenant view path must not issue getTable() RPCs",
          0L, getTableDelta);
      }

      // Tenant isolation: a second tenant's view on the same base table with a same-named row must
      // not leak across the shared view-index physical table.
      try (Connection tenant2Conn = getTenantConnection(tenant2)) {
        tenant2Conn.createStatement()
          .execute(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName2, baseTableName));
        tenant2Conn.createStatement().execute(String
          .format("CREATE INDEX %s ON %s (name) INCLUDE (age, city)", coveredIndex2, viewName2));
        PreparedStatement upsert2 = tenant2Conn.prepareStatement(String.format(
          "UPSERT INTO %s (id, name, age, city, salary, department) VALUES (?, ?, ?, ?, ?, ?)",
          viewName2));
        // Same id/name as tenant1's row id=3 but a distinct age/city.
        upsert2.setInt(1, 3);
        upsert2.setString(2, "Name3");
        upsert2.setInt(3, 999);
        upsert2.setString(4, "CityZ");
        upsert2.setInt(5, 999000);
        upsert2.setString(6, "DeptZ");
        upsert2.executeUpdate();
        tenant2Conn.commit();

        try (ResultSet rs = tenant2Conn.createStatement().executeQuery(
          String.format("SELECT age, city FROM %s WHERE name = 'Name3'", viewName2))) {
          assertTrue(rs.next());
          assertEquals(999, rs.getInt(1));
          assertEquals("CityZ", rs.getString(2));
          assertFalse("tenant2 covered index must return exactly its own row", rs.next());
        }
      }

      // tenant1's covered-index query for the same name must still return only tenant1's own data.
      try (ResultSet rs = tenant1Conn.createStatement()
        .executeQuery(String.format("SELECT age, city FROM %s WHERE name = 'Name3'", viewName1))) {
        assertTrue(rs.next());
        assertEquals("tenant1 must not see tenant2's index entries", 23, rs.getInt(1));
        assertEquals("City3", rs.getString(2));
        assertFalse("tenant1 covered index must return exactly its own row", rs.next());
      }
    }
  }

  @Test
  public void testPartialIndexOnTenantView() throws Exception {
    String baseTableName = generateUniqueName();
    String tenant = generateUniqueName();
    String viewName = "PV_" + generateUniqueName();
    String partialIndex = "PI_" + generateUniqueName();
    String viewIndexPhysical = MetaDataUtil.VIEW_INDEX_TABLE_PREFIX + baseTableName;

    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(QueryServices.INDEX_USE_SERVER_METADATA_ATTRIB,
      Boolean.toString(useServerMetadata));
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      createMultiTenantBaseTable(conn, baseTableName);
    }

    try (Connection tenantConn = getTenantConnection(tenant)) {
      tenantConn.createStatement()
        .execute(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName, baseTableName));
      // Partial index on the leading key column (age), covering city, for rows with age >= 50.
      tenantConn.createStatement().execute(String.format(
        "CREATE INDEX %s ON %s (age) INCLUDE (city) WHERE age >= 50", partialIndex, viewName));

      PreparedStatement upsert = tenantConn.prepareStatement(String.format(
        "UPSERT INTO %s (id, name, age, city, salary, department) VALUES (?, ?, ?, ?, ?, ?)",
        viewName));
      // Rows id=1..6 with age=10*id: ids 5 and 6 match the predicate (age 50, 60); ids 1..4 do
      // not. Maintenance of the partial index runs on the server-metadata path for these upserts.
      for (int id = 1; id <= 6; id++) {
        upsert.setInt(1, id);
        upsert.setString(2, "Name" + id);
        upsert.setInt(3, 10 * id);
        upsert.setString(4, "City" + id);
        upsert.setInt(5, 1000 * id);
        upsert.setString(6, "Dept" + (id % 5));
        upsert.executeUpdate();
        if (singleRowUpdate) {
          tenantConn.commit();
        }
      }
      if (!singleRowUpdate) {
        tenantConn.commit();
      }

      String query =
        String.format("SELECT age, city FROM %s WHERE age >= 50 ORDER BY age", viewName);
      assertEquals("Partial view index should be used", viewIndexPhysical,
        tenantConn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class).optimizeQuery()
          .getExplainPlan().getPlanStepsAsAttributes().getTableName());
      try (ResultSet rs = tenantConn.createStatement().executeQuery(query)) {
        assertTrue(rs.next());
        assertEquals(50, rs.getInt(1));
        assertEquals("City5", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(60, rs.getInt(1));
        assertEquals("City6", rs.getString(2));
        assertFalse("Partial index must contain only predicate-matching rows", rs.next());
      }
    }
  }

  private void createMultiTenantBaseTable(Connection conn, String baseTableName)
    throws SQLException {
    conn.createStatement().execute(String.format(
      "CREATE TABLE %s (tenant_id VARCHAR NOT NULL, id INTEGER NOT NULL, name VARCHAR(50), "
        + "age INTEGER, city VARCHAR(50), salary INTEGER, department VARCHAR(50) "
        + "CONSTRAINT pk PRIMARY KEY (tenant_id, id)) MULTI_TENANT=true, UPDATE_CACHE_FREQUENCY=%s",
      baseTableName, updateCacheFrequency));
  }

  private Connection getTenantConnection(String tenantId) throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    props.setProperty(QueryServices.INDEX_USE_SERVER_METADATA_ATTRIB,
      Boolean.toString(useServerMetadata));
    return DriverManager.getConnection(getUrl(), props);
  }

  private static void upsertTenantViewRow(PreparedStatement upsert, int id) throws SQLException {
    upsert.setInt(1, id);
    upsert.setString(2, "Name" + id);
    upsert.setInt(3, 20 + id);
    upsert.setString(4, "City" + id);
    upsert.setInt(5, 1000 * id);
    upsert.setString(6, "Dept" + (id % 5));
    upsert.executeUpdate();
  }

  private void assertCoveredIndexRow(Connection conn, String viewName, String viewIndexPhysical,
    int id) throws SQLException {
    String query =
      String.format("SELECT name, age, city FROM %s WHERE name = 'Name%d'", viewName, id);
    assertEquals("Covered view index should be used", viewIndexPhysical,
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class).optimizeQuery()
        .getExplainPlan().getPlanStepsAsAttributes().getTableName());
    try (ResultSet rs = conn.createStatement().executeQuery(query)) {
      assertTrue(rs.next());
      assertEquals("Name" + id, rs.getString(1));
      assertEquals(20 + id, rs.getInt(2));
      assertEquals("City" + id, rs.getString(3));
      assertFalse("Covered index query must return exactly one row", rs.next());
    }
  }

  private void assertUncoveredIndexRow(Connection conn, String viewName, String viewIndexPhysical,
    int id) throws SQLException {
    String query =
      String.format("SELECT id, salary FROM %s WHERE salary = %d", viewName, 1000 * id);
    assertEquals("Uncovered view index should be used", viewIndexPhysical,
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class).optimizeQuery()
        .getExplainPlan().getPlanStepsAsAttributes().getTableName());
    try (ResultSet rs = conn.createStatement().executeQuery(query)) {
      assertTrue(rs.next());
      assertEquals(id, rs.getInt(1));
      assertEquals(1000 * id, rs.getInt(2));
      assertFalse("Uncovered index query must return exactly one row", rs.next());
    }
  }

  private void attachCustomCoprocessor(Connection conn, String dataTableName) throws Exception {
    TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
    TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TrackingMetaDataEndpointImpl.class);
    TestUtil.removeCoprocessor(conn, dataTableName, ServerCachingEndpointImpl.class);
    TestUtil.addCoprocessor(conn, dataTableName, TrackingServerCachingEndpointImpl.class);
  }

}
