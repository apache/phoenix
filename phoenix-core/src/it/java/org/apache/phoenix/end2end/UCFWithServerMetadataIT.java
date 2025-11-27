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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
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

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Test GetTable rpc calls for the combination of UCF and useServerMetadata.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class UCFWithServerMetadataIT extends BaseTest {

  private final String updateCacheFrequency;
  private final boolean useServerMetadata;
  private static final AtomicLong getTableCallCount = new AtomicLong(0);

  public UCFWithServerMetadataIT(String updateCacheFrequency, boolean useServerMetadata) {
    this.updateCacheFrequency = updateCacheFrequency;
    this.useServerMetadata = useServerMetadata;
  }

  @Parameters(name = "UpdateCacheFrequency={0}, UseServerMetadata={1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { "60000", true }, { "60000", false }, { "ALWAYS", true },
      { "ALWAYS", false } });
  }

  public static class TrackingMetaDataEndpointImpl extends MetaDataEndpointImpl {

    @Override
    public void getTable(RpcController controller, GetTableRequest request,
      RpcCallback<MetaDataResponse> done) {
      getTableCallCount.incrementAndGet();
      super.getTable(controller, request, done);
    }
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
    Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
    setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
      new ReadOnlyProps(clientProps.entrySet().iterator()));
  }

  @Before
  public void setUp() {
    getTableCallCount.set(0);
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
      conn.setAutoCommit(true);
      attachCustomCoprocessor(conn);
      String createTableDDL =
        String.format("CREATE TABLE %s (id INTEGER PRIMARY KEY, name VARCHAR(50), "
          + "age INTEGER, city VARCHAR(50), salary INTEGER, department VARCHAR(50)"
          + ") UPDATE_CACHE_FREQUENCY=%s", dataTableName, updateCacheFrequency);
      conn.createStatement().execute(createTableDDL);
      conn.createStatement().execute(String
        .format("CREATE INDEX %s ON %s (name) INCLUDE (age, city)", coveredIndex1, dataTableName));
      conn.createStatement().execute(String.format(
        "CREATE INDEX %s ON %s (city) INCLUDE (salary, department)", coveredIndex2, dataTableName));
      conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (age)", uncoveredIndex1, dataTableName));
      conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (salary)", uncoveredIndex2, dataTableName));
      long startGetTableCalls = getTableCallCount.get();
      String upsertSQL = String.format(
        "UPSERT INTO %s (id, name, age, city, salary, department) VALUES (?, ?, ?, ?, ?, ?)",
        dataTableName);
      PreparedStatement stmt = conn.prepareStatement(upsertSQL);
      for (int i = 1; i <= 50; i++) {
        stmt.setInt(1, i);
        stmt.setString(2, "Name" + i);
        stmt.setInt(3, 20 + (i % 40));
        stmt.setString(4, "City" + (i % 10));
        stmt.setInt(5, 30000 + (i * 1000));
        stmt.setString(6, "Dept" + (i % 5));
        stmt.executeUpdate();
      }
      testDataTableReads(conn, dataTableName);
      testCoveredIndexReads(conn, dataTableName, coveredIndex1, coveredIndex2);
      testUncoveredIndexReads(conn, dataTableName, uncoveredIndex1, uncoveredIndex2);
      testFullTableScan(conn, dataTableName);
      long finalGetTableCalls = getTableCallCount.get();
      long actualGetTableCalls = finalGetTableCalls - startGetTableCalls;
      int expectedCalls;
      String caseKey = updateCacheFrequency + "_" + useServerMetadata;
      switch (caseKey) {
        case "60000_true":
          expectedCalls = 1;
          break;
        case "60000_false":
          expectedCalls = 0;
          break;
        case "ALWAYS_true":
          expectedCalls = 232;
          break;
        case "ALWAYS_false":
          expectedCalls = 182;
          break;
        default:
          throw new IllegalArgumentException("Unexpected test case: " + caseKey);
      }
      assertEquals("Expected exact number of getTable() calls for case: " + caseKey, expectedCalls,
        actualGetTableCalls);
    }
  }

  private void testFullTableScan(Connection conn, String dataTableName) throws SQLException {
    String query = String.format("SELECT id, name, department FROM %s WHERE department = 'Dept1'",
      dataTableName);
    try (PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet rs = stmt.executeQuery()) {
      int count = 0;
      while (rs.next()) {
        count++;
        assertEquals("Dept1", rs.getString("department"));
      }
      assertEquals(10, count);
      ExplainPlan plan =
        stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
      ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
      assertEquals("Should use data table for full scan", dataTableName,
        planAttributes.getTableName());
      assertEquals("Should be a full scan", "FULL SCAN ", planAttributes.getExplainScanType());
    }
  }

  private void attachCustomCoprocessor(Connection conn) throws Exception {
    TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
    TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TrackingMetaDataEndpointImpl.class);
  }

  private void testDataTableReads(Connection conn, String dataTableName) throws SQLException {
    String query = String.format("SELECT * FROM %s WHERE id BETWEEN 10 AND 15", dataTableName);
    try (PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet rs = stmt.executeQuery()) {
      int count = 0;
      while (rs.next()) {
        count++;
        assertTrue(rs.getInt("id") >= 10 && rs.getInt("id") <= 15);
      }
      assertEquals(6, count);
      ExplainPlan plan =
        stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
      ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
      assertEquals("Should use data table", dataTableName, planAttributes.getTableName());
      assertEquals("Should be a range scan", "RANGE SCAN ", planAttributes.getExplainScanType());
    }
  }

  private void testCoveredIndexReads(Connection conn, String dataTableName, String coveredIndex1,
    String coveredIndex2) throws SQLException {
    String query =
      String.format("SELECT name, age, city FROM %s WHERE name = 'Name25'", dataTableName);
    try (PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet rs = stmt.executeQuery()) {
      assertTrue(rs.next());
      assertEquals("Name25", rs.getString("name"));
      assertEquals(45, rs.getInt("age"));
      assertEquals("City5", rs.getString("city"));
      ExplainPlan plan =
        stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
      ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
      assertEquals("Should use covered index", coveredIndex1, planAttributes.getTableName());
      assertEquals("Should be a range scan", "RANGE SCAN ", planAttributes.getExplainScanType());
    }
    query =
      String.format("SELECT city, salary, department FROM %s WHERE city = 'City3'", dataTableName);
    try (PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet rs = stmt.executeQuery()) {
      int count = 0;
      while (rs.next()) {
        count++;
        assertEquals("City3", rs.getString("city"));
        assertTrue(rs.getInt("salary") > 30000);
      }
      assertEquals(5, count);
      ExplainPlan plan =
        stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
      ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
      assertEquals("Should use covered index", coveredIndex2, planAttributes.getTableName());
      assertEquals("Should be a range scan", "RANGE SCAN ", planAttributes.getExplainScanType());
    }
  }

  private void testUncoveredIndexReads(Connection conn, String dataTableName,
    String uncoveredIndex1, String uncoveredIndex2) throws SQLException {
    String query = String.format("SELECT /*+ INDEX(%s %s) */ id, name, age FROM %s WHERE age = 35",
      dataTableName, uncoveredIndex1, dataTableName);
    try (PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet rs = stmt.executeQuery()) {
      int count = 0;
      while (rs.next()) {
        count++;
        assertEquals(35, rs.getInt("age"));
      }
      assertTrue(count > 0);
      ExplainPlan plan =
        stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
      ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
      assertEquals("Should use uncovered index", uncoveredIndex1, planAttributes.getTableName());
      assertEquals("Should be a range scan", "RANGE SCAN ", planAttributes.getExplainScanType());
    }
    query =
      String.format("SELECT /*+ INDEX(%s %s) */ id, name, salary FROM %s WHERE salary = 45000",
        dataTableName, uncoveredIndex2, dataTableName);
    try (PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet rs = stmt.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(45000, rs.getInt("salary"));
      assertEquals("Name15", rs.getString("name"));
      ExplainPlan plan =
        stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
      ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
      assertEquals("Should use uncovered index", uncoveredIndex2, planAttributes.getTableName());
      assertEquals("Should be a range scan", "RANGE SCAN ", planAttributes.getExplainScanType());
    }
  }

}
