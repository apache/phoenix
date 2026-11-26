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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.protobuf.text.AcmeWidgetsFixtures;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.Descriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ProtoValueIndexIT extends ParallelStatsDisabledIT {
  private static final String ACCOUNT = "com.example.acmewidgets.account.v1.Account";
  private static final String ENTITLEMENT = "com.example.acmewidgets.billing.v1.Entitlement";
  private static final String ORDER = "com.example.acmewidgets.orders.v1.Order";

  private static final List<String> BUNDLE_NAMES = Arrays.asList(ACCOUNT, ENTITLEMENT, ORDER);

  private static Map<String, FileDescriptor> descriptorGraph;

  protected final boolean columnEncoded;
  protected final boolean coveredIndex;

  protected String indexDDLOptions = "";

  public ProtoValueIndexIT(boolean columnEncoded, boolean coveredIndex) {
    this.columnEncoded = columnEncoded;
    this.coveredIndex = coveredIndex;
  }

  protected void waitForEventualConsistency() throws Exception {
  }

  @Parameterized.Parameters(name = "columnEncoded={0}, coveredIndex={1}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(
      new Object[][] { { false, false }, { false, true }, { true, false }, { true, true } });
  }

  @BeforeClass
  public static synchronized void loadDescriptors() throws Exception {
    descriptorGraph = AcmeWidgetsFixtures.loadAndBuildDescriptorGraph();
  }

  private final List<String> createdTables = new ArrayList<>();

  @After
  public void cleanupTablesAndDescriptors() throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps());
      Statement stmt = conn.createStatement()) {
      for (String name : createdTables) {
        try {
          stmt.execute("DROP TABLE IF EXISTS " + name);
        } catch (SQLException ignore) {
        }
      }
      for (String name : BUNDLE_NAMES) {
        try {
          stmt.execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + name);
        } catch (SQLException ignore) {
        }
      }
    }
    createdTables.clear();
  }

  @Test
  public void topLevelScalarStringIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR')"));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "tenant-alpha", "tenant-beta", "tenant-charlie", "tenant-delta",
        "tenant-echo", "tenant-foxtrot", "tenant-golf", "tenant-hotel" };
      for (int i = 0; i < tenants.length; i++) {
        upsertRow(conn, table, (long) (i + 1), "c1-" + tenants[i], AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "Name-" + i, i % 2 == 0, "wh-" + i).toByteArray());
      }

      assertIndexRowCount(conn, index, tenants.length);

      String pointSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setString(1, "tenant-echo");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(5L, rs.getLong(1));
          assertEquals("c1-tenant-echo", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeAscSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') >= ?"
          + "   AND PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') < ?"
          + " ORDER BY PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeAscSql)) {
        ps.setString(1, "tenant-b");
        ps.setString(2, "tenant-g");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 2L, 3L, 4L, 5L, 6L };
          String[] expectedC1 = { "c1-tenant-beta", "c1-tenant-charlie", "c1-tenant-delta",
            "c1-tenant-echo", "c1-tenant-foxtrot" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("expected row " + i, rs.next());
            assertEquals("row " + i + " id", expectedIds[i], rs.getLong(1));
            assertEquals("row " + i + " c1", expectedC1[i], rs.getString(2));
          }
          assertFalse("expected exactly 5 rows in range", rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeDescSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') >= ?"
          + " ORDER BY PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') DESC";
      try (PreparedStatement ps = conn.prepareStatement(rangeDescSql)) {
        ps.setString(1, "tenant-e");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 8L, 7L, 6L, 5L };
          for (long expected : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1));
          }
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void deeplyNestedInt64Index() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement().execute(
        indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'line_items[0].line_total.units', 'BIGINT')"));

      long[] amounts = { 10L, 20L, 30L, 40L, 50L, 60L, 70L };
      for (int i = 0; i < amounts.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-ord-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, amounts[i], 5L, 37.0d, 2, "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, amounts.length);

      String pointSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'line_items[0].line_total.units', 'BIGINT') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setLong(1, 40L);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertEquals("c1-ord-4", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'line_items[0].line_total.units', 'BIGINT') "
        + "BETWEEN ? AND ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, 'line_items[0].line_total.units', 'BIGINT') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setLong(1, 30L);
        ps.setLong(2, 60L);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 3L, 4L, 5L, 6L };
          String[] expectedC1 = { "c1-ord-3", "c1-ord-4", "c1-ord-5", "c1-ord-6" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse("expected exactly 4 rows in [30, 60]", rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void deeplyNestedDoubleIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement().execute(indexDdl(index, table,
        "PROTO_VALUE(PAYLOAD, 'customer.contact.addresses[0].geo.latitude', 'DOUBLE')"));

      double[] lats = { 34.0522d, 36.1699d, 37.7749d, 40.7128d, 41.8781d, 47.6062d, 51.5074d };
      for (int i = 0; i < lats.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-city-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, 5L, lats[i], 2, "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, lats.length);

      String pointSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, "
        + "'customer.contact.addresses[0].geo.latitude', 'DOUBLE') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setDouble(1, 40.7128d);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertEquals("c1-city-4", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeDescSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, "
        + "'customer.contact.addresses[0].geo.latitude', 'DOUBLE') > ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, "
        + "'customer.contact.addresses[0].geo.latitude', 'DOUBLE') DESC";
      try (PreparedStatement ps = conn.prepareStatement(rangeDescSql)) {
        ps.setDouble(1, 40.0d);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 7L, 6L, 5L, 4L };
          for (long expected : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1));
          }
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void mapValueBooleanIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ENTITLEMENT + ">)"
          + encodingClause());
      conn.createStatement().execute(indexDdl(index, table,
        "PROTO_VALUE(PAYLOAD, 'policy_toggles.allow_returns.enabled', 'BOOLEAN')"));

      boolean[] allowReturns = { true, true, true, false, false };
      for (int i = 0; i < allowReturns.length; i++) {
        long id = i + 1L;
        Map<String, Boolean> toggles = new LinkedHashMap<>();
        toggles.put("allow_returns", allowReturns[i]);
        upsertRow(conn, table, id, "c1-ent-" + id, buildEntitlement(toggles).toByteArray());
      }
      Map<String, Boolean> keyAbsent = new LinkedHashMap<>();
      keyAbsent.put("allow_refunds", true);
      upsertRow(conn, table, 6L, "c1-ent-6", buildEntitlement(keyAbsent).toByteArray());

      // An unset 'enabled' under a present map entry reads as false, so the false rows (4, 5)
      // are indexed; only the key-absent row (6) reads as SQL NULL and is left out.
      assertIndexRowCount(conn, index, 5);

      String trueSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'policy_toggles.allow_returns.enabled', 'BOOLEAN') = ?"
        + " ORDER BY ID";
      try (PreparedStatement ps = conn.prepareStatement(trueSql)) {
        ps.setBoolean(1, true);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expected = { 1L, 2L, 3L };
          String[] expectedC1 = { "c1-ent-1", "c1-ent-2", "c1-ent-3" };
          for (int i = 0; i < expected.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expected[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nestedInt32Index() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement().execute(
        indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'line_items[0].quantity', 'INTEGER')"));

      int[] quantities = { 1, 2, 3, 5, 8, 13 };
      for (int i = 0; i < quantities.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-qty-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, 5L, 37.0d, quantities[i], "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, quantities.length);

      String rangeSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'line_items[0].quantity', 'INTEGER') >= ?"
        + "   AND PROTO_VALUE(PAYLOAD, 'line_items[0].quantity', 'INTEGER') <= ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, 'line_items[0].quantity', 'INTEGER') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setInt(1, 2);
        ps.setInt(2, 8);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 2L, 3L, 4L, 5L };
          String[] expectedC1 = { "c1-qty-2", "c1-qty-3", "c1-qty-4", "c1-qty-5" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void deepRepeatedOfRepeatedInt64Index() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table,
          "PROTO_VALUE(PAYLOAD, 'line_items[0].modifiers[0].sub_modifiers[0].amount.units', "
            + "'BIGINT')"));

      long[] subAmounts = { 5L, 10L, 15L, 25L, 50L, 100L, 200L };
      for (int i = 0; i < subAmounts.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-sub-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, subAmounts[i], 37.0d, 2, "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, subAmounts.length);

      String rangeSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].modifiers[0].sub_modifiers[0].amount.units', 'BIGINT') >= ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].modifiers[0].sub_modifiers[0].amount.units', 'BIGINT') DESC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setLong(1, 25L);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 7L, 6L, 5L, 4L };
          for (long expected : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void veryDeepSelfReferenceIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement().execute(indexDdl(index, table,
        "PROTO_VALUE(PAYLOAD, 'line_items[0].category.parent.parent.name', 'VARCHAR')"));

      String[] categories =
        { "Apparel", "Books", "Electronics", "Furniture", "Grocery", "Hardware" };
      for (int i = 0; i < categories.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-cat-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, 5L, 37.0d, 2, "Sub", categories[i]).toByteArray());
      }

      assertIndexRowCount(conn, index, categories.length);

      String rangeSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].category.parent.parent.name', 'VARCHAR') >= ?"
        + "   AND PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].category.parent.parent.name', 'VARCHAR') < ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].category.parent.parent.name', 'VARCHAR') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setString(1, "B");
        ps.setString(2, "G");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 2L, 3L, 4L };
          String[] expectedC1 = { "c1-cat-2", "c1-cat-3", "c1-cat-4" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nestedMessageLeafAsVarbinaryEncodedIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());

      // VARBINARY_ENCODED (not raw VARBINARY): Phoenix disallows raw VARBINARY as
      // the leading segment of a multi-part row key.
      conn.createStatement().execute(indexDdl(index, table,
        "PROTO_VALUE(PAYLOAD, 'line_items[0].line_total', 'VARBINARY_ENCODED')"));

      upsertRow(conn, table, 1L, "c1-order-1", AcmeWidgetsFixtures
        .newOrderRichlyPopulated(descriptorGraph, "ord-1", "cust-1").toByteArray());

      assertIndexRowCount(conn, index, 1);
      String sql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].line_total', 'VARBINARY_ENCODED') IS NOT NULL";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1));
          assertEquals("c1-order-1", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void unsetScalarIsIndexedAsTypeZero() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      String expr = "PROTO_VALUE(PAYLOAD, 'active', 'BOOLEAN')";
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement().execute(nonPartialIndexDdl(index, table, expr));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      // proto3 does not serialize false, so rows 2 and 4 carry no 'active' field on the wire.
      boolean[] active = { true, false, true, false, true };
      for (int i = 0; i < active.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-" + id, AcmeWidgetsFixtures
          .newAccount(account, "tenant-" + id, "Name-" + id, active[i], "wh-" + id).toByteArray());
      }
      upsertRow(conn, table, 6L, "c1-6", null);

      // Index rows store a BOOLEAN key as DECIMAL, so false and true read back as 0 and 1.
      assertIndexRows(conn, index, new Object[][] { { null, 6L }, { "0", 2L }, { "0", 4L },
        { "1", 1L }, { "1", 3L }, { "1", 5L } });
      assertNullLookup(conn, table, index, expr, 6L);

      String falseSql = "SELECT ID, C1 FROM " + table + " WHERE " + expr + " = ? ORDER BY ID";
      try (PreparedStatement ps = conn.prepareStatement(falseSql)) {
        ps.setBoolean(1, false);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
          assertEquals("c1-2", rs.getString(2));
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertEquals("c1-4", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      // The index must agree with evaluating the expression over the data table.
      String allSql = " ID, " + expr + " FROM " + table + " ORDER BY ID";
      assertEquals(queryRows(conn, "SELECT /*+ NO_INDEX */" + allSql),
        queryRows(conn, "SELECT" + allSql));
      assertEquals(Arrays.asList("1:true", "2:false", "3:true", "4:false", "5:true", "6:null"),
        queryRows(conn, "SELECT /*+ NO_INDEX */" + allSql));

      // Updates move the index row between zero and non-zero without leaving the old entry.
      upsertRow(conn, table, 1L, "c1-1",
        AcmeWidgetsFixtures.newAccount(account, "tenant-1", "Name-1", false, "wh-1").toByteArray());
      upsertRow(conn, table, 2L, "c1-2",
        AcmeWidgetsFixtures.newAccount(account, "tenant-2", "Name-2", true, "wh-2").toByteArray());
      assertIndexRows(conn, index, new Object[][] { { null, 6L }, { "0", 1L }, { "0", 4L },
        { "1", 2L }, { "1", 3L }, { "1", 5L } });
      upsertRow(conn, table, 1L, "c1-1",
        AcmeWidgetsFixtures.newAccount(account, "tenant-1", "Name-1", true, "wh-1").toByteArray());
      assertIndexRows(conn, index, new Object[][] { { null, 6L }, { "0", 4L }, { "1", 1L },
        { "1", 2L }, { "1", 3L }, { "1", 5L } });
      try (PreparedStatement ps = conn.prepareStatement(falseSql)) {
        ps.setBoolean(1, false);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
      assertEquals(queryRows(conn, "SELECT /*+ NO_INDEX */" + allSql),
        queryRows(conn, "SELECT" + allSql));
    }
  }

  @Test
  public void indexOnExpressionWithDefaultIndexesAbsentFieldAsDefault() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      String expr = "PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR', 'none')";
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      // Index maintenance evaluates the expression, default included, on the region server.
      conn.createStatement().execute(indexDdl(index, table, expr));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] warehouses = { "wh-1", null, "wh-3", null, "wh-5", "none" };
      for (int i = 0; i < warehouses.length; i++) {
        long id = i + 1L;
        DynamicMessage.Builder b = DynamicMessage.newBuilder(account)
          .setField(account.findFieldByName("tenant_key"), "tenant-" + id);
        if (warehouses[i] != null) {
          b.setField(account.findFieldByName("home_warehouse"), warehouses[i]);
        }
        upsertRow(conn, table, id, "c1-" + id, b.build().toByteArray());
      }

      assertIndexRows(conn, index, new Object[][] { { "none", 2L }, { "none", 4L }, { "none", 6L },
        { "wh-1", 1L }, { "wh-3", 3L }, { "wh-5", 5L } });

      String sql = "SELECT ID, C1 FROM " + table + " WHERE " + expr + " = ? ORDER BY ID";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "none");
        try (ResultSet rs = ps.executeQuery()) {
          for (long id : new long[] { 2L, 4L, 6L }) {
            assertTrue(rs.next());
            assertEquals(id, rs.getLong(1));
            assertEquals("c1-" + id, rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void uint64AsDecimalIndexOrdersByUnsignedValue() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      String expr = "PROTO_VALUE(PAYLOAD, 'version.version', 'DECIMAL')";
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ENTITLEMENT + ">)"
          + encodingClause());
      conn.createStatement().execute(indexDdl(index, table, expr));

      Map<String, Boolean> toggles = new LinkedHashMap<>();
      toggles.put("allow_returns", true);
      // Raw signed bits: Long.MIN_VALUE is 2^63 and -1 is 2^64 - 1 when read as uint64.
      long[] versions = { -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE + 1, 42L };
      for (int i = 0; i < versions.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-ent-" + id,
          AcmeWidgetsFixtures
            .newEntitlement(AcmeWidgetsFixtures.entitlementDescriptor(descriptorGraph),
              AcmeWidgetsFixtures.entitlementKeyDescriptor(descriptorGraph),
              AcmeWidgetsFixtures.entitlementVersionDescriptor(descriptorGraph),
              AcmeWidgetsFixtures.policyToggleDescriptor(descriptorGraph), "tenant-" + id,
              "ent-name", "ent-id", versions[i], toggles)
            .toByteArray());
      }
      assertIndexRowCount(conn, index, versions.length);

      String rangeSql =
        "SELECT ID, " + expr + " FROM " + table + " WHERE " + expr + " >= ? ORDER BY " + expr;
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setBigDecimal(1, new BigDecimal(Long.MAX_VALUE));
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 4L, 3L, 5L, 1L };
          String[] expectedValues = { "9223372036854775807", "9223372036854775808",
            "9223372036854775809", "18446744073709551615" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(new BigDecimal(expectedValues[i]), rs.getBigDecimal(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String descSql =
        "SELECT ID FROM " + table + " WHERE " + expr + " < ? ORDER BY " + expr + " DESC";
      try (PreparedStatement ps = conn.prepareStatement(descSql)) {
        ps.setBigDecimal(1, new BigDecimal("9223372036854775808"));
        try (ResultSet rs = ps.executeQuery()) {
          for (long id : new long[] { 4L, 6L, 2L }) {
            assertTrue(rs.next());
            assertEquals(id, rs.getLong(1));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nonPartialIndexIncludesAllRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(nonPartialIndexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR')"));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      upsertRow(conn, table, 1L, "c1-a",
        AcmeWidgetsFixtures.newAccount(account, "tenant-a", "A", true, "wh").toByteArray());
      upsertRow(conn, table, 2L, "c1-b",
        AcmeWidgetsFixtures.newAccount(account, "tenant-b", "B", true, "wh").toByteArray());

      upsertRow(conn, table, 3L, "c1-c", DynamicMessage.newBuilder(account)
        .setField(account.findFieldByName("display_name"), "C").build().toByteArray());

      assertIndexRowCount(conn, index, 3);
      assertIndexRows(conn, index,
        new Object[][] { { null, 3L }, { "tenant-a", 1L }, { "tenant-b", 2L } });
      assertNullLookup(conn, table, index, "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR')", 3L);
      String sql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "tenant-b");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
          assertEquals("c1-b", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nonPartialIndexOnNestedLeafWithMissingParent() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      String expr = "PROTO_VALUE(PAYLOAD, 'key.tenant_key', 'VARCHAR')";
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ENTITLEMENT + ">)"
          + encodingClause());
      conn.createStatement().execute(nonPartialIndexDdl(index, table, expr));
      upsertEntitlementsWithAndWithoutKey(conn, table);

      assertIndexRows(conn, index,
        new Object[][] { { null, 2L }, { null, 3L }, { "tenant-idx", 1L } });
      assertNullLookup(conn, table, index, expr, 2L, 3L);
      try (PreparedStatement ps =
        conn.prepareStatement("SELECT ID FROM " + table + " WHERE " + expr + " = ?")) {
        ps.setString(1, "tenant-idx");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void coveredIndexIncludingProtobufColumnReturnsPayloadFromIndex() throws Exception {
    Assume.assumeTrue("INCLUDE(PAYLOAD) only applies to covered indexes", this.coveredIndex);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute("CREATE INDEX " + index + " ON " + table
          + "(PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR')) INCLUDE(PAYLOAD) WHERE "
          + "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') IS NOT NULL" + indexDDLOptions);

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      byte[] alpha = AcmeWidgetsFixtures
        .newAccount(account, "alpha-tenant", "Alpha", true, "wh-north").toByteArray();
      byte[] beta = AcmeWidgetsFixtures.newAccount(account, "beta-tenant", "Beta", true, "wh-south")
        .toByteArray();
      upsertRow(conn, table, 1L, "c1-a", alpha);
      upsertRow(conn, table, 2L, "c1-b", beta);

      assertIndexRowCount(conn, index, 2);
      String sql = "SELECT ID, PAYLOAD FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "beta-tenant");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
          assertArrayEquals(beta, rs.getBytes(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void hexLiteralUpsertMaintainsIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR')"));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      byte[] alpha = AcmeWidgetsFixtures.newAccount(account, "tenant-alpha", "Alpha", true, "wh-1")
        .toByteArray();
      byte[] beta =
        AcmeWidgetsFixtures.newAccount(account, "tenant-beta", "Beta", false, "wh-2").toByteArray();
      byte[] charlie = AcmeWidgetsFixtures
        .newAccount(account, "tenant-charlie", "Charlie", true, "wh-3").toByteArray();
      upsertRowWithHexLiteral(conn, table, 1L, "c1-alpha", alpha);
      upsertRowWithHexLiteral(conn, table, 2L, "c1-beta", beta);
      upsertRowWithHexLiteral(conn, table, 3L, "c1-charlie", charlie);

      assertIndexRowCount(conn, index, 3);
      assertPayloadBytes(conn, table, 1L, alpha);
      assertPayloadBytes(conn, table, 2L, beta);
      assertPayloadBytes(conn, table, 3L, charlie);
      assertTenantLookup(conn, table, index, "tenant-beta", 2L, "c1-beta");

      // Overwrite row 2 via hex literal with a different tenant_key: the old index entry
      // must be removed and the new one must be reachable through the index.
      byte[] zulu =
        AcmeWidgetsFixtures.newAccount(account, "tenant-zulu", "Zulu", true, "wh-9").toByteArray();
      upsertRowWithHexLiteral(conn, table, 2L, "c1-zulu", zulu);

      assertIndexRowCount(conn, index, 3);
      assertPayloadBytes(conn, table, 2L, zulu);
      assertTenantLookup(conn, table, index, "tenant-beta", null, null);
      assertTenantLookup(conn, table, index, "tenant-zulu", 2L, "c1-zulu");

      // X'' is an empty byte literal, which Phoenix treats as NULL: the payload is cleared
      // and the row drops out of the partial index.
      conn.createStatement()
        .execute("UPSERT INTO " + table + " (ID, C1, PAYLOAD) VALUES (3, 'c1-cleared', X'')");
      conn.commit();

      assertIndexRowCount(conn, index, 2);
      assertPayloadBytes(conn, table, 3L, null);
      assertTenantLookup(conn, table, index, "tenant-charlie", null, null);
      assertTenantLookup(conn, table, index, "tenant-alpha", 1L, "c1-alpha");
    }
  }

  @Test
  public void hexLiteralUpsertCoveredIndexIncludingProtobufColumn() throws Exception {
    Assume.assumeTrue("INCLUDE(PAYLOAD) only applies to covered indexes", this.coveredIndex);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute("CREATE INDEX " + index + " ON " + table
          + "(PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR')) INCLUDE(PAYLOAD) WHERE "
          + "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') IS NOT NULL" + indexDDLOptions);

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      byte[] alpha = AcmeWidgetsFixtures
        .newAccount(account, "alpha-tenant", "Alpha", true, "wh-north").toByteArray();
      byte[] beta = AcmeWidgetsFixtures.newAccount(account, "beta-tenant", "Beta", true, "wh-south")
        .toByteArray();
      upsertRowWithHexLiteral(conn, table, 1L, "c1-a", alpha);
      upsertRowWithHexLiteral(conn, table, 2L, "c1-b", beta);

      assertIndexRowCount(conn, index, 2);
      String sql = "SELECT ID, PAYLOAD, PROTO_VALUE(PAYLOAD, 'display_name', 'VARCHAR') FROM "
        + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "beta-tenant");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
          assertArrayEquals(beta, rs.getBytes(2));
          assertEquals("Beta", rs.getString(3));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void multiColumnFunctionalIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(multiColumnIndexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR')",
          "PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR')"));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "tenant-a", "tenant-a", "tenant-a", "tenant-b", "tenant-b", "tenant-b",
        "tenant-c", "tenant-c", "tenant-d", "tenant-d" };
      String[] warehouses = { "wh-north", "wh-south", "wh-east", "wh-north", "wh-south", "wh-west",
        "wh-north", "wh-east", "wh-north", "wh-south" };
      for (int i = 0; i < tenants.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-" + tenants[i] + "-" + warehouses[i], AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "Name-" + i, true, warehouses[i]).toByteArray());
      }

      assertIndexRowCount(conn, index, tenants.length);

      String pointSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') = ?"
          + " AND PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setString(1, "tenant-b");
        ps.setString(2, "wh-north");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertEquals("c1-tenant-b-wh-north", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      // Compound-tenant scan + ORDER BY on the secondary indexed column: Phoenix
      // does not use the secondary index for ORDER BY on the second key, so this
      // path scans the base table. Assert rows and ORDER BY semantics anyway.
      String tenantScanSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') = ?"
          + " ORDER BY PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR') ASC";
      try (PreparedStatement ps = conn.prepareStatement(tenantScanSql)) {
        ps.setString(1, "tenant-a");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 3L, 1L, 2L };
          String[] expectedC1 =
            { "c1-tenant-a-wh-east", "c1-tenant-a-wh-north", "c1-tenant-a-wh-south" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
      }

      String warehouseCountSql = "SELECT PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR'), "
        + "COUNT(*) FROM " + table + " GROUP BY PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR')"
        + " ORDER BY PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR') ASC";
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(warehouseCountSql)) {
        String[] expectedWh = { "wh-east", "wh-north", "wh-south", "wh-west" };
        int[] expectedCounts = { 2, 4, 3, 1 };
        for (int i = 0; i < expectedWh.length; i++) {
          assertTrue(rs.next());
          assertEquals(expectedWh[i], rs.getString(1));
          assertEquals(expectedCounts[i], rs.getInt(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void topLevelScalarStringIndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR')"));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "tenant-alpha", "tenant-beta", "tenant-charlie", "tenant-delta",
        "tenant-echo", "tenant-foxtrot", "tenant-golf", "tenant-hotel" };
      for (int i = 0; i < tenants.length; i++) {
        upsertRow(conn, table, (long) (i + 1), "c1-" + tenants[i], AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "Name-" + i, i % 2 == 0, "wh-" + i).toByteArray());
      }

      assertIndexRowCount(conn, index, tenants.length);

      String pointSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setString(1, "tenant-echo");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(5L, rs.getLong(1));
          assertEquals("c1-tenant-echo", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeAscSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') >= ?"
          + "   AND PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') < ?"
          + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeAscSql)) {
        ps.setString(1, "tenant-b");
        ps.setString(2, "tenant-g");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 2L, 3L, 4L, 5L, 6L };
          String[] expectedC1 = { "c1-tenant-beta", "c1-tenant-charlie", "c1-tenant-delta",
            "c1-tenant-echo", "c1-tenant-foxtrot" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("expected row " + i, rs.next());
            assertEquals("row " + i + " id", expectedIds[i], rs.getLong(1));
            assertEquals("row " + i + " c1", expectedC1[i], rs.getString(2));
          }
          assertFalse("expected exactly 5 rows in range", rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeDescSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') >= ?"
          + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') DESC";
      try (PreparedStatement ps = conn.prepareStatement(rangeDescSql)) {
        ps.setString(1, "tenant-e");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 8L, 7L, 6L, 5L };
          for (long expected : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1));
          }
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void deeplyNestedInt64IndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].6.2', 'BIGINT')"));

      long[] amounts = { 10L, 20L, 30L, 40L, 50L, 60L, 70L };
      for (int i = 0; i < amounts.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-ord-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, amounts[i], 5L, 37.0d, 2, "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, amounts.length);

      String pointSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].6.2', 'BIGINT') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setLong(1, 40L);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertEquals("c1-ord-4", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].6.2', 'BIGINT') "
          + "BETWEEN ? AND ?" + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].6.2', 'BIGINT') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setLong(1, 30L);
        ps.setLong(2, 60L);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 3L, 4L, 5L, 6L };
          String[] expectedC1 = { "c1-ord-3", "c1-ord-4", "c1-ord-5", "c1-ord-6" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse("expected exactly 4 rows in [30, 60]", rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void deeplyNestedDoubleIndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '2.3.3[0].6.1', 'DOUBLE')"));

      double[] lats = { 34.0522d, 36.1699d, 37.7749d, 40.7128d, 41.8781d, 47.6062d, 51.5074d };
      for (int i = 0; i < lats.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-city-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, 5L, lats[i], 2, "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, lats.length);

      String pointSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'2.3.3[0].6.1', 'DOUBLE') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setDouble(1, 40.7128d);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertEquals("c1-city-4", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      String rangeDescSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'2.3.3[0].6.1', 'DOUBLE') > ?" + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'2.3.3[0].6.1', 'DOUBLE') DESC";
      try (PreparedStatement ps = conn.prepareStatement(rangeDescSql)) {
        ps.setDouble(1, 40.0d);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 7L, 6L, 5L, 4L };
          for (long expected : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1));
          }
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void mapValueBooleanIndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ENTITLEMENT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '5[0].2.2', 'BOOLEAN')"));

      boolean[] allowReturns = { true, true, true, false, false };
      for (int i = 0; i < allowReturns.length; i++) {
        long id = i + 1L;
        Map<String, Boolean> toggles = new LinkedHashMap<>();
        toggles.put("allow_returns", allowReturns[i]);
        upsertRow(conn, table, id, "c1-ent-" + id, buildEntitlement(toggles).toByteArray());
      }
      upsertRow(conn, table, 6L, "c1-ent-6",
        buildEntitlement(new LinkedHashMap<String, Boolean>()).toByteArray());

      // Same BOOLEAN false vs NULL partial-index gap as mapValueBooleanIndex.
      // Row 6 is an empty map: tag path '5[0].2.2' would also match allow_refunds.
      assertIndexRowCount(conn, index, 3);

      String trueSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '5[0].2.2', 'BOOLEAN') = ?" + " ORDER BY ID";
      try (PreparedStatement ps = conn.prepareStatement(trueSql)) {
        ps.setBoolean(1, true);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expected = { 1L, 2L, 3L };
          String[] expectedC1 = { "c1-ent-1", "c1-ent-2", "c1-ent-3" };
          for (int i = 0; i < expected.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expected[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nestedInt32IndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].4', 'INTEGER')"));

      int[] quantities = { 1, 2, 3, 5, 8, 13 };
      for (int i = 0; i < quantities.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-qty-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, 5L, 37.0d, quantities[i], "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, quantities.length);

      String rangeSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].4', 'INTEGER') >= ?"
        + "   AND PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].4', 'INTEGER') <= ?"
        + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].4', 'INTEGER') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setInt(1, 2);
        ps.setInt(2, 8);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 2L, 3L, 4L, 5L };
          String[] expectedC1 = { "c1-qty-2", "c1-qty-3", "c1-qty-4", "c1-qty-5" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void deepRepeatedOfRepeatedInt64IndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement().execute(
        indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].8[0].5[0].4.2', " + "'BIGINT')"));

      long[] subAmounts = { 5L, 10L, 15L, 25L, 50L, 100L, 200L };
      for (int i = 0; i < subAmounts.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-sub-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, subAmounts[i], 37.0d, 2, "Sub", "Root").toByteArray());
      }

      assertIndexRowCount(conn, index, subAmounts.length);

      String rangeSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'3[0].8[0].5[0].4.2', 'BIGINT') >= ?" + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'3[0].8[0].5[0].4.2', 'BIGINT') DESC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setLong(1, 25L);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 7L, 6L, 5L, 4L };
          for (long expected : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void veryDeepSelfReferenceIndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].3.3.3.2', 'VARCHAR')"));

      String[] categories =
        { "Apparel", "Books", "Electronics", "Furniture", "Grocery", "Hardware" };
      for (int i = 0; i < categories.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-cat-" + id,
          AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph, "ord-" + id,
            "cust-" + id, 100L, 5L, 37.0d, 2, "Sub", categories[i]).toByteArray());
      }

      assertIndexRowCount(conn, index, categories.length);

      String rangeSql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'3[0].3.3.3.2', 'VARCHAR') >= ?" + "   AND PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'3[0].3.3.3.2', 'VARCHAR') < ?" + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'3[0].3.3.3.2', 'VARCHAR') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setString(1, "B");
        ps.setString(2, "G");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 2L, 3L, 4L };
          String[] expectedC1 = { "c1-cat-2", "c1-cat-3", "c1-cat-4" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nestedMessageLeafAsVarbinaryEncodedIndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ORDER + ">)"
          + encodingClause());

      // VARBINARY_ENCODED (not raw VARBINARY): Phoenix disallows raw VARBINARY as
      // the leading segment of a multi-part row key.
      conn.createStatement().execute(
        indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '3[0].6', 'VARBINARY_ENCODED')"));

      upsertRow(conn, table, 1L, "c1-order-1", AcmeWidgetsFixtures
        .newOrderRichlyPopulated(descriptorGraph, "ord-1", "cust-1").toByteArray());

      assertIndexRowCount(conn, index, 1);
      String sql = "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, "
        + "'3[0].6', 'VARBINARY_ENCODED') IS NOT NULL";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1));
          assertEquals("c1-order-1", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void uint64AsBigIntIndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ENTITLEMENT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '2.1', 'BIGINT')"));

      Map<String, Boolean> toggles = new LinkedHashMap<>();
      toggles.put("allow_returns", true);
      long[] versions = { 100L, 150L, 200L, 300L, 500L, 800L };
      for (int i = 0; i < versions.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-ent-" + id,
          AcmeWidgetsFixtures
            .newEntitlement(AcmeWidgetsFixtures.entitlementDescriptor(descriptorGraph),
              AcmeWidgetsFixtures.entitlementKeyDescriptor(descriptorGraph),
              AcmeWidgetsFixtures.entitlementVersionDescriptor(descriptorGraph),
              AcmeWidgetsFixtures.policyToggleDescriptor(descriptorGraph), "tenant-" + id,
              "ent-name", "ent-id", versions[i], toggles)
            .toByteArray());
      }

      assertIndexRowCount(conn, index, versions.length);

      String rangeSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '2.1', 'BIGINT') > ?"
          + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, '2.1', 'BIGINT') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setLong(1, 200L);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 4L, 5L, 6L };
          String[] expectedC1 = { "c1-ent-4", "c1-ent-5", "c1-ent-6" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nonPartialIndexIncludesAllRowsAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(nonPartialIndexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR')"));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      upsertRow(conn, table, 1L, "c1-a",
        AcmeWidgetsFixtures.newAccount(account, "tenant-a", "A", true, "wh").toByteArray());
      upsertRow(conn, table, 2L, "c1-b",
        AcmeWidgetsFixtures.newAccount(account, "tenant-b", "B", true, "wh").toByteArray());

      upsertRow(conn, table, 3L, "c1-c", DynamicMessage.newBuilder(account)
        .setField(account.findFieldByName("display_name"), "C").build().toByteArray());

      assertIndexRowCount(conn, index, 3);
      assertIndexRows(conn, index,
        new Object[][] { { null, 3L }, { "tenant-a", 1L }, { "tenant-b", 2L } });
      assertNullLookup(conn, table, index, "PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR')", 3L);
      String sql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "tenant-b");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
          assertEquals("c1-b", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void nonPartialIndexOnNestedLeafWithMissingParentAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      // key = 1, key.tenant_key = 2
      String expr = "PROTO_VALUE_AT_TAG(PAYLOAD, '1.2', 'VARCHAR')";
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ENTITLEMENT + ">)"
          + encodingClause());
      conn.createStatement().execute(nonPartialIndexDdl(index, table, expr));
      upsertEntitlementsWithAndWithoutKey(conn, table);

      assertIndexRows(conn, index,
        new Object[][] { { null, 2L }, { null, 3L }, { "tenant-idx", 1L } });
      assertNullLookup(conn, table, index, expr, 2L, 3L);
      try (PreparedStatement ps =
        conn.prepareStatement("SELECT ID FROM " + table + " WHERE " + expr + " = ?")) {
        ps.setString(1, "tenant-idx");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void coveredIndexIncludingProtobufColumnReturnsPayloadFromIndexAtTag() throws Exception {
    Assume.assumeTrue("INCLUDE(PAYLOAD) only applies to covered indexes", this.coveredIndex);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute("CREATE INDEX " + index + " ON " + table
          + "(PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR')) INCLUDE(PAYLOAD) WHERE "
          + "PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') IS NOT NULL" + indexDDLOptions);

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      byte[] alpha = AcmeWidgetsFixtures
        .newAccount(account, "alpha-tenant", "Alpha", true, "wh-north").toByteArray();
      byte[] beta = AcmeWidgetsFixtures.newAccount(account, "beta-tenant", "Beta", true, "wh-south")
        .toByteArray();
      upsertRow(conn, table, 1L, "c1-a", alpha);
      upsertRow(conn, table, 2L, "c1-b", beta);

      assertIndexRowCount(conn, index, 2);
      String sql = "SELECT ID, PAYLOAD FROM " + table
        + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "beta-tenant");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
          assertArrayEquals(beta, rs.getBytes(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }
    }
  }

  @Test
  public void multiColumnFunctionalIndexAtTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      String index = "IDX_" + table;
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      conn.createStatement()
        .execute(multiColumnIndexDdl(index, table, "PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR')",
          "PROTO_VALUE_AT_TAG(PAYLOAD, '9', 'VARCHAR')"));

      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "tenant-a", "tenant-a", "tenant-a", "tenant-b", "tenant-b", "tenant-b",
        "tenant-c", "tenant-c", "tenant-d", "tenant-d" };
      String[] warehouses = { "wh-north", "wh-south", "wh-east", "wh-north", "wh-south", "wh-west",
        "wh-north", "wh-east", "wh-north", "wh-south" };
      for (int i = 0; i < tenants.length; i++) {
        long id = i + 1L;
        upsertRow(conn, table, id, "c1-" + tenants[i] + "-" + warehouses[i], AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "Name-" + i, true, warehouses[i]).toByteArray());
      }

      assertIndexRowCount(conn, index, tenants.length);

      String pointSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') = ?"
          + " AND PROTO_VALUE_AT_TAG(PAYLOAD, '9', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setString(1, "tenant-b");
        ps.setString(2, "wh-north");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertEquals("c1-tenant-b-wh-north", rs.getString(2));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index, "RANGE SCAN");
      }

      // Compound-tenant scan + ORDER BY on the secondary indexed column: Phoenix
      // does not use the secondary index for ORDER BY on the second key, so this
      // path scans the base table. Assert rows and ORDER BY semantics anyway.
      String tenantScanSql =
        "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE_AT_TAG(PAYLOAD, '1', 'VARCHAR') = ?"
          + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, '9', 'VARCHAR') ASC";
      try (PreparedStatement ps = conn.prepareStatement(tenantScanSql)) {
        ps.setString(1, "tenant-a");
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 3L, 1L, 2L };
          String[] expectedC1 =
            { "c1-tenant-a-wh-east", "c1-tenant-a-wh-north", "c1-tenant-a-wh-south" };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedC1[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
      }

      String warehouseCountSql = "SELECT PROTO_VALUE_AT_TAG(PAYLOAD, '9', 'VARCHAR'), "
        + "COUNT(*) FROM " + table + " GROUP BY PROTO_VALUE_AT_TAG(PAYLOAD, '9', 'VARCHAR')"
        + " ORDER BY PROTO_VALUE_AT_TAG(PAYLOAD, '9', 'VARCHAR') ASC";
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(warehouseCountSql)) {
        String[] expectedWh = { "wh-east", "wh-north", "wh-south", "wh-west" };
        int[] expectedCounts = { 2, 4, 3, 1 };
        for (int i = 0; i < expectedWh.length; i++) {
          assertTrue(rs.next());
          assertEquals(expectedWh[i], rs.getString(1));
          assertEquals(expectedCounts[i], rs.getInt(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  private Properties testProps() {
    return PropertiesUtil.deepCopy(TEST_PROPERTIES);
  }

  private String encodingClause() {
    return this.columnEncoded ? " COLUMN_ENCODED_BYTES=2" : " COLUMN_ENCODED_BYTES=0";
  }

  private String uniqueTableName() {
    String name = generateUniqueName();
    createdTables.add(name);
    return name;
  }

  private String indexDdl(String indexName, String tableName, String indexedExpr) {
    if (this.coveredIndex) {
      return "CREATE INDEX " + indexName + " ON " + tableName + "(" + indexedExpr
        + ") INCLUDE(C1) WHERE " + indexedExpr + " IS NOT NULL" + indexDDLOptions;
    }
    return "CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + "(" + indexedExpr
      + ") WHERE " + indexedExpr + " IS NOT NULL" + indexDDLOptions;
  }

  private String nonPartialIndexDdl(String indexName, String tableName, String indexedExpr) {
    if (this.coveredIndex) {
      return "CREATE INDEX " + indexName + " ON " + tableName + "(" + indexedExpr + ") INCLUDE(C1)"
        + indexDDLOptions;
    }
    return "CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + "(" + indexedExpr + ")"
      + indexDDLOptions;
  }

  private String multiColumnIndexDdl(String indexName, String tableName, String firstExpr,
    String secondExpr) {
    if (this.coveredIndex) {
      return "CREATE INDEX " + indexName + " ON " + tableName + "(" + firstExpr + ", " + secondExpr
        + ") INCLUDE(C1) WHERE " + firstExpr + " IS NOT NULL AND " + secondExpr + " IS NOT NULL"
        + indexDDLOptions;
    }
    return "CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + "(" + firstExpr + ", "
      + secondExpr + ") WHERE " + firstExpr + " IS NOT NULL AND " + secondExpr + " IS NOT NULL"
      + indexDDLOptions;
  }

  private void registerBundleIdempotent(Connection conn) throws SQLException {
    StringBuilder ddl = new StringBuilder("CREATE PROTOBUF BUNDLE IF NOT EXISTS (");
    for (int i = 0; i < BUNDLE_NAMES.size(); i++) {
      if (i > 0) {
        ddl.append(", ");
      }
      ddl.append(BUNDLE_NAMES.get(i));
    }
    ddl.append(") FROM '").append(AcmeWidgetsFixtures.bundleFileUrl()).append("'");
    conn.createStatement().execute(ddl.toString());
  }

  private DynamicMessage buildEntitlement(Map<String, Boolean> toggles) {
    return AcmeWidgetsFixtures.newEntitlement(
      AcmeWidgetsFixtures.entitlementDescriptor(descriptorGraph),
      AcmeWidgetsFixtures.entitlementKeyDescriptor(descriptorGraph),
      AcmeWidgetsFixtures.entitlementVersionDescriptor(descriptorGraph),
      AcmeWidgetsFixtures.policyToggleDescriptor(descriptorGraph), "tenant-idx",
      "unlimited-widgets", "ent-xyz", 1L, toggles);
  }

  private void upsertEntitlementsWithAndWithoutKey(Connection conn, String table)
    throws SQLException {
    Descriptor entitlement = AcmeWidgetsFixtures.entitlementDescriptor(descriptorGraph);
    Descriptor entitlementKey = AcmeWidgetsFixtures.entitlementKeyDescriptor(descriptorGraph);
    upsertRow(conn, table, 1L, "c1-1", buildEntitlement(Collections.emptyMap()).toByteArray());
    DynamicMessage keyWithoutTenant = DynamicMessage.newBuilder(entitlementKey)
      .setField(entitlementKey.findFieldByName("entitlement_name"), "no-tenant").build();
    upsertRow(conn, table, 2L, "c1-2", DynamicMessage.newBuilder(entitlement)
      .setField(entitlement.findFieldByName("key"), keyWithoutTenant).build().toByteArray());
    upsertRow(conn, table, 3L, "c1-3", DynamicMessage.newBuilder(entitlement)
      .setField(entitlement.findFieldByName("source_warehouse"), "wh-1").build().toByteArray());
  }

  private static void upsertRow(Connection conn, String table, long id, String c1, byte[] payload)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, C1, PAYLOAD) VALUES (?, ?, ?)")) {
      ps.setLong(1, id);
      ps.setString(2, c1);
      ps.setBytes(3, payload);
      ps.execute();
    }
    conn.commit();
  }

  private static void upsertRowWithHexLiteral(Connection conn, String table, long id, String c1,
    byte[] payload) throws SQLException {
    conn.createStatement().execute("UPSERT INTO " + table + " (ID, C1, PAYLOAD) VALUES (" + id
      + ", '" + c1 + "', X'" + Bytes.toHex(payload) + "')");
    conn.commit();
  }

  private static void assertPayloadBytes(Connection conn, String table, long id, byte[] expected)
    throws SQLException {
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT PAYLOAD FROM " + table + " WHERE ID = " + id)) {
      assertTrue(rs.next());
      assertArrayEquals(expected, rs.getBytes(1));
      assertFalse(rs.next());
    }
  }

  private void assertTenantLookup(Connection conn, String table, String indexName, String tenantKey,
    Long expectedId, String expectedC1) throws Exception {
    waitForEventualConsistency();
    String sql =
      "SELECT ID, C1 FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, tenantKey);
      try (ResultSet rs = ps.executeQuery()) {
        if (expectedId != null) {
          assertTrue(rs.next());
          assertEquals(expectedId.longValue(), rs.getLong(1));
          assertEquals(expectedC1, rs.getString(2));
        }
        assertFalse(rs.next());
      }
      validateExplainPlan(ps, indexName, "RANGE SCAN");
    }
  }

  private void assertIndexRowCount(Connection conn, String indexName, int expected)
    throws Exception {
    waitForEventualConsistency();
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + indexName)) {
      assertTrue(rs.next());
      assertEquals(expected, rs.getInt(1));
    }
  }

  private void assertIndexRows(Connection conn, String indexName, Object[][] expected)
    throws Exception {
    waitForEventualConsistency();
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + indexName)) {
      ResultSetMetaData md = rs.getMetaData();
      int valueCol = -1;
      int idCol = -1;
      for (int c = 1; c <= md.getColumnCount(); c++) {
        String label = md.getColumnLabel(c);
        if (label.contains("PROTO_VALUE")) {
          valueCol = c;
        } else if (label.equals(":ID")) {
          idCol = c;
        }
      }
      assertTrue("index value and ID columns not found", valueCol > 0 && idCol > 0);
      for (int i = 0; i < expected.length; i++) {
        assertTrue("missing index row " + i, rs.next());
        assertEquals("index row " + i + " value", expected[i][0], rs.getString(valueCol));
        assertEquals("index row " + i + " id", expected[i][1], rs.getLong(idCol));
      }
      assertFalse(rs.next());
    }
  }

  private void assertNullLookup(Connection conn, String table, String indexName, String indexedExpr,
    long... expectedIds) throws Exception {
    waitForEventualConsistency();
    try (PreparedStatement ps =
      conn.prepareStatement("SELECT ID FROM " + table + " WHERE " + indexedExpr + " IS NULL")) {
      try (ResultSet rs = ps.executeQuery()) {
        for (long id : expectedIds) {
          assertTrue(rs.next());
          assertEquals(id, rs.getLong(1));
        }
        assertFalse(rs.next());
      }
      validateExplainPlan(ps, indexName, "RANGE SCAN");
    }
  }

  private static List<String> queryRows(Connection conn, String sql) throws SQLException {
    List<String> rows = new ArrayList<>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        rows.add(rs.getLong(1) + ":" + rs.getString(2));
      }
    }
    return rows;
  }

  private static void validateExplainPlan(PreparedStatement ps, String indexName, String scanType)
    throws SQLException {
    ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
    ExplainPlanAttributes attrs = plan.getPlanStepsAsAttributes();
    assertEquals(indexName, attrs.getTableName());
    assertEquals("PARALLEL 1-WAY", attrs.getIteratorTypeAndScanSize());
    assertEquals(scanType, attrs.getExplainScanType());
  }
}
