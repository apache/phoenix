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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.protobuf.text.AcmeWidgetsFixtures;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.Descriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class AcmeWidgetsBundleIT extends ParallelStatsDisabledIT {
  private static final String ACCOUNT = "com.example.acmewidgets.account.v1.Account";
  private static final String WIDGET_ENDPOINT =
    "com.example.acmewidgets.governance.v1.WidgetEndpoint";
  private static final String ENTITLEMENT = "com.example.acmewidgets.billing.v1.Entitlement";
  private static final String ORDER = "com.example.acmewidgets.orders.v1.Order";

  private static final List<String> BUNDLE_NAMES =
    Arrays.asList(ACCOUNT, WIDGET_ENDPOINT, ENTITLEMENT, ORDER);

  private static Map<String, FileDescriptor> descriptorGraph;

  private final boolean columnEncoded;

  public AcmeWidgetsBundleIT(boolean columnEncoded) {
    this.columnEncoded = columnEncoded;
  }

  @Parameterized.Parameters(name = "columnEncoded={0}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false }, { true } });
  }

  @BeforeClass
  public static synchronized void loadDescriptors() throws Exception {
    descriptorGraph = AcmeWidgetsFixtures.loadAndBuildDescriptorGraph();
  }

  private Properties testProps() {
    return PropertiesUtil.deepCopy(TEST_PROPERTIES);
  }

  private String encodingClause() {
    return this.columnEncoded ? " COLUMN_ENCODED_BYTES=2" : " COLUMN_ENCODED_BYTES=0";
  }

  private final List<String> createdTables = new ArrayList<>();

  private String uniqueTableName() {
    String name = generateUniqueName();
    createdTables.add(name);
    return name;
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
  public void createProtobufBundleRegistersAllExpectedNames() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      for (String name : BUNDLE_NAMES) {
        assertTrue("Expected descriptor registered after CREATE PROTOBUF BUNDLE: " + name,
          isDescriptorRegistered(conn, name));
      }
      assertFalse(isDescriptorRegistered(conn, "com.example.acmewidgets.account.v1.Accoun"));
    }
  }

  @Test
  public void createTableWithBundleRegisteredProtobufColumnSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      assertLinkRowCount(conn, ACCOUNT, table, 1);
    }
  }

  @Test
  public void upsertAndSelectAccountRoundTripsByteIdentity() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "t-alpha", "t-bravo", "t-charlie", "t-delta", "t-echo", "t-foxtrot" };
      byte[][] wires = new byte[tenants.length][];
      for (int i = 0; i < tenants.length; i++) {
        wires[i] = AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "Name-" + i, i % 2 == 0, "wh-" + i).toByteArray();
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < tenants.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] roundTripped = rs.getBytes(2);
          assertArrayEquals("row " + i, wires[i], roundTripped);
          DynamicMessage parsedBack = DynamicMessage.parseFrom(account, roundTripped);
          assertEquals(tenants[i], parsedBack.getField(account.findFieldByName("tenant_key")));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void upsertAndSelectWidgetEndpointWithOneofArmRoundTrips() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + WIDGET_ENDPOINT + ">)" + encodingClause());
      Descriptor widgetEndpoint = AcmeWidgetsFixtures.widgetEndpointDescriptor(descriptorGraph);
      Descriptor catalogQuery = AcmeWidgetsFixtures.catalogQueryDescriptor(descriptorGraph);
      String[] names = { "ep-a", "ep-b", "ep-c", "ep-d", "ep-e", "ep-f" };
      byte[][] wires = new byte[names.length][];
      for (int i = 0; i < names.length; i++) {
        wires[i] = AcmeWidgetsFixtures.newWidgetEndpointWithCatalogQueryArm(widgetEndpoint,
          catalogQuery, "tenant-" + i, names[i]).toByteArray();
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < names.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] roundTripped = rs.getBytes(2);
          assertArrayEquals("row " + i, wires[i], roundTripped);
          DynamicMessage parsedBack = DynamicMessage.parseFrom(widgetEndpoint, roundTripped);
          assertEquals("catalog_query", parsedBack.getOneofFieldDescriptor(
            widgetEndpoint.findFieldByName("catalog_query").getContainingOneof()).getName());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void upsertAndSelectEntitlementWithMapAndRepeatedRoundTrips() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ENTITLEMENT + ">)" + encodingClause());
      boolean[] allowReturns = { true, true, false, false, true, false };
      byte[][] wires = new byte[allowReturns.length][];
      for (int i = 0; i < allowReturns.length; i++) {
        wires[i] = buildEntitlementWithToggles("t-" + i, allowReturns[i], i % 2 == 0).toByteArray();
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        Descriptor entitlement = AcmeWidgetsFixtures.entitlementDescriptor(descriptorGraph);
        for (int i = 0; i < allowReturns.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] roundTripped = rs.getBytes(2);
          assertArrayEquals("row " + i, wires[i], roundTripped);
          DynamicMessage parsedBack = DynamicMessage.parseFrom(entitlement, roundTripped);
          assertEquals(2,
            parsedBack.getRepeatedFieldCount(entitlement.findFieldByName("policy_toggles")));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void resultSetMetadataReturnsProtobufTypeName() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " LIMIT 0")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("PROTOBUF", md.getColumnTypeName(2));
        assertEquals(PDataType.PROTOBUF_TYPE, md.getColumnType(2));
      }
    }
  }

  @Test
  public void multiRowUpsertAndSelectPreservesAllPayloadsIndependently() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "tenant-A", "tenant-B", "tenant-C", "tenant-D", "tenant-E", "tenant-F" };
      byte[][] wires = new byte[tenants.length][];
      for (int i = 0; i < tenants.length; i++) {
        wires[i] = AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "N" + i, i % 2 == 0, "wh-" + i).toByteArray();
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < tenants.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertArrayEquals("row " + i, wires[i], rs.getBytes(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void nullProtobufColumnValueRoundTripsAsNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      boolean[] isNull = { true, false, true, false, true, false };
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + table + " (ID, PAYLOAD) VALUES (?, ?)")) {
        for (int i = 0; i < isNull.length; i++) {
          ps.setLong(1, i + 1L);
          if (isNull[i]) {
            ps.setNull(2, PDataType.PROTOBUF_TYPE);
          } else {
            ps.setBytes(2,
              AcmeWidgetsFixtures.newAccount(account, "t-" + i, "d", true, "w").toByteArray());
          }
          ps.execute();
        }
      }
      conn.commit();
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < isNull.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] value = rs.getBytes(2);
          if (isNull[i]) {
            assertNull(value);
            assertTrue(rs.wasNull());
          } else {
            assertTrue(value != null && value.length > 0);
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueScalarStringFieldRoundTrips() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "t-alpha", "t-bravo", "t-charlie", "t-delta", "t-echo", "t-foxtrot" };
      for (int i = 0; i < tenants.length; i++) {
        upsertPayload(conn, table, i + 1L, AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "N" + i, true, "wh-" + i).toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') FROM " + table
            + " ORDER BY ID ASC")) {
        for (int i = 0; i < tenants.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals(tenants[i], rs.getString(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueScalarBoolFieldRoundTripsWithProto3Default() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      boolean[] explicitActive = { true, true, false, true, false, false };
      for (int i = 0; i < explicitActive.length; i++) {
        if (i == 1 || i == 4) {
          DynamicMessage unset = DynamicMessage.newBuilder(account)
            .setField(account.findFieldByName("tenant_key"), "t-" + i)
            .setField(account.findFieldByName("display_name"), "d").build();
          upsertPayload(conn, table, i + 1L, unset.toByteArray());
        } else {
          upsertPayload(conn, table, i + 1L, AcmeWidgetsFixtures
            .newAccount(account, "t-" + i, "d", explicitActive[i], "w").toByteArray());
        }
      }
      boolean[] expected = { true, false, false, true, false, false };
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'active', 'BOOLEAN') FROM " + table
            + " ORDER BY ID ASC")) {
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expected[i], rs.getBoolean(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueNestedMessageFieldReturnsInnerBytes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + WIDGET_ENDPOINT + ">)" + encodingClause());
      Descriptor widgetEndpoint = AcmeWidgetsFixtures.widgetEndpointDescriptor(descriptorGraph);
      Descriptor catalogQuery = AcmeWidgetsFixtures.catalogQueryDescriptor(descriptorGraph);
      long[] versions = { 10L, 20L, 30L, 40L, 50L, 60L };
      for (int i = 0; i < versions.length; i++) {
        DynamicMessage catalogQueryMsg = DynamicMessage.newBuilder(catalogQuery)
          .setField(catalogQuery.findFieldByName("version"), versions[i]).build();
        DynamicMessage msg = DynamicMessage.newBuilder(widgetEndpoint)
          .setField(widgetEndpoint.findFieldByName("tenant_key"), "tenant-" + i)
          .setField(widgetEndpoint.findFieldByName("name"), "ep-" + i)
          .setField(widgetEndpoint.findFieldByName("catalog_query"), catalogQueryMsg).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'catalog_query', 'VARBINARY') FROM "
            + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < versions.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] innerBytes = rs.getBytes(2);
          assertTrue("row " + i, innerBytes != null);
          DynamicMessage inner = DynamicMessage.parseFrom(catalogQuery, innerBytes);
          assertEquals(versions[i], inner.getField(catalogQuery.findFieldByName("version")));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueOneofInactiveArmReturnsNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + WIDGET_ENDPOINT + ">)" + encodingClause());
      Descriptor widgetEndpoint = AcmeWidgetsFixtures.widgetEndpointDescriptor(descriptorGraph);
      Descriptor catalogQuery = AcmeWidgetsFixtures.catalogQueryDescriptor(descriptorGraph);
      for (int i = 0; i < 6; i++) {
        DynamicMessage msg = AcmeWidgetsFixtures
          .newWidgetEndpointWithCatalogQueryArm(widgetEndpoint, catalogQuery, "t-" + i, "e-" + i);
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'bulk_order', 'VARBINARY') FROM "
            + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertNull(rs.getBytes(2));
          assertTrue(rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueMapValueScalarFieldRoundTrips() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ENTITLEMENT + ">)" + encodingClause());
      boolean[] allowReturns = { true, true, true, false, false, false };
      for (int i = 0; i < allowReturns.length; i++) {
        upsertPayload(conn, table, i + 1L,
          buildEntitlementWithToggles("t-" + i, allowReturns[i], false).toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
          "SELECT ID, PROTO_VALUE(PAYLOAD, 'policy_toggles.allow_returns.enabled', 'BOOLEAN')"
            + " FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < allowReturns.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, allowReturns[i], rs.getBoolean(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueMissingMapKeyReturnsNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ENTITLEMENT + ">)" + encodingClause());
      for (int i = 0; i < 6; i++) {
        upsertPayload(conn, table, i + 1L,
          buildEntitlementWithToggles("t-" + i, true, false).toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
          "SELECT ID, PROTO_VALUE(PAYLOAD, 'policy_toggles.never_added.enabled', 'BOOLEAN')"
            + " FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          rs.getBoolean(2);
          assertTrue(rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueChildOfUnsetParentReturnsNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      for (int i = 0; i < 6; i++) {
        upsertPayload(conn, table, i + 1L,
          AcmeWidgetsFixtures.newAccount(account, "t-" + i, "d", true, "w").toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt
          .executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'deleted_time.seconds', 'BIGINT') FROM "
            + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          rs.getLong(2);
          assertTrue(rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueWithDefaultAppliesDefault() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      for (int i = 0; i < 6; i++) {
        upsertPayload(conn, table, i + 1L,
          AcmeWidgetsFixtures.newAccount(account, "t-" + i, "d", true, "w").toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, "
          + "'deleted_time.seconds', 'BIGINT', '999999') FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals(999999L, rs.getLong(2));
          assertFalse(rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueWithVarcharAndBigintDefaultsMixInSingleQuery() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "t-alpha", "t-beta", "t-gamma", "t-delta", "t-epsilon", "t-zeta" };
      for (int i = 0; i < tenants.length; i++) {
        upsertPayload(conn, table, i + 1L, AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "d-" + i, i % 2 == 0, "wh-" + i).toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR', 'no-tenant'), "
        + "PROTO_VALUE(PAYLOAD, 'deleted_time.seconds', 'BIGINT', '111111') " + "FROM " + table
        + " ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        for (int i = 0; i < tenants.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, tenants[i], rs.getString(2));
          assertEquals("row " + i, 111111L, rs.getLong(3));
          assertFalse(rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueWithLiteralNullDefaultReturnsSqlNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      for (long id = 1L; id <= 5L; id++) {
        upsertPayload(conn, table, id,
          AcmeWidgetsFixtures.newAccount(account, "t-" + id, "d", true, "w").toByteArray());
      }
      String sql =
        "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'deleted_time.seconds', 'BIGINT', 'null'), "
          + "PROTO_VALUE(PAYLOAD, 'deleted_time.seconds', 'BIGINT', 'NULL'), "
          + "PROTO_VALUE(PAYLOAD, 'deleted_time.seconds', 'BIGINT', 'NuLl') " + "FROM " + table
          + " ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        for (long id = 1L; id <= 5L; id++) {
          assertTrue("row " + id, rs.next());
          assertEquals(id, rs.getLong(1));
          for (int col = 2; col <= 4; col++) {
            rs.getLong(col);
            assertTrue("row " + id + " col " + col, rs.wasNull());
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueWithDoubleDefaultFiresForPastEndRepeatedIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + ORDER + ">)" + encodingClause());
      for (long id = 1L; id <= 6L; id++) {
        upsertPayload(conn, table, id, AcmeWidgetsFixtures
          .newOrderRichlyPopulated(descriptorGraph, "ord-" + id, "cust-" + id).toByteArray());
      }
      String sql =
        "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'customer.contact.addresses[5].geo.latitude', "
          + "'DOUBLE', '999.5'), "
          + "PROTO_VALUE(PAYLOAD, 'customer.contact.addresses[9].geo.longitude', "
          + "'DOUBLE', '-1.25') " + "FROM " + table + " ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        for (long id = 1L; id <= 6L; id++) {
          assertTrue("row " + id, rs.next());
          assertEquals(id, rs.getLong(1));
          assertEquals("row " + id, 999.5d, rs.getDouble(2), 1e-9);
          assertFalse(rs.wasNull());
          assertEquals("row " + id, -1.25d, rs.getDouble(3), 1e-9);
          assertFalse(rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueInWhereClauseFilters() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants =
        { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel" };
      for (int i = 0; i < tenants.length; i++) {
        upsertPayload(conn, table, i + 1L,
          AcmeWidgetsFixtures.newAccount(account, tenants[i], "N" + i, true, "w").toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') FROM "
            + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') >= 'charlie'"
            + "   AND PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') <= 'foxtrot'"
            + " ORDER BY PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') ASC")) {
        String[] expected = { "charlie", "delta", "echo", "foxtrot" };
        long[] expectedIds = { 3L, 4L, 5L, 6L };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(expectedIds[i], rs.getLong(1));
          assertEquals(expected[i], rs.getString(2));
        }
        assertFalse(rs.next());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') FROM "
            + table + " WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') >= 'charlie'"
            + "   AND PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') <= 'foxtrot'"
            + " ORDER BY PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') DESC")) {
        String[] expected = { "foxtrot", "echo", "delta", "charlie" };
        long[] expectedIds = { 6L, 5L, 4L, 3L };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("desc row " + i, rs.next());
          assertEquals(expectedIds[i], rs.getLong(1));
          assertEquals(expected[i], rs.getString(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoArrayLengthOnLineItemsCountsRepeatedMessageElements() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + ORDER + ">)" + encodingClause());
      Descriptor order = AcmeWidgetsFixtures.orderDescriptor(descriptorGraph);
      Descriptor lineItem = AcmeWidgetsFixtures.lineItemDescriptor(descriptorGraph);
      int[] lineItemsPerRow = { 0, 1, 3, 5, 2, 4 };
      for (int i = 0; i < lineItemsPerRow.length; i++) {
        upsertPayload(conn, table, i + 1L,
          newOrderWithLineItems(order, lineItem, "ord-" + (i + 1), lineItemsPerRow[i])
            .toByteArray());
      }
      String sql =
        "SELECT ID, PROTO_ARRAY_LENGTH(PAYLOAD, 'line_items') FROM " + table + " ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        for (int i = 0; i < lineItemsPerRow.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, lineItemsPerRow[i], rs.getInt(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoFieldTypeReturnsCanonicalSqlTypeNamesForOrderFields() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + ORDER + ">)" + encodingClause());
      for (long id = 1L; id <= 6L; id++) {
        DynamicMessage o =
          AcmeWidgetsFixtures.newOrderRichlyPopulated(descriptorGraph, "ord-" + id, "cust-" + id);
        upsertPayload(conn, table, id, o.toByteArray());
      }
      String sql = "SELECT ID, " + "PROTO_FIELD_TYPE(PAYLOAD, 'order_id'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'customer.contact.addresses[0].geo.latitude'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'line_items[0].quantity'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'line_items[0].line_total.units'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'line_items[0].line_total.nanos') " + "FROM " + table
        + " ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        for (long id = 1L; id <= 6L; id++) {
          assertTrue("row " + id, rs.next());
          assertEquals(id, rs.getLong(1));
          assertEquals("row " + id, "VARCHAR", rs.getString(2));
          assertEquals("row " + id, "DOUBLE", rs.getString(3));
          assertEquals("row " + id, "INTEGER", rs.getString(4));
          assertEquals("row " + id, "BIGINT", rs.getString(5));
          assertEquals("row " + id, "INTEGER", rs.getString(6));
        }
        assertFalse(rs.next());
      }
    }
  }

  private static DynamicMessage newOrderWithLineItems(Descriptor order, Descriptor lineItem,
    String orderId, int lineItemCount) {
    DynamicMessage.Builder builder =
      DynamicMessage.newBuilder(order).setField(order.findFieldByName("order_id"), orderId);
    for (int i = 0; i < lineItemCount; i++) {
      DynamicMessage li =
        DynamicMessage.newBuilder(lineItem).setField(lineItem.findFieldByName("line_id"), "li-" + i)
          .setField(lineItem.findFieldByName("quantity"), i + 1).build();
      builder.addRepeatedField(order.findFieldByName("line_items"), li);
    }
    return builder.build();
  }

  @Test
  public void protoExistsInWhereClauseFiltersRowsByHomeWarehousePresence() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] warehouses = { "w1", "", "w2", "", "w3", "", "w4" };
      for (int i = 0; i < warehouses.length; i++) {
        upsertPayload(conn, table, i + 1L,
          AcmeWidgetsFixtures
            .newAccount(account, "tenant-" + (i + 1), "A" + (i + 1), true, warehouses[i])
            .toByteArray());
      }
      String sql = "SELECT ID FROM " + table
        + " WHERE PROTO_EXISTS(PAYLOAD, 'home_warehouse') = TRUE ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        long[] expectedIds = { 1L, 3L, 5L, 7L };
        for (long expectedId : expectedIds) {
          assertTrue(rs.next());
          assertEquals(expectedId, rs.getLong(1));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueDeepFiveLevelMessageChainWithArrayIndex() throws Exception {
    runOrderQuery("PROTO_VALUE(PAYLOAD, 'customer.contact.addresses[0].geo.latitude', 'DOUBLE')",
      (rs, i) -> assertEquals(10.0d + i, rs.getDouble(2), 0.0001d));
  }

  @Test
  public void protoValueRepeatedOfRepeatedRecursiveTypeChain() throws Exception {
    runOrderQuery(
      "PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].modifiers[0].sub_modifiers[0].amount.units', 'BIGINT')",
      (rs, i) -> assertEquals(100L + i, rs.getLong(2)));
  }

  @Test
  public void protoValueMapValueMessageThenScalarChain() throws Exception {
    runOrderQuery("PROTO_VALUE(PAYLOAD, 'line_items[0].regional_prices.US.units', 'BIGINT')",
      (rs, i) -> assertEquals(40L, rs.getLong(2)));
  }

  @Test
  public void protoValueMapOfRecursiveThenRepeatedNestedAccess() throws Exception {
    runOrderQuery(
      "PROTO_VALUE(PAYLOAD, "
        + "'line_items[0].named_modifiers.holiday.sub_modifiers[0].display_name', " + "'VARCHAR')",
      (rs, i) -> assertEquals("mod-" + (i + 1), rs.getString(2)));
  }

  @Test
  public void protoValueRecursiveSelfReferenceChain() throws Exception {
    runOrderQuery("PROTO_VALUE(PAYLOAD, 'line_items[0].category.parent.parent.name', 'VARCHAR')",
      (rs, i) -> assertEquals("cat-" + (i + 1), rs.getString(2)));
  }

  @Test
  public void protoValueMultipleCallsPerRowShareCachedPlan() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + ORDER + ">)" + encodingClause());
      for (int i = 0; i < 6; i++) {
        DynamicMessage order = AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph,
          "ord-" + (i + 1), "cust-" + (i + 1), 10L * (i + 1), 100L + i, 10.0d + i, i + 1,
          "mod-" + (i + 1), "cat-" + (i + 1));
        upsertPayload(conn, table, i + 1L, order.toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, " + " PROTO_VALUE(PAYLOAD, 'order_id', 'VARCHAR'),"
            + " PROTO_VALUE(PAYLOAD, 'customer.customer_id', 'VARCHAR'),"
            + " PROTO_VALUE(PAYLOAD, 'line_items[0].line_id', 'VARCHAR'),"
            + " PROTO_VALUE(PAYLOAD, 'line_items[0].sku', 'VARCHAR')" + " FROM " + table
            + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("ord-" + (i + 1), rs.getString(2));
          assertEquals("cust-" + (i + 1), rs.getString(3));
          assertEquals("line-1", rs.getString(4));
          assertEquals("SKU-001", rs.getString(5));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueIndexPastRepeatedEndReturnsNull() throws Exception {
    runOrderQuery("PROTO_VALUE(PAYLOAD, 'line_items[99].line_id', 'VARCHAR')", (rs, i) -> {
      assertNull(rs.getString(2));
      assertTrue(rs.wasNull());
    });
  }

  @Test
  public void protoValueInt64ScalarFieldRoundTrips() throws Exception {
    runOrderQuery("PROTO_VALUE(PAYLOAD, 'line_items[0].line_total.units', 'BIGINT')",
      (rs, i) -> assertEquals(10L * (i + 1), rs.getLong(2)));
  }

  @Test
  public void protoValueOnVarbinaryColumnRejectedAtCompile() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, RAW_BYTES VARBINARY)" + encodingClause());
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery("SELECT PROTO_VALUE(RAW_BYTES, 'foo', 'VARCHAR') FROM " + table);
        fail("Expected PROTO_VALUE on VARBINARY column to be rejected");
      } catch (SQLException e) {
        assertTrue("Expected error to mention PROTO_VALUE_AT_TAG hint: " + e.getMessage(),
          e.getMessage().contains("PROTO_VALUE_AT_TAG") || e.getMessage().contains("PROTOBUF"));
      }
    }
  }

  private interface RowAssertion {
    void check(ResultSet rs, int rowIndex) throws SQLException;
  }

  private void runOrderQuery(String selectExpr, RowAssertion assertion) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + ORDER + ">)" + encodingClause());
      for (int i = 0; i < 6; i++) {
        DynamicMessage order = AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph,
          "ord-" + (i + 1), "cust-" + (i + 1), 10L * (i + 1), 100L + i, 10.0d + i, i + 1,
          "mod-" + (i + 1), "cat-" + (i + 1));
        upsertPayload(conn, table, i + 1L, order.toByteArray());
      }
      try (Statement stmt = conn.createStatement(); ResultSet rs =
        stmt.executeQuery("SELECT ID, " + selectExpr + " FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertion.check(rs, i);
        }
        assertFalse(rs.next());
      }
    }
  }

  private DynamicMessage buildEntitlementWithToggles(String tenantKey, boolean allowReturns,
    boolean allowRefunds) {
    Descriptor entitlement = AcmeWidgetsFixtures.entitlementDescriptor(descriptorGraph);
    Descriptor entitlementKey = AcmeWidgetsFixtures.entitlementKeyDescriptor(descriptorGraph);
    Descriptor entitlementVersion =
      AcmeWidgetsFixtures.entitlementVersionDescriptor(descriptorGraph);
    Descriptor policyToggle = AcmeWidgetsFixtures.policyToggleDescriptor(descriptorGraph);
    Map<String, Boolean> toggles = new LinkedHashMap<>();
    toggles.put("allow_returns", allowReturns);
    toggles.put("allow_refunds", allowRefunds);
    return AcmeWidgetsFixtures.newEntitlement(entitlement, entitlementKey, entitlementVersion,
      policyToggle, tenantKey, "unlimited-widgets", "ent-xyz", 1L, toggles);
  }

  @Test
  public void protoValueAtTagExtractsTopLevelStringByTagNumber() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, RAW VARBINARY)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" };
      for (int i = 0; i < tenants.length; i++) {
        upsertRaw(conn, table, i + 1L, AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "N" + i, true, "wh-" + i).toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') FROM "
          + table + " WHERE PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') >= 'bravo'"
          + "   AND PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') <= 'echo'"
          + " ORDER BY PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') ASC")) {
        String[] expected = { "bravo", "charlie", "delta", "echo" };
        long[] expectedIds = { 2L, 3L, 4L, 5L };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(expectedIds[i], rs.getLong(1));
          assertEquals(expected[i], rs.getString(2));
        }
        assertFalse(rs.next());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') FROM "
          + table + " WHERE PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') >= 'bravo'"
          + "   AND PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') <= 'echo'"
          + " ORDER BY PROTO_VALUE_AT_TAG(RAW, '1', 'VARCHAR') DESC")) {
        String[] expected = { "echo", "delta", "charlie", "bravo" };
        long[] expectedIds = { 5L, 4L, 3L, 2L };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("desc row " + i, rs.next());
          assertEquals(expectedIds[i], rs.getLong(1));
          assertEquals(expected[i], rs.getString(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueAtTagNestedOrderLineItemViaTagPath() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, RAW VARBINARY)" + encodingClause());
      for (int i = 0; i < 6; i++) {
        DynamicMessage order = AcmeWidgetsFixtures.newOrderRichlyPopulatedSeeded(descriptorGraph,
          "ord-" + (i + 1), "cust-" + (i + 1), 10L * (i + 1), 100L + i, 10.0d + i, i + 1,
          "mod-" + (i + 1), "cat-" + (i + 1));
        upsertRaw(conn, table, i + 1L, order.toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, PROTO_VALUE_AT_TAG(RAW, '3[0].1', 'VARCHAR') FROM " + table
            + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("line-1", rs.getString(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueAtTagMissingTagReturnsNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, RAW VARBINARY)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      for (int i = 0; i < 6; i++) {
        upsertRaw(conn, table, i + 1L,
          AcmeWidgetsFixtures.newAccount(account, "t-" + i, "d", true, "w").toByteArray());
      }
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(
        "SELECT ID, PROTO_VALUE_AT_TAG(RAW, '99', 'VARCHAR') FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertNull(rs.getString(2));
          assertTrue(rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueAtTagWithDefaultOnMissingTag() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, RAW VARBINARY)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      for (int i = 0; i < 6; i++) {
        upsertRaw(conn, table, i + 1L,
          AcmeWidgetsFixtures.newAccount(account, "t-" + i, "d", true, "w").toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs =
          stmt.executeQuery("SELECT ID, PROTO_VALUE_AT_TAG(RAW, '99', 'VARCHAR', 'fallback') FROM "
            + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < 6; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("fallback", rs.getString(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueAtTagInvalidSyntaxRejectedAtCompile() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, RAW VARBINARY)" + encodingClause());
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery("SELECT PROTO_VALUE_AT_TAG(RAW, 'abc', 'VARCHAR') FROM " + table);
        fail("Expected invalid tag-path syntax to be rejected");
      } catch (SQLException e) {
        assertTrue("Expected TAG_PATH_INVALID_SYNTAX: " + e.getMessage(),
          e.getErrorCode() == 11004 || e.getMessage().contains("tag-path"));
      }
    }
  }

  private static void upsertRaw(Connection conn, String table, long id, byte[] payload)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, RAW) VALUES (?, ?)")) {
      ps.setLong(1, id);
      ps.setBytes(2, payload);
      ps.execute();
    }
    conn.commit();
  }

  /**
   * Derived tables that cannot be flattened into the outer query (inner LIMIT) and UNION ALL
   * derived tables rebuild their columns from the inner projector; the PROTOBUF descriptor binding
   * must carry through so PROTO_VALUE still resolves on the outer reference.
   */
  @Test
  public void protoValueOverDerivedTableAndUnion() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
      String[] tenants = { "t-alpha", "t-bravo", "t-charlie", "t-delta", "t-echo", "t-foxtrot" };
      for (int i = 0; i < tenants.length; i++) {
        upsertPayload(conn, table, i + 1L, AcmeWidgetsFixtures
          .newAccount(account, tenants[i], "N" + i, true, "wh-" + i).toByteArray());
      }
      String inner = "SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID LIMIT 5";

      assertTenants(conn, "SELECT PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') FROM (" + inner
        + ") ORDER BY ID DESC", "t-echo", "t-delta", "t-charlie", "t-bravo", "t-alpha");
      assertTenants(conn,
        "SELECT PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') FROM (" + inner
          + ") WHERE PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') >= 't-charlie' ORDER BY ID",
        "t-charlie", "t-delta", "t-echo");
      assertTenants(conn,
        "SELECT PROTO_VALUE(S.PAYLOAD, 'tenant_key', 'VARCHAR') FROM " + table + " T JOIN (" + inner
          + ") S ON T.ID = S.ID WHERE T.ID BETWEEN 2 AND 4 ORDER BY T.ID",
        "t-bravo", "t-charlie", "t-delta");
      assertTenants(conn,
        "SELECT PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') FROM (SELECT ID, PAYLOAD FROM "
          + table + " WHERE ID <= 2 UNION ALL SELECT ID, PAYLOAD FROM " + table
          + " WHERE ID >= 5) ORDER BY ID DESC",
        "t-foxtrot", "t-echo", "t-bravo", "t-alpha");
    }
  }

  @Test
  public void protoValueOverUnionOfDifferentDescriptorsRejected() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerBundleIdempotent(conn);
      String accounts = uniqueTableName();
      String endpoints = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + accounts + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      conn.createStatement()
        .execute("CREATE TABLE " + endpoints + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + WIDGET_ENDPOINT + ">)" + encodingClause());
      try {
        conn.createStatement()
          .executeQuery("SELECT PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR') FROM (SELECT ID,"
            + " PAYLOAD FROM " + accounts + " UNION ALL SELECT ID, PAYLOAD FROM " + endpoints
            + ")");
        fail("PROTO_VALUE over a UNION ALL of differently bound PROTOBUF columns must fail");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH.getErrorCode(),
          e.getErrorCode());
      }
    }
  }

  private static void assertTenants(Connection conn, String sql, String... expected)
    throws SQLException {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      for (String tenant : expected) {
        assertTrue(sql, rs.next());
        assertEquals(sql, tenant, rs.getString(1));
      }
      assertFalse(sql, rs.next());
    }
  }

  private static void upsertPayload(Connection conn, String table, long id, byte[] payload)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, PAYLOAD) VALUES (?, ?)")) {
      ps.setLong(1, id);
      ps.setBytes(2, payload);
      ps.execute();
    }
    conn.commit();
  }

  private static void assertLinkRowCount(Connection conn, String protobufName, String tableName,
    int expected) throws SQLException {
    String sql = "SELECT COUNT(*) FROM SYSTEM.\"PROTO_DESC_LINK\" WHERE PROTOBUF_NAME = ? AND "
      + "TABLE_NAME = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, protobufName);
      ps.setString(2, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
      }
    }
  }

  private static boolean isDescriptorRegistered(Connection conn, String protobufName)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("SELECT 1 FROM SYSTEM.PROTO_DESCRIPTORS WHERE PROTOBUF_NAME = ?")) {
      ps.setString(1, protobufName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }
}
