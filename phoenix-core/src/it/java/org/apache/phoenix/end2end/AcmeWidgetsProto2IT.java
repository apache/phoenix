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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.protobuf.text.AcmeWidgetsFixtures;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.Descriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class AcmeWidgetsProto2IT extends ParallelStatsDisabledIT {
  private static final String TOKEN_REVOCATION =
    "com.example.acmewidgets.auth.v1.proto2.TokenRevocation";

  private static Map<String, FileDescriptor> descriptorGraph;

  private final boolean columnEncoded;

  public AcmeWidgetsProto2IT(boolean columnEncoded) {
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

  private void registerTokenRevocationDescriptor(Connection conn) throws SQLException {
    conn.createStatement().execute("CREATE PROTOBUF DESCRIPTOR IF NOT EXISTS " + TOKEN_REVOCATION
      + " FROM '" + AcmeWidgetsFixtures.bundleFileUrl() + "'");
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
      try {
        stmt.execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + TOKEN_REVOCATION);
      } catch (SQLException ignore) {
      }
    }
    createdTables.clear();
  }

  @Test
  public void createProto2DescriptorAndTableSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerTokenRevocationDescriptor(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + TOKEN_REVOCATION + ">)" + encodingClause());
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " LIMIT 0")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("PROTOBUF", md.getColumnTypeName(2));
      }
    }
  }

  @Test
  public void upsertAndSelectTokenRevocationAllRequiredFieldsSetRoundTrips() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerTokenRevocationDescriptor(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + TOKEN_REVOCATION + ">)" + encodingClause());
      Descriptor tokenRevocation = AcmeWidgetsFixtures.tokenRevocationDescriptor(descriptorGraph);
      String[] reasons = { "rotated", "revoked", "expired", "stolen", "lost", "other" };
      byte[][] wires = new byte[reasons.length][];
      for (int i = 0; i < reasons.length; i++) {
        wires[i] = AcmeWidgetsFixtures
          .newTokenRevocation(tokenRevocation, "tenant-" + i, "tok-" + i, reasons[i]).toByteArray();
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < reasons.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] roundTripped = rs.getBytes(2);
          assertArrayEquals("row " + i, wires[i], roundTripped);
          DynamicMessage parsedBack = DynamicMessage.parseFrom(tokenRevocation, roundTripped);
          assertTrue(parsedBack.hasField(tokenRevocation.findFieldByName("reason")));
          assertEquals(reasons[i], parsedBack.getField(tokenRevocation.findFieldByName("reason")));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void upsertAndSelectTokenRevocationWithReasonUnsetRoundTrips() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerTokenRevocationDescriptor(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + TOKEN_REVOCATION + ">)" + encodingClause());
      Descriptor tokenRevocation = AcmeWidgetsFixtures.tokenRevocationDescriptor(descriptorGraph);
      FieldDescriptor reason = tokenRevocation.findFieldByName("reason");
      byte[][] wires = new byte[6][];
      for (int i = 0; i < wires.length; i++) {
        wires[i] = AcmeWidgetsFixtures
          .newTokenRevocationWithReasonUnset(tokenRevocation, "tenant-" + i, "tok-" + i)
          .toByteArray();
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < wires.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] roundTripped = rs.getBytes(2);
          assertArrayEquals("row " + i, wires[i], roundTripped);
          DynamicMessage parsedBack = DynamicMessage.parseFrom(tokenRevocation, roundTripped);
          assertFalse(parsedBack.hasField(reason));
          assertEquals("rotated", parsedBack.getField(reason));
        }
        assertFalse(rs.next());
      }
      // An unset proto2 field reads as its declared default; PROTO_EXISTS still reports it unset.
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT PROTO_VALUE(PAYLOAD, 'reason', 'VARCHAR'),"
          + " PROTO_EXISTS(PAYLOAD, 'reason') FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < wires.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals("row " + i, "rotated", rs.getString(1));
          assertFalse("row " + i, rs.getBoolean(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void unsetProto2FieldsReadAsDeclaredDefaultAcrossRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerTokenRevocationDescriptor(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + TOKEN_REVOCATION + ">)" + encodingClause());
      Descriptor tokenRevocation = AcmeWidgetsFixtures.tokenRevocationDescriptor(descriptorGraph);
      // { reason, soft_revoke }; null means unset. reason defaults to "rotated", soft_revoke to
      // false.
      Object[][] rows = { { "compromised", true }, { null, null }, { "", null },
        { "rotated", false }, { null, true }, { "expired", null } };
      for (int i = 0; i < rows.length; i++) {
        DynamicMessage.Builder b = DynamicMessage.newBuilder(tokenRevocation)
          .setField(tokenRevocation.findFieldByName("tenant_id"), "tenant-" + i)
          .setField(tokenRevocation.findFieldByName("token_id"), "tok-" + i);
        if (rows[i][0] != null) {
          b.setField(tokenRevocation.findFieldByName("reason"), rows[i][0]);
        }
        if (rows[i][1] != null) {
          b.setField(tokenRevocation.findFieldByName("soft_revoke"), rows[i][1]);
        }
        upsertPayload(conn, table, i + 1L, b.build().toByteArray());
      }

      // Row 3 sets reason to "", which is on the wire, so neither default applies and Phoenix
      // reads the empty string as NULL.
      String[] expectedReason = { "compromised", "rotated", null, "rotated", "rotated", "expired" };
      String[] expectedWithUserDefault =
        { "compromised", "none", null, "rotated", "none", "expired" };
      boolean[] expectedSoftRevoke = { true, false, false, false, true, false };
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'reason', 'VARCHAR'),"
          + " PROTO_EXISTS(PAYLOAD, 'reason'),"
          + " PROTO_VALUE(PAYLOAD, 'reason', 'VARCHAR', 'none'),"
          + " PROTO_VALUE(PAYLOAD, 'soft_revoke', 'BOOLEAN'),"
          + " PROTO_EXISTS(PAYLOAD, 'soft_revoke') FROM " + table + " ORDER BY ID")) {
        for (int i = 0; i < rows.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expectedReason[i], rs.getString(2));
          assertEquals("row " + i, rows[i][0] != null, rs.getBoolean(3));
          assertEquals("row " + i, expectedWithUserDefault[i], rs.getString(4));
          assertEquals("row " + i, expectedSoftRevoke[i], rs.getObject(5));
          assertEquals("row " + i, rows[i][1] != null, rs.getBoolean(6));
        }
        assertFalse(rs.next());
      }

      // Filters evaluate on the server, where unset rows match the declared default too.
      assertIds(conn, "SELECT ID FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'reason', 'VARCHAR') = 'rotated' ORDER BY ID", 2L, 4L, 5L);
      assertIds(conn,
        "SELECT ID FROM " + table
          + " WHERE PROTO_VALUE(PAYLOAD, 'reason', 'VARCHAR', 'none') = 'none' ORDER BY ID",
        2L, 5L);
      assertIds(conn,
        "SELECT ID FROM " + table
          + " WHERE PROTO_VALUE(PAYLOAD, 'soft_revoke', 'BOOLEAN') = FALSE ORDER BY ID DESC",
        6L, 4L, 3L, 2L);
    }
  }

  // PROTO_VALUE_AT_TAG coverage of proto2 extensions is not asserted here yet.
  @Test
  public void upsertAndSelectTokenRevocationWithExtensionSurvivesRoundTrip() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerTokenRevocationDescriptor(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + TOKEN_REVOCATION + ">)" + encodingClause());
      Descriptor tokenRevocation = AcmeWidgetsFixtures.tokenRevocationDescriptor(descriptorGraph);
      FileDescriptor voucherFile = descriptorGraph.get("voucher_revocation_sync.proto");
      String[] partners = { "p-a", "p-b", "p-c", "p-d", "p-e", "p-f" };
      byte[][] wires = new byte[partners.length][];
      for (int i = 0; i < partners.length; i++) {
        wires[i] = AcmeWidgetsFixtures.newTokenRevocationWithPartnerSourceExtension(tokenRevocation,
          voucherFile, "tenant-" + i, "tok-" + i, partners[i]).toByteArray();
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < partners.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertArrayEquals("row " + i, wires[i], rs.getBytes(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void multiRowProto2UpsertAndSelectPreservesAllPayloadsIndependently() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerTokenRevocationDescriptor(conn);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + TOKEN_REVOCATION + ">)" + encodingClause());
      Descriptor tokenRevocation = AcmeWidgetsFixtures.tokenRevocationDescriptor(descriptorGraph);
      FileDescriptor voucherFile = descriptorGraph.get("voucher_revocation_sync.proto");
      byte[][] wires = new byte[6][];
      wires[0] = AcmeWidgetsFixtures
        .newTokenRevocation(tokenRevocation, "tenant-A", "tok-A", "revoked").toByteArray();
      wires[1] = AcmeWidgetsFixtures
        .newTokenRevocationWithReasonUnset(tokenRevocation, "tenant-B", "tok-B").toByteArray();
      wires[2] = AcmeWidgetsFixtures.newTokenRevocationWithPartnerSourceExtension(tokenRevocation,
        voucherFile, "tenant-C", "tok-C", "partner-c").toByteArray();
      wires[3] = AcmeWidgetsFixtures
        .newTokenRevocation(tokenRevocation, "tenant-D", "tok-D", "expired").toByteArray();
      wires[4] = AcmeWidgetsFixtures
        .newTokenRevocationWithReasonUnset(tokenRevocation, "tenant-E", "tok-E").toByteArray();
      wires[5] = AcmeWidgetsFixtures.newTokenRevocationWithPartnerSourceExtension(tokenRevocation,
        voucherFile, "tenant-F", "tok-F", "partner-f").toByteArray();
      for (int i = 0; i < wires.length; i++) {
        upsertPayload(conn, table, i + 1L, wires[i]);
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID, PAYLOAD FROM " + table + " ORDER BY ID ASC")) {
        for (int i = 0; i < wires.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertArrayEquals("row " + i, wires[i], rs.getBytes(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  private static void assertIds(Connection conn, String sql, long... expected) throws SQLException {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      for (long id : expected) {
        assertTrue(sql, rs.next());
        assertEquals(sql, id, rs.getLong(1));
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
}
