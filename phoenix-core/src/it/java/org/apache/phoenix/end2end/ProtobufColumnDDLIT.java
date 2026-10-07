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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class ProtobufColumnDDLIT extends ParallelStatsDisabledIT {
  private final boolean columnEncoded;

  public ProtobufColumnDDLIT(boolean columnEncoded) {
    this.columnEncoded = columnEncoded;
  }

  @Parameterized.Parameters(name = "columnEncoded={0}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false }, { true } });
  }

  private static final String BUNDLE_RESOURCE = "/protobuf/acmewidgets/acmewidgets_v1.desc";

  private static final String ACCOUNT = "com.example.acmewidgets.account.v1.Account";
  private static final String DELIVERY_WINDOW_RECORD =
    "com.example.acmewidgets.delivery.v1.DeliveryWindowRecord";
  private static final String TOKEN_REVOCATION =
    "com.example.acmewidgets.auth.v1.proto2.TokenRevocation";
  private static final String UNKNOWN_MESSAGE = "com.example.acmewidgets.account.v1.NotInTheBundle";
  // Registered only after a failed reference, so no other IT may register it.
  private static final String POLICY_TOGGLE = "com.example.acmewidgets.billing.v1.PolicyToggle";

  private static final List<String> KNOWN_FIXTURE_NAMES =
    Arrays.asList(ACCOUNT, DELIVERY_WINDOW_RECORD, TOKEN_REVOCATION, POLICY_TOGGLE);

  private static String fixtureFileUrl() {
    URL res = ProtobufColumnDDLIT.class.getResource(BUNDLE_RESOURCE);
    assertNotNull("Test fixture " + BUNDLE_RESOURCE + " not on the classpath", res);
    return res.toString();
  }

  private Properties testProps() {
    return PropertiesUtil.deepCopy(TEST_PROPERTIES);
  }

  private String encodingClause() {
    return this.columnEncoded ? " COLUMN_ENCODED_BYTES=2" : " COLUMN_ENCODED_BYTES=0";
  }

  private final List<String> createdTables = new ArrayList<>();
  private final List<String> createdViews = new ArrayList<>();

  private String uniqueTableName() {
    String name = generateUniqueName();
    createdTables.add(name);
    return name;
  }

  private String uniqueViewName() {
    String name = generateUniqueName();
    createdViews.add(name);
    return name;
  }

  @After
  public void cleanupTablesAndDescriptors() throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps());
      Statement stmt = conn.createStatement()) {
      for (String name : createdViews) {
        try {
          stmt.execute("DROP VIEW IF EXISTS " + name);
        } catch (SQLException ignore) {
        }
      }
      for (String name : createdTables) {
        try {
          stmt.execute("DROP TABLE IF EXISTS " + name);
        } catch (SQLException ignore) {
        }
      }
      for (String name : KNOWN_FIXTURE_NAMES) {
        try {
          stmt.execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + name);
        } catch (SQLException ignore) {
        }
      }
    }
    createdTables.clear();
    createdViews.clear();
  }

  private void registerDescriptorsIdempotent(Connection conn, String... names) throws SQLException {
    for (String name : names) {
      conn.createStatement().execute(
        "CREATE PROTOBUF DESCRIPTOR IF NOT EXISTS " + name + " FROM '" + fixtureFileUrl() + "'");
    }
  }

  @Test
  public void conditionalTtlOnProtoValueExpiresRowsOnCompaction() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause()
          + ", TTL = 'PROTO_VALUE(PAYLOAD, ''active'', ''BOOLEAN'') = TRUE'"
          + ", \"phoenix.max.lookback.age.seconds\" = 0");
      try (
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
        ps.setLong(1, 1L);
        ps.setBytes(2, new byte[] { 0x0A, 0x01, 'a' });
        ps.executeUpdate();
        ps.setLong(1, 2L);
        ps.setBytes(2, new byte[] { 0x0A, 0x01, 'b', 0x20, 0x01 });
        ps.executeUpdate();
      }
      conn.commit();
      TestUtil.flush(getUtility(), TableName.valueOf(table));
      TestUtil.majorCompact(getUtility(), TableName.valueOf(table));
      assertEquals(1, TestUtil.getRawRowCount(conn, TableName.valueOf(table)));
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT ID FROM " + table)) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void createTableWithProtobufColumnSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      assertEquals(ACCOUNT, getProtoMessageName(conn, table, "PAYLOAD"));
      assertLinkRowCount(conn, ACCOUNT, table, 1);
    }
  }

  @Test
  public void createTableWithMissingDescriptorRaisesNotFound() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = uniqueTableName();
      try {
        conn.createStatement()
          .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
            + UNKNOWN_MESSAGE + ">)" + encodingClause());
        fail("CREATE TABLE with unregistered descriptor must be rejected");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND.getErrorCode(),
          e.getErrorCode());
        assertTrue(e.getMessage().contains(UNKNOWN_MESSAGE));
      }
      assertTableDoesNotExist(conn, table);
    }
  }

  @Test
  public void createTableWithMultipleProtobufColumnsWritesAllLinks() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD, TOKEN_REVOCATION);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (" + "ID BIGINT NOT NULL PRIMARY KEY, " + "A PROTOBUF<"
          + ACCOUNT + ">, " + "D PROTOBUF<" + DELIVERY_WINDOW_RECORD + ">, " + "T PROTOBUF<"
          + TOKEN_REVOCATION + ">)" + encodingClause());
      assertEquals(ACCOUNT, getProtoMessageName(conn, table, "A"));
      assertEquals(DELIVERY_WINDOW_RECORD, getProtoMessageName(conn, table, "D"));
      assertEquals(TOKEN_REVOCATION, getProtoMessageName(conn, table, "T"));
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 1);
      assertLinkRowCount(conn, TOKEN_REVOCATION, table, 1);
    }
  }

  @Test
  public void createTableWithMixedColumnTypesOnlyLinksProtobufCols() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement().execute(
        "CREATE TABLE " + table + " (" + "ID BIGINT NOT NULL PRIMARY KEY, " + "NAME VARCHAR(64), "
          + "AGE INTEGER, " + "PAYLOAD PROTOBUF<" + ACCOUNT + ">)" + encodingClause());
      assertEquals(ACCOUNT, getProtoMessageName(conn, table, "PAYLOAD"));
      assertNull(getProtoMessageName(conn, table, "NAME"));
      assertNull(getProtoMessageName(conn, table, "AGE"));
      assertLinkRowCount(conn, ACCOUNT, table, 1);
    }
  }

  @Test
  public void alterTableAddProtobufColumnSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, NAME VARCHAR)" + encodingClause());
      conn.createStatement()
        .execute("ALTER TABLE " + table + " ADD PAYLOAD PROTOBUF<" + ACCOUNT + ">");
      assertEquals(ACCOUNT, getProtoMessageName(conn, table, "PAYLOAD"));
      assertLinkRowCount(conn, ACCOUNT, table, 1);
    }
  }

  @Test
  public void messageNameOnNonProtobufColumnIsRejected() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      assertMessageNameNotAllowed(conn,
        "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, C VARCHAR<" + ACCOUNT + ">)");
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, NAME VARCHAR)" + encodingClause());
      assertMessageNameNotAllowed(conn, "ALTER TABLE " + table + " ADD C VARCHAR<" + ACCOUNT + ">");
      assertLinkRowCount(conn, ACCOUNT, table, 0);
    }
  }

  private static void assertMessageNameNotAllowed(Connection conn, String sql) {
    try {
      conn.createStatement().execute(sql);
      fail("expected rejection for " + sql);
    } catch (SQLException e) {
      assertEquals(sql, SQLExceptionCode.PROTOBUF_MESSAGE_NAME_NOT_ALLOWED.getErrorCode(),
        e.getErrorCode());
    }
  }

  @Test
  public void alterTableAddProtobufColumnMissingDescriptorRaisesNotFound() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, NAME VARCHAR)" + encodingClause());
      try {
        conn.createStatement()
          .execute("ALTER TABLE " + table + " ADD PAYLOAD PROTOBUF<" + UNKNOWN_MESSAGE + ">");
        fail("ALTER TABLE ADD PROTOBUF column with unregistered descriptor must be rejected");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND.getErrorCode(),
          e.getErrorCode());
      }
      assertNull(getProtoMessageName(conn, table, "PAYLOAD"));
      assertLinkRowCount(conn, UNKNOWN_MESSAGE, table, 0);
    }
  }

  @Test
  public void createTableSucceedsOnRetryAfterDescriptorIsCreated() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = uniqueTableName();
      String ddl = "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
        + POLICY_TOGGLE + ">)" + encodingClause();
      try {
        conn.createStatement().execute(ddl);
        fail("CREATE TABLE with unregistered descriptor must be rejected");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND.getErrorCode(),
          e.getErrorCode());
      }
      registerDescriptorsIdempotent(conn, POLICY_TOGGLE);
      conn.createStatement().execute(ddl);
      assertEquals(POLICY_TOGGLE, getProtoMessageName(conn, table, "PAYLOAD"));
      assertLinkRowCount(conn, POLICY_TOGGLE, table, 1);
    }
  }

  @Test
  public void alterTableAddSucceedsOnRetryAfterDescriptorIsCreated() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY)" + encodingClause());
      String ddl = "ALTER TABLE " + table + " ADD PAYLOAD PROTOBUF<" + POLICY_TOGGLE + ">";
      try {
        conn.createStatement().execute(ddl);
        fail("ALTER TABLE ADD PROTOBUF column with unregistered descriptor must be rejected");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND.getErrorCode(),
          e.getErrorCode());
      }
      registerDescriptorsIdempotent(conn, POLICY_TOGGLE);
      conn.createStatement().execute(ddl);
      assertEquals(POLICY_TOGGLE, getProtoMessageName(conn, table, "PAYLOAD"));
      assertLinkRowCount(conn, POLICY_TOGGLE, table, 1);
    }
  }

  @Test
  public void alterTableAddMultipleProtobufColumnsWritesAllLinks() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY)" + encodingClause());
      conn.createStatement().execute("ALTER TABLE " + table + " ADD " + "A PROTOBUF<" + ACCOUNT
        + ">, D PROTOBUF<" + DELIVERY_WINDOW_RECORD + ">");
      assertEquals(ACCOUNT, getProtoMessageName(conn, table, "A"));
      assertEquals(DELIVERY_WINDOW_RECORD, getProtoMessageName(conn, table, "D"));
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 1);
    }
  }

  @Test
  public void dropProtobufDescriptorWithReferencingColumnRejected() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      try {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
        fail("DROP PROTOBUF DESCRIPTOR must reject an in-use descriptor");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_IN_USE.getErrorCode(), e.getErrorCode());
      }
      assertTrue(isDescriptorRegistered(conn, ACCOUNT));
    }
  }

  @Test
  public void dropProtobufDescriptorAfterDroppingTableSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      conn.createStatement().execute("DROP TABLE " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
    }
  }

  @Test
  public void dropProtobufDescriptorAfterDroppingColumnSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (" + "ID BIGINT NOT NULL PRIMARY KEY, " + "A PROTOBUF<"
          + ACCOUNT + ">, " + "D PROTOBUF<" + DELIVERY_WINDOW_RECORD + ">)" + encodingClause());
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 1);

      conn.createStatement().execute("ALTER TABLE " + table + " DROP COLUMN A");
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 1);

      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      try {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + DELIVERY_WINDOW_RECORD);
        fail("DELIVERY_WINDOW_RECORD is still referenced by column D");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_IN_USE.getErrorCode(), e.getErrorCode());
      }
    }
  }

  @Test
  public void dropProtobufDescriptorAfterDroppingTableWithMixedColumnsSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD, TOKEN_REVOCATION);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (" + "ID BIGINT NOT NULL PRIMARY KEY, "
          + "NAME VARCHAR(64), " + "AGE INTEGER, " + "A PROTOBUF<" + ACCOUNT + ">, " + "D PROTOBUF<"
          + DELIVERY_WINDOW_RECORD + ">, " + "T PROTOBUF<" + TOKEN_REVOCATION + ">)"
          + encodingClause());
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 1);
      assertLinkRowCount(conn, TOKEN_REVOCATION, table, 1);

      conn.createStatement().execute("DROP TABLE " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 0);
      assertLinkRowCount(conn, TOKEN_REVOCATION, table, 0);

      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + DELIVERY_WINDOW_RECORD);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + TOKEN_REVOCATION);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
      assertFalse(isDescriptorRegistered(conn, DELIVERY_WINDOW_RECORD));
      assertFalse(isDescriptorRegistered(conn, TOKEN_REVOCATION));
    }
  }

  @Test
  public void droppingNonProtobufColumnDoesNotTouchProtobufLinkRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (" + "ID BIGINT NOT NULL PRIMARY KEY, "
          + "NAME VARCHAR(64), " + "PAYLOAD PROTOBUF<" + ACCOUNT + ">)" + encodingClause());
      assertLinkRowCount(conn, ACCOUNT, table, 1);

      conn.createStatement().execute("ALTER TABLE " + table + " DROP COLUMN NAME");
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      try {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
        fail("PAYLOAD PROTOBUF column still references ACCOUNT");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_IN_USE.getErrorCode(), e.getErrorCode());
      }
    }
  }

  @Test
  public void droppingAllProtobufColumnsThenDroppingTableIsNoOpForLinks() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (" + "ID BIGINT NOT NULL PRIMARY KEY, " + "A PROTOBUF<"
          + ACCOUNT + ">, " + "D PROTOBUF<" + DELIVERY_WINDOW_RECORD + ">)" + encodingClause());
      conn.createStatement().execute("ALTER TABLE " + table + " DROP COLUMN A");
      conn.createStatement().execute("ALTER TABLE " + table + " DROP COLUMN D");
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 0);

      conn.createStatement().execute("DROP TABLE " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 0);

      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + DELIVERY_WINDOW_RECORD);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
      assertFalse(isDescriptorRegistered(conn, DELIVERY_WINDOW_RECORD));
    }
  }

  @Test
  public void reCreateTableWithSameDescriptorAfterDropSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      String create = "CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + ACCOUNT + ">)" + encodingClause();
      conn.createStatement().execute(create);
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      conn.createStatement().execute("DROP TABLE " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      conn.createStatement().execute(create);
      assertLinkRowCount(conn, ACCOUNT, table, 1);
    }
  }

  @Test
  public void dropProtobufDescriptorAfterDroppingViewSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      String view = uniqueViewName();
      conn.createStatement().execute(
        "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, BASE_PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      conn.createStatement().execute("CREATE VIEW " + view + " (" + "VIEW_PAYLOAD PROTOBUF<"
        + DELIVERY_WINDOW_RECORD + ">) AS SELECT * FROM " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, view, 1);

      conn.createStatement().execute("DROP VIEW " + view);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, view, 0);
      assertLinkRowCount(conn, ACCOUNT, table, 1);

      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + DELIVERY_WINDOW_RECORD);
      assertFalse(isDescriptorRegistered(conn, DELIVERY_WINDOW_RECORD));
    }
  }

  @Test
  public void dropTableCascadeKeepsViewDescriptorInUseUntilViewIsDropped() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      String view = uniqueViewName();
      conn.createStatement().execute(
        "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, BASE_PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      conn.createStatement().execute("CREATE VIEW " + view + " (VIEW_PAYLOAD PROTOBUF<"
        + DELIVERY_WINDOW_RECORD + ">) AS SELECT * FROM " + table);

      conn.createStatement().execute("DROP TABLE " + table + " CASCADE");

      assertLinkRowCount(conn, ACCOUNT, table, 0);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
      assertDropDescriptorRejectedAsInUse(conn, DELIVERY_WINDOW_RECORD);
    }
  }

  @Test
  public void linkWithoutCatalogColumnDoesNotBlockDropDescriptor() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      String missing = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, A PROTOBUF<"
          + DELIVERY_WINDOW_RECORD + ">, B PROTOBUF<" + DELIVERY_WINDOW_RECORD + ">)"
          + encodingClause());
      upsertLinkRow(conn, ACCOUNT, missing, "PAYLOAD");
      upsertLinkRow(conn, ACCOUNT, table, "A");
      upsertLinkRow(conn, ACCOUNT, table, "NOT_A_COLUMN");

      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
      assertDropDescriptorRejectedAsInUse(conn, DELIVERY_WINDOW_RECORD);
    }
  }

  @Test
  public void createAndAddColumnRejectDescriptorDroppedBehindClientCache() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, A PROTOBUF<" + ACCOUNT + ">)" + encodingClause());
      // Deleting the row directly leaves the client's cached descriptor in place.
      try (Table descriptors = getUtility().getConnection()
        .getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME_BYTES))) {
        descriptors.delete(new Delete(Bytes.toBytes(ACCOUNT)));
      }
      String rejected = uniqueTableName();
      assertFailsWithCode(conn, "CREATE TABLE " + rejected
        + " (ID BIGINT NOT NULL PRIMARY KEY, A PROTOBUF<" + ACCOUNT + ">)" + encodingClause(),
        SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND);
      assertTableDoesNotExist(conn, rejected);
      assertFailsWithCode(conn, "ALTER TABLE " + table + " ADD B PROTOBUF<" + ACCOUNT + ">",
        SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND);
      assertLinkRowCount(conn, ACCOUNT, rejected, 0);
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      conn.createStatement().execute("DROP TABLE " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      registerDescriptorsIdempotent(conn, ACCOUNT);
    }
  }

  @Test
  public void linkTableUnavailableFailsCreateAndAlterButNotDrop() throws Exception {
    TableName linkTable =
      TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_PROTO_DESC_LINK_NAME_BYTES);
    Admin admin = getUtility().getAdmin();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      String retried = uniqueTableName();
      String create = "CREATE TABLE " + retried + " (ID BIGINT NOT NULL PRIMARY KEY, A PROTOBUF<"
        + ACCOUNT + ">)" + encodingClause();
      conn.createStatement().execute("CREATE TABLE " + table
        + " (ID BIGINT NOT NULL PRIMARY KEY, A PROTOBUF<" + ACCOUNT + ">)" + encodingClause());
      admin.disableTable(linkTable);
      try {
        assertFailsWithCode(conn, create, SQLExceptionCode.UNABLE_TO_WRITE_PROTO_DESC_LINK);
        assertFailsWithCode(conn, "ALTER TABLE " + table + " ADD B PROTOBUF<" + ACCOUNT + ">",
          SQLExceptionCode.UNABLE_TO_WRITE_PROTO_DESC_LINK);
        conn.createStatement().execute("DROP TABLE " + table);
      } finally {
        admin.enableTable(linkTable);
      }
      assertTableDoesNotExist(conn, retried);
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);

      registerDescriptorsIdempotent(conn, ACCOUNT);
      conn.createStatement().execute(create);
      assertLinkRowCount(conn, ACCOUNT, retried, 1);
    }
  }

  @Test
  public void linkRowsShareCellTimestampWithCatalogColumnRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, A PROTOBUF<" + ACCOUNT
          + ">, B PROTOBUF<" + DELIVERY_WINDOW_RECORD + ">)" + encodingClause());
      conn.createStatement()
        .execute("ALTER TABLE " + table + " ADD C PROTOBUF<" + ACCOUNT + ">, D VARCHAR");

      assertLinkTimestampMatchesCatalog(ACCOUNT, table, "A");
      assertLinkTimestampMatchesCatalog(DELIVERY_WINDOW_RECORD, table, "B");
      assertLinkTimestampMatchesCatalog(ACCOUNT, table, "C");
    }
  }

  @Test
  public void dropColumnRemovesLinksOfCoveringAndDroppedIndexes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      String covering = generateUniqueName();
      String keyed = generateUniqueName();
      conn.createStatement().execute(
        "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, NAME VARCHAR, A PROTOBUF<"
          + ACCOUNT + ">, B PROTOBUF<" + DELIVERY_WINDOW_RECORD + ">)" + encodingClause());
      conn.createStatement()
        .execute("CREATE INDEX " + covering + " ON " + table + "(NAME) INCLUDE(A, B)");
      conn.createStatement()
        .execute("CREATE INDEX " + keyed + " ON " + table + "(NAME) INCLUDE(B)");
      assertLinkRowCount(conn, ACCOUNT, covering, 1);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, covering, 1);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, keyed, 1);

      conn.createStatement().execute("ALTER TABLE " + table + " DROP COLUMN A");
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      assertLinkRowCount(conn, ACCOUNT, covering, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, covering, 1);

      conn.createStatement().execute("ALTER TABLE " + table + " DROP COLUMN NAME");
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, covering, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, keyed, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, table, 1);

      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
    }
  }

  @Test
  public void droppingViewInheritingProtobufButAddingNoneCleanupIsNoOp() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      String view = uniqueViewName();
      conn.createStatement().execute(
        "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, BASE_PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      conn.createStatement()
        .execute("CREATE VIEW " + view + " (VIEW_LABEL VARCHAR) AS SELECT * FROM " + table);

      assertLinkRowCount(conn, ACCOUNT, table, 1);
      assertLinkRowCount(conn, ACCOUNT, view, 0);

      conn.createStatement().execute("DROP VIEW " + view);

      assertLinkRowCount(conn, ACCOUNT, table, 1);

      try {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
        fail("Base table's PROTOBUF column still references ACCOUNT");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_IN_USE.getErrorCode(), e.getErrorCode());
      }

      conn.createStatement().execute("DROP TABLE " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 0);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
    }
  }

  @Test
  public void dropProtobufDescriptorAfterDroppingSaltedTableAndViewSucceeds() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT, DELIVERY_WINDOW_RECORD);
      String table = uniqueTableName();
      String view = uniqueViewName();

      conn.createStatement().execute(
        "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, BASE_PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause() + ", SALT_BUCKETS=4");
      conn.createStatement().execute("CREATE VIEW " + view + " (" + "VIEW_PAYLOAD PROTOBUF<"
        + DELIVERY_WINDOW_RECORD + ">) AS SELECT * FROM " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 1);
      assertLinkRowCount(conn, ACCOUNT, view, 0);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, view, 1);

      conn.createStatement().execute("DROP VIEW " + view);
      assertLinkRowCount(conn, DELIVERY_WINDOW_RECORD, view, 0);
      assertLinkRowCount(conn, ACCOUNT, table, 1);

      conn.createStatement().execute("DROP TABLE " + table);
      assertLinkRowCount(conn, ACCOUNT, table, 0);

      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + DELIVERY_WINDOW_RECORD);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
      assertFalse(isDescriptorRegistered(conn, DELIVERY_WINDOW_RECORD));
    }
  }

  @Test
  public void dropProtobufDescriptorAfterTenantScopedViewLifecycleSucceeds() throws Exception {
    String tenantId = generateUniqueName();
    String table = uniqueTableName();
    String view = uniqueViewName();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (" + "TENANT_ID VARCHAR NOT NULL, ID BIGINT NOT NULL, "
          + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true," + encodingClause());
    }
    try (Connection tenantConn = getTenantConnection(tenantId)) {
      tenantConn.createStatement().execute("CREATE VIEW " + view + " (" + "TENANT_PAYLOAD PROTOBUF<"
        + ACCOUNT + ">) AS SELECT * FROM " + table);

      assertLinkRowCountForTenant(tenantConn, ACCOUNT, view, tenantId, 1);

      tenantConn.createStatement().execute("DROP VIEW " + view);
      assertLinkRowCountForTenant(tenantConn, ACCOUNT, view, tenantId, 0);
    }
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement().execute("DROP TABLE " + table);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertFalse(isDescriptorRegistered(conn, ACCOUNT));
    }
  }

  private static Connection getTenantConnection(String tenantId) throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    return DriverManager.getConnection(getUrl(), props);
  }

  private static void assertLinkRowCountForTenant(Connection conn, String protobufName,
    String tableName, String tenantId, int expected) throws SQLException {
    String sql = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA + "."
      + "\"" + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESC_LINK_TABLE + "\"" + " WHERE "
      + PhoenixDatabaseMetaData.PROTOBUF_NAME + " = ? AND " + PhoenixDatabaseMetaData.TENANT_ID
      + " = ? AND " + PhoenixDatabaseMetaData.TABLE_NAME + " = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, protobufName);
      ps.setString(2, tenantId);
      ps.setString(3, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
      }
    }
  }

  @Test
  public void qualifiedProtobufMessageNamePreservedCaseSensitive() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<"
          + ACCOUNT + ">)" + encodingClause());
      assertEquals(ACCOUNT, getProtoMessageName(conn, table, "PAYLOAD"));
    }
  }

  @Test
  public void createIndexIfNotExistsRetryMatchesOriginalLinkRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      String table = uniqueTableName();
      String index = generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, NAME VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + encodingClause());
      String indexDdl =
        "CREATE INDEX IF NOT EXISTS " + index + " ON " + table + "(NAME) INCLUDE(PAYLOAD)";
      conn.createStatement().execute(indexDdl);
      int afterCreate = linkRowCount(conn, ACCOUNT, index);
      conn.createStatement().execute(indexDdl);
      assertEquals(afterCreate, linkRowCount(conn, ACCOUNT, index));
      conn.createStatement().execute("DROP INDEX " + index + " ON " + table);
      assertLinkRowCount(conn, ACCOUNT, index, 0);
    }
  }

  @Test
  public void tenantViewAlterAddProtobufColumnWritesTenantScopedLinkRow() throws Exception {
    String tenantId = generateUniqueName();
    String table = uniqueTableName();
    String view = uniqueViewName();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      registerDescriptorsIdempotent(conn, ACCOUNT);
      conn.createStatement()
        .execute("CREATE TABLE " + table + " (" + "TENANT_ID VARCHAR NOT NULL, ID BIGINT NOT NULL, "
          + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true," + encodingClause());
    }
    try (Connection tenantConn = getTenantConnection(tenantId)) {
      tenantConn.createStatement()
        .execute("CREATE VIEW " + view + " (LABEL VARCHAR) AS SELECT * FROM " + table);
      tenantConn.createStatement()
        .execute("ALTER VIEW " + view + " ADD V1 PROTOBUF<" + ACCOUNT + ">");
      assertLinkRowCountForTenant(tenantConn, ACCOUNT, view, tenantId, 1);
      tenantConn.createStatement().execute("DROP VIEW " + view);
    }
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      assertLinkRowCount(conn, ACCOUNT, view, 0);
    }
  }

  private static void assertFailsWithCode(Connection conn, String sql, SQLExceptionCode code) {
    try {
      conn.createStatement().execute(sql);
      fail("Expected " + code + " for: " + sql);
    } catch (SQLException e) {
      assertEquals(code.getErrorCode(), e.getErrorCode());
    }
  }

  private static int linkRowCount(Connection conn, String protobufName, String tableName)
    throws SQLException {
    String sql = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA + ".\""
      + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESC_LINK_TABLE + "\" WHERE "
      + PhoenixDatabaseMetaData.PROTOBUF_NAME + " = ? AND " + PhoenixDatabaseMetaData.TABLE_NAME
      + " = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, protobufName);
      ps.setString(2, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getInt(1);
      }
    }
  }

  private static void upsertLinkRow(Connection conn, String protobufName, String tableName,
    String columnName) throws SQLException {
    String sql = "UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA + ".\""
      + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESC_LINK_TABLE + "\"("
      + PhoenixDatabaseMetaData.PROTOBUF_NAME + "," + PhoenixDatabaseMetaData.TABLE_NAME + ","
      + PhoenixDatabaseMetaData.COLUMN_FAMILY + "," + PhoenixDatabaseMetaData.COLUMN_NAME
      + ") VALUES (?, ?, '0', ?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, protobufName);
      ps.setString(2, tableName);
      ps.setString(3, columnName);
      ps.executeUpdate();
    }
    conn.commit();
  }

  private static void assertLinkTimestampMatchesCatalog(String protobufName, String tableName,
    String columnName) throws Exception {
    try (
      Table links = getUtility().getConnection()
        .getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_PROTO_DESC_LINK_NAME_BYTES));
      Table catalog = getUtility().getConnection()
        .getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES))) {
      Result link = links.get(new Get(MetaDataUtil.getProtoDescLinkKey(Bytes.toBytes(protobufName),
        null, null, Bytes.toBytes(tableName), Bytes.toBytes("0"), Bytes.toBytes(columnName))));
      Result column =
        catalog.get(new Get(SchemaUtil.getColumnKey(null, null, tableName, columnName, "0")));
      assertFalse(link.isEmpty());
      assertEquals(
        column.getColumnLatestCell(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
          PhoenixDatabaseMetaData.PROTOBUF_DESCRIPTOR_NAME_BYTES).getTimestamp(),
        link.rawCells()[0].getTimestamp());
    }
  }

  private static void assertDropDescriptorRejectedAsInUse(Connection conn, String protobufName) {
    try {
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + protobufName);
      fail("DROP PROTOBUF DESCRIPTOR must be rejected while a column references it");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_IN_USE.getErrorCode(), e.getErrorCode());
    }
  }

  private static String getProtoMessageName(Connection conn, String tableName, String columnName)
    throws SQLException {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    PTable pTable = pconn.getTable(tableName);
    try {
      return pTable.getColumnForColumnName(columnName).getProtoMessageName();
    } catch (ColumnNotFoundException e) {
      return null;
    }
  }

  private static void assertLinkRowCount(Connection conn, String protobufName, String tableName,
    int expected) throws SQLException {
    String sql = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA + "."
      + "\"" + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESC_LINK_TABLE + "\"" + " WHERE "
      + PhoenixDatabaseMetaData.PROTOBUF_NAME + " = ? AND " + PhoenixDatabaseMetaData.TABLE_NAME
      + " = ?";
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
    String sql = "SELECT 1 FROM " + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME
      + " WHERE " + PhoenixDatabaseMetaData.PROTOBUF_NAME + " = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, protobufName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static void assertTableDoesNotExist(Connection conn, String tableName) {
    try {
      conn.unwrap(PhoenixConnection.class).getTableNoCache(null, tableName);
      fail("Table " + tableName + " should not exist after rejected CREATE");
    } catch (TableNotFoundException expected) {
    } catch (SQLException e) {
      fail("Unexpected SQLException resolving " + tableName + ": " + e.getMessage());
    }
  }
}
