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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.AlterProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufBundleRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorMutationResponse;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorService;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistry;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistryHolder;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorResolver;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

@Category(ParallelStatsDisabledTest.class)
public class ProtobufDescriptorDDLIT extends ParallelStatsDisabledIT {

  private static final String BUNDLE_RESOURCE = "/protobuf/acmewidgets/acmewidgets_v1.desc";

  private static final String ACCOUNT = "com.example.acmewidgets.account.v1.Account";
  private static final String ACCOUNT_SYNC_RESPONSE =
    "com.example.acmewidgets.account.v1.AccountSyncResponse";
  private static final String CATALOG_SYNC_HEADER =
    "com.example.acmewidgets.common.v1.CatalogSyncHeader";
  private static final String DELIVERY_WINDOW_RECORD =
    "com.example.acmewidgets.delivery.v1.DeliveryWindowRecord";
  private static final String TOKEN_REVOCATION =
    "com.example.acmewidgets.auth.v1.proto2.TokenRevocation";
  private static final String CUSTOMER = "com.example.acmewidgets.orders.v1.Customer";
  private static final String COMMON_FILE = "widget_common.proto";
  private static final String UNKNOWN_MESSAGE = "com.example.acmewidgets.account.v1.NotInTheBundle";

  private static final List<String> KNOWN_FIXTURE_NAMES = Arrays.asList(ACCOUNT,
    ACCOUNT_SYNC_RESPONSE, CATALOG_SYNC_HEADER, DELIVERY_WINDOW_RECORD, TOKEN_REVOCATION, CUSTOMER);

  private static String fixtureFileUrl() {
    URL res = ProtobufDescriptorDDLIT.class.getResource(BUNDLE_RESOURCE);
    assertNotNull("Test fixture " + BUNDLE_RESOURCE + " not on the classpath — Maven "
      + "test-resources should have copied it from src/test/resources/", res);
    return res.toString();
  }

  private Properties testProps() {
    return PropertiesUtil.deepCopy(TEST_PROPERTIES);
  }

  @After
  public void cleanupDescriptors() throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps());
      Statement stmt = conn.createStatement()) {
      for (String name : KNOWN_FIXTURE_NAMES) {
        stmt.execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + name);
      }
    }
  }

  @Test
  public void createDescriptorRoundTripsToSystemTable() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .execute("CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      assertSystemTableHasDescriptor(conn, ACCOUNT);

      String unpackaged = "U" + generateUniqueName();
      java.nio.file.Path fixture = writeFixture(FileDescriptorSet
        .newBuilder().addFile(FileDescriptorProto.newBuilder().setName(unpackaged + ".proto")
          .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName(unpackaged)))
        .build());
      Properties scnProps = testProps();
      scnProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
        Long.toString(EnvironmentEdgeManager.currentTimeMillis()));
      try (Connection scnConn = DriverManager.getConnection(getUrl(), scnProps)) {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + unpackaged + " FROM '" + fixture.toUri() + "'");
        assertSystemTableHasDescriptor(conn, unpackaged);
        assertNotNull(
          scnConn.unwrap(PhoenixConnection.class).getProtoDescriptorRegistry().lookup(unpackaged));
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + unpackaged);
        Files.deleteIfExists(fixture);
      }
    }
  }

  @Test
  public void createDescriptorIfNotExistsIsIdempotent() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String sql =
        "CREATE PROTOBUF DESCRIPTOR IF NOT EXISTS " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'";
      conn.createStatement().execute(sql);
      conn.createStatement().execute(sql);
      assertSystemTableHasDescriptor(conn, ACCOUNT);
    }
  }

  @Test
  public void createDescriptorRejectsDuplicateWithoutIfNotExists() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .execute("CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      SQLException ex = expectSQLException(conn,
        "CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_ALREADY_EXISTS.getErrorCode(),
        ex.getErrorCode());

      ex = expectSQLException(conn, "CREATE PROTOBUF BUNDLE (" + CUSTOMER + ", " + ACCOUNT
        + ") FROM '" + fixtureFileUrl() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_ALREADY_EXISTS.getErrorCode(),
        ex.getErrorCode());
      assertSystemTableDoesNotHaveDescriptor(conn, CUSTOMER);
    }
  }

  @Test
  public void createDescriptorRejectsNameNotInFixture() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      SQLException ex = expectSQLException(conn,
        "CREATE PROTOBUF DESCRIPTOR " + UNKNOWN_MESSAGE + " FROM '" + fixtureFileUrl() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE.getErrorCode(),
        ex.getErrorCode());
      ex = expectSQLException(conn,
        "ALTER PROTOBUF DESCRIPTOR " + UNKNOWN_MESSAGE + " FROM '" + fixtureFileUrl() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE.getErrorCode(),
        ex.getErrorCode());
      assertSystemTableDoesNotHaveDescriptor(conn, UNKNOWN_MESSAGE);
    }
  }

  @Test
  public void alterDescriptorOverwritesRow() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .execute("CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");

      conn.createStatement()
        .execute("ALTER PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      assertSystemTableHasDescriptor(conn, ACCOUNT);
    }
  }

  @Test
  public void alterDescriptorCompatibleEvolutionRoundTripsWithVerifiedNewShape() throws Exception {
    String widget = "evolve.it.WidgetShape";
    java.nio.file.Path oldFixture = writeWidgetShapeFixture(null, Type.TYPE_INT32);
    java.nio.file.Path evolvedFixture = writeWidgetShapeFixture("description", Type.TYPE_INT32);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + widget + " FROM '" + oldFixture.toUri() + "'");

        Descriptors.Descriptor beforeAlter = readDescriptorFromSystemTable(conn, widget);
        assertEquals("old descriptor has 2 fields", 2, beforeAlter.getFields().size());
        assertEquals(Descriptors.FieldDescriptor.Type.STRING,
          beforeAlter.findFieldByName("id").getType());
        assertEquals(Descriptors.FieldDescriptor.Type.INT32,
          beforeAlter.findFieldByName("count").getType());
        assertNull("old descriptor should not have 'description' yet",
          beforeAlter.findFieldByName("description"));

        conn.createStatement().execute(
          "ALTER PROTOBUF DESCRIPTOR " + widget + " FROM '" + evolvedFixture.toUri() + "'");

        Descriptors.Descriptor afterAlter = readDescriptorFromSystemTable(conn, widget);
        assertEquals("evolved descriptor has 3 fields (added 'description', kept id + count)", 3,
          afterAlter.getFields().size());
        assertEquals("id preserved as STRING", Descriptors.FieldDescriptor.Type.STRING,
          afterAlter.findFieldByName("id").getType());
        assertEquals("count preserved as INT32", Descriptors.FieldDescriptor.Type.INT32,
          afterAlter.findFieldByName("count").getType());
        assertNotNull("description added", afterAlter.findFieldByName("description"));
        assertEquals("description is STRING", Descriptors.FieldDescriptor.Type.STRING,
          afterAlter.findFieldByName("description").getType());
        assertEquals("description tag = 3", 3,
          afterAlter.findFieldByName("description").getNumber());
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + widget);
      }
    } finally {
      Files.deleteIfExists(oldFixture);
      Files.deleteIfExists(evolvedFixture);
    }
  }

  @Test
  public void concurrentAltersAreValidatedAgainstEachOther() throws Exception {
    String widget = "evolve.it.WidgetShape";
    List<String> addedFields = Arrays.asList("description", "note");
    java.nio.file.Path oldFixture = writeWidgetShapeFixture(null, Type.TYPE_INT32);
    List<java.nio.file.Path> fixtures =
      Arrays.asList(writeWidgetShapeFixture(addedFields.get(0), Type.TYPE_INT32),
        writeWidgetShapeFixture(addedFields.get(1), Type.TYPE_INT32));
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + widget + " FROM '" + oldFixture.toUri() + "'");
        CyclicBarrier start = new CyclicBarrier(2);
        List<Future<Integer>> results = new ArrayList<>();
        for (java.nio.file.Path fixture : fixtures) {
          results.add(pool.submit(() -> {
            try (Connection c = DriverManager.getConnection(getUrl(), testProps())) {
              start.await();
              c.createStatement()
                .execute("ALTER PROTOBUF DESCRIPTOR " + widget + " FROM '" + fixture.toUri() + "'");
              return 0;
            } catch (SQLException e) {
              return e.getErrorCode();
            }
          }));
        }
        List<Integer> codes = Arrays.asList(results.get(0).get(), results.get(1).get());
        assertTrue(codes.toString(), codes.contains(0));
        assertTrue(codes.toString(),
          codes.contains(SQLExceptionCode.PROTOBUF_FIELD_RENAMED.getErrorCode()));
        assertEquals(addedFields.get(codes.indexOf(0)),
          readDescriptorFromSystemTable(conn, widget).findFieldByNumber(3).getName());
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + widget);
      }
    } finally {
      pool.shutdownNow();
      Files.deleteIfExists(oldFixture);
      for (java.nio.file.Path fixture : fixtures) {
        Files.deleteIfExists(fixture);
      }
    }
  }

  @Test
  public void alterDescriptorDefaultValueChangeIsRejected() throws Exception {
    String message = "evolve.it.WidgetDefault";
    java.nio.file.Path implicitFixture = writeWidgetDefaultFixture(null);
    java.nio.file.Path explicitZeroFixture = writeWidgetDefaultFixture("0");
    java.nio.file.Path changedFixture = writeWidgetDefaultFixture("6");
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement().execute(
          "CREATE PROTOBUF DESCRIPTOR " + message + " FROM '" + implicitFixture.toUri() + "'");
        conn.createStatement().execute(
          "ALTER PROTOBUF DESCRIPTOR " + message + " FROM '" + explicitZeroFixture.toUri() + "'");
        SQLException ex = expectSQLException(conn,
          "ALTER PROTOBUF DESCRIPTOR " + message + " FROM '" + changedFixture.toUri() + "'");
        assertEquals(SQLExceptionCode.PROTOBUF_DEFAULT_VALUE_CHANGE.getErrorCode(),
          ex.getErrorCode());
        assertTrue(ex.getMessage(), ex.getMessage().contains("count"));
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + message);
      }
    } finally {
      Files.deleteIfExists(implicitFixture);
      Files.deleteIfExists(explicitZeroFixture);
      Files.deleteIfExists(changedFixture);
    }
  }

  @Test
  public void alterDescriptorNewFirstEnumValueChangesDefaultAndIsRejected() throws Exception {
    String message = "evolve.it.WidgetColor";
    java.nio.file.Path oldFixture = writeWidgetColorFixture(false);
    java.nio.file.Path newFixture = writeWidgetColorFixture(true);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + message + " FROM '" + oldFixture.toUri() + "'");
        SQLException ex = expectSQLException(conn,
          "ALTER PROTOBUF DESCRIPTOR " + message + " FROM '" + newFixture.toUri() + "'");
        assertEquals(SQLExceptionCode.PROTOBUF_DEFAULT_VALUE_CHANGE.getErrorCode(),
          ex.getErrorCode());
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + message);
      }
    } finally {
      Files.deleteIfExists(oldFixture);
      Files.deleteIfExists(newFixture);
    }
  }

  @Test
  public void alterDescriptorTypeWideningIsRejected() throws Exception {
    String widget = "evolve.it.WidgetShape";
    java.nio.file.Path oldFixture = writeWidgetShapeFixture(null, Type.TYPE_INT32);
    java.nio.file.Path widenedFixture = writeWidgetShapeFixture(null, Type.TYPE_INT64);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + widget + " FROM '" + oldFixture.toUri() + "'");
        SQLException ex = expectSQLException(conn,
          "ALTER PROTOBUF DESCRIPTOR " + widget + " FROM '" + widenedFixture.toUri() + "'");
        assertEquals(SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE.getErrorCode(),
          ex.getErrorCode());
        assertTrue(ex.getMessage(), ex.getMessage().contains("count"));
        assertEquals(Descriptors.FieldDescriptor.Type.INT32,
          readDescriptorFromSystemTable(conn, widget).findFieldByName("count").getType());
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + widget);
      }
    } finally {
      Files.deleteIfExists(oldFixture);
      Files.deleteIfExists(widenedFixture);
    }
  }

  @Test
  public void alterAcmeWidgetsTokenRevocationCompatibleEvolutionRoundTrips() throws Exception {
    String newFieldName = "audit_trail_id";
    int newFieldTag = 5;
    java.nio.file.Path evolvedFixture =
      writeTokenRevocationWithAddedField(newFieldName, newFieldTag);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement().execute(
          "CREATE PROTOBUF DESCRIPTOR " + TOKEN_REVOCATION + " FROM '" + fixtureFileUrl() + "'");

        Descriptors.Descriptor beforeAlter = readDescriptorFromSystemTable(conn, TOKEN_REVOCATION);
        assertEquals("old TokenRevocation has 4 declared fields", 4,
          beforeAlter.getFields().size());
        assertNotNull(beforeAlter.findFieldByName("tenant_id"));
        assertNotNull(beforeAlter.findFieldByName("token_id"));
        assertNotNull(beforeAlter.findFieldByName("reason"));
        assertNotNull(beforeAlter.findFieldByName("soft_revoke"));
        assertNull("audit_trail_id should not exist yet",
          beforeAlter.findFieldByName(newFieldName));

        conn.createStatement().execute("ALTER PROTOBUF DESCRIPTOR " + TOKEN_REVOCATION + " FROM '"
          + evolvedFixture.toUri() + "'");

        Descriptors.Descriptor afterAlter = readDescriptorFromSystemTable(conn, TOKEN_REVOCATION);
        assertEquals("evolved TokenRevocation has 5 declared fields", 5,
          afterAlter.getFields().size());
        assertNotNull("tenant_id preserved", afterAlter.findFieldByName("tenant_id"));
        assertNotNull("token_id preserved", afterAlter.findFieldByName("token_id"));
        assertNotNull("reason preserved", afterAlter.findFieldByName("reason"));
        assertNotNull("soft_revoke preserved", afterAlter.findFieldByName("soft_revoke"));
        Descriptors.FieldDescriptor auditField = afterAlter.findFieldByName(newFieldName);
        assertNotNull("audit_trail_id added", auditField);
        assertEquals("audit_trail_id type is STRING", Descriptors.FieldDescriptor.Type.STRING,
          auditField.getType());
        assertEquals("audit_trail_id tag is 5", newFieldTag, auditField.getNumber());
        assertFalse("audit_trail_id is optional (proto2 default for new fields)",
          auditField.isRequired());
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + TOKEN_REVOCATION);
      }
    } finally {
      Files.deleteIfExists(evolvedFixture);
    }
  }

  @Test
  public void protoValueDescriptorRefreshAfterAlterReturnsNewField() throws Exception {
    String widget = "evolve.it.WidgetShape";
    java.nio.file.Path oldFixture = writeWidgetShapeFixture(null, Type.TYPE_INT32);
    java.nio.file.Path newFixture = writeWidgetShapeFixture("description", Type.TYPE_INT32);
    Descriptors.Descriptor oldDesc = buildLocalDescriptor(oldFixture, "WidgetShape");
    Descriptors.Descriptor newDesc = buildLocalDescriptor(newFixture, "WidgetShape");
    String table = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + widget + " FROM '" + oldFixture.toUri() + "'");
        conn.createStatement().execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + widget + ">)");
        String[] oldIds = { "w1", "w2", "w3", "w4", "w5", "w6" };
        int[] oldCounts = { 10, 20, 30, 40, 50, 60 };
        try (PreparedStatement ps =
          conn.prepareStatement("UPSERT INTO " + table + " (ID, PAYLOAD) VALUES (?, ?)")) {
          for (int i = 0; i < oldIds.length; i++) {
            byte[] oldPayload =
              DynamicMessage.newBuilder(oldDesc).setField(oldDesc.findFieldByName("id"), oldIds[i])
                .setField(oldDesc.findFieldByName("count"), oldCounts[i]).build().toByteArray();
            ps.setLong(1, i + 1L);
            ps.setBytes(2, oldPayload);
            ps.execute();
          }
        }
        conn.commit();
        try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'id', 'VARCHAR'), "
            + "PROTO_VALUE(PAYLOAD, 'count', 'INTEGER') FROM " + table + " ORDER BY ID ASC")) {
          for (int i = 0; i < oldIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(i + 1L, rs.getLong(1));
            assertEquals(oldIds[i], rs.getString(2));
            assertEquals(oldCounts[i], rs.getInt(3));
          }
          assertFalse(rs.next());
        }
        conn.createStatement()
          .execute("ALTER PROTOBUF DESCRIPTOR " + widget + " FROM '" + newFixture.toUri() + "'");
        String[] newIds = { "n1", "n2", "n3", "n4", "n5", "n6" };
        String[] descriptions = { "d-a", "d-b", "d-c", "d-d", "d-e", "d-f" };
        try (PreparedStatement ps =
          conn.prepareStatement("UPSERT INTO " + table + " (ID, PAYLOAD) VALUES (?, ?)")) {
          for (int i = 0; i < newIds.length; i++) {
            byte[] newPayload =
              DynamicMessage.newBuilder(newDesc).setField(newDesc.findFieldByName("id"), newIds[i])
                .setField(newDesc.findFieldByName("count"), 1000 + i)
                .setField(newDesc.findFieldByName("description"), descriptions[i]).build()
                .toByteArray();
            ps.setLong(1, 10L + i);
            ps.setBytes(2, newPayload);
            ps.execute();
          }
        }
        conn.commit();
        try (Statement stmt = conn.createStatement();
          ResultSet rs =
            stmt.executeQuery("SELECT ID, PROTO_VALUE(PAYLOAD, 'description', 'VARCHAR') FROM "
              + table + " WHERE PROTO_VALUE(PAYLOAD, 'description', 'VARCHAR') IS NOT NULL"
              + " ORDER BY ID ASC")) {
          for (int i = 0; i < descriptions.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(10L + i, rs.getLong(1));
            assertEquals(descriptions[i], rs.getString(2));
          }
          assertFalse(rs.next());
        }
      } finally {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("DROP TABLE IF EXISTS " + table);
          stmt.execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + widget);
        }
      }
    } finally {
      Files.deleteIfExists(oldFixture);
      Files.deleteIfExists(newFixture);
    }
  }

  @Test
  public void queryAndIndexOnFieldAddedElsewhereReloadDescriptor() throws Exception {
    // Unique name, so the per-name forced-refresh rate limit is not shared with other tests.
    String messageName = "W" + generateUniqueName();
    String fqName = "refresh.it." + messageName;
    FileDescriptorSet v1 = refreshFixture(messageName, false);
    FileDescriptorSet v2 = refreshFixture(messageName, true);
    Descriptors.Descriptor v2Desc =
      Descriptors.FileDescriptor.buildFrom(v2.getFile(0), new Descriptors.FileDescriptor[0])
        .findMessageTypeByName(messageName);
    String table = generateUniqueName();
    String index = generateUniqueName();
    java.nio.file.Path v1File = Files.createTempFile("refresh-it-v1-", ".desc");
    Files.write(v1File, v1.toByteArray());
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + fqName + " FROM '" + v1File.toUri() + "'");
        conn.createStatement().execute("CREATE TABLE " + table
          + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD PROTOBUF<" + fqName + ">)");
        upsertPayload(conn, table, 1, refreshPayload(v2Desc, "w1", null));
        // Warm the cache with v1 on both the compile and the scan path.
        try (ResultSet rs = conn.createStatement()
          .executeQuery("SELECT PROTO_VALUE(PAYLOAD, 'id', 'VARCHAR') FROM " + table)) {
          assertTrue(rs.next());
          assertEquals("w1", rs.getString(1));
        }
        assertNull(cachedField(fqName, "note"));

        try (PreparedStatement ps = conn
          .prepareStatement("UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME
            + " (PROTOBUF_NAME, FILE_DESCRIPTOR_SET) VALUES (?, ?)")) {
          ps.setString(1, fqName);
          ps.setBytes(2, v2.toByteArray());
          ps.executeUpdate();
        }
        conn.commit();
        assertNull("cache still holds v1", cachedField(fqName, "note"));

        upsertPayload(conn, table, 2, refreshPayload(v2Desc, "w2", "n2"));
        try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT ID FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'note', 'VARCHAR') = 'n2'")) {
          assertTrue(rs.next());
          assertEquals(2L, rs.getLong(1));
          assertFalse(rs.next());
        }
        assertNotNull("cache reloaded to v2", cachedField(fqName, "note"));

        conn.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (PROTO_VALUE(PAYLOAD, 'note', 'VARCHAR'))");
        // Index maintenance evaluates the new field on the region server.
        upsertPayload(conn, table, 3, refreshPayload(v2Desc, "w3", "n3"));
        String byNote = "SELECT /*+ INDEX(" + table + " " + index + ") */ ID FROM " + table
          + " WHERE PROTO_VALUE(PAYLOAD, 'note', 'VARCHAR') = 'n3'";
        try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + byNote)) {
          StringBuilder plan = new StringBuilder();
          while (rs.next()) {
            plan.append(rs.getString(1)).append('\n');
          }
          assertTrue(plan.toString(), plan.toString().contains(index));
        }
        try (ResultSet rs = conn.createStatement().executeQuery(byNote)) {
          assertTrue(rs.next());
          assertEquals(3L, rs.getLong(1));
          assertFalse(rs.next());
        }
      } finally {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("DROP TABLE IF EXISTS " + table);
          stmt.execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + fqName);
        }
      }
    } finally {
      Files.deleteIfExists(v1File);
    }
  }

  private static Descriptors.FieldDescriptor cachedField(String fqName, String field)
    throws SQLException {
    ProtoDescriptorRegistry registry = ProtoDescriptorRegistryHolder.getIfInstalled();
    assertNotNull(registry);
    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup(fqName);
    assertNotNull(cached);
    return cached.getDescriptor().findFieldByName(field);
  }

  private static void upsertPayload(Connection conn, String table, long id, byte[] payload)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, PAYLOAD) VALUES (?, ?)")) {
      ps.setLong(1, id);
      ps.setBytes(2, payload);
      ps.executeUpdate();
    }
    conn.commit();
  }

  private static byte[] refreshPayload(Descriptors.Descriptor desc, String id, String note) {
    DynamicMessage.Builder b =
      DynamicMessage.newBuilder(desc).setField(desc.findFieldByName("id"), id);
    if (note != null) {
      b.setField(desc.findFieldByName("note"), note);
    }
    return b.build().toByteArray();
  }

  private static FileDescriptorSet refreshFixture(String messageName, boolean withNote) {
    DescriptorProto.Builder msg = DescriptorProto.newBuilder().setName(messageName)
      .addField(FieldDescriptorProto.newBuilder().setName("id").setNumber(1)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).build());
    if (withNote) {
      msg.addField(FieldDescriptorProto.newBuilder().setName("note").setNumber(2)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).build());
    }
    return FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName("refresh_" + messageName + ".proto")
        .setPackage("refresh.it").setSyntax("proto3").addMessageType(msg))
      .build();
  }

  private static Descriptors.Descriptor buildLocalDescriptor(java.nio.file.Path fixture,
    String messageName) throws Exception {
    FileDescriptorSet set = FileDescriptorSet.parseFrom(Files.readAllBytes(fixture));

    Descriptors.FileDescriptor file =
      Descriptors.FileDescriptor.buildFrom(set.getFile(0), new Descriptors.FileDescriptor[0]);
    return file.findMessageTypeByName(messageName);
  }

  @Test
  public void alterDescriptorIncompatibleEvolutionIsRejected() throws Exception {
    String widget = "evolve.it.Widget";
    java.nio.file.Path oldFixture = writeMinimalWidgetFixture("id");
    java.nio.file.Path newFixture = writeMinimalWidgetFixture(null);
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      try {
        conn.createStatement()
          .execute("CREATE PROTOBUF DESCRIPTOR " + widget + " FROM '" + oldFixture.toUri() + "'");
        SQLException ex = expectSQLException(conn,
          "ALTER PROTOBUF DESCRIPTOR " + widget + " FROM '" + newFixture.toUri() + "'");
        assertEquals(SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED.getErrorCode(),
          ex.getErrorCode());

        assertTrue("SQLException message should reference removed field 'id': " + ex.getMessage(),
          ex.getMessage().contains("id"));

        assertSystemTableHasDescriptor(conn, widget);
      } finally {
        conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + widget);
      }
    } finally {
      Files.deleteIfExists(oldFixture);
      Files.deleteIfExists(newFixture);
    }
  }

  @Test
  public void alterDescriptorRejectsTypeChangeInImportedMessage() throws Exception {
    FileDescriptorSet.Builder evolved = FileDescriptorSet.newBuilder();
    for (FileDescriptorProto file : loadBundleSet().getFileList()) {
      if (!COMMON_FILE.equals(file.getName())) {
        evolved.addFile(file);
        continue;
      }
      FileDescriptorProto.Builder common = file.toBuilder();
      for (DescriptorProto.Builder msg : common.getMessageTypeBuilderList()) {
        if ("WarehouseEndpoint".equals(msg.getName())) {
          msg.getFieldBuilder(0).setType(Type.TYPE_INT64);
        }
      }
      evolved.addFile(common);
    }
    java.nio.file.Path newFixture = writeFixture(evolved.build());
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .execute("CREATE PROTOBUF DESCRIPTOR " + CUSTOMER + " FROM '" + fixtureFileUrl() + "'");
      SQLException ex = expectSQLException(conn,
        "ALTER PROTOBUF DESCRIPTOR " + CUSTOMER + " FROM '" + newFixture.toUri() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE.getErrorCode(),
        ex.getErrorCode());
      assertTrue(ex.getMessage(), ex.getMessage().contains("WarehouseEndpoint"));
      assertTrue(ex.getMessage(), ex.getMessage().contains("warehouse_id"));
      assertEquals(loadBundleSet(), FileDescriptorSet.parseFrom(storedBytes(conn, CUSTOMER)));
    } finally {
      Files.deleteIfExists(newFixture);
    }
  }

  @Test
  public void createRejectsSetWithMissingImport() throws Exception {
    FileDescriptorSet.Builder withoutCommon = FileDescriptorSet.newBuilder();
    for (FileDescriptorProto file : loadBundleSet().getFileList()) {
      if (!COMMON_FILE.equals(file.getName())) {
        withoutCommon.addFile(file);
      }
    }
    java.nio.file.Path fixture = writeFixture(withoutCommon.build());
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      SQLException ex = expectSQLException(conn,
        "CREATE PROTOBUF DESCRIPTOR " + CUSTOMER + " FROM '" + fixture.toUri() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_PARSE_FAILED.getErrorCode(),
        ex.getErrorCode());
      assertTrue(ex.getMessage(), ex.getMessage().contains(COMMON_FILE));
      assertSystemTableDoesNotHaveDescriptor(conn, CUSTOMER);

      ex = expectSQLException(conn, "CREATE PROTOBUF BUNDLE (" + ACCOUNT + ", " + CUSTOMER
        + ") FROM '" + fixture.toUri() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_PARSE_FAILED.getErrorCode(),
        ex.getErrorCode());
      assertSystemTableDoesNotHaveDescriptor(conn, ACCOUNT);
      assertSystemTableDoesNotHaveDescriptor(conn, CUSTOMER);
    } finally {
      Files.deleteIfExists(fixture);
    }
  }

  @Test
  public void alterDescriptorIfExistsMissingIsNoOp() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement().execute(
        "ALTER PROTOBUF DESCRIPTOR IF EXISTS " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      assertSystemTableDoesNotHaveDescriptor(conn, ACCOUNT);
    }
  }

  @Test
  public void alterDescriptorRejectsMissingWithoutIfExists() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      SQLException ex = expectSQLException(conn,
        "ALTER PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND.getErrorCode(),
        ex.getErrorCode());
    }
  }

  @Test
  public void dropDescriptorRemovesRow() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .execute("CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      assertSystemTableHasDescriptor(conn, ACCOUNT);
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertSystemTableDoesNotHaveDescriptor(conn, ACCOUNT);
    }
  }

  @Test
  public void dropDescriptorIfExistsMissingIsNoOp() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + ACCOUNT);
      assertSystemTableDoesNotHaveDescriptor(conn, ACCOUNT);
    }
  }

  @Test
  public void dropDescriptorRejectsMissingWithoutIfExists() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      SQLException ex = expectSQLException(conn, "DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND.getErrorCode(),
        ex.getErrorCode());
    }
  }

  @Test
  public void dropDescriptorMakesZeroReadsAgainstSystemCatalog() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .execute("CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");

      long readsBefore = countReadsOnSystemCatalog();
      conn.createStatement().execute("DROP PROTOBUF DESCRIPTOR " + ACCOUNT);
      long readsAfter = countReadsOnSystemCatalog();

      assertEquals(
        "DROP PROTOBUF DESCRIPTOR must not trigger any reads against "
          + "SYSTEM.CATALOG (the in-use probe targets SYSTEM.PROTO_DESC_LINK; the descriptor "
          + "delete targets SYSTEM.PROTO_DESCRIPTORS). delta=" + (readsAfter - readsBefore),
        readsBefore, readsAfter);
    }
  }

  @Test
  public void createBundleRegistersAllNamesAtomically() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement().execute("CREATE PROTOBUF BUNDLE (" + ACCOUNT + ", "
        + ACCOUNT_SYNC_RESPONSE + ", " + CATALOG_SYNC_HEADER + ") FROM '" + fixtureFileUrl() + "'");
      assertSystemTableHasDescriptor(conn, ACCOUNT);
      assertSystemTableHasDescriptor(conn, ACCOUNT_SYNC_RESPONSE);
      assertSystemTableHasDescriptor(conn, CATALOG_SYNC_HEADER);
    }
  }

  @Test
  public void createBundleRejectsAtomicallyOnAnyNameMissing() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      SQLException ex = expectSQLException(conn, "CREATE PROTOBUF BUNDLE (" + ACCOUNT + ", "
        + UNKNOWN_MESSAGE + ", " + CATALOG_SYNC_HEADER + ") FROM '" + fixtureFileUrl() + "'");
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE.getErrorCode(),
        ex.getErrorCode());
      assertSystemTableDoesNotHaveDescriptor(conn, ACCOUNT);
      assertSystemTableDoesNotHaveDescriptor(conn, CATALOG_SYNC_HEADER);
    }
  }

  @Test
  public void createBundleIfNotExistsSkipsAlreadyRegistered() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .execute("CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + fixtureFileUrl() + "'");
      conn.createStatement().execute("CREATE PROTOBUF BUNDLE IF NOT EXISTS (" + ACCOUNT + ", "
        + ACCOUNT_SYNC_RESPONSE + ") FROM '" + fixtureFileUrl() + "'");
      assertSystemTableHasDescriptor(conn, ACCOUNT);
      assertSystemTableHasDescriptor(conn, ACCOUNT_SYNC_RESPONSE);
    }
  }

  @Test
  public void systemProtoDescriptorsHasNonEncodedQualifiersAndOneCellPerColumn() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      conn.createStatement()
        .executeQuery(
          "SELECT * FROM " + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME + " LIMIT 1")
        .close();

      PTable pTable = conn.unwrap(PhoenixConnection.class)
        .getTable(PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME);
      assertEquals(
        "SYSTEM.PROTO_DESCRIPTORS encoding scheme must stay on NON_ENCODED for "
          + "ProtobufDescriptorEndpointImpl.buildDescriptorPut to write Phoenix-readable cells",
        QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, pTable.getEncodingScheme());
      assertEquals(
        "SYSTEM.PROTO_DESCRIPTORS storage scheme must stay on ONE_CELL_PER_COLUMN "
          + "for the data + empty-KV cells to be written separately",
        ImmutableStorageScheme.ONE_CELL_PER_COLUMN, pTable.getImmutableStorageScheme());
    }
  }

  @Test
  public void systemProtoDescriptorsIsPinnedToSingleRegion() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps());
      Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      TableName tableName =
        SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME_BYTES,
          conn.unwrap(PhoenixConnection.class).getQueryServices().getProps());
      TableDescriptor descriptor = admin.getDescriptor(tableName);

      assertEquals(
        "SYSTEM.PROTO_DESCRIPTORS must declare SystemProtoDescriptorsSplitPolicy "
          + "(see QueryConstants.SYSTEM_PROTO_DESCRIPTORS_SPLIT_POLICY_CLASSNAME + "
          + "SystemProtoDescriptorsSplitPolicy Javadoc for the rationale)",
        QueryConstants.SYSTEM_PROTO_DESCRIPTORS_SPLIT_POLICY_CLASSNAME,
        descriptor.getRegionSplitPolicyClassName());

      int regionCount = admin.getRegions(tableName).size();
      assertEquals("SYSTEM.PROTO_DESCRIPTORS must have exactly one region (split policy is "
        + "DisabledRegionSplitPolicy-backed); found " + regionCount, 1, regionCount);
    }
  }

  @Test
  public void descriptorDdlRejectsNonLocalUrls() throws Exception {
    String hdfsUrl = hdfsFixtureUrl();
    String localPath = Paths.get(new URI(fixtureFileUrl())).toString();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      for (String url : new String[] { hdfsUrl, "s3a://bucket/acmewidgets_v1.desc",
        "abfs://container@account.dfs.core.windows.net/acmewidgets_v1.desc", localPath,
        "file://otherhost" + localPath }) {
        assertUrlNotAllowed(conn, "CREATE PROTOBUF DESCRIPTOR " + ACCOUNT + " FROM '" + url + "'");
        assertUrlNotAllowed(conn,
          "ALTER PROTOBUF DESCRIPTOR IF EXISTS " + ACCOUNT + " FROM '" + url + "'");
        assertUrlNotAllowed(conn, "CREATE PROTOBUF BUNDLE (" + ACCOUNT + ", "
          + DELIVERY_WINDOW_RECORD + ") FROM '" + url + "'");
      }
      assertSystemTableDoesNotHaveDescriptor(conn, ACCOUNT);
    }
  }

  @Test
  public void serverRejectsRequestsWithoutInlineBytes() throws Throwable {
    // A client that skips the local read and sends only a URL must not get the region
    // server to fetch it, even for a URL the server could read.
    String hdfsUrl = hdfsFixtureUrl();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      for (String url : new String[] { hdfsUrl, fixtureFileUrl() }) {
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED.getErrorCode(),
          invokeRaw(conn, ACCOUNT,
            (svc, c,
              cb) -> svc.createProtobufDescriptor(c, CreateProtobufDescriptorRequest.newBuilder()
                .setProtobufName(ACCOUNT).setSourceUrl(url).setIfNotExists(false).build(), cb))
                  .getErrorCode());
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED.getErrorCode(),
          invokeRaw(conn, ACCOUNT,
            (svc, c,
              cb) -> svc.alterProtobufDescriptor(c, AlterProtobufDescriptorRequest.newBuilder()
                .setProtobufName(ACCOUNT).setSourceUrl(url).setIfExists(true).build(), cb))
                  .getErrorCode());
        assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED.getErrorCode(),
          invokeRaw(conn, ACCOUNT,
            (svc, c,
              cb) -> svc.createProtobufBundle(c, CreateProtobufBundleRequest.newBuilder()
                .addProtobufNames(ACCOUNT).setSourceUrl(url).setIfNotExists(false).build(), cb))
                  .getErrorCode());
      }
      assertSystemTableDoesNotHaveDescriptor(conn, ACCOUNT);
    }
  }

  @FunctionalInterface
  private interface RawCall {
    void run(ProtobufDescriptorService svc, ServerRpcController controller,
      CoprocessorRpcUtils.BlockingRpcCallback<ProtobufDescriptorMutationResponse> cb);
  }

  private static ProtobufDescriptorMutationResponse invokeRaw(Connection conn, String rowKey,
    RawCall call) throws Throwable {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    byte[] key = Bytes.toBytes(rowKey);
    try (Table table = pconn.getQueryServices().getTable(
      SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME_BYTES,
        pconn.getQueryServices().getProps()).getName())) {
      Map<byte[], ProtobufDescriptorMutationResponse> results =
        table.coprocessorService(ProtobufDescriptorService.class, key, key, svc -> {
          ServerRpcController controller = new ServerRpcController();
          CoprocessorRpcUtils.BlockingRpcCallback<ProtobufDescriptorMutationResponse> cb =
            new CoprocessorRpcUtils.BlockingRpcCallback<>();
          call.run(svc, controller, cb);
          if (controller.getFailedOn() != null) {
            throw controller.getFailedOn();
          }
          return cb.get();
        });
      assertEquals(1, results.size());
      return results.values().iterator().next();
    }
  }

  private static void assertUrlNotAllowed(Connection conn, String sql) {
    SQLException ex = expectSQLException(conn, sql);
    assertEquals(sql, SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED.getErrorCode(),
      ex.getErrorCode());
  }

  private static volatile String cachedHdfsFixtureUrl;
  private static final Object HDFS_INIT_LOCK = new Object();

  private static String hdfsFixtureUrl() throws Exception {
    if (cachedHdfsFixtureUrl != null) {
      return cachedHdfsFixtureUrl;
    }
    synchronized (HDFS_INIT_LOCK) {
      if (cachedHdfsFixtureUrl != null) {
        return cachedHdfsFixtureUrl;
      }
      FileSystem hdfsFs = getUtility().getDFSCluster().getFileSystem();
      Path hdfsTarget = new Path(hdfsFs.getHomeDirectory(), "protobuf/acmewidgets_v1.desc");
      Path hdfsDir = hdfsTarget.getParent();
      if (!hdfsFs.exists(hdfsDir)) {
        hdfsFs.mkdirs(hdfsDir);
      }
      if (!hdfsFs.exists(hdfsTarget)) {
        URL localUrl = ProtobufDescriptorDDLIT.class.getResource(BUNDLE_RESOURCE);
        assertNotNull("Local fixture " + BUNDLE_RESOURCE + " missing from test classpath",
          localUrl);

        Path localPath = new Path(localUrl.getPath());
        hdfsFs.copyFromLocalFile(localPath, hdfsTarget);
      }
      cachedHdfsFixtureUrl = hdfsFs.makeQualified(hdfsTarget).toString();
      return cachedHdfsFixtureUrl;
    }
  }

  private void assertSystemTableHasDescriptor(Connection conn, String protobufName)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT FILE_DESCRIPTOR_SET FROM "
      + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME + " WHERE PROTOBUF_NAME = ?")) {
      ps.setString(1, protobufName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("Expected row for descriptor '" + protobufName + "' in "
          + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME, rs.next());
        byte[] bytes = rs.getBytes(1);
        assertNotNull(bytes);
        assertTrue("FILE_DESCRIPTOR_SET cell for '" + protobufName + "' must be non-empty "
          + "(the server's Put landed at the right qualifier)", bytes.length > 0);
      }
    }
  }

  private void assertSystemTableDoesNotHaveDescriptor(Connection conn, String protobufName)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM "
      + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME + " WHERE PROTOBUF_NAME = ?")) {
      ps.setString(1, protobufName);
      try (ResultSet rs = ps.executeQuery()) {
        assertFalse("Did not expect a row for descriptor '" + protobufName + "' in "
          + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME, rs.next());
      }
    }
  }

  private long countReadsOnSystemCatalog() {
    TableName tableName = TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
    long total = 0;
    for (HRegion region : getUtility().getMiniHBaseCluster().getRegions(tableName)) {
      total += region.getReadRequestsCount();
    }
    return total;
  }

  private static SQLException expectSQLException(Connection conn, String sql) {
    try {
      conn.createStatement().execute(sql);
      fail("Expected SQLException for: " + sql);
      return null;
    } catch (SQLException e) {
      return e;
    }
  }

  private static FileDescriptorSet loadBundleSet() throws Exception {
    return FileDescriptorSet.parseFrom(Files.readAllBytes(Paths.get(new URI(fixtureFileUrl()))));
  }

  private static java.nio.file.Path writeFixture(FileDescriptorSet set) throws Exception {
    java.nio.file.Path temp = Files.createTempFile("bundle-variant-", ".desc");
    Files.write(temp, set.toByteArray());
    return temp;
  }

  private static byte[] storedBytes(Connection conn, String protobufName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT FILE_DESCRIPTOR_SET FROM "
      + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME + " WHERE PROTOBUF_NAME = ?")) {
      ps.setString(1, protobufName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getBytes(1);
      }
    }
  }

  private static java.nio.file.Path writeMinimalWidgetFixture(String stringFieldName)
    throws Exception {
    DescriptorProto.Builder msg = DescriptorProto.newBuilder().setName("Widget");
    if (stringFieldName != null) {
      msg.addField(FieldDescriptorProto.newBuilder().setName(stringFieldName).setNumber(1)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).build());
    }
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(FileDescriptorProto.newBuilder()
      .setName("evolve.proto").setPackage("evolve.it").setSyntax("proto3").addMessageType(msg))
      .build();
    java.nio.file.Path temp = Files.createTempFile("evolve-it-widget-", ".desc");
    Files.write(temp, set.toByteArray());
    return temp;
  }

  private static java.nio.file.Path writeWidgetDefaultFixture(String defaultValue)
    throws Exception {
    FieldDescriptorProto.Builder count = FieldDescriptorProto.newBuilder().setName("count")
      .setNumber(1).setType(Type.TYPE_INT32).setLabel(Label.LABEL_OPTIONAL);
    if (defaultValue != null) {
      count.setDefaultValue(defaultValue);
    }
    FileDescriptorSet set = FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName("evolve_default.proto")
        .setPackage("evolve.it").setSyntax("proto2")
        .addMessageType(DescriptorProto.newBuilder().setName("WidgetDefault").addField(count)))
      .build();
    java.nio.file.Path temp = Files.createTempFile("evolve-it-widgetdefault-", ".desc");
    Files.write(temp, set.toByteArray());
    return temp;
  }

  private static java.nio.file.Path writeWidgetColorFixture(boolean newFirstValue)
    throws Exception {
    EnumDescriptorProto.Builder color = EnumDescriptorProto.newBuilder().setName("Color")
      .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(1));
    if (newFirstValue) {
      color.clearValue()
        .addValue(EnumValueDescriptorProto.newBuilder().setName("GREEN").setNumber(0))
        .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(1));
    }
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(FileDescriptorProto.newBuilder()
      .setName("evolve_color.proto").setPackage("evolve.it").setSyntax("proto2").addEnumType(color)
      .addMessageType(DescriptorProto.newBuilder().setName("WidgetColor")
        .addField(FieldDescriptorProto.newBuilder().setName("color").setNumber(1)
          .setType(Type.TYPE_ENUM).setTypeName(".evolve.it.Color").setLabel(Label.LABEL_OPTIONAL))))
      .build();
    java.nio.file.Path temp = Files.createTempFile("evolve-it-widgetcolor-", ".desc");
    Files.write(temp, set.toByteArray());
    return temp;
  }

  private static java.nio.file.Path writeWidgetShapeFixture(String addedField, Type countType)
    throws Exception {
    DescriptorProto.Builder msg = DescriptorProto.newBuilder().setName("WidgetShape")
      .addField(FieldDescriptorProto.newBuilder().setName("id").setNumber(1)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).build())
      .addField(FieldDescriptorProto.newBuilder().setName("count").setNumber(2).setType(countType)
        .setLabel(Label.LABEL_OPTIONAL).build());
    if (addedField != null) {
      msg.addField(FieldDescriptorProto.newBuilder().setName(addedField).setNumber(3)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).build());
    }
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(FileDescriptorProto.newBuilder()
      .setName("evolve.proto").setPackage("evolve.it").setSyntax("proto3").addMessageType(msg))
      .build();
    java.nio.file.Path temp = Files.createTempFile(
      addedField != null ? "evolve-it-widgetshape-evolved-" : "evolve-it-widgetshape-old-",
      ".desc");
    Files.write(temp, set.toByteArray());
    return temp;
  }

  private static java.nio.file.Path writeTokenRevocationWithAddedField(String fieldName,
    int fieldTag) throws Exception {
    byte[] originalBytes;
    try (InputStream in = ProtobufDescriptorDDLIT.class.getResourceAsStream(BUNDLE_RESOURCE)) {
      assertNotNull("acmewidgets_v1.desc not on classpath", in);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      originalBytes = baos.toByteArray();
    }
    FileDescriptorSet originalSet = FileDescriptorSet.parseFrom(originalBytes);

    FileDescriptorSet.Builder evolvedSet = FileDescriptorSet.newBuilder();
    boolean modified = false;
    for (FileDescriptorProto file : originalSet.getFileList()) {
      if ("voucher_revocation_sync.proto".equals(file.getName())) {
        evolvedSet.addFile(addFieldToTokenRevocation(file, fieldName, fieldTag));
        modified = true;
      } else {
        evolvedSet.addFile(file);
      }
    }
    assertTrue("voucher_revocation_sync.proto must be present in acmewidgets_v1.desc", modified);
    java.nio.file.Path temp = Files.createTempFile("token-revocation-evolved-", ".desc");
    Files.write(temp, evolvedSet.build().toByteArray());
    return temp;
  }

  private static FileDescriptorProto addFieldToTokenRevocation(FileDescriptorProto original,
    String fieldName, int fieldTag) {
    FileDescriptorProto.Builder fileBuilder = original.toBuilder().clearMessageType();
    for (DescriptorProto msg : original.getMessageTypeList()) {
      if ("TokenRevocation".equals(msg.getName())) {
        DescriptorProto updated = msg
          .toBuilder().addField(FieldDescriptorProto.newBuilder().setName(fieldName)
            .setNumber(fieldTag).setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).build())
          .build();
        fileBuilder.addMessageType(updated);
      } else {
        fileBuilder.addMessageType(msg);
      }
    }
    return fileBuilder.build();
  }

  private static Descriptors.Descriptor readDescriptorFromSystemTable(Connection conn,
    String protobufName) throws Exception {
    byte[] storedBytes = null;
    try (PreparedStatement ps = conn.prepareStatement("SELECT FILE_DESCRIPTOR_SET FROM "
      + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME + " WHERE PROTOBUF_NAME = ?")) {
      ps.setString(1, protobufName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("Expected row for descriptor '" + protobufName + "'", rs.next());
        storedBytes = rs.getBytes(1);
        assertNotNull("FILE_DESCRIPTOR_SET cell must not be null", storedBytes);
      }
    }
    Descriptors.Descriptor resolved =
      new ProtoDescriptorResolver().resolve(storedBytes, protobufName);
    assertNotNull("Stored bytes for '" + protobufName + "' must resolve to a top-level "
      + "message with that name", resolved);
    return resolved;
  }
}
