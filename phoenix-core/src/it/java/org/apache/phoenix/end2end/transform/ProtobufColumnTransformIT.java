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
package org.apache.phoenix.end2end.transform;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.coprocessor.tasks.TransformMonitorTask;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper;
import org.apache.phoenix.protobuf.text.AcmeWidgetsFixtures;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.Descriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

/**
 * A storage-format transform rewrites every column's SYSTEM.CATALOG row at cutover. The PROTOBUF
 * descriptor binding must survive that rewrite, and PROTOBUF payloads must stay readable through
 * every phase: before the transform, during dual-write, after the copy and after cutover.
 */
@Category(ParallelStatsDisabledTest.class)
public class ProtobufColumnTransformIT extends ParallelStatsDisabledIT {
  private static final String ACCOUNT = "com.example.acmewidgets.account.v1.Account";

  // Expected payload fields of rows 1..5, indexed by ID - 1.
  private static final String[] ALL_TENANTS =
    { "tenant-alpha", "tenant-beta", "tenant-gamma", "tenant-delta", "tenant-epsilon" };
  private static final boolean[] ALL_ACTIVE = { true, false, true, true, false };
  private static final String[] ALL_WAREHOUSES = { "wh-1", "wh-2", null, "wh-4", "wh-5" };

  private static Map<String, FileDescriptor> descriptorGraph;

  @BeforeClass
  public static synchronized void setup() throws Exception {
    TransformMonitorTask.disableTransformMonitorTask(true);
    Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
    serverProps.put(PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED,
      Boolean.FALSE.toString());
    Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
    clientProps.put(PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED,
      Boolean.FALSE.toString());
    clientProps.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB, "0");
    setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
      new ReadOnlyProps(clientProps.entrySet().iterator()));
    descriptorGraph = AcmeWidgetsFixtures.loadAndBuildDescriptorGraph();
  }

  @AfterClass
  public static synchronized void cleanup() {
    TransformMonitorTask.disableTransformMonitorTask(false);
  }

  @Test
  public void protoFunctionsWorkAcrossStorageFormatTransform() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
    Descriptor account = AcmeWidgetsFixtures.accountDescriptor(descriptorGraph);
    try (Connection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
      conn.setAutoCommit(true);
      conn.createStatement().execute("CREATE PROTOBUF DESCRIPTOR IF NOT EXISTS " + ACCOUNT
        + " FROM '" + AcmeWidgetsFixtures.bundleFileUrl() + "'");
      conn.createStatement()
        .execute("CREATE TABLE " + fullTableName
          + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<" + ACCOUNT + ">)"
          + " IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN,"
          + " COLUMN_ENCODED_BYTES=NONE");
      upsertAccount(conn, fullTableName, 1L,
        AcmeWidgetsFixtures.newAccount(account, "tenant-alpha", "Alpha", true, "wh-1"));
      upsertAccount(conn, fullTableName, 2L,
        AcmeWidgetsFixtures.newAccount(account, "tenant-beta", "Beta", false, "wh-2"));
      upsertAccount(conn, fullTableName, 3L,
        DynamicMessage.newBuilder(account)
          .setField(account.findFieldByName("tenant_key"), "tenant-gamma")
          .setField(account.findFieldByName("active"), true).build());
      assertProtoReads(conn, fullTableName, 1L, 2L, 3L);

      conn.createStatement().execute("ALTER TABLE " + fullTableName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
      SystemTransformRecord record = Transform.getTransformRecord(schemaName, tableName, null, null,
        conn.unwrap(PhoenixConnection.class));
      assertNotNull(record);
      upsertAccount(conn, fullTableName, 4L,
        AcmeWidgetsFixtures.newAccount(account, "tenant-delta", "Delta", true, "wh-4"));
      assertProtoReads(conn, fullTableName, 1L, 2L, 3L, 4L);

      TransformToolIT.runTransformTool(TransformToolIT.getArgList(schemaName, tableName, null, null,
        null, null, false, false, false, false, false).toArray(new String[0]), 0);
      Transform.doCutover(conn.unwrap(PhoenixConnection.class), record);
      Transform.updateTransformRecord(conn.unwrap(PhoenixConnection.class), record,
        PTable.TransformStatus.COMPLETED);

      assertEquals(ACCOUNT, catalogDescriptorName(conn, schemaName, tableName, "PAYLOAD"));
      PTable table = conn.unwrap(PhoenixConnection.class).getTableNoCache(fullTableName);
      assertEquals(PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
        table.getImmutableStorageScheme());
      assertEquals(ACCOUNT, table.getColumnForColumnName("PAYLOAD").getProtoMessageName());
    }

    // A fresh connection plans against the post-cutover catalog rows, not a cached PTable.
    try (Connection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
      assertProtoReads(conn, fullTableName, 1L, 2L, 3L, 4L);
      conn.setAutoCommit(true);
      upsertAccount(conn, fullTableName, 5L,
        AcmeWidgetsFixtures.newAccount(account, "tenant-epsilon", "Epsilon", false, "wh-5"));
      // Predicates are not checked here: rows written after cutover are not marked verified, so
      // a filtered scan of the new physical table returns them regardless of the filter.
      assertProtoProjections(conn, fullTableName, 1L, 2L, 3L, 4L, 5L);
    }
  }

  private static void upsertAccount(Connection conn, String fullTableName, long id,
    DynamicMessage payload) throws Exception {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES (?, ?, ?)")) {
      ps.setLong(1, id);
      ps.setString(2, "c" + id);
      ps.setBytes(3, payload.toByteArray());
      ps.executeUpdate();
    }
  }

  private static void assertProtoReads(Connection conn, String fullTableName, Long... ids)
    throws Exception {
    assertProtoProjections(conn, fullTableName, ids);
    assertIds(conn,
      "SELECT ID FROM " + fullTableName
        + " WHERE PROTO_VALUE(PAYLOAD, 'active', 'BOOLEAN') = TRUE ORDER BY ID",
      Arrays.stream(ids).filter(id -> ALL_ACTIVE[(int) (id - 1)]).toArray(Long[]::new));
    assertIds(conn,
      "SELECT ID FROM " + fullTableName
        + " WHERE PROTO_EXISTS(PAYLOAD, 'home_warehouse') = TRUE ORDER BY ID",
      Arrays.stream(ids).filter(id -> ALL_WAREHOUSES[(int) (id - 1)] != null).toArray(Long[]::new));
  }

  private static void assertProtoProjections(Connection conn, String fullTableName, Long... ids)
    throws Exception {
    try (ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ID, C1, PROTO_VALUE(PAYLOAD, 'tenant_key', 'VARCHAR'),"
        + " PROTO_VALUE(PAYLOAD, 'active', 'BOOLEAN'),"
        + " PROTO_VALUE(PAYLOAD, 'home_warehouse', 'VARCHAR') FROM " + fullTableName
        + " ORDER BY ID")) {
      for (long id : ids) {
        int i = (int) id - 1;
        assertTrue(rs.next());
        assertEquals(id, rs.getLong(1));
        assertEquals("c" + id, rs.getString(2));
        assertEquals(ALL_TENANTS[i], rs.getString(3));
        assertEquals(ALL_ACTIVE[i], rs.getBoolean(4));
        assertEquals(ALL_WAREHOUSES[i], rs.getString(5));
      }
      assertFalse(rs.next());
    }
  }

  private static void assertIds(Connection conn, String sql, Long... expected) throws Exception {
    List<Long> actual = new ArrayList<>();
    try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
      while (rs.next()) {
        actual.add(rs.getLong(1));
      }
    }
    assertEquals(sql, Arrays.asList(expected), actual);
  }

  private static String catalogDescriptorName(Connection conn, String schemaName, String tableName,
    String columnName) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement("SELECT PROTOBUF_DESCRIPTOR_NAME"
      + " FROM SYSTEM.CATALOG WHERE TENANT_ID IS NULL AND TABLE_SCHEM = ? AND TABLE_NAME = ?"
      + " AND COLUMN_NAME = ? AND COLUMN_FAMILY IS NOT NULL")) {
      ps.setString(1, schemaName);
      ps.setString(2, tableName);
      ps.setString(3, columnName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        String name = rs.getString(1);
        assertFalse(rs.next());
        return name;
      }
    }
  }
}
