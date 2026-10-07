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
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ProtoValuePackedRepeatedIT extends ParallelStatsDisabledIT {
  private static final String DESCRIPTOR_PACKAGE = "phoenix.it.packed";
  private static final String DESCRIPTOR_NAME = DESCRIPTOR_PACKAGE + ".Container";

  private static Path descriptorPath;
  private static Descriptors.Descriptor containerDescriptor;
  private static Descriptors.EnumDescriptor colorEnum;

  private final boolean columnEncoded;
  private final boolean coveredIndex;
  private final List<String> createdTables = new ArrayList<>();

  public ProtoValuePackedRepeatedIT(boolean columnEncoded, boolean coveredIndex) {
    this.columnEncoded = columnEncoded;
    this.coveredIndex = coveredIndex;
  }

  @Parameterized.Parameters(name = "columnEncoded={0}, coveredIndex={1}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(
      new Object[][] { { false, false }, { false, true }, { true, false }, { true, true } });
  }

  @BeforeClass
  public static synchronized void buildDescriptor() throws Exception {
    EnumDescriptorProto colorEnumProto = EnumDescriptorProto.newBuilder().setName("Color")
      .addValue(EnumValueDescriptorProto.newBuilder().setName("UNSPECIFIED").setNumber(0))
      .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(1))
      .addValue(EnumValueDescriptorProto.newBuilder().setName("GREEN").setNumber(2))
      .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(3))
      .addValue(EnumValueDescriptorProto.newBuilder().setName("MAGENTA").setNumber(4)).build();
    DescriptorProto containerProto =
      DescriptorProto.newBuilder().setName("Container").addEnumType(colorEnumProto)
        .addField(FieldDescriptorProto.newBuilder().setName("nums").setNumber(1)
          .setType(Type.TYPE_INT64).setLabel(Label.LABEL_REPEATED).build())
        .addField(FieldDescriptorProto.newBuilder().setName("counts").setNumber(2)
          .setType(Type.TYPE_INT32).setLabel(Label.LABEL_REPEATED).build())
        .addField(FieldDescriptorProto.newBuilder().setName("flags").setNumber(3)
          .setType(Type.TYPE_BOOL).setLabel(Label.LABEL_REPEATED).build())
        .addField(FieldDescriptorProto.newBuilder().setName("weights").setNumber(4)
          .setType(Type.TYPE_FLOAT).setLabel(Label.LABEL_REPEATED).build())
        .addField(FieldDescriptorProto.newBuilder().setName("ratios").setNumber(5)
          .setType(Type.TYPE_DOUBLE).setLabel(Label.LABEL_REPEATED).build())
        .addField(FieldDescriptorProto.newBuilder().setName("palette").setNumber(6)
          .setType(Type.TYPE_ENUM).setTypeName("Color").setLabel(Label.LABEL_REPEATED).build())
        .build();
    FileDescriptorProto fileProto =
      FileDescriptorProto.newBuilder().setName("phoenix_it_packed.proto")
        .setPackage(DESCRIPTOR_PACKAGE).setSyntax("proto3").addMessageType(containerProto).build();
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(fileProto).build();

    descriptorPath = Files.createTempFile("proto-packed-it-", ".desc");
    Files.write(descriptorPath, set.toByteArray());

    Descriptors.FileDescriptor file =
      Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    containerDescriptor = file.findMessageTypeByName("Container");
    colorEnum = containerDescriptor.findEnumTypeByName("Color");
  }

  @AfterClass
  public static synchronized void deleteDescriptorFile() throws Exception {
    if (descriptorPath != null) {
      Files.deleteIfExists(descriptorPath);
    }
  }

  @After
  public void cleanupTablesAndDescriptor() throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps());
      Statement stmt = conn.createStatement()) {
      for (String name : createdTables) {
        try {
          stmt.execute("DROP TABLE IF EXISTS " + name);
        } catch (SQLException ignore) {
        }
      }
      try {
        stmt.execute("DROP PROTOBUF DESCRIPTOR IF EXISTS " + DESCRIPTOR_NAME);
      } catch (SQLException ignore) {
      }
    }
    createdTables.clear();
  }

  @Test
  public void packedRepeatedInt64ReadsAllElements() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[][] rows =
        { { 100L, 200L, 300L }, { 500L, 600L, 700L, 800L }, { 999L, 1000L, 1001L, 1002L, 1003L },
          { 42L }, { -1L, -2L, -3L }, { Long.MAX_VALUE, Long.MIN_VALUE }, { 0L, 0L, 0L } };
      for (int i = 0; i < rows.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithInt64Nums(rows[i]));
      }
      String pointSql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'nums[0]', 'BIGINT'), "
        + "PROTO_VALUE(PAYLOAD, 'nums[2]', 'BIGINT'), "
        + "PROTO_VALUE(PAYLOAD, 'nums[4]', 'BIGINT') " + "FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(pointSql);
        ResultSet rs = ps.executeQuery()) {
        Long[] expectedIdx0 = { 100L, 500L, 999L, 42L, -1L, Long.MAX_VALUE, 0L };
        Long[] expectedIdx2 = { 300L, 700L, 1001L, null, -3L, null, 0L };
        Long[] expectedIdx4 = { null, null, 1003L, null, null, null, null };
        for (int i = 0; i < expectedIdx0.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i + " nums[0]", (long) expectedIdx0[i], rs.getLong(2));
          if (expectedIdx2[i] == null) {
            assertEquals(0L, rs.getLong(3));
            assertTrue(rs.wasNull());
          } else {
            assertEquals("row " + i + " nums[2]", (long) expectedIdx2[i], rs.getLong(3));
          }
          if (expectedIdx4[i] == null) {
            assertEquals(0L, rs.getLong(4));
            assertTrue(rs.wasNull());
          } else {
            assertEquals("row " + i + " nums[4]", (long) expectedIdx4[i], rs.getLong(4));
          }
        }
        assertFalse("expected exactly 7 rows", rs.next());
      }
    }
  }

  @Test
  public void packedRepeatedInt32InRangeFilterOrdersCorrectly() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      int[][] rows = { { 5, 10 }, { 20, 25, 30 }, { 15 }, { 40, 45 }, { 55, 60, 65, 70 },
        { 75, 80 }, { 100, 200 } };
      for (int i = 0; i < rows.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithInt32Counts(rows[i]));
      }
      String rangeSql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'counts[0]', 'INTEGER') " + "FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'counts[0]', 'INTEGER') BETWEEN ? AND ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, 'counts[0]', 'INTEGER') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setInt(1, 15);
        ps.setInt(2, 75);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 3L, 2L, 4L, 5L, 6L };
          int[] expectedCount0 = { 15, 20, 40, 55, 75 };
          for (int i = 0; i < expectedIds.length; i++) {
            assertTrue("row " + i, rs.next());
            assertEquals(expectedIds[i], rs.getLong(1));
            assertEquals(expectedCount0[i], rs.getInt(2));
          }
          assertFalse("expected exactly 5 rows in [15, 75]", rs.next());
        }
      }
    }
  }

  @Test
  public void packedRepeatedBoolElementReadsBackCorrectly() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      boolean[][] rows = { { true, false, true }, { false, false }, { true },
        { true, true, true, true }, { false, true, false, true, false } };
      for (int i = 0; i < rows.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithBoolFlags(rows[i]));
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'flags[0]', 'BOOLEAN'), "
        + "PROTO_VALUE(PAYLOAD, 'flags[1]', 'BOOLEAN') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        boolean[] expected0 = { true, false, true, true, false };
        Boolean[] expected1 = { false, false, null, true, true };
        for (int i = 0; i < expected0.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i + " flags[0]", expected0[i], rs.getBoolean(2));
          if (expected1[i] == null) {
            assertFalse(rs.getBoolean(3));
            assertTrue(rs.wasNull());
          } else {
            assertEquals("row " + i + " flags[1]", expected1[i], rs.getBoolean(3));
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void packedRepeatedFloatAndDoubleReadCorrectly() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      float[][] weights = { { 1.5f, 2.5f }, { 3.14f, 6.28f, 9.42f }, { 0.001f },
        { 100.0f, 200.0f, 300.0f, 400.0f }, { -1.5f, 2.5f } };
      double[][] ratios = { { 0.1d, 0.2d }, { 1.5d, 2.5d, 3.5d, 4.5d }, { 99.99d },
        { -50.0d, 50.0d }, { 1.0d, 2.0d, 3.0d } };
      for (int i = 0; i < weights.length; i++) {
        upsertContainer(conn, table, i + 1L,
          buildContainerWithFloatWeightsAndDoubleRatios(weights[i], ratios[i]));
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'weights[0]', 'FLOAT'), "
        + "PROTO_VALUE(PAYLOAD, 'weights[2]', 'FLOAT'), "
        + "PROTO_VALUE(PAYLOAD, 'ratios[0]', 'DOUBLE'), "
        + "PROTO_VALUE(PAYLOAD, 'ratios[3]', 'DOUBLE') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        float[] w0 = { 1.5f, 3.14f, 0.001f, 100.0f, -1.5f };
        Float[] w2 = { null, 9.42f, null, 300.0f, null };
        double[] r0 = { 0.1d, 1.5d, 99.99d, -50.0d, 1.0d };
        Double[] r3 = { null, 4.5d, null, null, null };
        for (int i = 0; i < w0.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i + " weights[0]", w0[i], rs.getFloat(2), 1e-6f);
          if (w2[i] == null) {
            rs.getFloat(3);
            assertTrue(rs.wasNull());
          } else {
            assertEquals("row " + i + " weights[2]", w2[i], rs.getFloat(3), 1e-6f);
          }
          assertEquals("row " + i + " ratios[0]", r0[i], rs.getDouble(4), 1e-9d);
          if (r3[i] == null) {
            rs.getDouble(5);
            assertTrue(rs.wasNull());
          } else {
            assertEquals("row " + i + " ratios[3]", r3[i], rs.getDouble(5), 1e-9d);
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void packedRepeatedEnumElementReadsAsInteger() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String[][] palettes = { { "RED", "GREEN", "BLUE" }, { "MAGENTA" }, { "UNSPECIFIED", "RED" },
        { "BLUE", "BLUE", "GREEN" }, { "GREEN", "MAGENTA", "RED", "BLUE" } };
      int[] expectedIdx0 = { 1, 4, 0, 3, 2 };
      int[] expectedIdx2 = { 3, -1, -1, 2, 1 };
      for (int i = 0; i < palettes.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithEnumPalette(palettes[i]));
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'palette[0]', 'INTEGER'), "
        + "PROTO_VALUE(PAYLOAD, 'palette[2]', 'INTEGER') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < palettes.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i + " palette[0]", expectedIdx0[i], rs.getInt(2));
          if (expectedIdx2[i] < 0) {
            rs.getInt(3);
            assertTrue(rs.wasNull());
          } else {
            assertEquals("row " + i + " palette[2]", expectedIdx2[i], rs.getInt(3));
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void packedRepeatedWithDefaultForOutOfBoundsIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[][] rowNums = { { 100L }, { 200L, 201L }, { 300L, 301L, 302L, 303L }, { 400L, 401L },
        { 500L, 501L, 502L, 503L, 504L }, { 600L }, { 700L, 701L, 702L, 703L } };
      for (int i = 0; i < rowNums.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithInt64Nums(rowNums[i]));
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'nums[3]', 'BIGINT', '9999999') " + "FROM "
        + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        long[] expectedNums3 = { 9999999L, 9999999L, 303L, 9999999L, 503L, 9999999L, 703L };
        for (int i = 0; i < expectedNums3.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expectedNums3[i], rs.getLong(2));
          assertFalse("row " + i, rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void packedRepeatedFloatWithLiteralNullDefaultReturnsSqlNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      float[][] weights =
        { { 1.0f }, { 2.0f, 3.0f }, { 4.0f }, { 5.0f, 6.0f, 7.0f }, { 8.0f, 9.0f } };
      double[][] emptyRatios = { {}, {}, {}, {}, {} };
      for (int i = 0; i < weights.length; i++) {
        upsertContainer(conn, table, i + 1L,
          buildContainerWithFloatWeightsAndDoubleRatios(weights[i], emptyRatios[i]));
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'weights[10]', 'FLOAT', 'NuLl') " + "FROM "
        + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (long id = 1L; id <= 5L; id++) {
          assertTrue("row " + id, rs.next());
          assertEquals(id, rs.getLong(1));
          rs.getFloat(2);
          assertTrue("row " + id, rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void packedRepeatedInt64IndexServesRangeQueries() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String index = "IDX_" + table;
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'nums[0]', 'BIGINT')"));
      long[][] rows = { { 10L, 100L }, { 20L, 200L, 300L }, { 30L, 400L }, { 40L, 500L, 600L },
        { 50L, 700L }, { 60L, 800L, 900L }, { 70L, 1000L } };
      for (int i = 0; i < rows.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithInt64Nums(rows[i]));
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + index)) {
        assertTrue(rs.next());
        assertEquals(rows.length, rs.getInt(1));
      }
      String rangeSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'nums[0]', 'BIGINT') BETWEEN ? AND ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, 'nums[0]', 'BIGINT') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setLong(1, 20L);
        ps.setLong(2, 60L);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 2L, 3L, 4L, 5L, 6L };
          for (long expectedId : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expectedId, rs.getLong(1));
          }
          assertFalse("expected exactly 5 rows in [20, 60]", rs.next());
        }
        validateExplainPlan(ps, index);
      }
    }
  }

  @Test
  public void packedRepeatedDoubleIndexServesRangeQueries() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String index = "IDX_" + table;
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'ratios[0]', 'DOUBLE')"));
      double[][] ratiosPerRow = { { 0.1d, 9.9d }, { 0.5d }, { 1.5d, 2.5d, 3.5d }, { 2.5d, 5.0d },
        { 5.5d }, { 7.5d, 8.0d }, { 9.9d } };
      for (int i = 0; i < ratiosPerRow.length; i++) {
        upsertContainer(conn, table, i + 1L,
          buildContainerWithFloatWeightsAndDoubleRatios(new float[0], ratiosPerRow[i]));
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + index)) {
        assertTrue(rs.next());
        assertEquals(ratiosPerRow.length, rs.getInt(1));
      }
      String rangeSql = "SELECT ID FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'ratios[0]', 'DOUBLE') BETWEEN ? AND ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, 'ratios[0]', 'DOUBLE') ASC";
      try (PreparedStatement ps = conn.prepareStatement(rangeSql)) {
        ps.setDouble(1, 1.5d);
        ps.setDouble(2, 7.5d);
        try (ResultSet rs = ps.executeQuery()) {
          long[] expectedIds = { 3L, 4L, 5L, 6L };
          for (long expectedId : expectedIds) {
            assertTrue(rs.next());
            assertEquals(expectedId, rs.getLong(1));
          }
          assertFalse("expected exactly 4 rows in [1.5, 7.5]", rs.next());
        }
        validateExplainPlan(ps, index);
      }
    }
  }

  @Test
  public void protoArrayLengthCountsPackedRepeatedInt64Elements() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[][] numsPerRow =
        { {}, { 42L }, { 1L, 2L, 3L }, { 10L, 20L }, { 5L }, { 100L, 200L, 300L, 400L } };
      for (int i = 0; i < numsPerRow.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithInt64Nums(numsPerRow[i]));
      }
      String sql =
        "SELECT ID, PROTO_ARRAY_LENGTH(PAYLOAD, 'nums') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        int[] expected = { 0, 1, 3, 2, 1, 4 };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expected[i], rs.getInt(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  private String createRegisteredTable(Connection conn) throws SQLException {
    String descUrl = "file://" + descriptorPath.toAbsolutePath();
    conn.createStatement().execute(
      "CREATE PROTOBUF DESCRIPTOR IF NOT EXISTS " + DESCRIPTOR_NAME + " FROM '" + descUrl + "'");
    String table = generateUniqueName();
    createdTables.add(table);
    conn.createStatement().execute(
      "CREATE TABLE " + table + " (ID BIGINT NOT NULL PRIMARY KEY, C1 VARCHAR, PAYLOAD PROTOBUF<"
        + DESCRIPTOR_NAME + ">)" + encodingClause());
    return table;
  }

  private String encodingClause() {
    return this.columnEncoded ? " COLUMN_ENCODED_BYTES=2" : " COLUMN_ENCODED_BYTES=0";
  }

  private String indexDdl(String indexName, String tableName, String indexedExpr) {
    if (this.coveredIndex) {
      return "CREATE INDEX " + indexName + " ON " + tableName + "(" + indexedExpr
        + ") INCLUDE(C1) WHERE " + indexedExpr + " IS NOT NULL";
    }
    return "CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + "(" + indexedExpr
      + ") WHERE " + indexedExpr + " IS NOT NULL";
  }

  private static void upsertContainer(Connection conn, String table, long id, byte[] payload)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, C1, PAYLOAD) VALUES (?, ?, ?)")) {
      ps.setLong(1, id);
      ps.setString(2, "c1-" + id);
      ps.setBytes(3, payload);
      ps.executeUpdate();
    }
    conn.commit();
  }

  private static byte[] buildContainerWithInt64Nums(long[] nums) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(containerDescriptor);
    Descriptors.FieldDescriptor field = containerDescriptor.findFieldByName("nums");
    for (long n : nums) {
      builder.addRepeatedField(field, n);
    }
    return builder.build().toByteArray();
  }

  private static byte[] buildContainerWithInt32Counts(int[] counts) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(containerDescriptor);
    Descriptors.FieldDescriptor field = containerDescriptor.findFieldByName("counts");
    for (int c : counts) {
      builder.addRepeatedField(field, c);
    }
    return builder.build().toByteArray();
  }

  private static byte[] buildContainerWithBoolFlags(boolean[] flags) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(containerDescriptor);
    Descriptors.FieldDescriptor field = containerDescriptor.findFieldByName("flags");
    for (boolean f : flags) {
      builder.addRepeatedField(field, f);
    }
    return builder.build().toByteArray();
  }

  private static byte[] buildContainerWithFloatWeightsAndDoubleRatios(float[] weights,
    double[] ratios) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(containerDescriptor);
    Descriptors.FieldDescriptor wField = containerDescriptor.findFieldByName("weights");
    Descriptors.FieldDescriptor rField = containerDescriptor.findFieldByName("ratios");
    for (float w : weights) {
      builder.addRepeatedField(wField, w);
    }
    for (double r : ratios) {
      builder.addRepeatedField(rField, r);
    }
    return builder.build().toByteArray();
  }

  private static byte[] buildContainerWithEnumPalette(String[] symbols) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(containerDescriptor);
    Descriptors.FieldDescriptor field = containerDescriptor.findFieldByName("palette");
    for (String symbol : symbols) {
      builder.addRepeatedField(field, colorEnum.findValueByName(symbol));
    }
    return builder.build().toByteArray();
  }

  @Test
  public void protoValueAtTagPackedRepeatedInt64ThirdElement() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[][] numsPerRow =
        { {}, { 42L }, { 1L, 2L, 3L }, { 10L, 20L }, { 5L }, { 100L, 200L, 300L, 400L } };
      for (int i = 0; i < numsPerRow.length; i++) {
        upsertContainer(conn, table, i + 1L, buildContainerWithInt64Nums(numsPerRow[i]));
      }
      String sql = "SELECT ID, PROTO_VALUE_AT_TAG(PAYLOAD, '1[2]', 'BIGINT') FROM " + table
        + " ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        Long[] expected = { null, null, 3L, null, null, 300L };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + (i + 1), rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          if (expected[i] == null) {
            rs.getLong(2);
            assertTrue("row " + (i + 1) + " should be NULL", rs.wasNull());
          } else {
            assertEquals("row " + (i + 1), (long) expected[i], rs.getLong(2));
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  private static Properties testProps() {
    return PropertiesUtil.deepCopy(TEST_PROPERTIES);
  }

  private static void validateExplainPlan(PreparedStatement ps, String indexName)
    throws SQLException {
    ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
    ExplainPlanAttributes attrs = plan.getPlanStepsAsAttributes();
    assertEquals(indexName, attrs.getTableName());
    assertEquals("PARALLEL 1-WAY", attrs.getIteratorTypeAndScanSize());
    assertEquals("RANGE SCAN", attrs.getExplainScanType());
  }
}
