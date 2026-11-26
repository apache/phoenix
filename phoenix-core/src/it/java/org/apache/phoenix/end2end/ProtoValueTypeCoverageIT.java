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

import java.math.BigDecimal;
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
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.protobuf.ByteString;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.MessageOptions;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ProtoValueTypeCoverageIT extends ParallelStatsDisabledIT {
  private static final String DESCRIPTOR_PACKAGE = "phoenix.it.coverage";
  private static final String DESCRIPTOR_NAME = DESCRIPTOR_PACKAGE + ".AllTypes";

  private static Path descriptorPath;
  private static Descriptors.Descriptor allTypesDescriptor;

  private final boolean columnEncoded;
  private final boolean coveredIndex;
  private final List<String> createdTables = new ArrayList<>();

  public ProtoValueTypeCoverageIT(boolean columnEncoded, boolean coveredIndex) {
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
    DescriptorProto strIntMapEntry =
      mapEntryProto("StrIntMapEntry", Type.TYPE_STRING, Type.TYPE_INT64);
    DescriptorProto int32ToStringEntry =
      mapEntryProto("Int32ToStringEntry", Type.TYPE_INT32, Type.TYPE_STRING);
    DescriptorProto int64ToStringEntry =
      mapEntryProto("Int64ToStringEntry", Type.TYPE_INT64, Type.TYPE_STRING);
    DescriptorProto strFloatMapEntry =
      mapEntryProto("StrFloatMapEntry", Type.TYPE_STRING, Type.TYPE_FLOAT);
    DescriptorProto strDoubleMapEntry =
      mapEntryProto("StrDoubleMapEntry", Type.TYPE_STRING, Type.TYPE_DOUBLE);
    DescriptorProto childProto = DescriptorProto.newBuilder().setName("Child")
      .addField(FieldDescriptorProto.newBuilder().setName("payload_string").setNumber(1)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).build())
      .addField(FieldDescriptorProto.newBuilder().setName("filler").setNumber(2)
        .setType(Type.TYPE_INT32).setLabel(Label.LABEL_OPTIONAL).build())
      .addField(FieldDescriptorProto.newBuilder().setName("bool_flag").setNumber(3)
        .setType(Type.TYPE_BOOL).setLabel(Label.LABEL_OPTIONAL).build())
      .addField(FieldDescriptorProto.newBuilder().setName("float_val").setNumber(4)
        .setType(Type.TYPE_FLOAT).setLabel(Label.LABEL_OPTIONAL).build())
      .build();
    DescriptorProto allTypesProto =
      DescriptorProto.newBuilder().setName("AllTypes").addNestedType(strIntMapEntry)
        .addNestedType(int32ToStringEntry).addNestedType(int64ToStringEntry)
        .addNestedType(strFloatMapEntry).addNestedType(strDoubleMapEntry).addNestedType(childProto)
        .addField(FieldDescriptorProto.newBuilder().setName("fixed32_field").setNumber(1)
          .setType(Type.TYPE_FIXED32).setLabel(Label.LABEL_OPTIONAL).build())
        .addField(FieldDescriptorProto.newBuilder().setName("fixed64_field").setNumber(2)
          .setType(Type.TYPE_FIXED64).setLabel(Label.LABEL_OPTIONAL).build())
        .addField(FieldDescriptorProto.newBuilder().setName("sfixed32_field").setNumber(3)
          .setType(Type.TYPE_SFIXED32).setLabel(Label.LABEL_OPTIONAL).build())
        .addField(FieldDescriptorProto.newBuilder().setName("sfixed64_field").setNumber(4)
          .setType(Type.TYPE_SFIXED64).setLabel(Label.LABEL_OPTIONAL).build())
        .addField(FieldDescriptorProto.newBuilder().setName("uint32_field").setNumber(5)
          .setType(Type.TYPE_UINT32).setLabel(Label.LABEL_OPTIONAL).build())
        .addField(FieldDescriptorProto.newBuilder().setName("bytes_field").setNumber(6)
          .setType(Type.TYPE_BYTES).setLabel(Label.LABEL_OPTIONAL).build())
        .addField(FieldDescriptorProto.newBuilder().setName("int64_field").setNumber(7)
          .setType(Type.TYPE_INT64).setLabel(Label.LABEL_OPTIONAL).build())
        .addField(FieldDescriptorProto.newBuilder().setName("child_msg").setNumber(8)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_OPTIONAL).setTypeName("Child").build())
        .addField(FieldDescriptorProto.newBuilder().setName("str_int_map").setNumber(10)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_REPEATED).setTypeName("StrIntMapEntry")
          .build())
        .addField(FieldDescriptorProto.newBuilder().setName("int32_map").setNumber(11)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_REPEATED)
          .setTypeName("Int32ToStringEntry").build())
        .addField(FieldDescriptorProto.newBuilder().setName("int64_map").setNumber(12)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_REPEATED)
          .setTypeName("Int64ToStringEntry").build())
        .addField(FieldDescriptorProto.newBuilder().setName("str_float_map").setNumber(13)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_REPEATED).setTypeName("StrFloatMapEntry")
          .build())
        .addField(FieldDescriptorProto.newBuilder().setName("str_double_map").setNumber(14)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_REPEATED)
          .setTypeName("StrDoubleMapEntry").build())
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice"))
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_opt_int32"))
        .addField(FieldDescriptorProto.newBuilder().setName("opt_int32").setNumber(15)
          .setType(Type.TYPE_INT32).setLabel(Label.LABEL_OPTIONAL).setProto3Optional(true)
          .setOneofIndex(1).build())
        .addField(FieldDescriptorProto.newBuilder().setName("choice_int").setNumber(16)
          .setType(Type.TYPE_INT32).setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0).build())
        .build();
    FileDescriptorProto fileProto =
      FileDescriptorProto.newBuilder().setName("phoenix_it_coverage.proto")
        .setPackage(DESCRIPTOR_PACKAGE).setSyntax("proto3").addMessageType(allTypesProto).build();
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(fileProto).build();

    descriptorPath = Files.createTempFile("proto-coverage-it-", ".desc");
    Files.write(descriptorPath, set.toByteArray());
    Descriptors.FileDescriptor file =
      Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    allTypesDescriptor = file.findMessageTypeByName("AllTypes");
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
  public void singularFixed32AndFixed64UnsignedLeavesReadBackUnsigned() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      int[] fixed32s = { 0, 1, 100, 1_000_000, Integer.MAX_VALUE, -1, -2 };
      long[] fixed64s = { 0L, 1L, 1_000_000_000_000L, Long.MAX_VALUE, -1L, -2L, -3L };
      for (int i = 0; i < fixed32s.length; i++) {
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("fixed32_field"), fixed32s[i])
          .setField(allTypesDescriptor.findFieldByName("fixed64_field"), fixed64s[i])
          // Keeps the payload non-empty when both fields are zero.
          .setField(allTypesDescriptor.findFieldByName("int64_field"), 1L).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'fixed32_field', 'BIGINT'), "
        + "PROTO_VALUE(PAYLOAD, 'fixed64_field', 'DECIMAL'), "
        + "PROTO_VALUE(PAYLOAD, 'fixed64_field', 'DECIMAL', '7') FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        long[] expectedF32 =
          { 0L, 1L, 100L, 1_000_000L, 2_147_483_647L, 4_294_967_295L, 4_294_967_294L };
        String[] expectedF64Decimal = { "0", "1", "1000000000000", "9223372036854775807",
          "18446744073709551615", "18446744073709551614", "18446744073709551613" };
        for (int i = 0; i < expectedF32.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i + " f32", expectedF32[i], rs.getLong(2));
          assertEquals("row " + i + " f64 decimal", 0,
            new BigDecimal(expectedF64Decimal[i]).compareTo(rs.getBigDecimal(3)));
          // A zero is not serialized in proto3, so row 0 is unset and takes the default.
          String expectedWithDefault = fixed64s[i] == 0L ? "7" : expectedF64Decimal[i];
          assertEquals("row " + i + " f64 default", 0,
            new BigDecimal(expectedWithDefault).compareTo(rs.getBigDecimal(4)));
        }
        assertFalse("expected exactly 7 rows", rs.next());
      }
    }
  }

  @Test
  public void singularSfixed32AndSfixed64LeavesReadBackWithSign() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      int[] sfixed32s = { -1000, -1, 0, 1, 1000, Integer.MAX_VALUE, Integer.MIN_VALUE };
      long[] sfixed64s =
        { -1_000_000_000_000L, -1L, 0L, 1L, 1_000_000_000_000L, Long.MAX_VALUE, Long.MIN_VALUE };
      for (int i = 0; i < sfixed32s.length; i++) {
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("sfixed32_field"), sfixed32s[i])
          .setField(allTypesDescriptor.findFieldByName("sfixed64_field"), sfixed64s[i]).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'sfixed32_field', 'INTEGER'), "
        + "PROTO_VALUE(PAYLOAD, 'sfixed64_field', 'BIGINT') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < sfixed32s.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i + " sf32", sfixed32s[i], rs.getInt(2));
          assertEquals("row " + i + " sf64", sfixed64s[i], rs.getLong(3));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void singularUint32WidenedToBigintPreservesUnsignedSemantics() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      int[] uintValues = { 0, 1, 100, Integer.MAX_VALUE, -1, -2, -1000 };
      long[] expected =
        { 0L, 1L, 100L, 2_147_483_647L, 4_294_967_295L, 4_294_967_294L, 4_294_966_296L };
      for (int i = 0; i < uintValues.length; i++) {
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("uint32_field"), uintValues[i]).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'uint32_field', 'BIGINT') FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expected[i], rs.getLong(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void singularBytesLeafReadsBackAsVarbinary() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      byte[][] payloads =
        { new byte[0], new byte[] { 0x01 }, new byte[] { 0x00, (byte) 0xFF, 0x7F, (byte) 0x80 },
          "hello".getBytes(), new byte[] { 0x10, 0x20, 0x30, 0x40, 0x50 } };
      for (int i = 0; i < payloads.length; i++) {
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("bytes_field"),
            ByteString.copyFrom(payloads[i]))
          .build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'bytes_field', 'VARBINARY') FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < payloads.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          byte[] got = rs.getBytes(2);
          byte[] want = payloads[i];
          assertArrayEquals("row " + i, want, got == null ? new byte[0] : got);
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void singularInt64CoercedToDecimalReadsAsBigDecimal() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[] values = { 1L, 1_000_000L, Long.MAX_VALUE, -1L, -1_000_000_000L, Long.MIN_VALUE };
      for (int i = 0; i < values.length; i++) {
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), values[i]).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'int64_field', 'DECIMAL') FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < values.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, 0, new BigDecimal(values[i]).compareTo(rs.getBigDecimal(2)));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void singularFloatCoercedToDecimalMapsNonFiniteToNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      Descriptors.Descriptor childDescriptor =
        allTypesDescriptor.findFieldByName("child_msg").getMessageType();
      float[] values = { 0.1f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -2.5f };
      for (int i = 0; i < values.length; i++) {
        DynamicMessage child = DynamicMessage.newBuilder(childDescriptor)
          .setField(childDescriptor.findFieldByName("float_val"), values[i]).build();
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("child_msg"), child).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      String sql =
        "SELECT ID, PROTO_VALUE(PAYLOAD, 'child_msg.float_val', 'DECIMAL') FROM " + table;
      BigDecimal[] expected = { new BigDecimal("0.1"), null, null, null, new BigDecimal("-2.5") };
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < values.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expected[i], rs.getBigDecimal(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void tagVarintOutsideIntRangeReadsAsNullForIntegerAndUnsignedInt() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[] values = { 1L, 2147483648L, 4294967297L, 3000000000L, -1L };
      for (int i = 0; i < values.length; i++) {
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), values[i]).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'INTEGER'), "
        + "PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'UNSIGNED_INT'), "
        + "PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'INTEGER', '7'), "
        + "PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'UNSIGNED_INT', '7'), "
        + "PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'UNSIGNED_LONG', '7') FROM " + table;
      Integer[] expectedInt = { 1, null, null, null, -1 };
      Integer[] expectedUnsigned = { 1, null, null, null, null };
      Long[] expectedUnsignedLong = { 1L, 2147483648L, 4294967297L, 3000000000L, null };
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < values.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expectedInt[i], rs.getObject(2));
          assertEquals("row " + i, expectedUnsigned[i], rs.getObject(3));
          assertEquals("row " + i + " with default", expectedInt[i], rs.getObject(4));
          assertEquals("row " + i + " with default", expectedUnsigned[i], rs.getObject(5));
          assertEquals("row " + i + " with default", expectedUnsignedLong[i], rs.getObject(6));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void invalidPathOrTypeIsRejectedAtCompileTimeOnEmptyTable() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      int notFound = SQLExceptionCode.PROTOBUF_PATH_FIELD_NOT_FOUND.getErrorCode();
      int syntax = SQLExceptionCode.PROTOBUF_TAG_PATH_INVALID_SYNTAX.getErrorCode();
      int mismatch = SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH.getErrorCode();
      int narrowing = SQLExceptionCode.PROTOBUF_VALUE_TYPE_NARROWING_NOT_PERMITTED.getErrorCode();
      Object[][] cases = { { "PROTO_VALUE(PAYLOAD, 'no.such', 'INTEGER')", notFound },
        { "PROTO_EXISTS(PAYLOAD, 'no.such')", notFound },
        { "PROTO_ARRAY_LENGTH(PAYLOAD, 'no.such')", notFound },
        { "PROTO_VALUE(PAYLOAD, 'int64_field', 'BOGUS')", mismatch },
        { "PROTO_VALUE(PAYLOAD, 'int64_field', 'VARCHAR')", mismatch },
        { "PROTO_VALUE(PAYLOAD, 'int64_field', 'INTEGER')", narrowing },
        { "PROTO_VALUE_AT_TAG(PAYLOAD, '7.', 'BIGINT')", syntax },
        { "PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'BOGUS')", mismatch },
        { "PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'DATE')", mismatch } };
      for (Object[] c : cases) {
        String[] statements = { "SELECT " + c[0] + " FROM " + table,
          "CREATE INDEX " + generateUniqueName() + " ON " + table + " (" + c[0] + ")" };
        for (String sql : statements) {
          try {
            conn.createStatement().execute(sql);
            fail("expected compile failure for " + sql);
          } catch (SQLException e) {
            assertEquals(sql, c[1], e.getErrorCode());
          }
        }
      }
    }
  }

  @Test
  public void emptyPayloadReadsAsSqlNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      upsertPayload(conn, table, 1L, new byte[0]);
      upsertPayload(conn, table, 2L,
        DynamicMessage.newBuilder(allTypesDescriptor).build().toByteArray());
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'int64_field', 'BIGINT') FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        rs.getLong(2);
        assertTrue("empty payload → SQL NULL", rs.wasNull());
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        rs.getLong(2);
        // An all-defaults proto3 message encodes to zero bytes, which is stored as NULL.
        assertTrue("proto3 all-defaults payload → SQL NULL", rs.wasNull());
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void unsetScalarUnderPresentParentReadsAsTypeZero() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      Descriptors.Descriptor childDescriptor =
        allTypesDescriptor.findFieldByName("child_msg").getMessageType();
      upsertPayload(conn, table, 1L, DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 1L).build().toByteArray());
      upsertPayload(conn, table, 2L,
        DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("child_msg"),
            DynamicMessage.newBuilder(childDescriptor)
              .setField(childDescriptor.findFieldByName("filler"), 5).build())
          .build().toByteArray());
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'child_msg.payload_string', 'VARCHAR'), "
        + "PROTO_VALUE(PAYLOAD, 'child_msg.bool_flag', 'BOOLEAN'), "
        + "PROTO_VALUE(PAYLOAD, 'child_msg.float_val', 'FLOAT'), "
        + "PROTO_VALUE(PAYLOAD, 'uint32_field', 'BIGINT'), "
        + "PROTO_VALUE(PAYLOAD, 'fixed64_field', 'DECIMAL'), "
        + "PROTO_VALUE(PAYLOAD, 'bytes_field', 'VARBINARY'), "
        + "PROTO_EXISTS(PAYLOAD, 'uint32_field') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        // child_msg is unset, so its fields read as NULL.
        for (int col = 2; col <= 4; col++) {
          assertNull("id=1 col " + col, rs.getObject(col));
        }
        // Top-level fields: the root message is present.
        assertEquals(0L, rs.getLong(5));
        assertFalse(rs.wasNull());
        assertEquals(0, BigDecimal.ZERO.compareTo(rs.getBigDecimal(6)));
        // Phoenix reads an empty VARCHAR or VARBINARY as NULL.
        assertNull(rs.getBytes(7));
        assertFalse("PROTO_EXISTS distinguishes unset from zero", rs.getBoolean(8));

        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertEquals(Boolean.FALSE, rs.getObject(3));
        assertEquals(0.0f, rs.getFloat(4), 0.0f);
        assertFalse(rs.wasNull());
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void proto3OptionalReadsAsZeroWhenUnsetAndOneofArmReadsAsNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      // { opt_int32, choice_int }; null means unset. A set field is on the wire even when zero.
      Integer[][] rows = { { null, null }, { 0, null }, { 7, null }, { null, 0 }, { -3, 9 } };
      for (int i = 0; i < rows.length; i++) {
        DynamicMessage.Builder b = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), 1L);
        if (rows[i][0] != null) {
          b.setField(allTypesDescriptor.findFieldByName("opt_int32"), rows[i][0]);
        }
        if (rows[i][1] != null) {
          b.setField(allTypesDescriptor.findFieldByName("choice_int"), rows[i][1]);
        }
        upsertPayload(conn, table, i + 1L, b.build().toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'opt_int32', 'INTEGER'),"
        + " PROTO_EXISTS(PAYLOAD, 'opt_int32'), PROTO_VALUE(PAYLOAD, 'choice_int', 'INTEGER'),"
        + " PROTO_EXISTS(PAYLOAD, 'choice_int') FROM " + table + " ORDER BY ID";
      int[] expectedOpt = { 0, 0, 7, 0, -3 };
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < rows.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          // Explicit presence only changes PROTO_EXISTS; an unset optional still reads as zero.
          assertEquals("row " + i, Integer.valueOf(expectedOpt[i]), rs.getObject(2));
          assertEquals("row " + i, rows[i][0] != null, rs.getBoolean(3));
          // An unset oneof arm has no value to fall back to.
          assertEquals("row " + i, rows[i][1], rs.getObject(4));
          assertEquals("row " + i, rows[i][1] != null, rs.getBoolean(5));
        }
        assertFalse(rs.next());
      }
      String zeroSql = "SELECT ID FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'opt_int32', 'INTEGER') = 0 ORDER BY ID";
      try (PreparedStatement ps = conn.prepareStatement(zeroSql);
        ResultSet rs = ps.executeQuery()) {
        for (long id : new long[] { 1L, 2L, 4L }) {
          assertTrue(rs.next());
          assertEquals(id, rs.getLong(1));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void invalidDefaultLiteralIsRejectedAtCompileTime() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String[][] cases = { { "child_msg.filler", "INTEGER", "abc" },
        { "child_msg.filler", "INTEGER", "99999999999" }, { "int64_field", "BIGINT", "1.5" },
        { "child_msg.bool_flag", "BOOLEAN", "yes" }, { "child_msg.float_val", "FLOAT", "x" },
        { "uint32_field", "UNSIGNED_LONG", "-1" }, { "fixed64_field", "DECIMAL", "abc" },
        { "fixed64_field", "DECIMAL", "1e" } };
      for (String[] c : cases) {
        String sql = "SELECT ID FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, '" + c[0] + "', '"
          + c[1] + "', '" + c[2] + "') IS NOT NULL";
        try {
          conn.prepareStatement(sql).executeQuery();
          fail("expected default '" + c[2] + "' for " + c[1] + " to be rejected");
        } catch (SQLException e) {
          assertEquals(c[1] + " '" + c[2] + "'",
            SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
      }
      try {
        conn
          .prepareStatement(
            "SELECT PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'BIGINT', 'abc') FROM " + table)
          .executeQuery();
        fail("expected PROTO_VALUE_AT_TAG default 'abc' to be rejected");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH.getErrorCode(),
          e.getErrorCode());
      }
    }
  }

  @Test
  public void fullUnsignedRangeReadsLosslesslyWithSuggestedTypes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      // uint32 0xFFFFFFFF and fixed64 2^64-1 (as signed bits -1)
      upsertPayload(conn, table, 1L,
        DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("fixed64_field"), -1L)
          .setField(allTypesDescriptor.findFieldByName("uint32_field"), -1).build().toByteArray());
      String sql = "SELECT PROTO_FIELD_TYPE(PAYLOAD, 'uint32_field'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'fixed64_field') FROM " + table;
      String t32;
      String t64;
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        assertTrue(rs.next());
        t32 = rs.getString(1);
        t64 = rs.getString(2);
      }
      assertEquals("BIGINT", t32);
      assertEquals("DECIMAL", t64);
      sql = "SELECT PROTO_VALUE(PAYLOAD, 'uint32_field', '" + t32 + "'), "
        + "PROTO_VALUE(PAYLOAD, 'fixed64_field', '" + t64 + "'), "
        + "PROTO_VALUE(PAYLOAD, 'uint32_field', 'UNSIGNED_LONG') FROM " + table;
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals(4_294_967_295L, rs.getLong(1));
        assertEquals(0, new BigDecimal("18446744073709551615").compareTo(rs.getBigDecimal(2)));
        assertEquals(4_294_967_295L, rs.getLong(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void lossyUnsignedTargetTypesAreRejectedAtCompileTime() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String[][] cases = { { "uint32_field", "UNSIGNED_INT" }, { "fixed32_field", "UNSIGNED_INT" },
        { "fixed64_field", "UNSIGNED_LONG" }, { "fixed64_field", "UNSIGNED_INT" },
        { "fixed64_field", "BIGINT" } };
      for (String[] c : cases) {
        String expr = "PROTO_VALUE(PAYLOAD, '" + c[0] + "', '" + c[1] + "')";
        String[] statements = { "SELECT " + expr + " FROM " + table,
          "CREATE INDEX " + generateUniqueName() + " ON " + table + " (" + expr + ")" };
        for (String sql : statements) {
          try {
            conn.createStatement().execute(sql);
            fail("expected " + c[1] + " to be rejected for " + c[0] + ": " + sql);
          } catch (SQLException e) {
            assertEquals(sql,
              SQLExceptionCode.PROTOBUF_VALUE_TYPE_NARROWING_NOT_PERMITTED.getErrorCode(),
              e.getErrorCode());
          }
        }
      }
    }
  }

  @Test
  public void unsignedAndBooleanDefaultsFireWhenPathAbsent() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      upsertPayload(conn, table, 1L, DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 1L).build().toByteArray());
      String sql = "SELECT PROTO_VALUE(PAYLOAD, 'uint32_field', 'UNSIGNED_LONG', '12'), "
        + "PROTO_VALUE(PAYLOAD, 'fixed64_field', 'DECIMAL', '34'), "
        + "PROTO_VALUE(PAYLOAD, 'child_msg.bool_flag', 'BOOLEAN', 'TRUE'), "
        + "PROTO_VALUE(PAYLOAD, 'bytes_field', 'VARBINARY_ENCODED', 'enc'), "
        + "PROTO_VALUE_AT_TAG(PAYLOAD, '5', 'UNSIGNED_INT', '56') FROM " + table;
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(12, rs.getInt(1));
        assertEquals(34L, rs.getLong(2));
        assertTrue(rs.getBoolean(3));
        assertArrayEquals("enc".getBytes(), rs.getBytes(4));
        assertEquals(56, rs.getInt(5));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void mapInt32KeyBracketLookupReturnsExpectedValues() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      int[][] keys = { { 10, 20, 30 }, { 20, 40 }, { 15, 25, 30 }, { 10 }, { 40, 50, 60 } };
      String[][] values = { { "v10-a", "v20-a", "v30-a" }, { "v20-b", "v40-b" },
        { "v15-c", "v25-c", "v30-c" }, { "v10-d" }, { "v40-e", "v50-e", "v60-e" } };
      for (int i = 0; i < keys.length; i++) {
        upsertPayload(conn, table, i + 1L, buildInt32MapPayload(keys[i], values[i]).toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'int32_map[20]', 'VARCHAR') FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'int32_map[20]', 'VARCHAR') IS NOT NULL"
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals("v20-a", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals("v20-b", rs.getString(2));
        assertFalse("expected exactly 2 rows with key=20", rs.next());
      }
    }
  }

  @Test
  public void mapInt64KeyBracketLookupReturnsExpectedValues() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[][] keys = { { 100_000_000_000L, 200_000_000_000L },
        { 200_000_000_000L, 300_000_000_000L, 400_000_000_000L }, { 500_000_000_000L },
        { 200_000_000_000L, 600_000_000_000L }, { 700_000_000_000L, 800_000_000_000L } };
      String[][] values = { { "one", "two" }, { "two-b", "three", "four" }, { "five" },
        { "two-d", "six" }, { "seven", "eight" } };
      for (int i = 0; i < keys.length; i++) {
        upsertPayload(conn, table, i + 1L, buildInt64MapPayload(keys[i], values[i]).toByteArray());
      }
      String sql =
        "SELECT ID, PROTO_VALUE(PAYLOAD, 'int64_map[200000000000]', " + "'VARCHAR') FROM " + table
          + " WHERE PROTO_VALUE(PAYLOAD, 'int64_map[200000000000]', 'VARCHAR') IS NOT NULL"
          + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        long[] expectedIds = { 1L, 2L, 4L };
        String[] expectedVals = { "two", "two-b", "two-d" };
        for (int i = 0; i < expectedIds.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(expectedIds[i], rs.getLong(1));
          assertEquals(expectedVals[i], rs.getString(2));
        }
        assertFalse("expected exactly 3 rows with key=200000000000", rs.next());
      }
    }
  }

  @Test
  public void malformedPayloadReadsAsSqlNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      DynamicMessage valid = DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 42L).build();
      upsertPayload(conn, table, 1L, valid.toByteArray());
      upsertPayload(conn, table, 2L, new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });
      upsertPayload(conn, table, 3L, new byte[] { (byte) 0x08 });
      upsertPayload(conn, table, 4L, "not-a-proto".getBytes());
      upsertPayload(conn, table, 5L, new byte[] { 0x08, (byte) 0xFF });
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'int64_field', 'BIGINT') FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals(42L, rs.getLong(2));
        for (long expectedId = 2L; expectedId <= 5L; expectedId++) {
          assertTrue("row id " + expectedId, rs.next());
          assertEquals(expectedId, rs.getLong(1));
          rs.getLong(2);
          assertTrue("malformed row id " + expectedId + " must be SQL NULL", rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void childOfUnsetParentReturnsSqlNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      Descriptors.Descriptor childDescriptor =
        allTypesDescriptor.findFieldByName("child_msg").getMessageType();
      upsertPayload(conn, table, 1L,
        DynamicMessage.newBuilder(allTypesDescriptor).build().toByteArray());
      upsertPayload(conn, table, 2L,
        DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("child_msg"),
            DynamicMessage.newBuilder(childDescriptor)
              .setField(childDescriptor.findFieldByName("filler"), 999).build())
          .build().toByteArray());
      upsertPayload(conn, table, 3L,
        DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("child_msg"),
            DynamicMessage.newBuilder(childDescriptor)
              .setField(childDescriptor.findFieldByName("payload_string"), "hello").build())
          .build().toByteArray());
      upsertPayload(conn, table, 4L, DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 1L).build().toByteArray());
      String sql = "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'INTEGER', '77') "
        + "FROM " + table;
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        rs.getInt(2);
        assertTrue("id=1 empty payload is stored as NULL, so the default is ignored", rs.wasNull());

        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals("id=2 parent present + filler set → 999", 999, rs.getInt(2));
        assertFalse(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals("id=3 parent present + filler absent → user default 77", 77, rs.getInt(2));
        assertFalse("expected user default 77, not SQL NULL", rs.wasNull());

        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals("id=4 non-empty payload, parent absent", 77, rs.getInt(2));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void veryDeepMessageChainReadsLeafCorrectly() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      long[] values = { 111L, 222L, 333L, 444L, 555L, 666L, 777L };
      for (int i = 0; i < values.length; i++) {
        DynamicMessage payload =
          buildStrIntMapPayload(new String[] { "shared-key" }, new long[] { values[i] });
        upsertPayload(conn, table, i + 1L, payload.toByteArray());
      }
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'str_int_map.shared-key', 'BIGINT') " + "FROM "
        + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < values.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, values[i], rs.getLong(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void defaultsAcrossAllSupportedSqlTypesFireWhenPathAbsent() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      upsertPayload(conn, table, 1L, DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 1L).build().toByteArray());
      for (long id = 2L; id <= 6L; id++) {
        upsertPayload(conn, table, id, DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), id).build().toByteArray());
      }
      String sql =
        "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'child_msg.payload_string', 'VARCHAR', 'v-def'), "
          + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'INTEGER', '77'), "
          + "PROTO_VALUE(PAYLOAD, 'str_int_map.some-key', 'BIGINT', '9999'), "
          + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'BIGINT', '111111111111'), "
          + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'DECIMAL', '3.14159') " + "FROM " + table
          + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (long expectedId = 1L; expectedId <= 6L; expectedId++) {
          assertTrue("row " + expectedId, rs.next());
          assertEquals(expectedId, rs.getLong(1));
          assertEquals("v-def", rs.getString(2));
          assertFalse(rs.wasNull());
          assertEquals(77, rs.getInt(3));
          assertFalse(rs.wasNull());
          assertEquals(9999L, rs.getLong(4));
          assertFalse(rs.wasNull());
          assertEquals(111111111111L, rs.getLong(5));
          assertFalse(rs.wasNull());
          assertEquals(0, new BigDecimal("3.14159").compareTo(rs.getBigDecimal(6)));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void defaultsForBooleanFloatAndVarbinaryFireWhenPathAbsent() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      Descriptors.Descriptor childDescriptor =
        allTypesDescriptor.findFieldByName("child_msg").getMessageType();
      float[] oddFloats = { 1.25f, 3.75f, 5.25f };
      byte[][] oddBytes =
        { "row1-bytes".getBytes(), "row3-bytes".getBytes(), "row5-bytes".getBytes() };
      for (long id = 1L; id <= 6L; id++) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), id);
        if (id % 2 == 1) {
          int oddIdx = (int) (id / 2);
          builder.setField(allTypesDescriptor.findFieldByName("child_msg"),
            DynamicMessage.newBuilder(childDescriptor)
              .setField(childDescriptor.findFieldByName("bool_flag"), true)
              .setField(childDescriptor.findFieldByName("float_val"), oddFloats[oddIdx]).build());
          builder.setField(allTypesDescriptor.findFieldByName("bytes_field"),
            ByteString.copyFrom(oddBytes[oddIdx]));
        }
        upsertPayload(conn, table, id, builder.build().toByteArray());
      }
      String sql =
        "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'child_msg.bool_flag', 'BOOLEAN', 'false'), "
          + "PROTO_VALUE(PAYLOAD, 'child_msg.float_val', 'FLOAT', '2.5'), "
          + "PROTO_VALUE(PAYLOAD, 'bytes_field', 'VARBINARY', 'my-default') " + "FROM " + table
          + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        Boolean[] expectedBool = { true, false, true, false, true, false };
        Float[] expectedFloat = { 1.25f, 2.5f, 3.75f, 2.5f, 5.25f, 2.5f };
        byte[][] expectedBytes =
          { "row1-bytes".getBytes(), "my-default".getBytes(), "row3-bytes".getBytes(),
            "my-default".getBytes(), "row5-bytes".getBytes(), "my-default".getBytes() };
        for (int i = 0; i < expectedBool.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expectedBool[i], rs.getBoolean(2));
          assertFalse("row " + i, rs.wasNull());
          assertEquals("row " + i, expectedFloat[i], rs.getFloat(3), 1e-6f);
          assertFalse("row " + i, rs.wasNull());
          assertArrayEquals("row " + i, expectedBytes[i], rs.getBytes(4));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void defaultsMixedAcrossRowsPresentReturnsValueAbsentReturnsDefault() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      Descriptors.Descriptor childDescriptor =
        allTypesDescriptor.findFieldByName("child_msg").getMessageType();
      int[] expectedFillerValues = { 100, 555, 300, 555, 500, 555, 555 };
      for (int i = 0; i < expectedFillerValues.length; i++) {
        long id = i + 1L;
        boolean hasChildMsg = (id % 2 == 1) && id != 7L;
        boolean fillerSet = hasChildMsg;
        DynamicMessage.Builder outer = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), id);
        if (hasChildMsg) {
          DynamicMessage.Builder child = DynamicMessage.newBuilder(childDescriptor);
          if (fillerSet) {
            child.setField(childDescriptor.findFieldByName("filler"), (int) (id * 100L));
          }
          outer.setField(allTypesDescriptor.findFieldByName("child_msg"), child.build());
        }
        if (id == 7L) {
          DynamicMessage.Builder child = DynamicMessage.newBuilder(childDescriptor)
            .setField(childDescriptor.findFieldByName("payload_string"), "row7-str");
          outer.setField(allTypesDescriptor.findFieldByName("child_msg"), child.build());
        }
        upsertPayload(conn, table, id, outer.build().toByteArray());
      }
      String sql = "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'INTEGER', '555') "
        + "FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < expectedFillerValues.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expectedFillerValues[i], rs.getInt(2));
          assertFalse("row " + i, rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void defaultFiresForMalformedPayloadInsteadOfSqlNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      DynamicMessage valid = DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 42L).build();
      upsertPayload(conn, table, 1L, valid.toByteArray());
      upsertPayload(conn, table, 2L, new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });
      upsertPayload(conn, table, 3L, new byte[] { (byte) 0x08 });
      upsertPayload(conn, table, 4L, "not-a-proto".getBytes());
      upsertPayload(conn, table, 5L, valid.toByteArray());
      String sql = "SELECT ID, PROTO_VALUE(PAYLOAD, 'int64_field', 'BIGINT', '12345') " + "FROM "
        + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        long[] expected = { 42L, 12345L, 12345L, 12345L, 42L };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expected[i], rs.getLong(2));
          assertFalse("row " + i, rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void literalNullDefaultReturnsSqlNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      for (long id = 1L; id <= 6L; id++) {
        upsertPayload(conn, table, id, DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), id).build().toByteArray());
      }
      String sql = "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'INTEGER', 'null'), "
        + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'INTEGER', 'NULL'), "
        + "PROTO_VALUE(PAYLOAD, 'child_msg.filler', 'INTEGER', 'NuLl') " + "FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (long id = 1L; id <= 6L; id++) {
          assertTrue("row " + id, rs.next());
          assertEquals(id, rs.getLong(1));
          for (int col = 2; col <= 4; col++) {
            rs.getInt(col);
            assertTrue("row " + id + " col " + col, rs.wasNull());
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void mapWithFloatAndDoubleScalarValuesExerciseFixedWidthReadScalarBytes()
    throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String[] keys = { "alpha", "beta", "gamma", "delta", "epsilon", "zeta" };
      float[] floatValues = { 1.25f, 2.5f, 3.75f, 4.0f, 5.5f, 6.125f };
      double[] doubleValues = { 100.5d, 200.25d, 300.125d, 400.0d, 500.75d, 600.875d };
      for (int i = 0; i < keys.length; i++) {
        upsertPayload(conn, table, i + 1L,
          buildStrFloatAndDoubleMapPayload(keys, floatValues, doubleValues, i).toByteArray());
      }
      String sql = "SELECT ID, " + "PROTO_VALUE(PAYLOAD, 'str_float_map.gamma', 'FLOAT'), "
        + "PROTO_VALUE(PAYLOAD, 'str_double_map.epsilon', 'DOUBLE') FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (int i = 0; i < keys.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, 3.75f, rs.getFloat(2), 1e-6f);
          assertEquals("row " + i, 500.75d, rs.getDouble(3), 1e-9d);
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void singularBigintFieldFunctionalIndexServesRangeQueries() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String index = "IDX_" + table;
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'int64_field', 'BIGINT')"));
      long[] values = { 10L, 20L, 30L, 40L, 50L, 60L, 70L };
      for (int i = 0; i < values.length; i++) {
        DynamicMessage msg = DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), values[i]).build();
        upsertPayload(conn, table, i + 1L, msg.toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + index)) {
        assertTrue(rs.next());
        assertEquals(values.length, rs.getInt(1));
      }
      String rangeSql = "SELECT ID, C1 FROM " + table
        + " WHERE PROTO_VALUE(PAYLOAD, 'int64_field', 'BIGINT') BETWEEN ? AND ?"
        + " ORDER BY PROTO_VALUE(PAYLOAD, 'int64_field', 'BIGINT') ASC";
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
  public void mapInt32KeyFunctionalIndexServesPointLookups() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      String index = "IDX_" + table;
      conn.createStatement()
        .execute(indexDdl(index, table, "PROTO_VALUE(PAYLOAD, 'int32_map[42]', 'VARCHAR')"));
      int[][] keys =
        { { 42 }, { 42, 99 }, { 99 }, { 42, 100 }, { 100 }, { 42, 200 }, { 42, 300, 400 } };
      String[][] values =
        { { "row1-v" }, { "row2-v42", "row2-v99" }, { "row3-v99" }, { "row4-v42", "row4-v100" },
          { "row5-v100" }, { "row6-v42", "row6-v200" }, { "row7-v42", "row7-v300", "row7-v400" } };
      for (int i = 0; i < keys.length; i++) {
        upsertPayload(conn, table, i + 1L, buildInt32MapPayload(keys[i], values[i]).toByteArray());
      }
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + index)) {
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
      }
      String pointSql =
        "SELECT ID FROM " + table + " WHERE PROTO_VALUE(PAYLOAD, 'int32_map[42]', 'VARCHAR') = ?";
      try (PreparedStatement ps = conn.prepareStatement(pointSql)) {
        ps.setString(1, "row4-v42");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4L, rs.getLong(1));
          assertFalse(rs.next());
        }
        validateExplainPlan(ps, index);
      }
    }
  }

  @Test
  public void protoExistsAcrossPresenceAndAbsenceStates() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      Descriptors.Descriptor child =
        allTypesDescriptor.findFieldByName("child_msg").getMessageType();
      Descriptors.FieldDescriptor int32MapField = allTypesDescriptor.findFieldByName("int32_map");
      Descriptors.Descriptor int32EntryType = int32MapField.getMessageType();
      byte[][] payloads = new byte[6][];
      payloads[0] = DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 42L)
        .setField(allTypesDescriptor.findFieldByName("child_msg"),
          DynamicMessage.newBuilder(child)
            .setField(child.findFieldByName("payload_string"), "hello").build())
        .addRepeatedField(int32MapField,
          DynamicMessage.newBuilder(int32EntryType)
            .setField(int32EntryType.findFieldByName("key"), 42)
            .setField(int32EntryType.findFieldByName("value"), "v1").build())
        .build().toByteArray();
      payloads[1] = DynamicMessage.newBuilder(allTypesDescriptor)
        .addRepeatedField(int32MapField,
          DynamicMessage.newBuilder(int32EntryType)
            .setField(int32EntryType.findFieldByName("key"), 99)
            .setField(int32EntryType.findFieldByName("value"), "v2").build())
        .build().toByteArray();
      payloads[2] = DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 100L)
        .setField(allTypesDescriptor.findFieldByName("child_msg"),
          DynamicMessage.newBuilder(child).setField(child.findFieldByName("filler"), 1).build())
        .build().toByteArray();
      payloads[3] = DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 50L)
        .setField(allTypesDescriptor.findFieldByName("child_msg"),
          DynamicMessage.newBuilder(child).setField(child.findFieldByName("payload_string"), "")
            .build())
        .addRepeatedField(int32MapField,
          DynamicMessage.newBuilder(int32EntryType)
            .setField(int32EntryType.findFieldByName("key"), 42)
            .setField(int32EntryType.findFieldByName("value"), "v3").build())
        .build().toByteArray();
      payloads[4] = DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("int64_field"), 200L)
        .setField(allTypesDescriptor.findFieldByName("child_msg"),
          DynamicMessage.newBuilder(child).setField(child.findFieldByName("filler"), 2).build())
        .addRepeatedField(int32MapField,
          DynamicMessage.newBuilder(int32EntryType)
            .setField(int32EntryType.findFieldByName("key"), 42)
            .setField(int32EntryType.findFieldByName("value"), "v5").build())
        .build().toByteArray();
      payloads[5] = DynamicMessage.newBuilder(allTypesDescriptor)
        .setField(allTypesDescriptor.findFieldByName("child_msg"),
          DynamicMessage.newBuilder(child)
            .setField(child.findFieldByName("payload_string"), "present").build())
        .addRepeatedField(int32MapField,
          DynamicMessage.newBuilder(int32EntryType)
            .setField(int32EntryType.findFieldByName("key"), 42)
            .setField(int32EntryType.findFieldByName("value"), "v6").build())
        .build().toByteArray();
      for (int i = 0; i < payloads.length; i++) {
        upsertPayload(conn, table, i + 1L, payloads[i]);
      }
      String sql = "SELECT ID, PROTO_EXISTS(PAYLOAD, 'int64_field'), "
        + "PROTO_EXISTS(PAYLOAD, 'child_msg.payload_string'), "
        + "PROTO_EXISTS(PAYLOAD, 'int32_map[42]') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        boolean[] expInt64 = { true, false, true, true, true, false };
        boolean[] expChildStr = { true, false, false, false, false, true };
        boolean[] expMap42 = { true, false, false, true, true, true };
        for (int i = 0; i < expInt64.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i + " int64_field", expInt64[i], rs.getBoolean(2));
          assertEquals("row " + i + " child_msg.payload_string", expChildStr[i], rs.getBoolean(3));
          assertEquals("row " + i + " int32_map[42]", expMap42[i], rs.getBoolean(4));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoArrayLengthCountsMapEntries() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      int[][] keysPerRow = { {}, { 1 }, { 1, 2, 3 }, { 5, 10 }, { 100 } };
      String[][] valuesPerRow = { {}, { "a" }, { "a", "b", "c" }, { "d", "e" }, { "f" } };
      for (int i = 0; i < keysPerRow.length; i++) {
        upsertPayload(conn, table, i + 1L,
          buildInt32MapPayload(keysPerRow[i], valuesPerRow[i]).toByteArray());
      }
      String sql =
        "SELECT ID, PROTO_ARRAY_LENGTH(PAYLOAD, 'int32_map') FROM " + table + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        int[] expected = { 0, 1, 3, 2, 1 };
        for (int i = 0; i < expected.length; i++) {
          assertTrue("row " + i, rs.next());
          assertEquals(i + 1L, rs.getLong(1));
          assertEquals("row " + i, expected[i], rs.getInt(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoFieldTypeReturnsCanonicalSqlTypeNamesAcrossProtoTypes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      String table = createRegisteredTable(conn);
      for (long id = 1L; id <= 6L; id++) {
        upsertPayload(conn, table, id, DynamicMessage.newBuilder(allTypesDescriptor)
          .setField(allTypesDescriptor.findFieldByName("int64_field"), id).build().toByteArray());
      }
      String sql = "SELECT ID, " + "PROTO_FIELD_TYPE(PAYLOAD, 'int64_field'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'fixed32_field'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'sfixed64_field'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'bytes_field'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'child_msg.payload_string'), "
        + "PROTO_FIELD_TYPE(PAYLOAD, 'child_msg.bool_flag') " + "FROM " + table
        + " ORDER BY ID ASC";
      try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
        for (long id = 1L; id <= 6L; id++) {
          assertTrue("row " + id, rs.next());
          assertEquals(id, rs.getLong(1));
          assertEquals("row " + id, "BIGINT", rs.getString(2));
          assertEquals("row " + id, "BIGINT", rs.getString(3));
          assertEquals("row " + id, "BIGINT", rs.getString(4));
          assertEquals("row " + id, "VARBINARY", rs.getString(5));
          assertEquals("row " + id, "VARCHAR", rs.getString(6));
          assertEquals("row " + id, "BOOLEAN", rs.getString(7));
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

  private static void upsertPayload(Connection conn, String table, long id, byte[] payload)
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

  private static DynamicMessage buildStrIntMapPayload(String[] keys, long[] values) {
    Descriptors.FieldDescriptor mapField = allTypesDescriptor.findFieldByName("str_int_map");
    Descriptors.Descriptor entryType = mapField.getMessageType();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(allTypesDescriptor);
    for (int i = 0; i < keys.length; i++) {
      DynamicMessage entry =
        DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByName("key"), keys[i])
          .setField(entryType.findFieldByName("value"), values[i]).build();
      builder.addRepeatedField(mapField, entry);
    }
    return builder.build();
  }

  private static DynamicMessage buildInt32MapPayload(int[] keys, String[] values) {
    Descriptors.FieldDescriptor mapField = allTypesDescriptor.findFieldByName("int32_map");
    Descriptors.Descriptor entryType = mapField.getMessageType();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(allTypesDescriptor);
    for (int i = 0; i < keys.length; i++) {
      DynamicMessage entry =
        DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByName("key"), keys[i])
          .setField(entryType.findFieldByName("value"), values[i]).build();
      builder.addRepeatedField(mapField, entry);
    }
    return builder.build();
  }

  private static DynamicMessage buildInt64MapPayload(long[] keys, String[] values) {
    Descriptors.FieldDescriptor mapField = allTypesDescriptor.findFieldByName("int64_map");
    Descriptors.Descriptor entryType = mapField.getMessageType();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(allTypesDescriptor);
    for (int i = 0; i < keys.length; i++) {
      DynamicMessage entry =
        DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByName("key"), keys[i])
          .setField(entryType.findFieldByName("value"), values[i]).build();
      builder.addRepeatedField(mapField, entry);
    }
    return builder.build();
  }

  private static DynamicMessage buildStrFloatAndDoubleMapPayload(String[] keys, float[] floatValues,
    double[] doubleValues, int rowIndex) {
    Descriptors.FieldDescriptor floatMapField = allTypesDescriptor.findFieldByName("str_float_map");
    Descriptors.FieldDescriptor doubleMapField =
      allTypesDescriptor.findFieldByName("str_double_map");
    Descriptors.Descriptor floatEntry = floatMapField.getMessageType();
    Descriptors.Descriptor doubleEntry = doubleMapField.getMessageType();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(allTypesDescriptor)
      .setField(allTypesDescriptor.findFieldByName("int64_field"), rowIndex + 1L);
    for (int i = 0; i < keys.length; i++) {
      builder.addRepeatedField(floatMapField,
        DynamicMessage.newBuilder(floatEntry).setField(floatEntry.findFieldByName("key"), keys[i])
          .setField(floatEntry.findFieldByName("value"), floatValues[i]).build());
      builder.addRepeatedField(doubleMapField,
        DynamicMessage.newBuilder(doubleEntry).setField(doubleEntry.findFieldByName("key"), keys[i])
          .setField(doubleEntry.findFieldByName("value"), doubleValues[i]).build());
    }
    return builder.build();
  }

  private static DescriptorProto mapEntryProto(String name, Type keyType, Type valueType) {
    return DescriptorProto.newBuilder().setName(name)
      .setOptions(MessageOptions.newBuilder().setMapEntry(true))
      .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(keyType)
        .setLabel(Label.LABEL_OPTIONAL).build())
      .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(valueType)
        .setLabel(Label.LABEL_OPTIONAL).build())
      .build();
  }

  @Test
  public void protoValueAtTagNestedChildStringByTagNumber() throws Exception {
    String table = ensureTable();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      upsertRows(conn, table);
      String sql =
        "SELECT ID, PROTO_VALUE_AT_TAG(PAYLOAD, '8.1', 'VARCHAR') FROM " + table + " ORDER BY ID";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals("hello", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertTrue(rs.next());
        assertEquals(5L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertTrue(rs.next());
        assertEquals(6L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueAtTagVarintLeafAsBigint() throws Exception {
    String table = ensureTable();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      upsertRows(conn, table);
      String sql =
        "SELECT ID, PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'BIGINT') FROM " + table + " ORDER BY ID";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals(42L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertNull(rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(100L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(50L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(5L, rs.getLong(1));
        assertEquals(200L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(6L, rs.getLong(1));
        assertNull(rs.getObject(2));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueAtTagMapAsRepeatedIndexAccess() throws Exception {
    String table = ensureTable();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      upsertRows(conn, table);
      String sql = "SELECT ID, PROTO_VALUE_AT_TAG(PAYLOAD, '11[0].2', 'VARCHAR') FROM " + table
        + " ORDER BY ID";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals("v1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals("v2", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals("v3", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(5L, rs.getLong(1));
        assertEquals("v5", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(6L, rs.getLong(1));
        assertNull(rs.getString(2));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void protoValueAtTagWireTypeMismatchReturnsNull() throws Exception {
    String table = ensureTable();
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      upsertRows(conn, table);
      String sql =
        "SELECT ID, PROTO_VALUE_AT_TAG(PAYLOAD, '7', 'DOUBLE') FROM " + table + " ORDER BY ID ASC";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        for (long id = 1L; id <= 6L; id++) {
          assertTrue("row " + id, rs.next());
          assertEquals(id, rs.getLong(1));
          assertNull("row " + id, rs.getObject(2));
          assertTrue("row " + id, rs.wasNull());
        }
        assertFalse(rs.next());
      }
    }
  }

  private String atTagTable;

  private String ensureTable() throws Exception {
    if (atTagTable != null) {
      return atTagTable;
    }
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps())) {
      atTagTable = generateUniqueName();
      createdTables.add(atTagTable);
      conn.createStatement().execute(
        "CREATE TABLE " + atTagTable + " (ID BIGINT NOT NULL PRIMARY KEY, PAYLOAD VARBINARY)"
          + (columnEncoded ? " COLUMN_ENCODED_BYTES=2" : " COLUMN_ENCODED_BYTES=0"));
      upsertRows(conn, atTagTable);
    }
    return atTagTable;
  }

  private void upsertRows(Connection conn, String table) throws SQLException {
    Descriptors.FieldDescriptor int32MapField = allTypesDescriptor.findFieldByName("int32_map");
    Descriptors.Descriptor int32EntryType = int32MapField.getMessageType();
    Descriptors.Descriptor child = allTypesDescriptor.findFieldByName("child_msg").getMessageType();
    byte[][] payloads = new byte[6][];
    payloads[0] = DynamicMessage.newBuilder(allTypesDescriptor)
      .setField(allTypesDescriptor.findFieldByName("int64_field"), 42L)
      .setField(allTypesDescriptor.findFieldByName("child_msg"),
        DynamicMessage.newBuilder(child).setField(child.findFieldByName("payload_string"), "hello")
          .build())
      .addRepeatedField(int32MapField,
        DynamicMessage.newBuilder(int32EntryType)
          .setField(int32EntryType.findFieldByName("key"), 42)
          .setField(int32EntryType.findFieldByName("value"), "v1").build())
      .build().toByteArray();
    payloads[1] = DynamicMessage.newBuilder(allTypesDescriptor)
      .addRepeatedField(int32MapField,
        DynamicMessage.newBuilder(int32EntryType)
          .setField(int32EntryType.findFieldByName("key"), 99)
          .setField(int32EntryType.findFieldByName("value"), "v2").build())
      .build().toByteArray();
    payloads[2] = DynamicMessage.newBuilder(allTypesDescriptor)
      .setField(allTypesDescriptor.findFieldByName("int64_field"), 100L)
      .setField(allTypesDescriptor.findFieldByName("child_msg"),
        DynamicMessage.newBuilder(child).setField(child.findFieldByName("filler"), 1).build())
      .build().toByteArray();
    payloads[3] = DynamicMessage.newBuilder(allTypesDescriptor)
      .setField(allTypesDescriptor.findFieldByName("int64_field"), 50L)
      .setField(allTypesDescriptor.findFieldByName("child_msg"),
        DynamicMessage.newBuilder(child).setField(child.findFieldByName("payload_string"), "")
          .build())
      .addRepeatedField(int32MapField,
        DynamicMessage.newBuilder(int32EntryType)
          .setField(int32EntryType.findFieldByName("key"), 42)
          .setField(int32EntryType.findFieldByName("value"), "v3").build())
      .build().toByteArray();
    payloads[4] = DynamicMessage.newBuilder(allTypesDescriptor)
      .setField(allTypesDescriptor.findFieldByName("int64_field"), 200L)
      .setField(allTypesDescriptor.findFieldByName("child_msg"),
        DynamicMessage.newBuilder(child).setField(child.findFieldByName("filler"), 2).build())
      .addRepeatedField(int32MapField,
        DynamicMessage.newBuilder(int32EntryType)
          .setField(int32EntryType.findFieldByName("key"), 42)
          .setField(int32EntryType.findFieldByName("value"), "v5").build())
      .build().toByteArray();
    payloads[5] = DynamicMessage.newBuilder(allTypesDescriptor)
      .setField(allTypesDescriptor.findFieldByName("child_msg"),
        DynamicMessage.newBuilder(child).setField(child.findFieldByName("filler"), 3).build())
      .build().toByteArray();
    for (int i = 0; i < payloads.length; i++) {
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + table + " (ID, PAYLOAD) VALUES (?, ?)")) {
        ps.setLong(1, i + 1L);
        ps.setBytes(2, payloads[i]);
        ps.execute();
      }
    }
    conn.commit();
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
