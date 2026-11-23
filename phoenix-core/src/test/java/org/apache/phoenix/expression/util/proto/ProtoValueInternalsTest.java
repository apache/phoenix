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
package org.apache.phoenix.expression.util.proto;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.ProtoValueFunction;
import org.apache.phoenix.expression.util.proto.ProtoPathParser.ParsedSegment;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistry;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistryHolder;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.After;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.MessageOptions;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;
import org.apache.phoenix.thirdparty.com.google.protobuf.WireFormat;

public class ProtoValueInternalsTest {

  @Test
  public void parseValidMixedPathProducesThreeSegments() throws Exception {
    List<ParsedSegment> segments = ProtoPathParser.parse("foo.bar[2].baz");
    assertEquals(3, segments.size());
    assertEquals("foo", segments.get(0).identifier);
    assertNull(segments.get(0).intSubscript);
    assertEquals("bar", segments.get(1).identifier);
    assertEquals(Long.valueOf(2L), segments.get(1).intSubscript);
    assertEquals("baz", segments.get(2).identifier);
    assertNull(segments.get(2).intSubscript);
  }

  @Test
  public void parseRejectsEmptyPath() {
    assertParseFails("");
  }

  @Test
  public void parseRejectsTrailingDot() {
    assertParseFails("foo.");
  }

  @Test
  public void parseRejectsUnbalancedBracket() {
    assertParseFails("foo[3");
  }

  @Test
  public void parseRejectsNonNumericSubscript() {
    assertParseFails("foo[abc]");
  }

  @Test
  public void parseRejectsLeadingDigitInIdentifier() {
    assertParseFails("1foo");
  }

  @Test
  public void parseRejectsChainedBracketsOnSameIdentifier() throws Exception {
    try {
      ProtoPathParser.parse("a[1][5].x");
      fail("expected PROTOBUF_PATH_SYNTAX_ERROR for chained brackets");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.PROTOBUF_PATH_SYNTAX_ERROR.getErrorCode(), e.getErrorCode());
      assertTrue(e.getMessage(), e.getMessage().contains("chained subscripts")
        && e.getMessage().contains("multi-dimensional"));
    }
  }

  @Test
  public void wireTypeForRejectsGroupWithClearError() {
    try {
      ProtoPathResolver.wireTypeFor(Descriptors.FieldDescriptor.Type.GROUP);
      fail("expected IllegalStateException for GROUP");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(),
        e.getMessage().contains("GROUP") && e.getMessage().contains("upstream"));
    }
  }

  @Test
  public void resolveRejectsSubscriptOnScalar() throws Exception {
    Descriptors.Descriptor root = buildSyntheticRoot();
    try {
      ProtoPathResolver.resolve(ProtoPathParser.parse("id[0]"), root, PVarchar.INSTANCE, "id[0]");
      fail("expected PROTOBUF_PATH_CARDINALITY_MISMATCH");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.PROTOBUF_PATH_CARDINALITY_MISMATCH.getErrorCode(),
        e.getErrorCode());
    }
  }

  @Test
  public void resolveRejectsDotOnRepeatedField() throws Exception {
    Descriptors.Descriptor root = buildSyntheticRoot();
    try {
      ProtoPathResolver.resolve(ProtoPathParser.parse("tags.name"), root, PVarchar.INSTANCE,
        "tags.name");
      fail("expected PROTOBUF_PATH_CARDINALITY_MISMATCH");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.PROTOBUF_PATH_CARDINALITY_MISMATCH.getErrorCode(),
        e.getErrorCode());
    }
  }

  @Test
  public void walkerReadsRepeatedIndexScalarField() throws Exception {
    Descriptors.Descriptor root = buildSyntheticRoot();
    DynamicMessage msg =
      DynamicMessage.newBuilder(root).addRepeatedField(root.findFieldByName("tags"), "alpha")
        .addRepeatedField(root.findFieldByName("tags"), "beta")
        .addRepeatedField(root.findFieldByName("tags"), "gamma").build();
    PathPlan plan = ProtoPathResolver.resolve(ProtoPathParser.parse("tags[1]"), root,
      PVarchar.INSTANCE, "tags[1]");
    Object result = walkWhole(msg.toByteArray(), plan);
    assertEquals("beta", result);
  }

  @Test
  public void walkerReturnsScalarDefaultForMapEntryWithOmittedValueTag() throws Exception {
    FileDescriptorProto fileProto =
      FileDescriptorProto.newBuilder().setName("omitted_map_value.proto").setPackage("test")
        .setSyntax("proto3")
        .addMessageType(DescriptorProto.newBuilder().setName("MapHolder")
          .addField(FieldDescriptorProto.newBuilder().setName("attrs").setNumber(1)
            .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_REPEATED)
            .setTypeName("MapHolder.AttrsEntry"))
          .addNestedType(DescriptorProto.newBuilder().setName("AttrsEntry")
            .setOptions(MessageOptions.newBuilder().setMapEntry(true))
            .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
              .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL))
            .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2)
              .setType(Type.TYPE_INT64).setLabel(Label.LABEL_OPTIONAL))))
        .build();
    Descriptors.FileDescriptor file =
      Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    Descriptors.Descriptor mapHolder = file.findMessageTypeByName("MapHolder");

    ByteArrayOutputStream entryBuf = new ByteArrayOutputStream();
    CodedOutputStream entryOut = CodedOutputStream.newInstance(entryBuf);
    entryOut.writeString(1, "foo");
    entryOut.flush();
    byte[] entryBytes = entryBuf.toByteArray();

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(buf);
    out.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(entryBytes.length);
    out.writeRawBytes(entryBytes);
    out.flush();

    PathPlan plan = ProtoPathResolver.resolve(ProtoPathParser.parse("attrs.foo"), mapHolder,
      PLong.INSTANCE, "attrs.foo");
    Object result = walkWhole(buf.toByteArray(), plan);
    assertEquals(Long.valueOf(0L), result);
  }

  @Test
  public void walkerReturnsFirstOccurrenceOfMultiInstanceMessageField() throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
      .setName("multi_instance_message.proto").setPackage("test").setSyntax("proto3")
      .addMessageType(DescriptorProto.newBuilder().setName("Parent")
        .addField(FieldDescriptorProto.newBuilder().setName("child").setNumber(1)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_OPTIONAL).setTypeName("Child")))
      .addMessageType(DescriptorProto.newBuilder().setName("Child")
        .addField(FieldDescriptorProto.newBuilder().setName("a").setNumber(1)
          .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL))
        .addField(FieldDescriptorProto.newBuilder().setName("b").setNumber(2)
          .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL)))
      .build();
    Descriptors.FileDescriptor file =
      Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    Descriptors.Descriptor parent = file.findMessageTypeByName("Parent");
    Descriptors.Descriptor child = file.findMessageTypeByName("Child");
    DynamicMessage childA =
      DynamicMessage.newBuilder(child).setField(child.findFieldByName("a"), "from-first").build();
    DynamicMessage childB =
      DynamicMessage.newBuilder(child).setField(child.findFieldByName("b"), "from-second").build();
    byte[] childABytes = childA.toByteArray();
    byte[] childBBytes = childB.toByteArray();
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(buf);
    out.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(childABytes.length);
    out.writeRawBytes(childABytes);
    out.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(childBBytes.length);
    out.writeRawBytes(childBBytes);
    out.flush();

    PathPlan planA = ProtoPathResolver.resolve(ProtoPathParser.parse("child.a"), parent,
      PVarchar.INSTANCE, "child.a");
    Object aResult = walkWhole(buf.toByteArray(), planA);
    assertEquals("from-first", aResult);

    PathPlan planB = ProtoPathResolver.resolve(ProtoPathParser.parse("child.b"), parent,
      PVarchar.INSTANCE, "child.b");
    Object bResult = walkWhole(buf.toByteArray(), planB);
    assertSame(ProtoPathWalker.LEAF_ABSENT, bResult);
  }

  @Test
  public void walkersReturnFirstOccurrenceOfDuplicateScalarAndMapKey() throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder().setName("dup_scalar.proto")
      .setPackage("test").setSyntax("proto3")
      .addMessageType(
        DescriptorProto.newBuilder().setName("M").addField(FieldDescriptorProto.newBuilder()
          .setName("n").setNumber(1).setType(Type.TYPE_INT64).setLabel(Label.LABEL_OPTIONAL)))
      .build();
    Descriptors.Descriptor m = Descriptors.FileDescriptor
      .buildFrom(fileProto, new Descriptors.FileDescriptor[0]).findMessageTypeByName("M");
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(buf);
    out.writeInt64(1, 7L);
    out.writeInt64(1, 42L);
    out.flush();
    byte[] bytes = buf.toByteArray();
    assertEquals(Long.valueOf(7L), walkWhole(bytes,
      ProtoPathResolver.resolve(ProtoPathParser.parse("n"), m, PLong.INSTANCE, "n")));
    TagPathPlan scalar =
      new TagPathPlan(ProtoTagPathParser.parse("1"), PLong.INSTANCE, WireFormat.WIRETYPE_VARINT);
    assertEquals(7L, ProtoTagPathWalker.walk(bytes, 0, bytes.length, scalar));

    Descriptors.Descriptor holder = stringToInt64MapHolder();
    ByteArrayOutputStream mapBuf = new ByteArrayOutputStream();
    CodedOutputStream mapOut = CodedOutputStream.newInstance(mapBuf);
    writeStringInt64Entry(mapOut, "k", 1L);
    writeStringInt64Entry(mapOut, "k", 2L);
    mapOut.flush();
    assertEquals(Long.valueOf(1L), walkWhole(mapBuf.toByteArray(), ProtoPathResolver
      .resolve(ProtoPathParser.parse("attrs.k"), holder, PLong.INSTANCE, "attrs.k")));
  }

  @Test
  public void leafDefaultIsDeclaredDefaultOrTypeZeroAndNullForNonScalars() throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder().setName("leaf_default.proto")
      .setPackage("test").setSyntax("proto2")
      .addEnumType(EnumDescriptorProto.newBuilder().setName("Color")
        .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(3))
        .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(5)))
      .addMessageType(DescriptorProto.newBuilder().setName("M")
        .addField(optionalField("declared", 1, Type.TYPE_INT64).setDefaultValue("42"))
        .addField(optionalField("plain", 2, Type.TYPE_INT64))
        .addField(optionalField("color", 3, Type.TYPE_ENUM).setTypeName(".test.Color"))
        .addField(optionalField("blue", 4, Type.TYPE_ENUM).setTypeName(".test.Color")
          .setDefaultValue("BLUE"))
        .addField(optionalField("big", 5, Type.TYPE_FIXED32).setDefaultValue("4294967295"))
        .addField(optionalField("blob", 6, Type.TYPE_BYTES).setDefaultValue("ab"))
        .addField(optionalField("arm", 7, Type.TYPE_INT32).setOneofIndex(0))
        .addField(optionalField("child", 8, Type.TYPE_MESSAGE).setTypeName(".test.M"))
        .addField(FieldDescriptorProto.newBuilder().setName("list").setNumber(9)
          .setType(Type.TYPE_INT32).setLabel(Label.LABEL_REPEATED))
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice")))
      .build();
    Descriptors.Descriptor m = Descriptors.FileDescriptor
      .buildFrom(fileProto, new Descriptors.FileDescriptor[0]).findMessageTypeByName("M");
    assertEquals(42L, ProtoPathResolver.leafDefault(m.findFieldByName("declared")));
    assertEquals(0L, ProtoPathResolver.leafDefault(m.findFieldByName("plain")));
    assertEquals("first enum value", 3, ProtoPathResolver.leafDefault(m.findFieldByName("color")));
    assertEquals(5, ProtoPathResolver.leafDefault(m.findFieldByName("blue")));
    assertEquals(4294967295L, ProtoPathResolver.leafDefault(m.findFieldByName("big")));
    assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8),
      (byte[]) ProtoPathResolver.leafDefault(m.findFieldByName("blob")));
    assertNull(ProtoPathResolver.leafDefault(m.findFieldByName("arm")));
    assertNull(ProtoPathResolver.leafDefault(m.findFieldByName("child")));
    assertNull(ProtoPathResolver.leafDefault(m.findFieldByName("list")));
  }

  private static FieldDescriptorProto.Builder optionalField(String name, int number, Type type) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type)
      .setLabel(Label.LABEL_OPTIONAL);
  }

  @Test
  public void walkerDecodesSint32AndSint64ZigZag() throws Exception {
    Descriptors.Descriptor m = sintHolder();
    DynamicMessage msg = DynamicMessage.newBuilder(m).setField(m.findFieldByName("s32"), -5)
      .setField(m.findFieldByName("s64"), Long.MIN_VALUE)
      .addRepeatedField(m.findFieldByName("deltas"), -1L)
      .addRepeatedField(m.findFieldByName("deltas"), 2L)
      .addRepeatedField(m.findFieldByName("deltas"), -300L).build();
    byte[] bytes = msg.toByteArray();

    assertEquals(Integer.valueOf(-5), walkWhole(bytes,
      ProtoPathResolver.resolve(ProtoPathParser.parse("s32"), m, PInteger.INSTANCE, "s32")));
    assertEquals(Long.valueOf(Long.MIN_VALUE), walkWhole(bytes,
      ProtoPathResolver.resolve(ProtoPathParser.parse("s64"), m, PLong.INSTANCE, "s64")));
    // proto3 repeated sint64 is packed by default.
    assertEquals(Long.valueOf(-300L), walkWhole(bytes, ProtoPathResolver
      .resolve(ProtoPathParser.parse("deltas[2]"), m, PLong.INSTANCE, "deltas[2]")));
    assertEquals("INTEGER",
      ProtoPathResolver.canonicalSqlTypeName(m.findFieldByName("s32").getType()));
    assertEquals("BIGINT",
      ProtoPathResolver.canonicalSqlTypeName(m.findFieldByName("s64").getType()));
  }

  @Test
  public void sint64LeafRejectsNarrowingToInteger() throws Exception {
    Descriptors.Descriptor m = sintHolder();
    try {
      ProtoPathResolver.resolve(ProtoPathParser.parse("s64"), m, PInteger.INSTANCE, "s64");
      fail("expected narrowing rejection");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.PROTOBUF_VALUE_TYPE_NARROWING_NOT_PERMITTED.getErrorCode(),
        e.getErrorCode());
    }
  }

  @Test
  public void resolverRejectsSintMapKeys() throws Exception {
    assertResolveFails(sintHolder(), "by_offset[1]",
      SQLExceptionCode.PROTOBUF_PATH_UNSUPPORTED_FEATURE);
  }

  private static Descriptors.Descriptor sintHolder() throws Exception {
    FileDescriptorProto fileProto =
      FileDescriptorProto.newBuilder().setName("sint.proto").setPackage("test").setSyntax("proto3")
        .addMessageType(DescriptorProto.newBuilder().setName("S")
          .addNestedType(DescriptorProto.newBuilder().setName("ByOffsetEntry")
            .setOptions(MessageOptions.newBuilder().setMapEntry(true))
            .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
              .setType(Type.TYPE_SINT32).setLabel(Label.LABEL_OPTIONAL))
            .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2)
              .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL)))
          .addField(FieldDescriptorProto.newBuilder().setName("s32").setNumber(1)
            .setType(Type.TYPE_SINT32).setLabel(Label.LABEL_OPTIONAL))
          .addField(FieldDescriptorProto.newBuilder().setName("s64").setNumber(2)
            .setType(Type.TYPE_SINT64).setLabel(Label.LABEL_OPTIONAL))
          .addField(FieldDescriptorProto.newBuilder().setName("deltas").setNumber(3)
            .setType(Type.TYPE_SINT64).setLabel(Label.LABEL_REPEATED))
          .addField(FieldDescriptorProto.newBuilder().setName("by_offset").setNumber(4)
            .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_REPEATED)
            .setTypeName(".test.S.ByOffsetEntry")))
        .build();
    return Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0])
      .findMessageTypeByName("S");
  }

  @Test
  public void nestedMessageLeafReturnsVarbinaryNotProtobuf() throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder().setName("nested_leaf.proto")
      .setPackage("test").setSyntax("proto3")
      .addMessageType(DescriptorProto.newBuilder().setName("Parent")
        .addField(FieldDescriptorProto.newBuilder().setName("child").setNumber(1)
          .setType(Type.TYPE_MESSAGE).setLabel(Label.LABEL_OPTIONAL).setTypeName("Child")))
      .addMessageType(
        DescriptorProto.newBuilder().setName("Child").addField(FieldDescriptorProto.newBuilder()
          .setName("a").setNumber(1).setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL)))
      .build();
    Descriptors.Descriptor parent = Descriptors.FileDescriptor
      .buildFrom(fileProto, new Descriptors.FileDescriptor[0]).findMessageTypeByName("Parent");

    assertNotNull(ProtoPathResolver.resolve(ProtoPathParser.parse("child"), parent,
      PVarbinary.INSTANCE, "child"));
    try {
      ProtoPathResolver.resolve(ProtoPathParser.parse("child"), parent, PProtobuf.INSTANCE,
        "child");
      fail("a nested message must not be returned as PROTOBUF");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
    }
    PathPlan plan =
      ProtoPathResolver.resolveForExistence(ProtoPathParser.parse("child"), parent, "child");
    assertEquals("VARBINARY", ProtoPathResolver.canonicalSqlTypeName(plan.getLeafProtoFieldType()));
  }

  private static void assertResolveFails(Descriptors.Descriptor m, String path,
    SQLExceptionCode expected) {
    try {
      ProtoPathResolver.resolve(ProtoPathParser.parse(path), m, PVarchar.INSTANCE, path);
      fail("expected " + expected + " for " + path);
    } catch (SQLException e) {
      assertEquals(path + ": " + e.getMessage(), expected.getErrorCode(), e.getErrorCode());
    }
  }

  private static Descriptors.Descriptor stringToInt64MapHolder() throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder().setName("dup_map.proto")
      .setPackage("test").setSyntax("proto3")
      .addMessageType(DescriptorProto.newBuilder().setName("Holder")
        .addNestedType(DescriptorProto.newBuilder().setName("AttrsEntry")
          .setOptions(MessageOptions.newBuilder().setMapEntry(true))
          .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
            .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL))
          .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2)
            .setType(Type.TYPE_INT64).setLabel(Label.LABEL_OPTIONAL)))
        .addField(
          FieldDescriptorProto.newBuilder().setName("attrs").setNumber(1).setType(Type.TYPE_MESSAGE)
            .setLabel(Label.LABEL_REPEATED).setTypeName(".test.Holder.AttrsEntry")))
      .build();
    return Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0])
      .findMessageTypeByName("Holder");
  }

  private static void writeStringInt64Entry(CodedOutputStream out, String key, long value)
    throws Exception {
    ByteArrayOutputStream entry = new ByteArrayOutputStream();
    CodedOutputStream e = CodedOutputStream.newInstance(entry);
    e.writeString(1, key);
    e.writeInt64(2, value);
    e.flush();
    byte[] entryBytes = entry.toByteArray();
    out.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(entryBytes.length);
    out.writeRawBytes(entryBytes);
  }

  @Test
  public void walkerReadsPackedAndUnpackedRepeatedInt64AcrossSplitChunks() throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder().setName("packed_split.proto")
      .setPackage("test").setSyntax("proto3")
      .addMessageType(
        DescriptorProto.newBuilder().setName("Container").addField(FieldDescriptorProto.newBuilder()
          .setName("nums").setNumber(7).setType(Type.TYPE_INT64).setLabel(Label.LABEL_REPEATED)))
      .build();
    Descriptors.FileDescriptor file =
      Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    Descriptors.Descriptor container = file.findMessageTypeByName("Container");

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(buf);
    ByteArrayOutputStream packedA = new ByteArrayOutputStream();
    CodedOutputStream packedAOut = CodedOutputStream.newInstance(packedA);
    packedAOut.writeInt64NoTag(10L);
    packedAOut.writeInt64NoTag(20L);
    packedAOut.flush();
    out.writeTag(7, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(packedA.size());
    out.writeRawBytes(packedA.toByteArray());
    out.writeInt64(7, 30L);
    ByteArrayOutputStream packedB = new ByteArrayOutputStream();
    CodedOutputStream packedBOut = CodedOutputStream.newInstance(packedB);
    packedBOut.writeInt64NoTag(40L);
    packedBOut.writeInt64NoTag(50L);
    packedBOut.flush();
    out.writeTag(7, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(packedB.size());
    out.writeRawBytes(packedB.toByteArray());
    out.flush();
    byte[] payload = buf.toByteArray();

    long[] expected = { 10L, 20L, 30L, 40L, 50L };
    for (int i = 0; i < expected.length; i++) {
      PathPlan plan = ProtoPathResolver.resolve(ProtoPathParser.parse("nums[" + i + "]"), container,
        PLong.INSTANCE, "nums[" + i + "]");
      Object result = walkWhole(payload, plan);
      assertEquals("index " + i, Long.valueOf(expected[i]), result);
    }
    PathPlan pastEnd = ProtoPathResolver.resolve(ProtoPathParser.parse("nums[5]"), container,
      PLong.INSTANCE, "nums[5]");
    assertNull(walkWhole(payload, pastEnd));
  }

  @After
  public void resetHolder() {
    ProtoDescriptorRegistryHolder.uninstall();
  }

  @Test
  public void holderInstallIsIdempotentSecondCallReturnsFalse() {
    ProtoDescriptorRegistry first = newRegistryStub();
    ProtoDescriptorRegistry second = newRegistryStub();
    assertTrue(ProtoDescriptorRegistryHolder.install(first));
    assertFalse(ProtoDescriptorRegistryHolder.install(second));
    assertEquals(first, ProtoDescriptorRegistryHolder.get());
  }

  @Test
  public void holderGetBeforeInstallThrows() {
    try {
      ProtoDescriptorRegistryHolder.get();
      fail("expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertEquals(ProtoDescriptorRegistryHolder.ENDPOINT_NOT_REGISTERED, e.getMessage());
    }
  }

  @Test
  public void holderGetLazilyInstantiatesFromFactory() {
    ProtoDescriptorRegistry registry = newRegistryStub();
    AtomicInteger created = new AtomicInteger();
    assertTrue(ProtoDescriptorRegistryHolder.setFactory(() -> {
      created.incrementAndGet();
      return registry;
    }));
    assertNull(ProtoDescriptorRegistryHolder.getIfInstalled());
    assertSame(registry, ProtoDescriptorRegistryHolder.get());
    assertSame(registry, ProtoDescriptorRegistryHolder.getIfInstalled());
    assertEquals(1, created.get());
    assertSame(registry, ProtoDescriptorRegistryHolder.get());
    assertEquals(1, created.get());
  }

  @Test
  public void holderFactoryReturningNullThrows() {
    ProtoDescriptorRegistryHolder.setFactory(() -> null);
    try {
      ProtoDescriptorRegistryHolder.get();
      fail("expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("factory returned null"));
    }
  }

  @Test
  public void holderSetFactoryIsIdempotent() {
    ProtoDescriptorRegistry.Loader loader = name -> null;
    assertTrue(ProtoDescriptorRegistryHolder
      .setFactory(() -> new ProtoDescriptorRegistry(new Configuration(), loader)));
    assertFalse(ProtoDescriptorRegistryHolder
      .setFactory(() -> new ProtoDescriptorRegistry(new Configuration(), loader)));
  }

  @Test
  public void holderExplicitInstallShortCircuitsFactory() {
    ProtoDescriptorRegistry explicit = newRegistryStub();
    ProtoDescriptorRegistry viaFactory = newRegistryStub();
    AtomicInteger created = new AtomicInteger();
    assertTrue(ProtoDescriptorRegistryHolder.install(explicit));
    ProtoDescriptorRegistryHolder.setFactory(() -> {
      created.incrementAndGet();
      return viaFactory;
    });
    assertSame(explicit, ProtoDescriptorRegistryHolder.get());
    assertEquals(0, created.get());
  }

  @Test
  public void holderFactoryInvokedExactlyOnceUnderConcurrency() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryStub();
    AtomicInteger created = new AtomicInteger();
    ProtoDescriptorRegistryHolder.setFactory(() -> {
      created.incrementAndGet();
      LockSupport.parkNanos(10_000_000L);
      return registry;
    });
    int threadCount = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    try {
      CountDownLatch startGate = new CountDownLatch(1);
      List<Future<ProtoDescriptorRegistry>> futures = new ArrayList<>(threadCount);
      for (int i = 0; i < threadCount; i++) {
        futures.add(pool.submit(() -> {
          startGate.await();
          return ProtoDescriptorRegistryHolder.get();
        }));
      }
      startGate.countDown();
      for (Future<ProtoDescriptorRegistry> f : futures) {
        assertSame(registry, f.get(10, TimeUnit.SECONDS));
      }
      assertEquals(1, created.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void planCacheReturnsSameInstanceForSameKey() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup("test.Root");
    PathPlan first =
      cached.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
        d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));
    PathPlan second =
      cached.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
        d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));
    assertSame(first, second);
  }

  @Test
  public void planCacheKeyIncludesRequestedType() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup("test.Root");
    PathPlan asInt = cached.getOrCompilePathPlan("count", ProtoDescriptorRegistry.PlanKind.VALUE,
      PInteger.INSTANCE, d -> ProtoPathResolver.resolve(ProtoPathParser.parse("count"), d,
        PInteger.INSTANCE, "count"));
    PathPlan asLong =
      cached.getOrCompilePathPlan("count", ProtoDescriptorRegistry.PlanKind.VALUE, PLong.INSTANCE,
        d -> ProtoPathResolver.resolve(ProtoPathParser.parse("count"), d, PLong.INSTANCE, "count"));
    assertNotSame(asInt, asLong);
    assertSame(PInteger.INSTANCE, asInt.getRequestedType());
    assertSame(PLong.INSTANCE, asLong.getRequestedType());
  }

  @Test
  public void planCacheSingleFlightUnderConcurrency() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup("test.Root");
    AtomicInteger compileCount = new AtomicInteger();
    int threadCount = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    try {
      CountDownLatch startGate = new CountDownLatch(1);
      List<Future<PathPlan>> futures = new ArrayList<>(threadCount);
      for (int i = 0; i < threadCount; i++) {
        Callable<PathPlan> task = () -> {
          startGate.await();
          return cached.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE,
            PVarchar.INSTANCE, d -> {
              compileCount.incrementAndGet();
              LockSupport.parkNanos(20_000_000L);
              return ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE,
                "id");
            });
        };
        futures.add(pool.submit(task));
      }
      startGate.countDown();
      PathPlan shared = null;
      for (Future<PathPlan> f : futures) {
        PathPlan p = f.get(10, TimeUnit.SECONDS);
        if (shared == null) {
          shared = p;
        } else {
          assertSame(shared, p);
        }
      }
      assertEquals(1, compileCount.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void planCacheDroppedOnRegistryInvalidate() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistry.CachedDescriptor cachedBefore = registry.lookup("test.Root");
    PathPlan planBefore = cachedBefore.getOrCompilePathPlan("id",
      ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
      d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));

    registry.invalidate("test.Root");

    ProtoDescriptorRegistry.CachedDescriptor cachedAfter = registry.lookup("test.Root");
    assertNotSame(cachedBefore, cachedAfter);
    PathPlan planAfter = cachedAfter.getOrCompilePathPlan("id",
      ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
      d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));
    assertNotSame(planBefore, planAfter);
  }

  @Test
  public void planCacheDifferentDescriptorsHaveIndependentPlans() throws Exception {
    Loader loader = name -> {
      if ("test.Root".equals(name) || "test.RootTwo".equals(name)) {
        return newSyntheticRootFileSetBytes();
      }
      return null;
    };
    ProtoDescriptorRegistry registry = new ProtoDescriptorRegistry(new Configuration(), loader);
    ProtoDescriptorRegistry.CachedDescriptor first = registry.lookup("test.Root");
    ProtoDescriptorRegistry.CachedDescriptor second = registry.lookup("test.RootTwo");
    PathPlan planA =
      first.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
        d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));
    PathPlan planB =
      second.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
        d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));
    assertNotSame(planA, planB);
  }

  @Test
  public void planCacheDoesNotStoreCompilerFailures() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup("test.Root");
    try {
      cached.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
        d -> {
          throw new SQLException("boom");
        });
      fail("expected SQLException");
    } catch (SQLException e) {
      assertEquals("boom", e.getMessage());
    }
    PathPlan plan =
      cached.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
        d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));
    assertSame(plan, cached.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE,
      PVarchar.INSTANCE, d -> {
        throw new IllegalStateException("must not be called");
      }));
  }

  private static ProtoDescriptorRegistry newRegistryForSyntheticRoot() {
    byte[] setBytes = newSyntheticRootFileSetBytes();
    Loader loader = name -> {
      if ("test.Root".equals(name) || "test.RootTwo".equals(name)) {
        return setBytes;
      }
      return null;
    };
    return new ProtoDescriptorRegistry(new Configuration(), loader);
  }

  private static byte[] newSyntheticRootFileSetBytes() {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
      .setName("proto_value_internals_root_for_cache.proto").setPackage("test").setSyntax("proto3")
      .addMessageType(DescriptorProto.newBuilder().setName("Root")
        .addField(FieldDescriptorProto.newBuilder().setName("id").setNumber(1)
          .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL))
        .addField(FieldDescriptorProto.newBuilder().setName("count").setNumber(2)
          .setType(Type.TYPE_INT32).setLabel(Label.LABEL_OPTIONAL)))
      .addMessageType(
        DescriptorProto.newBuilder().setName("RootTwo").addField(FieldDescriptorProto.newBuilder()
          .setName("id").setNumber(1).setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL)))
      .build();
    return FileDescriptorSet.newBuilder().addFile(fileProto).build().toByteArray();
  }

  private interface Loader extends ProtoDescriptorRegistry.Loader {
  }

  @Test
  public void instancePlanCacheReusesSameIdentityAcrossEvaluates() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistryHolder.install(registry);
    ProtoValueFunction fn =
      newFunction("id", "VARCHAR", "test.Root", newRootBytesWithId("first-hit"));

    assertNull(readCachedPlan(fn));
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    Tuple tuple = new SingleKeyValueTuple();
    assertTrue(fn.evaluate(tuple, ptr));
    PathPlan first = readCachedPlan(fn);
    assertTrue(first != null);

    assertTrue(fn.evaluate(tuple, ptr));
    PathPlan second = readCachedPlan(fn);
    assertSame(first, second);
  }

  @Test
  public void instancePlanCacheSnapshotMatchesJvmWidePlan() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistryHolder.install(registry);
    ProtoValueFunction fn =
      newFunction("id", "VARCHAR", "test.Root", newRootBytesWithId("share-check"));
    fn.evaluate(new SingleKeyValueTuple(), new ImmutableBytesWritable());
    PathPlan instancePlan = readCachedPlan(fn);

    ProtoValueFunction fn2 =
      newFunction("id", "VARCHAR", "test.Root", newRootBytesWithId("share-check-2"));
    fn2.evaluate(new SingleKeyValueTuple(), new ImmutableBytesWritable());
    PathPlan instancePlan2 = readCachedPlan(fn2);

    assertSame(instancePlan, instancePlan2);

    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup("test.Root");
    PathPlan lookedUp =
      cached.getOrCompilePathPlan("id", ProtoDescriptorRegistry.PlanKind.VALUE, PVarchar.INSTANCE,
        d -> ProtoPathResolver.resolve(ProtoPathParser.parse("id"), d, PVarchar.INSTANCE, "id"));
    assertSame(instancePlan, lookedUp);
  }

  @Test
  public void instancePlanCacheClearedByReadFieldsWithDifferentChildren() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistryHolder.install(registry);

    ProtoValueFunction fnA = newFunction("id", "VARCHAR", "test.Root", newRootBytesWithId("hello"));
    fnA.evaluate(new SingleKeyValueTuple(), new ImmutableBytesWritable());
    PathPlan planForId = readCachedPlan(fnA);
    assertTrue(planForId != null);
    assertSame(PVarchar.INSTANCE, planForId.getRequestedType());

    ProtoValueFunction fnB =
      newFunction("count", "INTEGER", "test.Root", newRootBytesWithCount(42));
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buf)) {
      fnB.write(out);
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf.toByteArray()))) {
      fnA.readFields(in);
    }
    assertNull("readFields must invalidate the per-instance PathPlan snapshot",
      readCachedPlan(fnA));

    fnA.evaluate(new SingleKeyValueTuple(), new ImmutableBytesWritable());
    PathPlan planAfter = readCachedPlan(fnA);
    assertTrue(planAfter != null);
    assertNotSame(planForId, planAfter);
    assertSame(PInteger.INSTANCE, planAfter.getRequestedType());
  }

  @Test
  public void instancePlanCacheDoubleCheckedLockingIsSingleFlight() throws Exception {
    ProtoDescriptorRegistry registry = newRegistryForSyntheticRoot();
    ProtoDescriptorRegistryHolder.install(registry);
    ProtoValueFunction fn = newFunction("id", "VARCHAR", "test.Root", newRootBytesWithId("race"));

    int threadCount = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    try {
      CountDownLatch startGate = new CountDownLatch(1);
      List<Future<PathPlan>> futures = new ArrayList<>(threadCount);
      for (int i = 0; i < threadCount; i++) {
        Callable<PathPlan> task = () -> {
          startGate.await();
          fn.evaluate(new SingleKeyValueTuple(), new ImmutableBytesWritable());
          return readCachedPlan(fn);
        };
        futures.add(pool.submit(task));
      }
      startGate.countDown();
      PathPlan shared = null;
      for (Future<PathPlan> f : futures) {
        PathPlan p = f.get(10, TimeUnit.SECONDS);
        assertTrue("no thread should observe a null snapshot after evaluate", p != null);
        if (shared == null) {
          shared = p;
        } else {
          assertSame(shared, p);
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static PathPlan readCachedPlan(ProtoValueFunction fn) throws Exception {
    Field field = ProtoValueFunction.class.getDeclaredField("cachedPlan");
    field.setAccessible(true);
    return (PathPlan) field.get(fn);
  }

  private static ProtoValueFunction newFunction(String path, String sqlType, String descriptorName,
    byte[] payload) throws Exception {
    Expression payloadExpr = LiteralExpression.newConstant(payload, PVarbinary.INSTANCE);
    Expression pathExpr = LiteralExpression.newConstant(path, PVarchar.INSTANCE);
    Expression typeExpr = LiteralExpression.newConstant(sqlType, PVarchar.INSTANCE);
    Expression descriptorExpr = LiteralExpression.newConstant(descriptorName, PVarchar.INSTANCE);
    return new ProtoValueFunction(Arrays.asList(payloadExpr, pathExpr, typeExpr, descriptorExpr));
  }

  private static byte[] newRootBytesWithId(String id) throws Exception {
    Descriptors.Descriptor root = resolveRegistryRoot();
    return DynamicMessage.newBuilder(root).setField(root.findFieldByName("id"), id).build()
      .toByteArray();
  }

  private static byte[] newRootBytesWithCount(int count) throws Exception {
    Descriptors.Descriptor root = resolveRegistryRoot();
    return DynamicMessage.newBuilder(root).setField(root.findFieldByName("count"), count).build()
      .toByteArray();
  }

  private static Descriptors.Descriptor resolveRegistryRoot() throws Exception {
    Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
      FileDescriptorSet.parseFrom(newSyntheticRootFileSetBytes()).getFile(0),
      new Descriptors.FileDescriptor[0]);
    return file.findMessageTypeByName("Root");
  }

  private static void assertParseFails(String path) {
    try {
      ProtoPathParser.parse(path);
      fail("expected PROTOBUF_PATH_SYNTAX_ERROR for path: " + path);
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.PROTOBUF_PATH_SYNTAX_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  private static Object walkWhole(byte[] payload, PathPlan plan) throws Exception {
    return ProtoPathWalker.walk(payload, 0, payload.length, plan);
  }

  /** Embeds {@code payload} at {@code pre} inside a buffer padded with 0xFF garbage. */
  private static byte[] embed(byte[] payload, int pre, int post) {
    byte[] big = new byte[pre + payload.length + post];
    Arrays.fill(big, (byte) 0xFF);
    System.arraycopy(payload, 0, big, pre, payload.length);
    return big;
  }

  @Test
  public void walkersReadPayloadInPlaceAtOffsetWithoutAliasing() throws Exception {
    Descriptors.Descriptor root = buildSyntheticRoot();
    byte[] payload = DynamicMessage.newBuilder(root).setField(root.findFieldByName("id"), "row-1")
      .setField(root.findFieldByName("count"), 42)
      .addRepeatedField(root.findFieldByName("tags"), "alpha")
      .addRepeatedField(root.findFieldByName("tags"), "beta").build().toByteArray();
    byte[] big = embed(payload, 7, 5);

    PathPlan idPlan =
      ProtoPathResolver.resolve(ProtoPathParser.parse("id"), root, PVarbinary.INSTANCE, "id");
    PathPlan countPlan =
      ProtoPathResolver.resolve(ProtoPathParser.parse("count"), root, PInteger.INSTANCE, "count");
    PathPlan tagPlan = ProtoPathResolver.resolve(ProtoPathParser.parse("tags[1]"), root,
      PVarchar.INSTANCE, "tags[1]");
    PathPlan lenPlan =
      ProtoPathResolver.resolveForArrayLength(ProtoPathParser.parse("tags"), root, "tags");
    TagPathPlan tagPathPlan = new TagPathPlan(ProtoTagPathParser.parse("3[0]"), PVarbinary.INSTANCE,
      WireFormat.WIRETYPE_LENGTH_DELIMITED);

    assertEquals("row-1", ProtoPathWalker.walk(big, 7, payload.length, idPlan));
    assertEquals(Integer.valueOf(42), ProtoPathWalker.walk(big, 7, payload.length, countPlan));
    assertEquals("beta", ProtoPathWalker.walk(big, 7, payload.length, tagPlan));
    assertEquals(2, ProtoPathWalker.walkForCount(big, 7, payload.length, lenPlan));
    // A VARBINARY LEN leaf comes back as byte[].
    byte[] tagBytes = (byte[]) ProtoTagPathWalker.walk(big, 7, payload.length, tagPathPlan);
    assertEquals("alpha", new String(tagBytes, java.nio.charset.StandardCharsets.UTF_8));

    // Results are copies: overwriting the source buffer must not change them.
    Arrays.fill(big, (byte) 0);
    assertEquals("alpha", new String(tagBytes, java.nio.charset.StandardCharsets.UTF_8));

    // The walkers stay inside [offset, offset + length): an empty window is absent.
    assertNull(ProtoPathWalker.walk(embed(payload, 3, 3), 3, 0, countPlan));
    assertEquals(0, ProtoPathWalker.walkForCount(embed(payload, 3, 3), 3, 0, lenPlan));
    assertNull(ProtoTagPathWalker.walk(embed(payload, 3, 3), 3, 0, tagPathPlan));
  }

  @Test
  public void protoValueFunctionEvaluatesPayloadAtNonZeroPtrOffset() throws Exception {
    Descriptors.Descriptor root = buildSyntheticRoot();
    byte[] payload = DynamicMessage.newBuilder(root).setField(root.findFieldByName("count"), 42)
      .build().toByteArray();
    byte[] big = embed(payload, 11, 9);
    ProtoDescriptorRegistryHolder.install(newRegistryForSyntheticRoot());
    // Like a column value read from a cell: ptr points into a larger shared buffer.
    Expression payloadArg = new org.apache.phoenix.expression.BaseTerminalExpression() {
      @Override
      public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ptr.set(big, 11, payload.length);
        return true;
      }

      @Override
      public org.apache.phoenix.schema.types.PDataType getDataType() {
        return PVarbinary.INSTANCE;
      }

      @Override
      public <T> T accept(org.apache.phoenix.expression.visitor.ExpressionVisitor<T> visitor) {
        return null;
      }
    };
    List<Expression> children =
      Arrays.asList(payloadArg, LiteralExpression.newConstant("count", PVarchar.INSTANCE),
        LiteralExpression.newConstant("INTEGER", PVarchar.INSTANCE),
        LiteralExpression.newConstant("test.Root", PVarchar.INSTANCE));
    ProtoValueFunction fn = new ProtoValueFunction(children);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue(fn.evaluate(null, ptr));
    assertEquals(42, PInteger.INSTANCE.toObject(ptr.get(), ptr.getOffset(), ptr.getLength()));
  }

  private static Descriptors.Descriptor buildSyntheticRoot() throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
      .setName("proto_value_internals_root.proto").setPackage("test").setSyntax("proto3")
      .addMessageType(DescriptorProto.newBuilder().setName("Root")
        .addField(FieldDescriptorProto.newBuilder().setName("id").setNumber(1)
          .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL))
        .addField(FieldDescriptorProto.newBuilder().setName("count").setNumber(2)
          .setType(Type.TYPE_INT32).setLabel(Label.LABEL_OPTIONAL))
        .addField(FieldDescriptorProto.newBuilder().setName("tags").setNumber(3)
          .setType(Type.TYPE_STRING).setLabel(Label.LABEL_REPEATED)))
      .build();
    Descriptors.FileDescriptor file =
      Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    return file.findMessageTypeByName("Root");
  }

  private static ProtoDescriptorRegistry newRegistryStub() {
    return new ProtoDescriptorRegistry(new Configuration(), name -> null);
  }
}
