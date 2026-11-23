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
package org.apache.phoenix.protobuf.descriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumOptions;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldOptions;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

public class ProtoEvolutionValidatorTest {
  private static final String PKG = "test.evo";
  private static final String MSG_NAME = "Widget";
  private static final String FQ_NAME = PKG + "." + MSG_NAME;
  private static final String COMMON_PKG = "test.common";

  private final ProtoDescriptorResolver resolver = new ProtoDescriptorResolver();

  @Test
  public void renameFieldSameTagIsRejected() throws Exception {
    DescriptorProto oldMsg =
      message(MSG_NAME).addField(scalar("original_name", 1, Type.TYPE_STRING)).build();
    DescriptorProto newMsg =
      message(MSG_NAME).addField(scalar("renamed_name", 1, Type.TYPE_STRING)).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_FIELD_RENAMED, "renamed_name");
  }

  @Test
  public void renumberFieldSameNameIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 2, Type.TYPE_STRING)).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_TAG_RENAMED, "id");
  }

  @Test
  public void incompatibleTypeChangeIsRejected() throws Exception {
    DescriptorProto oldMsg =
      message(MSG_NAME).addField(scalar("count", 1, Type.TYPE_INT32)).build();
    DescriptorProto newMsg =
      message(MSG_NAME).addField(scalar("count", 1, Type.TYPE_STRING)).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE, "count");
  }

  private static final Type[] SCALAR_TYPES_FOR_MATRIX = { Type.TYPE_DOUBLE, Type.TYPE_FLOAT,
    Type.TYPE_INT64, Type.TYPE_UINT64, Type.TYPE_INT32, Type.TYPE_FIXED64, Type.TYPE_FIXED32,
    Type.TYPE_BOOL, Type.TYPE_STRING, Type.TYPE_BYTES, Type.TYPE_UINT32, Type.TYPE_ENUM,
    Type.TYPE_SFIXED32, Type.TYPE_SFIXED64, Type.TYPE_SINT32, Type.TYPE_SINT64, Type.TYPE_MESSAGE };

  @Test
  public void widenUint32ToUint64IsRejected() throws Exception {
    DescriptorProto oldMsg = messageWithTypedFieldAndDeps("count", Type.TYPE_UINT32).build();
    DescriptorProto newMsg = messageWithTypedFieldAndDeps("count", Type.TYPE_UINT64).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE, "count");
  }

  @Test
  public void widenSint32ToSint64IsRejected() throws Exception {
    DescriptorProto oldMsg = messageWithTypedFieldAndDeps("count", Type.TYPE_SINT32).build();
    DescriptorProto newMsg = messageWithTypedFieldAndDeps("count", Type.TYPE_SINT64).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE, "count");
  }

  @Test
  public void everyScalarTypeSameToSameIsAccepted() throws Exception {
    for (Type t : SCALAR_TYPES_FOR_MATRIX) {
      DescriptorProto same = messageWithTypedFieldAndDeps("f", t).build();
      ProtoEvolutionValidator.validate(resolve(same), resolve(same));
    }
  }

  @Test
  public void everyTypeChangeThrowsIncompatibleTypeChange() throws Exception {
    for (Type oldType : SCALAR_TYPES_FOR_MATRIX) {
      for (Type newType : SCALAR_TYPES_FOR_MATRIX) {
        if (oldType == newType) {
          continue;
        }
        DescriptorProto oldMsg = messageWithTypedFieldAndDeps("f", oldType).build();
        DescriptorProto newMsg = messageWithTypedFieldAndDeps("f", newType).build();
        try {
          ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(newMsg));
          fail("Expected PROTOBUF_INCOMPATIBLE_TYPE_CHANGE for " + oldType + " → " + newType);
        } catch (SQLException ex) {
          assertEquals("Wrong error code for " + oldType + " → " + newType + ": " + ex.getMessage(),
            SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE.getErrorCode(), ex.getErrorCode());
        }
      }
    }
  }

  @Test
  public void cardinalityChangeOptionalToRepeatedIsRejected() throws Exception {
    DescriptorProto oldMsg =
      message(MSG_NAME).addField(scalar("tags", 1, Type.TYPE_STRING)).build();
    DescriptorProto newMsg =
      message(MSG_NAME).addField(FieldDescriptorProto.newBuilder().setName("tags").setNumber(1)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_REPEATED).build()).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_INCOMPATIBLE_CARDINALITY_CHANGE,
      "tags");
  }

  @Test
  public void addPackedAnnotationIsRejected() throws Exception {
    DescriptorProto oldMsg =
      message(MSG_NAME).addField(FieldDescriptorProto.newBuilder().setName("nums").setNumber(1)
        .setType(Type.TYPE_INT32).setLabel(Label.LABEL_REPEATED).build()).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(FieldDescriptorProto.newBuilder()
      .setName("nums").setNumber(1).setType(Type.TYPE_INT32).setLabel(Label.LABEL_REPEATED)
      .setOptions(FieldOptions.newBuilder().setPacked(true).build()).build()).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_PACKED_ENCODING_CHANGE, "nums");
  }

  @Test
  public void addFieldIntoNewOneofIsAccepted() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(FieldDescriptorProto.newBuilder().setName("email").setNumber(2)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0).build())
      .addField(FieldDescriptorProto.newBuilder().setName("phone").setNumber(3)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0).build())
      .addOneofDecl(OneofDescriptorProto.newBuilder().setName("contact")).build();
    ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(newMsg));
  }

  @Test
  public void addFieldIntoExistingOneofIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(FieldDescriptorProto.newBuilder().setName("email").setNumber(2)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0).build())
      .addOneofDecl(OneofDescriptorProto.newBuilder().setName("contact")).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(FieldDescriptorProto.newBuilder().setName("email").setNumber(2)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0).build())
      .addField(FieldDescriptorProto.newBuilder().setName("phone").setNumber(3)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0).build())
      .addOneofDecl(OneofDescriptorProto.newBuilder().setName("contact")).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_ONEOF_MEMBERSHIP_CHANGE, "phone");
  }

  @Test
  public void moveFieldOutOfExistingOneofIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(FieldDescriptorProto.newBuilder().setName("email").setNumber(2)
        .setType(Type.TYPE_STRING).setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0).build())
      .addOneofDecl(OneofDescriptorProto.newBuilder().setName("contact")).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(scalar("email", 2, Type.TYPE_STRING))
      .addOneofDecl(OneofDescriptorProto.newBuilder().setName("contact")).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_ONEOF_MEMBERSHIP_CHANGE, "email");
  }

  @Test
  public void addNestedMessageIsAccepted() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addNestedType(message("Inner").addField(scalar("value", 1, Type.TYPE_STRING))).build();
    ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(newMsg));
  }

  @Test
  public void removeNestedMessageIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addNestedType(message("Inner").addField(scalar("value", 1, Type.TYPE_STRING))).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED, "Inner");
  }

  @Test
  public void nestedMessageFieldChangeIsValidatedRecursively() throws Exception {
    DescriptorProto oldInner =
      message("Inner").addField(scalar("original", 1, Type.TYPE_STRING)).build();
    DescriptorProto newInner =
      message("Inner").addField(scalar("renamed", 1, Type.TYPE_STRING)).build();
    DescriptorProto oldMsg =
      message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).addNestedType(oldInner).build();
    DescriptorProto newMsg =
      message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).addNestedType(newInner).build();
    SQLException ex =
      assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_FIELD_RENAMED, "renamed");
    assertTrue("path in error must include nested message name, got: " + ex.getMessage(),
      ex.getMessage().contains("Inner"));
  }

  @Test
  public void packageChangeIsRejected() throws Exception {
    DescriptorProto proto = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    Descriptors.Descriptor oldDesc = resolveInPackage(proto, "pkg.one");
    Descriptors.Descriptor newDesc = resolveInPackage(proto, "pkg.two");
    try {
      ProtoEvolutionValidator.validate(oldDesc, newDesc);
      fail("Expected PROTOBUF_PACKAGE_CHANGED SQLException");
    } catch (SQLException ex) {
      assertEquals(SQLExceptionCode.PROTOBUF_PACKAGE_CHANGED.getErrorCode(), ex.getErrorCode());
    }
  }

  @Test
  public void addEnumValueIsAccepted() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(
        enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("ACTIVE", 1)))
      .build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(enumType("Status").addValue(enumValue("UNKNOWN", 0))
        .addValue(enumValue("ACTIVE", 1)).addValue(enumValue("SUSPENDED", 2)))
      .build();
    ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(newMsg));
  }

  @Test
  public void removeEnumValueIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(
        enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("ACTIVE", 1)))
      .build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(enumType("Status").addValue(enumValue("UNKNOWN", 0))).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED, "ACTIVE");
  }

  @Test
  public void renameEnumValueSameTagIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(
        enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("ACTIVE", 1)))
      .build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(
        enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("LIVE", 1)))
      .build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_ENUM_VALUE_RENAMED, "LIVE");
  }

  @Test
  public void renumberEnumValueSameNameIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(
        enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("ACTIVE", 1)))
      .build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(
        enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("ACTIVE", 2)))
      .build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_ENUM_VALUE_RENUMBERED, "ACTIVE");
  }

  @Test
  public void removeEntireEnumIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(enumType("Status").addValue(enumValue("UNKNOWN", 0))).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED, "Status");
  }

  @Test
  public void preExistingAllowAliasEnumSurvivesUnchanged() throws Exception {
    DescriptorProto msg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(allowAliasEnumType("Status").addValue(enumValue("UNKNOWN", 0))
        .addValue(enumValue("OK", 1)).addValue(enumValue("SUCCESS", 1)))
      .build();
    ProtoEvolutionValidator.validate(resolve(msg), resolve(msg));
  }

  @Test
  public void addingNewValueThatDoesNotAliasIsAcceptedOnAllowAliasEnum() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(allowAliasEnumType("Status").addValue(enumValue("UNKNOWN", 0))
        .addValue(enumValue("OK", 1)).addValue(enumValue("SUCCESS", 1)))
      .build();
    DescriptorProto newMsg =
      message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
        .addEnumType(allowAliasEnumType("Status").addValue(enumValue("UNKNOWN", 0))
          .addValue(enumValue("OK", 1)).addValue(enumValue("SUCCESS", 1))
          .addValue(enumValue("SUSPENDED", 2)))
        .build();
    ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(newMsg));
  }

  @Test
  public void addingNewAliasToExistingTagOnAllowAliasEnumIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(allowAliasEnumType("Status").addValue(enumValue("UNKNOWN", 0))
        .addValue(enumValue("ACTIVE", 1)))
      .build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(allowAliasEnumType("Status").addValue(enumValue("UNKNOWN", 0))
        .addValue(enumValue("ACTIVE", 1)).addValue(enumValue("LIVE", 1)))
      .build();

    SQLException ex =
      assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_ENUM_TAG_REUSE, "LIVE");
    assertTrue("hint should identify the pre-existing colliding name 'ACTIVE': " + ex.getMessage(),
      ex.getMessage().contains("ACTIVE"));
  }

  @Test
  public void addingNewAliasAtBrandNewTagOnAllowAliasEnumIsAccepted() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(allowAliasEnumType("Status").addValue(enumValue("UNKNOWN", 0))
        .addValue(enumValue("ACTIVE", 1)))
      .build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addEnumType(allowAliasEnumType("Status").addValue(enumValue("UNKNOWN", 0))
        .addValue(enumValue("ACTIVE", 1)).addValue(enumValue("SUSPENDED", 2))
        .addValue(enumValue("ON_HOLD", 2)))
      .build();
    ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(newMsg));
  }

  @Test
  public void syntaxChangeProto3ToProto2IsRejected() throws Exception {
    DescriptorProto proto = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    Descriptors.Descriptor oldDesc = resolveInFile(proto, "test.evo.proto", PKG, "proto3");

    Descriptors.Descriptor newDesc = resolveInFile(proto, "test.evo.proto", PKG, null);
    try {
      ProtoEvolutionValidator.validate(oldDesc, newDesc);
      fail("Expected PROTOBUF_SYNTAX_CHANGED SQLException");
    } catch (SQLException ex) {
      assertEquals(SQLExceptionCode.PROTOBUF_SYNTAX_CHANGED.getErrorCode(), ex.getErrorCode());
    }
  }

  @Test
  public void addFieldOnReservedTagIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(5).setEnd(6).build())
      .build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(scalar("sneaky", 5, Type.TYPE_STRING))
      .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(5).setEnd(6).build())
      .build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_TAG_RESERVED, "sneaky");
  }

  @Test
  public void addFieldOnReservedNameIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addReservedName("retired_name").build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(scalar("retired_name", 2, Type.TYPE_STRING)).addReservedName("retired_name")
      .build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_NAME_RESERVED, "retired_name");
  }

  @Test
  public void reserveLiveTagIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(scalar("count", 2, Type.TYPE_INT32)).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(scalar("count", 2, Type.TYPE_INT32))
      .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(2).setEnd(3).build())
      .build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_RESERVE_OF_LIVE_TAG, "count");
  }

  @Test
  public void reserveLiveNameIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    DescriptorProto newMsg =
      message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).addReservedName("id").build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_RESERVE_OF_LIVE_NAME, "id");
  }

  @Test
  public void addNewReservedTagAndNameIsAccepted() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING)).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(50).setEnd(60).build())
      .addReservedName("some_future_name").build();
    ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(newMsg));
  }

  @Test
  public void changeMessageFieldTargetTypeIsRejected() throws Exception {
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto.newBuilder().setName("inner").setNumber(2).setType(Type.TYPE_MESSAGE)
          .setTypeName("." + FQ_NAME + ".Inner1").setLabel(Label.LABEL_OPTIONAL).build())
      .addNestedType(message("Inner1")).addNestedType(message("Inner2")).build();
    DescriptorProto newMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto.newBuilder().setName("inner").setNumber(2).setType(Type.TYPE_MESSAGE)
          .setTypeName("." + FQ_NAME + ".Inner2").setLabel(Label.LABEL_OPTIONAL).build())
      .addNestedType(message("Inner1")).addNestedType(message("Inner2")).build();
    assertRejected(oldMsg, newMsg, SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE, "inner");
  }

  @Test
  public void renumberInReferencedTopLevelEnumIsRejected() throws Exception {
    DescriptorProto holder = message("Holder").build();
    DescriptorProto widget = message(MSG_NAME)
      .addField(typed("status", 1, Type.TYPE_ENUM, "." + COMMON_PKG + ".Status")).build();
    EnumDescriptorProto oldStatus =
      enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("ACTIVE", 1)).build();
    EnumDescriptorProto newStatus =
      enumType("Status").addValue(enumValue("UNKNOWN", 0)).addValue(enumValue("ACTIVE", 2)).build();

    assertRejectedDescriptors(resolveWithImport(holder, oldStatus, widget),
      resolveWithImport(holder, newStatus, widget), SQLExceptionCode.PROTOBUF_ENUM_VALUE_RENUMBERED,
      "ACTIVE");
  }

  @Test
  public void recursiveMessageIsWalkedOnce() throws Exception {
    String self = "." + FQ_NAME;
    DescriptorProto oldMsg = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_STRING))
      .addField(typed("parent", 2, Type.TYPE_MESSAGE, self)).build();
    DescriptorProto added =
      oldMsg.toBuilder().addField(scalar("name", 3, Type.TYPE_STRING)).build();
    ProtoEvolutionValidator.validate(resolve(oldMsg), resolve(added));

    DescriptorProto retyped = message(MSG_NAME).addField(scalar("id", 1, Type.TYPE_BYTES))
      .addField(typed("parent", 2, Type.TYPE_MESSAGE, self)).build();
    assertRejected(oldMsg, retyped, SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE, "id");
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullOldDescriptorRejected() throws Exception {
    ProtoEvolutionValidator.validate(null, resolve(message(MSG_NAME).build()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullNewDescriptorRejected() throws Exception {
    ProtoEvolutionValidator.validate(resolve(message(MSG_NAME).build()), null);
  }

  @Test
  public void realBundleDescriptorIsCompatibleWithItself() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();

    String fqName = "com.example.acmewidgets.orders.v1.Order";
    Descriptors.Descriptor resolved = resolver.resolve(bundle, fqName);
    ProtoEvolutionValidator.validate(resolved, resolved);

    String modifierFqName = "com.example.acmewidgets.orders.v1.LineItem.Modifier";
    Descriptors.Descriptor modifier = resolver.resolve(bundle, modifierFqName);
    ProtoEvolutionValidator.validate(modifier, modifier);
  }

  private SQLException assertRejected(DescriptorProto oldMsg, DescriptorProto newMsg,
    SQLExceptionCode expectedCode, String expectedInMessage) throws Exception {
    return assertRejectedDescriptors(resolve(oldMsg), resolve(newMsg), expectedCode,
      expectedInMessage);
  }

  private static SQLException assertRejectedDescriptors(Descriptors.Descriptor oldDesc,
    Descriptors.Descriptor newDesc, SQLExceptionCode expectedCode, String expectedInMessage)
    throws Exception {
    try {
      ProtoEvolutionValidator.validate(oldDesc, newDesc);
      fail("Expected SQLException with code " + expectedCode.name());
      return null;
    } catch (SQLException ex) {
      assertEquals("Expected error code " + expectedCode.name() + " but got " + ex.getMessage(),
        expectedCode.getErrorCode(), ex.getErrorCode());
      assertTrue("Message must reference the offending element '" + expectedInMessage + "', got: "
        + ex.getMessage(), ex.getMessage().contains(expectedInMessage));
      return ex;
    }
  }

  private Descriptors.Descriptor resolve(DescriptorProto proto) throws Exception {
    return resolveInPackage(proto, PKG);
  }

  private Descriptors.Descriptor resolveInPackage(DescriptorProto proto, String pkg)
    throws Exception {
    return resolveInFile(proto, "test.proto", pkg, "proto3");
  }

  private Descriptors.Descriptor resolveInFile(DescriptorProto proto, String fileName, String pkg,
    String syntaxOrNull) throws Exception {
    FileDescriptorProto.Builder file =
      FileDescriptorProto.newBuilder().setName(fileName).setPackage(pkg).addMessageType(proto);
    if (syntaxOrNull != null) {
      file.setSyntax(syntaxOrNull);
    }
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(file).build();
    String fqName = pkg + "." + proto.getName();
    return resolver.resolve(set.toByteArray(), fqName);
  }

  private Descriptors.Descriptor resolveWithImport(DescriptorProto commonMsg,
    EnumDescriptorProto commonEnumOrNull, DescriptorProto main) throws Exception {
    FileDescriptorProto.Builder common = FileDescriptorProto.newBuilder().setName("common.proto")
      .setPackage(COMMON_PKG).setSyntax("proto3").addMessageType(commonMsg);
    if (commonEnumOrNull != null) {
      common.addEnumType(commonEnumOrNull);
    }
    FileDescriptorProto file =
      FileDescriptorProto.newBuilder().setName("test.proto").setPackage(PKG).setSyntax("proto3")
        .addDependency("common.proto").addMessageType(main).build();
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(common).addFile(file).build();
    return resolver.resolve(set.toByteArray(), PKG + "." + main.getName());
  }

  private static FieldDescriptorProto typed(String name, int number, Type type, String typeName) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type)
      .setTypeName(typeName).setLabel(Label.LABEL_OPTIONAL).build();
  }

  private static DescriptorProto.Builder message(String name) {
    return DescriptorProto.newBuilder().setName(name);
  }

  private static FieldDescriptorProto scalar(String name, int number, Type type) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type)
      .setLabel(Label.LABEL_OPTIONAL).build();
  }

  private static EnumDescriptorProto.Builder enumType(String name) {
    return EnumDescriptorProto.newBuilder().setName(name);
  }

  private static EnumDescriptorProto.Builder allowAliasEnumType(String name) {
    return EnumDescriptorProto.newBuilder().setName(name)
      .setOptions(EnumOptions.newBuilder().setAllowAlias(true).build());
  }

  private static EnumValueDescriptorProto enumValue(String name, int number) {
    return EnumValueDescriptorProto.newBuilder().setName(name).setNumber(number).build();
  }

  private static DescriptorProto.Builder messageWithTypedFieldAndDeps(String fieldName, Type type) {
    DescriptorProto.Builder builder = DescriptorProto.newBuilder().setName(MSG_NAME)
      .addNestedType(DescriptorProto.newBuilder().setName("NestedMsg")).addEnumType(
        EnumDescriptorProto.newBuilder().setName("NestedEnum").addValue(enumValue("VAL_0", 0)));
    FieldDescriptorProto.Builder fieldBuilder = FieldDescriptorProto.newBuilder().setName(fieldName)
      .setNumber(1).setType(type).setLabel(Label.LABEL_OPTIONAL);
    if (type == Type.TYPE_MESSAGE || type == Type.TYPE_GROUP) {
      fieldBuilder.setTypeName("." + FQ_NAME + ".NestedMsg");
    } else if (type == Type.TYPE_ENUM) {
      fieldBuilder.setTypeName("." + FQ_NAME + ".NestedEnum");
    }
    return builder.addField(fieldBuilder.build());
  }

  private static byte[] loadAcmeWidgetsBundle() throws IOException {
    ClassLoader cl = ProtoEvolutionValidatorTest.class.getClassLoader();
    try (InputStream in = cl.getResourceAsStream("protobuf/acmewidgets/acmewidgets_v1.desc")) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      return baos.toByteArray();
    }
  }
}
