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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldOptions;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

/**
 * Validates that a candidate PROTOBUF descriptor is a wire-compatible evolution of the
 * currently-registered one. Permitted: add unused fields/messages/enums/enum values. Everything
 * else, including any change to a field's type, is rejected with a specific
 * {@link SQLExceptionCode}. Walks nested types and every message or enum a field references, in the
 * same file or an imported one.
 */
public final class ProtoEvolutionValidator {

  private ProtoEvolutionValidator() {
    // Utility class
  }

  /** Throws on the first evolution violation; returns if {@code newDesc} is compatible. */
  public static void validate(Descriptors.Descriptor oldDesc, Descriptors.Descriptor newDesc)
    throws SQLException {
    if (oldDesc == null || newDesc == null) {
      throw new IllegalArgumentException(
        "Both oldDesc and newDesc must be non-null (old=" + oldDesc + ", new=" + newDesc + ")");
    }
    checkPackageUnchanged(oldDesc, newDesc);
    checkSyntaxUnchanged(oldDesc, newDesc);
    checkMessage(oldDesc, newDesc, oldDesc.getFullName(), new HashSet<>());
  }

  private static void checkPackageUnchanged(Descriptors.Descriptor oldDesc,
    Descriptors.Descriptor newDesc) throws SQLException {
    String oldPkg = oldDesc.getFile().getPackage();
    String newPkg = newDesc.getFile().getPackage();
    if (!Objects.equals(oldPkg, newPkg)) {
      throw sqlException(SQLExceptionCode.PROTOBUF_PACKAGE_CHANGED, oldDesc.getFullName(),
        "package '" + oldPkg + "' → '" + newPkg + "'");
    }
  }

  private static void checkSyntaxUnchanged(Descriptors.Descriptor oldDesc,
    Descriptors.Descriptor newDesc) throws SQLException {
    String oldSyntax = fileSyntaxDisplay(oldDesc.getFile());
    String newSyntax = fileSyntaxDisplay(newDesc.getFile());
    if (!oldSyntax.equals(newSyntax)) {
      throw sqlException(SQLExceptionCode.PROTOBUF_SYNTAX_CHANGED, oldDesc.getFullName(),
        "syntax '" + oldSyntax + "' → '" + newSyntax + "'");
    }
  }

  /** proto2 files omit {@code syntax}; treat unset as {@code "proto2"}. */
  private static String fileSyntaxDisplay(Descriptors.FileDescriptor file) {
    return file.toProto().hasSyntax() ? file.toProto().getSyntax() : "proto2";
  }

  private static void checkMessage(Descriptors.Descriptor oldMsg, Descriptors.Descriptor newMsg,
    String path, Set<String> visited) throws SQLException {
    if (!visited.add(oldMsg.getFullName())) {
      return;
    }
    checkFields(oldMsg, newMsg, path, visited);
    checkReservedRanges(oldMsg, newMsg, path);
    checkReservedNames(oldMsg, newMsg, path);
    checkEnums(oldMsg, newMsg, path, visited);
    checkNestedMessages(oldMsg, newMsg, path, visited);
  }

  private static void checkFields(Descriptors.Descriptor oldMsg, Descriptors.Descriptor newMsg,
    String path, Set<String> visited) throws SQLException {
    Map<Integer, Descriptors.FieldDescriptor> oldByTag = new HashMap<>();
    Map<String, Descriptors.FieldDescriptor> oldByName = new HashMap<>();
    for (Descriptors.FieldDescriptor f : oldMsg.getFields()) {
      oldByTag.put(f.getNumber(), f);
      oldByName.put(f.getName(), f);
    }

    Map<Integer, Descriptors.FieldDescriptor> newByTag = new HashMap<>();
    Map<String, Descriptors.FieldDescriptor> newByName = new HashMap<>();
    for (Descriptors.FieldDescriptor f : newMsg.getFields()) {
      newByTag.put(f.getNumber(), f);
      newByName.put(f.getName(), f);
    }
    for (Descriptors.FieldDescriptor oldField : oldMsg.getFields()) {
      Descriptors.FieldDescriptor newFieldSameTag = newByTag.get(oldField.getNumber());
      if (newFieldSameTag == null) {
        // Same name at a different tag is a renumber, not a removal.
        Descriptors.FieldDescriptor sameName = newByName.get(oldField.getName());
        if (sameName != null && sameName.getNumber() != oldField.getNumber()) {
          throw sqlException(SQLExceptionCode.PROTOBUF_TAG_RENAMED, path, "field '"
            + oldField.getName() + "' tag " + oldField.getNumber() + " → " + sameName.getNumber());
        }
        throw sqlException(SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED, path,
          "field '" + oldField.getName() + "' (tag " + oldField.getNumber()
            + ") is missing from the new descriptor");
      }
      checkFieldSameTag(oldField, newFieldSameTag, path);
      checkReferencedType(oldField, newFieldSameTag, visited);
    }
    for (Descriptors.FieldDescriptor newField : newMsg.getFields()) {
      if (oldByTag.containsKey(newField.getNumber())) {
        continue;
      }
      DescriptorProto oldProto = oldMsg.toProto();
      for (DescriptorProto.ReservedRange range : oldProto.getReservedRangeList()) {
        // ReservedRange is [start, end).
        if (newField.getNumber() >= range.getStart() && newField.getNumber() < range.getEnd()) {
          throw sqlException(SQLExceptionCode.PROTOBUF_TAG_RESERVED, path,
            "field '" + newField.getName() + "' tag " + newField.getNumber()
              + " falls in a reserved range [" + range.getStart() + ", " + range.getEnd() + ")");
        }
      }
      for (String reservedName : oldProto.getReservedNameList()) {
        if (reservedName.equals(newField.getName())) {
          throw sqlException(SQLExceptionCode.PROTOBUF_NAME_RESERVED, path,
            "field name '" + newField.getName() + "' is reserved by the current descriptor");
        }
      }
      // New field into an existing oneof is forbidden; a brand-new oneof is fine.
      if (newField.getContainingOneof() != null) {
        String oneofName = newField.getContainingOneof().getName();
        boolean oneofExistedInOld = false;
        for (Descriptors.OneofDescriptor oldOneof : oldMsg.getOneofs()) {
          if (oldOneof.getName().equals(oneofName)) {
            oneofExistedInOld = true;
            break;
          }
        }
        if (oneofExistedInOld) {
          throw sqlException(SQLExceptionCode.PROTOBUF_ONEOF_MEMBERSHIP_CHANGE, path,
            "new field '" + newField.getName() + "' added into existing oneof '" + oneofName + "'");
        }
      }
    }
  }

  /** Same-tag field must keep name, type, cardinality, oneof, packed annotation, and default. */
  private static void checkFieldSameTag(Descriptors.FieldDescriptor oldField,
    Descriptors.FieldDescriptor newField, String path) throws SQLException {
    if (!oldField.getName().equals(newField.getName())) {
      throw sqlException(SQLExceptionCode.PROTOBUF_FIELD_RENAMED, path,
        "field at tag " + oldField.getNumber() + " renamed '" + oldField.getName() + "' → '"
          + newField.getName() + "'");
    }
    if (oldField.toProto().getLabel() != newField.toProto().getLabel()) {
      throw sqlException(SQLExceptionCode.PROTOBUF_INCOMPATIBLE_CARDINALITY_CHANGE, path,
        "field '" + oldField.getName() + "' label " + oldField.toProto().getLabel().name() + " → "
          + newField.toProto().getLabel().name());
    }
    String oldOneof =
      oldField.getContainingOneof() == null ? null : oldField.getContainingOneof().getName();
    String newOneof =
      newField.getContainingOneof() == null ? null : newField.getContainingOneof().getName();
    if (!Objects.equals(oldOneof, newOneof)) {
      throw sqlException(SQLExceptionCode.PROTOBUF_ONEOF_MEMBERSHIP_CHANGE, path,
        "field '" + oldField.getName() + "' oneof "
          + (oldOneof == null ? "(none)" : "'" + oldOneof + "'") + " → "
          + (newOneof == null ? "(none)" : "'" + newOneof + "'"));
    }
    if (oldField.toProto().getType() != newField.toProto().getType()) {
      throw sqlException(SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE, path,
        "field '" + oldField.getName() + "' type " + oldField.toProto().getType().name() + " → "
          + newField.toProto().getType().name());
    }
    if (
      oldField.toProto().hasTypeName()
        && !oldField.toProto().getTypeName().equals(newField.toProto().getTypeName())
    ) {
      throw sqlException(SQLExceptionCode.PROTOBUF_INCOMPATIBLE_TYPE_CHANGE, path,
        "field '" + oldField.getName() + "' target type " + oldField.toProto().getTypeName() + " → "
          + newField.toProto().getTypeName());
    }
    if (
      isPackedExplicit(oldField.toProto().getOptions())
          != isPackedExplicit(newField.toProto().getOptions())
    ) {
      throw sqlException(SQLExceptionCode.PROTOBUF_PACKED_ENCODING_CHANGE, path,
        "field '" + oldField.getName() + "' [packed] annotation "
          + (isPackedExplicit(oldField.toProto().getOptions()) ? "removed" : "added"));
    }
    if (
      !oldField.isRepeated()
        && oldField.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE
    ) {
      Object oldDefault = effectiveDefault(oldField);
      Object newDefault = effectiveDefault(newField);
      if (!oldDefault.equals(newDefault)) {
        throw sqlException(SQLExceptionCode.PROTOBUF_DEFAULT_VALUE_CHANGE, path,
          "field '" + oldField.getName() + "' default " + oldDefault + " → " + newDefault);
      }
    }
  }

  private static Object effectiveDefault(Descriptors.FieldDescriptor field) {
    Object value = field.getDefaultValue();
    return value instanceof Descriptors.EnumValueDescriptor
      ? ((Descriptors.EnumValueDescriptor) value).getNumber()
      : value;
  }

  /** Old bytes stay readable only if the referenced type evolved compatibly too. */
  private static void checkReferencedType(Descriptors.FieldDescriptor oldField,
    Descriptors.FieldDescriptor newField, Set<String> visited) throws SQLException {
    if (oldField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
      Descriptors.Descriptor oldType = oldField.getMessageType();
      checkMessage(oldType, newField.getMessageType(), oldType.getFullName(), visited);
    } else if (oldField.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
      Descriptors.EnumDescriptor oldType = oldField.getEnumType();
      if (visited.add(oldType.getFullName())) {
        checkEnum(oldType, newField.getEnumType(), oldType.getFullName());
      }
    }
  }

  private static boolean isPackedExplicit(FieldOptions options) {
    return options.hasPacked() && options.getPacked();
  }

  private static void checkReservedRanges(Descriptors.Descriptor oldMsg,
    Descriptors.Descriptor newMsg, String path) throws SQLException {
    DescriptorProto oldProto = oldMsg.toProto();
    DescriptorProto newProto = newMsg.toProto();
    Set<String> oldRangeKeys = new HashSet<>();
    for (DescriptorProto.ReservedRange oldRange : oldProto.getReservedRangeList()) {
      oldRangeKeys.add(oldRange.getStart() + "-" + oldRange.getEnd());
    }
    for (DescriptorProto.ReservedRange newRange : newProto.getReservedRangeList()) {
      if (oldRangeKeys.contains(newRange.getStart() + "-" + newRange.getEnd())) {
        continue;
      }
      for (Descriptors.FieldDescriptor oldField : oldMsg.getFields()) {
        int tag = oldField.getNumber();
        if (tag >= newRange.getStart() && tag < newRange.getEnd()) {
          throw sqlException(SQLExceptionCode.PROTOBUF_RESERVE_OF_LIVE_TAG, path,
            "new reserved range [" + newRange.getStart() + ", " + newRange.getEnd()
              + ") covers live field '" + oldField.getName() + "' at tag " + tag);
        }
      }
    }
  }

  private static void checkReservedNames(Descriptors.Descriptor oldMsg,
    Descriptors.Descriptor newMsg, String path) throws SQLException {
    Set<String> oldReservedNames = new HashSet<>(oldMsg.toProto().getReservedNameList());
    for (String newReservedName : newMsg.toProto().getReservedNameList()) {
      if (oldReservedNames.contains(newReservedName)) {
        continue; // already reserved in old — no change
      }
      for (Descriptors.FieldDescriptor oldField : oldMsg.getFields()) {
        if (oldField.getName().equals(newReservedName)) {
          throw sqlException(SQLExceptionCode.PROTOBUF_RESERVE_OF_LIVE_NAME, path,
            "new reserved name '" + newReservedName + "' collides with live field of the "
              + "same name");
        }
      }
    }
  }

  private static void checkEnums(Descriptors.Descriptor oldMsg, Descriptors.Descriptor newMsg,
    String path, Set<String> visited) throws SQLException {
    Map<String, Descriptors.EnumDescriptor> oldEnumsByName = new HashMap<>();
    for (Descriptors.EnumDescriptor oldEnum : oldMsg.getEnumTypes()) {
      oldEnumsByName.put(oldEnum.getName(), oldEnum);
    }
    Map<String, Descriptors.EnumDescriptor> newEnumsByName = new HashMap<>();
    for (Descriptors.EnumDescriptor newEnum : newMsg.getEnumTypes()) {
      newEnumsByName.put(newEnum.getName(), newEnum);
    }
    for (Map.Entry<String, Descriptors.EnumDescriptor> entry : oldEnumsByName.entrySet()) {
      Descriptors.EnumDescriptor newEnum = newEnumsByName.get(entry.getKey());
      if (newEnum == null) {
        throw sqlException(SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED, path,
          "enum '" + entry.getKey() + "' is missing from the new descriptor");
      }
      if (visited.add(entry.getValue().getFullName())) {
        checkEnum(entry.getValue(), newEnum, path + "." + entry.getKey());
      }
    }
  }

  private static void checkEnum(Descriptors.EnumDescriptor oldEnum,
    Descriptors.EnumDescriptor newEnum, String path) throws SQLException {
    // Index by name; tag maps are unsafe under allow_alias.
    Map<String, EnumValueDescriptorProto> oldByName = new HashMap<>();
    Map<String, EnumValueDescriptorProto> newByName = new HashMap<>();
    for (EnumValueDescriptorProto v : oldEnum.toProto().getValueList()) {
      oldByName.put(v.getName(), v);
    }
    for (EnumValueDescriptorProto v : newEnum.toProto().getValueList()) {
      newByName.put(v.getName(), v);
    }
    for (EnumValueDescriptorProto oldValue : oldEnum.toProto().getValueList()) {
      EnumValueDescriptorProto sameName = newByName.get(oldValue.getName());
      if (sameName != null) {
        if (sameName.getNumber() != oldValue.getNumber()) {
          throw sqlException(SQLExceptionCode.PROTOBUF_ENUM_VALUE_RENUMBERED, path, "enum value '"
            + oldValue.getName() + "' tag " + oldValue.getNumber() + " → " + sameName.getNumber());
        }
        continue; // pre-existing value, unchanged
      }
      EnumValueDescriptorProto renameTarget = null;
      for (EnumValueDescriptorProto candidate : newEnum.toProto().getValueList()) {
        if (
          candidate.getNumber() == oldValue.getNumber()
            && !oldByName.containsKey(candidate.getName())
        ) {
          renameTarget = candidate;
          break;
        }
      }
      if (renameTarget != null) {
        throw sqlException(SQLExceptionCode.PROTOBUF_ENUM_VALUE_RENAMED, path,
          "enum value at tag " + oldValue.getNumber() + " renamed '" + oldValue.getName() + "' → '"
            + renameTarget.getName() + "'");
      }
      throw sqlException(SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED, path, "enum value '"
        + oldValue.getName() + "' (tag " + oldValue.getNumber() + ") is missing from the new enum");
    }
    for (EnumValueDescriptorProto newValue : newEnum.toProto().getValueList()) {
      if (oldByName.containsKey(newValue.getName())) {
        continue; // pre-existing name — already validated in pass 1
      }
      for (EnumValueDescriptorProto oldValue : oldEnum.toProto().getValueList()) {
        if (oldValue.getNumber() != newValue.getNumber()) {
          continue;
        }
        if (oldValue.getName().equals(newValue.getName())) {
          continue; // shouldn't happen (newValue is new-only) but defensive
        }
        if (newByName.containsKey(oldValue.getName())) {
          throw sqlException(SQLExceptionCode.PROTOBUF_ENUM_TAG_REUSE, path,
            "new enum value '" + newValue.getName() + "' uses tag " + newValue.getNumber()
              + " already assigned to '" + oldValue.getName() + "' in the old enum "
              + "(introducing a new alias during ALTER is not permitted; the old "
              + "enum's allow_alias only preserves pre-existing aliases)");
        }
      }
    }
  }

  private static void checkNestedMessages(Descriptors.Descriptor oldMsg,
    Descriptors.Descriptor newMsg, String path, Set<String> visited) throws SQLException {
    Map<String, Descriptors.Descriptor> newNestedByName = new HashMap<>();
    for (Descriptors.Descriptor n : newMsg.getNestedTypes()) {
      newNestedByName.put(n.getName(), n);
    }
    for (Descriptors.Descriptor oldNested : oldMsg.getNestedTypes()) {
      Descriptors.Descriptor newNested = newNestedByName.get(oldNested.getName());
      if (newNested == null) {
        throw sqlException(SQLExceptionCode.PROTOBUF_REMOVAL_NOT_SUPPORTED, path,
          "nested message '" + oldNested.getName() + "' is missing from the new descriptor");
      }
      checkMessage(oldNested, newNested, path + "." + oldNested.getName(), visited);
    }
  }

  private static SQLException sqlException(SQLExceptionCode code, String path, String detail) {
    return new SQLExceptionInfo.Builder(code).setMessage("[" + path + "] " + detail).build()
      .buildException();
  }
}
