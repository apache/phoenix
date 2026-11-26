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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.util.proto.PathPlan.PlanSegment;
import org.apache.phoenix.expression.util.proto.ProtoPathParser.ParsedSegment;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

import org.apache.phoenix.thirdparty.com.google.protobuf.ByteString;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.Descriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.EnumValueDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.WireFormat;

/**
 * Resolves a {@link ProtoPathParser}-parsed path against a protobuf {@link Descriptor} to produce a
 * compiled {@link PathPlan}.
 * <p>
 * Segment classification:
 * <ul>
 * <li>Dot-notation on a scalar/message field → SCALAR / MESSAGE.</li>
 * <li>Bracket-notation on a {@code repeated T} field → REPEATED_INDEX.</li>
 * <li>Dot-notation on {@code map<string, V>} + next-segment identifier as key → MAP_KEY_STRING
 * (consumes two parsed segments to produce one plan segment).</li>
 * <li>Bracket-notation on {@code map<intT, V>} with regular-varint key type → MAP_KEY_INT.</li>
 * </ul>
 */
public final class ProtoPathResolver {

  private ProtoPathResolver() {
  }

  public static PathPlan resolve(List<ParsedSegment> parsedSegments, Descriptor root,
    PDataType<?> requestedType, String originalPath) throws SQLException {
    return resolveInternal(parsedSegments, root, requestedType, originalPath, false);
  }

  public static PathPlan resolveForExistence(List<ProtoPathParser.ParsedSegment> parsedSegments,
    Descriptor root, String originalPath) throws SQLException {
    return resolveInternal(parsedSegments, root, null, originalPath, false);
  }

  public static PathPlan resolveForArrayLength(List<ProtoPathParser.ParsedSegment> parsedSegments,
    Descriptor root, String originalPath) throws SQLException {
    return resolveInternal(parsedSegments, root, null, originalPath, true);
  }

  public static String canonicalSqlTypeName(FieldDescriptor.Type protoType) {
    switch (protoType) {
      case INT32:
      case SINT32:
      case SFIXED32:
      case ENUM:
        return PInteger.INSTANCE.getSqlTypeName();
      case INT64:
      case SINT64:
      case SFIXED64:
        return PLong.INSTANCE.getSqlTypeName();
      // Phoenix UNSIGNED_INT and UNSIGNED_LONG only cover the non-negative signed range, so they
      // cannot hold the full unsigned range. Use BIGINT for 32-bit and DECIMAL for 64-bit.
      case UINT32:
      case FIXED32:
        return PLong.INSTANCE.getSqlTypeName();
      case UINT64:
      case FIXED64:
        return PDecimal.INSTANCE.getSqlTypeName();
      case BOOL:
        return PBoolean.INSTANCE.getSqlTypeName();
      case FLOAT:
        return PFloat.INSTANCE.getSqlTypeName();
      case DOUBLE:
        return PDouble.INSTANCE.getSqlTypeName();
      case STRING:
        return PVarchar.INSTANCE.getSqlTypeName();
      case BYTES:
        return PVarbinary.INSTANCE.getSqlTypeName();
      case MESSAGE:
        return PVarbinary.INSTANCE.getSqlTypeName();
      case GROUP:
      default:
        throw new IllegalStateException("proto type " + protoType + " not supported");
    }
  }

  private static PathPlan resolveInternal(List<ParsedSegment> parsedSegments, Descriptor root,
    PDataType<?> requestedType, String originalPath, boolean arrayLengthMode) throws SQLException {
    if (parsedSegments == null || parsedSegments.isEmpty()) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_PATH_SYNTAX_ERROR)
        .setMessage("PROTO_VALUE path must contain at least one segment").build().buildException();
    }
    if (root == null) {
      throw new IllegalArgumentException("root descriptor must not be null");
    }

    int totalSegments = parsedSegments.size();
    List<PlanSegment> plan = new ArrayList<>(totalSegments);
    Descriptor currentDescriptor = root;
    FieldDescriptor leafField = null;
    int i = 0;

    while (i < totalSegments) {
      ParsedSegment parsed = parsedSegments.get(i);
      boolean isLastParsedSegment = i == totalSegments - 1;
      if (currentDescriptor == null) {
        throw cardinality(
          "cannot navigate past a scalar or bytes leaf at segment '" + parsed.identifier + "'",
          originalPath);
      }
      FieldDescriptor field = currentDescriptor.findFieldByName(parsed.identifier);
      if (field == null) {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_PATH_FIELD_NOT_FOUND)
          .setMessage("PROTO_VALUE path '" + originalPath + "': field '" + parsed.identifier
            + "' not found in message '" + currentDescriptor.getFullName() + "'")
          .build().buildException();
      }
      if (field.getType() == FieldDescriptor.Type.GROUP) {
        throw unsupported("proto2 groups are not supported (segment '" + parsed.identifier
          + "'); this is a permanent design decision", originalPath);
      }
      if (
        arrayLengthMode && isLastParsedSegment && parsed.intSubscript == null
          && (field.isRepeated() || field.isMapField())
      ) {
        plan.add(buildCountArraySegment(field));
        leafField = field;
        i++;
        continue;
      }
      if (field.isMapField()) {
        FieldDescriptor keyField = field.getMessageType().findFieldByNumber(1);
        FieldDescriptor valueField = field.getMessageType().findFieldByNumber(2);
        if (parsed.intSubscript != null) {
          FieldDescriptor.Type keyType = keyField.getType();
          if (!isSupportedIntMapKey(keyType)) {
            throw unsupported("map<" + keyType.name().toLowerCase() + ", V> keys are "
              + "not supported (segment '" + parsed.identifier + "'); use int32, "
              + "int64, uint32, or uint64 map keys instead", originalPath);
          }
          plan.add(PlanSegment.mapKeyInt(field.getNumber(), parsed.intSubscript,
            wireTypeFor(valueField.getType())));
          currentDescriptor = valueField.getType() == FieldDescriptor.Type.MESSAGE
            ? valueField.getMessageType()
            : null;
          leafField = valueField;
          i++;
        } else {
          if (keyField.getType() != FieldDescriptor.Type.STRING) {
            throw cardinality("map<" + keyField.getType().name().toLowerCase()
              + ", V> access requires bracket-index '[N]', not dot-notation " + "(segment '"
              + parsed.identifier + "')", originalPath);
          }
          if (i + 1 >= parsedSegments.size()) {
            throw cardinality("map field '" + parsed.identifier + "' requires a key "
              + "segment (e.g. '" + parsed.identifier + ".key.value')", originalPath);
          }
          ParsedSegment keySegment = parsedSegments.get(i + 1);
          if (keySegment.intSubscript != null) {
            throw cardinality(
              "map<string, V> key segment '" + keySegment.identifier
                + "' cannot carry a bracket-index; use the key as an identifier only",
              originalPath);
          }
          plan.add(PlanSegment.mapKeyString(field.getNumber(), keySegment.identifier,
            wireTypeFor(valueField.getType())));
          currentDescriptor = valueField.getType() == FieldDescriptor.Type.MESSAGE
            ? valueField.getMessageType()
            : null;
          leafField = valueField;
          i += 2;
        }
      } else if (field.isRepeated()) {
        if (parsed.intSubscript == null) {
          throw cardinality(
            "repeated field '" + parsed.identifier + "' requires a bracket-index '[N]'",
            originalPath);
        }
        long idx = parsed.intSubscript;
        if (idx < 0 || idx > Integer.MAX_VALUE) {
          throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_PATH_SYNTAX_ERROR)
            .setMessage(
              "PROTO_VALUE path '" + originalPath + "': repeated index out of range: " + idx)
            .build().buildException();
        }
        plan.add(
          PlanSegment.repeatedIndex(field.getNumber(), wireTypeFor(field.getType()), (int) idx));
        currentDescriptor =
          field.getType() == FieldDescriptor.Type.MESSAGE ? field.getMessageType() : null;
        leafField = field;
        i++;
      } else {
        if (parsed.intSubscript != null) {
          throw cardinality(
            "cannot apply bracket-index to non-repeated non-map field '" + parsed.identifier + "'",
            originalPath);
        }
        if (field.getType() == FieldDescriptor.Type.MESSAGE) {
          plan.add(PlanSegment.message(field.getNumber()));
          currentDescriptor = field.getMessageType();
        } else {
          plan.add(PlanSegment.scalar(field.getNumber(), wireTypeFor(field.getType())));
          currentDescriptor = null;
        }
        leafField = field;
        i++;
      }
    }

    if (leafField == null) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_PATH_SYNTAX_ERROR)
        .setMessage("PROTO_VALUE path '" + originalPath + "' resolved no leaf field").build()
        .buildException();
    }

    if (arrayLengthMode && plan.get(plan.size() - 1).kind != PathPlan.SegmentKind.COUNT_ARRAY) {
      throw cardinality("PROTO_ARRAY_LENGTH path '" + originalPath + "' must end at a "
        + "repeated field or a map (received a scalar or specific element)", originalPath);
    }
    if (requestedType != null) {
      validateLeafCoercion(leafField, requestedType, originalPath);
    }

    return new PathPlan(plan, leafField.getType(), requestedType, leafDefault(leafField));
  }

  /**
   * Returns the value of {@code field} when it is unset but its parent is present, in the
   * {@link ProtoPathWalker} leaf conventions. Null for message and repeated fields and for an arm
   * of a real oneof, which read as SQL NULL when unset.
   */
  static Object leafDefault(FieldDescriptor field) {
    if (
      field.isRepeated() || field.getType() == FieldDescriptor.Type.MESSAGE
        || field.getRealContainingOneof() != null
    ) {
      return null;
    }
    Object value = field.getDefaultValue();
    switch (field.getType()) {
      case UINT32:
      case FIXED32:
        return ((Integer) value) & 0xFFFFFFFFL;
      case ENUM:
        return ((EnumValueDescriptor) value).getNumber();
      case BYTES:
        return ((ByteString) value).toByteArray();
      default:
        return value;
    }
  }

  private static PlanSegment buildCountArraySegment(FieldDescriptor field) {
    boolean scalarRepeated =
      field.isRepeated() && !field.isMapField() && field.getType() != FieldDescriptor.Type.MESSAGE
        && field.getType() != FieldDescriptor.Type.STRING
        && field.getType() != FieldDescriptor.Type.BYTES;
    int elementWireType = scalarRepeated ? wireTypeFor(field.getType()) : 0;
    return PlanSegment.countArray(field.getNumber(), scalarRepeated, elementWireType);
  }

  private static boolean isSupportedIntMapKey(FieldDescriptor.Type keyType) {
    return keyType == FieldDescriptor.Type.INT32 || keyType == FieldDescriptor.Type.INT64
      || keyType == FieldDescriptor.Type.UINT32 || keyType == FieldDescriptor.Type.UINT64;
  }

  /**
   * Maps a protobuf {@link FieldDescriptor.Type} to its on-wire type per the protobuf encoding spec
   * (https://protobuf.dev/programming-guides/encoding/).
   * @throws IllegalStateException for GROUP.
   */
  static int wireTypeFor(FieldDescriptor.Type type) {
    if (type == FieldDescriptor.Type.GROUP) {
      throw new IllegalStateException("wireTypeFor called with unsupported protobuf type " + type
        + "; upstream GROUP rejection and leaf-type validation should have " + "prevented this");
    }
    // Descriptors.FieldDescriptor.Type and WireFormat.FieldType share constant names.
    return WireFormat.FieldType.valueOf(type.name()).getWireType();
  }

  private static void validateLeafCoercion(FieldDescriptor leafField, PDataType<?> requestedType,
    String path) throws SQLException {
    FieldDescriptor.Type protoType = leafField.getType();
    switch (protoType) {
      case STRING:
        if (requestedType == PVarchar.INSTANCE || requestedType == PVarbinary.INSTANCE) {
          return;
        }
        break;
      case BOOL:
        if (requestedType == PBoolean.INSTANCE) {
          return;
        }
        break;
      case INT32:
      case SINT32:
      case SFIXED32:
        if (
          requestedType == PInteger.INSTANCE || requestedType == PLong.INSTANCE
            || requestedType == PDecimal.INSTANCE
        ) {
          return;
        }
        if (requestedType == PBoolean.INSTANCE || requestedType == PVarchar.INSTANCE) {
          break;
        }
        break;
      case INT64:
      case SINT64:
      case SFIXED64:
        if (requestedType == PLong.INSTANCE || requestedType == PDecimal.INSTANCE) {
          return;
        }
        if (requestedType == PInteger.INSTANCE) {
          throw narrowing(protoType, requestedType, path);
        }
        break;
      case UINT32:
      case FIXED32:
        if (
          requestedType == PLong.INSTANCE || requestedType == PUnsignedLong.INSTANCE
            || requestedType == PDecimal.INSTANCE
        ) {
          return;
        }
        if (requestedType == PInteger.INSTANCE || requestedType == PUnsignedInt.INSTANCE) {
          // UNSIGNED_INT tops out at 2^31-1, so values up to 2^32-1 would not fit.
          throw narrowing(protoType, requestedType, path);
        }
        break;
      case UINT64:
      case FIXED64:
        if (requestedType == PDecimal.INSTANCE) {
          return;
        }
        if (
          requestedType == PInteger.INSTANCE || requestedType == PUnsignedInt.INSTANCE
            || requestedType == PLong.INSTANCE || requestedType == PUnsignedLong.INSTANCE
        ) {
          // BIGINT and UNSIGNED_LONG top out at 2^63-1, so values up to 2^64-1 would not fit.
          throw narrowing(protoType, requestedType, path);
        }
        break;
      case FLOAT:
        if (
          requestedType == PFloat.INSTANCE || requestedType == PDouble.INSTANCE
            || requestedType == PDecimal.INSTANCE
        ) {
          return;
        }
        break;
      case DOUBLE:
        if (requestedType == PDouble.INSTANCE || requestedType == PDecimal.INSTANCE) {
          return;
        }
        if (requestedType == PFloat.INSTANCE) {
          throw narrowing(protoType, requestedType, path);
        }
        break;
      case BYTES:
        if (requestedType == PVarbinary.INSTANCE || requestedType == PVarbinaryEncoded.INSTANCE) {
          return;
        }
        break;
      case MESSAGE:
        if (requestedType == PVarbinary.INSTANCE || requestedType == PVarbinaryEncoded.INSTANCE) {
          return;
        }
        if (requestedType == PProtobuf.INSTANCE) {
          throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
            .setMessage("PROTO_VALUE path '" + path + "': a nested message has no "
              + "descriptor binding and cannot be returned as PROTOBUF; request VARBINARY")
            .build().buildException();
        }
        break;
      case ENUM:
        if (requestedType == PInteger.INSTANCE || requestedType == PLong.INSTANCE) {
          return;
        }
        if (requestedType == PVarchar.INSTANCE) {
          throw unsupported("enum-leaf-as-VARCHAR (symbolic name) is not yet supported", path);
        }
        break;
      default:
        break;
    }
    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
      .setMessage("PROTO_VALUE path '" + path + "': cannot coerce protobuf type " + protoType.name()
        + " to requested SQL type " + requestedType.getSqlTypeName())
      .build().buildException();
  }

  private static SQLException narrowing(FieldDescriptor.Type protoType, PDataType<?> requestedType,
    String path) {
    return new SQLExceptionInfo.Builder(
      SQLExceptionCode.PROTOBUF_VALUE_TYPE_NARROWING_NOT_PERMITTED)
        .setMessage("PROTO_VALUE path '" + path + "': cannot narrow protobuf type "
          + protoType.name() + " to requested SQL type " + requestedType.getSqlTypeName()
          + "; use a wider type or cast at the caller")
        .build().buildException();
  }

  private static SQLException cardinality(String detail, String path) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_PATH_CARDINALITY_MISMATCH)
      .setMessage("PROTO_VALUE path '" + path + "': " + detail).build().buildException();
  }

  private static SQLException unsupported(String detail, String path) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_PATH_UNSUPPORTED_FEATURE)
      .setMessage("PROTO_VALUE path '" + path + "': " + detail).build().buildException();
  }
}
