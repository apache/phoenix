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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.phoenix.expression.util.proto.PathPlan.PlanSegment;
import org.apache.phoenix.expression.util.proto.PathPlan.SegmentKind;

import org.apache.phoenix.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.WireFormat;

/**
 * Walks raw protobuf payload bytes with a compiled {@link PathPlan} and returns the leaf value as a
 * Java {@link Object} (or {@code null} for missing paths). Runs on the hot path — one call per row
 * per {@code PROTO_VALUE} function instance.
 * <p>
 * Uses {@link CodedInputStream} directly (no {@code DynamicMessage} allocation). For
 * length-delimited children (MESSAGE, MAP_ENTRY, repeated-of-message), the walker slices the child
 * bytes via {@code readByteArray()} and recurses on the remaining plan tail.
 * <p>
 * Leaf return conventions:
 * <ul>
 * <li>STRING → {@link String}</li>
 * <li>BOOL → {@link Boolean}</li>
 * <li>INT32 / SINT32 / SFIXED32 / ENUM → {@link Integer}</li>
 * <li>INT64 / SINT64 / SFIXED64 / UINT32 / FIXED32 → {@link Long}</li>
 * <li>UINT64 / FIXED64 → {@link Long} (raw bits; values of 2^63 and above are negative, and the
 * caller reads them as unsigned)</li>
 * <li>FLOAT → {@link Float}</li>
 * <li>DOUBLE → {@link Double}</li>
 * <li>BYTES / MESSAGE → {@code byte[]}</li>
 * </ul>
 * A caller-visible {@code null} means "path not present" (missing message, out-of-range repeated
 * index, missing map key, child-of-unset-parent). {@link #LEAF_ABSENT} means the scalar leaf is
 * unset but its parent is present; the caller then uses {@link PathPlan#getLeafDefault()}.
 * <p>
 * <b>Non-conformance trade-off for duplicate tags.</b> The protobuf wire-format spec requires
 * parsers to keep the <b>last</b> value of a non-repeated scalar that appears more than once, to
 * merge multiple encoded instances of the same MESSAGE field via {@code MergeFrom} (equivalent to
 * concatenating the LEN-delimited slices), and to keep the last map entry for a repeated key. This
 * walker instead returns the <b>first</b> occurrence and short-circuits, so a lookup stops as soon
 * as it finds the tag. The difference is only observable if a non-repeated field or map key appears
 * more than once in the same parent's bytes, which no canonical serializer (Google Java / C++ /
 * Python / Go / Rust generated code) ever produces. Concatenated or handcrafted byte streams CAN
 * produce duplicates; those fall outside the supported input contract.
 */
public final class ProtoPathWalker {

  public static final Object LEAF_ABSENT = new Object();

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final Object MAP_VALUE_OMITTED = new Object();

  private ProtoPathWalker() {
  }

  /**
   * Walks {@code length} payload bytes starting at {@code offset} in place. The buffer is only
   * read, and every returned byte[] or String is a copy.
   */
  public static Object walk(byte[] buf, int offset, int length, PathPlan plan) throws IOException {
    if (buf == null || length == 0) {
      return null;
    }
    return walkSegments(buf, offset, length, plan.getSegments(), 0, plan);
  }

  public static int walkForCount(byte[] buf, int offset, int length, PathPlan plan)
    throws IOException {
    Object result = walk(buf, offset, length, plan);
    if (result == null || result == LEAF_ABSENT) {
      return 0;
    }
    if (result instanceof Integer) {
      return (Integer) result;
    }
    throw new IOException(
      "walkForCount expected Integer count, got " + result.getClass().getName());
  }

  private static Object walkSegments(byte[] buf, int offset, int length, List<PlanSegment> segments,
    int segIdx, PathPlan plan) throws IOException {
    if (segIdx >= segments.size()) {
      return null;
    }
    PlanSegment seg = segments.get(segIdx);
    boolean isLast = (segIdx == segments.size() - 1);
    switch (seg.kind) {
      case SCALAR:
        return findFirstScalar(buf, offset, length, seg.tagNumber, seg.wireType,
          plan.getLeafProtoFieldType());
      case MESSAGE:
        return handleMessage(buf, offset, length, segments, segIdx, plan, isLast);
      case REPEATED_INDEX:
        return handleRepeatedIndex(buf, offset, length, segments, segIdx, plan, isLast);
      case MAP_KEY_STRING:
      case MAP_KEY_INT:
        return handleMapEntry(buf, offset, length, segments, segIdx, plan, isLast);
      case COUNT_ARRAY:
        return countAtTag(buf, offset, length, seg.tagNumber, seg.scalarRepeated, seg.wireType);
      default:
        return null;
    }
  }

  private static int countAtTag(byte[] buf, int offset, int length, int tagNumber,
    boolean scalarRepeated, int elementWireType) throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(buf, offset, length);
    int count = 0;
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int fieldNum = WireFormat.getTagFieldNumber(tag);
      int wt = WireFormat.getTagWireType(tag);
      if (fieldNum != tagNumber) {
        cis.skipField(tag);
        continue;
      }
      if (scalarRepeated && wt == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        int chunkLen = cis.readRawVarint32();
        int oldLimit = cis.pushLimit(chunkLen);
        while (!cis.isAtEnd()) {
          skipOneScalarElement(cis, elementWireType);
          count++;
        }
        cis.popLimit(oldLimit);
      } else {
        cis.skipField(tag);
        count++;
      }
    }
    return count;
  }

  static void skipOneScalarElement(CodedInputStream cis, int elementWireType) throws IOException {
    switch (elementWireType) {
      case WireFormat.WIRETYPE_VARINT:
        cis.readRawVarint64();
        return;
      case WireFormat.WIRETYPE_FIXED32:
        cis.readRawLittleEndian32();
        return;
      case WireFormat.WIRETYPE_FIXED64:
        cis.readRawLittleEndian64();
        return;
      default:
        throw new IOException("packed chunk with unexpected element wire type " + elementWireType);
    }
  }

  private static Object handleMessage(byte[] buf, int offset, int length,
    List<PlanSegment> segments, int segIdx, PathPlan plan, boolean isLast) throws IOException {
    // First-occurrence fast path — see the class Javadoc for the trade-off.
    byte[] inner = findNthLenBytes(buf, offset, length, segments.get(segIdx).tagNumber, 0);
    if (inner == null) {
      return null;
    }
    if (isLast) {
      return inner;
    }
    return walkSegments(inner, 0, inner.length, segments, segIdx + 1, plan);
  }

  private static Object handleRepeatedIndex(byte[] buf, int offset, int length,
    List<PlanSegment> segments, int segIdx, PathPlan plan, boolean isLast) throws IOException {
    PlanSegment seg = segments.get(segIdx);
    if (seg.wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
      byte[] elem = findNthLenBytes(buf, offset, length, seg.tagNumber, seg.repeatedIndex);
      if (elem == null) {
        return null;
      }
      if (isLast) {
        if (plan.getLeafProtoFieldType() == FieldDescriptor.Type.STRING) {
          return new String(elem, StandardCharsets.UTF_8);
        }
        return elem;
      }
      return walkSegments(elem, 0, elem.length, segments, segIdx + 1, plan);
    }
    // Scalar element — per protobuf spec, parsers accept both packed and
    // unpacked encodings on the same tag regardless of what the schema
    // declares. Proto3 defaults to packed; proto2 to unpacked; both may
    // appear intermixed on the wire during schema evolution.
    Object value = findNthScalarAcceptingPacked(buf, offset, length, seg.tagNumber, seg.wireType,
      seg.repeatedIndex, plan.getLeafProtoFieldType());
    if (value == null) {
      return null;
    }
    return isLast ? value : null;
  }

  private static Object handleMapEntry(byte[] buf, int offset, int length,
    List<PlanSegment> segments, int segIdx, PathPlan plan, boolean isLast) throws IOException {
    PlanSegment seg = segments.get(segIdx);
    FieldDescriptor.Type leafType = plan.getLeafProtoFieldType();
    Object rawValue = findMapEntryValue(buf, offset, length, seg, leafType);
    if (rawValue == null) {
      return null;
    }
    boolean valueOmitted = (rawValue == MAP_VALUE_OMITTED);
    if (seg.mapValueWireType != WireFormat.WIRETYPE_LENGTH_DELIMITED) {
      // Scalar value, already decoded by matchAndExtractValue; it cannot be walked into.
      if (!isLast) {
        return null;
      }
      return valueOmitted ? scalarDefault(seg.mapValueWireType, leafType) : rawValue;
    }
    // Omitted value tag → EMPTY_BYTES, which decodes to the type default.
    byte[] valueBytes = valueOmitted ? EMPTY_BYTES : (byte[]) rawValue;
    if (isLast) {
      if (leafType == FieldDescriptor.Type.STRING) {
        return new String(valueBytes, StandardCharsets.UTF_8);
      }
      return valueBytes;
    }
    return walkSegments(valueBytes, 0, valueBytes.length, segments, segIdx + 1, plan);
  }

  private static Object scalarDefault(int wireType, FieldDescriptor.Type leafType) {
    switch (wireType) {
      case WireFormat.WIRETYPE_VARINT:
        if (leafType == FieldDescriptor.Type.BOOL) {
          return Boolean.FALSE;
        }
        if (
          leafType == FieldDescriptor.Type.INT32 || leafType == FieldDescriptor.Type.SINT32
            || leafType == FieldDescriptor.Type.ENUM
        ) {
          return Integer.valueOf(0);
        }
        return Long.valueOf(0L);
      case WireFormat.WIRETYPE_FIXED64:
        if (leafType == FieldDescriptor.Type.DOUBLE) {
          return Double.valueOf(0.0d);
        }
        return Long.valueOf(0L);
      case WireFormat.WIRETYPE_FIXED32:
        if (leafType == FieldDescriptor.Type.FLOAT) {
          return Float.valueOf(0.0f);
        }
        if (leafType == FieldDescriptor.Type.SFIXED32) {
          return Integer.valueOf(0);
        }
        return Long.valueOf(0L);
      default:
        return null;
    }
  }

  private static Object findFirstScalar(byte[] buf, int offset, int length, int tagNumber,
    int wireType, FieldDescriptor.Type leafProtoFieldType) throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(buf, offset, length);
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int fieldNum = WireFormat.getTagFieldNumber(tag);
      int wt = WireFormat.getTagWireType(tag);
      if (fieldNum == tagNumber && wt == wireType) {
        return readScalarByWireType(cis, wireType, leafProtoFieldType);
      }
      cis.skipField(tag);
    }
    return LEAF_ABSENT;
  }

  /** Returns the n-th (0-based) LEN-delimited occurrence of tagNumber, or null. */
  static byte[] findNthLenBytes(byte[] buf, int offset, int length, int tagNumber, int n)
    throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(buf, offset, length);
    int count = 0;
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int fieldNum = WireFormat.getTagFieldNumber(tag);
      int wt = WireFormat.getTagWireType(tag);
      if (fieldNum == tagNumber && wt == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        if (count == n) {
          return cis.readByteArray();
        }
        cis.skipField(tag);
        count++;
      } else {
        cis.skipField(tag);
      }
    }
    return null;
  }

  private static Object findNthScalarAcceptingPacked(byte[] buf, int offset, int length,
    int tagNumber, int elementWireType, int n, FieldDescriptor.Type leafProtoFieldType)
    throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(buf, offset, length);
    int count = 0;
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int fieldNum = WireFormat.getTagFieldNumber(tag);
      int wt = WireFormat.getTagWireType(tag);
      if (fieldNum != tagNumber) {
        cis.skipField(tag);
        continue;
      }
      if (wt == elementWireType) {
        Object value = readScalarByWireType(cis, elementWireType, leafProtoFieldType);
        if (count == n) {
          return value;
        }
        count++;
      } else if (wt == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        byte[] packed = cis.readByteArray();
        CodedInputStream chunk = CodedInputStream.newInstance(packed);
        while (!chunk.isAtEnd()) {
          Object value = readScalarByWireType(chunk, elementWireType, leafProtoFieldType);
          if (count == n) {
            return value;
          }
          count++;
        }
      } else {
        cis.skipField(tag);
      }
    }
    return null;
  }

  private static Object findMapEntryValue(byte[] buf, int offset, int length, PlanSegment seg,
    FieldDescriptor.Type leafType) throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(buf, offset, length);
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int fieldNum = WireFormat.getTagFieldNumber(tag);
      int wt = WireFormat.getTagWireType(tag);
      if (fieldNum == seg.tagNumber && wt == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        byte[] entryBytes = cis.readByteArray();
        Object result = matchAndExtractValue(entryBytes, seg, leafType);
        if (result != null) {
          return result;
        }
      } else {
        cis.skipField(tag);
      }
    }
    return null;
  }

  /**
   * Returns the entry's value if its key matches: raw bytes for a LEN-delimited value, the decoded
   * Java value for a scalar value, {@link #MAP_VALUE_OMITTED} if the value tag is absent; or null
   * if the key does not match.
   */
  private static Object matchAndExtractValue(byte[] entryBytes, PlanSegment seg,
    FieldDescriptor.Type leafType) throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(entryBytes);
    boolean keyMatched = false;
    boolean valueSeen = false;
    Object value = null;
    boolean valueIsLenDelimited = (seg.mapValueWireType == WireFormat.WIRETYPE_LENGTH_DELIMITED);
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int fieldNum = WireFormat.getTagFieldNumber(tag);
      if (fieldNum == 1) {
        if (seg.kind == SegmentKind.MAP_KEY_STRING) {
          String key = cis.readString();
          if (seg.mapKeyString.equals(key)) {
            keyMatched = true;
          }
        } else {
          long key = cis.readInt64();
          if (key == seg.mapKeyInt) {
            keyMatched = true;
          }
        }
      } else if (fieldNum == 2) {
        valueSeen = true;
        value = valueIsLenDelimited
          ? cis.readByteArray()
          : readScalarByWireType(cis, seg.mapValueWireType, leafType);
      } else {
        cis.skipField(tag);
      }
    }
    if (!keyMatched) {
      return null;
    }
    // Key matched. Value tag may have been omitted (proto3 defaulted value) —
    // signal that to the caller via MAP_VALUE_OMITTED so it can synthesize the
    // value type's implicit-presence default rather than treating the entry as
    // absent.
    return valueSeen ? value : MAP_VALUE_OMITTED;
  }

  private static Object readScalarByWireType(CodedInputStream cis, int wireType,
    FieldDescriptor.Type leafProtoFieldType) throws IOException {
    switch (wireType) {
      case WireFormat.WIRETYPE_VARINT:
        return readVarintByLeafType(cis, leafProtoFieldType);
      case WireFormat.WIRETYPE_FIXED64:
        return readFixed64ByLeafType(cis, leafProtoFieldType);
      case WireFormat.WIRETYPE_FIXED32:
        return readFixed32ByLeafType(cis, leafProtoFieldType);
      case WireFormat.WIRETYPE_LENGTH_DELIMITED:
        if (leafProtoFieldType == FieldDescriptor.Type.STRING) {
          return cis.readString();
        }
        return cis.readByteArray();
      default:
        return null;
    }
  }

  private static Object readVarintByLeafType(CodedInputStream cis, FieldDescriptor.Type leafType)
    throws IOException {
    if (leafType == FieldDescriptor.Type.BOOL) {
      return cis.readBool();
    }
    if (leafType == FieldDescriptor.Type.INT32 || leafType == FieldDescriptor.Type.ENUM) {
      return cis.readInt32();
    }
    if (leafType == FieldDescriptor.Type.SINT32) {
      return cis.readSInt32();
    }
    if (leafType == FieldDescriptor.Type.SINT64) {
      return cis.readSInt64();
    }
    if (leafType == FieldDescriptor.Type.UINT32) {
      // Widen to Long so unsigned semantics are preserved for values above
      // Integer.MAX_VALUE without accidental sign flip.
      return cis.readInt64() & 0xFFFFFFFFL;
    }
    // INT64, UINT64 and any other varint leaf type: raw value as Long.
    return cis.readInt64();
  }

  private static Object readFixed64ByLeafType(CodedInputStream cis, FieldDescriptor.Type leafType)
    throws IOException {
    if (leafType == FieldDescriptor.Type.DOUBLE) {
      return cis.readDouble();
    }
    return cis.readFixed64();
  }

  private static Object readFixed32ByLeafType(CodedInputStream cis, FieldDescriptor.Type leafType)
    throws IOException {
    if (leafType == FieldDescriptor.Type.FLOAT) {
      return cis.readFloat();
    }
    if (leafType == FieldDescriptor.Type.SFIXED32) {
      return cis.readSFixed32();
    }
    return cis.readFixed32() & 0xFFFFFFFFL;
  }
}
