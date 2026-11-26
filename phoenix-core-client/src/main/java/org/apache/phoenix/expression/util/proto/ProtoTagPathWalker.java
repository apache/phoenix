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
import java.math.BigDecimal;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.util.proto.ProtoTagPathParser.TagSegment;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

import org.apache.phoenix.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.phoenix.thirdparty.com.google.protobuf.WireFormat;

/**
 * Walks raw protobuf payload bytes using a {@link TagPathPlan} compiled from a schema-less
 * tag-number path. Uses {@link CodedInputStream} directly without any descriptor. Returns a decoded
 * Java object for the leaf value, or {@code null} when the tag is not found or the wire type
 * mismatches.
 */
public final class ProtoTagPathWalker {

  /**
   * Returned by {@link #walk} when the tag is present but its wire type does not match the
   * requested SQL type (for example a fixed32 field read as BIGINT). Unlike {@code null}, which
   * means the tag is absent, this must not trigger a user default.
   */
  public static final Object WIRE_TYPE_MISMATCH = new Object();

  private ProtoTagPathWalker() {
  }

  /**
   * Maps a Phoenix SQL type to its expected protobuf wire type for schema-less access. Returns -1
   * for unsupported types.
   */
  public static int wireTypeForSqlType(PDataType<?> sqlType) {
    if (
      sqlType == PVarchar.INSTANCE || sqlType == PVarbinary.INSTANCE
        || sqlType == PVarbinaryEncoded.INSTANCE
    ) {
      return WireFormat.WIRETYPE_LENGTH_DELIMITED;
    }
    if (
      sqlType == PBoolean.INSTANCE || sqlType == PInteger.INSTANCE || sqlType == PLong.INSTANCE
        || sqlType == PUnsignedInt.INSTANCE || sqlType == PUnsignedLong.INSTANCE
        || sqlType == PDecimal.INSTANCE
    ) {
      return WireFormat.WIRETYPE_VARINT;
    }
    if (sqlType == PFloat.INSTANCE) {
      return WireFormat.WIRETYPE_FIXED32;
    }
    if (sqlType == PDouble.INSTANCE) {
      return WireFormat.WIRETYPE_FIXED64;
    }
    return -1;
  }

  /**
   * Walks {@code length} payload bytes starting at {@code offset} in place. The buffer is only
   * read, and every returned byte[] or String is a copy. Returns the decoded leaf value, {@code
   * null} if the tag is absent, or {@link #WIRE_TYPE_MISMATCH} if the tag is present with a wire
   * type that does not match the requested SQL type.
   * <p>
   * Schema-less limits: integer types are read from varint fields only, as a two's-complement
   * signed 64-bit value (so a uint64 of 2^63 or more reads as negative, and sint zigzag is not
   * decoded). Fixed-width integer fields never match and give {@link #WIRE_TYPE_MISMATCH}. Use
   * PROTO_VALUE with a registered descriptor for those.
   */
  public static Object walk(byte[] buf, int offset, int length, TagPathPlan plan)
    throws IOException {
    if (buf == null || length == 0) {
      return null;
    }
    return walkSegments(buf, offset, length, plan.getSegments(), 0, plan);
  }

  private static Object walkSegments(byte[] buf, int offset, int length, List<TagSegment> segments,
    int segIdx, TagPathPlan plan) throws IOException {
    if (segIdx >= segments.size()) {
      return null;
    }
    TagSegment seg = segments.get(segIdx);
    boolean isLast = segIdx == segments.size() - 1;

    if (isLast) {
      return readLeaf(buf, offset, length, seg, plan);
    }
    byte[] inner = ProtoPathWalker.findNthLenBytes(buf, offset, length, seg.tagNumber,
      seg.hasIndex() ? seg.index : 0);
    if (inner == null) {
      return null;
    }
    return walkSegments(inner, 0, inner.length, segments, segIdx + 1, plan);
  }

  private static Object readLeaf(byte[] buf, int offset, int length, TagSegment seg,
    TagPathPlan plan) throws IOException {
    int expectedWire = plan.getLeafWireType();
    PDataType<?> reqType = plan.getRequestedType();

    if (seg.hasIndex()) {
      return findNthElement(buf, offset, length, seg.tagNumber, expectedWire, seg.index, reqType);
    }
    CodedInputStream cis = CodedInputStream.newInstance(buf, offset, length);
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int fieldNum = WireFormat.getTagFieldNumber(tag);
      int wt = WireFormat.getTagWireType(tag);
      if (fieldNum == seg.tagNumber) {
        return wt == expectedWire ? decodeLeaf(cis, expectedWire, reqType) : WIRE_TYPE_MISMATCH;
      }
      cis.skipField(tag);
    }
    return null;
  }

  private static Object findNthElement(byte[] buf, int offset, int length, int tagNumber,
    int expectedWire, int n, PDataType<?> reqType) throws IOException {
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
      if (wt == expectedWire) {
        if (count == n) {
          return decodeLeaf(cis, expectedWire, reqType);
        }
        cis.skipField(tag);
        count++;
      } else if (wt == WireFormat.WIRETYPE_LENGTH_DELIMITED && isScalarWire(expectedWire)) {
        // Packed encoding: LEN-delimited chunk of scalars.
        byte[] packed = cis.readByteArray();
        CodedInputStream chunk = CodedInputStream.newInstance(packed);
        while (!chunk.isAtEnd()) {
          if (count == n) {
            return decodeLeaf(chunk, expectedWire, reqType);
          }
          ProtoPathWalker.skipOneScalarElement(chunk, expectedWire);
          count++;
        }
      } else {
        return WIRE_TYPE_MISMATCH;
      }
    }
    return null;
  }

  private static Object decodeLeaf(CodedInputStream cis, int wireType, PDataType<?> reqType)
    throws IOException {
    switch (wireType) {
      case WireFormat.WIRETYPE_VARINT:
        return decodeVarint(cis, reqType);
      case WireFormat.WIRETYPE_FIXED64:
        return decodeFixed64(cis, reqType);
      case WireFormat.WIRETYPE_FIXED32:
        return decodeFixed32(cis, reqType);
      case WireFormat.WIRETYPE_LENGTH_DELIMITED:
        return decodeLenDelimited(cis, reqType);
      default:
        return null;
    }
  }

  private static Object decodeVarint(CodedInputStream cis, PDataType<?> reqType)
    throws IOException {
    long raw = cis.readRawVarint64();
    if (reqType == PBoolean.INSTANCE) {
      return raw != 0;
    }
    if (reqType == PLong.INSTANCE) {
      return raw;
    }
    if (reqType == PUnsignedLong.INSTANCE) {
      return raw;
    }
    if (reqType == PDecimal.INSTANCE) {
      return BigDecimal.valueOf(raw);
    }
    return raw;
  }

  private static Object decodeFixed64(CodedInputStream cis, PDataType<?> reqType)
    throws IOException {
    if (reqType == PDouble.INSTANCE) {
      return cis.readDouble();
    }
    return cis.readFixed64();
  }

  private static Object decodeFixed32(CodedInputStream cis, PDataType<?> reqType)
    throws IOException {
    if (reqType == PFloat.INSTANCE) {
      return cis.readFloat();
    }
    return cis.readFixed32();
  }

  private static Object decodeLenDelimited(CodedInputStream cis, PDataType<?> reqType)
    throws IOException {
    byte[] bytes = cis.readByteArray();
    if (reqType == PVarchar.INSTANCE) {
      return Bytes.toString(bytes);
    }
    return bytes;
  }

  private static boolean isScalarWire(int wireType) {
    return wireType == WireFormat.WIRETYPE_VARINT || wireType == WireFormat.WIRETYPE_FIXED32
      || wireType == WireFormat.WIRETYPE_FIXED64;
  }
}
