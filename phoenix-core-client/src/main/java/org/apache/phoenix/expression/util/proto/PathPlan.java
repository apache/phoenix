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

import java.util.Collections;
import java.util.List;
import org.apache.phoenix.schema.types.PDataType;

import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Immutable compiled walk plan produced by {@link ProtoPathResolver} and consumed by
 * {@link ProtoPathWalker}. One plan corresponds to one resolved {@code PROTO_VALUE(...)} function
 * instance. Cached on the function instance so a single scan pays the resolve cost once and reuses
 * it for every row.
 */
public final class PathPlan {

  /** Distinguishes how a walker consumes a segment against the encoded payload. */
  public enum SegmentKind {
    /** Scalar leaf — the segment field is a primitive protobuf type. */
    SCALAR,
    /** Nested message — recurse into the length-delimited inner payload. */
    MESSAGE,
    /** N-th element of a repeated field (unpacked). */
    REPEATED_INDEX,
    /** String key of a {@code map<string, V>} entry. */
    MAP_KEY_STRING,
    /** Integer key of a {@code map<int32|int64|uint32|uint64, V>} entry. */
    MAP_KEY_INT,
    /** Count of elements in a repeated field or entries in a map (used by PROTO_ARRAY_LENGTH). */
    COUNT_ARRAY
  }

  /** One step in the compiled walk. Field is public for allocation-free hot-path read. */
  public static final class PlanSegment {
    public final int tagNumber;
    public final int wireType;
    public final SegmentKind kind;
    public final int repeatedIndex;
    public final String mapKeyString;
    public final long mapKeyInt;
    public final int mapValueWireType;
    public final boolean scalarRepeated;

    public PlanSegment(int tagNumber, int wireType, SegmentKind kind, int repeatedIndex,
      String mapKeyString, long mapKeyInt, int mapValueWireType, boolean scalarRepeated) {
      this.tagNumber = tagNumber;
      this.wireType = wireType;
      this.kind = kind;
      this.repeatedIndex = repeatedIndex;
      this.mapKeyString = mapKeyString;
      this.mapKeyInt = mapKeyInt;
      this.mapValueWireType = mapValueWireType;
      this.scalarRepeated = scalarRepeated;
    }

    public static PlanSegment scalar(int tagNumber, int wireType) {
      return new PlanSegment(tagNumber, wireType, SegmentKind.SCALAR, 0, null, 0L, 0, false);
    }

    public static PlanSegment message(int tagNumber) {
      return new PlanSegment(tagNumber, 2, SegmentKind.MESSAGE, 0, null, 0L, 0, false);
    }

    public static PlanSegment repeatedIndex(int tagNumber, int elementWireType, int index) {
      return new PlanSegment(tagNumber, elementWireType, SegmentKind.REPEATED_INDEX, index, null,
        0L, 0, false);
    }

    public static PlanSegment mapKeyString(int tagNumber, String key, int valueWireType) {
      return new PlanSegment(tagNumber, 2, SegmentKind.MAP_KEY_STRING, 0, key, 0L, valueWireType,
        false);
    }

    public static PlanSegment mapKeyInt(int tagNumber, long key, int valueWireType) {
      return new PlanSegment(tagNumber, 2, SegmentKind.MAP_KEY_INT, 0, null, key, valueWireType,
        false);
    }

    public static PlanSegment countArray(int tagNumber, boolean scalarRepeated,
      int elementWireType) {
      return new PlanSegment(tagNumber, elementWireType, SegmentKind.COUNT_ARRAY, 0, null, 0L, 0,
        scalarRepeated);
    }
  }

  private final List<PlanSegment> segments;
  private final FieldDescriptor.Type leafProtoFieldType;
  private final PDataType<?> requestedType;
  private final Object leafDefault;

  public PathPlan(List<PlanSegment> segments, FieldDescriptor.Type leafProtoFieldType,
    PDataType<?> requestedType, Object leafDefault) {
    if (segments == null || segments.isEmpty()) {
      throw new IllegalArgumentException("segments must be non-empty");
    }
    this.segments = Collections.unmodifiableList(segments);
    this.leafProtoFieldType = leafProtoFieldType;
    this.requestedType = requestedType;
    this.leafDefault = leafDefault;
  }

  public List<PlanSegment> getSegments() {
    return segments;
  }

  /**
   * @return the leaf field's protobuf type so the walker can dispatch to the correct scalar decoder
   *         without holding a live {@code FieldDescriptor} reference on the hot path
   */
  public FieldDescriptor.Type getLeafProtoFieldType() {
    return leafProtoFieldType;
  }

  public PDataType<?> getRequestedType() {
    return requestedType;
  }

  /**
   * @return the value of an unset leaf whose parent is present: the declared
   *         {@code [default = ...]} if any, else the type's zero value; null if such a leaf reads
   *         as SQL NULL
   */
  public Object getLeafDefault() {
    return leafDefault;
  }
}
