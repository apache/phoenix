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
package org.apache.phoenix.schema.types;

import java.sql.Array;
import java.sql.SQLException;
import java.text.Format;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;

/**
 * Data type representing fixed-dimension single-precision floating point vectors,
 * {@code VECTOR(FLOAT, N)}.
 * <p>
 * Physical serialization stores elements contiguously as big-endian IEEE 754 single-precision
 * floating point values. The total byte length is fixed at {@code 4 * dimension}. Because vector
 * columns are stored as cell values rather than row keys, elements use standard binary float
 * representations rather than order-preserving sign-magnitude encoding.
 */
public class PVectorFloat extends PDataType<float[]> {

  public static final PVectorFloat INSTANCE = new PVectorFloat();

  /**
   * Maximum vector dimension supported in column definitions to constrain physical cell sizes and
   * execution overhead during distance calculations.
   */
  public static final int MAX_VECTOR_DIMENSION = 65536;

  private PVectorFloat() {
    super("VECTOR(FLOAT)", PDataType.VECTOR_FLOAT_TYPE, float[].class, null, 51);
  }

  /** Returns the vector dimension corresponding to the given byte length. */
  public static int dimension(int byteLength) {
    return byteLength / Bytes.SIZEOF_FLOAT;
  }

  /**
   * Reads a single float element by index from a packed vector buffer in ascending sort order.
   */
  public static float readElement(byte[] buf, int offset, int index) {
    return Bytes.toFloat(buf, offset + index * Bytes.SIZEOF_FLOAT);
  }

  /**
   * Reads a single float element by index from a packed vector buffer with the specified sort
   * order.
   */
  public static float readElement(byte[] buf, int offset, int index, SortOrder sortOrder) {
    int pos = offset + index * Bytes.SIZEOF_FLOAT;
    int bits = Bytes.toInt(buf, pos);
    if (sortOrder == SortOrder.DESC) {
      bits ^= 0xFFFFFFFF;
    }
    return Float.intBitsToFloat(bits);
  }

  /**
   * Reads a single float element by index from a vector byte pointer in ascending sort order.
   */
  public static float readElement(ImmutableBytesWritable ptr, int index) {
    return readElement(ptr.get(), ptr.getOffset(), index);
  }

  /**
   * Decodes all elements from a packed ascending-order vector byte range into a float array.
   */
  public static float[] readElements(byte[] buf, int offset, int length) {
    int dim = length / Bytes.SIZEOF_FLOAT;
    float[] out = new float[dim];
    for (int i = 0; i < dim; i++) {
      out[i] = Bytes.toFloat(buf, offset + i * Bytes.SIZEOF_FLOAT);
    }
    return out;
  }

  /**
   * Decodes all elements from a packed vector byte range using the specified sort order.
   */
  public static float[] readElements(byte[] buf, int offset, int length, SortOrder sortOrder) {
    int dim = length / Bytes.SIZEOF_FLOAT;
    float[] out = new float[dim];
    if (sortOrder == SortOrder.ASC) {
      for (int i = 0; i < dim; i++) {
        out[i] = Bytes.toFloat(buf, offset + i * Bytes.SIZEOF_FLOAT);
      }
    } else {
      for (int i = 0; i < dim; i++) {
        out[i] =
          Float.intBitsToFloat(Bytes.toInt(buf, offset + i * Bytes.SIZEOF_FLOAT) ^ 0xFFFFFFFF);
      }
    }
    return out;
  }

  /**
   * Writes a float array into a byte buffer as packed contiguous big-endian IEEE 754 values.
   * @throws IllegalArgumentException if the buffer is too small to contain the vector
   */
  public static void writeElements(float[] vector, byte[] buf, int offset) {
    if (buf.length < offset + (long) vector.length * Bytes.SIZEOF_FLOAT) {
      throw new IllegalArgumentException("Buffer too small: need "
        + (offset + vector.length * Bytes.SIZEOF_FLOAT) + " bytes, have " + buf.length);
    }
    for (int i = 0; i < vector.length; i++) {
      Bytes.putFloat(buf, offset + i * Bytes.SIZEOF_FLOAT, vector[i]);
    }
  }

  /**
   * Returns a newly allocated byte array containing the transcoded vector bytes converted to the
   * destination sort order without materializing an intermediate array.
   */
  public static byte[] transcodeBytes(byte[] src, int srcOffset, int srcLen, SortOrder srcOrder,
    SortOrder destOrder) {
    byte[] dest = new byte[srcLen];
    transcodeBytes(src, srcOffset, srcLen, srcOrder, dest, 0, destOrder);
    return dest;
  }

  /**
   * Copies and optionally transcodes packed vector bytes between sort orders directly at the byte
   * level without allocating an intermediate float array.
   * @param src        source byte array
   * @param srcOffset  start offset in source buffer
   * @param srcLen     number of bytes to copy
   * @param srcOrder   sort order of the source encoding
   * @param dest       destination byte array
   * @param destOffset start offset in destination buffer
   * @param destOrder  desired sort order for destination encoding
   */
  public static void transcodeBytes(byte[] src, int srcOffset, int srcLen, SortOrder srcOrder,
    byte[] dest, int destOffset, SortOrder destOrder) {
    if (srcOrder == destOrder) {
      System.arraycopy(src, srcOffset, dest, destOffset, srcLen);
    } else {
      // Invert byte values when converting between sort orders.
      SortOrder.invert(src, srcOffset, dest, destOffset, srcLen);
    }
  }

  /**
   * Decodes a centroid vector from the given byte range.
   */
  public static float[] decodeCentroid(byte[] bytes, int offset, int length, SortOrder sortOrder) {
    if (bytes == null || length == 0) {
      return null;
    }
    return readElements(bytes, offset, length, sortOrder);
  }

  /**
   * Decodes a centroid vector from the given byte pointer.
   */
  public static float[] decodeCentroid(ImmutableBytesWritable ptr, SortOrder sortOrder) {
    if (ptr == null || ptr.getLength() == 0) {
      return null;
    }
    return readElements(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder);
  }

  @Override
  public boolean isVectorType() {
    return true;
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public boolean canBePrimaryKey() {
    return false;
  }

  @Override
  public Integer getByteSize() {
    return null;
  }

  public int getByteSize(int dimension) {
    return Bytes.SIZEOF_FLOAT * dimension;
  }

  @Override
  public int estimateByteSize(Object o) {
    if (o == null) {
      return 0;
    }
    if (o instanceof float[]) {
      return ((float[]) o).length * Bytes.SIZEOF_FLOAT;
    }
    byte[] bytes = toBytes(o);
    return bytes.length;
  }

  @Override
  public Integer estimateByteSizeFromLength(Integer length) {
    if (length == null) {
      return null;
    }
    return length * Bytes.SIZEOF_FLOAT;
  }

  @Override
  public Integer getMaxLength(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof float[]) {
      return ((float[]) o).length;
    } else if (o instanceof Float[]) {
      return ((Float[]) o).length;
    } else if (o instanceof PhoenixArray) {
      return ((PhoenixArray) o).getDimensions();
    }
    return null;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    if (lhs == rhs) {
      return 0;
    }
    if (lhs == null) {
      return -1;
    }
    if (rhs == null) {
      return 1;
    }
    if (rhsType == this && lhs instanceof float[] && rhs instanceof float[]) {
      float[] lhsArr = (float[]) lhs;
      float[] rhsArr = (float[]) rhs;
      int minLen = Math.min(lhsArr.length, rhsArr.length);
      for (int i = 0; i < minLen; i++) {
        int cmp = Float.compare(lhsArr[i], rhsArr[i]);
        if (cmp != 0) {
          return cmp;
        }
      }
      return Integer.compare(lhsArr.length, rhsArr.length);
    }
    byte[] lhsBytes = toBytes(lhs);
    byte[] rhsBytes = rhsType.toBytes(rhs);
    return Bytes.compareTo(lhsBytes, rhsBytes);
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    float[] vector = toFloatArray(object);
    byte[] bytes = new byte[vector.length * Bytes.SIZEOF_FLOAT];
    writeElements(vector, bytes, 0);
    return bytes;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      return 0;
    }
    float[] vector = toFloatArray(object);
    writeElements(vector, bytes, offset);
    return vector.length * Bytes.SIZEOF_FLOAT;
  }

  @Override
  public byte[] toBytes(Object object, SortOrder sortOrder) {
    if (sortOrder == null) {
      sortOrder = SortOrder.getDefault();
    }
    byte[] bytes = toBytes(object);
    if (sortOrder == SortOrder.DESC) {
      SortOrder.invert(bytes, 0, bytes, 0, bytes.length);
    }
    return bytes;
  }

  public byte[] toBytes(Object object, SortOrder sortOrder, Integer expectedDimension) {
    if (sortOrder == null) {
      sortOrder = SortOrder.getDefault();
    }
    byte[] bytes = toBytes(object);
    if (expectedDimension != null) {
      int actualDim = bytes.length / Bytes.SIZEOF_FLOAT;
      if (actualDim != expectedDimension) {
        throw new ConstraintViolationException(
          "Vector dimension mismatch: expected " + expectedDimension + ", but got " + actualDim);
      }
    }
    if (sortOrder == SortOrder.DESC) {
      SortOrder.invert(bytes, 0, bytes, 0, bytes.length);
    }
    return bytes;
  }

  @Override
  public float[] toObject(byte[] bytes, int offset, int length, PDataType actualType,
    SortOrder sortOrder, Integer maxLength, Integer scale) {
    if (bytes == null || length == 0) {
      return null;
    }
    if (sortOrder == null) {
      sortOrder = SortOrder.getDefault();
    }
    return readElements(bytes, offset, length, sortOrder);
  }

  @Override
  public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
    SortOrder sortOrder, Integer maxLength, Integer scale, Class jdbcType) throws SQLException {
    if (float[].class.isAssignableFrom(jdbcType) || Object.class.equals(jdbcType)) {
      return toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
    }
    if (Float[].class.isAssignableFrom(jdbcType)) {
      float[] raw = toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
      if (raw == null) {
        return null;
      }
      Float[] boxed = new Float[raw.length];
      for (int i = 0; i < raw.length; i++) {
        boxed[i] = raw[i];
      }
      return boxed;
    }
    if (java.sql.Array.class.isAssignableFrom(jdbcType)) {
      float[] raw = toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
      if (raw == null) {
        return null;
      }
      Float[] boxed = new Float[raw.length];
      for (int i = 0; i < raw.length; i++) {
        boxed[i] = raw[i];
      }
      return PArrayDataType.instantiatePhoenixArray(PFloat.INSTANCE, boxed);
    }
    throw newMismatchException(actualType, jdbcType);
  }

  @Override
  public Object toObject(String value) {
    if (value == null) {
      return null;
    }
    String str = value.trim();
    if (str.isEmpty()) {
      return null;
    }
    if (str.startsWith("[") && str.endsWith("]")) {
      str = str.substring(1, str.length() - 1).trim();
    }
    if (str.isEmpty()) {
      return new float[0];
    }
    String[] parts = str.split(",");
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      vector[i] = Float.parseFloat(parts[i].trim());
    }
    return vector;
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    if (object == null) {
      return null;
    }
    if (object instanceof float[]) {
      return object;
    }
    if (object instanceof Float[]) {
      Float[] arr = (Float[]) object;
      float[] vector = new float[arr.length];
      for (int i = 0; i < arr.length; i++) {
        vector[i] = arr[i] == null ? 0.0f : arr[i];
      }
      return vector;
    }
    if (object instanceof PhoenixArray) {
      PhoenixArray pArr = (PhoenixArray) object;
      try {
        Object arrayObj = pArr.getArray();
        if (arrayObj instanceof float[]) {
          return arrayObj;
        } else if (arrayObj instanceof Float[]) {
          Float[] arr = (Float[]) arrayObj;
          float[] vector = new float[arr.length];
          for (int i = 0; i < arr.length; i++) {
            vector[i] = arr[i] == null ? 0.0f : arr[i];
          }
          return vector;
        } else if (arrayObj instanceof Object[]) {
          Object[] arr = (Object[]) arrayObj;
          float[] vector = new float[arr.length];
          for (int i = 0; i < arr.length; i++) {
            vector[i] = arr[i] == null ? 0.0f : ((Number) arr[i]).floatValue();
          }
          return vector;
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    if (object instanceof Array) {
      try {
        Object arrayObj = ((Array) object).getArray();
        if (arrayObj instanceof float[]) {
          return arrayObj;
        } else if (arrayObj instanceof Float[]) {
          Float[] arr = (Float[]) arrayObj;
          float[] vector = new float[arr.length];
          for (int i = 0; i < arr.length; i++) {
            vector[i] = arr[i] == null ? 0.0f : arr[i];
          }
          return vector;
        } else if (arrayObj instanceof Object[]) {
          Object[] arr = (Object[]) arrayObj;
          float[] vector = new float[arr.length];
          for (int i = 0; i < arr.length; i++) {
            vector[i] = arr[i] == null ? 0.0f : ((Number) arr[i]).floatValue();
          }
          return vector;
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    if (actualType == PVarchar.INSTANCE || actualType == PChar.INSTANCE) {
      return toObject((String) object);
    }
    if (actualType == PVarbinary.INSTANCE || actualType == PBinary.INSTANCE) {
      byte[] b = (byte[]) object;
      return toObject(b, 0, b.length);
    }
    if (object instanceof String) {
      return toObject((String) object);
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return this.equals(targetType) || targetType.equals(PFloatArray.INSTANCE)
      || super.isCoercibleTo(targetType);
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return this.equals(targetType) || targetType.isArrayType()
      || targetType.equals(PVarchar.INSTANCE) || super.isCastableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value == null) {
      return true;
    }
    return isCoercibleTo(targetType);
  }

  @Override
  public boolean isBytesComparableWith(PDataType otherType) {
    return this.equals(otherType);
  }

  @Override
  public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
    SortOrder sortOrder, Integer maxLength, Integer scale, Integer desiredMaxLength,
    Integer desiredScale) {
    if (desiredMaxLength == null) {
      return true;
    }
    int expectedDimension = desiredMaxLength;
    int actualDimension = -1;
    if (value != null) {
      if (value instanceof float[]) {
        actualDimension = ((float[]) value).length;
      } else if (value instanceof Float[]) {
        actualDimension = ((Float[]) value).length;
      } else if (value instanceof PhoenixArray) {
        actualDimension = ((PhoenixArray) value).getDimensions();
      } else if (value instanceof Array) {
        try {
          Object arr = ((Array) value).getArray();
          if (arr instanceof Object[]) {
            actualDimension = ((Object[]) arr).length;
          } else if (arr instanceof float[]) {
            actualDimension = ((float[]) arr).length;
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (actualDimension != -1 && actualDimension != expectedDimension) {
      throw new ConstraintViolationException("Vector dimension mismatch: expected "
        + expectedDimension + ", but got " + actualDimension);
    }
    if (ptr != null && ptr.getLength() != 0) {
      int actualDim = ptr.getLength() / Bytes.SIZEOF_FLOAT;
      if (actualDim != expectedDimension) {
        throw new ConstraintViolationException(
          "Vector dimension mismatch: expected " + expectedDimension + ", but got " + actualDim);
      }
    }
    return true;
  }

  @Override
  public void coerceBytes(ImmutableBytesWritable ptr, Object o, PDataType actualType,
    Integer actualMaxLength, Integer actualScale, SortOrder actualModifier,
    Integer desiredMaxLength, Integer desiredScale, SortOrder expectedModifier) {
    super.coerceBytes(ptr, o, actualType, actualMaxLength, actualScale, actualModifier,
      desiredMaxLength, desiredScale, expectedModifier);
    if (ptr.getLength() > 0 && desiredMaxLength != null) {
      int actualDim = ptr.getLength() / Bytes.SIZEOF_FLOAT;
      if (actualDim != desiredMaxLength) {
        throw new ConstraintViolationException(
          "Vector dimension mismatch: expected " + desiredMaxLength + ", but got " + actualDim);
      }
    }
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    int dim = maxLength == null ? (arrayLength == null ? 3 : arrayLength) : maxLength;
    float[] sample = new float[dim];
    for (int i = 0; i < dim; i++) {
      sample[i] = RANDOM.get().nextFloat();
    }
    return sample;
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
    if (o == null) {
      return String.valueOf((Object) null);
    }
    float[] array = (float[]) o;
    StringBuilder buf = new StringBuilder("[");
    for (int i = 0; i < array.length; i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(array[i]);
    }
    buf.append("]");
    return buf.toString();
  }

  private float[] toFloatArray(Object object) {
    if (object instanceof float[]) {
      return (float[]) object;
    }
    return (float[]) toObject(object, this);
  }
}
