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

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.text.Format;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;

/**
 * Base class for fixed-dimension vector types {@code VECTOR(FLOAT, N)} and
 * {@code VECTOR(DOUBLE, N)}.
 * <p>
 * Vectors are serialized as dense, contiguous IEEE 754 big-endian values without header metadata or
 * null masks. Vector dimension is stored in column max length. Vectors do not support primary key
 * usage or ordering comparisons.
 * @param <T> the primitive array type of the vector, {@code float[]} or {@code double[]}
 */
public abstract class PVectorDataType<T> extends PDataType<T> {

  /** Maximum supported vector dimension for column definitions. */
  public static final int MAX_VECTOR_DIMENSION = 65536;

  protected PVectorDataType(String sqlTypeName, int sqlType, Class<T> clazz, int ordinal) {
    super(sqlTypeName, sqlType, clazz, null, ordinal);
  }

  /** Returns the width in bytes of one vector element. */
  public abstract int getElementByteSize();

  /** Returns the scalar type of one vector element. */
  public abstract PDataType getElementType();

  /** Returns the number of elements in a vector of this type. */
  protected abstract int length(T vector);

  /** Allocates a vector of this type with the given dimension. */
  protected abstract T allocate(int dimension);

  /** Sets one element of a vector of this type from a numeric value. */
  protected abstract void set(T vector, int index, Number value);

  /** Returns one element of a vector of this type, boxed. */
  protected abstract Number get(T vector, int index);

  /** Parses one element from its string representation. */
  protected abstract Number parseElement(String s);

  /** Packs a vector into a byte buffer as contiguous big-endian IEEE 754 values. */
  protected abstract void pack(T vector, byte[] buf, int offset);

  /** Unpacks a vector from a packed byte range written with the given sort order. */
  protected abstract T unpack(byte[] buf, int offset, int length, SortOrder sortOrder);

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
  public boolean isComparisonSupported() {
    return false;
  }

  @Override
  public Integer getByteSize() {
    return null;
  }

  @Override
  public int estimateByteSize(Object o) {
    Integer dim = getMaxLength(o);
    return dim == null ? 0 : dim * getElementByteSize();
  }

  @Override
  public Integer estimateByteSizeFromLength(Integer length) {
    return length == null ? null : length * getElementByteSize();
  }

  @Override
  public Integer getMaxLength(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof PhoenixArray) {
      return ((PhoenixArray) o).getDimensions();
    }
    if (o instanceof java.sql.Array) {
      o = getArray((java.sql.Array) o);
    }
    return o != null && o.getClass().isArray() ? Array.getLength(o) : null;
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
    return Bytes.compareTo(toBytes(lhs), rhsType.toBytes(rhs));
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    T vector = toVector(object);
    byte[] bytes = new byte[length(vector) * getElementByteSize()];
    pack(vector, bytes, 0);
    return bytes;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      return 0;
    }
    T vector = toVector(object);
    pack(vector, bytes, offset);
    return length(vector) * getElementByteSize();
  }

  @Override
  public byte[] toBytes(Object object, SortOrder sortOrder) {
    byte[] bytes = toBytes(object);
    if (sortOrder == SortOrder.DESC) {
      SortOrder.invert(bytes, 0, bytes, 0, bytes.length);
    }
    return bytes;
  }

  @Override
  public T toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder,
    Integer maxLength, Integer scale) {
    if (bytes == null || length == 0) {
      return null;
    }
    return unpack(bytes, offset, length, sortOrder == null ? SortOrder.getDefault() : sortOrder);
  }

  @Override
  public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
    SortOrder sortOrder, Integer maxLength, Integer scale, Class jdbcType) throws SQLException {
    if (getJavaClass().isAssignableFrom(jdbcType) || Object.class.equals(jdbcType)) {
      return toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
    }
    boolean boxed = jdbcType.isArray() && !jdbcType.getComponentType().isPrimitive();
    if (boxed || java.sql.Array.class.isAssignableFrom(jdbcType)) {
      T raw = toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
      if (raw == null) {
        return null;
      }
      Object[] elements =
        (Object[]) Array.newInstance(getElementType().getJavaClass(), length(raw));
      for (int i = 0; i < elements.length; i++) {
        elements[i] = get(raw, i);
      }
      return boxed ? elements : PArrayDataType.instantiatePhoenixArray(getElementType(), elements);
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
      return allocate(0);
    }
    String[] parts = str.split(",");
    T vector = allocate(parts.length);
    for (int i = 0; i < parts.length; i++) {
      set(vector, i, parseElement(parts[i].trim()));
    }
    return vector;
  }

  /**
   * Coerces an array or collection into a typed vector. Supports numeric arrays (primitive or
   * boxed), Phoenix arrays, and JDBC arrays. Null elements are prohibited.
   */
  @Override
  public Object toObject(Object object, PDataType actualType) {
    if (object == null) {
      return null;
    }
    if (getJavaClass().isInstance(object)) {
      return object;
    }
    if (object instanceof java.sql.Array) {
      object = getArray((java.sql.Array) object);
    }
    if (object.getClass().isArray()) {
      int dim = Array.getLength(object);
      T vector = allocate(dim);
      for (int i = 0; i < dim; i++) {
        Object e = Array.get(object, i);
        if (e == null) {
          throw new ConstraintViolationException("Vector elements must not be NULL");
        }
        if (!(e instanceof Number)) {
          return throwConstraintViolationException(actualType, this);
        }
        set(vector, i, (Number) e);
      }
      return vector;
    }
    if (object instanceof String) {
      return toObject((String) object);
    }
    if (object instanceof byte[]) {
      byte[] b = (byte[]) object;
      return toObject(b, 0, b.length);
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return this.equals(targetType) || targetType.isArrayType()
      || targetType.equals(PVarchar.INSTANCE) || super.isCastableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return value == null || isCoercibleTo(targetType);
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
    Integer actualDimension = getMaxLength(value);
    if (actualDimension != null) {
      checkDimension(desiredMaxLength, actualDimension);
    }
    if (ptr != null && ptr.getLength() != 0 && srcType == this) {
      checkDimension(desiredMaxLength, ptr.getLength() / getElementByteSize());
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
      checkDimension(desiredMaxLength, ptr.getLength() / getElementByteSize());
    }
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    int dim = maxLength == null ? (arrayLength == null ? 3 : arrayLength) : maxLength;
    T sample = allocate(dim);
    for (int i = 0; i < dim; i++) {
      set(sample, i, RANDOM.get().nextFloat());
    }
    return sample;
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
    if (o == null) {
      return String.valueOf((Object) null);
    }
    T vector = toVector(o);
    StringBuilder buf = new StringBuilder("[");
    for (int i = 0; i < length(vector); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(get(vector, i));
    }
    return buf.append(']').toString();
  }

  @SuppressWarnings("unchecked")
  private T toVector(Object object) {
    return (T) toObject(object, this);
  }

  private static void checkDimension(int expected, int actual) {
    if (actual != expected) {
      throw new ConstraintViolationException(
        "Vector dimension mismatch: expected " + expected + ", but got " + actual);
    }
  }

  private static Object getArray(java.sql.Array array) {
    try {
      return array.getArray();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
