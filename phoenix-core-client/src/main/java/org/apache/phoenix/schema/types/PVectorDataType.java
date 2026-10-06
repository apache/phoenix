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
 * Base class for the fixed-dimension vector types {@code VECTOR(FLOAT, N)} and
 * {@code VECTOR(DOUBLE, N)}.
 * <p>
 * The serialized form of a vector is a dense sequence of little-endian IEEE 754 values. It has no
 * header and no null mask. The column max length holds the vector dimension. A vector column cannot
 * be part of a primary key, and vectors do not support order comparisons.
 * @param <T> the primitive array type of the vector, {@code float[]} or {@code double[]}
 */
public abstract class PVectorDataType<T> extends PDataType<T> {

  /** The maximum dimension that a vector column definition can declare. */
  public static final int MAX_VECTOR_DIMENSION = 65536;

  protected PVectorDataType(String sqlTypeName, int sqlType, Class<T> clazz, int ordinal) {
    super(sqlTypeName, sqlType, clazz, null, ordinal);
  }

  /** Returns the size in bytes of one vector element. */
  public abstract int getElementByteSize();

  /** Returns the scalar type of one vector element. */
  public abstract PDataType getElementType();

  /** Returns the number of elements in a vector of this type. */
  protected abstract int length(T vector);

  /** Creates a vector of this type with the given dimension. All elements are zero. */
  protected abstract T allocate(int dimension);

  /** Sets one element of the vector to a numeric value, converted to the element type. */
  protected abstract void set(T vector, int index, Number value);

  /** Returns one element of the vector as a boxed number. */
  protected abstract Number get(T vector, int index);

  /** Parses one element from its string form. */
  protected abstract Number parseElement(String s);

  /** Writes a vector into a byte buffer as a dense sequence of little-endian IEEE 754 values. */
  protected abstract void pack(T vector, byte[] buf, int offset);

  /** Unpacks a vector from a packed byte range that the writer encoded in the given sort order. */
  protected abstract T unpack(byte[] buf, int offset, int length, SortOrder sortOrder);

  /** Returns the index of the first NaN or infinite element, or -1 if all elements are finite. */
  protected abstract int indexOfNonFinite(T vector);

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
    byte[] rhsBytes = rhsType == this ? packed(asVector(rhs)) : rhsType.toBytes(rhs);
    return Bytes.compareTo(packed(asVector(lhs)), rhsBytes);
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    return packed(toVector(object));
  }

  private byte[] packed(T vector) {
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
   * Converts a value to a vector of this type. The value can be a vector of this type, a primitive
   * or boxed numeric array, a Phoenix or JDBC array, or a string. Array elements must be numeric
   * and not null, and all elements must be finite. A value that does not obey these rules causes a
   * {@link ConstraintViolationException}. A string element that is not a number causes a
   * {@link NumberFormatException}.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Object toObject(Object object, PDataType actualType) {
    if (object == null) {
      return null;
    }
    if (getJavaClass().isInstance(object)) {
      return checkFinite((T) object);
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
      return checkFinite(vector);
    }
    if (object instanceof String) {
      return checkFinite((T) toObject((String) object));
    }
    return throwConstraintViolationException(actualType, this);
  }

  /**
   * A vector can be cast only to a type that it can be coerced to. The inherited check uses
   * comparability, which also accepts a cast from a vector to an array. That cast has no
   * conversion, because the coercion from an array to a vector goes in one direction only.
   */
  @Override
  public boolean isCastableTo(PDataType targetType) {
    return isCoercibleTo(targetType);
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
    T vector = asVector(o);
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

  /**
   * Returns a value that is already a vector of this type without change. Thus the render or
   * comparison of a stored vector does not scan or reject its elements again. Other values go
   * through the conversion and finite checks of {@link #toObject(Object, PDataType)}.
   */
  @SuppressWarnings("unchecked")
  private T asVector(Object object) {
    return getJavaClass().isInstance(object) ? (T) object : toVector(object);
  }

  /**
   * Throws a ConstraintViolationException if the vector has a NaN or infinite element, because no
   * distance metric can rank such a vector. A null vector passes.
   */
  private T checkFinite(T vector) {
    int i = vector == null ? -1 : indexOfNonFinite(vector);
    if (i >= 0) {
      throw new ConstraintViolationException(
        "Vector elements must be finite, but element " + i + " is " + get(vector, i));
    }
    return vector;
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
