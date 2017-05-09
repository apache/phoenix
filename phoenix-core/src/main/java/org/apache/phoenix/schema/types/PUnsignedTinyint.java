/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema.types;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.base.Preconditions;

public class PUnsignedTinyint extends PWholeNumber<Byte> {

  public static final PUnsignedTinyint INSTANCE = new PUnsignedTinyint();

  private PUnsignedTinyint() {
    super("UNSIGNED_TINYINT", 11, Byte.class, new UnsignedByteCodec(), 18);
  }

  @Override
  public Integer getScale(Object o) {
    return ZERO;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return PLong.INSTANCE.compareTo(lhs, rhs, rhsType);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return Bytes.SIZEOF_BYTE;
  }

  @Override
  public Integer getMaxLength(Object o) {
    return BYTE_PRECISION;
  }

  @Override
  public byte[] toBytes(Object object) {
    byte[] b = new byte[Bytes.SIZEOF_BYTE];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeByte(((Number) object).byteValue(), bytes, offset);
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      Byte b = Byte.parseByte(value);
      if (b.byteValue() < 0) {
        throw newIllegalDataException("Value may not be negative(" + b + ")");
      }
      return b;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Byte v = (Byte) PTinyint.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Byte v = (Byte) PTinyint.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return targetType.equals(this) || PUnsignedSmallint.INSTANCE.isCoercibleTo(targetType)
        || PTinyint.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PTinyint.INSTANCE
        .isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isComparableTo(PDataType targetType) {
    return PDecimal.INSTANCE.isComparableTo(targetType);
  }

  @Override
  public int getResultSetSqlType() {
    return PTinyint.INSTANCE.getResultSetSqlType();
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return ((Integer) RANDOM.get().nextInt(Byte.MAX_VALUE)).byteValue();
  }

  static class UnsignedByteCodec extends PTinyint.ByteCodec {

    @Override
    public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_BYTE);
      byte v = b[o];
      if (sortOrder == SortOrder.DESC) {
        v = SortOrder.invert(v);
      }
      if (v < 0) {
        throw newIllegalDataException();
      }
      return v;
    }

    @Override
    public int encodeByte(byte v, byte[] b, int o) {
      if (v < 0) {
        throw newIllegalDataException();
      }
      Bytes.putByte(b, o, v);
      return Bytes.SIZEOF_BYTE;
    }
  }
}
