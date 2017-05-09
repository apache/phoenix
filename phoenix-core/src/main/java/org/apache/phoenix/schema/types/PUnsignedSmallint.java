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

public class PUnsignedSmallint extends PWholeNumber<Short> {

  public static final PUnsignedSmallint INSTANCE = new PUnsignedSmallint();

  private PUnsignedSmallint() {
    super("UNSIGNED_SMALLINT", 13, Short.class, new UnsignedShortCodec(), 17);
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
    return Bytes.SIZEOF_SHORT;
  }

  @Override
  public Integer getMaxLength(Object o) {
    return SHORT_PRECISION;
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    byte[] b = new byte[Bytes.SIZEOF_SHORT];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeShort(((Number) object).shortValue(), bytes, offset);
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      Short b = Short.parseShort(value);
      if (b.shortValue() < 0) {
        throw newIllegalDataException("Value may not be negative(" + b + ")");
      }
      return b;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Short v = (Short) PSmallint.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Short v = (Short) PSmallint.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public boolean isComparableTo(PDataType targetType) {
    return PDecimal.INSTANCE.isComparableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return targetType.equals(this) || PUnsignedInt.INSTANCE.isCoercibleTo(targetType) || PSmallint.INSTANCE
        .isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PSmallint.INSTANCE
        .isCoercibleTo(targetType, value);
  }

  @Override
  public int getResultSetSqlType() {
    return PSmallint.INSTANCE.getResultSetSqlType();
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return ((Integer) RANDOM.get().nextInt(Short.MAX_VALUE)).shortValue();
  }

  static class UnsignedShortCodec extends PSmallint.ShortCodec {

    @Override
    public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
      if (sortOrder == SortOrder.DESC) {
        b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_SHORT], 0, Bytes.SIZEOF_SHORT);
      }
      short v = Bytes.toShort(b, o);
      if (v < 0) {
        throw newIllegalDataException();
      }
      return v;
    }

    @Override
    public int encodeShort(short v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
      if (v < 0) {
        throw newIllegalDataException();
      }
      Bytes.putShort(b, o, v);
      return Bytes.SIZEOF_SHORT;
    }
  }
}
