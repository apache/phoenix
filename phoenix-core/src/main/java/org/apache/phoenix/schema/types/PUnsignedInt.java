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
import org.apache.hadoop.hbase.util.Order;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.base.Preconditions;

/**
 * Unsigned integer type that restricts values to be from 0 to {@link Integer#MAX_VALUE}
 * inclusive. May be used to map to existing HTable values created through
 * {@link org.apache.hadoop.hbase.util.Bytes#toBytes(int)}
 * as long as all values are non negative (the leading sign bit of negative numbers would cause
 * them to sort ahead of positive numbers when they're used as part of the row key when using the
 * HBase utility methods).
 */
public class PUnsignedInt extends PWholeNumber<Integer> {

  public static final PUnsignedInt INSTANCE = new PUnsignedInt();

  private PUnsignedInt() {
    super("UNSIGNED_INT", 9 /* no constant available in Types */, Integer.class,
        new UnsignedIntCodec(), 16);
  }

  @Override
  public boolean isOrderPreserving() {
    return true;
  }

  @Override
  public Order getOrder() {
    return Order.ASCENDING;
  }

  @Override
  public boolean isSkippable() {
    return true;
  }

  @Override
  public Integer getScale(Object o) {
    return ZERO;
  }

  @Override
  public byte[] toBytes(Object object) {
    byte[] b = new byte[Bytes.SIZEOF_INT];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] b, int o) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeInt(((Number) object).intValue(), b, o);
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Integer v = (Integer) PInteger.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Integer v =
        (Integer) PInteger.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return targetType.equals(this) || targetType.equals(PUnsignedFloat.INSTANCE)
        || PUnsignedLong.INSTANCE.isCoercibleTo(targetType)
        || PInteger.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PInteger.INSTANCE
        .isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return Bytes.SIZEOF_INT;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return PLong.INSTANCE.compareTo(lhs, rhs, rhsType);
  }

  @Override
  public boolean isComparableTo(PDataType targetType) {
    return PDecimal.INSTANCE.isComparableTo(targetType);
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      Integer i = Integer.parseInt(value);
      if (i.intValue() < 0) {
        throw newIllegalDataException("Value may not be negative(" + i + ")");
      }
      return i;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public int getResultSetSqlType() {
    return PInteger.INSTANCE.getResultSetSqlType();
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return Math.abs((Integer) PInteger.INSTANCE.getSampleValue(maxLength, arrayLength));
  }

  static class UnsignedIntCodec extends PInteger.IntCodec {

    @Override
    public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_INT);
      if (sortOrder == SortOrder.DESC) {
        b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_INT], 0, Bytes.SIZEOF_INT);
      }
      int v = Bytes.toInt(b, o);
      if (v < 0) {
        throw newIllegalDataException();
      }
      return v;
    }

    @Override
    public int encodeInt(int v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_INT);
      if (v < 0) {
        throw newIllegalDataException();
      }
      Bytes.putInt(b, o, v);
      return Bytes.SIZEOF_INT;
    }
  }
}
