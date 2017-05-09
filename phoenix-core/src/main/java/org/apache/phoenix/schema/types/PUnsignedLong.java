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

import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

/**
 * Unsigned long type that restricts values to be from 0 to {@link Long#MAX_VALUE}
 * inclusive. May be used to map to existing HTable values created through
 * {@link org.apache.hadoop.hbase.util.Bytes#toBytes(long)}
 * as long as all values are non negative (the leading sign bit of negative numbers would cause
 * them to sort ahead of positive numbers when they're used as part of the row key when using the
 * HBase utility methods).
 */
public class PUnsignedLong extends PWholeNumber<Long> {

  public static final PUnsignedLong INSTANCE = new PUnsignedLong();

  private PUnsignedLong() {
    super("UNSIGNED_LONG", 10 /* no constant available in Types */, Long.class,
        new UnsignedLongCodec(), 15);
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
    byte[] b = new byte[Bytes.SIZEOF_LONG];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] b, int o) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeLong(((Number) object).longValue(), b, o);
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Long v = (Long) PLong.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Long v = (Long) PLong.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
    public boolean isCastableTo(PDataType targetType) {
      return super.isCastableTo(targetType) || targetType.isCoercibleTo(PTimestamp.INSTANCE);
    }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return targetType == this || targetType == PUnsignedDouble.INSTANCE || PLong.INSTANCE
        .isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PLong.INSTANCE.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return Bytes.SIZEOF_LONG;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    if (rhsType == PDecimal.INSTANCE) {
      return -((BigDecimal) rhs).compareTo(BigDecimal.valueOf(((Number) lhs).longValue()));
    } else if (equalsAny(rhsType, PDouble.INSTANCE, PFloat.INSTANCE, PUnsignedDouble.INSTANCE,
        PUnsignedFloat.INSTANCE)) {
      return Doubles.compare(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
    }
    return Longs.compare(((Number) lhs).longValue(), ((Number) rhs).longValue());
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
      Long l = Long.parseLong(value);
      if (l.longValue() < 0) {
        throw newIllegalDataException("Value may not be negative(" + l + ")");
      }
      return l;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public int getResultSetSqlType() {
    return PLong.INSTANCE.getResultSetSqlType();
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return Math.abs((Long) PLong.INSTANCE.getSampleValue(maxLength, arrayLength));
  }

  static class UnsignedLongCodec extends PLong.LongCodec {

    @Override
    public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
      long v = 0;
      if (sortOrder == SortOrder.ASC) {
        for (int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
          v <<= 8;
          v ^= b[i] & 0xFF;
        }
      } else {
        for (int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
          v <<= 8;
          v ^= (b[i] & 0xFF) ^ 0xFF;
        }
      }
      if (v < 0) {
        throw newIllegalDataException();
      }
      return v;
    }

    @Override
    public int encodeLong(long v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
      if (v < 0) {
        throw newIllegalDataException();
      }
      Bytes.putLong(b, o, v);
      return Bytes.SIZEOF_LONG;
    }
  }
}
