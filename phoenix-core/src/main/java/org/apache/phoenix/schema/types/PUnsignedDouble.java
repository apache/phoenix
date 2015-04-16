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
import org.apache.phoenix.schema.SortOrder;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;

public class PUnsignedDouble extends PRealNumber<PDouble> {

  public static final PUnsignedDouble INSTANCE = new PUnsignedDouble();

  private PUnsignedDouble() {
    super("UNSIGNED_DOUBLE", 15, Double.class, new UnsignedDoubleCodec(), 20);
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    if (rhsType == PDecimal.INSTANCE) {
      return -((BigDecimal) rhs).compareTo(BigDecimal.valueOf(((Number) lhs).doubleValue()));
    }
    return Doubles.compare(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return Bytes.SIZEOF_DOUBLE;
  }

  @Override
  public Integer getScale(Object o) {
    return PDouble.INSTANCE.getScale(o);
  }

  @Override
  public Integer getMaxLength(Object o) {
    return PDouble.INSTANCE.getMaxLength(o);
  }

  @Override
  public byte[] toBytes(Object object) {
    byte[] b = new byte[Bytes.SIZEOF_DOUBLE];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeDouble(((Number) object).doubleValue(),
        bytes, offset);
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      Double d = Double.parseDouble(value);
      if (d.doubleValue() < 0) {
        throw newIllegalDataException("Value may not be negative("
            + d + ")");
      }
      return d;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Double v = (Double) PDouble.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Double v = (Double) PDouble.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PDouble.INSTANCE
        .isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return this.equals(targetType) || PDouble.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public int getResultSetSqlType() {
    return PDouble.INSTANCE.getResultSetSqlType();
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return Math.abs((Double) PDouble.INSTANCE.getSampleValue(maxLength, arrayLength));
  }

  static class UnsignedDoubleCodec extends PDouble.DoubleCodec {

    @Override
    public int encodeDouble(double v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_DOUBLE);
      if (v < 0) {
        throw newIllegalDataException();
      }
      Bytes.putDouble(b, o, v);
      return Bytes.SIZEOF_DOUBLE;
    }

    @Override
    public double decodeDouble(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_DOUBLE);
      if (sortOrder == SortOrder.DESC) {
        b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_DOUBLE], 0, Bytes.SIZEOF_DOUBLE);
      }
      double v = Bytes.toDouble(b, o);
      if (v < 0) {
        throw newIllegalDataException();
      }
      return v;
    }
  }
}
