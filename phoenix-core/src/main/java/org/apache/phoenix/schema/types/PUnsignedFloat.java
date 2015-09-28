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

public class PUnsignedFloat extends PRealNumber<PFloat> {

  public static final PUnsignedFloat INSTANCE = new PUnsignedFloat();

  private PUnsignedFloat() {
    super("UNSIGNED_FLOAT", 14, Float.class, new UnsignedFloatCodec(), 19);
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return PFloat.INSTANCE.compareTo(lhs, rhs, rhsType);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return Bytes.SIZEOF_FLOAT;
  }

  @Override
  public Integer getScale(Object o) {
    return PFloat.INSTANCE.getScale(o);
  }

  @Override
  public Integer getMaxLength(Object o) {
    return PFloat.INSTANCE.getMaxLength(o);
  }

  @Override
  public byte[] toBytes(Object object) {
    byte[] b = new byte[Bytes.SIZEOF_FLOAT];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeFloat(((Number) object).floatValue(),
        bytes, offset);
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      Float f = Float.parseFloat(value);
      if (f.floatValue() < 0) {
        throw newIllegalDataException("Value may not be negative("
            + f + ")");
      }
      return f;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Float v = (Float) PFloat.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Float v = (Float) PFloat.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType) || PFloat.INSTANCE.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return this.equals(targetType) || PUnsignedDouble.INSTANCE.isCoercibleTo(targetType) || PFloat.INSTANCE
        .isCoercibleTo(targetType);
  }

  @Override
  public int getResultSetSqlType() {
    return PFloat.INSTANCE.getResultSetSqlType();
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return Math.abs((Float) PFloat.INSTANCE.getSampleValue(maxLength, arrayLength));
  }

  static class UnsignedFloatCodec extends PFloat.FloatCodec {

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
      if (v < 0) {
        throw newIllegalDataException();
      }
      Bytes.putFloat(b, o, v);
      return Bytes.SIZEOF_FLOAT;
    }

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
      if (sortOrder == SortOrder.DESC) {
        b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_FLOAT], 0, Bytes.SIZEOF_FLOAT);
      }
      float v = Bytes.toFloat(b, o);
      if (v < 0) {
        throw newIllegalDataException();
      }
      return v;
    }
  }
}
