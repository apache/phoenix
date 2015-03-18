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
import java.sql.Types;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

public class PLong extends PWholeNumber<Long> {

  public static final PLong INSTANCE = new PLong();

  private PLong() {
    super("BIGINT", Types.BIGINT, Long.class, new LongCodec(), 2);
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
    if (object == null) {
      return null;
    }
    long s;
    if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
      return object;
    } else if (equalsAny(actualType, PUnsignedInt.INSTANCE,
        PInteger.INSTANCE)) {
      s = (Integer) object;
      return s;
    } else if (equalsAny(actualType, PTinyint.INSTANCE, PUnsignedTinyint.INSTANCE)) {
      s = (Byte) object;
      return s;
    } else if (equalsAny(actualType, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE)) {
      s = (Short) object;
      return s;
    } else if (equalsAny(actualType, PFloat.INSTANCE, PUnsignedFloat.INSTANCE)) {
      Float f = (Float) object;
      if (f > Long.MAX_VALUE || f < Long.MIN_VALUE) {
        throw newIllegalDataException(
            actualType + " value " + f + " cannot be cast to Long without changing its value");
      }
      s = f.longValue();
      return s;
    } else if (equalsAny(actualType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE)) {
      Double de = (Double) object;
      if (de > Long.MAX_VALUE || de < Long.MIN_VALUE) {
        throw newIllegalDataException(
            actualType + " value " + de + " cannot be cast to Long without changing its value");
      }
      s = de.longValue();
      return s;
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal d = (BigDecimal) object;
      return d.longValueExact();
    } else if (equalsAny(actualType, PDate.INSTANCE, PUnsignedDate.INSTANCE, PTime.INSTANCE,
        PUnsignedTime.INSTANCE)) {
      java.util.Date date = (java.util.Date) object;
      return date.getTime();
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public Long toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    if (l == 0) {
      return null;
    }
    if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE,
        PInteger.INSTANCE, PUnsignedInt.INSTANCE, PSmallint.INSTANCE,
        PUnsignedSmallint.INSTANCE, PTinyint.INSTANCE, PUnsignedTinyint.INSTANCE, PFloat.INSTANCE,
        PUnsignedFloat.INSTANCE, PDouble.INSTANCE, PUnsignedDouble.INSTANCE, PDate.INSTANCE,
        PUnsignedDate.INSTANCE, PTime.INSTANCE, PUnsignedTime.INSTANCE)) {
      return actualType.getCodec().decodeLong(b, o, sortOrder);
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
      return bd.longValueExact();
    }
    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    // In general, don't allow conversion of LONG to INTEGER. There are times when
    // we check isComparableTo for a more relaxed check and then throw a runtime
    // exception if we overflow
    return equalsAny(targetType, this, PDecimal.INSTANCE, PVarbinary.INSTANCE, PBinary.INSTANCE, PDouble.INSTANCE);
  }

  @Override
  public boolean isComparableTo(PDataType targetType) {
    return PDecimal.INSTANCE.isComparableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value != null) {
      long l;
      if (equalsAny(targetType, PUnsignedDouble.INSTANCE, PUnsignedFloat.INSTANCE,
          PUnsignedLong.INSTANCE)) {
        l = (Long) value;
        return l >= 0;
      } else if (targetType.equals(PUnsignedInt.INSTANCE)) {
        l = (Long) value;
        return (l >= 0 && l <= Integer.MAX_VALUE);
      } else if (targetType.equals(PInteger.INSTANCE)) {
        l = (Long) value;
        return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE);
      } else if (targetType.equals(PUnsignedSmallint.INSTANCE)) {
        l = (Long) value;
        return (l >= 0 && l <= Short.MAX_VALUE);
      } else if (targetType.equals(PSmallint.INSTANCE)) {
        l = (Long) value;
        return (l >= Short.MIN_VALUE && l <= Short.MAX_VALUE);
      } else if (targetType.equals(PTinyint.INSTANCE)) {
        l = (Long) value;
        return (l >= Byte.MIN_VALUE && l < Byte.MAX_VALUE);
      } else if (targetType.equals(PUnsignedTinyint.INSTANCE)) {
        l = (Long) value;
        return (l >= 0 && l < Byte.MAX_VALUE);
      }
    }
    return super.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return super.isCastableTo(targetType) || targetType.isCoercibleTo(PTimestamp.INSTANCE);
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
  public Integer getMaxLength(Object o) {
    return LONG_PRECISION;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    if (rhsType == PDecimal.INSTANCE) {
      return -((BigDecimal) rhs).compareTo(BigDecimal.valueOf(((Number) lhs).longValue()));
    } else if (equalsAny(rhsType, PDouble.INSTANCE, PFloat.INSTANCE, PUnsignedDouble.INSTANCE, PUnsignedFloat.INSTANCE)) {
      return Doubles.compare(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
    }
    return Longs.compare(((Number) lhs).longValue(), ((Number) rhs).longValue());
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return RANDOM.get().nextLong();
  }

  static class LongCodec extends BaseCodec {

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      return decodeLong(b, o, sortOrder);
    }

    @Override
    public double decodeDouble(byte[] b, int o, SortOrder sortOrder) {
      return decodeLong(b, o, sortOrder);
    }

    @Override
    public long decodeLong(byte[] bytes, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(bytes, o, Bytes.SIZEOF_LONG);
      long v;
      byte b = bytes[o];
      if (sortOrder == SortOrder.ASC) {
        v = b ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
          b = bytes[o + i];
          v = (v << 8) + (b & 0xff);
        }
      } else {
        b = (byte) (b ^ 0xff);
        v = b ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
          b = bytes[o + i];
          b ^= 0xff;
          v = (v << 8) + (b & 0xff);
        }
      }
      return v;
    }

    @Override
    public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
      long v = decodeLong(b, o, sortOrder);
      if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Integer without changing its value");
      }
      return (int) v;
    }

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Long without changing its value");
      }
      return encodeLong((long) v, b, o);
    }

    @Override
    public int encodeDouble(double v, byte[] b, int o) {
      if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Long without changing its value");
      }
      return encodeLong((long) v, b, o);
    }

    @Override
    public int encodeLong(long v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
      b[o + 0] = (byte) ((v >> 56) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
      b[o + 1] = (byte) (v >> 48);
      b[o + 2] = (byte) (v >> 40);
      b[o + 3] = (byte) (v >> 32);
      b[o + 4] = (byte) (v >> 24);
      b[o + 5] = (byte) (v >> 16);
      b[o + 6] = (byte) (v >> 8);
      b[o + 7] = (byte) v;
      return Bytes.SIZEOF_LONG;
    }

    @Override
    public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
      long v = decodeLong(b, o, sortOrder);
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Byte without changing its value");
      }
      return (byte) v;
    }

    @Override
    public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
      long v = decodeLong(b, o, sortOrder);
      if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Short without changing its value");
      }
      return (short) v;
    }

    @Override
    public int encodeByte(byte v, byte[] b, int o) {
      return encodeLong(v, b, o);
    }

    @Override
    public int encodeShort(short v, byte[] b, int o) {
      return encodeLong(v, b, o);
    }

    @Override
    public PhoenixArrayFactory getPhoenixArrayFactory() {
      return new PhoenixArrayFactory() {
        @Override
        public PhoenixArray newArray(PDataType type, Object[] elements) {
          return new PhoenixArray.PrimitiveLongPhoenixArray(type, elements);
        }
      };
    }
  }
}
