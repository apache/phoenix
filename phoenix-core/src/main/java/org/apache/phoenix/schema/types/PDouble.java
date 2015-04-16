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

public class PDouble extends PRealNumber<Double> {
  public static final PDouble INSTANCE = new PDouble();

  private PDouble() {
    super("DOUBLE", Types.DOUBLE, Double.class, new DoubleCodec(), 7);
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
    if (o == null) {
      return null;
    }
    Double v = (Double) o;
    BigDecimal bd = BigDecimal.valueOf(v);
    return bd.scale() == 0 ? null : bd.scale();
  }

  @Override
  public Integer getMaxLength(Object o) {
    if (o == null) {
      return null;
    }
    Double v = (Double) o;
    BigDecimal db = BigDecimal.valueOf(v);
    return db.precision();
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
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    if (object == null) {
      return null;
    }
    double de;
    if (equalsAny(actualType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE)) {
      return object;
    } else if (equalsAny(actualType, PFloat.INSTANCE, PUnsignedFloat.INSTANCE)) {
      de = (Float) object;
      return de;
    } else if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
      de = (Long) object;
      return de;
    } else if (equalsAny(actualType, PInteger.INSTANCE, PUnsignedInt.INSTANCE)) {
      de = (Integer) object;
      return de;
    } else if (equalsAny(actualType, PTinyint.INSTANCE, PUnsignedTinyint.INSTANCE)) {
      de = (Byte) object;
      return de;
    } else if (equalsAny(actualType, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE)) {
      de = (Short) object;
      return de;
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal d = (BigDecimal) object;
      return d.doubleValue();
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public Double toObject(byte[] b, int o, int l, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale) {
    if (l <= 0) {
      return null;
    }
    if (equalsAny(actualType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE, PFloat.INSTANCE,
        PUnsignedFloat.INSTANCE, PLong.INSTANCE, PUnsignedLong.INSTANCE, PInteger.INSTANCE,
        PUnsignedInt.INSTANCE, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE, PTinyint.INSTANCE,
        PUnsignedTinyint.INSTANCE)) {
      return actualType.getCodec().decodeDouble(b, o, sortOrder);
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
      return bd.doubleValue();
    }
    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value != null) {
      double d = (Double) value;
      if (targetType.equals(PUnsignedDouble.INSTANCE)) {
        return d >= 0;
      } else if (targetType.equals(PFloat.INSTANCE)) {
        return Double.isNaN(d)
            || d == Double.POSITIVE_INFINITY
            || d == Double.NEGATIVE_INFINITY
            || (d >= -Float.MAX_VALUE && d <= Float.MAX_VALUE);
      } else if (targetType.equals(PUnsignedFloat.INSTANCE)) {
        return Double.isNaN(d) || d == Double.POSITIVE_INFINITY
            || (d >= 0 && d <= Float.MAX_VALUE);
      } else if (targetType.equals(PUnsignedLong.INSTANCE)) {
        return (d >= 0 && d <= Long.MAX_VALUE);
      } else if (targetType.equals(PLong.INSTANCE)) {
        return (d >= Long.MIN_VALUE && d <= Long.MAX_VALUE);
      } else if (targetType.equals(PUnsignedInt.INSTANCE)) {
        return (d >= 0 && d <= Integer.MAX_VALUE);
      } else if (targetType.equals(PInteger.INSTANCE)) {
        return (d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE);
      } else if (targetType.equals(PUnsignedSmallint.INSTANCE)) {
        return (d >= 0 && d <= Short.MAX_VALUE);
      } else if (targetType.equals(PSmallint.INSTANCE)) {
        return (d >= Short.MIN_VALUE && d <= Short.MAX_VALUE);
      } else if (targetType.equals(PTinyint.INSTANCE)) {
        return (d >= Byte.MIN_VALUE && d < Byte.MAX_VALUE);
      } else if (targetType.equals(PUnsignedTinyint.INSTANCE)) {
        return (d >= 0 && d < Byte.MAX_VALUE);
      }
    }
    return super.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return equalsAny(targetType, this, PDecimal.INSTANCE, PVarbinary.INSTANCE, PBinary.INSTANCE);
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return RANDOM.get().nextDouble();
  }

  static class DoubleCodec extends BaseCodec {

    @Override
    public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
      double v = decodeDouble(b, o, sortOrder);
      if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Long without changing its value");
      }
      return (long) v;
    }

    @Override
    public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
      double v = decodeDouble(b, o, sortOrder);
      if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Integer without changing its value");
      }
      return (int) v;
    }

    @Override
    public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
      double v = decodeDouble(b, o, sortOrder);
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Byte without changing its value");
      }
      return (byte) v;
    }

    @Override
    public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
      double v = decodeDouble(b, o, sortOrder);
      if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Short without changing its value");
      }
      return (short) v;
    }

    @Override
    public double decodeDouble(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
      if (sortOrder == SortOrder.DESC) {
        for (int i = o; i < Bytes.SIZEOF_LONG; i++) {
          b[i] = (byte) (b[i] ^ 0xff);
        }
      }
      long l = Bytes.toLong(b, o);
      l--;
      l ^= (~l >> Long.SIZE - 1) | Long.MIN_VALUE;
      return Double.longBitsToDouble(l);
    }

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      double v = decodeDouble(b, o, sortOrder);
      if (Double.isNaN(v) || v == Double.NEGATIVE_INFINITY
          || v == Double.POSITIVE_INFINITY
          || (v >= -Float.MAX_VALUE && v <= Float.MAX_VALUE)) {
        return (float) v;
      } else {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Float without changing its value");
      }

    }

    @Override
    public int encodeShort(short v, byte[] b, int o) {
      return encodeDouble(v, b, o);
    }

    @Override
    public int encodeLong(long v, byte[] b, int o) {
      return encodeDouble(v, b, o);
    }

    @Override
    public int encodeInt(int v, byte[] b, int o) {
      return encodeDouble(v, b, o);
    }

    @Override
    public int encodeByte(byte v, byte[] b, int o) {
      return encodeDouble(v, b, o);
    }

    @Override
    public int encodeDouble(double v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
      long l = Double.doubleToLongBits(v);
      l = (l ^ ((l >> Long.SIZE - 1) | Long.MIN_VALUE)) + 1;
      Bytes.putLong(b, o, l);
      return Bytes.SIZEOF_LONG;
    }

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      return encodeDouble(v, b, o);
    }

    @Override
    public PhoenixArrayFactory getPhoenixArrayFactory() {
      return new PhoenixArrayFactory() {
        @Override
        public PhoenixArray newArray(PDataType type, Object[] elements) {
          return new PhoenixArray.PrimitiveDoublePhoenixArray(type, elements);
        }
      };
    }
  }
}
