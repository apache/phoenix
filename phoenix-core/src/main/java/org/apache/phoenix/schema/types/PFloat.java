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

public class PFloat extends PRealNumber<Float> {

  public static final PFloat INSTANCE = new PFloat();

  private PFloat() {
    super("FLOAT", Types.FLOAT, Float.class, new FloatCodec(), 6);
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return PDouble.INSTANCE.compareTo(lhs, rhs, rhsType);
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
    if (o == null) {
      return null;
    }
    Float v = (Float) o;
    BigDecimal bd = BigDecimal.valueOf(v);
    return bd.scale() == 0 ? null : bd.scale();
  }

  @Override
  public Integer getMaxLength(Object o) {
    if (o == null) {
      return null;
    }
    Float v = (Float) o;
    BigDecimal bd = BigDecimal.valueOf(v);
    return bd.precision();
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
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    if (object == null) {
      return null;
    }
    float f;
    if (equalsAny(actualType, PFloat.INSTANCE, PUnsignedFloat.INSTANCE)) {
      return object;
    } else if (equalsAny(actualType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE)) {
      double d = (Double) object;
      if (Double.isNaN(d)
          || d == Double.POSITIVE_INFINITY
          || d == Double.NEGATIVE_INFINITY
          || (d >= -Float.MAX_VALUE && d <= Float.MAX_VALUE)) {
        return (float) d;
      } else {
        throw newIllegalDataException(
            actualType + " value " + d + " cannot be cast to Float without changing its value");
      }
    } else if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
      f = (Long) object;
      return f;
    } else if (equalsAny(actualType, PInteger.INSTANCE, PUnsignedInt.INSTANCE)) {
      f = (Integer) object;
      return f;
    } else if (equalsAny(actualType, PTinyint.INSTANCE, PUnsignedTinyint.INSTANCE)) {
      f = (Byte) object;
      return f;
    } else if (equalsAny(actualType, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE)) {
      f = (Short) object;
      return f;
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal dl = (BigDecimal) object;
      return dl.floatValue();
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public Float toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    if (l <= 0) {
      return null;
    }
    if (equalsAny(actualType, PFloat.INSTANCE, PUnsignedFloat.INSTANCE, PDouble.INSTANCE,
        PUnsignedDouble.INSTANCE, PLong.INSTANCE, PUnsignedLong.INSTANCE, PInteger.INSTANCE,
        PUnsignedInt.INSTANCE, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE, PTinyint.INSTANCE,
        PUnsignedTinyint.INSTANCE)) {
      return actualType.getCodec().decodeFloat(b, o, sortOrder);
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
      return bd.floatValue();
    }

    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value != null) {
      float f = (Float) value;
      if (targetType.equals(PUnsignedFloat.INSTANCE)) {
        return f >= 0;
      } else if (targetType.equals(PUnsignedLong.INSTANCE)) {
        return (f >= 0 && f <= Long.MAX_VALUE);
      } else if (targetType.equals(PLong.INSTANCE)) {
        return (f >= Long.MIN_VALUE && f <= Long.MAX_VALUE);
      } else if (targetType.equals(PUnsignedInt.INSTANCE)) {
        return (f >= 0 && f <= Integer.MAX_VALUE);
      } else if (targetType.equals(PInteger.INSTANCE)) {
        return (f >= Integer.MIN_VALUE && f <= Integer.MAX_VALUE);
      } else if (targetType.equals(PUnsignedSmallint.INSTANCE)) {
        return (f >= 0 && f <= Short.MAX_VALUE);
      } else if (targetType.equals(PSmallint.INSTANCE)) {
        return (f >= Short.MIN_VALUE && f <= Short.MAX_VALUE);
      } else if (targetType.equals(PTinyint.INSTANCE)) {
        return (f >= Byte.MIN_VALUE && f < Byte.MAX_VALUE);
      } else if (targetType.equals(PUnsignedTinyint.INSTANCE)) {
        return (f >= 0 && f < Byte.MAX_VALUE);
      }
    }
    return super.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return this.equals(targetType) || PDouble.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return RANDOM.get().nextFloat();
  }

  static class FloatCodec extends BaseCodec {

    @Override
    public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
      float v = decodeFloat(b, o, sortOrder);
      if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Long without changing its value");
      }
      return (long) v;
    }

    @Override
    public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
      float v = decodeFloat(b, o, sortOrder);
      if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Integer without changing its value");
      }
      return (int) v;
    }

    @Override
    public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
      float v = decodeFloat(b, o, sortOrder);
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Byte without changing its value");
      }
      return (byte) v;
    }

    @Override
    public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
      float v = decodeFloat(b, o, sortOrder);
      if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Short without changing its value");
      }
      return (short) v;
    }

    @Override
    public double decodeDouble(byte[] b, int o,
        SortOrder sortOrder) {
      return decodeFloat(b, o, sortOrder);
    }

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_INT);
      if (sortOrder == SortOrder.DESC) {
        for (int i = o; i < Bytes.SIZEOF_INT; i++) {
          b[i] = (byte) (b[i] ^ 0xff);
        }
      }
      int i = Bytes.toInt(b, o);
      i--;
      i ^= (~i >> Integer.SIZE - 1) | Integer.MIN_VALUE;
      return Float.intBitsToFloat(i);
    }

    @Override
    public int encodeShort(short v, byte[] b, int o) {
      return encodeFloat(v, b, o);
    }

    @Override
    public int encodeLong(long v, byte[] b, int o) {
      return encodeFloat(v, b, o);
    }

    @Override
    public int encodeInt(int v, byte[] b, int o) {
      return encodeFloat(v, b, o);
    }

    @Override
    public int encodeByte(byte v, byte[] b, int o) {
      return encodeFloat(v, b, o);
    }

    @Override
    public int encodeDouble(double v, byte[] b, int o) {
      if (Double.isNaN(v) || v == Double.POSITIVE_INFINITY
          || v == Double.NEGATIVE_INFINITY
          || (v >= -Float.MAX_VALUE && v <= Float.MAX_VALUE)) {
        return encodeFloat((float) v, b, o);
      } else {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Float without changing its value");
      }

    }

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
      int i = Float.floatToIntBits(v);
      i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1;
      Bytes.putInt(b, o, i);
      return Bytes.SIZEOF_FLOAT;
    }

    @Override
    public PhoenixArrayFactory getPhoenixArrayFactory() {
      return new PhoenixArrayFactory() {
        @Override
        public PhoenixArray newArray(PDataType type, Object[] elements) {
          return new PhoenixArray.PrimitiveFloatPhoenixArray(type, elements);
        }
      };
    }
  }
}
