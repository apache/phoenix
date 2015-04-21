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

public class PInteger extends PWholeNumber<Integer> {

  public static final PInteger INSTANCE = new PInteger();

  private PInteger() {
    super("INTEGER", Types.INTEGER, Integer.class, new IntCodec(), 3);
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
    Object o = PLong.INSTANCE.toObject(object, actualType);
    if (!(o instanceof Long) || o == null) {
      return o;
    }
    long l = (Long) o;
    if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
      throw newIllegalDataException(
          actualType + " value " + l + " cannot be cast to Integer without changing its value");
    }
    int v = (int) l;
    return v;
  }

  @Override
  public Integer toObject(byte[] b, int o, int l, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale) {
    if (l == 0) {
      return null;
    }
    if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE, PInteger.INSTANCE,
        PUnsignedInt.INSTANCE, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE, PTinyint.INSTANCE,
        PUnsignedTinyint.INSTANCE, PFloat.INSTANCE, PUnsignedFloat.INSTANCE, PDouble.INSTANCE,
        PUnsignedDouble.INSTANCE)) {
      return actualType.getCodec().decodeInt(b, o, sortOrder);
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
      return bd.intValueExact();
    }
    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value != null) {
      int i;
      if (equalsAny(targetType, PUnsignedDouble.INSTANCE, PUnsignedFloat.INSTANCE,
          PUnsignedLong.INSTANCE, PUnsignedInt.INSTANCE)) {
        i = (Integer) value;
        return i >= 0;
      } else if (targetType.equals(PUnsignedSmallint.INSTANCE)) {
        i = (Integer) value;
        return (i >= 0 && i <= Short.MAX_VALUE);
      } else if (targetType.equals(PSmallint.INSTANCE)) {
        i = (Integer) value;
        return (i >= Short.MIN_VALUE && i <= Short.MAX_VALUE);
      } else if (targetType.equals(PTinyint.INSTANCE)) {
        i = (Integer) value;
        return (i >= Byte.MIN_VALUE && i <= Byte.MAX_VALUE);
      } else if (targetType.equals(PUnsignedTinyint.INSTANCE)) {
        i = (Integer) value;
        return (i >= 0 && i < Byte.MAX_VALUE);
      }
    }
    return super.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return equalsAny(targetType, this, PFloat.INSTANCE) || PLong.INSTANCE.isCoercibleTo(targetType);
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
  public Integer getMaxLength(Object o) {
    return INT_PRECISION;
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
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return RANDOM.get().nextInt();
  }

  static class IntCodec extends BaseCodec {

    @Override
    public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
      return decodeInt(b, o, sortOrder);
    }

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      return decodeInt(b, o, sortOrder);
    }

    @Override
    public double decodeDouble(byte[] b, int o,
        SortOrder sortOrder) {
      return decodeInt(b, o, sortOrder);
    }

    @Override
    public int decodeInt(byte[] bytes, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(bytes, o, Bytes.SIZEOF_INT);
      int v;
      if (sortOrder == SortOrder.ASC) {
        v = bytes[o] ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
          v = (v << 8) + (bytes[o + i] & 0xff);
        }
      } else {
        v = bytes[o] ^ 0xff ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
          v = (v << 8) + ((bytes[o + i] ^ 0xff) & 0xff);
        }
      }
      return v;
    }

    @Override
    public int encodeInt(int v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_INT);
      b[o + 0] = (byte) ((v >> 24) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
      b[o + 1] = (byte) (v >> 16);
      b[o + 2] = (byte) (v >> 8);
      b[o + 3] = (byte) v;
      return Bytes.SIZEOF_INT;
    }

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Integer without changing its value");
      }
      return encodeInt((int) v, b, o);
    }

    @Override
    public int encodeDouble(double v, byte[] b, int o) {
      if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Integer without changing its value");
      }
      return encodeInt((int) v, b, o);
    }

    @Override
    public int encodeLong(long v, byte[] b, int o) {
      if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Integer without changing its value");
      }
      return encodeInt((int) v, b, o);
    }

    @Override
    public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
      int v = decodeInt(b, o, sortOrder);
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Byte without changing its value");
      }
      return (byte) v;
    }

    @Override
    public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
      int v = decodeInt(b, o, sortOrder);
      if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be cast to Short without changing its value");
      }
      return (short) v;
    }

    @Override
    public int encodeByte(byte v, byte[] b, int o) {
      return encodeInt(v, b, o);
    }

    @Override
    public int encodeShort(short v, byte[] b, int o) {
      return encodeInt(v, b, o);
    }

    @Override
    public PhoenixArrayFactory getPhoenixArrayFactory() {
      return new PhoenixArrayFactory() {
        @Override
        public PhoenixArray newArray(PDataType type, Object[] elements) {
          return new PhoenixArray.PrimitiveIntPhoenixArray(type, elements);
        }
      };
    }
  }
}
