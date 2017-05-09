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

public class PSmallint extends PWholeNumber<Short> {

  public static final PSmallint INSTANCE = new PSmallint();

  private PSmallint() {
    super("SMALLINT", Types.SMALLINT, Short.class, new ShortCodec(), 4);
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
    public boolean isComparableTo(PDataType targetType) {
      return PDecimal.INSTANCE.isComparableTo(targetType);
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
      byte[] b = new byte[Bytes.SIZEOF_SHORT];
      toBytes(object, b, 0);
      return b;
    }

    @Override
    public int toBytes(Object object, byte[] bytes, int offset) {
      if (object == null) {
        throw newIllegalDataException(this + " may not be null");
      }
      return this.getCodec().encodeShort(((Number)object).shortValue(), bytes, offset);
    }

    @Override
    public Object toObject(Object object, PDataType actualType) {
      Object o = PLong.INSTANCE.toObject(object, actualType);
      if (!(o instanceof Long) || o == null) {
        return o;
      }
      long l = (Long)o;
      if (l < Short.MIN_VALUE || l > Short.MAX_VALUE) {
        throw newIllegalDataException(actualType + " value " + l + " cannot be cast to Short without changing its value");
      }
      short s = (short)l;
      return s;
    }

    @Override
    public Short toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
      if (l == 0) {
        return null;
      }
      if (equalsAny(actualType, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE, PTinyint.INSTANCE,
          PUnsignedTinyint.INSTANCE, PLong.INSTANCE, PUnsignedLong.INSTANCE, PInteger.INSTANCE,
          PUnsignedInt.INSTANCE, PFloat.INSTANCE, PUnsignedFloat.INSTANCE, PDouble.INSTANCE,
          PUnsignedDouble.INSTANCE)) {
        return actualType.getCodec().decodeShort(b, o, sortOrder);
      } else if (actualType == PDecimal.INSTANCE) {
        BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
        return bd.shortValueExact();
      }
      throwConstraintViolationException(actualType,this);
      return null;
    }

    @Override
    public Object toObject(String value) {
      if (value == null || value.length() == 0) {
        return null;
      }
      try {
        return Short.parseShort(value);
      } catch (NumberFormatException e) {
        throw newIllegalDataException(e);
      }
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType, Object value) {
      if (value != null) {
        short i;
        if (equalsAny(targetType, PUnsignedDouble.INSTANCE, PUnsignedFloat.INSTANCE,
            PUnsignedLong.INSTANCE, PUnsignedInt.INSTANCE, PUnsignedSmallint.INSTANCE)) {
          i = (Short) value;
          return i >= 0;
        } else if (targetType == PUnsignedTinyint.INSTANCE) {
          i = (Short) value;
          return (i >= 0 && i <= Byte.MAX_VALUE);
        } else if (targetType == PTinyint.INSTANCE) {
          i = (Short) value;
          return (i >= Byte.MIN_VALUE && i <= Byte.MAX_VALUE);
        }
      }
      return super.isCoercibleTo(targetType, value);
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType) {
      return this.equals(targetType) || PInteger.INSTANCE.isCoercibleTo(targetType);
    }

    @Override
    public Object getSampleValue(Integer maxLength, Integer arrayLength) {
      return ((Integer) PInteger.INSTANCE.getSampleValue(maxLength, arrayLength)).shortValue();
    }

  static class ShortCodec extends BaseCodec {

      @Override
      public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
        return decodeShort(b, o, sortOrder);
      }

      @Override
      public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
        return decodeShort(b, o, sortOrder);
      }

      @Override
      public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
        short v = decodeShort(b, o, sortOrder);
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
            throw newIllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
        }
        return (byte)v;
      }

      @Override
      public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
    	Preconditions.checkNotNull(sortOrder);
        checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
        int v;
        if (sortOrder == SortOrder.ASC) {
            v = b[o] ^ 0x80; // Flip sign bit back
            for (int i = 1; i < Bytes.SIZEOF_SHORT; i++) {
                v = (v << 8) + (b[o + i] & 0xff);
            }
        } else {
            v = b[o] ^ 0xff ^ 0x80; // Flip sign bit back
            for (int i = 1; i < Bytes.SIZEOF_SHORT; i++) {
                v = (v << 8) + ((b[o + i] ^ 0xff) & 0xff);
            }
        }
        return (short)v;
      }

      @Override
      public int encodeShort(short v, byte[] b, int o) {
          checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
          b[o + 0] = (byte) ((v >> 8) ^ 0x80); // Flip sign bit so that Short is binary comparable
          b[o + 1] = (byte) v;
          return Bytes.SIZEOF_SHORT;
      }

      @Override
      public int encodeLong(long v, byte[] b, int o) {
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw newIllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
          }
          return encodeShort((short)v,b,o);
      }

      @Override
      public int encodeInt(int v, byte[] b, int o) {
        if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
          throw newIllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
        }
        return encodeShort((short)v,b,o);
      }

      @Override
      public int encodeByte(byte v, byte[] b, int o) {
        return encodeShort(v,b,o);
      }

      @Override
      public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
          return decodeShort(b, o, sortOrder);
      }

      @Override
      public double decodeDouble(byte[] b, int o,
              SortOrder sortOrder) {
          return decodeShort(b, o, sortOrder);
      }

      @Override
      public int encodeDouble(double v, byte[] b, int o) {
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw newIllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
          }
          return encodeShort((short)v,b,o);
      }

      @Override
      public int encodeFloat(float v, byte[] b, int o) {
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw newIllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
          }
          return encodeShort((short)v,b,o);
      }

      @Override
      public PhoenixArrayFactory getPhoenixArrayFactory() {
          return new PhoenixArrayFactory() {
              @Override
              public PhoenixArray newArray(PDataType type, Object[] elements) {
                  return new PhoenixArray.PrimitiveShortPhoenixArray(type, elements);
              }
          };
      }
    }
}
