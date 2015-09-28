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

public class PTinyint extends PWholeNumber<Byte> {

  public static final PTinyint INSTANCE = new PTinyint();

  private PTinyint() {
    super("TINYINT", Types.TINYINT, Byte.class, new ByteCodec(), 5);
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
    return Bytes.SIZEOF_BYTE;
  }

  @Override
  public Integer getMaxLength(Object o) {
    return BYTE_PRECISION;
  }

  @Override
  public byte[] toBytes(Object object) {
    byte[] b = new byte[Bytes.SIZEOF_BYTE];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeByte(((Number) object).byteValue(), bytes, offset);
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      Byte b = Byte.parseByte(value);
      return b;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Object o = PLong.INSTANCE.toObject(object, actualType);
    if (!(o instanceof Long) || o == null) {
      return o;
    }
    long l = (Long) o;
    if (l < Byte.MIN_VALUE || l > Byte.MAX_VALUE) {
      throw newIllegalDataException(
          actualType + " value " + l + " cannot be cast to Byte without changing its value");
    }
    return (byte) l;
  }

  @Override
  public Byte toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    if (l == 0) {
      return null;
    }
    if (equalsAny(actualType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE, PFloat.INSTANCE,
        PUnsignedFloat.INSTANCE, PLong.INSTANCE, PUnsignedLong.INSTANCE, PInteger.INSTANCE,
        PUnsignedInt.INSTANCE, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE, PTinyint.INSTANCE,
        PUnsignedTinyint.INSTANCE)) {
      return actualType.getCodec().decodeByte(b, o, sortOrder);
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
      return bd.byteValueExact();
    }
    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value != null) {
      if (equalsAny(targetType, PUnsignedDouble.INSTANCE, PUnsignedFloat.INSTANCE,
          PUnsignedLong.INSTANCE, PUnsignedInt.INSTANCE, PUnsignedSmallint.INSTANCE,
          PUnsignedTinyint.INSTANCE)) {
        byte i = (Byte) value;
        return i >= 0;
      }
    }
    return super.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return this.equals(targetType) || PSmallint.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return ((Integer) PInteger.INSTANCE.getSampleValue(maxLength, arrayLength))
        .byteValue();
  }

  static class ByteCodec extends BaseCodec {

    @Override
    public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
      return decodeByte(b, o, sortOrder);
    }

    @Override
    public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
      return decodeByte(b, o, sortOrder);
    }

    @Override
    public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_BYTE);
      int v;
      if (sortOrder == SortOrder.ASC) {
        v = b[o] ^ 0x80; // Flip sign bit back
      } else {
        v = b[o] ^ 0xff ^ 0x80; // Flip sign bit back
      }
      return (byte) v;
    }

    @Override
    public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
      return decodeByte(b, o, sortOrder);
    }

    @Override
    public int encodeShort(short v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_BYTE);
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Byte without changing its value");
      }
      return encodeByte((byte) v, b, o);
    }

    @Override
    public int encodeLong(long v, byte[] b, int o) {
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Byte without changing its value");
      }
      return encodeByte((byte) v, b, o);
    }

    @Override
    public int encodeInt(int v, byte[] b, int o) {
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Byte without changing its value");
      }
      return encodeByte((byte) v, b, o);
    }

    @Override
    public int encodeByte(byte v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_BYTE);
      b[o] = (byte) (v ^ 0x80); // Flip sign bit so that Short is binary comparable
      return Bytes.SIZEOF_BYTE;
    }

    @Override
    public double decodeDouble(byte[] b, int o, SortOrder sortOrder) {
      return decodeByte(b, o, sortOrder);
    }

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      return decodeByte(b, o, sortOrder);
    }

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Byte without changing its value");
      }
      return encodeByte((byte) v, b, o);
    }

    @Override
    public int encodeDouble(double v, byte[] b, int o) {
      if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
        throw newIllegalDataException(
            "Value " + v + " cannot be encoded as an Byte without changing its value");
      }
      return encodeByte((byte) v, b, o);
    }

    @Override
    public PhoenixArrayFactory getPhoenixArrayFactory() {
      return new PhoenixArrayFactory() {
        @Override
        public PhoenixArray newArray(PDataType type, Object[] elements) {
          return new PhoenixArray.PrimitiveBytePhoenixArray(type, elements);
        }
      };
    }
  }
}
