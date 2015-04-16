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
import java.sql.Timestamp;
import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.NumberUtil;

import com.google.common.base.Preconditions;

public class PDecimal extends PRealNumber<BigDecimal> {

  public static final PDecimal INSTANCE = new PDecimal();

  private static final BigDecimal MIN_DOUBLE_AS_BIG_DECIMAL =
      BigDecimal.valueOf(-Double.MAX_VALUE);
  private static final BigDecimal MAX_DOUBLE_AS_BIG_DECIMAL =
      BigDecimal.valueOf(Double.MAX_VALUE);
  private static final BigDecimal MIN_FLOAT_AS_BIG_DECIMAL =
      BigDecimal.valueOf(-Float.MAX_VALUE);
  private static final BigDecimal MAX_FLOAT_AS_BIG_DECIMAL =
      BigDecimal.valueOf(Float.MAX_VALUE);

  private PDecimal() {
    super("DECIMAL", Types.DECIMAL, BigDecimal.class, null, 8);
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    BigDecimal v = (BigDecimal) object;
    v = NumberUtil.normalize(v);
    int len = getLength(v);
    byte[] result = new byte[Math.min(len, MAX_BIG_DECIMAL_BYTES)];
    PDataType.toBytes(v, result, 0, len);
    return result;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      return 0;
    }
    BigDecimal v = (BigDecimal) object;
    v = NumberUtil.normalize(v);
    int len = getLength(v);
    return PDataType.toBytes(v, bytes, offset, len);
  }

  private int getLength(BigDecimal v) {
    int signum = v.signum();
    if (signum == 0) { // Special case for zero
      return 1;
    }
            /*
             * Size of DECIMAL includes:
             * 1) one byte for exponent
             * 2) one byte for terminal byte if negative
             * 3) one byte for every two digits with the following caveats:
             *    a) add one to round up in the case when there is an odd number of digits
             *    b) add one in the case that the scale is odd to account for 10x of lowest significant digit
             *       (basically done to increase the range of exponents that can be represented)
             */
    return (signum < 0 ? 2 : 1) + (v.precision() + 1 + (v.scale() % 2 == 0 ? 0 : 1)) / 2;
  }

  @Override
  public int estimateByteSize(Object o) {
    if (o == null) {
      return 1;
    }
    BigDecimal v = (BigDecimal) o;
    // TODO: should we strip zeros and round here too?
    return Math.min(getLength(v), MAX_BIG_DECIMAL_BYTES);
  }

  @Override
  public Integer getMaxLength(Object o) {
    if (o == null) {
      return MAX_PRECISION;
    }
    BigDecimal v = (BigDecimal) o;
    return v.precision();
  }

  @Override
  public Integer getScale(Object o) {
    return null;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Preconditions.checkNotNull(sortOrder);
    if (l == 0) {
      return null;
    }
    if (actualType == PDecimal.INSTANCE) {
      if (sortOrder == SortOrder.DESC) {
        b = SortOrder.invert(b, o, new byte[l], 0, l);
        o = 0;
      }
      return toBigDecimal(b, o, l);
    } else if (equalsAny(actualType, PDate.INSTANCE, PTime.INSTANCE, PUnsignedDate.INSTANCE,
        PUnsignedTime.INSTANCE, PLong.INSTANCE, PUnsignedLong.INSTANCE, PInteger.INSTANCE,
        PUnsignedInt.INSTANCE, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE, PTinyint.INSTANCE,
        PUnsignedTinyint.INSTANCE)) {
      return BigDecimal.valueOf(actualType.getCodec().decodeLong(b, o, sortOrder));
    } else if (equalsAny(actualType, PFloat.INSTANCE, PUnsignedFloat.INSTANCE)) {
      return BigDecimal.valueOf(actualType.getCodec().decodeFloat(b, o, sortOrder));
    } else if (equalsAny(actualType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE)) {
      return BigDecimal.valueOf(actualType.getCodec().decodeDouble(b, o, sortOrder));
    } else if (equalsAny(actualType, PTimestamp.INSTANCE,
        PUnsignedTimestamp.INSTANCE)) {
      long millisPart = actualType.getCodec().decodeLong(b, o, sortOrder);
      int nanoPart = PUnsignedInt.INSTANCE.getCodec().decodeInt(b, o + Bytes.SIZEOF_LONG, sortOrder);
      BigDecimal nanosPart = BigDecimal.valueOf(
          (nanoPart % QueryConstants.MILLIS_TO_NANOS_CONVERTOR)
              / QueryConstants.MILLIS_TO_NANOS_CONVERTOR);
      return BigDecimal.valueOf(millisPart).add(nanosPart);
    } else if (actualType == PBoolean.INSTANCE) {
      return (Boolean) PBoolean.INSTANCE.toObject(b, o, l, actualType, sortOrder) ?
          BigDecimal.ONE :
          BigDecimal.ZERO;
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    if (object == null) {
      return null;
    }
    if (equalsAny(actualType, PInteger.INSTANCE, PUnsignedInt.INSTANCE)) {
      return BigDecimal.valueOf((Integer) object);
    } else if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
      return BigDecimal.valueOf((Long) object);
    } else if (equalsAny(actualType, PSmallint.INSTANCE, PUnsignedSmallint.INSTANCE)) {
      return BigDecimal.valueOf((Short) object);
    } else if (equalsAny(actualType, PTinyint.INSTANCE, PUnsignedTinyint.INSTANCE)) {
      return BigDecimal.valueOf((Byte) object);
    } else if (equalsAny(actualType, PFloat.INSTANCE, PUnsignedFloat.INSTANCE)) {
      return BigDecimal.valueOf((Float) object);
    } else if (equalsAny(actualType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE)) {
      return BigDecimal.valueOf((Double) object);
    } else if (actualType == PDecimal.INSTANCE) {
      return object;
    } else if (equalsAny(actualType, PDate.INSTANCE, PUnsignedDate.INSTANCE, PTime.INSTANCE,
        PUnsignedTime.INSTANCE)) {
      java.util.Date d = (java.util.Date) object;
      return BigDecimal.valueOf(d.getTime());
    } else if (equalsAny(actualType, PTimestamp.INSTANCE,
        PUnsignedTimestamp.INSTANCE)) {
      Timestamp ts = (Timestamp) object;
      long millisPart = ts.getTime();
      BigDecimal nanosPart = BigDecimal.valueOf(
          (ts.getNanos() % QueryConstants.MILLIS_TO_NANOS_CONVERTOR)
              / QueryConstants.MILLIS_TO_NANOS_CONVERTOR);
      BigDecimal value = BigDecimal.valueOf(millisPart).add(nanosPart);
      return value;
    } else if (actualType == PBoolean.INSTANCE) {
      return ((Boolean) object) ? BigDecimal.ONE : BigDecimal.ZERO;
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public boolean isFixedWidth() {
    return false;
  }

  @Override
  public Integer getByteSize() {
    return MAX_BIG_DECIMAL_BYTES;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    if (rhsType == PDecimal.INSTANCE) {
      return ((BigDecimal) lhs).compareTo((BigDecimal) rhs);
    }
    return -rhsType.compareTo(rhs, lhs, this);
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return super.isCastableTo(targetType) || targetType.isCoercibleTo(
        PTimestamp.INSTANCE) || targetType.equals(PBoolean.INSTANCE);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value != null) {
      BigDecimal bd;
      if (equalsAny(targetType, PUnsignedLong.INSTANCE, PUnsignedInt.INSTANCE,
          PUnsignedSmallint.INSTANCE, PUnsignedTinyint.INSTANCE)) {
        bd = (BigDecimal) value;
        if (bd.signum() == -1) {
          return false;
        }
      } else if (targetType.equals(PLong.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          bd.longValueExact();
          return true;
        } catch (ArithmeticException e) {
          return false;
        }
      } else if (targetType.equals(PInteger.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          bd.intValueExact();
          return true;
        } catch (ArithmeticException e) {
          return false;
        }
      } else if (targetType.equals(PSmallint.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          bd.shortValueExact();
          return true;
        } catch (ArithmeticException e) {
          return false;
        }
      } else if (targetType.equals(PTinyint.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          bd.byteValueExact();
          return true;
        } catch (ArithmeticException e) {
          return false;
        }
      } else if (targetType.equals(PUnsignedFloat.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          BigDecimal maxFloat = MAX_FLOAT_AS_BIG_DECIMAL;
          boolean isNegtive = (bd.signum() == -1);
          return bd.compareTo(maxFloat) <= 0 && !isNegtive;
        } catch (Exception e) {
          return false;
        }
      } else if (targetType.equals(PFloat.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          BigDecimal maxFloat = MAX_FLOAT_AS_BIG_DECIMAL;
          // Float.MIN_VALUE should not be used here, as this is the
          // smallest in terms of closest to zero.
          BigDecimal minFloat = MIN_FLOAT_AS_BIG_DECIMAL;
          return bd.compareTo(maxFloat) <= 0 && bd.compareTo(minFloat) >= 0;
        } catch (Exception e) {
          return false;
        }
      } else if (targetType.equals(PUnsignedDouble.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          BigDecimal maxDouble = MAX_DOUBLE_AS_BIG_DECIMAL;
          boolean isNegtive = (bd.signum() == -1);
          return bd.compareTo(maxDouble) <= 0 && !isNegtive;
        } catch (Exception e) {
          return false;
        }
      } else if (targetType.equals(PDouble.INSTANCE)) {
        bd = (BigDecimal) value;
        try {
          BigDecimal maxDouble = MAX_DOUBLE_AS_BIG_DECIMAL;
          BigDecimal minDouble = MIN_DOUBLE_AS_BIG_DECIMAL;
          return bd.compareTo(maxDouble) <= 0 && bd.compareTo(minDouble) >= 0;
        } catch (Exception e) {
          return false;
        }
      }
    }
    return super.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
      Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
    if (ptr.getLength() == 0) {
      return true;
    }
    // Use the scale from the value if provided, as it prevents a deserialization.
    // The maxLength and scale for the underlying expression are ignored, because they
    // are not relevant in this case: for example a DECIMAL(10,2) may be assigned to a
    // DECIMAL(5,0) as long as the value fits.
    if (value != null) {
      BigDecimal v = (BigDecimal) value;
      maxLength = v.precision();
      scale = v.scale();
    } else {
      int[] v = getDecimalPrecisionAndScale(ptr.get(), ptr.getOffset(), ptr.getLength());
      maxLength = v[0];
      scale = v[1];
    }
    if (desiredMaxLength != null && desiredScale != null && maxLength != null && scale != null &&
        ((desiredScale == null && desiredMaxLength < maxLength) ||
            (desiredMaxLength - desiredScale) < (maxLength - scale))) {
      return false;
    }
    return true;
  }

  @Override
  public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType,
      Integer maxLength, Integer scale, SortOrder actualModifier, Integer desiredMaxLength, Integer desiredScale,
      SortOrder expectedModifier) {
    if (desiredScale == null) {
      // deiredScale not available, or we do not have scale requirement, delegate to parents.
      super.coerceBytes(ptr, object, actualType, maxLength, scale, actualModifier, desiredMaxLength,
          desiredScale, expectedModifier);
      return;
    }
    if (ptr.getLength() == 0) {
      return;
    }
    if (scale == null) {
      if (object != null) {
        BigDecimal v = (BigDecimal) object;
        scale = v.scale();
      } else {
        int[] v = getDecimalPrecisionAndScale(ptr.get(), ptr.getOffset(), ptr.getLength());
        scale = v[1];
      }
    }
    if (this == actualType && scale <= desiredScale) {
      // No coerce and rescale necessary
      return;
    } else {
      BigDecimal decimal;
      // Rescale is necessary.
      if (object != null) { // value object is passed in.
        decimal = (BigDecimal) toObject(object, actualType);
      } else { // only value bytes is passed in, need to convert to object first.
        decimal = (BigDecimal) toObject(ptr);
      }
      decimal = decimal.setScale(desiredScale, BigDecimal.ROUND_DOWN);
      ptr.set(toBytes(decimal));
    }
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      return new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Integer estimateByteSizeFromLength(Integer length) {
    // No association of runtime byte size from decimal precision.
    return null;
  }

  @Override
  public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
    if (formatter == null) {
      BigDecimal o = (BigDecimal) toObject(b, offset, length);
      return o.toPlainString();
    }
    return super.toStringLiteral(b, offset, length, formatter);
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
      if (formatter == null) {
          return ((BigDecimal)o).toPlainString();
        }
        return super.toStringLiteral(o, formatter);
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return new BigDecimal((Long) PLong.INSTANCE.getSampleValue(maxLength, arrayLength));
  }

    // take details from org.apache.phoenix.schema.types.PDataType#toBigDecimal(byte[], int, int)
    @Override
    public int signum(byte[] bytes, int offset, int length, SortOrder sortOrder, Integer maxLength,
            Integer scale) {
        byte signByte;
        if (sortOrder == SortOrder.DESC) {
            signByte = SortOrder.invert(bytes[offset]);
        } else {
            signByte = bytes[offset];
        }
        if (length == 1 && signByte == ZERO_BYTE) {
            return 0;
        }
        return ((signByte & 0x80) == 0) ? -1 : 1;
    }
}
