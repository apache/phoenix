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
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.Format;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.types.DataType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

/**
 * The data types of PColumns
 */
public abstract class PDataType<T> implements DataType<T>, Comparable<PDataType<?>> {

  private final String sqlTypeName;
  private final int sqlType;
  private final Class clazz;
  private final byte[] clazzNameBytes;
  private final byte[] sqlTypeNameBytes;
  private final PDataCodec codec;
  private final int ordinal;

  protected PDataType(String sqlTypeName, int sqlType, Class clazz, PDataCodec codec, int ordinal) {
    this.sqlTypeName = sqlTypeName;
    this.sqlType = sqlType;
    this.clazz = clazz;
    this.clazzNameBytes = Bytes.toBytes(clazz.getName());
    this.sqlTypeNameBytes = Bytes.toBytes(sqlTypeName);
    this.codec = codec;
    this.ordinal = ordinal;
  }

  @Deprecated
  public static PDataType[] values() {
    return PDataTypeFactory.getInstance().getOrderedTypes();
  }

  @Deprecated
  public int ordinal() {
    return ordinal;
  }

  @Override
  public Class<T> encodedClass() {
    return getJavaClass();
  }

  public boolean isCastableTo(PDataType targetType) {
    return isComparableTo(targetType);
  }

  public final PDataCodec getCodec() {
    return codec;
  }

  public boolean isBytesComparableWith(PDataType otherType) {
    return this == otherType
        || this.getClass() == PVarbinary.class
        || otherType == PVarbinary.INSTANCE
        || this.getClass() == PBinary.class
        || otherType == PBinary.INSTANCE;
  }

  public int estimateByteSize(Object o) {
    if (isFixedWidth()) {
      return getByteSize();
    }
    if (isArrayType()) {
      PhoenixArray array = (PhoenixArray) o;
      int noOfElements = array.numElements;
      int totalVarSize = 0;
      for (int i = 0; i < noOfElements; i++) {
        totalVarSize += array.estimateByteSize(i);
      }
      return totalVarSize;
    }
    // Non fixed width types must override this
    throw new UnsupportedOperationException();
  }

  public Integer getMaxLength(Object o) {
    return null;
  }

  public Integer getScale(Object o) {
    return null;
  }

  /**
   * Estimate the byte size from the type length. For example, for char, byte size would be the
   * same as length. For decimal, byte size would have no correlation with the length.
   */
  public Integer estimateByteSizeFromLength(Integer length) {
    if (isFixedWidth()) {
      return getByteSize();
    }
    if (isArrayType()) {
      return null;
    }
    // If not fixed width, default to say the byte size is the same as length.
    return length;
  }

  public final String getSqlTypeName() {
    return sqlTypeName;
  }

  public final int getSqlType() {
    return sqlType;
  }

  public final Class getJavaClass() {
    return clazz;
  }

  public boolean isArrayType() {
    return false;
  }

  public final int compareTo(byte[] lhs, int lhsOffset, int lhsLength, SortOrder lhsSortOrder,
      byte[] rhs, int rhsOffset, int rhsLength, SortOrder rhsSortOrder,
      PDataType rhsType) {
    Preconditions.checkNotNull(lhsSortOrder);
    Preconditions.checkNotNull(rhsSortOrder);
    if (this.isBytesComparableWith(rhsType)) { // directly compare the bytes
      return compareTo(lhs, lhsOffset, lhsLength, lhsSortOrder, rhs, rhsOffset, rhsLength,
          rhsSortOrder);
    }
    PDataCodec lhsCodec = this.getCodec();
    if (lhsCodec
        == null) { // no lhs native type representation, so convert rhsType to bytes representation of lhsType
      byte[] rhsConverted =
          this.toBytes(this.toObject(rhs, rhsOffset, rhsLength, rhsType, rhsSortOrder));
      if (rhsSortOrder == SortOrder.DESC) {
        rhsSortOrder = SortOrder.ASC;
      }
      if (lhsSortOrder == SortOrder.DESC) {
        lhs = SortOrder.invert(lhs, lhsOffset, new byte[lhsLength], 0, lhsLength);
      }
      return Bytes.compareTo(lhs, lhsOffset, lhsLength, rhsConverted, 0, rhsConverted.length);
    }
    PDataCodec rhsCodec = rhsType.getCodec();
    if (rhsCodec == null) {
      byte[] lhsConverted =
          rhsType.toBytes(rhsType.toObject(lhs, lhsOffset, lhsLength, this, lhsSortOrder));
      if (lhsSortOrder == SortOrder.DESC) {
        lhsSortOrder = SortOrder.ASC;
      }
      if (rhsSortOrder == SortOrder.DESC) {
        rhs = SortOrder.invert(rhs, rhsOffset, new byte[rhsLength], 0, rhsLength);
      }
      return Bytes.compareTo(lhsConverted, 0, lhsConverted.length, rhs, rhsOffset, rhsLength);
    }
    // convert to native and compare
    if (this.isCoercibleTo(PLong.INSTANCE)
        && rhsType
        .isCoercibleTo(PLong.INSTANCE)) { // native long to long comparison
      return Longs.compare(this.getCodec().decodeLong(lhs, lhsOffset, lhsSortOrder),
          rhsType.getCodec().decodeLong(rhs, rhsOffset, rhsSortOrder));
    } else if (isDoubleOrFloat(this) && isDoubleOrFloat(
        rhsType)) { // native double to double comparison
      return Doubles.compare(this.getCodec().decodeDouble(lhs, lhsOffset, lhsSortOrder),
          rhsType.getCodec().decodeDouble(rhs, rhsOffset, rhsSortOrder));
    } else { // native float/double to long comparison
      float fvalue = 0.0F;
      double dvalue = 0.0;
      long lvalue = 0;
      boolean isFloat = false;
      int invert = 1;

      if (this.isCoercibleTo(PLong.INSTANCE)) {
        lvalue = this.getCodec().decodeLong(lhs, lhsOffset, lhsSortOrder);
      } else if (this.getClass() == PFloat.class) {
        isFloat = true;
        fvalue = this.getCodec().decodeFloat(lhs, lhsOffset, lhsSortOrder);
      } else if (this.isCoercibleTo(PDouble.INSTANCE)) {
        dvalue = this.getCodec().decodeDouble(lhs, lhsOffset, lhsSortOrder);
      }
      if (rhsType.isCoercibleTo(PLong.INSTANCE)) {
        lvalue = rhsType.getCodec().decodeLong(rhs, rhsOffset, rhsSortOrder);
      } else if (rhsType == PFloat.INSTANCE) {
        invert = -1;
        isFloat = true;
        fvalue = rhsType.getCodec().decodeFloat(rhs, rhsOffset, rhsSortOrder);
      } else if (rhsType.isCoercibleTo(PDouble.INSTANCE)) {
        invert = -1;
        dvalue = rhsType.getCodec().decodeDouble(rhs, rhsOffset, rhsSortOrder);
      }
      // Invert the comparison if float/double value is on the RHS
      return invert * (isFloat ?
          compareFloatToLong(fvalue, lvalue) :
          compareDoubleToLong(dvalue, lvalue));
    }
  }

  public static boolean isDoubleOrFloat(PDataType type) {
    return type == PFloat.INSTANCE
        || type == PDouble.INSTANCE
        || type == PUnsignedFloat.INSTANCE || type == PUnsignedDouble.INSTANCE;
  }

  /**
   * Compares a float against a long. Behaves better than
   * {@link #compareDoubleToLong(double, long)} for float
   * values outside of Integer.MAX_VALUE and Integer.MIN_VALUE.
   *
   * @param f a float value
   * @param l a long value
   * @return -1 if f is less than l, 1 if f is greater than l, and 0 if f is equal to l
   */
  private static int compareFloatToLong(float f, long l) {
    if (f > Integer.MAX_VALUE || f < Integer.MIN_VALUE) {
      return f < l ? -1 : f > l ? 1 : 0;
    }
    long diff = (long) f - l;
    return Long.signum(diff);
  }

  /**
   * Compares a double against a long.
   *
   * @param d a double value
   * @param l a long value
   * @return -1 if d is less than l, 1 if d is greater than l, and 0 if d is equal to l
   */
  private static int compareDoubleToLong(double d, long l) {
    if (d > Long.MAX_VALUE) {
      return 1;
    }
    if (d < Long.MIN_VALUE) {
      return -1;
    }
    long diff = (long) d - l;
    return Long.signum(diff);
  }

  protected static void checkForSufficientLength(byte[] b, int offset, int requiredLength) {
    if (b.length < offset + requiredLength) {
      throw new RuntimeException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
          .setMessage(
              "Expected length of at least " + requiredLength + " bytes, but had " + (b.length
                  - offset)).build().buildException());
    }
  }

  protected static Void throwConstraintViolationException(PDataType source, PDataType target) {
    throw new ConstraintViolationException(
        new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_MISMATCH)
            .setMessage(source + " cannot be coerced to " + target).build().buildException());
  }

  protected static RuntimeException newIllegalDataException() {
    return new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
        .build().buildException());
  }

  protected static RuntimeException newIllegalDataException(String msg) {
    return new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
        .setMessage(msg).build().buildException());
  }

  protected static RuntimeException newIllegalDataException(Exception e) {
    return new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
        .setRootCause(e).build().buildException());
  }

  @Override
  public boolean equals(Object o) {
    // PDataType's are expected to be singletons.
    // TODO: this doesn't jive with HBase's DataType
    if (o == null) return false;
    return getClass() == o.getClass();
  }

  /**
   * @return true when {@code lhs} equals any of {@code rhs}.
   */
  public static boolean equalsAny(PDataType lhs, PDataType... rhs) {
    for (int i = 0; i < rhs.length; i++) {
      if (lhs.equals(rhs[i])) return true;
    }
    return false;
  }

  public static interface PDataCodec {
    public long decodeLong(ImmutableBytesWritable ptr, SortOrder sortOrder);

    public long decodeLong(byte[] b, int o, SortOrder sortOrder);

    public int decodeInt(ImmutableBytesWritable ptr, SortOrder sortOrder);

    public int decodeInt(byte[] b, int o, SortOrder sortOrder);

    public byte decodeByte(ImmutableBytesWritable ptr, SortOrder sortOrder);

    public byte decodeByte(byte[] b, int o, SortOrder sortOrder);

    public short decodeShort(ImmutableBytesWritable ptr, SortOrder sortOrder);

    public short decodeShort(byte[] b, int o, SortOrder sortOrder);

    public float decodeFloat(ImmutableBytesWritable ptr, SortOrder sortOrder);

    public float decodeFloat(byte[] b, int o, SortOrder sortOrder);

    public double decodeDouble(ImmutableBytesWritable ptr, SortOrder sortOrder);

    public double decodeDouble(byte[] b, int o, SortOrder sortOrder);

    public int encodeLong(long v, ImmutableBytesWritable ptr);

    public int encodeLong(long v, byte[] b, int o);

    public int encodeInt(int v, ImmutableBytesWritable ptr);

    public int encodeInt(int v, byte[] b, int o);

    public int encodeByte(byte v, ImmutableBytesWritable ptr);

    public int encodeByte(byte v, byte[] b, int o);

    public int encodeShort(short v, ImmutableBytesWritable ptr);

    public int encodeShort(short v, byte[] b, int o);

    public int encodeFloat(float v, ImmutableBytesWritable ptr);

    public int encodeFloat(float v, byte[] b, int o);

    public int encodeDouble(double v, ImmutableBytesWritable ptr);

    public int encodeDouble(double v, byte[] b, int o);

    public PhoenixArrayFactory getPhoenixArrayFactory();
  }

  public static abstract class BaseCodec implements PDataCodec {
    @Override
    public int decodeInt(ImmutableBytesWritable ptr, SortOrder sortOrder) {
      return decodeInt(ptr.get(), ptr.getOffset(), sortOrder);
    }

    @Override
    public long decodeLong(ImmutableBytesWritable ptr, SortOrder sortOrder) {
      return decodeLong(ptr.get(), ptr.getOffset(), sortOrder);
    }

    @Override
    public byte decodeByte(ImmutableBytesWritable ptr, SortOrder sortOrder) {
      return decodeByte(ptr.get(), ptr.getOffset(), sortOrder);
    }

    @Override
    public short decodeShort(ImmutableBytesWritable ptr, SortOrder sortOrder) {
      return decodeShort(ptr.get(), ptr.getOffset(), sortOrder);
    }

    @Override
    public float decodeFloat(ImmutableBytesWritable ptr, SortOrder sortOrder) {
      return decodeFloat(ptr.get(), ptr.getOffset(), sortOrder);
    }

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double decodeDouble(ImmutableBytesWritable ptr, SortOrder sortOrder) {
      return decodeDouble(ptr.get(), ptr.getOffset(), sortOrder);
    }

    @Override
    public double decodeDouble(byte[] b, int o, SortOrder sortOrder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int encodeInt(int v, ImmutableBytesWritable ptr) {
      return encodeInt(v, ptr.get(), ptr.getOffset());
    }

    @Override
    public int encodeLong(long v, ImmutableBytesWritable ptr) {
      return encodeLong(v, ptr.get(), ptr.getOffset());
    }

    @Override
    public int encodeByte(byte v, ImmutableBytesWritable ptr) {
      return encodeByte(v, ptr.get(), ptr.getOffset());
    }

    @Override
    public int encodeShort(short v, ImmutableBytesWritable ptr) {
      return encodeShort(v, ptr.get(), ptr.getOffset());
    }

    @Override
    public int encodeFloat(float v, ImmutableBytesWritable ptr) {
      return encodeFloat(v, ptr.get(), ptr.getOffset());
    }

    @Override
    public int encodeDouble(double v, ImmutableBytesWritable ptr) {
      return encodeDouble(v, ptr.get(), ptr.getOffset());
    }

    @Override
    public int encodeInt(int v, byte[] b, int o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int encodeLong(long v, byte[] b, int o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int encodeByte(byte v, byte[] b, int o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int encodeShort(short v, byte[] b, int o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int encodeDouble(double v, byte[] b, int o) {
      throw new UnsupportedOperationException();
    }
  }

  public static final int MAX_PRECISION = 38;
      // Max precision guaranteed to fit into a long (and this should be plenty)
  public static final int MIN_DECIMAL_AVG_SCALE = 4;
  public static final MathContext DEFAULT_MATH_CONTEXT =
      new MathContext(MAX_PRECISION, RoundingMode.HALF_UP);
  public static final int DEFAULT_SCALE = 0;

  protected static final Integer MAX_BIG_DECIMAL_BYTES = 21;
  protected static final Integer MAX_TIMESTAMP_BYTES = Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;

  protected static final byte ZERO_BYTE = (byte) 0x80;
  protected static final byte NEG_TERMINAL_BYTE = (byte) 102;
  protected static final int EXP_BYTE_OFFSET = 65;
  protected static final int POS_DIGIT_OFFSET = 1;
  protected static final int NEG_DIGIT_OFFSET = 101;
  protected static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
  protected static final BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
  protected static final long MAX_LONG_FOR_DESERIALIZE = Long.MAX_VALUE / 1000;
  protected static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);

  protected static final byte FALSE_BYTE = 0;
  protected static final byte TRUE_BYTE = 1;
  public static final byte[] FALSE_BYTES = new byte[] { FALSE_BYTE };
  public static final byte[] TRUE_BYTES = new byte[] { TRUE_BYTE };
  public static final byte[] NULL_BYTES = ByteUtil.EMPTY_BYTE_ARRAY;
  protected static final Integer BOOLEAN_LENGTH = 1;

  public final static Integer ZERO = 0;
  public final static Integer INT_PRECISION = 10;
  public final static Integer LONG_PRECISION = 19;
  public final static Integer SHORT_PRECISION = 5;
  public final static Integer BYTE_PRECISION = 3;
  public final static Integer DOUBLE_PRECISION = 15;

  public static final int ARRAY_TYPE_BASE = 3000;
  public static final String ARRAY_TYPE_SUFFIX = "ARRAY";

  protected static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  /**
   * Serialize a BigDecimal into a variable length byte array in such a way that it is
   * binary comparable.
   *
   * @param v      the BigDecimal
   * @param result the byte array to contain the serialized bytes.  Max size
   *               necessary would be 21 bytes.
   * @param length the number of bytes required to store the big decimal. May be
   *               adjusted down if it exceeds {@link #MAX_BIG_DECIMAL_BYTES}
   * @return the number of bytes that make up the serialized BigDecimal
   */
  protected static int toBytes(BigDecimal v, byte[] result, final int offset, int length) {
    // From scale to exponent byte (if BigDecimal is positive):  (-(scale+(scale % 2 == 0 : 0 : 1)) / 2 + 65) | 0x80
    // If scale % 2 is 1 (i.e. it's odd), then multiple last base-100 digit by 10
    // For example: new BigDecimal(BigInteger.valueOf(1), -4);
    // (byte)((-(-4+0) / 2 + 65) | 0x80) = -61
    // From scale to exponent byte (if BigDecimal is negative): ~(-(scale+1)/2 + 65 + 128) & 0x7F
    // For example: new BigDecimal(BigInteger.valueOf(1), 2);
    // ~(-2/2 + 65 + 128) & 0x7F = 63
    int signum = v.signum();
    if (signum == 0) {
      result[offset] = ZERO_BYTE;
      return 1;
    }
    int index = offset + length;
    int scale = v.scale();
    int expOffset = scale % 2 * (scale < 0 ? -1 : 1);
    // In order to get twice as much of a range for scale, it
    // is multiplied by 2. If the scale is an odd number, then
    // the first digit is multiplied by 10 to make up for the
    // scale being off by one.
    int multiplyBy;
    BigInteger divideBy;
    if (expOffset == 0) {
      multiplyBy = 1;
      divideBy = ONE_HUNDRED;
    } else {
      multiplyBy = 10;
      divideBy = BigInteger.TEN;
    }
    // Normalize the scale based on what is necessary to end up with a base 100 decimal (i.e. 10.123e3)
    int digitOffset;
    BigInteger compareAgainst;
    if (signum == 1) {
      digitOffset = POS_DIGIT_OFFSET;
      compareAgainst = MAX_LONG;
      scale -= (length - 2) * 2;
      result[offset] = (byte) ((-(scale + expOffset) / 2 + EXP_BYTE_OFFSET) | 0x80);
    } else {
      digitOffset = NEG_DIGIT_OFFSET;
      compareAgainst = MIN_LONG;
      // Scale adjustment shouldn't include terminal byte in length
      scale -= (length - 2 - 1) * 2;
      result[offset] = (byte) (~(-(scale + expOffset) / 2 + EXP_BYTE_OFFSET + 128) & 0x7F);
      if (length <= MAX_BIG_DECIMAL_BYTES) {
        result[--index] = NEG_TERMINAL_BYTE;
      } else {
        // Adjust length and offset down because we don't have enough room
        length = MAX_BIG_DECIMAL_BYTES;
        index = offset + length;
      }
    }
    BigInteger bi = v.unscaledValue();
    // Use BigDecimal arithmetic until we can fit into a long
    while (bi.compareTo(compareAgainst) * signum > 0) {
      BigInteger[] dandr = bi.divideAndRemainder(divideBy);
      bi = dandr[0];
      int digit = dandr[1].intValue();
      result[--index] = (byte) (digit * multiplyBy + digitOffset);
      multiplyBy = 1;
      divideBy = ONE_HUNDRED;
    }
    long l = bi.longValue();
    do {
      long divBy = 100 / multiplyBy;
      long digit = l % divBy;
      l /= divBy;
      result[--index] = (byte) (digit * multiplyBy + digitOffset);
      multiplyBy = 1;
    } while (l != 0);

    return length;
  }

  /**
   * Deserialize a variable length byte array into a BigDecimal. Note that because of
   * the normalization that gets done to the scale, if you roundtrip a BigDecimal,
   * it may not be equal before and after. However, the before and after number will
   * always compare to be equal (i.e. <nBefore>.compareTo(<nAfter>) == 0)
   *
   * @param bytes  the bytes containing the number
   * @param offset the offset into the byte array
   * @param length the length of the serialized BigDecimal
   * @return the BigDecimal value.
   */
  protected static BigDecimal toBigDecimal(byte[] bytes, int offset, int length) {
    // From exponent byte back to scale: (<exponent byte> & 0x7F) - 65) * 2
    // For example, (((-63 & 0x7F) - 65) & 0xFF) * 2 = 0
    // Another example: ((-64 & 0x7F) - 65) * 2 = -2 (then swap the sign for the scale)
    // If number is negative, going from exponent byte back to scale: (byte)((~<exponent byte> - 65 - 128) * 2)
    // For example: new BigDecimal(new BigInteger("-1"), -2);
    // (byte)((~61 - 65 - 128) * 2) = 2, so scale is -2
    // Potentially, when switching back, the scale can be added by one and the trailing zero dropped
    // For digits, just do a mod 100 on the BigInteger. Use long if BigInteger fits
    if (length == 1 && bytes[offset] == ZERO_BYTE) {
      return BigDecimal.ZERO;
    }
    int signum = ((bytes[offset] & 0x80) == 0) ? -1 : 1;
    int scale;
    int index;
    int digitOffset;
    long multiplier = 100L;
    int begIndex = offset + 1;
    if (signum == 1) {
      scale = (byte) (((bytes[offset] & 0x7F) - 65) * -2);
      index = offset + length;
      digitOffset = POS_DIGIT_OFFSET;
    } else {
      scale = (byte) ((~bytes[offset] - 65 - 128) * -2);
      index = offset + length - (bytes[offset + length - 1] == NEG_TERMINAL_BYTE ? 1 : 0);
      digitOffset = -NEG_DIGIT_OFFSET;
    }
    length = index - offset;
    long l = signum * bytes[--index] - digitOffset;
    if (l % 10 == 0) { // trailing zero
      scale--; // drop trailing zero and compensate in the scale
      l /= 10;
      multiplier = 10;
    }
    // Use long arithmetic for as long as we can
    while (index > begIndex) {
      if (l >= MAX_LONG_FOR_DESERIALIZE || multiplier >= Long.MAX_VALUE / 100) {
        multiplier = LongMath.divide(multiplier, 100L, RoundingMode.UNNECESSARY);
        break; // Exit loop early so we don't overflow our multiplier
      }
      int digit100 = signum * bytes[--index] - digitOffset;
      l += digit100 * multiplier;
      multiplier = LongMath.checkedMultiply(multiplier, 100);
    }

    BigInteger bi;
    // If still more digits, switch to BigInteger arithmetic
    if (index > begIndex) {
      bi = BigInteger.valueOf(l);
      BigInteger biMultiplier = BigInteger.valueOf(multiplier).multiply(ONE_HUNDRED);
      do {
        int digit100 = signum * bytes[--index] - digitOffset;
        bi = bi.add(biMultiplier.multiply(BigInteger.valueOf(digit100)));
        biMultiplier = biMultiplier.multiply(ONE_HUNDRED);
      } while (index > begIndex);
      if (signum == -1) {
        bi = bi.negate();
      }
    } else {
      bi = BigInteger.valueOf(l * signum);
    }
    // Update the scale based on the precision
    scale += (length - 2) * 2;
    BigDecimal v = new BigDecimal(bi, scale);
    return v;
  }

  // Calculate the precision and scale of a raw decimal bytes. Returns the values as an int
  // array. The first value is precision, the second value is scale.
  // Default scope for testing
  protected static int[] getDecimalPrecisionAndScale(byte[] bytes, int offset, int length) {
    // 0, which should have no precision nor scale.
    if (length == 1 && bytes[offset] == ZERO_BYTE) {
      return new int[] { 0, 0 };
    }
    int signum = ((bytes[offset] & 0x80) == 0) ? -1 : 1;
    int scale;
    int index;
    int digitOffset;
    if (signum == 1) {
      scale = (byte) (((bytes[offset] & 0x7F) - 65) * -2);
      index = offset + length;
      digitOffset = POS_DIGIT_OFFSET;
    } else {
      scale = (byte) ((~bytes[offset] - 65 - 128) * -2);
      index = offset + length - (bytes[offset + length - 1] == NEG_TERMINAL_BYTE ? 1 : 0);
      digitOffset = -NEG_DIGIT_OFFSET;
    }
    length = index - offset;
    int precision = 2 * (length - 1);
    int d = signum * bytes[--index] - digitOffset;
    if (d % 10 == 0) { // trailing zero
      // drop trailing zero and compensate in the scale and precision.
      d /= 10;
      scale--;
      precision -= 1;
    }
    d = signum * bytes[offset + 1] - digitOffset;
    if (d < 10) { // Leading single digit
      // Compensate in the precision.
      precision -= 1;
    }
    // Update the scale based on the precision
    scale += (length - 2) * 2;
    if (scale < 0) {
      precision = precision - scale;
      scale = 0;
    }
    return new int[] { precision, scale };
  }

  public boolean isCoercibleTo(PDataType targetType) {
    return this.equals(targetType) || targetType.equals(PVarbinary.INSTANCE);
  }

  // Specialized on enums to take into account type hierarchy (i.e. UNSIGNED_LONG is comparable to INTEGER)
  public boolean isComparableTo(PDataType targetType) {
    return targetType.isCoercibleTo(this) || this.isCoercibleTo(targetType);
  }

  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return isCoercibleTo(targetType);
  }

  public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
      Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
    return true;
  }

  public int compareTo(byte[] b1, byte[] b2) {
    return compareTo(b1, 0, b1.length, SortOrder.getDefault(), b2, 0, b2.length,
        SortOrder.getDefault());
  }

  public final int compareTo(ImmutableBytesWritable ptr1, ImmutableBytesWritable ptr2) {
    return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), SortOrder.getDefault(),
        ptr2.get(), ptr2.getOffset(), ptr2.getLength(), SortOrder.getDefault());
  }

  public final int compareTo(byte[] ba1, int offset1, int length1, SortOrder so1, byte[] ba2,
      int offset2, int length2, SortOrder so2) {
    Preconditions.checkNotNull(so1);
    Preconditions.checkNotNull(so2);
    if (so1 != so2) {
      int length = Math.min(length1, length2);
      for (int i = 0; i < length; i++) {
        byte b1 = ba1[offset1 + i];
        byte b2 = ba2[offset2 + i];
        if (so1 == SortOrder.DESC) {
          b1 = SortOrder.invert(b1);
        } else {
          b2 = SortOrder.invert(b2);
        }
        int c = b1 - b2;
        if (c != 0) {
          return c;
        }
      }
      return (length1 - length2);
    }
    return Bytes.compareTo(ba1, offset1, length1, ba2, offset2, length2) * (so1 == SortOrder.DESC ?
        -1 :
        1);
  }

  public final int compareTo(ImmutableBytesWritable ptr1, SortOrder ptr1SortOrder,
      ImmutableBytesWritable ptr2, SortOrder ptr2SortOrder, PDataType type2) {
    return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), ptr1SortOrder, ptr2.get(),
        ptr2.getOffset(), ptr2.getLength(), ptr2SortOrder, type2);
  }

  public int compareTo(Object lhs, Object rhs) {
    return compareTo(lhs, rhs, this);
  }

  /*
   * We need an empty byte array to mean null, since
   * we have no other representation in the row key
   * for null.
   */
  public final boolean isNull(byte[] value) {
    return value == null || value.length == 0;
  }

  public byte[] toBytes(Object object, SortOrder sortOrder) {
    Preconditions.checkNotNull(sortOrder);
    byte[] bytes = toBytes(object);
    if (sortOrder == SortOrder.DESC) {
      SortOrder.invert(bytes, 0, bytes, 0, bytes.length);
    }
    return bytes;
  }

  public void coerceBytes(ImmutableBytesWritable ptr, Object o, PDataType actualType,
      Integer actualMaxLength, Integer actualScale, SortOrder actualModifier,
      Integer desiredMaxLength, Integer desiredScale, SortOrder expectedModifier) {
    Preconditions.checkNotNull(actualModifier);
    Preconditions.checkNotNull(expectedModifier);
    if (ptr.getLength() == 0) {
      return;
    }
    if (this.isBytesComparableWith(actualType)) { // No coerce necessary
      if (actualModifier == expectedModifier) {
        return;
      }
      byte[] b = ptr.copyBytes();
      SortOrder.invert(b, 0, b, 0, b.length);
      ptr.set(b);
      return;
    }

    // Optimization for cases in which we already have the object around
    if (o == null) {
      o = actualType.toObject(ptr, actualType, actualModifier);
    }

    o = toObject(o, actualType);
    byte[] b = toBytes(o, expectedModifier);
    ptr.set(b);
  }

  public final void coerceBytes(ImmutableBytesWritable ptr, PDataType actualType,
      SortOrder actualModifier, SortOrder expectedModifier) {
    coerceBytes(ptr, null, actualType, null, null, actualModifier, null, null, expectedModifier);
  }

  public final void coerceBytes(ImmutableBytesWritable ptr, PDataType actualType,
      SortOrder actualModifier,
      SortOrder expectedModifier, Integer desiredMaxLength) {
    coerceBytes(ptr, null, actualType, null, null, actualModifier, desiredMaxLength, null,
        expectedModifier);
  }

  protected static boolean isNonNegativeDate(java.util.Date date) {
    return (date == null || date.getTime() >= 0);
  }

  protected static void throwIfNonNegativeDate(java.util.Date date) {
    if (!isNonNegativeDate(date)) {
      throw newIllegalDataException("Value may not be negative(" + date + ")");
    }
  }

  protected static boolean isNonNegativeNumber(Number v) {
    return v == null || v.longValue() >= 0;
  }

  protected static void throwIfNonNegativeNumber(Number v) {
    if (!isNonNegativeNumber(v)) {
      throw newIllegalDataException("Value may not be negative(" + v + ")");
    }
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  public abstract Integer getByteSize();

  @Override
  public int encodedLength(T val) {
    // default implementation based on existing PDataType methods.
    return getByteSize();
  }

  @Override
  public int skip(PositionedByteRange pbr) {
    // default implementation based on existing PDataType methods.
    int len = getByteSize();
    pbr.setPosition(pbr.getPosition() + len);
    return len;
  }

  @Override
  public boolean isOrderPreserving() {
    return true;
  }

  @Override
  public boolean isSkippable() {
    return true;
  }

  @Override
  public Order getOrder() {
    return Order.ASCENDING;
  }

  public abstract boolean isFixedWidth();

  public abstract int compareTo(Object lhs, Object rhs, PDataType rhsType);

  @Override
  public int compareTo(PDataType<?> other) {
    return Integer.compare(this.ordinal(), other.ordinal());
  }

  /**
   * Convert from the object representation of a data type value into
   * the serialized byte form.
   *
   * @param object the object to convert
   * @param bytes  the byte array into which to put the serialized form of object
   * @param offset the offset from which to start writing the serialized form
   * @return the byte length of the serialized object
   */
  public abstract int toBytes(Object object, byte[] bytes, int offset);

  @Override
  public int encode(PositionedByteRange pbr, T val) {
    // default implementation based on existing PDataType methods.
    int pos = pbr.getPosition();
    pbr.put(toBytes(val));
    return pbr.getPosition() - pos;
  }

  @Override
  public String toString() {
    return sqlTypeName;
  }

  public abstract byte[] toBytes(Object object);

  /**
   * Convert from a string to the object representation of a given type
   *
   * @param value a stringified value
   * @return the object representation of a string value
   */
  public abstract Object toObject(String value);

  /*
   * Each enum must override this to define the set of object it may be coerced to
   */
  public abstract Object toObject(Object object, PDataType actualType);

  /*
   * Each enum must override this to define the set of objects it may create
   */
  public abstract Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale);

  @Override
  public T decode(PositionedByteRange pbr) {
    // default implementation based on existing PDataType methods.
    byte[] b = new byte[getByteSize()];
    pbr.get(b);
    return (T) toObject(b, 0, b.length, this, SortOrder.ASC, getMaxLength(null), getScale(null));
  }

  /*
     * Return a valid object of this enum type
     */
  public abstract Object getSampleValue(Integer maxLength, Integer arrayLength);

  public final Object getSampleValue() {
    return getSampleValue(null);
  }

  public final Object getSampleValue(Integer maxLength) {
    return getSampleValue(maxLength, null);
  }

  public final Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
      SortOrder sortOrder) {
    return toObject(bytes, offset, length, actualType, sortOrder, null, null);
  }

  public final Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
    return toObject(bytes, offset, length, actualType, SortOrder.getDefault());
  }

  public final Object toObject(ImmutableBytesWritable ptr, PDataType actualType) {
    return toObject(ptr, actualType, SortOrder.getDefault());
  }

  public final Object toObject(ImmutableBytesWritable ptr, PDataType actualType,
      SortOrder sortOrder) {
    return this.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), actualType, sortOrder);
  }

  public final Object toObject(ImmutableBytesWritable ptr, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale) {
    return this
        .toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), actualType, sortOrder, maxLength,
            scale);
  }

  public final Object toObject(ImmutableBytesWritable ptr, SortOrder sortOrder, Integer maxLength,
      Integer scale) {
    return this
        .toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), this, sortOrder, maxLength, scale);
  }

  public final Object toObject(ImmutableBytesWritable ptr) {
    return toObject(ptr.get(), ptr.getOffset(), ptr.getLength());
  }

  public final Object toObject(ImmutableBytesWritable ptr, SortOrder sortOrder) {
    return toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), this, sortOrder);
  }

  public final Object toObject(byte[] bytes, int offset, int length) {
    return toObject(bytes, offset, length, this);
  }

  public final Object toObject(byte[] bytes) {
    return toObject(bytes, SortOrder.getDefault());
  }

  public final Object toObject(byte[] bytes, SortOrder sortOrder) {
    return toObject(bytes, 0, bytes.length, this, sortOrder);
  }

  public final Object toObject(byte[] bytes, SortOrder sortOrder, PDataType actualType) {
    return toObject(bytes, 0, bytes.length, actualType, sortOrder);
  }

  public static PDataType fromSqlTypeName(String sqlTypeName) {
    for (PDataType t : PDataTypeFactory.getInstance().getTypes()) {
      if (t.getSqlTypeName().equals(sqlTypeName)) return t;
    }
    throw newIllegalDataException("Unsupported sql type: " + sqlTypeName);
  }

  public static int sqlArrayType(String sqlTypeName) {
    PDataType fromSqlTypeName = fromSqlTypeName(sqlTypeName);
    return fromSqlTypeName.getSqlType() + PDataType.ARRAY_TYPE_BASE;
  }

  protected static interface PhoenixArrayFactory {
    PhoenixArray newArray(PDataType type, Object[] elements);
  }

  public static PDataType fromTypeId(int typeId) {
    for (PDataType t : PDataTypeFactory.getInstance().getTypes()) {
      if (t.getSqlType() == typeId) return t;
    }
    throw newIllegalDataException("Unsupported sql type: " + typeId);
  }

  public String getJavaClassName() {
    return getJavaClass().getName();
  }

  public byte[] getJavaClassNameBytes() {
    return clazzNameBytes;
  }

  public byte[] getSqlTypeNameBytes() {
    return sqlTypeNameBytes;
  }

  /**
   * By default returns sqlType for the PDataType,
   * however it allows unknown types (our unsigned types)
   * to return the regular corresponding sqlType so
   * that tools like SQuirrel correctly display values
   * of this type.
   *
   * @return integer representing the SQL type for display
   * of a result set of this type
   */
  public int getResultSetSqlType() {
    return this.sqlType;
  }

  public KeyRange getKeyRange(byte[] point) {
    return getKeyRange(point, true, point, true);
  }

  public final String toStringLiteral(ImmutableBytesWritable ptr, Format formatter) {
    return toStringLiteral(ptr.get(), ptr.getOffset(), ptr.getLength(), formatter);
  }

  public final String toStringLiteral(byte[] b, Format formatter) {
    return toStringLiteral(b, 0, b.length, formatter);
  }

  public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
    Object o = toObject(b, offset, length);
    return toStringLiteral(o, formatter);
  }
  
  public String toStringLiteral(Object o, Format formatter) {
      if (formatter != null) {
          return formatter.format(o);
        }
        return o.toString();
  }

  private static final PhoenixArrayFactory DEFAULT_ARRAY_FACTORY = new PhoenixArrayFactory() {
    @Override public PhoenixArray newArray(PDataType type, Object[] elements) {
      return new PhoenixArray(type, elements);
    }
  };

  public PhoenixArrayFactory getArrayFactory() {
    if (getCodec() != null)
      return getCodec().getPhoenixArrayFactory();
    else
      return DEFAULT_ARRAY_FACTORY;
  }

  public static PhoenixArray instantiatePhoenixArray(PDataType actualType, Object[] elements) {
    return actualType.getArrayFactory().newArray(actualType, elements);
  }

  public KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange,
      boolean upperInclusive) {
        /*
         * Force lower bound to be inclusive for fixed width keys because it makes
         * comparisons less expensive when you can count on one bound or the other
         * being inclusive. Comparing two fixed width exclusive bounds against each
         * other is inherently more expensive, because you need to take into account
         * if the bigger key is equal to the next key after the smaller key. For
         * example:
         *   (A-B] compared against [A-B)
         * An exclusive lower bound A is bigger than an exclusive upper bound B.
         * Forcing a fixed width exclusive lower bound key to be inclusive prevents
         * us from having to do this extra logic in the compare function.
         */
    if (lowerRange != KeyRange.UNBOUND && !lowerInclusive && isFixedWidth()) {
      lowerRange = ByteUtil.nextKey(lowerRange);
      lowerInclusive = true;
    }
    return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
  }

  public static PDataType fromLiteral(Object value) {
    if (value == null) {
      return null;
    }
    for (PDataType type : PDataType.values()) {
      if (type.isArrayType()) {
        PhoenixArray arr = (PhoenixArray) value;
        if ((type.getSqlType() == arr.baseType.sqlType + PDataType.ARRAY_TYPE_BASE)
            && type.getJavaClass().isInstance(value)) {
          return type;
        }
      } else {
        if (type.getJavaClass().isInstance(value)) {
          return type;
        }
      }
    }
    throw new UnsupportedOperationException(
        "Unsupported literal value [" + value + "] of type " + value.getClass().getName());
  }

  public int getNanos(ImmutableBytesWritable ptr, SortOrder sortOrder) {
    throw new UnsupportedOperationException("Operation not supported for type " + this);
  }

  public long getMillis(ImmutableBytesWritable ptr, SortOrder sortOrder) {
    throw new UnsupportedOperationException("Operation not supported for type " + this);
  }

  public Object pad(Object object, Integer maxLength) {
    return object;
  }

  public void pad(ImmutableBytesWritable ptr, Integer maxLength) {
  }

  public static PDataType arrayBaseType(PDataType arrayType) {
    Preconditions.checkArgument(arrayType.isArrayType(), "Not a phoenix array type");
    return fromTypeId(arrayType.getSqlType() - ARRAY_TYPE_BASE);
  }
}
