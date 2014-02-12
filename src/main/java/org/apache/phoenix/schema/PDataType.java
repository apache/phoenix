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
package org.apache.phoenix.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.Format;
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.StringUtil;


/**
 * The data types of PColumns
 *
 * 
 * 
 * @since 0.1
 *
 * TODO: cleanup implementation to reduce copy/paste duplication
 */
@SuppressWarnings("rawtypes")
public enum PDataType {
    VARCHAR("VARCHAR", Types.VARCHAR, String.class, null) {
        @Override
        public byte[] toBytes(Object object) {
            // TODO: consider using avro UTF8 object instead of String
            // so that we get get the size easily
            if (object == null) {
                return ByteUtil.EMPTY_BYTE_ARRAY;
            }
            return Bytes.toBytes((String)object);
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                return 0;
            }
            byte[] b = toBytes(object); // TODO: no byte[] allocation: use CharsetEncoder
            System.arraycopy(b, 0, bytes, offset, b.length);
            return b.length;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (!actualType.isCoercibleTo(this)) {
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            return length == 0 ? null : Bytes.toString(bytes, offset, length);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            switch (actualType) {
            case VARCHAR:
            case CHAR:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            // TODO: should CHAR not be here?
            return this == targetType || targetType == CHAR || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (isCoercibleTo(targetType)) {
                if (targetType == PDataType.CHAR) {
                    return value != null;
                }
                return true;
            }
            return false;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if (srcType == PDataType.CHAR && maxLength != null && desiredMaxLength != null) {
                return maxLength <= desiredMaxLength;
            }
            return true;
        }

        @Override
        public boolean isFixedWidth() {
            return false;
        }

        @Override
        public int estimateByteSize(Object o) {
            String value = (String) o;
            return value == null ? 1 : value.length();
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return ((String)lhs).compareTo((String)rhs);
        }

        @Override
        public Object toObject(String value) {
            return value;
        }

        @Override
        public boolean isBytesComparableWith(PDataType otherType) {
            return super.isBytesComparableWith(otherType) || this == CHAR;
        }

        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            while (b[length-1] == 0) {
                length--;
            }
            if (formatter != null) {
                Object o = toObject(b,offset,length);
                return "'" + formatter.format(o) + "'";
            }
            return "'" + Bytes.toStringBinary(b, offset, length) + "'";
        }
    },
    /**
     * Fixed length single byte characters
     */
    CHAR("CHAR", Types.CHAR, String.class, null) { // Delegate to VARCHAR
        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            byte[] b = VARCHAR.toBytes(object);
            if (b.length != ((String) object).length()) {
                throw new IllegalDataException("CHAR types may only contain single byte characters (" + object + ")");
            }
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            int len = VARCHAR.toBytes(object, bytes, offset);
            if (len != ((String) object).length()) {
                throw new IllegalDataException("CHAR types may only contain single byte characters (" + object + ")");
            }
            return len;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (!actualType.isCoercibleTo(this)) { // TODO: have isCoercibleTo that takes bytes, offset?
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            if (length == 0) {
                return null;
            }
            length = StringUtil.getUnpaddedCharLength(bytes, offset, length, null);
            String s = Bytes.toString(bytes, offset, length);
            if (length != s.length()) {
               throw new IllegalDataException("CHAR types may only contain single byte characters (" + s + ")");
            }
            return s;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            switch (actualType) {
            case VARCHAR:
            case CHAR:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == VARCHAR || targetType == BINARY || targetType == VARBINARY;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if ((srcType == PDataType.VARCHAR && ((String)value).length() != b.length) ||
                    (maxLength != null && desiredMaxLength != null && maxLength > desiredMaxLength)){
                return false;
            }
            return true;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public Integer getMaxLength(Object o) {
            if (o == null) {
                return null;
            }
            String value = (String) o;
            return value.length();
        }

        @Override
        public int estimateByteSize(Object o) {
            String value = (String) o;
            return value.length();
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return VARCHAR.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            if (StringUtil.hasMultiByteChars(value)) {
                throw new IllegalDataException("CHAR types may only contain single byte characters (" + value + ")");
            }
            return value;
        }

        @Override
        public Integer estimateByteSizeFromLength(Integer length) {
            return length;
        }

        @Override
        public boolean isBytesComparableWith(PDataType otherType) {
            return super.isBytesComparableWith(otherType) || this == VARCHAR;
        }
        
        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            return VARCHAR.toStringLiteral(b, offset, length, formatter);
        }
    },
    LONG("BIGINT", Types.BIGINT, Long.class, new LongCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_LONG];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeLong(((Number)object).longValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case LONG:
            case UNSIGNED_LONG:
                return object;
            case UNSIGNED_INT:
            case INTEGER:
                long s = (Integer)object;
                return s;
            case TINYINT:
            case UNSIGNED_TINYINT:
                s = (Byte)object;
                return s;
            case SMALLINT:
            case UNSIGNED_SMALLINT:
                s = (Short)object;
                return s;
            case FLOAT:
            case UNSIGNED_FLOAT:
                Float f = (Float)object;
                if (f > Long.MAX_VALUE || f < Long.MIN_VALUE) {
                    throw new IllegalDataException(actualType + " value " + f + " cannot be cast to Long without changing its value");
                }
                s = f.longValue();
                return s;
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                Double de = (Double) object;
                if (de > Long.MAX_VALUE || de < Long.MIN_VALUE) {
                    throw new IllegalDataException(actualType + " value " + de + " cannot be cast to Long without changing its value");
                }
                s = de.longValue();
                return s;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                return d.longValueExact();
            case VARBINARY:
            case BINARY:
                byte[] o = (byte[]) object;
                if (o.length == Bytes.SIZEOF_LONG) {
                    return Bytes.toLong(o);
                } else if (o.length == Bytes.SIZEOF_INT) {
                    int iv = Bytes.toInt(o);
                    return (long) iv;
                } else {
                    throw new IllegalDataException("Bytes passed doesn't represent an integer.");
                }
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case LONG:
            case UNSIGNED_LONG:
            case INTEGER:
            case UNSIGNED_INT:
            case SMALLINT:
            case UNSIGNED_SMALLINT:
            case UNSIGNED_TINYINT:
            case TINYINT:
            case UNSIGNED_FLOAT:
            case FLOAT:
            case UNSIGNED_DOUBLE:
            case DOUBLE:
                return actualType.getCodec().decodeLong(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            // In general, don't allow conversion of LONG to INTEGER. There are times when
            // we check isComparableTo for a more relaxed check and then throw a runtime
            // exception if we overflow
            return this == targetType || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY
                    || targetType == FLOAT || targetType == DOUBLE;
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                long l;
                switch (targetType) {
                    case UNSIGNED_DOUBLE:
                    case UNSIGNED_FLOAT:
                    case UNSIGNED_LONG:
                        l = (Long) value;
                        return l >= 0;
                    case UNSIGNED_INT:
                        l = (Long) value;
                        return (l >= 0 && l <= Integer.MAX_VALUE);
                    case INTEGER:
                        l = (Long) value;
                        return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE);
                    case UNSIGNED_SMALLINT:
                        l = (Long) value;
                        return (l >= 0 && l <= Short.MAX_VALUE);
                    case SMALLINT:
                        l = (Long) value;
                        return (l >=Short.MIN_VALUE && l<=Short.MAX_VALUE);
                    case TINYINT:
                        l = (Long)value;
                        return (l >=Byte.MIN_VALUE && l<Byte.MAX_VALUE);
                    case UNSIGNED_TINYINT:
                        l = (Long)value;
                        return (l >=0 && l<Byte.MAX_VALUE);
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
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
        public Integer getScale(Object o) {
            return ZERO;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return -((BigDecimal)rhs).compareTo(BigDecimal.valueOf(((Number)lhs).longValue()));
            } else if (rhsType == DOUBLE || rhsType == FLOAT || rhsType == UNSIGNED_DOUBLE || rhsType == UNSIGNED_FLOAT) {
                return Doubles.compare(((Number)lhs).doubleValue(), ((Number)rhs).doubleValue());
            }
            return Longs.compare(((Number)lhs).longValue(), ((Number)rhs).longValue());
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
    },
    INTEGER("INTEGER", Types.INTEGER, Integer.class, new IntCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_INT];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeInt(((Number)object).intValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            Object o = LONG.toObject(object, actualType);
            if (!(o instanceof Long) || o == null) {
                return o;
            }
            long l = (Long)o;
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                throw new IllegalDataException(actualType + " value " + l + " cannot be cast to Integer without changing its value");
            }
            int v = (int)l;
            return v;
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case LONG:
            case UNSIGNED_LONG:
            case INTEGER:
            case UNSIGNED_INT:
            case SMALLINT:
            case UNSIGNED_SMALLINT:
            case TINYINT:
            case UNSIGNED_TINYINT:
            case FLOAT:
            case UNSIGNED_FLOAT:
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                return actualType.getCodec().decodeInt(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                switch (targetType) {
                    case UNSIGNED_DOUBLE:
                    case UNSIGNED_FLOAT:
                    case UNSIGNED_LONG:
                    case UNSIGNED_INT:
                        int i = (Integer) value;
                        return i >= 0;
                    case UNSIGNED_SMALLINT:
                        i = (Integer) value;
                        return (i >= 0 && i <= Short.MAX_VALUE);
                    case SMALLINT:
                        i = (Integer) value;
                        return (i >=Short.MIN_VALUE && i<=Short.MAX_VALUE);
                    case TINYINT:
                        i = (Integer)value;
                        return (i >=Byte.MIN_VALUE && i<=Byte.MAX_VALUE);
                    case UNSIGNED_TINYINT:
                        i = (Integer)value;
                        return (i >=0 && i<Byte.MAX_VALUE);
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || LONG.isCoercibleTo(targetType);
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
        public Integer getScale(Object o) {
            return ZERO;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return LONG.compareTo(lhs,rhs,rhsType);
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
    },
    SMALLINT("SMALLINT", Types.SMALLINT, Short.class, new ShortCodec()){

      @Override
      public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
        return LONG.compareTo(lhs, rhs, rhsType);
      }
      
      @Override
      public boolean isComparableTo(PDataType targetType) {
          return DECIMAL.isComparableTo(targetType);
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
      public Integer getScale(Object o) {
          return ZERO;
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
          throw new ConstraintViolationException(this + " may not be null");
        }
        return this.getCodec().encodeShort(((Number)object).shortValue(), bytes, offset);
      }
      
      @Override
      public Object toObject(Object object, PDataType actualType) {
          Object o = LONG.toObject(object, actualType);
          if (!(o instanceof Long) || o == null) {
              return o;
          }
          long l = (Long)o;
          if (l < Short.MIN_VALUE || l > Short.MAX_VALUE) {
              throw new IllegalDataException(actualType + " value " + l + " cannot be cast to Short without changing its value");
          }
          short s = (short)l;
          return s;
      }

      @Override
      public Object toObject(byte[] b, int o, int l, PDataType actualType) {
          if (l == 0) {
              return null;
          }
          switch (actualType) {
          case SMALLINT:
          case UNSIGNED_SMALLINT:
          case TINYINT:
          case UNSIGNED_TINYINT:
          case LONG:
          case UNSIGNED_LONG:
          case INTEGER:
          case UNSIGNED_INT:
          case FLOAT:
          case UNSIGNED_FLOAT:
          case DOUBLE:
          case UNSIGNED_DOUBLE:
              return actualType.getCodec().decodeShort(b, o, null);
          default:
              return super.toObject(b,o,l,actualType);
          }
      }

      @Override
      public Object toObject(String value) {
        if (value == null || value.length() == 0) {
          return null;
        }
        try {
            return Short.parseShort(value);
        } catch (NumberFormatException e) {
            throw new IllegalDataException(e);
        }
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType, Object value) {
          if (value != null) {
              switch (targetType) {
                  case UNSIGNED_DOUBLE:
                  case UNSIGNED_FLOAT:
                  case UNSIGNED_LONG:
                  case UNSIGNED_INT:
                  case UNSIGNED_SMALLINT:
                      short i = (Short) value;
                      return i >= 0;
                  case UNSIGNED_TINYINT:
                      i = (Short) value;
                      return (i>=0 && i<= Byte.MAX_VALUE);
                  case TINYINT:
                      i = (Short)value;
                      return (i>=Byte.MIN_VALUE && i<= Byte.MAX_VALUE);
                  default:
                      break;
              }
          }
          return super.isCoercibleTo(targetType, value);
      }

      @Override
      public boolean isCoercibleTo(PDataType targetType) {
          return this == targetType || INTEGER.isCoercibleTo(targetType);
      }
      
    },
    TINYINT("TINYINT", Types.TINYINT, Byte.class, new ByteCodec()) {

      @Override
      public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
        return LONG.compareTo(lhs, rhs, rhsType);
      }
      
      @Override
      public boolean isComparableTo(PDataType targetType) {
          return DECIMAL.isComparableTo(targetType);
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
      public Integer getScale(Object o) {
          return ZERO;
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
          throw new ConstraintViolationException(this + " may not be null");
        }
        return this.getCodec().encodeByte(((Number)object).byteValue(), bytes, offset);
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
          throw new IllegalDataException(e);
        }
      }
      
      @Override
      public Object toObject(Object object, PDataType actualType) {
          Object o = LONG.toObject(object, actualType);
          if(!(o instanceof Long) || o == null) {
              return o;
          }
          long l = (Long)o;
          if (l < Byte.MIN_VALUE || l > Byte.MAX_VALUE) {
              throw new IllegalDataException(actualType + " value " + l + " cannot be cast to Byte without changing its value");
          }
          return (byte)l;
      }
      
      @Override
      public Object toObject(byte[] b, int o, int l, PDataType actualType) {
          if (l == 0) {
              return null;
          }
          switch (actualType) {
          case UNSIGNED_DOUBLE:
          case DOUBLE:
          case UNSIGNED_FLOAT:
          case FLOAT:
          case UNSIGNED_LONG:
          case LONG:
          case UNSIGNED_INT:
          case INTEGER:
          case UNSIGNED_SMALLINT:
          case SMALLINT:
          case UNSIGNED_TINYINT:
          case TINYINT:
              return actualType.getCodec().decodeByte(b, o, null);
          default:
              return super.toObject(b,o,l,actualType);
          }
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType, Object value) {
          if (value != null) {
              switch (targetType) {
                  case UNSIGNED_DOUBLE:
                  case UNSIGNED_FLOAT:
                  case UNSIGNED_LONG:
                  case UNSIGNED_INT:
                  case UNSIGNED_SMALLINT:
                  case UNSIGNED_TINYINT:
                      byte i = (Byte) value;
                      return i >= 0;
                  default:
                      break;
              }
          }
          return super.isCoercibleTo(targetType, value);
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType) {
          return this == targetType || SMALLINT.isCoercibleTo(targetType);
      }
      
    },
    FLOAT("FLOAT", Types.FLOAT, Float.class, new FloatCodec()) {

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return DOUBLE.compareTo(lhs, rhs, rhsType);
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
        public Integer getScale(Object o) {
            if (o == null) {
                return null;
            }
            Float v = (Float) o;
            BigDecimal bd = BigDecimal.valueOf(v);
            return bd.scale();
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
            byte[] b = new byte[Bytes.SIZEOF_INT];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
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
                throw new IllegalDataException(e);
            }
        }
        
        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case FLOAT:
            case UNSIGNED_FLOAT:
                return object;
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                double d = (Double)object;
                if (Double.isNaN(d)
                        || d == Double.POSITIVE_INFINITY
                        || d == Double.NEGATIVE_INFINITY
                        || (d >= -Float.MAX_VALUE && d <= Float.MAX_VALUE)) {
                    return (float) d;
                } else {
                    throw new IllegalDataException(actualType + " value " + d + " cannot be cast to Float without changing its value");
                }
            case LONG:
            case UNSIGNED_LONG:
                float f = (Long)object;
                return f;
            case UNSIGNED_INT:
            case INTEGER:
                f = (Integer)object;
                return f;
            case TINYINT:
            case UNSIGNED_TINYINT:
                f = (Byte)object;
                return f;
            case SMALLINT:
            case UNSIGNED_SMALLINT:
                f = (Short)object;
                return f;
            case DECIMAL:
                BigDecimal dl = (BigDecimal)object;
                return dl.floatValue();
            default:
                return super.toObject(object, actualType);
            }
        }
        
        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l <= 0) {
                return null;
            }
            switch (actualType) {
            case FLOAT:
            case UNSIGNED_FLOAT:
            case DOUBLE:
            case UNSIGNED_DOUBLE:
            case UNSIGNED_LONG:
            case LONG:
            case UNSIGNED_INT:
            case INTEGER:
            case UNSIGNED_SMALLINT:
            case SMALLINT:
            case UNSIGNED_TINYINT:
            case TINYINT:
                return actualType.getCodec().decodeFloat(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                float f = (Float)value;
                switch (targetType) {
                case UNSIGNED_FLOAT:
                    return f >= 0;
                case UNSIGNED_LONG:
                    return (f >= 0 && f <= Long.MAX_VALUE);
                case LONG:
                    return (f >= Long.MIN_VALUE && f <= Long.MAX_VALUE);
                case UNSIGNED_INT:
                    return (f >= 0 && f <= Integer.MAX_VALUE);
                case INTEGER:
                    return (f >= Integer.MIN_VALUE && f <= Integer.MAX_VALUE);
                case UNSIGNED_SMALLINT:
                    return (f >= 0 && f <= Short.MAX_VALUE);
                case SMALLINT:
                    return (f >=Short.MIN_VALUE && f<=Short.MAX_VALUE);
                case TINYINT:
                    return (f >=Byte.MIN_VALUE && f<Byte.MAX_VALUE);
                case UNSIGNED_TINYINT:
                    return (f >=0 && f<Byte.MAX_VALUE);
                default:
                    break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || DOUBLE.isCoercibleTo(targetType);
        }
        
    },
    DOUBLE("DOUBLE", Types.DOUBLE, Double.class, new DoubleCodec()) {

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return -((BigDecimal)rhs).compareTo(BigDecimal.valueOf(((Number)lhs).doubleValue()));
            }
            return Doubles.compare(((Number)lhs).doubleValue(), ((Number)rhs).doubleValue());
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
        public Integer getScale(Object o) {
            if (o == null) {
                return null;
            }
            Double v = (Double) o;
            BigDecimal bd = BigDecimal.valueOf(v);
            return bd.scale();
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
            byte[] b = new byte[Bytes.SIZEOF_LONG];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
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
                throw new IllegalDataException(e);
            }
        }
        
        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            double de;
            switch (actualType) {
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                return object; 
            case FLOAT:
            case UNSIGNED_FLOAT:
                de = (Float)object;
                return de;
            case LONG:
            case UNSIGNED_LONG:
                de = (Long)object;
                return de;
            case UNSIGNED_INT:
            case INTEGER:
                de = (Integer)object;
                return de;
            case TINYINT:
            case UNSIGNED_TINYINT:
                de = (Byte)object;
                return de;
            case SMALLINT:
            case UNSIGNED_SMALLINT:
                de = (Short)object;
                return de;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                return d.doubleValue();
            default:
                return super.toObject(object, actualType);
            }
        }
        
        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l <= 0) {
                return null;
            }
            switch (actualType) {
            case DOUBLE:
            case UNSIGNED_DOUBLE:
            case FLOAT:
            case UNSIGNED_FLOAT:
            case UNSIGNED_LONG:
            case LONG:
            case UNSIGNED_INT:
            case INTEGER:
            case UNSIGNED_SMALLINT:
            case SMALLINT:
            case UNSIGNED_TINYINT:
            case TINYINT:
                return actualType.getCodec().decodeDouble(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                double d = (Double)value;
                switch (targetType) {
                case UNSIGNED_DOUBLE:
                    return d >= 0;
                case FLOAT:
                    return Double.isNaN(d)
                            || d == Double.POSITIVE_INFINITY
                            || d == Double.NEGATIVE_INFINITY
                            || (d >= -Float.MAX_VALUE && d <= Float.MAX_VALUE);
                case UNSIGNED_FLOAT:
                    return Double.isNaN(d) || d == Double.POSITIVE_INFINITY
                            || (d >= 0 && d <= Float.MAX_VALUE);
                case UNSIGNED_LONG:
                    return (d >= 0 && d <= Long.MAX_VALUE);
                case LONG:
                    return (d >= Long.MIN_VALUE && d <= Long.MAX_VALUE);
                case UNSIGNED_INT:
                    return (d >= 0 && d <= Integer.MAX_VALUE);
                case INTEGER:
                    return (d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE);
                case UNSIGNED_SMALLINT:
                    return (d >= 0 && d <= Short.MAX_VALUE);
                case SMALLINT:
                    return (d >=Short.MIN_VALUE && d<=Short.MAX_VALUE);
                case TINYINT:
                    return (d >=Byte.MIN_VALUE && d<Byte.MAX_VALUE);
                case UNSIGNED_TINYINT:
                    return (d >=0 && d<Byte.MAX_VALUE);
                default:
                    break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY;
        }
        
    },
    DECIMAL("DECIMAL", Types.DECIMAL, BigDecimal.class, null) {
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
            return (signum < 0 ? 2 : 1) + (v.precision() +  1 + (v.scale() % 2 == 0 ? 0 : 1)) / 2;
        }

        @Override
        public int estimateByteSize(Object o) {
            if (o == null) {
                return 1;
            }
            BigDecimal v = (BigDecimal) o;
            // TODO: should we strip zeros and round here too?
            return Math.min(getLength(v),MAX_BIG_DECIMAL_BYTES);
        }

        @Override
        public Integer getMaxLength(Object o) {
            if (o == null) {
                return null;
            }
            BigDecimal v = (BigDecimal) o;
            return v.precision();
        }

        @Override
        public Integer getScale(Object o) {
            if (o == null) {
                return null;
            }
            BigDecimal v = (BigDecimal) o;
            return v.scale();
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, ColumnModifier columnModifier) {
            if (l == 0) {
                return null;
            }
            if (columnModifier != null) {
                b = columnModifier.apply(b, o, new byte[l], 0, l);
                o = 0;
            }
            switch (actualType) {
            case DECIMAL:
                return toBigDecimal(b, o, l);
            case DATE:
            case TIME:
            case LONG:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case UNSIGNED_LONG:
            case UNSIGNED_INT:
            case UNSIGNED_SMALLINT:
            case UNSIGNED_TINYINT:
                return BigDecimal.valueOf(actualType.getCodec().decodeLong(b, o, null));
            case FLOAT:
            case UNSIGNED_FLOAT:
                return BigDecimal.valueOf(actualType.getCodec().decodeFloat(b, o, null));
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                return BigDecimal.valueOf(actualType.getCodec().decodeDouble(b, o, null));
            case TIMESTAMP:
                Timestamp ts = (Timestamp) actualType.toObject(b, o, l) ;
                long millisPart = ts.getTime();
                BigDecimal nanosPart = BigDecimal.valueOf((ts.getNanos() % QueryConstants.MILLIS_TO_NANOS_CONVERTOR)/QueryConstants.MILLIS_TO_NANOS_CONVERTOR);
                BigDecimal value = BigDecimal.valueOf(millisPart).add(nanosPart);
                return value;
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case INTEGER:
            case UNSIGNED_INT:
                return BigDecimal.valueOf((Integer)object);
            case LONG:
            case UNSIGNED_LONG:
                return BigDecimal.valueOf((Long)object);
            case SMALLINT:
            case UNSIGNED_SMALLINT:
                return BigDecimal.valueOf((Short)object);
            case TINYINT:
            case UNSIGNED_TINYINT:
                return BigDecimal.valueOf((Byte)object);
            case FLOAT:
            case UNSIGNED_FLOAT:
                return BigDecimal.valueOf((Float)object);
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                return BigDecimal.valueOf((Double)object);
            case DECIMAL:
                return object;
            case TIMESTAMP:
                Timestamp ts = (Timestamp)object;
                long millisPart = ts.getTime();
                BigDecimal nanosPart = BigDecimal.valueOf((ts.getNanos() % QueryConstants.MILLIS_TO_NANOS_CONVERTOR)/QueryConstants.MILLIS_TO_NANOS_CONVERTOR);
                BigDecimal value = BigDecimal.valueOf(millisPart).add(nanosPart);
                return value;
            default:
                return super.toObject(object, actualType);
            }
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
            if (rhsType == DECIMAL) {
                return ((BigDecimal)lhs).compareTo((BigDecimal)rhs);
            }
            return -rhsType.compareTo(rhs, lhs, this);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                BigDecimal bd;
                switch (targetType) {
                    case UNSIGNED_LONG:
                    case UNSIGNED_INT:
                    case UNSIGNED_SMALLINT:
                    case UNSIGNED_TINYINT:
                        bd = (BigDecimal) value;
                        if (bd.signum() == -1) {
                            return false;
                        }
                    case LONG:
                        bd = (BigDecimal) value;
                        try {
                            bd.longValueExact();
                            return true;
                        } catch (ArithmeticException e) {
                            return false;
                        }
                    case INTEGER:
                        bd = (BigDecimal) value;
                        try {
                            bd.intValueExact();
                            return true;
                        } catch (ArithmeticException e) {
                            return false;
                        }
                    case SMALLINT:
                        bd = (BigDecimal) value;
                        try {
                            bd.shortValueExact();
                            return true;
                        } catch (ArithmeticException e) {
                            return false;
                        }
                    case TINYINT:
                        bd = (BigDecimal) value;
                        try {
                            bd.byteValueExact();
                            return true;
                        } catch (ArithmeticException e) {
                            return false;
                        }
                    case UNSIGNED_FLOAT:
                        bd = (BigDecimal) value;
                        try {
                            BigDecimal maxFloat = MAX_FLOAT_AS_BIG_DECIMAL;
                            boolean isNegtive = (bd.signum() == -1);
                            return bd.compareTo(maxFloat)<=0 && !isNegtive;
                        } catch(Exception e) {
                            return false;
                        }
                    case FLOAT:
                        bd = (BigDecimal) value;
                        try {
                            BigDecimal maxFloat = MAX_FLOAT_AS_BIG_DECIMAL;
                            // Float.MIN_VALUE should not be used here, as this is the
                            // smallest in terms of closest to zero.
                            BigDecimal minFloat = MIN_FLOAT_AS_BIG_DECIMAL;
                            return bd.compareTo(maxFloat)<=0 && bd.compareTo(minFloat)>=0;
                        } catch(Exception e) {
                            return false;
                        }
                    case UNSIGNED_DOUBLE:
                        bd = (BigDecimal) value;
                        try {
                            BigDecimal maxDouble = MAX_DOUBLE_AS_BIG_DECIMAL;
                            boolean isNegtive = (bd.signum() == -1);
                            return bd.compareTo(maxDouble)<=0 && !isNegtive;
                        } catch(Exception e) {
                            return false;
                        }
                    case DOUBLE:
                        bd = (BigDecimal) value;
                        try {
                            BigDecimal maxDouble = MAX_DOUBLE_AS_BIG_DECIMAL;
                            BigDecimal minDouble = MIN_DOUBLE_AS_BIG_DECIMAL;
                            return bd.compareTo(maxDouble)<=0 && bd.compareTo(minDouble)>=0;
                        } catch(Exception e) {
                            return false;
                        }
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b, Integer maxLength,
        		Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            // Get precision and scale if it is not already passed in and either the object or byte values
            // is meaningful.
            if (maxLength == null && scale == null) {
                if (value != null) {
                    BigDecimal v = (BigDecimal) value;
                    maxLength = v.precision();
                    scale = v.scale();
                } else if (b != null && b.length > 0) {
                    int[] v = getDecimalPrecisionAndScale(b, 0, b.length);
                    maxLength = v[0];
                    scale = v[1];
                } else {
                    // the value does not contains maxLength nor scale. Just return true.
                    return true;
                }
            }
            if (desiredMaxLength != null && desiredScale != null && maxLength != null && scale != null &&
            		((desiredScale == null && desiredMaxLength < maxLength) || 
            				(desiredMaxLength - desiredScale) < (maxLength - scale))) {
                return false;
            }
            return true;
        }

        @Override
        public byte[] coerceBytes(byte[] b, Object object, PDataType actualType, Integer maxLength, Integer scale,
                Integer desiredMaxLength, Integer desiredScale) {
            if (desiredScale == null) {
                // deiredScale not available, or we do not have scale requirement, delegate to parents.
                return super.coerceBytes(b, object, actualType);
            }
            if (scale == null) {
                if (object != null) {
                    BigDecimal v = (BigDecimal) object;
                    scale = v.scale();
                } else if (b != null && b.length > 0) {
                    int[] v = getDecimalPrecisionAndScale(b, 0, b.length);
                    scale = v[1];
                } else {
                    // Neither the object value nor byte value is meaningful, delegate to super.
                    return super.coerceBytes(b, object, actualType);
                }
            }
            if (this == actualType && scale <= desiredScale) {
                // No coerce and rescale necessary
                return b;
            } else {
                BigDecimal decimal;
                // Rescale is necessary.
                if (object != null) { // value object is passed in.
                    decimal = (BigDecimal) toObject(object, actualType);
                } else { // only value bytes is passed in, need to convert to object first.
                    decimal = (BigDecimal) toObject(b);
                }
                decimal = decimal.setScale(desiredScale, BigDecimal.ROUND_DOWN);
                return toBytes(decimal);
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
                throw new IllegalDataException(e);
            }
        }

        @Override
        public Integer estimateByteSizeFromLength(Integer length) {
            // No association of runtime byte size from decimal precision.
            return null;
        }
    },
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, Timestamp.class, new DateCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            byte[] bytes = new byte[getByteSize()];
            toBytes(object, bytes, 0);
            return bytes;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            int size = Bytes.SIZEOF_LONG;
            Timestamp value = (Timestamp)object;
            offset = Bytes.putLong(bytes, offset, value.getTime());
            
            /*
             * By not getting the stuff that got spilled over from the millis part,
             * it leaves the timestamp's byte representation saner - 8 bytes of millis | 4 bytes of nanos.
             * Also, it enables timestamp bytes to be directly compared with date/time bytes.   
             */
            Bytes.putInt(bytes, offset, value.getNanos() % 1000000);  
            size += Bytes.SIZEOF_INT;
            return size;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case DATE:
                return new Timestamp(((Date)object).getTime());
            case TIME:
                return new Timestamp(((Time)object).getTime());
            case TIMESTAMP:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, ColumnModifier columnModifier) {
            if (actualType == null || l == 0) {
                return null;
            }
            if (columnModifier != null) {
                b = columnModifier.apply(b, o, new byte[l], 0, l);
                o = 0;
            }
            switch (actualType) {
            case TIMESTAMP:
                long millisDeserialized = Bytes.toLong(b, o, Bytes.SIZEOF_LONG);
                Timestamp v = new Timestamp(millisDeserialized);
                int nanosDeserialized = Bytes.toInt(b, o + Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
                /*
                 * There was a bug in serialization of timestamps which was causing the sub-second millis part
                 * of time stamp to be present both in the LONG and INT bytes. Having the <100000 check
                 * makes this serialization fix backward compatible.
                 */
                v.setNanos(nanosDeserialized < 1000000 ? v.getNanos() + nanosDeserialized : nanosDeserialized);
                return v;
            case DATE:
            case TIME:
                return new Timestamp(getCodec().decodeLong(b, o, null));
            case DECIMAL:
                BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l);
                long ms = bd.longValue();
                int nanos = (bd.remainder(BigDecimal.ONE).multiply(QueryConstants.BD_MILLIS_NANOS_CONVERSION)).intValue();
                v = DateUtil.getTimestamp(ms, nanos);
                return v;
            default:
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
        }
        
        @Override
        public boolean isCastableTo(PDataType targetType) {
            return DATE.isCastableTo(targetType);
        }

       @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == TIMESTAMP) {
                return ((Timestamp)lhs).compareTo((Timestamp)rhs);
            }
            int c = ((Date)rhs).compareTo((Date)lhs);
            if (c != 0) return c;
            return ((Timestamp)lhs).getNanos();
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return DateUtil.parseTimestamp(value);
        }
        
        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            Timestamp value = (Timestamp)toObject(b,offset,length);
            if (formatter == null || formatter == DateUtil.DEFAULT_DATE_FORMATTER) {
                // If default formatter has not been overridden,
                // use one that displays milliseconds.
                formatter = DateUtil.DEFAULT_MS_DATE_FORMATTER;
            }
            return "'" + super.toStringLiteral(b, offset, length, formatter) + "." + value.getNanos() + "'";
        }
    },
    TIME("TIME", Types.TIME, Time.class, new DateCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return Bytes.toBytes(((Time)object).getTime());
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            Bytes.putLong(bytes, offset, ((Time)object).getTime());
            return this.getByteSize();
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP: // TODO: throw if nanos?
            case DATE:
            case TIME:
                return new Time(this.getCodec().decodeLong(b, o, null));
            default:
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case DATE:
                return new Time(((Date)object).getTime());
            case TIMESTAMP:
                return new Time(((Timestamp)object).getTime());
            case TIME:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public boolean isCastableTo(PDataType targetType) {
            return DATE.isCastableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == DATE || targetType == TIMESTAMP
                    || targetType == VARBINARY || targetType == BINARY;
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
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == TIMESTAMP) {
                return -TIMESTAMP.compareTo(rhs, lhs, TIME);
            }
            return ((Date)rhs).compareTo((Date)lhs);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return DateUtil.parseTime(value);
        }

        @Override
        public boolean isBytesComparableWith(PDataType otherType) {
            return super.isBytesComparableWith(otherType) ||  this == DATE;
        }
        
        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            if (formatter == null || formatter == DateUtil.DEFAULT_DATE_FORMATTER) {
                // If default formatter has not been overridden,
                // use one that displays milliseconds.
                formatter = DateUtil.DEFAULT_MS_DATE_FORMATTER;
            }
            return "'" + super.toStringLiteral(b, offset, length, formatter) + "'";
        }
    },
    DATE("DATE", Types.DATE, Date.class, new DateCodec()) { // After TIMESTAMP and DATE to ensure toLiteral finds those first

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return Bytes.toBytes(((Date)object).getTime());
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            Bytes.putLong(bytes, offset, ((Date)object).getTime());
            return this.getByteSize();
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case TIME:
                return new Date(((Time)object).getTime());
            case TIMESTAMP:
                return new Date(((Timestamp)object).getTime());
            case DATE:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP: // TODO: throw if nanos?
            case DATE:
            case TIME:
                return new Date(this.getCodec().decodeLong(b, o, null));
            default:
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
        }

        @Override
        public boolean isCastableTo(PDataType targetType) {
            return super.isCastableTo(targetType) || DECIMAL.isCastableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == TIME || targetType == TIMESTAMP
                    || targetType == VARBINARY || targetType == BINARY;
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
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return TIME.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return DateUtil.parseDate(value);
        }

        @Override
        public boolean isBytesComparableWith(PDataType otherType) {
            return super.isBytesComparableWith(otherType) || this == TIME;
        }
        
        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            if (formatter == null || formatter == DateUtil.DEFAULT_DATE_FORMATTER) {
                // If default formatter has not been overridden,
                // use one that displays milliseconds.
                formatter = DateUtil.DEFAULT_MS_DATE_FORMATTER;
            }
            return "'" + super.toStringLiteral(b, offset, length, formatter) + "'";
        }
    },
    /**
     * Unsigned long type that restricts values to be from 0 to {@link java.lang.Long#MAX_VALUE} inclusive. May be used to map to existing HTable values created through {@link org.apache.hadoop.hbase.util.Bytes#toBytes(long)}
     * as long as all values are non negative (the leading sign bit of negative numbers would cause them to sort ahead of positive numbers when
     * they're used as part of the row key when using the HBase utility methods).
     */
    UNSIGNED_LONG("UNSIGNED_LONG", 10 /* no constant available in Types */, Long.class, new UnsignedLongCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_LONG];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeLong(((Number)object).longValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case LONG:
            case UNSIGNED_LONG:
                long v = (Long) object;
                if (v < 0) {
                    throw new IllegalDataException("Value may not be negative(" + v + ")");
                }
                return v;
            case UNSIGNED_INT:
            case INTEGER:
                v = (Integer) object;
                if (v < 0) {
                    throw new IllegalDataException("Value may not be negative(" + v + ")");
                }
                return v;
            case SMALLINT:
            case UNSIGNED_SMALLINT:
                v = (Short) object;
                if (v < 0) {
                    throw new IllegalDataException("Value may not be negative(" + v + ")");
                }
                return v;
            case UNSIGNED_TINYINT:
            case TINYINT:
                v = (Byte) object;
                if (v < 0) {
                    throw new IllegalDataException("Value may not be negative(" + v + ")");
                }
                return v;
            case UNSIGNED_FLOAT:
            case FLOAT:
                Float f = (Float) object;
                v = f.longValue();
                if (v < 0) {
                    throw new IllegalDataException("Value may not be negative(" + v + ")");
                }
                return v;
            case UNSIGNED_DOUBLE:
            case DOUBLE:
                Double de = (Double) object;
                v = de.longValue();
                if (v < 0) {
                    throw new IllegalDataException("Value may not be negative(" + v + ")");
                }
                return v;
            case DECIMAL:
                BigDecimal d = (BigDecimal) object;
                if (d.signum() == -1) {
                    throw new IllegalDataException("Value may not be negative(" + d + ")");
                }
                return d.longValueExact();
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case INTEGER:
            case LONG:
            case UNSIGNED_LONG:
            case UNSIGNED_INT:
            case SMALLINT:
            case UNSIGNED_SMALLINT:
            case TINYINT:
            case UNSIGNED_TINYINT:
            case FLOAT:
            case UNSIGNED_FLOAT:
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                return actualType.getCodec().decodeLong(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == LONG || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY 
                    || targetType == FLOAT || targetType == DOUBLE || targetType == UNSIGNED_FLOAT
                    || targetType == UNSIGNED_DOUBLE;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                switch (targetType) {
                    case UNSIGNED_INT:
                    case INTEGER:
                        long l = (Long) value;
                        return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE);
                    case UNSIGNED_SMALLINT:
                    case SMALLINT:
                        long s = (Long)value;
                        return (s>=Short.MIN_VALUE && s<=Short.MAX_VALUE);
                    case TINYINT:
                        long t = (Long)value;
                        return (t>=Byte.MIN_VALUE && t<=Byte.MAX_VALUE);
                    case UNSIGNED_TINYINT:
                        t = (Long)value;
                        return (t>=0 && t<=Byte.MAX_VALUE);
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
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
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return -((BigDecimal)rhs).compareTo(BigDecimal.valueOf(((Number)lhs).longValue()));
            } else if (rhsType == DOUBLE || rhsType == FLOAT || rhsType == UNSIGNED_DOUBLE || rhsType == UNSIGNED_FLOAT) {
                return Doubles.compare(((Number)lhs).doubleValue(), ((Number)rhs).doubleValue());
            }
            return Longs.compare(((Number)lhs).longValue(), ((Number)rhs).longValue());
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                Long l = Long.parseLong(value);
                if (l.longValue() < 0) {
                    throw new IllegalDataException("Value may not be negative(" + l + ")");
                }
                return l;
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
        
        @Override
        public int getResultSetSqlType() {
            return LONG.getResultSetSqlType();
        }
    },
    /**
     * Unsigned integer type that restricts values to be from 0 to {@link java.lang.Integer#MAX_VALUE} inclusive. May be used to map to existing HTable values created through {@link org.apache.hadoop.hbase.util.Bytes#toBytes(int)}
     * as long as all values are non negative (the leading sign bit of negative numbers would cause them to sort ahead of positive numbers when
     * they're used as part of the row key when using the HBase utility methods).
     */
    UNSIGNED_INT("UNSIGNED_INT", 9 /* no constant available in Types */, Integer.class, new UnsignedIntCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_INT];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeInt(((Number)object).intValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            Object o = UNSIGNED_LONG.toObject(object, actualType);
            if(!(o instanceof Long) || o == null) {
                return o;
            }
            long l = (Long)o;
            if (l > Integer.MAX_VALUE) {
                throw new IllegalDataException(actualType + " value " + l + " cannot be cast to Unsigned Integer without changing its value");
            }
            return (int)l;
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case UNSIGNED_LONG:
            case LONG:
            case UNSIGNED_INT:
            case INTEGER:
            case SMALLINT:
            case UNSIGNED_SMALLINT:
            case TINYINT:
            case UNSIGNED_TINYINT:
            case FLOAT:
            case UNSIGNED_FLOAT:
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                return actualType.getCodec().decodeInt(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == INTEGER || UNSIGNED_LONG.isCoercibleTo(targetType);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                switch (targetType) {
                    case UNSIGNED_SMALLINT:
                        int s = (Integer)value;
                        return (s>=0 && s<=Short.MAX_VALUE);
                    case SMALLINT:
                        s = (Integer)value;
                        return (s>=Short.MIN_VALUE && s<=Short.MAX_VALUE);
                    case TINYINT:
                        s = (Integer)value;
                        return (s>=Byte.MIN_VALUE && s<=Byte.MAX_VALUE);
                    case UNSIGNED_TINYINT:
                        s = (Integer)value;
                        return (s>=0 && s<=Byte.MAX_VALUE);
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
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
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return LONG.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                Integer i = Integer.parseInt(value);
                if (i.intValue() < 0) {
                    throw new IllegalDataException("Value may not be negative(" + i + ")");
                }
                return i;
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
        
        @Override
        public int getResultSetSqlType() {
            return INTEGER.getResultSetSqlType();
        }
    },
    UNSIGNED_SMALLINT("UNSIGNED_SMALLINT", 13, Short.class, new UnsignedShortCodec()) {

      @Override
      public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
        return LONG.compareTo(lhs, rhs, rhsType);
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
      public Integer getScale(Object o) {
          return ZERO;
      }
      
      @Override
      public Integer getMaxLength(Object o) {
          return SHORT_PRECISION;
      }

      @Override
      public byte[] toBytes(Object object) {
        if (object == null) {
          throw new ConstraintViolationException(this + " may not be null");
        }
        byte[] b = new byte[Bytes.SIZEOF_INT];
        toBytes(object, b, 0);
        return b;
      }

      @Override
      public int toBytes(Object object, byte[] bytes, int offset) {
        if (object == null) {
          throw new ConstraintViolationException(this + " may not be null");
        }
        return this.getCodec().encodeShort(((Number)object).shortValue(), bytes, offset);
      }

      @Override
      public Object toObject(String value) {
        if (value == null || value.length() == 0) {
          return null;
        }
        try {
          Short b = Short.parseShort(value);
          if (b.shortValue()<0) {
              throw new IllegalDataException("Value may not be negative(" + b + ")");
          }
          return b;
        } catch (NumberFormatException e) {
          throw new IllegalDataException(e);
        }
      }
      
      @Override
      public Object toObject(Object object, PDataType actualType) {
          Object o = UNSIGNED_LONG.toObject(object, actualType);
          if(!(o instanceof Long) || o == null) {
              return o;
          }
          long l = (Long)o;
          if (l > Short.MAX_VALUE) {
              throw new IllegalDataException(actualType + " value " + l + " cannot be cast to Unsigned Short without changing its value");
          }
          return (short)l;
      }
      
      @Override
      public Object toObject(byte[] b, int o, int l, PDataType actualType) {
          if (l == 0) {
              return null;
          }
          switch (actualType) {
          case UNSIGNED_LONG:
          case LONG:
          case UNSIGNED_INT:
          case INTEGER:
          case UNSIGNED_SMALLINT:
          case SMALLINT:
          case UNSIGNED_TINYINT:
          case TINYINT:
          case UNSIGNED_FLOAT:
          case FLOAT:
          case UNSIGNED_DOUBLE:
          case DOUBLE:
              return actualType.getCodec().decodeShort(b, o, null);
          default:
              return super.toObject(b,o,l,actualType);
          }
      }
      
      @Override
      public boolean isComparableTo(PDataType targetType) {
          return DECIMAL.isComparableTo(targetType);
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType) {
          return this == targetType || targetType == SMALLINT || UNSIGNED_INT.isCoercibleTo(targetType);
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType, Object value) {
          if (value != null) {
              switch (targetType) {
                  case TINYINT:
                    short ts = (Short)value;
                    return (ts>=Byte.MIN_VALUE && ts<=Byte.MAX_VALUE);
                  case UNSIGNED_TINYINT:
                      short s = (Short)value;
                      return (s>=0 && s<=Byte.MAX_VALUE);
                  default:
                      break;
              }
          }
          return super.isCoercibleTo(targetType, value);
      }
      
      
      @Override
      public int getResultSetSqlType() {
          return SMALLINT.getResultSetSqlType();
      }
    },
    UNSIGNED_TINYINT("UNSIGNED_TINYINT", 11, Byte.class, new UnsignedByteCodec()) {

      @Override
      public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
        return LONG.compareTo(lhs, rhs, rhsType);
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
      public Integer getScale(Object o) {
          return ZERO;
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
          throw new ConstraintViolationException(this + " may not be null");
        }
        return this.getCodec().encodeByte(((Number)object).byteValue(), bytes, offset);
      }

      @Override
      public Object toObject(String value) {
        if (value == null || value.length() == 0) {
          return null;
        }
        try {
          Byte b = Byte.parseByte(value);
          if (b.byteValue()<0) {
              throw new IllegalDataException("Value may not be negative(" + b + ")");
          }
          return b;
        } catch (NumberFormatException e) {
          throw new IllegalDataException(e);
        }
      }
      
      @Override
      public Object toObject(Object object, PDataType actualType) {
          Object o = UNSIGNED_LONG.toObject(object, actualType);
          if(!(o instanceof Long) || o == null) {
              return o;
          }
          long l = (Long)o;
          if (l > Byte.MAX_VALUE) {
              throw new IllegalDataException(actualType + " value " + l + " cannot be cast to Unsigned Byte without changing its value");
          }
          return (byte)l;
      }
      
      @Override
      public Object toObject(byte[] b, int o, int l, PDataType actualType) {
          if (l == 0) {
              return null;
          }
          switch (actualType) {
          case UNSIGNED_LONG:
          case LONG:
          case UNSIGNED_INT:
          case INTEGER:
          case UNSIGNED_SMALLINT:
          case SMALLINT:
          case UNSIGNED_TINYINT:
          case TINYINT:
          case UNSIGNED_FLOAT:
          case FLOAT:
          case UNSIGNED_DOUBLE:
          case DOUBLE:
              return actualType.getCodec().decodeByte(b, o, null);
          default:
              return super.toObject(b,o,l,actualType);
          }
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType) {
          return this == targetType || targetType == TINYINT || UNSIGNED_SMALLINT.isCoercibleTo(targetType);
      }
      
      @Override
      public boolean isComparableTo(PDataType targetType) {
          return DECIMAL.isComparableTo(targetType);
      }
      
      
      @Override
      public int getResultSetSqlType() {
          return TINYINT.getResultSetSqlType();
      }
    },
    UNSIGNED_FLOAT("UNSIGNED_FLOAT", 14, Float.class, new UnsignedFloatCodec()) {

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return DOUBLE.compareTo(lhs, rhs, rhsType);
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
        public Integer getScale(Object o) {
            if (o == null) {
                return null;
            }
            Float v = (Float) o;
            BigDecimal bd = BigDecimal.valueOf(v);
            return bd.scale();
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
            byte[] b = new byte[Bytes.SIZEOF_INT];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
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
                    throw new IllegalDataException("Value may not be negative("
                            + f + ")");
                }
                return f;
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
        
        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case UNSIGNED_FLOAT:
                return object;
            case FLOAT:
                float f = (Float)object;
                if (f < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + f + ")");
                }
                return object;
            case UNSIGNED_DOUBLE:
            case DOUBLE:
                double d = (Double)object;
                if (Double.isNaN(d)
                        || d == Double.POSITIVE_INFINITY
                        || (d >= 0 && d <= Float.MAX_VALUE)) {
                    return (float) d;
                } else {
                    throw new IllegalDataException(actualType + " value " + d + " cannot be cast to Float without changing its value");
                }
            case LONG:
            case UNSIGNED_LONG:
                f = (Long)object;
                if (f < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + f + ")");
                }
                return f;
            case UNSIGNED_INT:
            case INTEGER:
                f = (Integer)object;
                if (f < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + f + ")");
                }
                return f;
            case TINYINT:
            case UNSIGNED_TINYINT:
                f = (Byte)object;
                if (f < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + f + ")");
                }
                return f;
            case SMALLINT:
            case UNSIGNED_SMALLINT:
                f = (Short)object;
                if (f < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + f + ")");
                }
                return f;
            case DECIMAL:
                BigDecimal dl = (BigDecimal)object;
                if (dl.signum() == -1) {
                    throw new IllegalDataException("Value may not be negative(" + dl + ")");
                }
                return dl.floatValue();
            default:
                return super.toObject(object, actualType);
            }
        }
        
        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l <= 0) {
                return null;
            }
            switch (actualType) {
            case UNSIGNED_FLOAT:
            case FLOAT:
            case UNSIGNED_DOUBLE:
            case DOUBLE:
            case UNSIGNED_LONG:
            case LONG:
            case UNSIGNED_INT:
            case INTEGER:
            case UNSIGNED_SMALLINT:
            case SMALLINT:
            case UNSIGNED_TINYINT:
            case TINYINT:
                return actualType.getCodec().decodeFloat(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                float f = (Float)value;
                switch (targetType) {
                case UNSIGNED_FLOAT:
                    return f >= 0;
                case UNSIGNED_LONG:
                    return (f >= 0 && f <= Long.MAX_VALUE);
                case LONG:
                    return (f >= Long.MIN_VALUE && f <= Long.MAX_VALUE);
                case UNSIGNED_INT:
                    return (f >= 0 && f <= Integer.MAX_VALUE);
                case INTEGER:
                    return (f >= Integer.MIN_VALUE && f <= Integer.MAX_VALUE);
                case UNSIGNED_SMALLINT:
                    return (f >= 0 && f <= Short.MAX_VALUE);
                case SMALLINT:
                    return (f >=Short.MIN_VALUE && f<=Short.MAX_VALUE);
                case TINYINT:
                    return (f >=Byte.MIN_VALUE && f<Byte.MAX_VALUE);
                case UNSIGNED_TINYINT:
                    return (f >=0 && f<Byte.MAX_VALUE);
                default:
                    break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == FLOAT || UNSIGNED_DOUBLE.isCoercibleTo(targetType);
        }
        
        
        @Override
        public int getResultSetSqlType() {
            return FLOAT.getResultSetSqlType();
        }
    },
    UNSIGNED_DOUBLE("UNSIGNED_DOUBLE", 15, Double.class, new UnsignedDoubleCodec()) {

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return -((BigDecimal)rhs).compareTo(BigDecimal.valueOf(((Number)lhs).doubleValue()));
            }
            return Doubles.compare(((Number)lhs).doubleValue(), ((Number)rhs).doubleValue());
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
        public Integer getScale(Object o) {
            if (o == null) {
                return null;
            }
            Double v = (Double) o;
            BigDecimal bd = BigDecimal.valueOf(v);
            return bd.scale();
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
            byte[] b = new byte[Bytes.SIZEOF_LONG];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
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
                    throw new IllegalDataException("Value may not be negative("
                            + d + ")");
                }
                return d;
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
        
        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            double de;
            switch (actualType) {
            case UNSIGNED_DOUBLE:
                return object;
            case DOUBLE:
                de = (Double)object;
                if (de < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + de + ")");
                }
                return object;
            case UNSIGNED_FLOAT:
            case FLOAT:
                de = (Float)object;
                if (de < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + de + ")");
                }
                return de;
            case LONG:
            case UNSIGNED_LONG:
                de = (Long)object;
                if (de < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + de + ")");
                }
                return de;
            case UNSIGNED_INT:
            case INTEGER:
                de = (Integer)object;
                if (de < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + de + ")");
                }
                return de;
            case TINYINT:
            case UNSIGNED_TINYINT:
                de = (Byte)object;
                if (de < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + de + ")");
                }
                return de;
            case SMALLINT:
            case UNSIGNED_SMALLINT:
                de = (Short)object;
                if (de < 0) {
                    throw new IllegalDataException("Value may not be negative("
                            + de + ")");
                }
                return de;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                if (d.signum() == -1) {
                    throw new IllegalDataException("Value may not be negative(" + d + ")");
                }
                return d.doubleValue();
            default:
                return super.toObject(object, actualType);
            }
        }
        
        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l <= 0) {
                return null;
            }
            switch (actualType) {
            case UNSIGNED_DOUBLE:
            case DOUBLE:
            case UNSIGNED_FLOAT:
            case FLOAT:
            case UNSIGNED_LONG:
            case LONG:
            case UNSIGNED_INT:
            case INTEGER:
            case UNSIGNED_SMALLINT:
            case SMALLINT:
            case UNSIGNED_TINYINT:
            case TINYINT:
                return actualType.getCodec().decodeDouble(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                double d = (Double)value;
                switch (targetType) {
                case UNSIGNED_FLOAT:
                    return Double.isNaN(d) || d == Double.POSITIVE_INFINITY
                            || (d >= 0 && d <= Float.MAX_VALUE);
                case FLOAT:
                    return Double.isNaN(d)
                            || d == Double.POSITIVE_INFINITY
                            || (d >= -Float.MAX_VALUE && d <= Float.MAX_VALUE);
                case UNSIGNED_LONG:
                    return (d >= 0 && d <= Long.MAX_VALUE);
                case LONG:
                    return (d >= Long.MIN_VALUE && d <= Long.MAX_VALUE);
                case UNSIGNED_INT:
                    return (d >= 0 && d <= Integer.MAX_VALUE);
                case INTEGER:
                    return (d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE);
                case UNSIGNED_SMALLINT:
                    return (d >= 0 && d <= Short.MAX_VALUE);
                case SMALLINT:
                    return (d >=Short.MIN_VALUE && d<=Short.MAX_VALUE);
                case TINYINT:
                    return (d >=Byte.MIN_VALUE && d<Byte.MAX_VALUE);
                case UNSIGNED_TINYINT:
                    return (d >=0 && d<Byte.MAX_VALUE);
                default:
                    break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || DOUBLE.isCoercibleTo(targetType);
        }
        
        
        @Override
        public int getResultSetSqlType() {
            return  DOUBLE.getResultSetSqlType();
        }
    },
    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, null) {

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return ((Boolean)object).booleanValue() ? TRUE_BYTES : FALSE_BYTES;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            bytes[offset] = ((Boolean)object).booleanValue() ? TRUE_BYTE : FALSE_BYTE;
            return BOOLEAN_LENGTH;
        }

        @Override
        public byte[] toBytes(Object object, ColumnModifier columnModifier) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            // Override to prevent any byte allocation
            if (columnModifier == null) {
                return ((Boolean)object).booleanValue() ? TRUE_BYTES : FALSE_BYTES;
            }
            return ((Boolean)object).booleanValue() ? MOD_TRUE_BYTES[columnModifier.ordinal()] : MOD_FALSE_BYTES[columnModifier.ordinal()];
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType targetType) {
            if (!isCoercibleTo(targetType)) {
                throw new ConstraintViolationException(this + " cannot be coerced to " + targetType);
            }
            return length == 0 ? null : bytes[offset] == FALSE_BYTE ? Boolean.FALSE : Boolean.TRUE;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return BOOLEAN_LENGTH;
        }

        @Override
        public int estimateByteSize(Object o) {
            return BOOLEAN_LENGTH;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return Booleans.compare((Boolean)lhs, (Boolean)rhs);
        }

        @Override
        public Object toObject(String value) {
            return Boolean.parseBoolean(value);
        }
    },
    VARBINARY("VARBINARY", Types.VARBINARY, byte[].class, null) {
        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                return ByteUtil.EMPTY_BYTE_ARRAY;
            }
            return (byte[])object;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                return 0;
            }
            byte[] o = (byte[])object;
            // assumes there's enough room
            System.arraycopy(bytes, offset, o, 0, o.length);
            return o.length;
        }

        /**
         * Override because we must always create a new byte array
         */
        @Override
        public byte[] toBytes(Object object, ColumnModifier columnModifier) {
            byte[] bytes = toBytes(object);
            // Override because we need to allocate a new buffer in this case
            if (columnModifier != null) {
                return columnModifier.apply(bytes, 0, new byte[bytes.length], 0, bytes.length);
            }
            return bytes;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (length == 0) {
                return null;
            }
            if (offset == 0 && bytes.length == length) {
                return bytes;
            }
            byte[] o = new byte[length];
            System.arraycopy(bytes, offset, o, 0, length);
            return o;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            return actualType.toBytes(object);
        }

        @Override
        public boolean isFixedWidth() {
            return false;
        }

        @Override
        public int estimateByteSize(Object o) {
            byte[] value = (byte[]) o;
            return value == null ? 1 : value.length;
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == BINARY;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if (srcType == PDataType.BINARY && maxLength != null && desiredMaxLength != null) {
                return maxLength <= desiredMaxLength;
            }
            return true;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (lhs == null && rhs == null) {
                return 0;
            } else if (lhs == null) {
                return -1;
            } else if (rhs == null) {
                return 1;
            }
            if (rhsType == PDataType.VARBINARY || rhsType == PDataType.BINARY) {
                return Bytes.compareTo((byte[])lhs, (byte[])rhs);
            } else {
                byte[] rhsBytes = rhsType.toBytes(rhs);
                return Bytes.compareTo((byte[])lhs, rhsBytes);
            }
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return Base64.decode(value);
        }
        
        @Override
        public String toStringLiteral(byte[] b, int o, int length, Format formatter) {
            if (formatter != null) {
                return formatter.format(b);
            }
            StringBuilder buf = new StringBuilder();
            buf.append('[');
            for (int i = 0; i < b.length; i++) {
                buf.append(0xFF & b[i]);
                buf.append(',');
            }
            buf.setCharAt(buf.length()-1, ']');
            return buf.toString();
        }
    },
    BINARY("BINARY", Types.BINARY, byte[].class, null) {
        @Override
        public byte[] toBytes(Object object) { // Deligate to VARBINARY
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return VARBINARY.toBytes(object);
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return VARBINARY.toBytes(object, bytes, offset);
            
        }

        @Override
        public byte[] toBytes(Object object, ColumnModifier columnModifier) {
            byte[] bytes = toBytes(object);
            if (columnModifier != null) {
                return columnModifier.apply(bytes, 0, new byte[bytes.length], 0, bytes.length);
            }
            return bytes;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (!actualType.isCoercibleTo(this)) {
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            return VARBINARY.toObject(bytes, offset, length, actualType);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            return actualType.toBytes(object);
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public int estimateByteSize(Object o) {
            byte[] value = (byte[]) o;
            return value == null ? 1 : value.length;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == VARBINARY;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if ((srcType == PDataType.VARBINARY && ((String)value).length() != b.length) ||
                    (maxLength != null && desiredMaxLength != null && maxLength > desiredMaxLength)){
                return false;
            }
            return true;
        }

        @Override
        public Integer estimateByteSizeFromLength(Integer length) {
            return length;
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (lhs == null && rhs == null) {
                return 0;
            } else if (lhs == null) {
                return -1;
            } else if (rhs == null) {
                return 1;
            }
            if (rhsType == PDataType.VARBINARY || rhsType == PDataType.BINARY) {
                return Bytes.compareTo((byte[])lhs, (byte[])rhs);
            } else {
                byte[] rhsBytes = rhsType.toBytes(rhs);
                return Bytes.compareTo((byte[])lhs, rhsBytes);
            }
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return Base64.decode(value);
        }
        
        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            if (formatter == null && b.length == 1) {
                return Integer.toString(0xFF & b[0]);
            }
            return VARBINARY.toStringLiteral(b, offset, length, formatter);
        }
    },
    ;

    private static final BigDecimal MIN_DOUBLE_AS_BIG_DECIMAL = BigDecimal.valueOf(-Double.MAX_VALUE);
    private static final BigDecimal MAX_DOUBLE_AS_BIG_DECIMAL = BigDecimal.valueOf(Double.MAX_VALUE);
    private static final BigDecimal MIN_FLOAT_AS_BIG_DECIMAL = BigDecimal.valueOf(-Float.MAX_VALUE);
    private static final BigDecimal MAX_FLOAT_AS_BIG_DECIMAL = BigDecimal.valueOf(Float.MAX_VALUE);
    private final String sqlTypeName;
    private final int sqlType;
    private final Class clazz;
    private final byte[] clazzNameBytes;
    private final byte[] sqlTypeNameBytes;
    private final PDataCodec codec;

    private PDataType(String sqlTypeName, int sqlType, Class clazz, PDataCodec codec) {
        this.sqlTypeName = sqlTypeName;
        this.sqlType = sqlType;
        this.clazz = clazz;
        this.clazzNameBytes = Bytes.toBytes(clazz.getName());
        this.sqlTypeNameBytes = Bytes.toBytes(sqlTypeName);
        this.codec = codec;
    }

    public boolean isCastableTo(PDataType targetType) {
        return isComparableTo(targetType);
    }

    public final PDataCodec getCodec() {
        return codec;
    }

    public boolean isBytesComparableWith(PDataType otherType) {
        return this == otherType || this == PDataType.VARBINARY || otherType == PDataType.VARBINARY || this == PDataType.BINARY || otherType == PDataType.BINARY;
    }

    public int estimateByteSize(Object o) {
        if (isFixedWidth()) {
            return getByteSize();
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

    public final int compareTo(byte[] lhs, int lhsOffset, int lhsLength, ColumnModifier lhsColumnModifier,
                               byte[] rhs, int rhsOffset, int rhsLength, ColumnModifier rhsColumnModifier, PDataType rhsType) {
        if (this.isBytesComparableWith(rhsType)) { // directly compare the bytes
            return compareTo(lhs, lhsOffset, lhsLength, lhsColumnModifier, rhs, rhsOffset, rhsLength, rhsColumnModifier);
        }
        PDataCodec lhsCodec = this.getCodec();
        if (lhsCodec == null) { // no lhs native type representation, so convert rhsType to bytes representation of lhsType
            byte[] rhsConverted = this.toBytes(this.toObject(rhs, rhsOffset, rhsLength, rhsType, rhsColumnModifier));            
            if (rhsColumnModifier != null) {
                rhsColumnModifier = null;
            }            
            if (lhsColumnModifier != null) {
                lhs = lhsColumnModifier.apply(lhs, lhsOffset, new byte[lhsLength], 0, lhsLength);
            }                        
            return Bytes.compareTo(lhs, lhsOffset, lhsLength, rhsConverted, 0, rhsConverted.length);
        }
        PDataCodec rhsCodec = rhsType.getCodec();
        if (rhsCodec == null) {
            byte[] lhsConverted = rhsType.toBytes(rhsType.toObject(lhs, lhsOffset, lhsLength, this, lhsColumnModifier));
            if (lhsColumnModifier != null) {
                lhsColumnModifier = null;
            }
            if (rhsColumnModifier != null) {
                rhs = rhsColumnModifier.apply(rhs, rhsOffset, new byte[rhsLength], 0, rhsLength);
            }            
            return Bytes.compareTo(lhsConverted, 0, lhsConverted.length, rhs, rhsOffset, rhsLength);
        }
        // convert to native and compare
        if(this.isCoercibleTo(PDataType.LONG) && rhsType.isCoercibleTo(PDataType.LONG)) { // native long to long comparison
            return Longs.compare(this.getCodec().decodeLong(lhs, lhsOffset, lhsColumnModifier), rhsType.getCodec().decodeLong(rhs, rhsOffset, rhsColumnModifier));
        } else if (isDoubleOrFloat(this) && isDoubleOrFloat(rhsType)) { // native double to double comparison
            return Doubles.compare(this.getCodec().decodeDouble(lhs, lhsOffset, lhsColumnModifier), rhsType.getCodec().decodeDouble(rhs, rhsOffset, rhsColumnModifier));
        } else { // native float/double to long comparison
            float fvalue = 0.0F;
            double dvalue = 0.0;
            long lvalue = 0;
            boolean isFloat = false;
            int invert = 1;
            
            if (this.isCoercibleTo(PDataType.LONG)) {
                lvalue = this.getCodec().decodeLong(lhs, lhsOffset, lhsColumnModifier);
            } else if (this == PDataType.FLOAT) {
                isFloat = true;
                fvalue = this.getCodec().decodeFloat(lhs, lhsOffset, lhsColumnModifier);
            } else if (this.isCoercibleTo(PDataType.DOUBLE)) {
                dvalue = this.getCodec().decodeDouble(lhs, lhsOffset, lhsColumnModifier);
            }
            if (rhsType.isCoercibleTo(PDataType.LONG)) {
                lvalue = rhsType.getCodec().decodeLong(rhs, rhsOffset, rhsColumnModifier);
            } else if (rhsType == PDataType.FLOAT) {
                invert = -1;
                isFloat = true;
                fvalue = rhsType.getCodec().decodeFloat(rhs, rhsOffset, rhsColumnModifier);
            } else if (rhsType.isCoercibleTo(PDataType.DOUBLE)) {
                invert = -1;
                dvalue = rhsType.getCodec().decodeDouble(rhs, rhsOffset, rhsColumnModifier);
            }
            // Invert the comparison if float/double value is on the RHS
            return invert * (isFloat ? compareFloatToLong(fvalue, lvalue) : compareDoubleToLong(dvalue, lvalue));
        }
    }
    
    public static boolean isDoubleOrFloat(PDataType type){
        return type == PDataType.FLOAT || type == PDataType.DOUBLE
                || type == PDataType.UNSIGNED_FLOAT || type == PDataType.UNSIGNED_DOUBLE;
    }
    
    /**
     * Compares a float against a long. Behaves better than
     * {@link #compareDoubleToLong(double, long)} for float
     * values outside of Integer.MAX_VALUE and Integer.MIN_VALUE.
     * @param f a float value
     * @param l a long value
     * @return -1 if f is less than l, 1 if f is greater than l, and 0 if f is equal to l
     */
    private static int compareFloatToLong(float f, long l) {
        if (f > Integer.MAX_VALUE || f < Integer.MIN_VALUE) {
            return f < l ? -1 : f > l ? 1 : 0;
        }
        long diff = (long)f - l;
        return Long.signum(diff);
    }

    /**
     * Compares a double against a long.
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
        long diff = (long)d - l;
        return Long.signum(diff);
    }

    public static interface PDataCodec {
        public long decodeLong(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier);
        public int decodeInt(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier);
        public byte decodeByte(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier);
        public short decodeShort(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public short decodeShort(byte[] b, int o, ColumnModifier columnModifier);
        public float decodeFloat(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier);
        public double decodeDouble(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public double decodeDouble(byte[] b, int o, ColumnModifier columnModifier);

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
    }

    public static abstract class BaseCodec implements PDataCodec {
        @Override
        public int decodeInt(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeInt(ptr.get(), ptr.getOffset(), columnModifier);
        }

        @Override
        public long decodeLong(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeLong(ptr.get(),ptr.getOffset(), columnModifier);
        }

        @Override
        public byte decodeByte(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeByte(ptr.get(), ptr.getOffset(), columnModifier);
        }
        
        @Override
        public short decodeShort(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeShort(ptr.get(), ptr.getOffset(), columnModifier);
        }
        
        @Override
        public float decodeFloat(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeFloat(ptr.get(), ptr.getOffset(), columnModifier);
        }
        
        @Override
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public double decodeDouble(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeDouble(ptr.get(), ptr.getOffset(), columnModifier);
        }
        
        @Override
        public double decodeDouble(byte[] b, int o, ColumnModifier columnModifier) {
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


    public static class LongCodec extends BaseCodec {

        private LongCodec() {
        }
        
        @Override
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
            return decodeLong(b, o, columnModifier);
        }
        
        @Override
        public double decodeDouble(byte[] b, int o, ColumnModifier columnModifier) {
            return decodeLong(b, o, columnModifier);
        }

        @Override
        public long decodeLong(byte[] bytes, int o, ColumnModifier columnModifier) {
            long v;
            byte b = bytes[o];
            if (columnModifier == null) {
                v = b ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
                    b = bytes[o + i];
                    v = (v << 8) + (b & 0xff);
                }
            } else { // ColumnModifier.SORT_DESC
                b = (byte)(b ^ 0xff);
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
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            long v = decodeLong(b, o, columnModifier);
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Integer without changing its value");
            }
            return (int)v;
        }
        
        @Override
        public int encodeFloat(float v, byte[] b, int o) {
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Long without changing its value");
            }
            return encodeLong((long)v, b, o);
        }
        
        @Override
        public int encodeDouble(double v, byte[] b, int o) {
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Long without changing its value");
            }
            return encodeLong((long)v, b, o);
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
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
        public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier) {
          long v = decodeLong(b, o, columnModifier);
          if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
          }
          return (byte)v;
        }

        @Override
        public short decodeShort(byte[] b, int o, ColumnModifier columnModifier) {
          long v = decodeLong(b, o, columnModifier);
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be cast to Short without changing its value");
          }
          return (short)v;
        }
        
        @Override
        public int encodeByte(byte v, byte[] b, int o) {
            return encodeLong(v, b, o);
        }
        
        @Override
        public int encodeShort(short v, byte[] b, int o) {
          return encodeLong(v, b, o);
        }
    }

    public static class IntCodec extends BaseCodec {

        private IntCodec() {
        }

        @Override
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
            return decodeInt(b, o, columnModifier);
        }
        
        @Override
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
            return decodeInt(b, o, columnModifier);
        }
        
        @Override
        public double decodeDouble(byte[] b, int o,
                ColumnModifier columnModifier) {
            return decodeInt(b, o, columnModifier);
        }

        @Override
        public int decodeInt(byte[] bytes, int o, ColumnModifier columnModifier) {            
            int v;
            if (columnModifier == null) {
                v = bytes[o] ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
                    v = (v << 8) + (bytes[o + i] & 0xff);
                }
            } else { // ColumnModifier.SORT_DESC
                v = bytes[o] ^ 0xff ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
                    v = (v << 8) + ((bytes[o + i] ^ 0xff) & 0xff);
                }
            }
            return v;
        }

        @Override
        public int encodeInt(int v, byte[] b, int o) {
            b[o + 0] = (byte) ((v >> 24) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
            b[o + 1] = (byte) (v >> 16);
            b[o + 2] = (byte) (v >> 8);
            b[o + 3] = (byte) v;
            return Bytes.SIZEOF_INT;
        }
        
        @Override
        public int encodeFloat(float v, byte[] b, int o) {
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Integer without changing its value");
            }
            return encodeInt((int)v, b, o);
        }
        
        @Override
        public int encodeDouble(double v, byte[] b, int o) {
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Integer without changing its value");
            }
            return encodeInt((int)v, b, o);
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Integer without changing its value");
            }
            return encodeInt((int)v,b,o);
        }

        @Override
        public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier) {
          int v = decodeInt(b, o, columnModifier);
          if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
          }
          return (byte)v;
        }

        @Override
        public short decodeShort(byte[] b, int o, ColumnModifier columnModifier) {
          int v = decodeInt(b, o, columnModifier);
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be cast to Short without changing its value");
          }
          return (short)v;
        }
        
        @Override
        public int encodeByte(byte v, byte[] b, int o) {
            return encodeInt(v, b, o);
        }
        
        @Override
        public int encodeShort(short v, byte[] b, int o) {
          return encodeInt(v, b, o);
        }
    }
    
    public static class ShortCodec extends BaseCodec {

      private ShortCodec(){
      }
      
      @Override
      public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
        return decodeShort(b, o, columnModifier);
      }

      @Override
      public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
        return decodeShort(b, o, columnModifier);
      }

      @Override
      public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier) {
        short v = decodeShort(b, o, columnModifier);
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
            throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
        }
        return (byte)v;
      }

      @Override
      public short decodeShort(byte[] b, int o, ColumnModifier columnModifier) {
        int v;
        if (columnModifier == null) {
            v = b[o] ^ 0x80; // Flip sign bit back
            for (int i = 1; i < Bytes.SIZEOF_SHORT; i++) {
                v = (v << 8) + (b[o + i] & 0xff);
            }
        } else { // ColumnModifier.SORT_DESC
            v = b[o] ^ 0xff ^ 0x80; // Flip sign bit back
            for (int i = 1; i < Bytes.SIZEOF_SHORT; i++) {
                v = (v << 8) + ((b[o + i] ^ 0xff) & 0xff);
            }
        }
        return (short)v;
      }
      
      @Override
      public int encodeShort(short v, byte[] b, int o) {
          b[o + 0] = (byte) ((v >> 8) ^ 0x80); // Flip sign bit so that Short is binary comparable
          b[o + 1] = (byte) v;
          return Bytes.SIZEOF_SHORT;
      }

      @Override
      public int encodeLong(long v, byte[] b, int o) {
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
          }
          return encodeShort((short)v,b,o);
      }
      
      @Override
      public int encodeInt(int v, byte[] b, int o) {
        if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
          throw new IllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
        }
        return encodeShort((short)v,b,o);
      }
      
      @Override
      public int encodeByte(byte v, byte[] b, int o) {
        return encodeShort(v,b,o);
      }
      
      @Override
      public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
          return decodeShort(b, o, columnModifier);
      }
      
      @Override
      public double decodeDouble(byte[] b, int o,
              ColumnModifier columnModifier) {
          return decodeShort(b, o, columnModifier);
      }
      
      @Override
      public int encodeDouble(double v, byte[] b, int o) {
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
          }
          return encodeShort((short)v,b,o);
      }
    
      @Override
      public int encodeFloat(float v, byte[] b, int o) {
          if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be encoded as an Short without changing its value");
          }
          return encodeShort((short)v,b,o);
      }
    }
    
    public static class ByteCodec extends BaseCodec {

      private ByteCodec(){
      }
      
      @Override
      public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
        return decodeByte(b, o, columnModifier);
      }

      @Override
      public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
        return decodeByte(b, o, columnModifier);
      }

      @Override
      public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier) {
        int v;
        if (columnModifier == null) {
            v = b[o] ^ 0x80; // Flip sign bit back
        } else { // ColumnModifier.SORT_DESC
            v = b[o] ^ 0xff ^ 0x80; // Flip sign bit back
        }
        return (byte)v;
      }

      @Override
      public short decodeShort(byte[] b, int o, ColumnModifier columnModifier) {
          return decodeByte(b, o, columnModifier);
      }
      
      @Override
      public int encodeShort(short v, byte[] b, int o) {
          if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be encoded as an Byte without changing its value");
          }
          return encodeByte((byte)v,b,o);
      }

      @Override
      public int encodeLong(long v, byte[] b, int o) {
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
          throw new IllegalDataException("Value " + v + " cannot be encoded as an Byte without changing its value");
        }
        return encodeByte((byte)v,b,o);
      }
      
      @Override
      public int encodeInt(int v, byte[] b, int o) {
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
          throw new IllegalDataException("Value " + v + " cannot be encoded as an Byte without changing its value");
        }
        return encodeByte((byte)v,b,o);
      }
      
      @Override
      public int encodeByte(byte v, byte[] b, int o) {
        b[o] = (byte) (v ^ 0x80); // Flip sign bit so that Short is binary comparable
        return Bytes.SIZEOF_BYTE;
      }
        @Override
        public double decodeDouble(byte[] b, int o,
                ColumnModifier columnModifier) {
            return decodeByte(b, o, columnModifier);
        }

        @Override
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
            return decodeByte(b, o, columnModifier);
        }
      
        @Override
        public int encodeFloat(float v, byte[] b, int o) {
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Byte without changing its value");
            }
            return encodeByte((byte)v,b,o);
        }
        
        @Override
        public int encodeDouble(double v, byte[] b, int o) {
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Byte without changing its value");
            }
            return encodeByte((byte)v,b,o);
        }
    }
    
    public static class UnsignedByteCodec extends ByteCodec {

      private UnsignedByteCodec(){  
      }

      @Override
      public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier) {
        if (columnModifier != null) {
          b = columnModifier.apply(b, o, new byte[Bytes.SIZEOF_BYTE], 0, Bytes.SIZEOF_BYTE);
        }
        byte v = b[o];
        if (v < 0) {
          throw new IllegalDataException();
        }
        return v;
      }
      
      @Override
      public int encodeByte(byte v, byte[] b, int o) {
        if (v < 0) {
          throw new IllegalDataException();
        }
        Bytes.putByte(b, o, v);
        return Bytes.SIZEOF_BYTE;
      }
    }

    public static class UnsignedLongCodec extends LongCodec {

        private UnsignedLongCodec() {
        }

        @Override
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
            long v = 0;
            if (columnModifier == null) {
                for(int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
                  v <<= 8;
                  v ^= b[i] & 0xFF;
                }
            } else { // ColumnModifier.SORT_DESC
                for(int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
                    v <<= 8;
                    v ^= (b[i] & 0xFF) ^ 0xFF;
                  }
            }
            if (v < 0) {
                throw new IllegalDataException();
            }
            return v;
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            if (v < 0) {
                throw new IllegalDataException();
            }
            Bytes.putLong(b, o, v);
            return Bytes.SIZEOF_LONG;
        }
    }
    
    public static class UnsignedShortCodec extends ShortCodec {
      private UnsignedShortCodec(){
      }
      
      @Override
      public short decodeShort(byte[] b, int o, ColumnModifier columnModifier) {
          if (columnModifier != null) {
              b = columnModifier.apply(b, o, new byte[Bytes.SIZEOF_INT], 0, Bytes.SIZEOF_INT);
          }
          short v = Bytes.toShort(b, o);
          if (v < 0) {
              throw new IllegalDataException();
          }
          return v;
      }

      @Override
      public int encodeShort(short v, byte[] b, int o) {
          if (v < 0) {
              throw new IllegalDataException();
          }
          Bytes.putShort(b, o, v);
          return Bytes.SIZEOF_SHORT;
      }
    }

    public static class UnsignedIntCodec extends IntCodec {

        private UnsignedIntCodec() {
        }

        @Override
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            if (columnModifier != null) {
                b = columnModifier.apply(b, o, new byte[Bytes.SIZEOF_INT], 0, Bytes.SIZEOF_INT);
            }
            int v = Bytes.toInt(b, o);
            if (v < 0) {
                throw new IllegalDataException();
            }
            return v;
        }

        @Override
        public int encodeInt(int v, byte[] b, int o) {
            if (v < 0) {
                throw new IllegalDataException();
            }
            Bytes.putInt(b, o, v);
            return Bytes.SIZEOF_INT;
        }
    }
    
    public static class FloatCodec extends BaseCodec {

        private FloatCodec(){
        }
        
        @Override
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
            float v = decodeFloat(b, o, columnModifier);
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Long without changing its value");
            }
            return (long)v;
        }

        @Override
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            float v = decodeFloat(b, o, columnModifier);
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Integer without changing its value");
            }
            return (int) v;
        }

        @Override
        public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier) {
            float v = decodeFloat(b, o, columnModifier);
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
            }
            return (byte) v;
        }

        @Override
        public short decodeShort(byte[] b, int o, ColumnModifier columnModifier) {
            float v = decodeFloat(b, o, columnModifier);
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Short without changing its value");
            }
            return (short) v;
        }

        @Override
        public double decodeDouble(byte[] b, int o,
                ColumnModifier columnModifier) {
            return decodeFloat(b, o, columnModifier);
        }
        
        @Override
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
            if (columnModifier != null) {// ColumnModifier.SORT_DESC
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
                return encodeFloat((float)v, b, o);
            } else {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Float without changing its value");
            }
            
        }
        
        @Override
        public int encodeFloat(float v, byte[] b, int o) {
            int i = Float.floatToIntBits(v);
            i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1;
            Bytes.putInt(b, o, i);
            return Bytes.SIZEOF_INT;
        }
    }
    
    public static class DoubleCodec extends BaseCodec {

        private DoubleCodec(){
        }
        
        @Override
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
            double v = decodeDouble(b, o, columnModifier);
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Long without changing its value");
            }
            return (long) v;
        }

        @Override
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            double v = decodeDouble(b, o, columnModifier);
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Integer without changing its value");
            }
            return (int) v;
        }

        @Override
        public byte decodeByte(byte[] b, int o, ColumnModifier columnModifier) {
            double v = decodeDouble(b, o, columnModifier);
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
            }
            return (byte) v;
        }

        @Override
        public short decodeShort(byte[] b, int o, ColumnModifier columnModifier) {
            double v = decodeDouble(b, o, columnModifier);
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Short without changing its value");
            }
            return (short) v;
        }
        
        @Override
        public double decodeDouble(byte[] b, int o, ColumnModifier columnModifier) {
            if (columnModifier != null) {// ColumnModifier.SORT_DESC
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
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
            double v = decodeDouble(b, o, columnModifier);
            if (Double.isNaN(v) || v == Double.NEGATIVE_INFINITY
                    || v == Double.POSITIVE_INFINITY
                    || (v >= -Float.MAX_VALUE && v <= Float.MAX_VALUE)) {
                return (float) v;
            } else {
                throw new IllegalDataException("Value " + v + " cannot be cast to Float without changing its value");
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
            long l = Double.doubleToLongBits(v);
            l = (l ^ ((l >> Long.SIZE - 1) | Long.MIN_VALUE)) + 1;
            Bytes.putLong(b, o, l);
            return Bytes.SIZEOF_LONG;
        }
        
        @Override
        public int encodeFloat(float v, byte[] b, int o) {
            return encodeDouble(v, b, o);
        }
        
    }
    
    public static class UnsignedFloatCodec extends FloatCodec {
        private UnsignedFloatCodec() {
        }
        
        @Override
        public int encodeFloat(float v, byte[] b, int o) {
            if (v < 0) {
                throw new IllegalDataException();
            }
            Bytes.putFloat(b, o, v);
            return Bytes.SIZEOF_FLOAT;
        }
        
        @Override
        public float decodeFloat(byte[] b, int o, ColumnModifier columnModifier) {
            if (columnModifier != null) {
                b = columnModifier.apply(b, o, new byte[Bytes.SIZEOF_FLOAT], 0, Bytes.SIZEOF_FLOAT);
            }
            float v = Bytes.toFloat(b, o);
            if (v < 0) {
                throw new IllegalDataException();
            }
            return v;
        }
    }
    
    public static class UnsignedDoubleCodec extends DoubleCodec {
        private UnsignedDoubleCodec() {
        }
        
        @Override
        public int encodeDouble(double v, byte[] b, int o) {
            if (v < 0) {
                throw new IllegalDataException();
            }
            Bytes.putDouble(b, o, v);
            return Bytes.SIZEOF_DOUBLE;
        }
        
        @Override
        public double decodeDouble(byte[] b, int o,
                ColumnModifier columnModifier) {
            if (columnModifier != null) {
                b = columnModifier.apply(b, o, new byte[Bytes.SIZEOF_DOUBLE], 0, Bytes.SIZEOF_DOUBLE);
            }
            double v = Bytes.toDouble(b, o);
            if (v < 0) {
                throw new IllegalDataException();
            }
            return v;
        }
    }

    public static class DateCodec extends UnsignedLongCodec {

        private DateCodec() {
        }

        @Override
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            throw new UnsupportedOperationException();
        }
    }

    public static final int MAX_PRECISION = 38; // Max precision guaranteed to fit into a long (and this should be plenty)
    public static final int MIN_DECIMAL_AVG_SCALE = 4;
    public static final MathContext DEFAULT_MATH_CONTEXT = new MathContext(MAX_PRECISION, RoundingMode.HALF_UP);
    public static final int DEFAULT_SCALE = 0;

    private static final Integer MAX_BIG_DECIMAL_BYTES = 21;

    private static final byte ZERO_BYTE = (byte)0x80;
    private static final byte NEG_TERMINAL_BYTE = (byte)102;
    private static final int EXP_BYTE_OFFSET = 65;
    private static final int POS_DIGIT_OFFSET = 1;
    private static final int NEG_DIGIT_OFFSET = 101;
    private static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
    private static final long MAX_LONG_FOR_DESERIALIZE = Long.MAX_VALUE / 1000;
    private static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);

    private static final byte FALSE_BYTE = 0;
    private static final byte TRUE_BYTE = 1;
    public static final byte[] FALSE_BYTES = new byte[] {FALSE_BYTE};
    public static final byte[] TRUE_BYTES = new byte[] {TRUE_BYTE};
    public static final byte[] MOD_FALSE_BYTES[] = new byte[ColumnModifier.values().length][];
    static {
        for (ColumnModifier columnModifier : ColumnModifier.values()) {
            MOD_FALSE_BYTES[columnModifier.ordinal()] = columnModifier.apply(FALSE_BYTES, 0, new byte[FALSE_BYTES.length], 0, FALSE_BYTES.length);
        }
    }
    public static final byte[] MOD_TRUE_BYTES[] = new byte[ColumnModifier.values().length][];
    static {
        for (ColumnModifier columnModifier : ColumnModifier.values()) {
            MOD_TRUE_BYTES[columnModifier.ordinal()] = columnModifier.apply(TRUE_BYTES, 0, new byte[TRUE_BYTES.length], 0, TRUE_BYTES.length);
        }
    }
    public static final byte[] NULL_BYTES = ByteUtil.EMPTY_BYTE_ARRAY;
    private static final Integer BOOLEAN_LENGTH = 1;

    public final static Integer ZERO = 0;
    public final static Integer INT_PRECISION = 10;
    public final static Integer LONG_PRECISION = 19;
    public final static Integer SHORT_PRECISION = 5;
    public final static Integer BYTE_PRECISION = 3;

    /**
     * Serialize a BigDecimal into a variable length byte array in such a way that it is
     * binary comparable.
     * @param v the BigDecimal
     * @param result the byte array to contain the serialized bytes.  Max size
     * necessary would be 21 bytes.
     * @param length the number of bytes required to store the big decimal. May be
     * adjusted down if it exceeds {@link #MAX_BIG_DECIMAL_BYTES}
     * @return the number of bytes that make up the serialized BigDecimal
     */
    private static int toBytes(BigDecimal v, byte[] result, final int offset, int length) {
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
            result[offset] = (byte)((-(scale+expOffset)/2 + EXP_BYTE_OFFSET) | 0x80);
        } else {
            digitOffset = NEG_DIGIT_OFFSET;
            compareAgainst = MIN_LONG;
            // Scale adjustment shouldn't include terminal byte in length
            scale -= (length - 2 - 1) * 2;
            result[offset] = (byte)(~(-(scale+expOffset)/2 + EXP_BYTE_OFFSET + 128) & 0x7F);
            if (length <= MAX_BIG_DECIMAL_BYTES) {
                result[--index] = NEG_TERMINAL_BYTE;
            } else {
                // Adjust length and offset down because we don't have enough room
                length = MAX_BIG_DECIMAL_BYTES;
                index = offset + length - 1;
            }
        }
        BigInteger bi = v.unscaledValue();
        // Use BigDecimal arithmetic until we can fit into a long
        while (bi.compareTo(compareAgainst) * signum > 0) {
            BigInteger[] dandr = bi.divideAndRemainder(divideBy);
            bi = dandr[0];
            int digit = dandr[1].intValue();
            result[--index] = (byte)(signum * digit * multiplyBy + digitOffset);
            multiplyBy = 1;
            divideBy = ONE_HUNDRED;
        }
        long l = bi.longValue();
        do {
            long divBy = 100/multiplyBy;
            long digit = l % divBy;
            l /= divBy;
            result[--index] = (byte)(digit * multiplyBy + digitOffset);
            multiplyBy = 1;
        } while (l != 0);

        return length;
    }

    /**
     * Deserialize a variable length byte array into a BigDecimal. Note that because of
     * the normalization that gets done to the scale, if you roundtrip a BigDecimal,
     * it may not be equal before and after. However, the before and after number will
     * always compare to be equal (i.e. <nBefore>.compareTo(<nAfter>) == 0)
     * @param bytes the bytes containing the number
     * @param offset the offset into the byte array
     * @param length the length of the serialized BigDecimal
     * @return the BigDecimal value.
     */
    private static BigDecimal toBigDecimal(byte[] bytes, int offset, int length) {
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
            scale = (byte)(((bytes[offset] & 0x7F) - 65) * -2);
            index = offset + length;
            digitOffset = POS_DIGIT_OFFSET;
        } else {
            scale = (byte)((~bytes[offset] - 65 - 128) * -2);
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
            l += digit100*multiplier;
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
    static int[] getDecimalPrecisionAndScale(byte[] bytes, int offset, int length) {
        // 0, which should have no precision nor scale.
        if (length == 1 && bytes[offset] == ZERO_BYTE) {
            return new int[] {0, 0};
        }
        int signum = ((bytes[offset] & 0x80) == 0) ? -1 : 1;
        int scale;
        int index;
        int digitOffset;
        if (signum == 1) {
            scale = (byte)(((bytes[offset] & 0x7F) - 65) * -2);
            index = offset + length;
            digitOffset = POS_DIGIT_OFFSET;
        } else {
            scale = (byte)((~bytes[offset] - 65 - 128) * -2);
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
        d = signum * bytes[offset+1] - digitOffset;
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
        return new int[] {precision, scale};
    }

    public boolean isCoercibleTo(PDataType targetType) {
        return this == targetType || targetType == VARBINARY;
    }

    // Specialized on enums to take into account type hierarchy (i.e. UNSIGNED_LONG is comparable to INTEGER)
    public boolean isComparableTo(PDataType targetType) {
        return targetType.isCoercibleTo(this) || this.isCoercibleTo(targetType);
    }

    public boolean isCoercibleTo(PDataType targetType, Object value) {
        return isCoercibleTo(targetType);
    }

    public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
            Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
        return true;
    }

    public int compareTo(byte[] b1, byte[] b2) {
        return compareTo(b1, 0, b1.length, null, b2, 0, b2.length, null);
    }

    public int compareTo(ImmutableBytesWritable ptr1, ImmutableBytesWritable ptr2) {
        return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), null, ptr2.get(), ptr2.getOffset(), ptr2.getLength(), null);
    }

    public int compareTo(byte[] ba1, int offset1, int length1, ColumnModifier mod1, byte[] ba2, int offset2, int length2, ColumnModifier mod2) {
        if (mod1 != mod2) {
            int length = Math.min(length1, length2);
            for (int i = 0; i < length; i++) {
                byte b1 = ba1[offset1+i];
                byte b2 = ba2[offset2+i];
                if (mod1 != null) {
                    b1 = mod1.apply(b1);
                } else {
                    b2 = mod2.apply(b2);
                }
                int c = b1 - b2;
                if (c != 0) {
                    return c;
                }
            }
            return (length1 - length2);
        }
        return Bytes.compareTo(ba1, offset1, length1, ba2, offset2, length2) * (mod1 == ColumnModifier.SORT_DESC ? -1 : 1);
    }

    public int compareTo(ImmutableBytesWritable ptr1, ColumnModifier ptr1ColumnModifier, ImmutableBytesWritable ptr2, ColumnModifier ptr2ColumnModifier, PDataType type2) {
        return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), ptr1ColumnModifier, ptr2.get(), ptr2.getOffset(), ptr2.getLength(), ptr2ColumnModifier, type2);
    }

    public abstract int compareTo(Object lhs, Object rhs, PDataType rhsType);

    public int compareTo(Object lhs, Object rhs) {
        return compareTo(lhs,rhs,this);
    }

    public abstract boolean isFixedWidth();
    public abstract Integer getByteSize();

    public abstract byte[] toBytes(Object object);
    
    public byte[] toBytes(Object object, ColumnModifier columnModifier) {
    	byte[] bytes = toBytes(object);
    	if (columnModifier != null) {
            columnModifier.apply(bytes, 0, bytes, 0, bytes.length);
    	}
    	return bytes;
    }

    /**
     * Convert from the object representation of a data type value into
     * the serialized byte form.
     * @param object the object to convert
     * @param bytes the byte array into which to put the serialized form of object
     * @param offset the offset from which to start writing the serialized form
     * @return the byte length of the serialized object
     */
    public abstract int toBytes(Object object, byte[] bytes, int offset);
   
//   TODO: we need one that coerces and inverts and scales
//    public void coerceBytes(ImmutableBytesWritable ptr, PDataType actualType, 
//            Integer actualMaxLength, Integer actualScale, ColumnModifier actualModifier,
//            Integer desiredMaxLength, Integer desiredScale, ColumnModifier desiredModifier) {
//        
//    }
    
    public void coerceBytes(ImmutableBytesWritable ptr, PDataType actualType, ColumnModifier actualModifier, ColumnModifier expectedModifier) {
        if (ptr.getLength() == 0) {
            return;
        }
        if (this.isBytesComparableWith(actualType)) { // No coerce necessary
            if (actualModifier == expectedModifier) {
                return;
            }
            // TODO: generalize ColumnModifier?
            byte[] b = ptr.copyBytes();
            ColumnModifier.SORT_DESC.apply(b, 0, b, 0, b.length);
            ptr.set(b);
            return;
        }
        
        Object coercedValue = toObject(ptr, actualType, actualModifier);
        byte[] b = toBytes(coercedValue, expectedModifier);
        ptr.set(b);
    }

    public byte[] coerceBytes(byte[] b, Object object, PDataType actualType) {
        if (this.isBytesComparableWith(actualType)) { // No coerce necessary
            return b;
        } else { // TODO: optimize in specific cases
            Object coercedValue = toObject(object, actualType);
            return toBytes(coercedValue);
        }
    }

    public byte[] coerceBytes(byte[] b, Object object, PDataType actualType, Integer maxLength, Integer scale,
            Integer desiredMaxLength, Integer desiredScale) {
        return coerceBytes(b, object, actualType);
    }

    /**
     * Convert from a string to the object representation of a given type
     * @param value a stringified value
     * @return the object representation of a string value
     */
    public abstract Object toObject(String value);
    
    public Object toObject(Object object, PDataType actualType) {
        if (this == actualType) {
            return object;
        }
        throw new IllegalDataException("Cannot convert from " + actualType + " to " + this);
    }

    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
        return toObject(bytes, offset, length, actualType, null);
    }

    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType, ColumnModifier columnModifier) {
        if (actualType == null) {
            return null;
        }
    	if (columnModifier != null) {
    	    bytes = columnModifier.apply(bytes, offset, new byte[length], 0, length);
    	    offset = 0;
    	}
        Object o = actualType.toObject(bytes, offset, length);
        return this.toObject(o, actualType);
    }
    
    public Object toObject(ImmutableBytesWritable ptr, PDataType actualType) {
        return toObject(ptr, actualType, null);
    }    
    
    public Object toObject(ImmutableBytesWritable ptr, PDataType actualType, ColumnModifier sortOrder) { 
        return this.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), actualType, sortOrder);
    }

    public Object toObject(ImmutableBytesWritable ptr) {
        return toObject(ptr.get(), ptr.getOffset(), ptr.getLength());
    }
    
    public Object toObject(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
        return toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), this, columnModifier);        
    }    

    public Object toObject(byte[] bytes, int offset, int length) {
        return toObject(bytes, offset, length, this);
    }

    public Object toObject(byte[] bytes) {
        return toObject(bytes, (ColumnModifier)null);
    }

    public Object toObject(byte[] bytes, ColumnModifier columnModifier) {
        return toObject(bytes, 0, bytes.length, this, columnModifier);
    }

    private static final Map<String, PDataType> SQL_TYPE_NAME_TO_PCOLUMN_DATA_TYPE;
    static {
        ImmutableMap.Builder<String, PDataType> builder =
            ImmutableMap.<String, PDataType>builder();
        for (PDataType dataType : PDataType.values()) {
            builder.put(dataType.getSqlTypeName(), dataType);
        }
        SQL_TYPE_NAME_TO_PCOLUMN_DATA_TYPE = builder.build();
    }

    public static PDataType fromSqlTypeName(String sqlTypeName) {
        PDataType dataType = SQL_TYPE_NAME_TO_PCOLUMN_DATA_TYPE.get(sqlTypeName);
        if (dataType != null) {
            return dataType;
        }
        throw new IllegalDataException("Unsupported sql type: " + sqlTypeName);
    }

    private static final int SQL_TYPE_OFFSET;
    private static final PDataType[] SQL_TYPE_TO_PCOLUMN_DATA_TYPE;
    static {
        int minSqlType = Integer.MAX_VALUE;
        int maxSqlType = Integer.MIN_VALUE;
        for (PDataType dataType : PDataType.values()) {
            int sqlType = dataType.getSqlType();
            if (sqlType < minSqlType) {
                minSqlType = sqlType;
            }
            if (sqlType > maxSqlType) {
                maxSqlType = sqlType;
            }
        }
        SQL_TYPE_OFFSET = minSqlType;
        SQL_TYPE_TO_PCOLUMN_DATA_TYPE = new PDataType[maxSqlType-minSqlType+1];
        for (PDataType dataType : PDataType.values()) {
            int sqlType = dataType.getSqlType();
            SQL_TYPE_TO_PCOLUMN_DATA_TYPE[sqlType-SQL_TYPE_OFFSET] = dataType;
        }
    }

    public static PDataType fromSqlType(Integer sqlType) {
        int offset = sqlType - SQL_TYPE_OFFSET;
        if (offset >= 0 && offset < SQL_TYPE_TO_PCOLUMN_DATA_TYPE.length) {
            PDataType type = SQL_TYPE_TO_PCOLUMN_DATA_TYPE[offset];
            if (type != null) {
                return type;
            }
        }
        throw new IllegalDataException("Unsupported sql type: " + sqlType);
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
     * @return integer representing the SQL type for display
     * of a result set of this type
     */
    public int getResultSetSqlType() {
        return this.sqlType;
    }

    public KeyRange getKeyRange(byte[] point) {
        return getKeyRange(point, true, point, true);
    }
    
    public String toStringLiteral(ImmutableBytesWritable ptr, Format formatter) {
        return toStringLiteral(ptr.get(),ptr.getOffset(),ptr.getLength(),formatter);
    }
    public String toStringLiteral(byte[] b, Format formatter) {
        return toStringLiteral(b,0,b.length,formatter);
    }
    public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
        Object o = toObject(b,offset,length);
        if (formatter != null) {
            return formatter.format(o);
        }
        return o.toString();
    }
    
    public KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
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
            if (type.getJavaClass().isInstance(value)) {
                return type;
            }
        }
        throw new UnsupportedOperationException("Unsupported literal value [" + value + "] of type " + value.getClass().getName());
    }
}
