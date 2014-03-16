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
import org.apache.phoenix.exception.ValueTypeIncompatibleException;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.StringUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

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
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            if (!actualType.isCoercibleTo(this)) {
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            if (length == 0) {
                return null;
            }
            if (sortOrder == SortOrder.DESC) {
                bytes = SortOrder.invert(bytes, offset, length);
                offset = 0;
            }
            return Bytes.toString(bytes, offset, length);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            switch (actualType) {
            case VARCHAR:
            case CHAR:
                String s = (String)object;
                return s == null || s.length() > 0 ? s : null;
            default:
                return throwConstraintViolationException(actualType,this);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
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
        public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
                Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
            if (ptr.getLength() != 0 && maxLength != null && desiredMaxLength != null) {
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
        public Object pad(Object object, int maxLength) {
            String s = (String) object;
            if (s == null) {
                return s;
            }
            if (s.length() == maxLength) {
                return object;
            }
            if (s.length() > maxLength) {
                throw new ValueTypeIncompatibleException(this,maxLength,null);
            }
            return Strings.padEnd(s, maxLength, ' ');
        }
        
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
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            if (!actualType.isCoercibleTo(this)) { // TODO: have isCoercibleTo that takes bytes, offset?
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            if (length == 0) {
                return null;
            }
            length = StringUtil.getUnpaddedCharLength(bytes, offset, length, sortOrder);
            if (sortOrder == SortOrder.DESC) {
                bytes = SortOrder.invert(bytes, offset, length);
                offset = 0;
            }
            // TODO: UTF-8 decoder that will invert as it decodes
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
                String s = (String)object;
                return s == null || s.length() > 0 ? s : null;
            default:
                return throwConstraintViolationException(actualType,this);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == VARCHAR || targetType == BINARY || targetType == VARBINARY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object o, PDataType actualType, 
                Integer actualMaxLength, Integer actualScale, SortOrder actualModifier,
                Integer desiredMaxLength, Integer desiredScale, SortOrder expectedModifier) {
            if (o != null && actualType == PDataType.VARCHAR && ((String)o).length() != ptr.getLength()) {
                throw new IllegalDataException("CHAR types may only contain single byte characters (" + o + ")");
            }
            super.coerceBytes(ptr, o, actualType, actualMaxLength, actualScale, actualModifier, desiredMaxLength, desiredScale, expectedModifier);
        }
        
        @Override
        public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
                Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
            return VARCHAR.isSizeCompatible(ptr, value, srcType, maxLength, scale, desiredMaxLength, desiredScale);
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
            default:
                return throwConstraintViolationException(actualType,this);
            }
        }

        @Override
        public Long toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
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
                return actualType.getCodec().decodeLong(b, o, sortOrder);
            case DECIMAL:
                BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
                return bd.longValueExact();
            }
            throwConstraintViolationException(actualType,this);
            return null;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            // In general, don't allow conversion of LONG to INTEGER. There are times when
            // we check isComparableTo for a more relaxed check and then throw a runtime
            // exception if we overflow
            return this == targetType || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY
                    || targetType == DOUBLE;
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
        public Integer toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
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
                return actualType.getCodec().decodeInt(b, o, sortOrder);
            case DECIMAL:
                BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
                return bd.intValueExact();
            }
            throwConstraintViolationException(actualType,this);
            return null;
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
            return this == targetType || targetType == FLOAT || LONG.isCoercibleTo(targetType);
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
        public Integer getScale(Object o) {
            return ZERO;
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
      public Short toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
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
              return actualType.getCodec().decodeShort(b, o, sortOrder);
          case DECIMAL:
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
        public Integer getScale(Object o) {
            return ZERO;
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
      public Byte toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
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
              return actualType.getCodec().decodeByte(b, o, sortOrder);
          case DECIMAL:
              BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
              return bd.byteValueExact();
          }
          throwConstraintViolationException(actualType,this);
          return null;
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
                return throwConstraintViolationException(actualType,this);
            }
        }
        
        @Override
        public Float toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
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
                return actualType.getCodec().decodeFloat(b, o, sortOrder);
            case DECIMAL:
                BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
                return bd.floatValue();
            }
            
            throwConstraintViolationException(actualType,this);
            return null;
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
                return throwConstraintViolationException(actualType,this);
            }
        }
        
        @Override
        public Double toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
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
                return actualType.getCodec().decodeDouble(b, o, sortOrder);
            case DECIMAL:
                BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
                return bd.doubleValue();
            }
            throwConstraintViolationException(actualType,this);
            return null;
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
                return MAX_PRECISION;
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
            int scale = v.scale();
            if (scale == 0) {
                return null;
            }
            // If we have 5.0, we still want scale to be null
            return v.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) == 0 ? null : scale;
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Preconditions.checkNotNull(sortOrder);        	
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case DECIMAL:
                if (sortOrder == SortOrder.DESC) {
                    b = SortOrder.invert(b, o, new byte[l], 0, l);
                    o = 0;
                }
                return toBigDecimal(b, o, l);
            case DATE:
            case TIME:
            case UNSIGNED_DATE:
            case UNSIGNED_TIME:
            case LONG:
            case UNSIGNED_LONG:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case UNSIGNED_INT:
            case UNSIGNED_SMALLINT:
            case UNSIGNED_TINYINT:
                return BigDecimal.valueOf(actualType.getCodec().decodeLong(b, o, sortOrder));
            case FLOAT:
            case UNSIGNED_FLOAT:
                return BigDecimal.valueOf(actualType.getCodec().decodeFloat(b, o, sortOrder));
            case DOUBLE:
            case UNSIGNED_DOUBLE:
                return BigDecimal.valueOf(actualType.getCodec().decodeDouble(b, o, sortOrder));
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
                long millisPart = actualType.getCodec().decodeLong(b, o, sortOrder);
                int nanoPart = UNSIGNED_INT.getCodec().decodeInt(b, o+Bytes.SIZEOF_LONG, sortOrder);
                BigDecimal nanosPart = BigDecimal.valueOf((nanoPart % QueryConstants.MILLIS_TO_NANOS_CONVERTOR)/QueryConstants.MILLIS_TO_NANOS_CONVERTOR);
                BigDecimal value = BigDecimal.valueOf(millisPart).add(nanosPart);
                return value;
            case BOOLEAN:
                return (Boolean)BOOLEAN.toObject(b, o, l, actualType, sortOrder) ? BigDecimal.ONE : BigDecimal.ZERO;
            default:
                return throwConstraintViolationException(actualType,this);
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
            case DATE:
            case UNSIGNED_DATE:
            case TIME:
            case UNSIGNED_TIME:
                java.util.Date d = (java.util.Date)object;
                return BigDecimal.valueOf(d.getTime());
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
                Timestamp ts = (Timestamp)object;
                long millisPart = ts.getTime();
                BigDecimal nanosPart = BigDecimal.valueOf((ts.getNanos() % QueryConstants.MILLIS_TO_NANOS_CONVERTOR)/QueryConstants.MILLIS_TO_NANOS_CONVERTOR);
                BigDecimal value = BigDecimal.valueOf(millisPart).add(nanosPart);
                return value;
            case BOOLEAN:
                return ((Boolean)object) ? BigDecimal.ONE : BigDecimal.ZERO;
            default:
                return throwConstraintViolationException(actualType,this);
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
        public boolean isCastableTo(PDataType targetType) {
            return super.isCastableTo(targetType) || targetType.isCoercibleTo(TIMESTAMP) || targetType == BOOLEAN;
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
        public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType, Integer maxLength,
        		Integer scale, Integer desiredMaxLength, Integer desiredScale) {
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
            } else  {
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
                Integer maxLength, Integer scale, SortOrder actualModifier,
                Integer desiredMaxLength, Integer desiredScale, SortOrder expectedModifier) {
            if (desiredScale == null) {
                // deiredScale not available, or we do not have scale requirement, delegate to parents.
                super.coerceBytes(ptr, object, actualType, maxLength, scale, actualModifier, desiredMaxLength, desiredScale, expectedModifier);
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
                throw new IllegalDataException(e);
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
                BigDecimal o = (BigDecimal)toObject(b, offset, length);
                return o.toPlainString();
            }
            return super.toStringLiteral(b,offset, length, formatter);
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
            Timestamp value = (Timestamp)object;
            DATE.getCodec().encodeLong(value.getTime(), bytes, offset);
            
            /*
             * By not getting the stuff that got spilled over from the millis part,
             * it leaves the timestamp's byte representation saner - 8 bytes of millis | 4 bytes of nanos.
             * Also, it enables timestamp bytes to be directly compared with date/time bytes.   
             */
            Bytes.putInt(bytes, offset + Bytes.SIZEOF_LONG, value.getNanos() % 1000000);  
            return getByteSize();
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case DATE:
            case TIME:
            case UNSIGNED_DATE:
            case UNSIGNED_TIME:
                return new Timestamp(((java.util.Date)object).getTime());
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
                return object;
            case LONG:
            case UNSIGNED_LONG:
                return new Timestamp((Long)object);
            case DECIMAL:
                BigDecimal bd = (BigDecimal)object;
                long ms = bd.longValue();
                int nanos = (bd.remainder(BigDecimal.ONE).multiply(QueryConstants.BD_MILLIS_NANOS_CONVERSION)).intValue();
                return DateUtil.getTimestamp(ms, nanos);
            default:
                return throwConstraintViolationException(actualType,this);
            }
        }

        @Override
        public Timestamp toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            if (actualType == null || l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
                long millisDeserialized = (actualType == TIMESTAMP ? DATE : UNSIGNED_DATE).getCodec().decodeLong(b, o, sortOrder);
                Timestamp v = new Timestamp(millisDeserialized);
                int nanosDeserialized = PDataType.UNSIGNED_INT.getCodec().decodeInt(b, o + Bytes.SIZEOF_LONG, sortOrder);
                /*
                 * There was a bug in serialization of timestamps which was causing the sub-second millis part
                 * of time stamp to be present both in the LONG and INT bytes. Having the <100000 check
                 * makes this serialization fix backward compatible.
                 */
                v.setNanos(nanosDeserialized < 1000000 ? v.getNanos() + nanosDeserialized : nanosDeserialized);
                return v;
            case DATE:
            case TIME:
            case LONG:
            case UNSIGNED_LONG:
            case UNSIGNED_DATE:
            case UNSIGNED_TIME:
                return new Timestamp(actualType.getCodec().decodeLong(b, o, sortOrder));
            case DECIMAL:
                BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
                long ms = bd.longValue();
                int nanos = (bd.remainder(BigDecimal.ONE).multiply(QueryConstants.BD_MILLIS_NANOS_CONVERSION)).intValue();
                v = DateUtil.getTimestamp(ms, nanos);
                return v;
            }
            throwConstraintViolationException(actualType,this);
            return null;
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
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                switch (targetType) {
                    case UNSIGNED_TIMESTAMP:
                        return ((java.util.Date)value).getTime() >= 0;
                    case UNSIGNED_DATE:
                    case UNSIGNED_TIME:
                        return ((java.util.Date)value).getTime() >= 0 && ((Timestamp)value).getNanos() == 0;
                    case DATE:
                    case TIME:
                        return ((Timestamp)value).getNanos() == 0;
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
            return MAX_TIMESTAMP_BYTES;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == TIMESTAMP || rhsType == UNSIGNED_TIMESTAMP) {
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
        
        @Override
        public int getNanos(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            int nanos = PDataType.UNSIGNED_INT.getCodec().decodeInt(ptr.get(), ptr.getOffset() + PDataType.LONG.getByteSize(), sortOrder);
            return nanos;
        }
        
        @Override
        public long getMillis(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            long millis = PDataType.LONG.getCodec().decodeLong(ptr.get(),ptr.getOffset(), sortOrder);
            return millis;
        }

    },
    TIME("TIME", Types.TIME, Time.class, new DateCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            return DATE.toBytes(object);
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            return DATE.toBytes(object, bytes, offset);
        }

        @Override
        public Time toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
            case DATE:
            case TIME:
            case LONG:
            case UNSIGNED_LONG:
            case UNSIGNED_DATE:
            case UNSIGNED_TIME:
                return new Time(actualType.getCodec().decodeLong(b, o, sortOrder));
            case DECIMAL:
                BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
                return new Time(bd.longValueExact());
            }
            throwConstraintViolationException(actualType,this);
            return null;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case DATE:
            case UNSIGNED_DATE:
                return new Time(((Date)object).getTime());
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
                return new Time(((Timestamp)object).getTime());
            case TIME:
            case UNSIGNED_TIME:
                return object;
            case LONG:
            case UNSIGNED_LONG:
                return new Time((Long)object);
            case DECIMAL:
                return new Time(((BigDecimal)object).longValueExact());
            default:
                return throwConstraintViolationException(actualType,this);
            }
        }

        @Override
        public boolean isCastableTo(PDataType targetType) {
            return DATE.isCastableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return DATE.isCoercibleTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return DATE.isCoercibleTo(targetType, value);
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
            return DATE.compareTo(lhs, rhs, rhsType);
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
            // TODO: different default formatter for TIME?
            return DATE.toStringLiteral(b, offset, length, formatter);
        }
    },
    DATE("DATE", Types.DATE, Date.class, new DateCodec()) { // After TIMESTAMP and DATE to ensure toLiteral finds those first

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
            getCodec().encodeLong(((java.util.Date)object).getTime(), bytes, offset);
            return this.getByteSize();
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case TIME:
            case UNSIGNED_TIME:
                return new Date(((Time)object).getTime());
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
                return new Date(((Timestamp)object).getTime());
            case DATE:
            case UNSIGNED_DATE:
                return object;
            case LONG:
            case UNSIGNED_LONG:
                return new Date((Long)object);
            case DECIMAL:
                return new Date(((BigDecimal)object).longValueExact());
            default:
                return throwConstraintViolationException(actualType,this);
            }
        }

        @Override
        public Date toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP:
            case UNSIGNED_TIMESTAMP:
            case DATE:
            case TIME:
            case LONG:
            case UNSIGNED_LONG:
            case UNSIGNED_DATE:
            case UNSIGNED_TIME:
                return new Date(actualType.getCodec().decodeLong(b, o, sortOrder));
            case DECIMAL:
                BigDecimal bd = (BigDecimal)actualType.toObject(b, o, l, actualType, sortOrder);
                return new Date(bd.longValueExact());
            }
            throwConstraintViolationException(actualType,this);
            return null;
        }

        @Override
        public boolean isCastableTo(PDataType targetType) {
            return super.isCastableTo(targetType) || targetType == DECIMAL || targetType == LONG || targetType == UNSIGNED_LONG;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return targetType == DATE || targetType == TIME || targetType == TIMESTAMP
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                switch (targetType) {
                    case UNSIGNED_TIMESTAMP:
                    case UNSIGNED_DATE:
                    case UNSIGNED_TIME:
                        return ((java.util.Date)value).getTime() >= 0;
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
            if (rhsType == TIMESTAMP || rhsType == UNSIGNED_TIMESTAMP) {
                return -rhsType.compareTo(rhs, lhs, TIME);
            }
            return ((java.util.Date)rhs).compareTo((java.util.Date)lhs);
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
        
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, 
                Integer maxLength, Integer scale, SortOrder actualModifier,
                Integer desiredMaxLength, Integer desiredScale, SortOrder expectedModifier) {
            if (ptr.getLength() > 0 && actualType  == PDataType.TIMESTAMP && actualModifier == expectedModifier) {
                ptr.set(ptr.get(), ptr.getOffset(), getByteSize());
                return;
            }
            super.coerceBytes(ptr, object, actualType, maxLength, scale, actualModifier, desiredMaxLength, desiredScale, expectedModifier);
        }
    },
    UNSIGNED_TIMESTAMP("UNSIGNED_TIMESTAMP", 19, Timestamp.class, null) {

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
            Timestamp value = (Timestamp)object;
            UNSIGNED_DATE.getCodec().encodeLong(value.getTime(), bytes, offset);
            
            /*
             * By not getting the stuff that got spilled over from the millis part,
             * it leaves the timestamp's byte representation saner - 8 bytes of millis | 4 bytes of nanos.
             * Also, it enables timestamp bytes to be directly compared with date/time bytes.   
             */
            Bytes.putInt(bytes, offset + Bytes.SIZEOF_LONG, value.getNanos() % 1000000);  
            return getByteSize();
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            Timestamp ts = (Timestamp)TIMESTAMP.toObject(object, actualType);
            throwIfNonNegativeDate(ts);
            return ts;
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Timestamp ts = (Timestamp) TIMESTAMP.toObject(b, o, l, actualType, sortOrder);
            throwIfNonNegativeDate(ts);
            return ts;
        }
        
        @Override
        public boolean isCastableTo(PDataType targetType) {
            return UNSIGNED_DATE.isCastableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return targetType == this || UNSIGNED_DATE.isCoercibleTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return super.isCoercibleTo(targetType, value) || TIMESTAMP.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return TIMESTAMP.getByteSize();
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return TIMESTAMP.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public Object toObject(String value) {
            return TIMESTAMP.toObject(value);
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
        
        @Override
        public int getNanos(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            int nanos = PDataType.UNSIGNED_INT.getCodec().decodeInt(ptr.get(), ptr.getOffset() + PDataType.LONG.getByteSize(), sortOrder);
            return nanos;
        }
        
        @Override
        public long getMillis(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            long millis = PDataType.UNSIGNED_LONG.getCodec().decodeLong(ptr.get(),ptr.getOffset(), sortOrder);
            return millis;
        }
    },
    UNSIGNED_TIME("UNSIGNED_TIME", 18, Time.class, new UnsignedDateCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            return UNSIGNED_DATE.toBytes(object);
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            return UNSIGNED_DATE.toBytes(object, bytes, offset);
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Time t = (Time)TIME.toObject(b, o, l, actualType, sortOrder);
            throwIfNonNegativeDate(t);
            return t;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            Time t = (Time)TIME.toObject(object, actualType);
            throwIfNonNegativeDate(t);
            return t;
        }

        @Override
        public boolean isCastableTo(PDataType targetType) {
            return UNSIGNED_DATE.isCastableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return UNSIGNED_DATE.isCoercibleTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return super.isCoercibleTo(targetType, value) || TIME.isCoercibleTo(targetType, value);
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
            return TIME.toObject(value);
        }

        @Override
        public boolean isBytesComparableWith(PDataType otherType) {
            return super.isBytesComparableWith(otherType) ||  this == UNSIGNED_DATE;
        }
        
        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            return UNSIGNED_DATE.toStringLiteral(b, offset, length, formatter);
        }
    },
    UNSIGNED_DATE("UNSIGNED_DATE", 19, Date.class, new UnsignedDateCodec()) { // After TIMESTAMP and DATE to ensure toLiteral finds those first

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
            getCodec().encodeLong(((java.util.Date)object).getTime(), bytes, offset);
            return this.getByteSize();
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            Date d = (Date)DATE.toObject(object, actualType);
            throwIfNonNegativeDate(d);
            return d;
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Date d = (Date)DATE.toObject(b,o,l,actualType, sortOrder);
            throwIfNonNegativeDate(d);
            return d;
        }

        @Override
        public boolean isCastableTo(PDataType targetType) {
            return DATE.isCastableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return targetType == this || targetType == UNSIGNED_TIME || targetType == UNSIGNED_TIMESTAMP
                    || DATE.isCoercibleTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return super.isCoercibleTo(targetType, value) || DATE.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return DATE.getByteSize();
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return DATE.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public Object toObject(String value) {
            return DATE.toObject(value);
        }

        @Override
        public boolean isBytesComparableWith(PDataType otherType) {
            return super.isBytesComparableWith(otherType) || this == UNSIGNED_TIME;
        }
        
        @Override
        public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
            // Can't delegate, as the super.toStringLiteral calls this.toBytes
            if (formatter == null || formatter == DateUtil.DEFAULT_DATE_FORMATTER) {
                // If default formatter has not been overridden,
                // use one that displays milliseconds.
                formatter = DateUtil.DEFAULT_MS_DATE_FORMATTER;
            }
            return "'" + super.toStringLiteral(b, offset, length, formatter) + "'";
        }
        
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, 
                Integer maxLength, Integer scale, SortOrder actualModifier,
                Integer desiredMaxLength, Integer desiredScale, SortOrder expectedModifier) {
            if (ptr.getLength() > 0 && actualType  == PDataType.UNSIGNED_TIMESTAMP && actualModifier == expectedModifier) {
                ptr.set(ptr.get(), ptr.getOffset(), getByteSize());
                return;
            }
            super.coerceBytes(ptr, object, actualType, maxLength, scale, actualModifier, desiredMaxLength, desiredScale, expectedModifier);
        }
    },
    /**
     * Unsigned long type that restricts values to be from 0 to {@link java.lang.Long#MAX_VALUE} inclusive. May be used to map to existing HTable values created through {@link org.apache.hadoop.hbase.util.Bytes#toBytes(long)}
     * as long as all values are non negative (the leading sign bit of negative numbers would cause them to sort ahead of positive numbers when
     * they're used as part of the row key when using the HBase utility methods).
     */
    UNSIGNED_LONG("UNSIGNED_LONG", 10 /* no constant available in Types */, Long.class, new UnsignedLongCodec()) {
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
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeLong(((Number)object).longValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            Long v = (Long)LONG.toObject(object, actualType);
            throwIfNonNegativeNumber(v);
            return v;
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Long v = (Long)LONG.toObject(b, o, l, actualType, sortOrder);
            throwIfNonNegativeNumber(v);
            return v;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return targetType == this || targetType == UNSIGNED_DOUBLE || LONG.isCoercibleTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return super.isCoercibleTo(targetType, value) || LONG.isCoercibleTo(targetType, value);
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
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeInt(((Number)object).intValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            Integer v = (Integer)INTEGER.toObject(object, actualType);
            throwIfNonNegativeNumber(v);
            return v;
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Integer v = (Integer)INTEGER.toObject(b, o, l, actualType, sortOrder);
            throwIfNonNegativeNumber(v);
            return v;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return targetType == this || targetType == UNSIGNED_FLOAT || UNSIGNED_LONG.isCoercibleTo(targetType) || INTEGER.isCoercibleTo(targetType);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return super.isCoercibleTo(targetType, value) || INTEGER.isCoercibleTo(targetType, value);
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
        public Integer getScale(Object o) {
            return ZERO;
        }

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
      public Integer getMaxLength(Object o) {
          return SHORT_PRECISION;
      }

      @Override
      public byte[] toBytes(Object object) {
        if (object == null) {
          throw new ConstraintViolationException(this + " may not be null");
        }
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
          Short v = (Short)SMALLINT.toObject(object, actualType);
          throwIfNonNegativeNumber(v);
          return v;
      }
      
      @Override
      public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
          Short v = (Short)SMALLINT.toObject(b, o, l, actualType, sortOrder);
          throwIfNonNegativeNumber(v);
          return v;
      }
      
      @Override
      public boolean isComparableTo(PDataType targetType) {
          return DECIMAL.isComparableTo(targetType);
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType) {
          return targetType == this || UNSIGNED_INT.isCoercibleTo(targetType) || SMALLINT.isCoercibleTo(targetType);
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType, Object value) {
          return super.isCoercibleTo(targetType, value) || SMALLINT.isCoercibleTo(targetType, value);
      }
      
      @Override
      public int getResultSetSqlType() {
          return SMALLINT.getResultSetSqlType();
      }
    },
    UNSIGNED_TINYINT("UNSIGNED_TINYINT", 11, Byte.class, new UnsignedByteCodec()) {
        @Override
        public Integer getScale(Object o) {
            return ZERO;
        }

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
          Byte v = (Byte)TINYINT.toObject(object, actualType);
          throwIfNonNegativeNumber(v);
          return v;
      }
      
      @Override
      public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
          Byte v = (Byte)TINYINT.toObject(b, o, l, actualType, sortOrder);
          throwIfNonNegativeNumber(v);
          return v;
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType) {
          return targetType == this || UNSIGNED_SMALLINT.isCoercibleTo(targetType) || TINYINT.isCoercibleTo(targetType);
      }
      
      @Override
      public boolean isCoercibleTo(PDataType targetType, Object value) {
          return super.isCoercibleTo(targetType, value) || TINYINT.isCoercibleTo(targetType, value);
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
            return FLOAT.compareTo(lhs, rhs, rhsType);
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
            return FLOAT.getScale(o);
        }
        
        @Override
        public Integer getMaxLength(Object o) {
            return FLOAT.getMaxLength(o);
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
            Float v = (Float)FLOAT.toObject(object, actualType);
            throwIfNonNegativeNumber(v);
            return v;
        }
        
        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Float v = (Float)FLOAT.toObject(b, o, l, actualType, sortOrder);
            throwIfNonNegativeNumber(v);
            return v;
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return super.isCoercibleTo(targetType) || FLOAT.isCoercibleTo(targetType,value);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || UNSIGNED_DOUBLE.isCoercibleTo(targetType) || FLOAT.isCoercibleTo(targetType);
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
            return Bytes.SIZEOF_DOUBLE;
        }
        
        @Override
        public Integer getScale(Object o) {
            return DOUBLE.getScale(o);
        }
        
        @Override
        public Integer getMaxLength(Object o) {
            return DOUBLE.getMaxLength(o);
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
            Double v = (Double)DOUBLE.toObject(object, actualType);
            throwIfNonNegativeNumber(v);
            return v;
        }
        
        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Double v = (Double)DOUBLE.toObject(b, o, l, actualType, sortOrder);
            throwIfNonNegativeNumber(v);
            return v;
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            return super.isCoercibleTo(targetType, value)|| DOUBLE.isCoercibleTo(targetType, value);
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return  this == targetType || DOUBLE.isCoercibleTo(targetType);
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
                // TODO: review - return null?
                throw new ConstraintViolationException(this + " may not be null");
            }
            return ((Boolean)object).booleanValue() ? TRUE_BYTES : FALSE_BYTES;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                // TODO: review - return null?
                throw new ConstraintViolationException(this + " may not be null");
            }
            bytes[offset] = ((Boolean)object).booleanValue() ? TRUE_BYTE : FALSE_BYTE;
            return BOOLEAN_LENGTH;
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            if (object == null) {
                // TODO: review - return null?
                throw new ConstraintViolationException(this + " may not be null");
            }
            return ((Boolean)object).booleanValue() ^ sortOrder == SortOrder.ASC ? TRUE_BYTES : FALSE_BYTES;
        }

        @Override
        public Boolean toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            Preconditions.checkNotNull(sortOrder);          
            if (length == 0) {
                return null;
            }
            switch (actualType) {
                case BOOLEAN:
                    if (length > 1) {
                        throw new IllegalDataException("BOOLEAN may only be a single byte");
                    }
                    return ((bytes[offset] == FALSE_BYTE ^ sortOrder == SortOrder.DESC) ? Boolean.FALSE : Boolean.TRUE);
                case DECIMAL:
                    // false translated to the ZERO_BYTE
                    return ((bytes[offset] == ZERO_BYTE ^ sortOrder == SortOrder.DESC) ? Boolean.FALSE : Boolean.TRUE);
            }
            throwConstraintViolationException(actualType,this);
            return null;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return super.isCoercibleTo(targetType) || targetType == BINARY;
        }
        
        @Override
        public boolean isCastableTo(PDataType targetType) {
            // Allow cast to BOOLEAN so it can be used in an index or group by
            return super.isCastableTo(targetType) || targetType == DECIMAL;
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

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (actualType == this || object == null) {
                return object;
            }
            if (actualType == VARBINARY || actualType == BINARY) {
                byte[] bytes = (byte[])object;
                return toObject(bytes, 0, bytes.length);
            }
            return throwConstraintViolationException(actualType,this);
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
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            byte[] bytes = toBytes(object);
            // Override because we need to allocate a new buffer in this case
            if (sortOrder == SortOrder.DESC) {
                return SortOrder.invert(bytes, 0, new byte[bytes.length], 0, bytes.length);
            }
            return bytes;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            if (length == 0) {
                return null;
            }
            if (offset == 0 && bytes.length == length && sortOrder == SortOrder.ASC) {
                return bytes;
            }
            byte[] bytesCopy = new byte[length];
            System.arraycopy(bytes, offset, bytesCopy, 0, length);
            if (sortOrder == SortOrder.DESC) {
                bytesCopy = SortOrder.invert(bytes, offset, bytesCopy, 0, length);
                offset = 0;
            }
            return bytesCopy;
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
        public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
                Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
            if (ptr.getLength() != 0 && srcType == PDataType.BINARY && maxLength != null && desiredMaxLength != null) {
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
        public Object pad(Object object, int maxLength) {
            byte[] b = (byte[]) object;
            if (b == null) {
                return null;
            }
            if (b.length == maxLength) {
                return object;
            }
            if (b.length > maxLength) {
                throw new ValueTypeIncompatibleException(this,maxLength,null);
            }
            byte[] newBytes = new byte[maxLength];
            System.arraycopy(b, 0, newBytes, 0, b.length);
            
            return newBytes;
        }
        
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
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            byte[] bytes = toBytes(object);
            if (sortOrder == SortOrder.DESC) {
                return SortOrder.invert(bytes, 0, new byte[bytes.length], 0, bytes.length);
            }
            return bytes;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            if (!actualType.isCoercibleTo(this)) {
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            return VARBINARY.toObject(bytes, offset, length, actualType, sortOrder);
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
        public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
                Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
            if (ptr.getLength() != 0 && ((srcType == PDataType.VARBINARY && ((String)value).length() != ptr.getLength()) ||
                    (maxLength != null && desiredMaxLength != null && maxLength > desiredMaxLength))){
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
        public Integer getMaxLength(Object o) {
            if (o == null) { return null; }
            byte[] value = (byte[])o;
            return value.length;
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
    INTEGER_ARRAY("INTEGER_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.INTEGER.getSqlType(), PhoenixArray.class, null) {
    	@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}

		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.INTEGER, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.INTEGER, sortOrder, maxLength, scale,
                    PDataType.INTEGER);
		}
		
		@Override
		public boolean isCoercibleTo(PDataType targetType) {
			return pDataTypeForArray.isCoercibleTo(targetType, PDataType.INTEGER_ARRAY);
		}

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
		
		@Override
		public boolean isCoercibleTo(PDataType targetType, Object value) {
		   PhoenixArray pArr = (PhoenixArray)value;
		   int[] intArr = (int[])pArr.array;
           for (int i : intArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.INTEGER, i)) {
                   return false;
               }
           }
		   return true;
		}

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
	},
    BOOLEAN_ARRAY("BOOLEAN_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.BOOLEAN.getSqlType(), PhoenixArray.class, null) {
    	@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}

		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.BOOLEAN, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.BOOLEAN, sortOrder, maxLength, scale,
                    PDataType.BOOLEAN);
		}
		
		@Override
		public boolean isCoercibleTo(PDataType targetType) {
			return pDataTypeForArray.isCoercibleTo(targetType, PDataType.BOOLEAN_ARRAY);
		}
		
		@Override
		public boolean isCoercibleTo(PDataType targetType, Object value) {
		   
		   PhoenixArray pArr = (PhoenixArray)value;
		   boolean[] booleanArr = (boolean[])pArr.array;
           for (boolean i : booleanArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.BOOLEAN, i)) {
                   return false;
               }
           }
		   return true;
		}

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
		
	},
	VARCHAR_ARRAY("VARCHAR_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.VARCHAR.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.VARCHAR, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.VARCHAR, sortOrder, maxLength, scale,
                    PDataType.VARCHAR);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.VARCHAR_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] charArr = (Object[])pArr.array;
           for (Object i : charArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.VARCHAR, i)) {
                   return false;
               }
           }
           return true;
        }
		
        @Override
        public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType, Integer maxLength,
                Integer scale, Integer desiredMaxLength, Integer desiredScale) {
            return pDataTypeForArray.isSizeCompatible(ptr, value, srcType, maxLength, scale, desiredMaxLength,
                    desiredScale);
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
	},
	VARBINARY_ARRAY("VARBINARY_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.VARBINARY.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.VARBINARY, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.VARBINARY, sortOrder, maxLength, scale,
                    PDataType.VARBINARY);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.VARBINARY_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] charArr = (Object[])pArr.array;
           for (Object i : charArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.VARBINARY, i)) {
                   return false;
               }
           }
           return true;
        }
		
		@Override
		public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value,
				PDataType srcType, Integer maxLength, Integer scale,
				Integer desiredMaxLength, Integer desiredScale) {
			return pDataTypeForArray.isSizeCompatible(ptr, value, srcType,
					maxLength, scale, desiredMaxLength, desiredScale);
		}

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
	},
	BINARY_ARRAY("BINARY_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.BINARY.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.BINARY, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.BINARY, sortOrder, maxLength, scale,
                    PDataType.BINARY);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.BINARY_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] charArr = (Object[])pArr.array;
           for (Object i : charArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.BINARY, i)) {
                   return false;
               }
           }
           return true;
        }
		
		@Override
		public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value,
				PDataType srcType, Integer maxLength, Integer scale,
				Integer desiredMaxLength, Integer desiredScale) {
			return pDataTypeForArray.isSizeCompatible(ptr, value, srcType,
					maxLength, scale, desiredMaxLength, desiredScale);
		}
	
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
    },
	CHAR_ARRAY("CHAR_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.CHAR.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.CHAR, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
	
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.CHAR, sortOrder, maxLength, scale,
                    PDataType.CHAR);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.CHAR_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] charArr = (Object[])pArr.array;
           for (Object i : charArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.CHAR, i)) {
                   return false;
               }
           }
           return true;
        }
		
		@Override
		public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value,
				PDataType srcType, Integer maxLength, Integer scale,
				Integer desiredMaxLength, Integer desiredScale) {
			return pDataTypeForArray.isSizeCompatible(ptr, value, srcType,
					maxLength, scale, desiredMaxLength, desiredScale);
		}
		
		@Override
		public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength, Integer scale,
		        SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale, SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
		}

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
	},
	LONG_ARRAY("LONG_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.LONG.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.LONG, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.LONG, sortOrder, maxLength, scale,
                    PDataType.LONG);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.LONG_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           long[] longArr = (long[])pArr.array;
           for (long i : longArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.LONG, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
		
	},
	SMALLINT_ARRAY("SMALLINT_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.SMALLINT.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.SMALLINT, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.SMALLINT, sortOrder, maxLength, scale,
                    PDataType.SMALLINT);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.SMALLINT_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           short[] shortArr = (short[])pArr.array;
           for (short i : shortArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.SMALLINT, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
		
	},
	TINYINT_ARRAY("TINYINT_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.TINYINT.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.TINYINT, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}

		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.TINYINT, sortOrder, maxLength, scale,
                    PDataType.TINYINT);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.TINYINT_ARRAY);
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           byte[] byteArr = (byte[])pArr.array;
           for (byte i : byteArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.TINYINT, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
	},
	FLOAT_ARRAY("FLOAT_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.FLOAT.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.FLOAT, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}

		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.FLOAT, sortOrder, maxLength, scale,
                    PDataType.FLOAT);
		}
	
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.FLOAT_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           float[] floatArr = (float[])pArr.array;
           for (float i : floatArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.FLOAT, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
		
	},
	DOUBLE_ARRAY("DOUBLE_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.DOUBLE.getSqlType(), PhoenixArray.class, null) {
	    final PArrayDataType pDataTypeForArray = new PArrayDataType();
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.DOUBLE, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}

		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.DOUBLE, sortOrder, maxLength, scale,
                    PDataType.DOUBLE);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.DOUBLE_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           double[] doubleArr = (double[])pArr.array;
           for (double i : doubleArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.DOUBLE, i)) {
                   return false;
               }
           }
           return true;
        }
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

	},
	
	DECIMAL_ARRAY("DECIMAL_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.DECIMAL.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.DECIMAL, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.DECIMAL, sortOrder, maxLength, scale,
                    PDataType.DECIMAL);
		}
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
	
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.DECIMAL_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] decimalArr = (Object[])pArr.array;
           for (Object i : decimalArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.DECIMAL, i)) {
                   return false;
               }
           }
           return true;
        }
		
		@Override
		public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value,
				PDataType srcType, Integer maxLength, Integer scale,
				Integer desiredMaxLength, Integer desiredScale) {
			return pDataTypeForArray.isSizeCompatible(ptr, value, srcType,
					maxLength, scale, desiredMaxLength, desiredScale);
		}

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
	
	},
	TIMESTAMP_ARRAY("TIMESTAMP_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.TIMESTAMP.getSqlType(), PhoenixArray.class,
			null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.TIMESTAMP, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.TIMESTAMP, sortOrder, maxLength, scale,
                    PDataType.TIMESTAMP);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.TIMESTAMP_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] timeStampArr = (Object[])pArr.array;
           for (Object i : timeStampArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.TIMESTAMP, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

	},
	UNSIGNED_TIMESTAMP_ARRAY("UNSIGNED_TIMESTAMP_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_TIMESTAMP.getSqlType(), PhoenixArray.class,
			null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_TIMESTAMP, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
	
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_TIMESTAMP, sortOrder,
                    maxLength, scale, PDataType.UNSIGNED_TIMESTAMP);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_TIMESTAMP_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] timeStampArr = (Object[])pArr.array;
           for (Object i : timeStampArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_TIMESTAMP, i)) {
                   return false;
               }
           }
           return true;
        }
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

	},
	TIME_ARRAY("TIME_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.TIME.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.TIME, sortOrder);
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            return pDataTypeForArray.toBytes(object, bytes, offset);
        }

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.TIME, sortOrder, maxLength, scale,
                    PDataType.TIME);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.TIME_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] timeArr = (Object[])pArr.array;
           for (Object i : timeArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.TIME, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
        
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

	},
	UNSIGNED_TIME_ARRAY("UNSIGNED_TIME_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_TIME.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_TIME, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_TIME, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_TIME);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_TIME_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] timeArr = (Object[])pArr.array;
           for (Object i : timeArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_TIME, i)) {
                   return false;
               }
           }
           return true;
        }
		
        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

	},
	DATE_ARRAY("DATE_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.DATE.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.DATE, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}

		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.DATE, sortOrder, maxLength, scale,
                    PDataType.DATE);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.DATE_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] dateArr = (Object[])pArr.array;
           for (Object i : dateArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.DATE, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

	},
	UNSIGNED_DATE_ARRAY("UNSIGNED_DATE_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_DATE.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_DATE, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_DATE, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_DATE);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_DATE_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           Object[] dateArr = (Object[])pArr.array;
           for (Object i : dateArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_DATE, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

	},
	UNSIGNED_LONG_ARRAY("UNSIGNED_LONG_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_LONG.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_LONG, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_LONG, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_LONG);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_LONG_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           long[] longArr = (long[])pArr.array;
           for (long i : longArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_LONG, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
	
	},
	UNSIGNED_INT_ARRAY("UNSIGNED_INT_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_INT.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_INT, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_INT, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_INT);
		}

		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_INT_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           int[] intArr = (int[])pArr.array;
           for (int i : intArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_INT, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

	},
	UNSIGNED_SMALLINT_ARRAY("UNSIGNED_SMALLINT_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_SMALLINT.getSqlType(),
			PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_SMALLINT, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_SMALLINT, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_SMALLINT);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_SMALLINT_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           short[] shortArr = (short[])pArr.array;
           for (short i : shortArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

	},
	UNSIGNED_TINYINT_ARRAY("UNSIGNED_TINYINT__ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_TINYINT.getSqlType(), PhoenixArray.class,
			null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_TINYINT, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_TINYINT, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_TINYINT);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_TINYINT_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           byte[] byteArr = (byte[])pArr.array;
           for (byte i : byteArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_TINYINT, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
	},
	UNSIGNED_FLOAT_ARRAY("UNSIGNED_FLOAT_ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_FLOAT.getSqlType(), PhoenixArray.class, null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_FLOAT, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_FLOAT, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_FLOAT);
		}

		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_FLOAT_ARRAY);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           float[] floatArr = (float[])pArr.array;
           for (float i : floatArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_FLOAT, i)) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }

	},
	UNSIGNED_DOUBLE_ARRAY("UNSIGNED_DOUBLE__ARRAY", PDataType.ARRAY_TYPE_BASE + PDataType.UNSIGNED_DOUBLE.getSqlType(), PhoenixArray.class,
			null) {
		@Override
    	public boolean isArrayType() {
    		return true;
    	}
		@Override
		public boolean isFixedWidth() {
			return false;
		}
		@Override
		public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
			return pDataTypeForArray.compareTo(lhs, rhs);
		}

		@Override
		public Integer getByteSize() {
			return null;
		}

        @Override
        public byte[] toBytes(Object object) {
            return toBytes(object, SortOrder.ASC);
        }

        @Override
        public byte[] toBytes(Object object, SortOrder sortOrder) {
            return pDataTypeForArray.toBytes(object, PDataType.UNSIGNED_DOUBLE, sortOrder);
        }

		@Override
		public int toBytes(Object object, byte[] bytes, int offset) {
			return pDataTypeForArray.toBytes(object, bytes, offset);
		}

		@Override
		public Object toObject(String value) {
			return pDataTypeForArray.toObject(value);
		}
		
		@Override
		public Object toObject(Object object, PDataType actualType) {
			return pDataTypeForArray.toObject(object, actualType);
		}
		
		@Override
		public Object toObject(byte[] bytes, int offset, int length,
				PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
            return pDataTypeForArray.toObject(bytes, offset, length, PDataType.UNSIGNED_DOUBLE, sortOrder, maxLength,
                    scale, PDataType.UNSIGNED_DOUBLE);
		}
		
		@Override
        public boolean isCoercibleTo(PDataType targetType) {
            return pDataTypeForArray.isCoercibleTo(targetType, PDataType.UNSIGNED_DOUBLE_ARRAY);
        }

        @Override
        public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType, Integer maxLength,
                Integer scale, SortOrder actualModifer, Integer desiredMaxLength, Integer desiredScale,
                SortOrder desiredModifier) {
            pDataTypeForArray.coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
                    this, actualModifer, desiredModifier);
        }
		
		@Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
           PhoenixArray pArr = (PhoenixArray)value;
           double[] doubleArr = (double[])pArr.array;
           for (double i : doubleArr) {
               if(!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, i) && (!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_TIMESTAMP, i))
            		   && (!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_TIME, i)) && (!pDataTypeForArray.isCoercibleTo(PDataType.UNSIGNED_DATE, i))) {
                   return false;
               }
           }
           return true;
        }

        @Override
        public int getResultSetSqlType() {
            return Types.ARRAY;
        }
		
	};

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
    final PArrayDataType pDataTypeForArray = new PArrayDataType();
    
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
        if(isArrayType()) {
            PhoenixArray array = (PhoenixArray)o;
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
        if(isArrayType()) {
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
                               byte[] rhs, int rhsOffset, int rhsLength, SortOrder rhsSortOrder, PDataType rhsType) {
        Preconditions.checkNotNull(lhsSortOrder);    	    	
        Preconditions.checkNotNull(rhsSortOrder);    	
        if (this.isBytesComparableWith(rhsType)) { // directly compare the bytes
            return compareTo(lhs, lhsOffset, lhsLength, lhsSortOrder, rhs, rhsOffset, rhsLength, rhsSortOrder);
        }
        PDataCodec lhsCodec = this.getCodec();
        if (lhsCodec == null) { // no lhs native type representation, so convert rhsType to bytes representation of lhsType
            byte[] rhsConverted = this.toBytes(this.toObject(rhs, rhsOffset, rhsLength, rhsType, rhsSortOrder));            
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
            byte[] lhsConverted = rhsType.toBytes(rhsType.toObject(lhs, lhsOffset, lhsLength, this, lhsSortOrder));
            if (lhsSortOrder == SortOrder.DESC) {
                lhsSortOrder = SortOrder.ASC;
            }
            if (rhsSortOrder == SortOrder.DESC) {
                rhs = SortOrder.invert(rhs, rhsOffset, new byte[rhsLength], 0, rhsLength);
            }            
            return Bytes.compareTo(lhsConverted, 0, lhsConverted.length, rhs, rhsOffset, rhsLength);
        }
        // convert to native and compare
        if(this.isCoercibleTo(PDataType.LONG) && rhsType.isCoercibleTo(PDataType.LONG)) { // native long to long comparison
            return Longs.compare(this.getCodec().decodeLong(lhs, lhsOffset, lhsSortOrder), rhsType.getCodec().decodeLong(rhs, rhsOffset, rhsSortOrder));
        } else if (isDoubleOrFloat(this) && isDoubleOrFloat(rhsType)) { // native double to double comparison
            return Doubles.compare(this.getCodec().decodeDouble(lhs, lhsOffset, lhsSortOrder), rhsType.getCodec().decodeDouble(rhs, rhsOffset, rhsSortOrder));
        } else { // native float/double to long comparison
            float fvalue = 0.0F;
            double dvalue = 0.0;
            long lvalue = 0;
            boolean isFloat = false;
            int invert = 1;
            
            if (this.isCoercibleTo(PDataType.LONG)) {
                lvalue = this.getCodec().decodeLong(lhs, lhsOffset, lhsSortOrder);
            } else if (this == PDataType.FLOAT) {
                isFloat = true;
                fvalue = this.getCodec().decodeFloat(lhs, lhsOffset, lhsSortOrder);
            } else if (this.isCoercibleTo(PDataType.DOUBLE)) {
                dvalue = this.getCodec().decodeDouble(lhs, lhsOffset, lhsSortOrder);
            }
            if (rhsType.isCoercibleTo(PDataType.LONG)) {
                lvalue = rhsType.getCodec().decodeLong(rhs, rhsOffset, rhsSortOrder);
            } else if (rhsType == PDataType.FLOAT) {
                invert = -1;
                isFloat = true;
                fvalue = rhsType.getCodec().decodeFloat(rhs, rhsOffset, rhsSortOrder);
            } else if (rhsType.isCoercibleTo(PDataType.DOUBLE)) {
                invert = -1;
                dvalue = rhsType.getCodec().decodeDouble(rhs, rhsOffset, rhsSortOrder);
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
            return decodeLong(ptr.get(),ptr.getOffset(), sortOrder);
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


    public static class LongCodec extends BaseCodec {

        private LongCodec() {
        }
        
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
            long v;
            byte b = bytes[o];
            if (sortOrder == SortOrder.ASC) {
                v = b ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
                    b = bytes[o + i];
                    v = (v << 8) + (b & 0xff);
                }
            } else {
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
        public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
            long v = decodeLong(b, o, sortOrder);
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
        public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
          long v = decodeLong(b, o, sortOrder);
          if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
          }
          return (byte)v;
        }

        @Override
        public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
          long v = decodeLong(b, o, sortOrder);
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

    public static class IntCodec extends BaseCodec {

        private IntCodec() {
        }

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
        public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
          int v = decodeInt(b, o, sortOrder);
          if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
              throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
          }
          return (byte)v;
        }

        @Override
        public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
          int v = decodeInt(b, o, sortOrder);
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
    
    public static class ShortCodec extends BaseCodec {

      private ShortCodec(){
      }
      
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
            throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
        }
        return (byte)v;
      }

      @Override
      public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
    	Preconditions.checkNotNull(sortOrder);
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
    
    public static class ByteCodec extends BaseCodec {

      private ByteCodec(){
      }
      
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
        int v;
        if (sortOrder == SortOrder.ASC) {
            v = b[o] ^ 0x80; // Flip sign bit back
        } else {
            v = b[o] ^ 0xff ^ 0x80; // Flip sign bit back
        }
        return (byte)v;
      }

      @Override
      public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
          return decodeByte(b, o, sortOrder);
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
                SortOrder sortOrder) {
            return decodeByte(b, o, sortOrder);
        }

        @Override
        public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
            return decodeByte(b, o, sortOrder);
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
    
    public static class UnsignedByteCodec extends ByteCodec {

      private UnsignedByteCodec(){  
      }

      @Override
      public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
    	Preconditions.checkNotNull(sortOrder);
        if (sortOrder == SortOrder.DESC) {
          b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_BYTE], 0, Bytes.SIZEOF_BYTE);
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
        public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
        	Preconditions.checkNotNull(sortOrder);
            long v = 0;
            if (sortOrder == SortOrder.ASC) {
                for(int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
                  v <<= 8;
                  v ^= b[i] & 0xFF;
                }
            } else {
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
      public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
    	  Preconditions.checkNotNull(sortOrder);
          if (sortOrder == SortOrder.DESC) {
              b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_INT], 0, Bytes.SIZEOF_INT);
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
        public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
        	Preconditions.checkNotNull(sortOrder);
            if (sortOrder == SortOrder.DESC) {
                b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_INT], 0, Bytes.SIZEOF_INT);
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
        public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
            float v = decodeFloat(b, o, sortOrder);
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Long without changing its value");
            }
            return (long)v;
        }

        @Override
        public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
            float v = decodeFloat(b, o, sortOrder);
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Integer without changing its value");
            }
            return (int) v;
        }

        @Override
        public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
            float v = decodeFloat(b, o, sortOrder);
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
            }
            return (byte) v;
        }

        @Override
        public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
            float v = decodeFloat(b, o, sortOrder);
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Short without changing its value");
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
    
    public static class DoubleCodec extends BaseCodec {

        private DoubleCodec(){
        }
        
        @Override
        public long decodeLong(byte[] b, int o, SortOrder sortOrder) {
            double v = decodeDouble(b, o, sortOrder);
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Long without changing its value");
            }
            return (long) v;
        }

        @Override
        public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
            double v = decodeDouble(b, o, sortOrder);
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Integer without changing its value");
            }
            return (int) v;
        }

        @Override
        public byte decodeByte(byte[] b, int o, SortOrder sortOrder) {
            double v = decodeDouble(b, o, sortOrder);
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Byte without changing its value");
            }
            return (byte) v;
        }

        @Override
        public short decodeShort(byte[] b, int o, SortOrder sortOrder) {
            double v = decodeDouble(b, o, sortOrder);
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Short without changing its value");
            }
            return (short) v;
        }
        
        @Override
        public double decodeDouble(byte[] b, int o, SortOrder sortOrder) {
        	Preconditions.checkNotNull(sortOrder);
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
        public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
        	Preconditions.checkNotNull(sortOrder);
            if (sortOrder == SortOrder.DESC) {
                b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_FLOAT], 0, Bytes.SIZEOF_FLOAT);
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
                SortOrder sortOrder) {
        	Preconditions.checkNotNull(sortOrder);
            if (sortOrder == SortOrder.DESC) {
                b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_DOUBLE], 0, Bytes.SIZEOF_DOUBLE);
            }
            double v = Bytes.toDouble(b, o);
            if (v < 0) {
                throw new IllegalDataException();
            }
            return v;
        }
    }

    public static class DateCodec extends LongCodec {

        private DateCodec() {
        }

        @Override
        public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public PhoenixArrayFactory getPhoenixArrayFactory() {
            return new PhoenixArrayFactory() {
                
                @Override
                public PhoenixArray newArray(PDataType type, Object[] elements) {
                    return new PhoenixArray(type, elements);
                }
            };
        }
    }

    public static class UnsignedDateCodec extends UnsignedLongCodec {

        private UnsignedDateCodec() {
        }

        @Override
        public int decodeInt(byte[] b, int o, SortOrder sortOrder) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public PhoenixArrayFactory getPhoenixArrayFactory() {
            return new PhoenixArrayFactory() {
                
                @Override
                public PhoenixArray newArray(PDataType type, Object[] elements) {
                    return new PhoenixArray(type, elements);
                }
            };
        }
    }

    public static final int MAX_PRECISION = 38; // Max precision guaranteed to fit into a long (and this should be plenty)
    public static final int MIN_DECIMAL_AVG_SCALE = 4;
    public static final MathContext DEFAULT_MATH_CONTEXT = new MathContext(MAX_PRECISION, RoundingMode.HALF_UP);
    public static final int DEFAULT_SCALE = 0;

    private static final Integer MAX_BIG_DECIMAL_BYTES = 21;
    private static final Integer MAX_TIMESTAMP_BYTES = Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;

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
    public static final byte[] NULL_BYTES = ByteUtil.EMPTY_BYTE_ARRAY;
    private static final Integer BOOLEAN_LENGTH = 1;

    public final static Integer ZERO = 0;
    public final static Integer INT_PRECISION = 10;
    public final static Integer LONG_PRECISION = 19;
    public final static Integer SHORT_PRECISION = 5;
    public final static Integer BYTE_PRECISION = 3;

    public static final int ARRAY_TYPE_BASE = 3000;
    
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

    public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
            Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
         return true;
    }

    public int compareTo(byte[] b1, byte[] b2) {
        return compareTo(b1, 0, b1.length, SortOrder.getDefault(), b2, 0, b2.length, SortOrder.getDefault());
    }

    public final int compareTo(ImmutableBytesWritable ptr1, ImmutableBytesWritable ptr2) {
        return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), SortOrder.getDefault(), ptr2.get(), ptr2.getOffset(), ptr2.getLength(), SortOrder.getDefault());
    }

    public final int compareTo(byte[] ba1, int offset1, int length1, SortOrder so1, byte[] ba2, int offset2, int length2, SortOrder so2) {
    	Preconditions.checkNotNull(so1);
    	Preconditions.checkNotNull(so2);
        if (so1 != so2) {
            int length = Math.min(length1, length2);
            for (int i = 0; i < length; i++) {
                byte b1 = ba1[offset1+i];
                byte b2 = ba2[offset2+i];
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
        return Bytes.compareTo(ba1, offset1, length1, ba2, offset2, length2) * (so1 == SortOrder.DESC ? -1 : 1);
    }

    public final int compareTo(ImmutableBytesWritable ptr1, SortOrder ptr1SortOrder, ImmutableBytesWritable ptr2, SortOrder ptr2SortOrder, PDataType type2) {
        return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), ptr1SortOrder, ptr2.get(), ptr2.getOffset(), ptr2.getLength(), ptr2SortOrder, type2);
    }

    public final int compareTo(Object lhs, Object rhs) {
        return compareTo(lhs,rhs,this);
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
    
    public final void coerceBytes(ImmutableBytesWritable ptr, PDataType actualType, SortOrder actualModifier, SortOrder expectedModifier) {
        coerceBytes(ptr, null, actualType, null, null, actualModifier, null, null, expectedModifier);
    }

    public final void coerceBytes(ImmutableBytesWritable ptr, PDataType actualType, SortOrder actualModifier,
            SortOrder expectedModifier, Integer desiredMaxLength) {
        coerceBytes(ptr, null, actualType, null, null, actualModifier, desiredMaxLength, null, expectedModifier);
    }

    private static Void throwConstraintViolationException(PDataType source, PDataType target) {
        throw new ConstraintViolationException(source + " cannot be coerced to " + target);
    }
    
    private static boolean isNonNegativeDate(java.util.Date date) {
        return (date == null || date.getTime() >= 0);
    }
    
    private static void throwIfNonNegativeDate(java.util.Date date) {
        if (!isNonNegativeDate(date)) {
            throw new IllegalDataException("Value may not be negative(" + date + ")");
        }
    }
    
    private static boolean isNonNegativeNumber(Number v) {
        return v == null || v.longValue() >= 0;
    }

    private static void throwIfNonNegativeNumber(Number v) {
        if (!isNonNegativeNumber(v)) {
            throw new IllegalDataException("Value may not be negative(" + v + ")");
        }
    }
    
    public abstract Integer getByteSize();
    public abstract boolean isFixedWidth();
    public abstract int compareTo(Object lhs, Object rhs, PDataType rhsType);

    /**
     * Convert from the object representation of a data type value into
     * the serialized byte form.
     * @param object the object to convert
     * @param bytes the byte array into which to put the serialized form of object
     * @param offset the offset from which to start writing the serialized form
     * @return the byte length of the serialized object
     */
    public abstract int toBytes(Object object, byte[] bytes, int offset);
    public abstract byte[] toBytes(Object object);
       
    /**
     * Convert from a string to the object representation of a given type
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
    public abstract Object toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale);
    
    public final Object toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder) {
        return toObject(bytes, offset, length, actualType, sortOrder, null, null);
    }
    
    public final Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
        return toObject(bytes, offset, length, actualType, SortOrder.getDefault());
    }
    
    public final Object toObject(ImmutableBytesWritable ptr, PDataType actualType) {
        return toObject(ptr, actualType, SortOrder.getDefault());
    }    
    
    public final Object toObject(ImmutableBytesWritable ptr, PDataType actualType, SortOrder sortOrder) {
        return this.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), actualType, sortOrder);
    }
    
    public final Object toObject(ImmutableBytesWritable ptr, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
        return this.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), actualType, sortOrder, maxLength, scale);
    }
    
    public final Object toObject(ImmutableBytesWritable ptr, SortOrder sortOrder, Integer maxLength, Integer scale) {
        return this.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), this, sortOrder, maxLength, scale);
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
    
    public static int sqlArrayType(String sqlTypeName) {
    	PDataType fromSqlTypeName = fromSqlTypeName(sqlTypeName);
    	return fromSqlTypeName.getSqlType() + PDataType.ARRAY_TYPE_BASE;
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

         
	private static interface PhoenixArrayFactory {
		PhoenixArray newArray(PDataType type, Object[] elements);
	}

	public static PhoenixArrayFactory[] ARRAY_FACTORY = new PhoenixArrayFactory[PDataType
			.values().length];
	static {
		int i = 0;
		for (PDataType type : PDataType.values()) {
			if (!type.isArrayType()) {
				if (type.getCodec() != null) {
					ARRAY_FACTORY[i++] = type.getCodec()
							.getPhoenixArrayFactory();
				} else {
					ARRAY_FACTORY[i++] = new PhoenixArrayFactory() {

						@Override
						public PhoenixArray newArray(PDataType type,
								Object[] elements) {
							return new PhoenixArray(type, elements);
						}
					};
				}
			}
		}
	}
	public static PDataType fromTypeId(int typeId) {
		int offset = typeId - SQL_TYPE_OFFSET;
		if (offset >= 0 && offset < SQL_TYPE_TO_PCOLUMN_DATA_TYPE.length) {
			PDataType type = SQL_TYPE_TO_PCOLUMN_DATA_TYPE[offset];
			if (type != null) {
				return type;
			}
		}
		throw new IllegalDataException("Unsupported sql type: " + typeId);
	}
	
	public static PhoenixArrayFactory[] getArrayFactory() {
		return ARRAY_FACTORY;
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
    
    public final String toStringLiteral(ImmutableBytesWritable ptr, Format formatter) {
        return toStringLiteral(ptr.get(),ptr.getOffset(),ptr.getLength(),formatter);
    }
    public final String toStringLiteral(byte[] b, Format formatter) {
        return toStringLiteral(b,0,b.length,formatter);
    }
    public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
        Object o = toObject(b,offset,length);
        if (formatter != null) {
            return formatter.format(o);
        }
        return o.toString();
    }

    public static PhoenixArray instantiatePhoenixArray(PDataType actualType,
            Object[] elements) {
        PhoenixArrayFactory factory = ARRAY_FACTORY[actualType.ordinal()];
        if (factory == null) {
            throw new IllegalArgumentException("Cannot create an array of " + actualType);
         }
         return factory.newArray(actualType, elements);
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
        throw new UnsupportedOperationException("Unsupported literal value [" + value + "] of type " + value.getClass().getName());
    }
    
    public int getNanos(ImmutableBytesWritable ptr, SortOrder sortOrder) {
        throw new UnsupportedOperationException("Operation not supported for type " + this);
    }
    
    public long getMillis(ImmutableBytesWritable ptr, SortOrder sortOrder) {
        throw new UnsupportedOperationException("Operation not supported for type " + this);
    }

    public Object pad(Object object, int maxLength) {
        return object;
    }
    
}
