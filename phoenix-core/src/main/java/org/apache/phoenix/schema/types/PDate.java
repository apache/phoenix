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
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.text.Format;
import java.time.ZoneOffset;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.StringUtil;

public class PDate extends PDataType<Date> {

    public static final PDate INSTANCE = new PDate();

    private PDate() {
        super("DATE", Types.DATE, Date.class,
                new DateCodec(), 11); // After TIMESTAMP and DATE to ensure toLiteral finds those first
    }

    @Override
    public byte[] toBytes(Object object) {
        byte[] bytes = new byte[getByteSize()];
        toBytes(object, bytes, 0);
        return bytes;
    }

    @Override
    public int toBytes(Object object, byte[] bytes, int offset) {
        if (object == null) {
            getCodec().encodeLong(0l, bytes, offset);
            return this.getByteSize();
        }
        getCodec().encodeLong(((java.util.Date) object).getTime(), bytes, offset);
        return this.getByteSize();
    }

    @Override
    public Object toObject(Object object, PDataType actualType) {
        if (object == null) {
            return null;
        }
        if (equalsAny(actualType, PTime.INSTANCE, PUnsignedTime.INSTANCE)) {
            return new Date(((java.sql.Time) object).getTime());
        } else if (equalsAny(actualType, PTimestamp.INSTANCE, PUnsignedTimestamp.INSTANCE)) {
            return new Date(((java.sql.Timestamp) object).getTime());
        } else if (equalsAny(actualType, PDate.INSTANCE, PUnsignedDate.INSTANCE)) {
            return object;
        } else if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
            return new Date((Long) object);
        } else if (actualType == PDecimal.INSTANCE) {
            return new Date(((BigDecimal) object).longValueExact());
        } else if (actualType == PVarchar.INSTANCE) {
            return DateUtil.parseDate((String) object);
        }
        return throwConstraintViolationException(actualType, this);
    }

    @Override
    public Date toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
        if (l == 0) {
            return null;
        }
        if (actualType.getCodec() != null ) {
            return new Date(actualType.getCodec().decodeLong(b, o, sortOrder));
        } else if (actualType == PTimestamp.INSTANCE) {
            return new Date(PDate.INSTANCE.getCodec().decodeLong(b, o, sortOrder));
        } else if (actualType == PUnsignedTimestamp.INSTANCE) {
            return new Date(PUnsignedDate.INSTANCE.getCodec().decodeLong(b, o, sortOrder));
        } else if (actualType == PDecimal.INSTANCE) {
            BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
            return new Date(bd.longValueExact());
        }
        throwConstraintViolationException(actualType, this);
        return null;
    }

    // Keep this in sync with PUnsignedDate
    @Override
    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
            SortOrder sortOrder, Integer maxLength, Integer scale, Class jdbcType)
            throws SQLException {
        java.sql.Date sqlDate =
                toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
        return dateToClass(sqlDate, actualType, jdbcType);
    }

    Object dateToClass(java.sql.Date sqlDate, PDataType actualType, Class jdbcType)
            throws SQLException {
        // FIXME java.time.Local conversions use ISO chronology, unlike the rest of Phoenix.
        if (jdbcType == java.time.LocalDate.class) {
            // FIXME this does a lot of unnecessary computation.
            return java.time.LocalDateTime
                    .ofInstant(java.time.Instant.ofEpochMilli(sqlDate.getTime()), ZoneOffset.UTC)
                    .toLocalDate();
        } else if (jdbcType == java.time.LocalDateTime.class) {
            // This is NOT JDBC compliant, but is useful because Dates are really Timestamps.
            // We cannot use toInstant(), as that nulls the time fields.
            return java.time.LocalDateTime
                    .ofInstant(java.time.Instant.ofEpochMilli(sqlDate.getTime()), ZoneOffset.UTC);
        } else if (jdbcType == java.time.LocalTime.class) {
            // This is NOT JDBC compliant, but is useful because Dates are really Timestamps.
            return java.time.LocalDateTime
                    .ofInstant(java.time.Instant.ofEpochMilli(sqlDate.getTime()), ZoneOffset.UTC)
                    .toLocalTime();
        } else if (jdbcType == java.sql.Date.class) {
            return sqlDate;
        } else if (jdbcType == java.sql.Time.class) {
            return new java.sql.Time(sqlDate.getTime());
        } else if (jdbcType == java.sql.Timestamp.class) {
            return new java.sql.Timestamp(sqlDate.getTime());
        } else if (jdbcType == java.util.Date.class) {
            return new java.util.Date(sqlDate.getTime());
        }
        throw newMismatchException(actualType, jdbcType);
    }

    @Override
    public boolean isCastableTo(PDataType targetType) {
        return super.isCastableTo(targetType) ||
                equalsAny(targetType, PDecimal.INSTANCE, PLong.INSTANCE, PUnsignedLong.INSTANCE);
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType) {
        return equalsAny(targetType, PDate.INSTANCE, PTime.INSTANCE, PTimestamp.INSTANCE, PVarbinary.INSTANCE, PBinary.INSTANCE);
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType, Object value) {
        if (value != null) {
            if (equalsAny(targetType, PUnsignedTimestamp.INSTANCE, PUnsignedDate.INSTANCE,
                    PUnsignedTime.INSTANCE)) {
                return ((java.util.Date) value).getTime() >= 0;
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
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return -1;
        }
        if (rhs == null) {
            return 1;
        }
        if (rhsType == PTimestamp.INSTANCE || rhsType == PUnsignedTimestamp.INSTANCE) {
            return -rhsType.compareTo(rhs, lhs, PTime.INSTANCE);
        }
        return ((java.util.Date) lhs).compareTo((java.util.Date) rhs);
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
        return super.isBytesComparableWith(otherType) || otherType == PTime.INSTANCE || otherType == PTimestamp.INSTANCE || otherType == PLong.INSTANCE;
    }

    @Override
    public String toStringLiteral(Object o, Format formatter) {
        if (formatter == null) {
            // If default formatter has not been overridden,
            // use default one.
            formatter = DateUtil.DEFAULT_DATE_FORMATTER;
        }
        return null == o ? String.valueOf(o) : "'"
                + StringUtil.escapeStringConstant(super.toStringLiteral(o, formatter)) + "'";
    }

    @Override
    public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType,
            Integer maxLength, Integer scale, SortOrder actualModifier, Integer desiredMaxLength, Integer desiredScale,
            SortOrder expectedModifier) {
        // Decrease size of TIMESTAMP to size of DATE and continue coerce
        if (ptr.getLength() > getByteSize()) {
            ptr.set(ptr.get(), ptr.getOffset(), getByteSize());
        }
        super.coerceBytes(ptr, object, actualType, maxLength, scale, actualModifier, desiredMaxLength,
                desiredScale, expectedModifier);
    }

    @Override
    public Object getSampleValue(Integer maxLength, Integer arrayLength) {
        return new Date((Long) PLong.INSTANCE.getSampleValue(maxLength, arrayLength));
    }

    static class DateCodec extends PLong.LongCodec {

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
}
