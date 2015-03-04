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

import java.sql.Date;
import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.DateUtil;

public class PUnsignedDate extends PDataType<Date> {

  public static final PUnsignedDate INSTANCE = new PUnsignedDate();

  private PUnsignedDate() {
    super("UNSIGNED_DATE", 19, Date.class,
        new UnsignedDateCodec(), 14); // After TIMESTAMP and DATE to ensure toLiteral finds those first
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    byte[] bytes = new byte[getByteSize()];
    toBytes(object, bytes, 0);
    return bytes;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    getCodec().encodeLong(((java.util.Date) object).getTime(), bytes, offset);
    return this.getByteSize();
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Date d = (Date) PDate.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeDate(d);
    return d;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
    Date d = (Date) PDate.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeDate(d);
    return d;
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return PDate.INSTANCE.isCastableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return equalsAny(targetType, this, PUnsignedTime.INSTANCE, PUnsignedTimestamp.INSTANCE)
        || PDate.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PDate.INSTANCE.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return PDate.INSTANCE.getByteSize();
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return PDate.INSTANCE.compareTo(lhs, rhs, rhsType);
  }

  @Override
  public Object toObject(String value) {
    return PDate.INSTANCE.toObject(value);
  }

  @Override
  public boolean isBytesComparableWith(PDataType otherType) {
    return super.isBytesComparableWith(otherType) || otherType.equals(PUnsignedTime.INSTANCE);
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
    // Can't delegate, as the super.toStringLiteral calls this.toBytes
    if (formatter == null || formatter == DateUtil.DEFAULT_DATE_FORMATTER) {
      // If default formatter has not been overridden,
      // use one that displays milliseconds.
      formatter = DateUtil.DEFAULT_MS_DATE_FORMATTER;
    }
    return "'" + super.toStringLiteral(o, formatter) + "'";
  }

  @Override
  public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType,
      Integer maxLength, Integer scale, SortOrder actualModifier,
      Integer desiredMaxLength, Integer desiredScale,
      SortOrder expectedModifier) {
    if (ptr.getLength() > 0 && actualType == PUnsignedTimestamp.INSTANCE
        && actualModifier == expectedModifier) {
      ptr.set(ptr.get(), ptr.getOffset(), getByteSize());
      return;
    }
    super.coerceBytes(ptr, object, actualType, maxLength, scale, actualModifier, desiredMaxLength,
        desiredScale, expectedModifier);
  }

  @Override
  public int getResultSetSqlType() {
    return Types.DATE;
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return new Date((Long) PUnsignedLong.INSTANCE.getSampleValue(maxLength, arrayLength));
  }

  static class UnsignedDateCodec extends PUnsignedLong.UnsignedLongCodec {

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
