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
import java.sql.Time;
import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.DateUtil;

public class PTime extends PDataType<Time> {

  public static final PTime INSTANCE = new PTime();

  private PTime() {
    super("TIME", Types.TIME, java.sql.Time.class, new PDate.DateCodec(), 10);
  }

  @Override
  public byte[] toBytes(Object object) {
    return PDate.INSTANCE.toBytes(object);
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    return PDate.INSTANCE.toBytes(object, bytes, offset);
  }

  @Override
  public java.sql.Time toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    if (l == 0) {
      return null;
    }
    if (equalsAny(actualType, PTimestamp.INSTANCE, PUnsignedTimestamp.INSTANCE, PDate.INSTANCE,
        PUnsignedDate.INSTANCE, PTime.INSTANCE, PUnsignedTime.INSTANCE, PLong.INSTANCE,
        PUnsignedLong.INSTANCE)) {
      return new java.sql.Time(actualType.getCodec().decodeLong(b, o, sortOrder));
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
      return new java.sql.Time(bd.longValueExact());
    }
    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    if (object == null) {
      return null;
    }
    if (equalsAny(actualType, PDate.INSTANCE, PUnsignedDate.INSTANCE)) {
      return new java.sql.Time(((java.util.Date) object).getTime());
    } else if (equalsAny(actualType, PTimestamp.INSTANCE, PUnsignedTimestamp.INSTANCE)) {
      return new java.sql.Time(((java.sql.Timestamp) object).getTime());
    } else if (equalsAny(actualType, PTime.INSTANCE, PUnsignedTime.INSTANCE)) {
      return object;
    } else if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
      return new java.sql.Time((Long) object);
    } else if (actualType == PDecimal.INSTANCE) {
      return new java.sql.Time(((BigDecimal) object).longValueExact());
    } else if (actualType == PVarchar.INSTANCE) {
      return DateUtil.parseTime((String) object);
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return PDate.INSTANCE.isCastableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return PDate.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return PDate.INSTANCE.isCoercibleTo(targetType, value);
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
    return PDate.INSTANCE.compareTo(lhs, rhs, rhsType);
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
    return super.isBytesComparableWith(otherType) || otherType.equals(PDate.INSTANCE);
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
      if (formatter == null) {
          formatter = DateUtil.DEFAULT_TIME_FORMATTER;
        }
        return "'" + super.toStringLiteral(o, formatter) + "'";
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return new java.sql.Time((Long) PLong.INSTANCE.getSampleValue(maxLength, arrayLength));
  }
}
