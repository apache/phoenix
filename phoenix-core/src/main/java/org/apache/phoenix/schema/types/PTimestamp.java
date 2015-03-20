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
import org.apache.phoenix.util.DateUtil;

public class PTimestamp extends PDataType<Timestamp> {

  public static final PTimestamp INSTANCE = new PTimestamp();

  private PTimestamp() {
    super("TIMESTAMP", Types.TIMESTAMP, java.sql.Timestamp.class,
        new PDate.DateCodec(), 9);
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
    java.sql.Timestamp value = (java.sql.Timestamp) object;
    PDate.INSTANCE.getCodec().encodeLong(value.getTime(), bytes, offset);

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
    if (equalsAny(actualType, PDate.INSTANCE, PUnsignedDate.INSTANCE, PTime.INSTANCE,
        PUnsignedTime.INSTANCE)) {
      return new java.sql.Timestamp(((java.util.Date) object).getTime());
    } else if (equalsAny(actualType, PTimestamp.INSTANCE, PUnsignedTimestamp.INSTANCE)) {
      return object;
    } else if (equalsAny(actualType, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
      return new java.sql.Timestamp((Long) object);
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) object;
      long ms = bd.longValue();
      int nanos =
          (bd.remainder(BigDecimal.ONE).multiply(QueryConstants.BD_MILLIS_NANOS_CONVERSION))
              .intValue();
      return DateUtil.getTimestamp(ms, nanos);
    } else if (actualType == PVarchar.INSTANCE) {
      return DateUtil.parseTimestamp((String) object);
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public java.sql.Timestamp toObject(byte[] b, int o, int l, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale) {
    if (actualType == null || l == 0) {
      return null;
    }
    java.sql.Timestamp v;
    if (equalsAny(actualType, PTimestamp.INSTANCE, PUnsignedTimestamp.INSTANCE)) {
      long millisDeserialized =
          (actualType == PTimestamp.INSTANCE ? PDate.INSTANCE : PUnsignedDate.INSTANCE).getCodec()
              .decodeLong(b, o, sortOrder);
      v = new java.sql.Timestamp(millisDeserialized);
      int nanosDeserialized =
          PUnsignedInt.INSTANCE.getCodec().decodeInt(b, o + Bytes.SIZEOF_LONG, sortOrder);
                /*
                 * There was a bug in serialization of timestamps which was causing the sub-second millis part
                 * of time stamp to be present both in the LONG and INT bytes. Having the <100000 check
                 * makes this serialization fix backward compatible.
                 */
      v.setNanos(
          nanosDeserialized < 1000000 ? v.getNanos() + nanosDeserialized : nanosDeserialized);
      return v;
    } else if (equalsAny(actualType, PDate.INSTANCE, PUnsignedDate.INSTANCE, PTime.INSTANCE,
        PUnsignedTime.INSTANCE, PLong.INSTANCE, PUnsignedLong.INSTANCE)) {
      return new java.sql.Timestamp(actualType.getCodec().decodeLong(b, o, sortOrder));
    } else if (actualType == PDecimal.INSTANCE) {
      BigDecimal bd = (BigDecimal) actualType.toObject(b, o, l, actualType, sortOrder);
      long ms = bd.longValue();
      int nanos = (bd.remainder(BigDecimal.ONE).multiply(QueryConstants.BD_MILLIS_NANOS_CONVERSION))
          .intValue();
      v = DateUtil.getTimestamp(ms, nanos);
      return v;
    }
    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return PDate.INSTANCE.isCastableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return equalsAny(targetType, this, PVarbinary.INSTANCE, PBinary.INSTANCE);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value != null) {
      if (targetType.equals(PUnsignedTimestamp.INSTANCE)) {
        return ((java.util.Date) value).getTime() >= 0;
      } else if (equalsAny(targetType, PUnsignedDate.INSTANCE, PUnsignedTime.INSTANCE)) {
        return ((java.util.Date) value).getTime() >= 0
            && ((java.sql.Timestamp) value).getNanos() == 0;
      } else if (equalsAny(targetType, PDate.INSTANCE, PTime.INSTANCE)) {
        return ((java.sql.Timestamp) value).getNanos() == 0;
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
    if (equalsAny(rhsType, PTimestamp.INSTANCE, PUnsignedTimestamp.INSTANCE)) {
      return ((java.sql.Timestamp) lhs).compareTo((java.sql.Timestamp) rhs);
    }
    int c = ((java.util.Date) rhs).compareTo((java.util.Date) lhs);
    if (c != 0) return c;
    return ((java.sql.Timestamp) lhs).getNanos();
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    return DateUtil.parseTimestamp(value);
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
      if (formatter == null) {
          formatter = DateUtil.DEFAULT_TIMESTAMP_FORMATTER;
        }
        return "'" + super.toStringLiteral(o, formatter) + "'";
  }


  @Override
  public int getNanos(ImmutableBytesWritable ptr, SortOrder sortOrder) {
    int nanos = PUnsignedInt.INSTANCE.getCodec()
        .decodeInt(ptr.get(), ptr.getOffset() + PLong.INSTANCE.getByteSize(), sortOrder);
    return nanos;
  }

  @Override
  public long getMillis(ImmutableBytesWritable ptr, SortOrder sortOrder) {
    long millis = PLong.INSTANCE.getCodec().decodeLong(ptr.get(), ptr.getOffset(), sortOrder);
    return millis;
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return new java.sql.Timestamp(
        (Long) PLong.INSTANCE.getSampleValue(maxLength, arrayLength));
  }
}
