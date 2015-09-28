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

import java.sql.Timestamp;
import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.DateUtil;

public class PUnsignedTimestamp extends PDataType<Timestamp> {

  public static final PUnsignedTimestamp INSTANCE = new PUnsignedTimestamp();

  private PUnsignedTimestamp() {
    super("UNSIGNED_TIMESTAMP", 20, java.sql.Timestamp.class, new PUnsignedDate.UnsignedDateCodec(), 12);
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
    PUnsignedDate.INSTANCE.getCodec().encodeLong(value.getTime(), bytes, offset);

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
    java.sql.Timestamp ts = (java.sql.Timestamp) PTimestamp.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeDate(ts);
    return ts;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    java.sql.Timestamp ts =
        (java.sql.Timestamp) PTimestamp.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeDate(ts);
    return ts;
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return PUnsignedDate.INSTANCE.isCastableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return targetType.equals(this) || PUnsignedDate.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PTimestamp.INSTANCE
        .isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return PTimestamp.INSTANCE.getByteSize();
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return PTimestamp.INSTANCE.compareTo(lhs, rhs, rhsType);
  }

  @Override
  public Object toObject(String value) {
    return PTimestamp.INSTANCE.toObject(value);
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
    long millis =
        PUnsignedLong.INSTANCE.getCodec().decodeLong(ptr.get(), ptr.getOffset(), sortOrder);
    return millis;
  }

  @Override
  public int getResultSetSqlType() {
    return Types.TIMESTAMP;
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return new java.sql.Timestamp(
        (Long) PUnsignedLong.INSTANCE.getSampleValue(maxLength, arrayLength));
  }
}
