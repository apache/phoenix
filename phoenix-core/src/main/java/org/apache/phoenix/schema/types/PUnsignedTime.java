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

import java.sql.Time;
import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;

public class PUnsignedTime extends PDataType<Time> {

  public static final PUnsignedTime INSTANCE = new PUnsignedTime();

  private PUnsignedTime() {
    super("UNSIGNED_TIME", 18, java.sql.Time.class, new PUnsignedDate.UnsignedDateCodec(), 13);
  }

  @Override
  public byte[] toBytes(Object object) {
    return PUnsignedDate.INSTANCE.toBytes(object);
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    return PUnsignedDate.INSTANCE.toBytes(object, bytes, offset);
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    java.sql.Time t = (java.sql.Time) PTime.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeDate(t);
    return t;
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    java.sql.Time t = (java.sql.Time) PTime.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeDate(t);
    return t;
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    return PUnsignedDate.INSTANCE.isCastableTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return PUnsignedDate.INSTANCE.isCoercibleTo(targetType);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType, value) || PTime.INSTANCE.isCoercibleTo(targetType, value);
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
    return PTime.INSTANCE.compareTo(lhs, rhs, rhsType);
  }

  @Override
  public Object toObject(String value) {
    return PTime.INSTANCE.toObject(value);
  }

  @Override
  public boolean isBytesComparableWith(PDataType otherType) {
    return super.isBytesComparableWith(otherType) || otherType == PUnsignedDate.INSTANCE;
  }

  @Override
  public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
    return PUnsignedDate.INSTANCE.toStringLiteral(b, offset, length, formatter);
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
    return PUnsignedDate.INSTANCE.toStringLiteral(o, formatter);
  }

  @Override
  public int getResultSetSqlType() {
    return Types.TIME;
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return new java.sql.Time(
        (Long) PUnsignedLong.INSTANCE.getSampleValue(maxLength, arrayLength));
  }
}
