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

import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;

import com.google.common.base.Preconditions;

public class PVarchar extends PDataType<String> {

  public static final PVarchar INSTANCE = new PVarchar();

  private PVarchar() {
    super("VARCHAR", Types.VARCHAR, String.class, null, 0);
  }

  @Override
  public byte[] toBytes(Object object) {
    // TODO: consider using avro UTF8 object instead of String
    // so that we get get the size easily
    if (object == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    return Bytes.toBytes((String) object);
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
  public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale) {
    if (!actualType.isCoercibleTo(this)) {
      throwConstraintViolationException(actualType, this);
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
    if (equalsAny(actualType, this, PChar.INSTANCE)) {
      String s = (String) object;
      return s == null || s.length() > 0 ? s : null;
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return equalsAny(targetType, this, PChar.INSTANCE, PVarbinary.INSTANCE, PBinary.INSTANCE);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (isCoercibleTo(targetType)) {
      if (targetType.equals(PChar.INSTANCE)) {
        return value != null;
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
      Integer maxLength, Integer scale, Integer desiredMaxLength,
      Integer desiredScale) {
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
    return ((String) lhs).compareTo((String) rhs);
  }

  @Override
  public Object toObject(String value) {
    return value;
  }

  @Override
  public boolean isBytesComparableWith(PDataType otherType) {
    return super.isBytesComparableWith(otherType) || otherType == PChar.INSTANCE;
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
    if (formatter != null) {
      return "'" + formatter.format(o) + "'";
    }
    return "'" + StringUtil.escapeStringConstant(o.toString()) + "'";
  }

  private char[] sampleChars = new char[1];

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    Preconditions.checkArgument(maxLength == null || maxLength >= 0);
    int length = maxLength != null ? maxLength : 1;
    if (length != sampleChars.length) {
      sampleChars = new char[length];
    }
    for (int i = 0; i < length; i++) {
      sampleChars[i] = (char) RANDOM.get().nextInt(Byte.MAX_VALUE);
    }
    return new String(sampleChars);
  }
}
