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
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;

public class PVarbinary extends PDataType<byte[]> {

  public static final PVarbinary INSTANCE = new PVarbinary();

  private PVarbinary() {
    super("VARBINARY", Types.VARBINARY, byte[].class, null, 22);
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    return (byte[]) object;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      return 0;
    }
    byte[] o = (byte[]) object;
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
  public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale) {
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
    return equalsAny(targetType, this, PBinary.INSTANCE);
  }

  @Override
  public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
      Integer maxLength, Integer scale, Integer desiredMaxLength,
      Integer desiredScale) {
    if (ptr.getLength() != 0 && srcType.equals(PBinary.INSTANCE) && maxLength != null
        && desiredMaxLength != null) {
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
    if (equalsAny(rhsType, this, PBinary.INSTANCE)) {
      return Bytes.compareTo((byte[]) lhs, (byte[]) rhs);
    } else {
      byte[] rhsBytes = rhsType.toBytes(rhs);
      return Bytes.compareTo((byte[]) lhs, rhsBytes);
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
    StringBuilder buf = new StringBuilder();
    buf.append('[');
    if (length > 0) {
        for (int i = o; i < length; i++) {
          buf.append(0xFF & b[i]);
          buf.append(',');
        }
        buf.setLength(buf.length()-1);
    }
    buf.append(']');
    return buf.toString();
  }

  @Override
  public String toStringLiteral(Object o, Format formatter) {
      return toStringLiteral((byte[])o, 0, ((byte[]) o).length, formatter);
  }
  
  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    int length = maxLength != null && maxLength > 0 ? maxLength : 1;
    byte[] b = new byte[length];
    RANDOM.get().nextBytes(b);
    return b;
  }
}
