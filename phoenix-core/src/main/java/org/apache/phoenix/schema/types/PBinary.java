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
import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.SortOrder;

public class PBinary extends PBinaryBase {

    public static final PBinary INSTANCE = new PBinary();

    private PBinary() {
        super("BINARY", Types.BINARY, byte[].class, null, 23);
    }

    @Override
    public void coerceBytes(ImmutableBytesWritable ptr, Object o, PDataType actualType, Integer actualMaxLength,
            Integer actualScale, SortOrder actualModifier, Integer desiredMaxLength, Integer desiredScale,
            SortOrder expectedModifier) {
        PVarbinary.INSTANCE.coerceBytes(ptr, o, actualType, actualMaxLength, actualScale, actualModifier, desiredMaxLength, desiredScale, expectedModifier);
        if (null != desiredMaxLength && null != expectedModifier) {
            pad(ptr, desiredMaxLength, expectedModifier);
        }
    }

    @Override
    public byte[] pad(byte[] b, Integer maxLength, SortOrder sortOrder) {
        if (b == null || b.length >= maxLength) {
            return b;
        }
        byte[] newBytes = new byte[maxLength];
        System.arraycopy(b, 0, newBytes, 0, b.length);
        if (sortOrder == SortOrder.DESC) {
            Arrays.fill(newBytes, b.length, maxLength, QueryConstants.DESC_SEPARATOR_BYTE);
        }
        return newBytes;
    }

    @Override
    public void pad(ImmutableBytesWritable ptr, Integer maxLength, SortOrder sortOrder) {
        if (ptr.getLength() >= maxLength) {
            return;
        }
        byte[] newBytes = new byte[maxLength];
        System.arraycopy(ptr.get(), ptr.getOffset(), newBytes, 0, ptr.getLength());
        if (sortOrder == SortOrder.DESC) {
            Arrays.fill(newBytes, ptr.getLength(), maxLength, QueryConstants.DESC_SEPARATOR_BYTE);
        }
        ptr.set(newBytes);
    }

    @Override
    public Object pad(Object object, Integer maxLength) {
        byte[] b = (byte[]) object;
        int length = (b == null ? 0 : b.length);
        if (length == maxLength) {
            return object;
        }
        if (length > maxLength) {
            throw new DataExceedsCapacityException(this, maxLength, null);
        }
        byte[] newBytes = new byte[maxLength];
        System.arraycopy(b, 0, newBytes, 0, length);

        return newBytes;
    }

    @Override
    public byte[] toBytes(Object object) { // Delegate to VARBINARY
        if (object == null) {
            throw newIllegalDataException(this + " may not be null");
        }
        return PVarbinary.INSTANCE.toBytes(object);
    }

    @Override
    public int toBytes(Object object, byte[] bytes, int offset) {
        if (object == null) {
            throw newIllegalDataException(this + " may not be null");
        }
        return PVarbinary.INSTANCE.toBytes(object, bytes, offset);

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
    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
            SortOrder sortOrder, Integer maxLength, Integer scale) {
        if (!actualType.isCoercibleTo(this)) {
            throwConstraintViolationException(actualType, this);
        }
        return PVarbinary.INSTANCE.toObject(bytes, offset, length, actualType, sortOrder);
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
        return equalsAny(targetType, this, PVarbinary.INSTANCE);
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
        if (equalsAny(rhsType, PVarbinary.INSTANCE, PBinary.INSTANCE)) {
            return Bytes.compareTo((byte[]) lhs, (byte[]) rhs);
        } else {
            byte[] rhsBytes = rhsType.toBytes(rhs);
            return Bytes.compareTo((byte[]) lhs, rhsBytes);
        }
    }

    @Override
    public Integer getMaxLength(Object o) {
        if (o == null) {
            return null;
        }
        byte[] value = (byte[]) o;
        return value.length;
    }

    @Override
    public Object toObject(String value) {
        return PVarbinary.INSTANCE.toObject(value);
    }

    @Override
    public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
        if (length == 1) {
            return Integer.toString(0xFF & b[offset]);
        }
        return PVarbinary.INSTANCE.toStringLiteral(b, offset, length, formatter);
    }

    @Override
    public String toStringLiteral(Object o, Format formatter) {
        return toStringLiteral((byte[])o, 0, ((byte[]) o).length, formatter);
    }

    @Override
    public Object getSampleValue(Integer maxLength, Integer arrayLength) {
        return PVarbinary.INSTANCE.getSampleValue(maxLength, arrayLength);
    }
}
