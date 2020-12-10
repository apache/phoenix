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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;

public abstract class PBinaryBase extends PDataType<byte[]> {

    protected PBinaryBase(String sqlTypeName, int sqlType, Class clazz,
            org.apache.phoenix.schema.types.PDataType.PDataCodec codec, int ordinal) {
        super(sqlTypeName, sqlType, clazz, codec, ordinal);
    }

    public void getByte(ImmutableBytesWritable ptr, SortOrder sortOrder, int offset,
            ImmutableBytesWritable outPtr) {
        getByte(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder, offset, outPtr);
    }

    public void getByte(byte[] bytes, int offset, int length, SortOrder sortOrder, int off,
            ImmutableBytesWritable outPtr) {
        byte ret = bytes[offset + off];
        if (sortOrder == SortOrder.DESC) ret = SortOrder.invert(ret);
        outPtr.set(PInteger.INSTANCE.toBytes(Integer.valueOf(ret)));
    }

    public void setByte(ImmutableBytesWritable ptr, SortOrder sortOrder, int offset, byte newValue,
            ImmutableBytesWritable outPtr) {
        setByte(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder, offset, newValue, outPtr);
    }

    public void setByte(byte[] bytes, int offset, int length, SortOrder sortOrder, int off,
            byte newValue, ImmutableBytesWritable outPtr) {
        byte[] ret;
        if (sortOrder == SortOrder.ASC) {
            ret = new byte[length];
            System.arraycopy(bytes, offset, ret, 0, length);
        } else {
            ret = SortOrder.invert(bytes, offset, length);
        }
        ret[off] = newValue;
        outPtr.set(ret);
    }

    public void getBit(ImmutableBytesWritable ptr, SortOrder sortOrder, int offset,
            ImmutableBytesWritable outPtr) {
        getBit(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder, offset, outPtr);
    }

    public void getBit(byte[] bytes, int offset, int length, SortOrder sortOrder, int off,
            ImmutableBytesWritable outPtr) {
        byte ret = bytes[offset + (off / Byte.SIZE)];
        if (sortOrder == SortOrder.DESC) ret = SortOrder.invert(ret);
        ret &= 1 << (off % Byte.SIZE);
        ret = (ret != 0) ? (byte) 1 : (byte) 0;
        outPtr.set(PInteger.INSTANCE.toBytes(Integer.valueOf(ret)));
    }

    public void setBit(ImmutableBytesWritable ptr, SortOrder sortOrder, int offset, byte newValue,
            ImmutableBytesWritable outPtr) {
        setBit(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder, offset, newValue, outPtr);
    }

    public void setBit(byte[] bytes, int offset, int length, SortOrder sortOrder, int off,
            byte newValue, ImmutableBytesWritable outPtr) {
        byte ret = bytes[offset + (off / Byte.SIZE)];
        if (sortOrder == SortOrder.DESC) ret = SortOrder.invert(ret);
        ret = (byte) ((ret & (~(1 << (off % Byte.SIZE)))) | (newValue << (off % Byte.SIZE)));
        setByte(bytes, offset, length, sortOrder, off / Byte.SIZE, ret, outPtr);
    }

    public void octetLength(ImmutableBytesWritable ptr, SortOrder sortOrder,
            ImmutableBytesWritable outPtr) {
        octetLength(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder, outPtr);
    }

    public void octetLength(byte[] bytes, int offset, int length, SortOrder sortOrder,
            ImmutableBytesWritable outPtr) {
        bytes = new byte[PInteger.INSTANCE.getByteSize()];
        PInteger.INSTANCE.getCodec().encodeInt(length, bytes, 0);
        outPtr.set(bytes);
    }

    @Override
    public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
        SortOrder sortOrder, Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
        if (ptr.getLength() != 0 && desiredMaxLength != null) {
            if (maxLength == null) { // If not specified, compute
                if (value != null && srcType instanceof PBinaryBase) { // Use value if provided
                    maxLength = ((byte[])value).length;
                } else { // Else use ptr, coercing (which is likely a noop)
                    this.coerceBytes(ptr, value, srcType, maxLength, scale, sortOrder, desiredMaxLength, desiredScale, sortOrder, true);
                    maxLength = ptr.getLength();
                }
            }
            return maxLength <= desiredMaxLength;
        }
        return true;
    }
}
