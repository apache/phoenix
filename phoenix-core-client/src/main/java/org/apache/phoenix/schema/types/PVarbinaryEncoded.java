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

import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;

public class PVarbinaryEncoded extends PVarbinary {

    public static final PVarbinaryEncoded INSTANCE = new PVarbinaryEncoded();

    private PVarbinaryEncoded() {
        super("VARBINARY_ENCODED", PDataType.VARBINARY_ENCODED_TYPE, byte[].class, null, 50);
    }

    private byte[] encodeBytesAscOrder(byte[] bytes) {
        int countZeros = 0;
        for (byte b : bytes) {
            if (b == (byte) 0x00) {
                countZeros++;
            }
        }
        if (countZeros == 0) {
            return bytes;
        }
        byte[] encodedBytes = new byte[bytes.length + countZeros];
        int pos = 0;
        for (byte b : bytes) {
            if (b != (byte) 0x00) {
                encodedBytes[pos++] = b;
            } else {
                encodedBytes[pos++] = (byte) 0x00;
                encodedBytes[pos++] = (byte) 0xFF;
            }
        }
        return encodedBytes;
    }

    private byte[] decodeBytesAscOrder(byte[] bytes) {
        int countZeros = 0;
        for (int i = 0; i < (bytes.length - 1); i++) {
            if (bytes[i] == (byte) 0x00 && bytes[i + 1] == (byte) 0xFF) {
                countZeros++;
            }
        }
        if (countZeros == 0) {
            return bytes;
        }
        byte[] decodedBytes = new byte[bytes.length - countZeros];
        int pos = 0;
        int i = 0;
        for (; i < (bytes.length - 1); i++) {
            if (bytes[i] == (byte) 0x00 && bytes[i + 1] == (byte) 0xFF) {
                decodedBytes[pos++] = (byte) 0x00;
                i++;
            } else {
                decodedBytes[pos++] = bytes[i];
            }
        }
        if (i == (bytes.length - 1)) {
            decodedBytes[pos] = bytes[bytes.length - 1];
        }
        return decodedBytes;
    }

    private byte[] encodeBytesDescOrder(byte[] bytes) {
        int countZeros = 0;
        for (byte b : bytes) {
            if (b == SortOrder.invert((byte) 0x00)) {
                countZeros++;
            }
        }
        if (countZeros == 0) {
            return bytes;
        }
        byte[] encodedBytes = new byte[bytes.length + countZeros];
        int pos = 0;
        for (byte b : bytes) {
            if (b != SortOrder.invert((byte) 0x00)) {
                encodedBytes[pos++] = b;
            } else {
                encodedBytes[pos++] = SortOrder.invert((byte) 0x00);
                encodedBytes[pos++] = SortOrder.invert((byte) 0xFF);
            }
        }
        return encodedBytes;
    }

    private byte[] decodeBytesDescOrder(byte[] bytes) {
        int countZeros = 0;
        for (int i = 0; i < (bytes.length - 1); i++) {
            if (bytes[i] == SortOrder.invert((byte) 0x00)
                && bytes[i + 1] == SortOrder.invert((byte) 0xFF)) {
                countZeros++;
            }
        }
        if (countZeros == 0) {
            return bytes;
        }
        byte[] decodedBytes = new byte[bytes.length - countZeros];
        int pos = 0;
        int i = 0;
        for (; i < (bytes.length - 1); i++) {
            if (bytes[i] == SortOrder.invert((byte) 0x00)
                && bytes[i + 1] == SortOrder.invert((byte) 0xFF)) {
                decodedBytes[pos++] = SortOrder.invert((byte) 0x00);
                i++;
            } else {
                decodedBytes[pos++] = bytes[i];
            }
        }
        if (i == (bytes.length - 1)) {
            decodedBytes[pos] = bytes[bytes.length - 1];
        }
        return decodedBytes;
    }

    @Override
    public byte[] toBytes(Object object) {
        return encodeBytesAscOrder(super.toBytes(object));
    }

    @Override
    public int toBytes(Object object, byte[] bytes, int offset) {
        if (object == null) {
            return 0;
        }
        byte[] o = (byte[]) object;
        System.arraycopy(bytes, offset, o, 0, o.length);
        byte[] result = encodeBytesAscOrder(o);
        return result.length;
    }

    @Override
    public byte[] toBytes(Object object, SortOrder sortOrder) {
        byte[] bytes;
        if (object == null) {
            bytes = ByteUtil.EMPTY_BYTE_ARRAY;
        } else {
            bytes = (byte[]) object;
        }
        if (sortOrder == SortOrder.DESC) {
            byte[] result = SortOrder.invert(bytes, 0, new byte[bytes.length], 0, bytes.length);
            return encodeBytesDescOrder(result);
        }
        return encodeBytesAscOrder(bytes);
    }

    @Override
    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
            SortOrder sortOrder, Integer maxLength, Integer scale) {
        if (length == 0) {
            return null;
        }
        if (offset == 0 && bytes.length == length && sortOrder == SortOrder.ASC) {
            return decodeBytesAscOrder(bytes);
        }
        byte[] bytesCopy = new byte[length];
        System.arraycopy(bytes, offset, bytesCopy, 0, length);
        if (sortOrder == SortOrder.DESC) {
            bytesCopy = SortOrder.invert(bytes, offset, bytesCopy, 0, length);
        }
        return decodeBytesAscOrder(bytesCopy);
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType) {
        return equalsAny(targetType, this, PBinary.INSTANCE, PVarbinary.INSTANCE);
    }

}
