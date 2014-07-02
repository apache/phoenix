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
package org.apache.phoenix.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Container for data transfer between server and client aggregation (FIRST|LAST|NTH)_VALUE functions
 *
 */
public class FirstLastNthValueDataContainer {

    protected boolean isAscending = false;
    protected int offset;
    protected TreeMap<byte[], byte[]> data;
    protected boolean isOrderValuesFixedLength = false;
    protected boolean isDataValuesFixedLength = false;

    public void setIsAscending(boolean ascending) {
        isAscending = ascending;
    }

    public void setData(TreeMap<byte[], byte[]> topValues) {
        data = topValues;
    }

    public void setFixedWidthOrderValues(boolean fixedSize) {
        isOrderValuesFixedLength = fixedSize;
    }

    public void setFixedWidthDataValues(boolean fixedSize) {
        isDataValuesFixedLength = fixedSize;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void setPayload(byte[] payload) {
        if (payload[0] == (byte) 1) {
            isAscending = true;
        }

        int lengthOfOrderValues = Bytes.toInt(payload, 1);
        int lengthOfDataValues = Bytes.toInt(payload, 5);
        int sizeOfMap = Bytes.toInt(payload, 9);

        data = new TreeMap<byte[], byte[]>(new Bytes.ByteArrayComparator());

        int payloadOffset = 13;

        for (; sizeOfMap != 0; sizeOfMap--) {
            byte[] key;
            byte[] value;

            if (lengthOfOrderValues != 0) {
                key = copy(payload, payloadOffset, lengthOfOrderValues);
                payloadOffset += lengthOfOrderValues;
            } else {
                int l = Bytes.toInt(payload, payloadOffset);
                payloadOffset += 4;
                key = copy(payload, payloadOffset, l);
                payloadOffset += l;
            }

            if (lengthOfDataValues != 0) {
                value = copy(payload, payloadOffset, lengthOfDataValues);
                payloadOffset += lengthOfDataValues;
            } else {
                int l = Bytes.toInt(payload, payloadOffset);
                payloadOffset += 4;
                value = copy(payload, payloadOffset, l);
                payloadOffset += l;
            }

            data.put(key, value);
        }

    }

    public byte[] getPayload() throws IOException {
        /*
        PAYLOAD STUCTURE

        what                    | size (bytes) | info
        is ascending            | 1            | 1 = asc, 0 = desc
        length of order by vals | 4            | 0 if dynamic length, size otherwise
        length of values        | 4            | 0 if dynamic length, size otherwise
      [ lenght of first order   | 4            | set if order is var length (optional) ]
        first order value       | n            | order by val
      [ lenght of first value   | 4            | set if value is var length (optional) ]
        first order value       | n            | data val
        ... and so on, repeat order by values and data values


        example with fixed length for data and order by values
        0           | 0000 0004         | 0000 0004        | 0000 0001       | 0000 000FF  | ...
        is ascendig | length order vals | length data vals | first order val | first value | ... more values

        example with dynamic length for data (length will be zeros)
        0           | 0000 0000         | 0000 0000        | 0000 0004          | 0000 000FF        | ...
        is ascendig | length order vals | length data vals | first order length | first order value | ... more values

        */

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        bos.write(isAscending ? (byte) 1 : (byte) 0);

        Entry<byte[], byte[]> firstEntry = data.firstEntry();
        if (isOrderValuesFixedLength) {
            bos.write(Bytes.toBytes(firstEntry.getKey().length));
        } else {
            bos.write(Bytes.toBytes(0));
        }

        if (isDataValuesFixedLength) {
            bos.write(Bytes.toBytes(firstEntry.getValue().length));
        } else {
            bos.write(Bytes.toBytes(0));
        }

        bos.write(Bytes.toBytes(data.size()));

        for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {

            if (!isOrderValuesFixedLength) {
                bos.write(Bytes.toBytes(entry.getKey().length));
            }
            bos.write(entry.getKey());

            if (!isDataValuesFixedLength) {
                bos.write(Bytes.toBytes(entry.getValue().length));
            }
            bos.write(entry.getValue());
        }

        return bos.toByteArray();
    }

    public boolean getIsAscending() {
        return isAscending;
    }

    public TreeMap getData() {
        return data;
    }

    // foreport functionality from org.apache.hadoop.hbase.util.Bytes 0.96
    public static byte[] copy(byte[] bytes, final int offset, final int length) {
        if (bytes == null) {
            return null;
        }

        byte[] result = new byte[length];
        System.arraycopy(bytes, offset, result, 0, length);
        return result;
    }
}
