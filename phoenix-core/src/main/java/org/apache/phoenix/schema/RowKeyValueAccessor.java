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
package org.apache.phoenix.schema;

import static org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.util.ByteUtil;


/**
 * 
 * Class that encapsulates accessing a value stored in the row key.
 *
 * 
 * @since 0.1
 */
public class RowKeyValueAccessor implements Writable   {
    /**
     * Constructor solely for use during deserialization. Should not
     * otherwise be used.
     */
    public RowKeyValueAccessor() {
    }
    
    /**
     * Constructor to compile access to the value in the row key formed from
     * a list of PData.
     * 
     * @param data the list of data that make up the key
     * @param index the zero-based index of the data item to access.
     */
    public RowKeyValueAccessor(List<? extends PDatum> data, int index) {
        this.index = index;
        int[] offsets = new int[data.size()];
        int nOffsets = 0;
        Iterator<? extends PDatum> iterator = data.iterator();
        PDatum datum = iterator.next();
        int pos = 0;
        while (pos < index) {
            int offset = 0;
            if (datum.getDataType().isFixedWidth()) {
                do {
                    Integer maxLength = datum.getMaxLength();
                    offset += maxLength == null ? datum.getDataType().getByteSize() : maxLength;
                    datum = iterator.next();
                    pos++;
                } while (pos < index && datum.getDataType().isFixedWidth());
                offsets[nOffsets++] = offset; // Encode fixed byte offset as positive
            } else {
                do {
                    offset++; // Count the number of variable length columns
                    datum = iterator.next();
                    pos++;
                } while (pos < index && !datum.getDataType().isFixedWidth());
                offsets[nOffsets++] = -offset; // Encode number of variable length columns as negative
            }
        }
        if (nOffsets < offsets.length) {
            this.offsets = Arrays.copyOf(offsets, nOffsets);
        } else {
            this.offsets = offsets;
        }
        // Remember this so that we don't bother looking for the null separator byte in this case
        this.isFixedLength = datum.getDataType().isFixedWidth();
        this.hasSeparator = !isFixedLength && (datum != data.get(data.size()-1));
    }
    
    RowKeyValueAccessor(int[] offsets, boolean isFixedLength, boolean hasSeparator) {
        this.offsets = offsets;
        this.isFixedLength = isFixedLength;
        this.hasSeparator = hasSeparator;
    }

    private int index = -1; // Only available on client side
    private int[] offsets;
    private boolean isFixedLength;
    private boolean hasSeparator;

    public int getIndex() {
        return index;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (hasSeparator ? 1231 : 1237);
        result = prime * result + (isFixedLength ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(offsets);
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        RowKeyValueAccessor other = (RowKeyValueAccessor)obj;
        if (hasSeparator != other.hasSeparator) return false;
        if (isFixedLength != other.isFixedLength) return false;
        if (!Arrays.equals(offsets, other.offsets)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "RowKeyValueAccessor [offsets=" + Arrays.toString(offsets) + ", isFixedLength=" + isFixedLength
                + ", hasSeparator=" + hasSeparator + "]";
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // Decode hasSeparator and isFixedLength from vint storing offset array length
        int length = WritableUtils.readVInt(input);
        hasSeparator = (length & 0x02) != 0;
        isFixedLength = (length & 0x01) != 0;
        length >>= 2;
        offsets = ByteUtil.deserializeVIntArray(input, length);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // Encode hasSeparator and isFixedLength into vint storing offset array length
        // (since there's plenty of room)
        int length = offsets.length << 2;
        length |= (hasSeparator ? 1 << 1 : 0) | (isFixedLength ? 1 : 0);
        ByteUtil.serializeVIntArray(output, offsets, length);
    }
    
    /**
     * Calculate the byte offset in the row key to the start of the PK column value
     * @param keyBuffer the byte array of the row key
     * @param keyOffset the offset in the byte array of where the key begins
     * @return byte offset to the start of the PK column value
     */
    public int getOffset(byte[] keyBuffer, int keyOffset) {
        // Use encoded offsets to navigate through row key buffer
        for (int offset : offsets) {
            if (offset >= 0) { // If offset is non negative, it's a byte offset
                keyOffset += offset;
            } else { // Else, a negative offset is the number of variable length values to skip
                while (offset++ < 0) {
                    // FIXME: keyOffset < keyBuffer.length required because HBase passes bogus keys to filter to position scan (HBASE-6562)
                    while (keyOffset < keyBuffer.length && keyBuffer[keyOffset++] != SEPARATOR_BYTE) {
                    }
                }
            }
        }
        return keyOffset;
    }
    
    /**
     * Calculate the length of the PK column value
     * @param keyBuffer the byte array of the row key
     * @param keyOffset the offset in the byte array of where the key begins
     * @param maxOffset maximum offset to use while calculating length 
     * @return the length of the PK column value
     */
    public int getLength(byte[] keyBuffer, int keyOffset, int maxOffset) {
        if (!hasSeparator) {
            return maxOffset - keyOffset;
        }
        int offset = keyOffset;
        // FIXME: offset < maxOffset required because HBase passes bogus keys to filter to position scan (HBASE-6562)
        while (offset < maxOffset && keyBuffer[offset] != SEPARATOR_BYTE) {
            offset++;
        }
        return offset - keyOffset;
    }
}
