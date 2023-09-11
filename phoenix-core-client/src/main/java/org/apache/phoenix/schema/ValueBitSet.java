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

import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.util.SizedUtil;


/**
 * 
 * Class to track whether or not a value is null.
 * The value is a zero-based position in the schema provided.
 *
 * 
 * @since 0.1
 * 
 */
public class ValueBitSet {
    public final static ValueBitSet EMPTY_VALUE_BITSET = new ValueBitSet();
    private static final int BITS_PER_LONG = 64;
    private static final int BITS_PER_SHORT = 16;
    private final long[] bits;
    private final ValueSchema schema;
    
    private int maxSetBit = -1;
    
    public static ValueBitSet newInstance(ValueSchema schema) {
        if (schema.getFieldCount() == schema.getMinNullable()) {
            return EMPTY_VALUE_BITSET;
        }
        return new ValueBitSet(schema);
    }
    
    private ValueBitSet() {
        schema = null;
        bits = new long[0];
    }
    
    private ValueBitSet(ValueSchema schema) {
        this.schema = schema;
        bits = new long[Math.max(1,(schema.getFieldCount() - schema.getMinNullable() + BITS_PER_LONG -1) / BITS_PER_LONG)];
    }
    
    public int getMaxSetBit() {
        return maxSetBit;
    }
    
    private boolean isVarLength() {
        return schema == null ? false : schema.getFieldCount() - schema.getMinNullable() > BITS_PER_SHORT;
    }
    
    public int getNullCount(int nBit, int nFields) {
        if (schema == null) {
            return 0;
        }
        int count = 0;
        int index = nBit/BITS_PER_LONG;
        // Shift right based on the bit index, because we aren't interested in the bits before this.
        int shiftRight = nBit % BITS_PER_LONG;
        int bitsToLeft = BITS_PER_LONG - shiftRight;
        // Shift left based on the number of fields we're interested in counting.
        int shiftLeft = Math.max(0, (BITS_PER_LONG - nFields));
        // Mask off the bits of interest by shifting the bitset.
        count += Math.min(nFields, bitsToLeft) - (Long.bitCount((bits[index] >>> shiftRight) << shiftLeft));
        // Subtract from the number of fields the total number of possible fields we looked at
        nFields -= bitsToLeft;
        if (nFields > 0) {
            // If more fields to count, then walk through the successive long bits
            while (nFields > BITS_PER_LONG) {
                count += BITS_PER_LONG - Long.bitCount(bits[++index]);
                nFields -= BITS_PER_LONG;
            }
            // Count the final remaining fields
            if (nFields > 0) {
                count += nFields - Long.bitCount(bits[++index] << (BITS_PER_LONG - nFields));
            }
        }
        return count;
    }
    
    /**
     * Serialize the value bit set into a byte array. The byte array
     * is expected to have enough room (use {@link #getEstimatedLength()}
     * to ensure enough room exists.
     * @param b the byte array into which to put the serialized bit set
     * @param offset the offset into the byte array
     * @return the incremented offset
     */
    public int toBytes(byte[] b, int offset) {
        if (schema == null) {
            return offset;
        }
        // If the total number of possible values is bigger than 16 bits (the
        // size of a short), then serialize the long array followed by the
        // array length.
        if (isVarLength()) {
            short nLongs = (short)((maxSetBit + BITS_PER_LONG) / BITS_PER_LONG);
            for (int i = 0; i < nLongs; i++) {
                offset = Bytes.putLong(b, offset, bits[i]);
            }
            offset = Bytes.putShort(b, offset, nLongs);
        } else { 
            // Else if the number of values is less than or equal to 16,
            // serialize the bits directly into a short.
            offset = Bytes.putShort(b, offset, (short)bits[0]);            
        }
        return offset;
    }
    
    public void clear() {
        Arrays.fill(bits, 0);
        maxSetBit = -1;
    }
    
    public boolean get(int nBit) {
        int lIndex = nBit / BITS_PER_LONG;
        int bIndex = nBit % BITS_PER_LONG;
        return (bits[lIndex] & (1L << bIndex)) != 0;
    }
    
    public void set(int nBit) {
        int lIndex = nBit / BITS_PER_LONG;
        int bIndex = nBit % BITS_PER_LONG;
        bits[lIndex] |= (1L << bIndex);
        maxSetBit = Math.max(maxSetBit, nBit);
    }
    
    public void or(ImmutableBytesWritable ptr) {
        or(ptr, isVarLength() ? Bytes.SIZEOF_SHORT + 1 : Bytes.SIZEOF_SHORT);
    }
    
    public void or(ImmutableBytesWritable ptr, int length) {
        if (schema == null || length == 0) {
            return;
        }
        if (length > Bytes.SIZEOF_SHORT) {
            int offset = ptr.getOffset() + ptr.getLength() - Bytes.SIZEOF_SHORT;
            short nLongs = Bytes.toShort(ptr.get(), offset);
            offset -= nLongs * Bytes.SIZEOF_LONG;
            for (int i = 0; i < nLongs; i++) {
                bits[i] |= Bytes.toLong(ptr.get(), offset);
                offset += Bytes.SIZEOF_LONG;
            }
            maxSetBit = Math.max(maxSetBit, nLongs * BITS_PER_LONG - 1);
        } else {
            long l = Bytes.toShort(ptr.get(), ptr.getOffset() + ptr.getLength() - Bytes.SIZEOF_SHORT);
            bits[0] |= l;
            maxSetBit = Math.max(maxSetBit, (bits[0] == 0 ? 0 : BITS_PER_SHORT) - 1);
        }
        
    }
    
    /**
     * @return Max serialization size
     */
    public int getEstimatedLength() {
        if (schema == null) {
            return 0;
        }
        return Bytes.SIZEOF_SHORT + (isVarLength() ? (maxSetBit + BITS_PER_LONG) / BITS_PER_LONG * Bytes.SIZEOF_LONG : 0);
    }
    
    public static int getSize(int nBits) {
        return SizedUtil.OBJECT_SIZE + SizedUtil.POINTER_SIZE + SizedUtil.ARRAY_SIZE + SizedUtil.INT_SIZE + (nBits + BITS_PER_LONG - 1) / BITS_PER_LONG * Bytes.SIZEOF_LONG;
    }
    
    /**
     * @return Size of object in memory
     */
    public int getSize() {
        if (schema == null) {
            return 0;
        }
        return SizedUtil.OBJECT_SIZE + SizedUtil.POINTER_SIZE + SizedUtil.ARRAY_SIZE + SizedUtil.LONG_SIZE * bits.length + SizedUtil.INT_SIZE;
    }

    public void or(ValueBitSet isSet) {
        for (int i = 0; i < bits.length; i++) {
            bits[i] |= isSet.bits[i];
        }
        maxSetBit = Math.max(maxSetBit, isSet.maxSetBit);
    }
}

