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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.google.common.base.Preconditions;

/**
 * A ColumnModifier implementation modifies how bytes are stored in a primary key column.</p>  
 * The {@link ColumnModifier#apply apply} method is called when the bytes for a specific column are first written to HBase and again
 * when they are read back.  Phoenix attemps to minimize calls to apply when bytes are read out of HBase.   
 * 
 * 
 * @since 1.2
 */
public enum ColumnModifier {
    /**
     * Invert the bytes in the src byte array to support descending ordering of row keys.
     */
    SORT_DESC(1) {
        @Override
        public byte[] apply(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length) {
            Preconditions.checkNotNull(src);            
            Preconditions.checkNotNull(dest);            
            for (int i = 0; i < length; i++) {
                dest[dstOffset+i] = (byte)(src[srcOffset+i] ^ 0xFF);
            }                       
            return dest;
        }

        @Override
        public byte apply(byte b) {
            return (byte)(b ^ 0xFF);
        }

        @Override
        public CompareOp transform(CompareOp op) {
            switch (op) {
                case EQUAL:
                    return op;
                case GREATER:
                    return CompareOp.LESS;
                case GREATER_OR_EQUAL:
                    return CompareOp.LESS_OR_EQUAL;
                case LESS:
                    return CompareOp.GREATER;
                case LESS_OR_EQUAL:
                    return CompareOp.GREATER_OR_EQUAL;
                default:
                    throw new IllegalArgumentException("Unknown operator " + op);
            }
        }

        @Override
        public byte[] apply(byte[] src, int srcOffset, int length) {
            return apply(src, srcOffset, new byte[length], 0, length);
        }
    };
        
    private final int serializationId;
    
    ColumnModifier(int serializationId) {
        this.serializationId = serializationId;
    }
    
    public int getSerializationId() {
        return serializationId;
    }
    /**
     * Returns the ColumnModifier for the specified DDL stmt keyword.
     */
    public static ColumnModifier fromDDLValue(String modifier) {
        if (modifier == null) {
            return null;
        } else if (modifier.equalsIgnoreCase("ASC")) {
            return null;
        } else if (modifier.equalsIgnoreCase("DESC")) {
            return SORT_DESC;
        } else {
            return null;
        }                       
    }

   /**
    * Returns the ColumnModifier for the specified internal value.
    */
    public static ColumnModifier fromSystemValue(int value) {
        for (ColumnModifier mod : ColumnModifier.values()) {
            if (mod.getSerializationId() == value) {
                return mod;
            }
        }
        return null;
    }

    /**
     * Returns an internal value representing the specified ColumnModifier.
     */
    public static int toSystemValue(ColumnModifier columnModifier) {
        if (columnModifier == null) {
            return 0;
        }
        return columnModifier.getSerializationId();
    }

    /**
     * Copies the bytes from source array to destination array and applies the column modifier operation on the bytes
     * starting at the specified offsets.  The column modifier is applied to the number of bytes matching the 
     * specified length.
     * @param src  the source byte array to copy from, cannot be null
     * @param srcOffset the offset into the source byte array at which to begin.
     * @param dest the destination byte array into which to transfer the modified bytes.
     * @param dstOffset the offset into the destination byte array at which to begin
     * @param length the number of bytes for which to apply the modification
     * @return the destination byte array
     */
    public abstract byte[] apply(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length);
    /**
     * Copies the bytes from source array to a newly allocated destination array and applies the column
     * modifier operation on the bytes starting at the specified offsets.  The column modifier is applied
     * to the number of bytes matching the specified length.
     * @param src  the source byte array to copy from, cannot be null
     * @param srcOffset the offset into the source byte array at which to begin.
     * @param length the number of bytes for which to apply the modification
     * @return the newly allocated destination byte array
     */
    public abstract byte[] apply(byte[] src, int srcOffset, int length);
    public abstract byte apply(byte b);
    
    public abstract CompareOp transform(CompareOp op);
}
