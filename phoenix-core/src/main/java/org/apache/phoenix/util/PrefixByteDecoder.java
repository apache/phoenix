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

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 * Prefix decoder for byte arrays. For encoding, see {@link PrefixByteEncoder}.
 * 
 */
public class PrefixByteDecoder {
    private final int maxLength;
    private final ImmutableBytesWritable previous;
    
    /**
     * Used when the maximum length of encoded byte array is not known. Will
     * cause a new byte array to be allocated for each call to {@link #decode(DataInput)}.
     */
    public PrefixByteDecoder() {
        maxLength = -1;
        previous = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
    }

    /**
     * Used when the maximum length of encoded byte array is known in advance. 
     * Will not allocate new byte array with each call to {@link #decode(DataInput)}.
     * @param maxLength maximum length needed for any call to {@link #decode(DataInput)}.
     */
    public PrefixByteDecoder(int maxLength) {
        if (maxLength > 0) {
            this.maxLength = maxLength;
            this.previous = new ImmutableBytesWritable(new byte[maxLength], 0, 0);
        } else {
            this.maxLength = -1;
            previous = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
        }
    }
    
    /**
     * Resets state of decoder if it will be used to decode bytes from a
     * different DataInput.
     */
    public void reset() {
        previous.set(previous.get(),0,0);
    }
    
    /**
     * Decodes bytes encoded with {@link PrefixByteEncoder}.
     * @param in Input from which bytes are read.
     * @return Pointer containing bytes that were decoded. Note that the
     * same pointer will be returned with each call, so it must be consumed
     * prior to calling decode again.
     * @throws IOException
     */
    public ImmutableBytesWritable decode(DataInput in) throws IOException {
        int prefixLen = WritableUtils.readVInt(in);
        int suffixLen = WritableUtils.readVInt(in);
        int length = prefixLen + suffixLen;
        byte[] b;
        if (maxLength == -1) { // Allocate new byte array each time
            b = new byte[length];
            System.arraycopy(previous.get(), previous.getOffset(), b, 0, prefixLen);
        } else { // Reuse same buffer each time
            b = previous.get();
        }
        in.readFully(b, prefixLen, suffixLen);
        previous.set(b, 0, length);
        return previous;
    }
}