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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 * Prefix encoder for byte arrays. For decoding, see {@link PrefixByteDecoder}.
 *
 */
public class PrefixByteEncoder {
    private int maxLength;
    private final ImmutableBytesWritable previous = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
    
    public PrefixByteEncoder() {
    }

    /**
     * Resets the state of the encoder to its initial state (i.e. forgetting
     * the previous byte array that may have been encoded).
     */
    public void reset() {
        previous.set(ByteUtil.EMPTY_BYTE_ARRAY);
    }
    
    /**
     * @return the maximum length byte array encountered while encoding
     */
    public int getMaxLength() {
        return maxLength;
    }
    
    /**
     * Prefix encodes the byte array pointed to into the output stream
     * @param out output stream to encode into
     * @param ptr pointer to byte array to encode.
     * @throws IOException
     */
    public void encode(DataOutput out, ImmutableBytesWritable ptr) throws IOException {
        encode(out, ptr.get(), ptr.getOffset(), ptr.getLength());
    }
    
    /**
     * Prefix encodes the byte array into the output stream
     * @param out output stream to encode into
     * @param b byte array to encode
     * @throws IOException
     */
    public void encode(DataOutput out, byte[] b) throws IOException {
        encode(out, b, 0, b.length);
    }

    /**
     * Prefix encodes the byte array from offset to length into output stream. 
     * Instead of writing the entire byte array, only the portion of the byte array
     * that differs from the beginning of the previous byte array written is written.
     *  
     * @param out output stream to encode into
     * @param b byte array buffer
     * @param offset offset into byte array to start encoding
     * @param length length of byte array to encode
     * @throws IOException
     */
    public void encode(DataOutput out, byte[] b, int offset, int length) throws IOException {
          int i = 0;
          int prevOffset = previous.getOffset();
          byte[] prevBytes = previous.get();
          int prevLength = previous.getLength();
          int minLength = prevLength < b.length ? prevLength : b.length;
          for(i = 0; (i < minLength) && (prevBytes[prevOffset + i] == b[offset + i]); i++);
          WritableUtils.writeVInt(out, i);
          Bytes.writeByteArray(out, b, offset + i, length - i);
          previous.set(b, offset, length);
          if (length > maxLength) {
              maxLength = length;
          }
    }
}