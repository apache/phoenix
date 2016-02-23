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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;

public class PrefixByteCodec {
    
    public static List<byte[]> decodeBytes(ImmutableBytesWritable encodedBytes, int maxLength) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(encodedBytes.get(), encodedBytes.getOffset(), encodedBytes.getLength());
        DataInput input = new DataInputStream(stream);
        PrefixByteDecoder decoder = new PrefixByteDecoder(maxLength);
        List<byte[]> listOfBytes = Lists.newArrayList();
        try {
            while (true) {
                ImmutableBytesWritable ptr = decoder.decode(input);
                // For this test, copy the bytes, but we wouldn't do this unless
                // necessary for non testing
                listOfBytes.add(ptr.copyBytes());
            }
        } catch (EOFException e) { // Ignore as this signifies we're done
            
        }
        return listOfBytes;
    }
    
    public static int encodeBytes(List<byte[]> listOfBytes, ImmutableBytesWritable ptr) throws IOException {
        try (TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(calculateSize(listOfBytes))) {
            DataOutput output = new DataOutputStream(stream);
            PrefixByteEncoder encoder = new PrefixByteEncoder();
            for (byte[] bytes : listOfBytes) {
                encoder.encode(output, bytes, 0, bytes.length);
            }
            ptr.set(stream.getBuffer(), 0, stream.size());
            return encoder.getMaxLength();
        }
    }
    
    public static int calculateSize(List<byte[]> listOfBytes) {
        int size = 0;
        for (byte[] bytes : listOfBytes) {
            size += bytes.length;
        }
        return size;
    }

    public static ImmutableBytesWritable decode(PrefixByteDecoder decoder, DataInput input) throws EOFException {
        try {
            ImmutableBytesWritable val= decoder.decode(input);
            return val;
        } catch(EOFException eof){
            throw eof;
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}