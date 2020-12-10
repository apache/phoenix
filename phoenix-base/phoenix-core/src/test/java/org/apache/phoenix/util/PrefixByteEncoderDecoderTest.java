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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.query.QueryConstants;
import org.junit.Test;


public class PrefixByteEncoderDecoderTest {

    static final List<byte[]> guideposts = Arrays.asList(
            ByteUtil.concat(Bytes.toBytes("aaaaaaaaaa"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(1000L), Bytes.toBytes("bbbbbbbbbb")),
            ByteUtil.concat(Bytes.toBytes("aaaaaaaaaa"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(1000L), Bytes.toBytes("bbbbbccccc")),
            ByteUtil.concat(Bytes.toBytes("aaaaaaaaaa"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(2000L), Bytes.toBytes("bbbbbbbbbb")),
            ByteUtil.concat(Bytes.toBytes("bbbbbbbbbb"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(1000L), Bytes.toBytes("bbbbbbbbbb")),
            ByteUtil.concat(Bytes.toBytes("bbbbbbbbbb"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(2000L), Bytes.toBytes("bbbbbbbbbb")),
            ByteUtil.concat(Bytes.toBytes("bbbbbbbbbb"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(2000L), Bytes.toBytes("c")),
            ByteUtil.concat(Bytes.toBytes("bbbbbbbbbbb"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(1000L), Bytes.toBytes("bbbbbbbbbb")),
            ByteUtil.concat(Bytes.toBytes("d"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(1000L), Bytes.toBytes("bbbbbbbbbb")),
            ByteUtil.concat(Bytes.toBytes("d"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(1000L), Bytes.toBytes("bbbbbbbbbbc")),
            ByteUtil.concat(Bytes.toBytes("e"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(1000L), Bytes.toBytes("bbbbbbbbbb"))
            );
    
    @Test
    public void testEncode() throws IOException {
        List<byte[]> listOfBytes = Arrays.asList(Bytes.toBytes("aaaaa"), Bytes.toBytes("aaaabb"));
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        int maxLength = PrefixByteCodec.encodeBytes(listOfBytes, ptr);
        assertEquals(6, maxLength);
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(PrefixByteCodec.calculateSize(listOfBytes));
        DataOutput output = new DataOutputStream(stream);
        WritableUtils.writeVInt(output, 0);
        WritableUtils.writeVInt(output, 5);
        output.write(Bytes.toBytes("aaaaa")); // No space savings on first key
        WritableUtils.writeVInt(output, 4);
        WritableUtils.writeVInt(output, 2);
        output.write(Bytes.toBytes("bb")); // Only writes part of second key that's different
        assertArrayEquals(stream.toByteArray(), ptr.copyBytes());
    }
    
    @Test
    public void testEncodeDecodeWithSingleBuffer() throws IOException {
        testEncodeDecode(true);
    }
    
    @Test
    public void testEncodeDecodeWithNewBuffer() throws IOException {
        testEncodeDecode(false);
    }
    
    private void testEncodeDecode(boolean useSingleBuffer) throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        int maxLength = PrefixByteCodec.encodeBytes(guideposts, ptr);
        int encodedSize = ptr.getLength();
        int unencodedSize = PrefixByteCodec.calculateSize(guideposts);
        assertTrue(encodedSize < unencodedSize);
        List<byte[]> listOfBytes = PrefixByteCodec.decodeBytes(ptr, useSingleBuffer ? maxLength : -1);
        assertListByteArraysEquals(guideposts, listOfBytes);
    }
    
    private static void assertListByteArraysEquals(List<byte[]> listOfBytes1, List<byte[]> listOfBytes2) {
        assertEquals(listOfBytes1.size(), listOfBytes2.size());
        for (int i = 0; i < listOfBytes1.size(); i++) {
            assertArrayEquals(listOfBytes1.get(i), listOfBytes2.get(i));
        }
    }

}