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

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;

public class ByteUtilTest {

    @Test
    public void testSplitBytes() {
        byte[] startRow = Bytes.toBytes("EA");
        byte[] stopRow = Bytes.toBytes("EZ");
        byte[][] splitPoints = Bytes.split(startRow, stopRow, 10);
        for (byte[] splitPoint : splitPoints) {
            assertTrue(Bytes.toStringBinary(splitPoint), Bytes.compareTo(startRow, splitPoint) <= 0);
            assertTrue(Bytes.toStringBinary(splitPoint), Bytes.compareTo(stopRow, splitPoint) >= 0);
        }
    }
    
    @Test
    public void testVIntToBytes() {
        for (int i = -10000; i <= 10000; i++) {
            byte[] vint = Bytes.vintToBytes(i);
            int vintSize = vint.length;
            byte[] vint2 = new byte[vint.length];
            assertEquals(vintSize, ByteUtil.vintToBytes(vint2, 0, i));
            assertTrue(Bytes.BYTES_COMPARATOR.compare(vint,vint2) == 0);
        }
    }
    
    @Test
    public void testNextKey() {
        byte[] key = new byte[] {1};
        assertEquals((byte)2, ByteUtil.nextKey(key)[0]); 
        key = new byte[] {1, (byte)255};
        byte[] nextKey = ByteUtil.nextKey(key);
        byte[] expectedKey = new byte[] {2,(byte)0};
        assertArrayEquals(expectedKey, nextKey); 
        key = ByteUtil.concat(Bytes.toBytes("00D300000000XHP"), PInteger.INSTANCE.toBytes(Integer.MAX_VALUE));
        nextKey = ByteUtil.nextKey(key);
        expectedKey = ByteUtil.concat(Bytes.toBytes("00D300000000XHQ"), PInteger.INSTANCE.toBytes(Integer.MIN_VALUE));
        assertArrayEquals(expectedKey, nextKey);
        
        key = new byte[] {(byte)255};
        assertNull(ByteUtil.nextKey(key));
    }
}
