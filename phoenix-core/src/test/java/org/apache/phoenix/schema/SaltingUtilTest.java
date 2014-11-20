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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class SaltingUtilTest {

    @Test
    public void testGetSaltingByte() {
        Set<Byte> saltBytes = Sets.newHashSet();
        for (int i = 0; i < 100; i++) {
            saltBytes.add(SaltingUtil.getSaltingByte(Bytes.toBytes(i), 0, Bytes.SIZEOF_INT, 3));
        }
        assertEquals(ImmutableSet.of((byte)0, (byte)1, (byte)2), saltBytes);
    }


    /**
     * Check an edge case where a row key's hash code is equal to Integer.MIN_VALUE.
     */
    @Test
    public void testGetSaltingByte_EdgeCaseHashCode() {
        // This array has a hashCode of Integer.MIN_VALUE based on the hashing in SaltingUtil
        byte[] rowKey = new byte[] { -106, 0, -10, 0, 19, -2 };
        byte saltingByte = SaltingUtil.getSaltingByte(rowKey, 0, rowKey.length, 3);

        assertTrue("Salting byte should be 0 or 1 or 2 but was " + saltingByte,
                ImmutableSet.of((byte)0, (byte)1, (byte)2).contains(saltingByte));

    }
}