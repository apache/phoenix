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
package org.apache.phoenix.execute;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

public class DescVarLengthFastByteComparisonsTest {
    
    @Test
    public void testNullIsSmallest() {
        byte[] b1 = ByteUtil.EMPTY_BYTE_ARRAY;
        byte[] b2 = Bytes.toBytes("a");
        int cmp = DescVarLengthFastByteComparisons.compareTo(b1, 0, b1.length, b2, 0, b2.length);
        assertTrue(cmp < 0);
        cmp = DescVarLengthFastByteComparisons.compareTo(b2, 0, b2.length, b1, 0, b1.length);
        assertTrue(cmp > 0);
    }
    
    @Test
    public void testShorterSubstringIsBigger() {
        byte[] b1 = Bytes.toBytes("ab");
        byte[] b2 = Bytes.toBytes("a");
        int cmp = DescVarLengthFastByteComparisons.compareTo(b1, 0, b1.length, b2, 0, b2.length);
        assertTrue(cmp < 0);
    }
}
