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

import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.ONE_BYTE_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.THREE_BYTE_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.phoenix.schema.PTable.QualifierEncodingScheme.InvalidQualifierBytesException;
import org.junit.Test;

public class QualifierEncodingSchemeTest {
    
    @Test
    public void testOneByteQualifierEncodeDecode() {
        assertEquals(1, ONE_BYTE_QUALIFIERS.decode(ONE_BYTE_QUALIFIERS.encode(1)));
        assertEquals(127, ONE_BYTE_QUALIFIERS.decode(ONE_BYTE_QUALIFIERS.encode(127)));
        assertEquals(63, ONE_BYTE_QUALIFIERS.decode(ONE_BYTE_QUALIFIERS.encode(63)));
        assertEquals(130, ONE_BYTE_QUALIFIERS.decode(ONE_BYTE_QUALIFIERS.encode(130)));
        assertEquals(255, ONE_BYTE_QUALIFIERS.decode(ONE_BYTE_QUALIFIERS.encode(255)));
        byte[] arr1 = ONE_BYTE_QUALIFIERS.encode(255);
        byte[] arr2 = new byte[] {-128, arr1[0]};
        assertEquals(255, ONE_BYTE_QUALIFIERS.decode(arr2, 1, 1));
        try {
            ONE_BYTE_QUALIFIERS.decode(arr2);
            fail();
        } catch (InvalidQualifierBytesException expected) {}
        try {
            ONE_BYTE_QUALIFIERS.decode(arr2, 0, 2);
            fail();
        } catch (InvalidQualifierBytesException expected) {}
        
    }
    
    @Test
    public void testTwoByteQualifierEncodeDecode() {
        assertEquals(1, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(1)));
        assertEquals(127, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(127)));
        assertEquals(63, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(63)));
        assertEquals(130, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(130)));
        assertEquals(128, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(128)));
        assertEquals(129, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(129)));
        assertEquals(32767, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(32767)));
        assertEquals(32768, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(32768)));
        assertEquals(65535, TWO_BYTE_QUALIFIERS.decode(TWO_BYTE_QUALIFIERS.encode(65535)));
        byte[] arr1 = TWO_BYTE_QUALIFIERS.encode(65535);
        byte[] arr2 = new byte[] {-128, arr1[0], arr1[1]};
        assertEquals(65535, TWO_BYTE_QUALIFIERS.decode(arr2, 1, 2));
        try {
            TWO_BYTE_QUALIFIERS.decode(arr2);
            fail();
        } catch (InvalidQualifierBytesException expected) {}
    }
    
    @Test
    public void testThreeByteQualifierEncodeDecode() {
        assertEquals(1, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(1)));
        assertEquals(127, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(127)));
        assertEquals(63, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(63)));
        assertEquals(130, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(130)));
        assertEquals(128, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(128)));
        assertEquals(129, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(129)));
        assertEquals(32767, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(32767)));
        assertEquals(32768, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(32768)));
        assertEquals(65535, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(65535)));
        assertEquals(16777215, THREE_BYTE_QUALIFIERS.decode(THREE_BYTE_QUALIFIERS.encode(16777215)));
        byte[] arr1 = THREE_BYTE_QUALIFIERS.encode(16777215);
        byte[] arr2 = new byte[] {-128, arr1[0], arr1[1], arr1[2]};
        assertEquals(16777215, THREE_BYTE_QUALIFIERS.decode(arr2, 1, 3));
        try {
            THREE_BYTE_QUALIFIERS.decode(arr2, 0, 2);
            fail();
        } catch (InvalidQualifierBytesException expected) {}
    }
    
    @Test
    public void testFourByteQualifierEncodeDecode() {
        assertEquals(1, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(1)));
        assertEquals(127, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(127)));
        assertEquals(63, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(63)));
        assertEquals(130, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(130)));
        assertEquals(128, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(128)));
        assertEquals(129, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(129)));
        assertEquals(32767, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(32767)));
        assertEquals(32768, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(32768)));
        assertEquals(65535, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(65535)));
        assertEquals(Integer.MAX_VALUE, FOUR_BYTE_QUALIFIERS.decode(FOUR_BYTE_QUALIFIERS.encode(Integer.MAX_VALUE)));
        byte[] arr1 = FOUR_BYTE_QUALIFIERS.encode(Integer.MAX_VALUE);
        byte[] arr2 = new byte[] {-128, arr1[0], arr1[1], arr1[2], arr1[3]};
        assertEquals(Integer.MAX_VALUE, FOUR_BYTE_QUALIFIERS.decode(arr2, 1, 4));
        try {
            FOUR_BYTE_QUALIFIERS.decode(arr2);
            fail();
        } catch (InvalidQualifierBytesException expected) {}
        try {
            FOUR_BYTE_QUALIFIERS.decode(arr2, 0, 3);
            fail();
        } catch (InvalidQualifierBytesException expected) {}
    }
    
}
