/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.phoenix.util;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

public class StringUtilTest {

    private void testLpad(String inputString, int length, String fillString, String expectedOutput) throws Exception {
        byte[] input = inputString.getBytes();
        byte[] fill = fillString.getBytes();
        byte[] output = StringUtil.lpad(input, 0, input.length, fill, 0, fill.length, false, length);
        assertArrayEquals("Incorrect output of lpad", expectedOutput.getBytes(), output);
    }

    @Test
    public void testLpadFillLengthLessThanPadLength() throws Exception {
        testLpad("ABCD", 8, "12", "1212ABCD");
    }

    @Test
    public void testLpadFillLengthEqualPadLength() throws Exception {
        testLpad("ABCD", 8, "1234", "1234ABCD");
    }
    
    @Test
    public void testLpadFillLengthGreaterThanPadLength() throws Exception {
        testLpad("ABCD", 8, "12345", "1234ABCD");
    }

    @Test
    public void testLpadZeroPadding() throws Exception {
        testLpad("ABCD", 4, "1234", "ABCD");
    }
    
}
