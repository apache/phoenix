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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.junit.Test;

public class PCharPadTest {

    public void test(String value, PDataType dataType, int length, SortOrder sortOrder, byte[] result) throws SQLException {
        LiteralExpression expr = LiteralExpression.newConstant(value, dataType, sortOrder);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr(expr.getBytes());
        dataType.pad(ptr, length, sortOrder);
        String resultValue = (String) dataType.toObject(ptr, dataType, sortOrder);
        assertTrue(Arrays.equals(result, ptr.get()));
        assertEquals(value, resultValue);
    }

    @Test
    public void testCharPaddingAsc1() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "hellow";
        byte[] result = new byte[]{104, 101, 108, 108, 111, 119, 32, 32, 32, 32};
        test(str, dataType, 10, SortOrder.ASC, result);
    }

    @Test
    public void testCharPaddingAsc2() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "phoenix";
        byte[] result = new byte[]{112, 104, 111, 101, 110, 105, 120, 32, 32, 32, 32, 32, 32, 32};
        test(str, dataType, 14, SortOrder.ASC, result);
    }

    @Test
    public void testCharPaddingAsc3() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "phoenix";
        byte[] result = new byte[]{112, 104, 111, 101, 110, 105, 120};
        test(str, dataType, 7, SortOrder.ASC, result);
    }

    @Test
    public void testCharPaddingAsc4() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "hello world";
        byte[] result = new byte[]{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 32, 32, 32, 32, 32};
        test(str, dataType, 16, SortOrder.ASC, result);
    }

    @Test
    public void testCharPaddingDesc1() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "hellow";
        byte[] result = new byte[]{-105, -102, -109, -109, -112, -120, -33, -33, -33, -33};
        test(str, dataType, 10, SortOrder.DESC, result);
    }

    @Test
    public void testCharPaddingDesc2() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "phoenix";
        byte[] result = new byte[]{-113, -105, -112, -102, -111, -106, -121, -33, -33, -33, -33, -33, -33, -33, -33};
        test(str, dataType, 15, SortOrder.DESC, result);
    }

    @Test
    public void testCharPaddingDesc3() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "phoenix";
        byte[] result = new byte[]{-113, -105, -112, -102, -111, -106, -121};
        test(str, dataType, 7, SortOrder.DESC, result);
    }

    @Test
    public void testCharPaddingDesc4() throws SQLException {
        PDataType dataType = PChar.INSTANCE;
        String str = "hello world";
        byte[] result = new byte[]{-105, -102, -109, -109, -112, -33, -120, -112, -115, -109, -101, -33, -33, -33, -33, -33};
        test(str, dataType, 16, SortOrder.DESC, result);
    }

    @Test
    public void testRelativeByteArrayOrder() throws SQLException {
        String[] inputs = {"foo", "foo!", "fooA", "foo~"};
        PDataType dataType = PChar.INSTANCE;
        Arrays.sort(inputs);
        List<byte[]> ascOrderedInputs = new ArrayList<>(inputs.length);
        SortOrder sortOrder = SortOrder.ASC;
        for (String input : inputs) {
            LiteralExpression expr = LiteralExpression.newConstant(input, dataType, sortOrder);
            ImmutableBytesPtr ptr = new ImmutableBytesPtr(expr.getBytes());
            dataType.pad(ptr, 8, sortOrder);
            ascOrderedInputs.add(ptr.copyBytes());
        }
        Collections.sort(ascOrderedInputs, Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < inputs.length; i++) {
            byte[] bytes = ascOrderedInputs.get(i);
            String resultValue = (String) dataType.toObject(bytes, 0, bytes.length, dataType, sortOrder);
            assertEquals(inputs[i], resultValue);
        }

        List<byte[]> descOrderedInputs = new ArrayList<>(inputs.length);
        sortOrder = SortOrder.DESC;
        for (String input : inputs) {
            LiteralExpression expr = LiteralExpression.newConstant(input, dataType, sortOrder);
            ImmutableBytesPtr ptr = new ImmutableBytesPtr(expr.getBytes());
            dataType.pad(ptr, 8, sortOrder);
            descOrderedInputs.add(ptr.copyBytes());
        }
        Collections.sort(descOrderedInputs, Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < inputs.length; i++) {
            byte[] bytes = descOrderedInputs.get(i);
            String resultValue = (String) dataType.toObject(bytes, 0, bytes.length, dataType, sortOrder);
            assertEquals(inputs[inputs.length - 1 - i], resultValue);
        }
    }
}
