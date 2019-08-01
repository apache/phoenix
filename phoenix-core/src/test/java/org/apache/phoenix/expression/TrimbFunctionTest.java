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
package org.apache.phoenix.expression;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.TrimbFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link org.apache.phoenix.expression.function.TrimbFunction}
 */
public class TrimbFunctionTest {

    private static final char HIGHEST_UNICODE_3BYTE_CHAR = 0xffff;
    private static final char HIGHEST_UNICODE_2BYTE_CHAR = 0x07ff;
    private static final char HIGHEST_UNICODE_1BYTE_CHAR = 0x007f;
    private static final char NULL_CHAR = 0x0000;
    private static final String ABC = "ABC";

    @Test
    public void testTrimTrippleByteChar() throws SQLException {
        char unicodeChar = HIGHEST_UNICODE_3BYTE_CHAR;
        byte[] resultBytes = ABC.getBytes();
        while(unicodeChar > HIGHEST_UNICODE_2BYTE_CHAR) {
            String dataString = unicodeChar+ABC+unicodeChar;
            byte[] dataBytes = dataString.getBytes();
            byte[] trimBytes = Character.toString(unicodeChar).getBytes();
            testTrimb(dataBytes, trimBytes, PBinary.INSTANCE, resultBytes);
            testTrimb(dataBytes, trimBytes, PVarbinary.INSTANCE, resultBytes);
            unicodeChar--;
        }
    }

    @Test
    public void testTrimDoubleByteChar() throws SQLException {
        char unicodeChar = HIGHEST_UNICODE_2BYTE_CHAR;
        byte[] resultBytes = ABC.getBytes();
        while(unicodeChar > HIGHEST_UNICODE_1BYTE_CHAR) {
            String dataString = unicodeChar+ABC+unicodeChar;
            byte[] dataBytes = dataString.getBytes();
            byte[] trimBytes = Character.toString(unicodeChar).getBytes();
            testTrimb(dataBytes, trimBytes, PBinary.INSTANCE, resultBytes);
            testTrimb(dataBytes, trimBytes, PVarbinary.INSTANCE, resultBytes);
            unicodeChar--;
        }
    }

    @Test
    public void testTrimSingleByteChar() throws SQLException {
        char unicodeChar = HIGHEST_UNICODE_1BYTE_CHAR;
        String resultString = "\0";
        byte[] resultBytes = resultString.getBytes();
        while(unicodeChar > NULL_CHAR) {
            String dataString = unicodeChar+resultString+unicodeChar;
            byte[] dataBytes = dataString.getBytes();
            byte[] trimBytes = Character.toString(unicodeChar).getBytes();
            testTrimb(dataBytes, trimBytes, PBinary.INSTANCE, resultBytes);
            testTrimb(dataBytes, trimBytes, PVarbinary.INSTANCE, resultBytes);
            unicodeChar--;
        }
    }

    @Test
    public void testTrimNullChar() throws SQLException {
        char unicodeChar = NULL_CHAR;
        byte[] resultBytes = ABC.getBytes();
        String dataString = unicodeChar+ABC+unicodeChar;
        byte[] dataBytes = dataString.getBytes();
        byte[] trimBytes = Character.toString(unicodeChar).getBytes();
        testTrimb(dataBytes, trimBytes, PBinary.INSTANCE, resultBytes);
        testTrimb(dataBytes, trimBytes, PVarbinary.INSTANCE, resultBytes);
    }

    private void testTrimbExpression(Expression data, Expression trim, byte[] expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList(data, trim);
        Expression trimbFunction = new TrimbFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        trimbFunction.evaluate(null, ptr);
        byte[] result = (byte[]) trimbFunction.getDataType().toObject(ptr,
                        trimbFunction.getSortOrder());
        assertEquals(new String(expected), new String(result));
    }

    private void testTrimb(byte[] bytes, byte[] trims, PDataType dataType, byte[] expected)
            throws SQLException {
        LiteralExpression dataBytesExpr, trimBytesExpr;
        dataBytesExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.ASC);
        trimBytesExpr = LiteralExpression.newConstant(trims, PVarbinary.INSTANCE, SortOrder.ASC);
        testTrimbExpression(dataBytesExpr, trimBytesExpr, expected);
        dataBytesExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.DESC);
        trimBytesExpr = LiteralExpression.newConstant(trims, PVarbinary.INSTANCE, SortOrder.DESC);
        testTrimbExpression(dataBytesExpr, trimBytesExpr, expected);
    }
}
