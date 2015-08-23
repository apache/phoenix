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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ArrayToStringFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.*;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ArrayToStringFunctionTest {

    private static void testExpression(LiteralExpression array, LiteralExpression delimiter, LiteralExpression nullString, String expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) array);
        expressions.add(delimiter);
        expressions.add(nullString);

        Expression arrayToStringFunction = new ArrayToStringFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayToStringFunction.evaluate(null, ptr);
        String result = (String) arrayToStringFunction.getDataType().toObject(ptr, arrayToStringFunction.getSortOrder(), arrayToStringFunction.getMaxLength(), arrayToStringFunction.getScale());
        assertEquals(expected, result);
    }

    private static void test(PhoenixArray array, PDataType arrayDataType, Integer arrMaxLen, Integer arrScale, String delimiter, String nullString, String expected, SortOrder arraySortOrder, SortOrder delimiterSortOrder, SortOrder nullStringSortOrder) throws SQLException {
        LiteralExpression arrayLiteral, delimiterLiteral, nullStringLiteral;
        arrayLiteral = LiteralExpression.newConstant(array, arrayDataType, arrMaxLen, arrScale, arraySortOrder, Determinism.ALWAYS);
        delimiterLiteral = LiteralExpression.newConstant(delimiter, PVarchar.INSTANCE, null, null, delimiterSortOrder, Determinism.ALWAYS);
        nullStringLiteral = LiteralExpression.newConstant(nullString, PVarchar.INSTANCE, null, null, nullStringSortOrder, Determinism.ALWAYS);
        testExpression(arrayLiteral, delimiterLiteral, nullStringLiteral, expected);
    }

    @Test
    public void testInt1() throws SQLException {
        PDataType type = PIntegerArray.INSTANCE;
        PDataType base = PInteger.INSTANCE;
        Object[] o1 = new Object[]{1, 2, 3, 4, 5};
        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "1,2,3,4,5";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testInt2() throws SQLException {
        PDataType type = PIntegerArray.INSTANCE;
        PDataType base = PInteger.INSTANCE;
        Object[] o1 = new Object[]{1, 2, 3, 4, 5};
        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "";
        String expected = "1,2,3,4,5";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testInt3() throws SQLException {
        PDataType type = PIntegerArray.INSTANCE;
        PDataType base = PInteger.INSTANCE;
        Object[] o1 = new Object[]{1, 2, 3, 4, 5};
        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(base, o1);
        String delimiter = "";
        String nullString = "";
        String expected = null;
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testInt4() throws SQLException {
        PDataType type = PIntegerArray.INSTANCE;
        PDataType base = PInteger.INSTANCE;
        Object[] o1 = new Object[]{1};
        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "";
        String expected = "1";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testFloat1() throws SQLException {
        PDataType type = PFloatArray.INSTANCE;
        PDataType base = PFloat.INSTANCE;
        Object[] o1 = new Object[]{(float) 1.1, (float) 2.2, (float) 3.3, (float) 4.4, (float) 5.5};
        PhoenixArray arr = new PhoenixArray.PrimitiveFloatPhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "1.1,2.2,3.3,4.4,5.5";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testFloat2() throws SQLException {
        PDataType type = PFloatArray.INSTANCE;
        PDataType base = PFloat.INSTANCE;
        Object[] o1 = new Object[]{(float) 1.1, (float) 2.2, (float) 3.3, (float) 4.4, (float) 5.5};
        PhoenixArray arr = new PhoenixArray.PrimitiveFloatPhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "*";
        String expected = "1.1, 2.2, 3.3, 4.4, 5.5";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
    }
    
    @Test
    public void testDate() throws SQLException {
        PDataType type = PDateArray.INSTANCE;
        PDataType base = PDate.INSTANCE;
        Object[] o1 = new Object[]{new Date(0l), new Date(0l), new Date(0l)};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "*";
        String expected = "";
        for (int i = 0; i < o1.length - 1; i++) {
            expected += o1[i].toString() + ", ";
        }
        expected += o1[o1.length - 1];
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testTime() throws SQLException {
        PDataType type = PTimeArray.INSTANCE;
        PDataType base = PTime.INSTANCE;
        Object[] o1 = new Object[]{new Time(0l), new Time(0l), new Time(0l)};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "*";
        String expected = "";
        for (int i = 0; i < o1.length - 1; i++) {
            expected += o1[i].toString() + ", ";
        }
        expected += o1[o1.length - 1];
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testTimestamp() throws SQLException {
        PDataType type = PTimestampArray.INSTANCE;
        PDataType base = PTimestamp.INSTANCE;
        Object[] o1 = new Object[]{new Timestamp(0l), new Timestamp(0l), new Timestamp(0l)};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "*";
        String expected = "";
        for (int i = 0; i < o1.length - 1; i++) {
            expected += o1[i].toString() + ", ";
        }
        expected += o1[o1.length - 1];
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testVarchar1() throws SQLException {
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;
        Object[] o1 = new Object[]{"hello", null, "hello", null};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "*";
        String expected = "hello, *, hello, *";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testVarchar2() throws SQLException {
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;
        Object[] o1 = new Object[]{"hello", null, "hello", null, null};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "*";
        String expected = "hello, *, hello, *, *";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testVarchar3() throws SQLException {
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;
        Object[] o1 = new Object[]{"hello", null, "hello", null, null};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "";
        String expected = "hello, hello";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testVarchar4() throws SQLException {
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;
        Object[] o1 = new Object[]{null, "hello", "hello", null, null};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "";
        String expected = "hello, hello";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testVarchar5() throws SQLException {
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;
        Object[] o1 = new Object[]{"hello"};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "";
        String expected = "hello";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testVarchar6() throws SQLException {
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;
        Object[] o1 = new Object[]{null, null, null, null, "hello"};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "";
        String expected = "hello";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testVarchar7() throws SQLException {
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;
        Object[] o1 = new Object[]{null, null, null, null, "hello"};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ", ";
        String nullString = "*";
        String expected = "*, *, *, *, hello";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testDouble() throws SQLException {
        PDataType type = PDoubleArray.INSTANCE;
        PDataType base = PDouble.INSTANCE;
        Object[] o1 = new Object[]{23.4, 56.8, 2.4};
        PhoenixArray arr = new PhoenixArray.PrimitiveDoublePhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "23.4,56.8,2.4";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testTinyint() throws SQLException {
        PDataType type = PTinyintArray.INSTANCE;
        PDataType base = PTinyint.INSTANCE;
        Object[] o1 = new Object[]{(byte) 2, (byte) 4, (byte) 5};
        PhoenixArray arr = new PhoenixArray.PrimitiveBytePhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "2,4,5";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testSmallint() throws SQLException {
        PDataType type = PSmallintArray.INSTANCE;
        PDataType base = PSmallint.INSTANCE;
        Object[] o1 = new Object[]{(short) 6, (short) 7, (short) 8};
        PhoenixArray arr = new PhoenixArray.PrimitiveShortPhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "6,7,8";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testBoolean() throws SQLException {
        PDataType type = PBooleanArray.INSTANCE;
        PDataType base = PBoolean.INSTANCE;
        Object[] o1 = new Object[]{true, false, true};
        PhoenixArray arr = new PhoenixArray.PrimitiveBooleanPhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "true,false,true";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testLong() throws SQLException {
        PDataType type = PLongArray.INSTANCE;
        PDataType base = PLong.INSTANCE;
        Object[] o1 = new Object[]{(long) 23, (long) 34, (long) 45};
        PhoenixArray arr = new PhoenixArray.PrimitiveLongPhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "23,34,45";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testDecimal1() throws SQLException {
        PDataType type = PDecimalArray.INSTANCE;
        PDataType base = PDecimal.INSTANCE;
        Object[] o1 = new Object[]{BigDecimal.valueOf(23.45), BigDecimal.valueOf(2.345), BigDecimal.valueOf(234.5)};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "23.45,2.345,234.5";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testDecimal2() throws SQLException {
        PDataType type = PDecimalArray.INSTANCE;
        PDataType base = PDecimal.INSTANCE;
        Object[] o1 = new Object[]{BigDecimal.valueOf(23.45), BigDecimal.valueOf(2.345), BigDecimal.valueOf(234.5), null};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "23.45,2.345,234.5,*";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testDecimal3() throws SQLException {
        PDataType type = PDecimalArray.INSTANCE;
        PDataType base = PDecimal.INSTANCE;
        Object[] o1 = new Object[]{BigDecimal.valueOf(23.45), BigDecimal.valueOf(2.345), null, BigDecimal.valueOf(234.5)};
        PhoenixArray arr = new PhoenixArray(base, o1);
        String delimiter = ",";
        String nullString = "*";
        String expected = "23.45,2.345,*,234.5";
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC);
        test(arr, type, null, null, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC);
    }
}
