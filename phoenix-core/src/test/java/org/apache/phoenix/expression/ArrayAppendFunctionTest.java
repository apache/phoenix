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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ArrayAppendFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.*;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ArrayAppendFunctionTest {

    private static void testExpression(LiteralExpression array, LiteralExpression element, PhoenixArray expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) array);
        expressions.add(element);

        Expression arrayAppendFunction = new ArrayAppendFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayAppendFunction.evaluate(null, ptr);
        PhoenixArray result = (PhoenixArray) arrayAppendFunction.getDataType().toObject(ptr, expressions.get(0).getSortOrder(), array.getMaxLength(), array.getScale());
        assertTrue(result.equals(expected));
    }

    private static void test(PhoenixArray array, Object element, PDataType arrayDataType, Integer arrMaxLen, Integer arrScale, PDataType elementDataType, Integer elemMaxLen, Integer elemScale, PhoenixArray expected, SortOrder arraySortOrder, SortOrder elementSortOrder) throws SQLException {
        LiteralExpression arrayLiteral, elementLiteral;
        arrayLiteral = LiteralExpression.newConstant(array, arrayDataType, arrMaxLen, arrScale, arraySortOrder, Determinism.ALWAYS);
        elementLiteral = LiteralExpression.newConstant(element, elementDataType, elemMaxLen, elemScale, elementSortOrder, Determinism.ALWAYS);
        testExpression(arrayLiteral, elementLiteral, expected);
    }

    @Test
    public void testArrayAppendFunction1() throws Exception {
        Object[] o = new Object[]{1, 2, -3, 4};
        Object[] o2 = new Object[]{1, 2, -3, 4, 5};
        Object element = 5;
        PDataType baseType = PInteger.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction2() throws Exception {
        Object[] o = new Object[]{"1", "2", "3", "4"};
        Object[] o2 = new Object[]{"1", "2", "3", "4", "56"};
        Object element = "56";
        PDataType baseType = PVarchar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction3() throws Exception {
        //offset array short to int transition
        Object[] o = new Object[Short.MAX_VALUE + 1];
        for (int i = 0; i < o.length; i++) {
            o[i] = "a";
        }
        Object[] o2 = new Object[Short.MAX_VALUE + 2];
        for (int i = 0; i < o2.length - 1; i++) {
            o2[i] = "a";
        }
        Object element = "b";
        o2[o2.length - 1] = element;
        PDataType baseType = PVarchar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction4() throws Exception {
        //offset array int
        Object[] o = new Object[Short.MAX_VALUE + 7];
        for (int i = 0; i < o.length; i++) {
            o[i] = "a";
        }
        Object[] o2 = new Object[Short.MAX_VALUE + 8];
        for (int i = 0; i < o2.length - 1; i++) {
            o2[i] = "a";
        }
        Object element = "b";
        o2[o2.length - 1] = element;
        PDataType baseType = PVarchar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunctionBoolean() throws Exception {
        Boolean[] o = new Boolean[] { true, false, false, true };
        Boolean[] o2 = new Boolean[] { true, false, false, true, false };
        Boolean element = false;
        PDataType baseType = PBoolean.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveBooleanPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveBooleanPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE),
            null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction6() throws Exception {
        Object[] o = new Object[]{new Float(2.3), new Float(7.9), new Float(-9.6), new Float(2.3)};
        Object[] o2 = new Object[]{new Float(2.3), new Float(7.9), new Float(-9.6), new Float(2.3), new Float(8.9)};
        Object element = 8.9;
        PDataType baseType = PFloat.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveFloatPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveFloatPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction7() throws Exception {
        Object[] o = new Object[]{4.78, 9.54, 2.34, -9.675, Double.MAX_VALUE};
        Object[] o2 = new Object[]{4.78, 9.54, 2.34, -9.675, Double.MAX_VALUE, 12.67};
        Object element = 12.67;
        PDataType baseType = PDouble.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveDoublePhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveDoublePhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction8() throws Exception {
        Object[] o = new Object[]{123l, 677l, 98789l, -78989l, 66787l};
        Object[] o2 = new Object[]{123l, 677l, 98789l, -78989l, 66787l, 543l};
        Object element = 543l;
        PDataType baseType = PLong.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveLongPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveLongPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction9() throws Exception {
        Object[] o = new Object[]{(short) 34, (short) -23, (short) -89, (short) 999, (short) 34};
        Object[] o2 = new Object[]{(short) 34, (short) -23, (short) -89, (short) 999, (short) 34, (short) 7};
        Object element = (short) 7;
        PDataType baseType = PSmallint.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveShortPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveShortPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction10() throws Exception {
        Object[] o = new Object[]{(byte) 4, (byte) 8, (byte) 9};
        Object[] o2 = new Object[]{(byte) 4, (byte) 8, (byte) 9, (byte) 6};
        Object element = (byte) 6;
        PDataType baseType = PTinyint.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveBytePhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveBytePhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction11() throws Exception {
        Object[] o = new Object[]{BigDecimal.valueOf(2345), BigDecimal.valueOf(-23.45), BigDecimal.valueOf(785)};
        Object[] o2 = new Object[]{BigDecimal.valueOf(2345), BigDecimal.valueOf(-23.45), BigDecimal.valueOf(785), BigDecimal.valueOf(-19)};
        Object element = BigDecimal.valueOf(-19);
        PDataType baseType = PDecimal.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction12() throws Exception {
        Calendar calendar = Calendar.getInstance();
        java.util.Date currentDate = calendar.getTime();
        java.sql.Date date = new java.sql.Date(currentDate.getTime());

        Object[] o = new Object[]{date, date, date};
        Object[] o2 = new Object[]{date, date, date, date};
        PDataType baseType = PDate.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, date, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction13() throws Exception {
        Calendar calendar = Calendar.getInstance();
        java.util.Date currentDate = calendar.getTime();
        java.sql.Time time = new java.sql.Time(currentDate.getTime());

        Object[] o = new Object[]{time, time, time};
        Object[] o2 = new Object[]{time, time, time, time};
        PDataType baseType = PTime.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, time, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction14() throws Exception {
        Calendar calendar = Calendar.getInstance();
        java.util.Date currentDate = calendar.getTime();
        java.sql.Timestamp timestamp = new java.sql.Timestamp(currentDate.getTime());

        Object[] o = new Object[]{timestamp, timestamp, timestamp};
        Object[] o2 = new Object[]{timestamp, timestamp, timestamp, timestamp};
        PDataType baseType = PTimestamp.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, timestamp, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction15() throws Exception {
        Object[] o = new Object[]{1, 2, -3, 4};
        Object[] o2 = new Object[]{1, 2, -3, 4, 5};
        Object element = 5;
        PDataType baseType = PInteger.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction16() throws Exception {
        Object[] o = new Object[]{1, 2, -3, 4};
        Object[] o2 = new Object[]{1, 2, -3, 4, 5};
        Object element = 5;
        PDataType baseType = PInteger.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.DESC, SortOrder.DESC);
    }

    @Test
    public void testArrayAppendFunction17() throws Exception {
        Object[] o = new Object[]{1, 2, -3, 4};
        Object[] o2 = new Object[]{1, 2, -3, 4, 5};
        Object element = 5;
        PDataType baseType = PInteger.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.DESC);
    }

    @Test
    public void testArrayAppendFunction18() throws Exception {
        Object[] o = new Object[]{"1   ", "2   ", "3   ", "4   "};
        Object[] o2 = new Object[]{"1", "2", "3", "4", "5"};
        Object element = "5";
        PDataType baseType = PChar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), 4, null, baseType, 1, null, expected, SortOrder.ASC, SortOrder.DESC);
    }

    @Test
    public void testArrayAppendFunction19() throws Exception {
        Object[] o = new Object[]{"1   ", "2   ", "3   ", "4   "};
        Object[] o2 = new Object[]{"1", "2", "3", "4", "5"};
        Object element = "5";
        PDataType baseType = PChar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), 4, null, baseType, 1, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testArrayAppendFunction20() throws Exception {
        Object[] o = new Object[]{"1   ", "2   ", "3   ", "4   "};
        Object[] o2 = new Object[]{"1", "2", "3", "4", "5"};
        Object element = "5";
        PDataType baseType = PChar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), 4, null, baseType, 1, null, expected, SortOrder.DESC, SortOrder.DESC);
    }

    @Test
    public void testArrayAppendFunction21() throws Exception {
        Object[] o = new Object[]{4.78, 9.54, 2.34, -9.675, Double.MAX_VALUE};
        Object[] o2 = new Object[]{4.78, 9.54, 2.34, -9.675, Double.MAX_VALUE, 12.67};
        Object element = 12.67;
        PDataType baseType = PDouble.INSTANCE;

        PhoenixArray arr = new PhoenixArray.PrimitiveDoublePhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray.PrimitiveDoublePhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType, null, null, expected, SortOrder.ASC, SortOrder.DESC);
    }

    @Test
    public void testArrayAppendFunction22() throws Exception {
        Object[] o = new Object[]{"1   ", "2   ", "3   ", "4   "};
        Object[] o2 = new Object[]{"1", "2", "3", "4"};
        Object element = null;
        PDataType baseType = PChar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        PhoenixArray expected = new PhoenixArray(baseType, o2);
        test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), 4, null, baseType, 1, null, expected, SortOrder.ASC, SortOrder.DESC);
    }

    @Test
    public void testForCorrectSeparatorBytes1() throws Exception {
        Object[] o = new Object[]{"a", "b", "c"};
        Object element = "d";
        PDataType baseType = PVarchar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        LiteralExpression arrayLiteral, elementLiteral;
        arrayLiteral = LiteralExpression.newConstant(arr, PVarcharArray.INSTANCE, null, null, SortOrder.ASC, Determinism.ALWAYS);
        elementLiteral = LiteralExpression.newConstant(element, baseType, null, null, SortOrder.ASC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) arrayLiteral);
        expressions.add(elementLiteral);

        Expression arrayAppendFunction = new ArrayAppendFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayAppendFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{97, 0, 98, 0, 99, 0, 100, 0, 0, 0, -128, 1, -128, 3, -128, 5, -128, 7, 0, 0, 0, 10, 0, 0, 0, 4, 1};
        assertArrayEquals(expected, ptr.get());
    }

    @Test
    public void testForCorrectSeparatorBytes2() throws Exception {
        Object[] o = new Object[]{"a", "b", "c"};
        Object element = "d";
        PDataType baseType = PVarchar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        LiteralExpression arrayLiteral, elementLiteral;
        arrayLiteral = LiteralExpression.newConstant(arr, PVarcharArray.INSTANCE, null, null, SortOrder.DESC, Determinism.ALWAYS);
        elementLiteral = LiteralExpression.newConstant(element, baseType, null, null, SortOrder.ASC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) arrayLiteral);
        expressions.add(elementLiteral);

        Expression arrayAppendFunction = new ArrayAppendFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayAppendFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{-98, -1, -99, -1, -100, -1, -101, -1, -1, -1, -128, 1, -128, 3, -128, 5, -128, 7, 0, 0, 0, 10, 0, 0, 0, 4, 1};
        assertArrayEquals(expected, ptr.get());
    }

    @Test
    public void testForCorrectSeparatorBytes3() throws Exception {
        Object[] o = new Object[]{"a", null, null, "c"};
        Object element = "d";
        PDataType baseType = PVarchar.INSTANCE;

        PhoenixArray arr = new PhoenixArray(baseType, o);
        LiteralExpression arrayLiteral, elementLiteral;
        arrayLiteral = LiteralExpression.newConstant(arr, PVarcharArray.INSTANCE, null, null, SortOrder.DESC, Determinism.ALWAYS);
        elementLiteral = LiteralExpression.newConstant(element, baseType, null, null, SortOrder.ASC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) arrayLiteral);
        expressions.add(elementLiteral);

        Expression arrayAppendFunction = new ArrayAppendFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayAppendFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{-98, -1, 0, -2, -100, -1, -101, -1, -1, -1, -128, 1, -128, 3, -128, 3, -128, 5, -128, 7, 0, 0, 0, 10, 0, 0, 0, 5, 1};
        assertArrayEquals(expected, ptr.get());
    }
}
