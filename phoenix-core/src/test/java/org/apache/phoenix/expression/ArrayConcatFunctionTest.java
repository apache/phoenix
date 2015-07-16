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
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.expression.function.ArrayConcatFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.*;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ArrayConcatFunctionTest {

    private static void testExpression(LiteralExpression array1, LiteralExpression array2, PhoenixArray expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) array1);
        expressions.add(array2);

        Expression arrayConcatFunction = new ArrayConcatFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayConcatFunction.evaluate(null, ptr);
        PhoenixArray result = (PhoenixArray) arrayConcatFunction.getDataType().toObject(ptr, expressions.get(0).getSortOrder(), array1.getMaxLength(), array1.getScale());
        assertEquals(expected, result);
    }

    private static void test(PhoenixArray array1, PhoenixArray array2, PDataType array1DataType, Integer arr1MaxLen, Integer arr1Scale, PDataType array2DataType, Integer arr2MaxLen, Integer arr2Scale, PhoenixArray expected, SortOrder array1SortOrder, SortOrder array2SortOrder) throws SQLException {
        LiteralExpression array1Literal, array2Literal;
        array1Literal = LiteralExpression.newConstant(array1, array1DataType, arr1MaxLen, arr1Scale, array1SortOrder, Determinism.ALWAYS);
        array2Literal = LiteralExpression.newConstant(array2, array2DataType, arr2MaxLen, arr2Scale, array2SortOrder, Determinism.ALWAYS);
        testExpression(array1Literal, array2Literal, expected);
    }

    @Test
    public void testChar1() throws SQLException {
        Object[] o1 = new Object[]{"aa", "bb"};
        Object[] o2 = new Object[]{"c", "d"};
        Object[] e = new Object[]{"aa", "bb", "c", "d"};
        PDataType type = PCharArray.INSTANCE;
        PDataType base = PChar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.DESC, SortOrder.ASC);

    }

    @Test
    public void testChar2() throws SQLException {
        Object[] o1 = new Object[]{"aa", "bb"};
        Object[] o2 = new Object[]{"cc", "dc", "ee"};
        Object[] e = new Object[]{"aa", "bb", "cc", "dc", "ee"};
        PDataType type = PCharArray.INSTANCE;
        PDataType base = PChar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, 2, null, type, 2, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, 2, null, type, 2, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, 2, null, type, 2, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, 2, null, type, 2, null, expected, SortOrder.DESC, SortOrder.ASC);

    }

    @Test(expected = DataExceedsCapacityException.class)
    public void testChar3() throws SQLException {
        Object[] o1 = new Object[]{"c", "d"};
        Object[] o2 = new Object[]{"aa", "bb"};
        Object[] e = new Object[]{"aa", "bb", "c", "d"};
        PDataType type = PCharArray.INSTANCE;
        PDataType base = PChar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, 2, null, type, 1, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testInt1() throws SQLException {
        Object[] o1 = new Object[]{1, 2};
        Object[] o2 = new Object[]{5, 6, 7};
        Object[] e = new Object[]{1, 2, 5, 6, 7};
        PDataType type = PIntegerArray.INSTANCE;
        PDataType base = PInteger.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray.PrimitiveIntPhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveIntPhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray.PrimitiveIntPhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testFloat1() throws SQLException {
        Object[] o1 = new Object[]{(float) 1.2, (float) 2};
        Object[] o2 = new Object[]{(float) 5, (float) 6, (float) 7};
        Object[] e = new Object[]{(float) 1.2, (float) 2, (float) 5, (float) 6, (float) 7};
        PDataType type = PFloatArray.INSTANCE;
        PDataType base = PFloat.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray.PrimitiveFloatPhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveFloatPhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray.PrimitiveFloatPhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testDouble1() throws SQLException {
        Object[] o1 = new Object[]{(double) 1.2, (double) 2};
        Object[] o2 = new Object[]{(double) 5.2, (double) 6, (double) 7};
        Object[] e = new Object[]{(double) 1.2, (double) 2, (double) 5.2, (double) 6, (double) 7};
        PDataType type = PDoubleArray.INSTANCE;
        PDataType base = PDouble.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray.PrimitiveDoublePhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveDoublePhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray.PrimitiveDoublePhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testLong1() throws SQLException {
        Object[] o1 = new Object[]{(long) 1, (long) 2};
        Object[] o2 = new Object[]{(long) 5, (long) 6, (long) 7};
        Object[] e = new Object[]{(long) 1, (long) 2, (long) 5, (long) 6, (long) 7};
        PDataType type = PLongArray.INSTANCE;
        PDataType base = PLong.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray.PrimitiveLongPhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveLongPhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray.PrimitiveLongPhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testShort1() throws SQLException {
        Object[] o1 = new Object[]{(short) 1, (short) 2};
        Object[] o2 = new Object[]{(short) 5, (short) 6, (short) 7};
        Object[] e = new Object[]{(short) 1, (short) 2, (short) 5, (short) 6, (short) 7};
        PDataType type = PSmallintArray.INSTANCE;
        PDataType base = PSmallint.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray.PrimitiveShortPhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveShortPhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray.PrimitiveShortPhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testBoolean1() throws SQLException {
        Object[] o1 = new Object[]{true, true};
        Object[] o2 = new Object[]{false, false, false};
        Object[] e = new Object[]{true, true, false, false, false};
        PDataType type = PBooleanArray.INSTANCE;
        PDataType base = PBoolean.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray.PrimitiveBooleanPhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveBooleanPhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray.PrimitiveBooleanPhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
    }

    @Test
    public void testTinyInt1() throws SQLException {
        Object[] o1 = new Object[]{(byte) 2, (byte) 2};
        Object[] o2 = new Object[]{(byte) 5, (byte) 6, (byte) 7};
        Object[] e = new Object[]{(byte) 2, (byte) 2, (byte) 5, (byte) 6, (byte) 7};
        PDataType type = PTinyintArray.INSTANCE;
        PDataType base = PTinyint.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray.PrimitiveBytePhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveBytePhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray.PrimitiveBytePhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testDate1() throws SQLException {
        Object[] o1 = new Object[]{new Date(0l), new Date(0l)};
        Object[] o2 = new Object[]{new Date(0l), new Date(0l), new Date(0l)};
        Object[] e = new Object[]{new Date(0l), new Date(0l), new Date(0l), new Date(0l), new Date(0l)};
        PDataType type = PDateArray.INSTANCE;
        PDataType base = PDate.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testDecimal1() throws SQLException {
        Object[] o1 = new Object[]{BigDecimal.valueOf(32.4), BigDecimal.valueOf(34)};
        Object[] o2 = new Object[]{BigDecimal.valueOf(32.4), BigDecimal.valueOf(34)};
        Object[] e = new Object[]{BigDecimal.valueOf(32.4), BigDecimal.valueOf(34), BigDecimal.valueOf(32.4), BigDecimal.valueOf(34)};
        PDataType type = PDecimalArray.INSTANCE;
        PDataType base = PDecimal.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar1() throws SQLException {
        Object[] o1 = new Object[]{"a", "b"};
        Object[] o2 = new Object[]{"c", "d"};
        Object[] e = new Object[]{"a", "b", "c", "d"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar2() throws SQLException {
        Object[] o1 = new Object[]{"a"};
        Object[] o2 = new Object[]{"c", "d"};
        Object[] e = new Object[]{"a", "c", "d"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar3() throws SQLException {
        Object[] o1 = new Object[]{"a", "b"};
        Object[] o2 = new Object[]{"c"};
        Object[] e = new Object[]{"a", "b", "c"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar4() throws SQLException {
        Object[] o1 = new Object[]{"a"};
        Object[] o2 = new Object[]{null, "c"};
        Object[] e = new Object[]{"a", null, "c"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar5() throws SQLException {
        Object[] o1 = new Object[]{"a", null , null};
        Object[] o2 = new Object[]{null, null, "c"};
        Object[] e = new Object[]{"a", null, null, null, null, "c"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar6() throws SQLException {
        Object[] o1 = new Object[]{"a", "b"};
        Object[] e = new Object[]{"a", "b"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = null;
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar7() throws SQLException {
        Object[] o2 = new Object[]{"a", "b"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = null;
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = null;
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testVarchar8() throws SQLException {
        Object[] o1 = new Object[]{"a", null, null, "b"};
        Object[] o2 = new Object[]{"c", null, "d", null, "e"};
        Object[] e = new Object[]{"a", null, null, "b", "c", null, "d", null, "e"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test(expected = TypeMismatchException.class)
    public void testVarchar9() throws SQLException {
        Object[] o1 = new Object[]{"a", "b"};
        Object[] o2 = new Object[]{1, 2};

        PhoenixArray arr1 = new PhoenixArray(PVarchar.INSTANCE, o1);
        PhoenixArray arr2 = new PhoenixArray.PrimitiveIntPhoenixArray(PInteger.INSTANCE, o2);
        test(arr1, arr2, PVarcharArray.INSTANCE, null, null, PIntegerArray.INSTANCE, null, null, null, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, PVarcharArray.INSTANCE, null, null, PIntegerArray.INSTANCE, null, null, null, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, PVarcharArray.INSTANCE, null, null, PIntegerArray.INSTANCE, null, null, null, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, PVarcharArray.INSTANCE, null, null, PIntegerArray.INSTANCE, null, null, null, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWithIntOffsetArray() throws SQLException {
        Object[] o1 = new Object[Short.MAX_VALUE + 7];
        Object[] o2 = new Object[]{"b", "b"};
        Object[] e = new Object[Short.MAX_VALUE + 9];
        for (int i = 0; i < o1.length; i++) {
            o1[i] = "a";
            e[i] = "a";
        }
        e[Short.MAX_VALUE + 7] = "b";
        e[Short.MAX_VALUE + 8] = "b";
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWithShortToIntOffsetArray() throws SQLException {
        Object[] o1 = new Object[Short.MAX_VALUE + 1];
        Object[] o2 = new Object[]{"b", "b"};
        Object[] e = new Object[Short.MAX_VALUE + 3];
        for (int i = 0; i < o1.length; i++) {
            o1[i] = "a";
            e[i] = "a";
        }
        e[Short.MAX_VALUE + 2] = "b";
        e[Short.MAX_VALUE + 1] = "b";
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWithShortToIntOffsetArray2() throws SQLException {
        Object[] o1 = new Object[Short.MAX_VALUE + 1];
        Object[] o2 = new Object[]{null, "b"};
        Object[] e = new Object[Short.MAX_VALUE + 3];
        for (int i = 0; i < o1.length; i++) {
            o1[i] = "a";
            e[i] = "a";
        }
        e[Short.MAX_VALUE + 1] = null;
        e[Short.MAX_VALUE + 2] = "b";
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWith10NullsAnd246Nulls()throws SQLException{
        Object[] o1 = new Object[11];
        Object[] o2 = new Object[247];
        Object[] e = new Object[258];
        o1[0] = "a";
        o2[o2.length - 1] = "a";
        e[e.length - 1] = "a";
        e[0] = "a";

        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWith0NullsAnd256Nulls()throws SQLException{
        Object[] o1 = new Object[1];
        Object[] o2 = new Object[257];
        Object[] e = new Object[258];
        o1[0] = "a";
        o2[o2.length - 1] = "a";
        e[e.length - 1] = "a";
        e[0] = "a";

        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWith256NullsAnd0Nulls()throws SQLException{
        Object[] o1 = new Object[257];
        Object[] o2 = new Object[1];
        Object[] e = new Object[258];
        o1[0] = "a";
        o2[o2.length - 1] = "a";
        e[e.length - 1] = "a";
        e[0] = "a";

        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWith255NullsAnd0Nulls()throws SQLException{
        Object[] o1 = new Object[256];
        Object[] o2 = new Object[1];
        Object[] e = new Object[257];
        o1[0] = "a";
        o2[o2.length - 1] = "a";
        e[e.length - 1] = "a";
        e[0] = "a";

        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWith0NullsAnd255Nulls()throws SQLException{
        Object[] o1 = new Object[1];
        Object[] o2 = new Object[256];
        Object[] e = new Object[257];
        o1[0] = "a";
        o2[o2.length - 1] = "a";
        e[e.length - 1] = "a";
        e[0] = "a";

        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testWith10NullsAnd245Nulls()throws SQLException{
        Object[] o1 = new Object[11];
        Object[] o2 = new Object[246];
        Object[] e = new Object[257];
        o1[0] = "a";
        o2[o2.length - 1] = "a";
        e[e.length - 1] = "a";
        e[0] = "a";

        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        PhoenixArray expected = new PhoenixArray(base, e);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.ASC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.ASC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.DESC);
        test(arr1, arr2, type, null, null, type, null, null, expected, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForCorrectSeparatorBytes1() throws Exception {
        Object[] o1 = new Object[]{"a", "b"};
        Object[] o2 = new Object[]{"c", "d", "e"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        LiteralExpression array1Literal, array2Literal;
        array1Literal = LiteralExpression.newConstant(arr1, type, null, null, SortOrder.ASC, Determinism.ALWAYS);
        array2Literal = LiteralExpression.newConstant(arr2, type, null, null, SortOrder.ASC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) array1Literal);
        expressions.add(array2Literal);

        Expression arrayConcatFunction = new ArrayConcatFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayConcatFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{97, 0, 98, 0, 99, 0, 100, 0, 101, 0, 0, 0, -128, 1, -128, 3, -128, 5, -128, 7, -128, 9, 0, 0, 0, 12, 0, 0, 0, 5, 1};
        assertArrayEquals(expected, ptr.get());
    }

    @Test
    public void testForCorrectSeparatorBytes2() throws Exception {
        Object[] o1 = new Object[]{"a", "b"};
        Object[] o2 = new Object[]{"c", "d", "e"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        LiteralExpression array1Literal, array2Literal;
        array1Literal = LiteralExpression.newConstant(arr1, type, null, null, SortOrder.ASC, Determinism.ALWAYS);
        array2Literal = LiteralExpression.newConstant(arr2, type, null, null, SortOrder.DESC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) array1Literal);
        expressions.add(array2Literal);

        Expression arrayConcatFunction = new ArrayConcatFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayConcatFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{97, 0, 98, 0, 99, 0, 100, 0, 101, 0, 0, 0, -128, 1, -128, 3, -128, 5, -128, 7, -128, 9, 0, 0, 0, 12, 0, 0, 0, 5, 1};
        assertArrayEquals(expected, ptr.get());
    }

    @Test
    public void testForCorrectSeparatorBytes3() throws Exception {
        Object[] o1 = new Object[]{"a", "b"};
        Object[] o2 = new Object[]{"c", "d", "e"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        LiteralExpression array1Literal, array2Literal;
        array1Literal = LiteralExpression.newConstant(arr1, type, null, null, SortOrder.DESC, Determinism.ALWAYS);
        array2Literal = LiteralExpression.newConstant(arr2, type, null, null, SortOrder.DESC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) array1Literal);
        expressions.add(array2Literal);

        Expression arrayConcatFunction = new ArrayConcatFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayConcatFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{-98, -1, -99, -1, -100, -1, -101, -1, -102, -1, -1, -1, -128, 1, -128, 3, -128, 5, -128, 7, -128, 9, 0, 0, 0, 12, 0, 0, 0, 5, 1};
        assertArrayEquals(expected, ptr.get());
    }

    @Test
    public void testForCorrectSeparatorBytes4() throws Exception {
        Object[] o1 = new Object[]{"a", "b", null};
        Object[] o2 = new Object[]{null, "c", "d", "e"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        LiteralExpression array1Literal, array2Literal;
        array1Literal = LiteralExpression.newConstant(arr1, type, null, null, SortOrder.ASC, Determinism.ALWAYS);
        array2Literal = LiteralExpression.newConstant(arr2, type, null, null, SortOrder.DESC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) array1Literal);
        expressions.add(array2Literal);

        Expression arrayConcatFunction = new ArrayConcatFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayConcatFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{97, 0, 98, 0, 0, -2, 99, 0, 100, 0, 101, 0, 0, 0, -128, 1, -128, 3, -128, 5, -128, 5, -128, 7, -128, 9, -128, 11, 0, 0, 0, 14, 0, 0, 0, 7, 1};
        assertArrayEquals(expected, ptr.get());
    }

    @Test
    public void testForCorrectSeparatorBytes5() throws Exception {
        Object[] o1 = new Object[]{"a", "b", null, null};
        Object[] o2 = new Object[]{null, "c", "d", "e"};
        PDataType type = PVarcharArray.INSTANCE;
        PDataType base = PVarchar.INSTANCE;

        PhoenixArray arr1 = new PhoenixArray(base, o1);
        PhoenixArray arr2 = new PhoenixArray(base, o2);
        LiteralExpression array1Literal, array2Literal;
        array1Literal = LiteralExpression.newConstant(arr1, type, null, null, SortOrder.DESC, Determinism.ALWAYS);
        array2Literal = LiteralExpression.newConstant(arr2, type, null, null, SortOrder.DESC, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList((Expression) array1Literal);
        expressions.add(array2Literal);

        Expression arrayConcatFunction = new ArrayConcatFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayConcatFunction.evaluate(null, ptr);
        byte[] expected = new byte[]{-98, -1, -99, -1, 0, -3, -100, -1, -101, -1, -102, -1, -1, -1, -128, 1, -128, 3, -128, 5, -128, 5, -128, 5, -128, 7, -128, 9, -128, 11, 0, 0, 0, 14, 0, 0, 0, 8, 1};
        assertArrayEquals(expected, ptr.get());
    }
}
