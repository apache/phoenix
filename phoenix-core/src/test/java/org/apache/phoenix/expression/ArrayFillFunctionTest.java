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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ArrayFillFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.*;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ArrayFillFunctionTest {

    private static void testExpression(LiteralExpression element, LiteralExpression length, PhoenixArray expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) element);
        expressions.add(length);

        Expression arrayFillFunction = new ArrayFillFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        arrayFillFunction.evaluate(null, ptr);
        PhoenixArray result = (PhoenixArray) arrayFillFunction.getDataType().toObject(ptr, arrayFillFunction.getSortOrder(), arrayFillFunction.getMaxLength(), arrayFillFunction.getScale());
        assertEquals(expected, result);
    }

    private static void test(Object element, Object length, PDataType elementDataType, Integer elementMaxLen, Integer elementScale, PDataType lengthDataType, Integer lengthMaxlen, Integer lengthScale, PhoenixArray expected, SortOrder elementSortOrder, SortOrder lengthSortOrder) throws SQLException {
        LiteralExpression elementLiteral, lengthLiteral;
        elementLiteral = LiteralExpression.newConstant(element, elementDataType, elementMaxLen, elementScale, elementSortOrder, Determinism.ALWAYS);
        lengthLiteral = LiteralExpression.newConstant(length, lengthDataType, lengthMaxlen, lengthScale, lengthSortOrder, Determinism.ALWAYS);
        testExpression(elementLiteral, lengthLiteral, expected);
    }

    @Test
    public void testForInt() throws SQLException {
        Object element = 5;
        Object length = 3;
        PDataType baseType = PInteger.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, new Object[]{5, 5, 5});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForBoolean() throws SQLException {
        Object element = false;
        Object length = 3;
        PDataType baseType = PBoolean.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveBooleanPhoenixArray(baseType, new Object[]{false, false, false});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForVarchar() throws SQLException {
        Object element = "foo";
        Object length = 3;
        PDataType baseType = PVarchar.INSTANCE;
        PhoenixArray e = new PhoenixArray(baseType, new Object[]{"foo", "foo", "foo"});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForChar() throws SQLException {
        Object element = "foo";
        Object length = 3;
        PDataType baseType = PChar.INSTANCE;
        PhoenixArray e = new PhoenixArray(baseType, new Object[]{"foo", "foo", "foo"});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForDouble() throws SQLException {
        Object element = 34.67;
        Object length = 3;
        PDataType baseType = PDouble.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveDoublePhoenixArray(baseType, new Object[]{34.67, 34.67, 34.67});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
    }

    @Test
    public void testForFloat() throws SQLException {
        Object element = 5.6;
        Object length = 3;
        PDataType baseType = PFloat.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveFloatPhoenixArray(baseType, new Object[]{(float) 5.6, (float) 5.6, (float) 5.6});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
    }

    @Test
    public void testForSmallint() throws SQLException {
        Object element = 5;
        Object length = 3;
        PDataType baseType = PSmallint.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveShortPhoenixArray(baseType, new Object[]{(short) 5, (short) 5, (short) 5});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForTinyint() throws SQLException {
        Object element = 6;
        Object length = 3;
        PDataType baseType = PTinyint.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveBytePhoenixArray(baseType, new Object[]{(byte) 6, (byte) 6, (byte) 6});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForLong() throws SQLException {
        Object element = 34567l;
        Object length = 3;
        PDataType baseType = PLong.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveLongPhoenixArray(baseType, new Object[]{34567l, 34567l, 34567l});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForDecimal() throws SQLException {
        Object element = BigDecimal.valueOf(345.67);
        Object length = 3;
        PDataType baseType = PDecimal.INSTANCE;
        PhoenixArray e = new PhoenixArray(baseType, new Object[]{BigDecimal.valueOf(345.67), BigDecimal.valueOf(345.67), BigDecimal.valueOf(345.67)});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForDate() throws SQLException {
        Object element = new Date(23);
        Object length = 3;
        PDataType baseType = PDate.INSTANCE;
        PhoenixArray e = new PhoenixArray(baseType, new Object[]{new Date(23), new Date(23), new Date(23)});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForTime() throws SQLException {
        Object element = new Time(23);
        Object length = 3;
        PDataType baseType = PTime.INSTANCE;
        PhoenixArray e = new PhoenixArray(baseType, new Object[]{new Time(23), new Time(23), new Time(23)});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForNulls1() throws SQLException {
        Object element = null;
        Object length = 3;
        PDataType baseType = PInteger.INSTANCE;
        PhoenixArray e = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, new Object[]{0, 0, 0});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }

    @Test
    public void testForNulls2() throws SQLException {
        Object element = null;
        Object length = 3;
        PDataType baseType = PVarchar.INSTANCE;
        PhoenixArray e = new PhoenixArray(baseType, new Object[]{null, null, null});
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.ASC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.ASC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.DESC);
        test(element, length, baseType, null, null, PInteger.INSTANCE, null, null, e, SortOrder.DESC, SortOrder.ASC);
    }
}
