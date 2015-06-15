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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.AbsFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PNumericType;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link AbsFunction}
 */
public class AbsFunctionTest {

    private static void testExpression(LiteralExpression literal, Number expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) literal);
        Expression absFunction = new AbsFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        absFunction.evaluate(null, ptr);
        Number result =
                (Number) absFunction.getDataType().toObject(ptr, absFunction.getSortOrder());
        assertTrue(result.getClass().equals(expected.getClass()));
        if (result instanceof BigDecimal) {
            assertTrue(((BigDecimal) result).compareTo((BigDecimal) expected) == 0);
        } else {
            assertTrue(result.equals(expected));
        }
    }

    private static void test(Number value, PNumericType dataType, Number expected)
            throws SQLException {
        LiteralExpression literal;
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.ASC);
        testExpression(literal, expected);
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.DESC);
        testExpression(literal, expected);
    }

    private static void
            testBatch(Number[] value, PNumericType dataType, ArrayList<Number> expected)
                    throws SQLException {
        assertEquals(value.length, expected.size());
        for (int i = 0; i < value.length; ++i) {
            test(value[i], dataType, expected.get(i));
        }
    }

    @Test
    public void testAbsFunction() throws Exception {
        Random random = new Random();
        Number[] value;
        ArrayList<Number> expected = new ArrayList<Number>();
        value = new BigDecimal[] { BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.0),
                        BigDecimal.valueOf(-1.0), BigDecimal.valueOf(123.1234),
                        BigDecimal.valueOf(-123.1234) };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(((BigDecimal) value[i]).abs());
        testBatch(value, PDecimal.INSTANCE, expected);

        value = new Float[] { 1.0f, 0.0f, -1.0f, 123.1234f, -123.1234f, Float.MIN_VALUE,
                        Float.MAX_VALUE, -Float.MIN_VALUE, -Float.MAX_VALUE, random.nextFloat(),
                        random.nextFloat(), random.nextFloat() };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Float) value[i]));
        testBatch(value, PFloat.INSTANCE, expected);

        value = new Float[] { 1.0f, 0.0f, 123.1234f, Float.MIN_VALUE, Float.MAX_VALUE, };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Float) value[i]));
        testBatch(value, PUnsignedFloat.INSTANCE, expected);

        value = new Double[] { 1.0, 0.0, -1.0, 123.1234, -123.1234, Double.MIN_VALUE,
                        Double.MAX_VALUE, -Double.MIN_VALUE, -Double.MAX_VALUE,
                        random.nextDouble(), random.nextDouble(), random.nextDouble() };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Double) value[i]));
        testBatch(value, PDouble.INSTANCE, expected);

        value = new Double[] { 1.0, 0.0, 123.1234, Double.MIN_VALUE, Double.MAX_VALUE, };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Double) value[i]));
        testBatch(value, PUnsignedDouble.INSTANCE, expected);

        value = new Long[] { 1L, 0L, -1L, 123L, -123L, Long.MIN_VALUE + 1, Long.MAX_VALUE,
                        random.nextLong(), random.nextLong(), random.nextLong(), };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Long) value[i]));
        testBatch(value, PLong.INSTANCE, expected);

        value = new Long[] { 1L, 0L, 123L, Long.MAX_VALUE };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Long) value[i]));
        testBatch(value, PUnsignedLong.INSTANCE, expected);

        value = new Integer[] { 1, 0, -1, 123, -123, Integer.MIN_VALUE + 1, Integer.MAX_VALUE,
                        random.nextInt(), random.nextInt(), random.nextInt(), };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Integer) value[i]));
        testBatch(value, PInteger.INSTANCE, expected);

        value = new Integer[] { 1, 0, 123, Integer.MAX_VALUE };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add(Math.abs((Integer) value[i]));
        testBatch(value, PUnsignedInt.INSTANCE, expected);

        value = new Short[] { 1, 0, -1, 123, -123, Short.MIN_VALUE + 1, Short.MAX_VALUE };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add((short) Math.abs((Short) value[i]));
        testBatch(value, PSmallint.INSTANCE, expected);

        value = new Short[] { 1, 0, 123, Short.MAX_VALUE };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add((short) Math.abs((Short) value[i]));
        testBatch(value, PUnsignedSmallint.INSTANCE, expected);

        value = new Byte[] { 1, 0, -1, 123, -123, Byte.MIN_VALUE + 1, Byte.MAX_VALUE };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add((byte) Math.abs((Byte) value[i]));
        testBatch(value, PTinyint.INSTANCE, expected);

        value = new Byte[] { 1, 0, 123, Byte.MAX_VALUE };
        expected.clear();
        for (int i = 0; i < value.length; ++i)
            expected.add((byte) Math.abs((Byte) value[i]));
        testBatch(value, PUnsignedTinyint.INSTANCE, expected);
    }
}
