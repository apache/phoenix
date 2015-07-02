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
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ExpFunction;
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
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link ExpFunction}
 */
public class ExpFunctionTest {
    private static final double ZERO = 1e-9;

    private static boolean twoDoubleEquals(double a, double b) {
        if (Double.isNaN(a) ^ Double.isNaN(b)) return false;
        if (Double.isNaN(a)) return true;
        if (Double.isInfinite(a) ^ Double.isInfinite(b)) return false;
        if (Double.isInfinite(a)) {
            if ((a > 0) ^ (b > 0)) return false;
            else return true;
        }
        if (Math.abs(a - b) <= ZERO) {
            return true;
        } else {
            return false;
        }
    }

    private static boolean testExpression(LiteralExpression literal, double expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) literal);
        Expression sqrtFunction = new ExpFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean ret = sqrtFunction.evaluate(null, ptr);
        if (ret) {
            Double result =
                    (Double) sqrtFunction.getDataType().toObject(ptr, sqrtFunction.getSortOrder());
            assertTrue(twoDoubleEquals(result.doubleValue(), expected));
        }
        return ret;
    }

    private static void test(Number value, PNumericType dataType, double expected)
            throws SQLException {
        LiteralExpression literal;
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.ASC);
        boolean ret1 = testExpression(literal, expected);
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.DESC);
        boolean ret2 = testExpression(literal, expected);
        assertEquals(ret1, ret2);
    }

    private static void testBatch(Number[] value, PNumericType dataType) throws SQLException {
        double[] expected = new double[value.length];
        for (int i = 0; i < expected.length; ++i) {
            expected[i] = Math.exp(value[i].doubleValue());
        }
        assertEquals(value.length, expected.length);
        for (int i = 0; i < value.length; ++i) {
            test(value[i], dataType, expected[i]);
        }
    }

    @Test
    public void testSqrtFunction() throws Exception {
        Random random = new Random();

        testBatch(
            new BigDecimal[] { BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.0),
                    BigDecimal.valueOf(-1.0), BigDecimal.valueOf(123.1234),
                    BigDecimal.valueOf(-123.1234), BigDecimal.valueOf(random.nextDouble()),
                    BigDecimal.valueOf(random.nextDouble()) }, PDecimal.INSTANCE);

        testBatch(new Float[] { 1.0f, 0.0f, -1.0f, 123.1234f, -123.1234f, random.nextFloat(),
                random.nextFloat() }, PFloat.INSTANCE);

        testBatch(new Float[] { 1.0f, 0.0f, -1.0f, 123.1234f, -123.1234f, random.nextFloat(),
                random.nextFloat() }, PFloat.INSTANCE);

        testBatch(new Float[] { 1.0f, 0.0f, 123.1234f, }, PUnsignedFloat.INSTANCE);

        testBatch(
            new Double[] { 1.0, 0.0, -1.0, 123.1234, -123.1234, random.nextDouble(),
                    random.nextDouble() }, PDouble.INSTANCE);

        testBatch(new Double[] { 1.0, 0.0, 123.1234, }, PUnsignedDouble.INSTANCE);

        testBatch(
            new Long[] { 1L, 0L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 123L, -123L,
                    random.nextLong(), random.nextLong() }, PLong.INSTANCE);

        testBatch(new Long[] { 1L, 0L, Long.MAX_VALUE, 123L }, PUnsignedLong.INSTANCE);

        testBatch(
            new Integer[] { 1, 0, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 123, -123,
                    random.nextInt(), random.nextInt() }, PInteger.INSTANCE);

        testBatch(new Integer[] { 1, 0, Integer.MAX_VALUE, 123 }, PUnsignedInt.INSTANCE);

        testBatch(new Short[] { (short) 1, (short) 0, (short) -1, Short.MAX_VALUE, Short.MIN_VALUE,
                (short) 123, (short) -123 }, PSmallint.INSTANCE);

        testBatch(new Short[] { (short) 1, (short) 0, Short.MAX_VALUE, (short) 123 },
            PSmallint.INSTANCE);

        testBatch(new Byte[] { (byte) 1, (byte) 0, (byte) -1, Byte.MAX_VALUE, Byte.MIN_VALUE,
                (byte) 123, (byte) -123 }, PTinyint.INSTANCE);

        testBatch(new Byte[] { (byte) 1, (byte) 0, Byte.MAX_VALUE, (byte) 123 }, PTinyint.INSTANCE);
    }
}
