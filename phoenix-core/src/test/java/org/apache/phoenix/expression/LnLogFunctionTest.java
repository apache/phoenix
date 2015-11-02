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
import org.apache.phoenix.expression.function.LnFunction;
import org.apache.phoenix.expression.function.LogFunction;
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
 * Unit tests for {@link LnFunction} and {@link LogFunction}
 */
public class LnLogFunctionTest {
    private static final double ZERO = 1e-9;
    private static final Expression THREE = LiteralExpression.newConstant(3);
    private static final Expression DEFAULT_VALUE = LiteralExpression.newConstant(10.0);

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

    private static boolean testExpression(LiteralExpression literal, LiteralExpression literal2,
            LiteralExpression literal3, double exptForLn, double exptForLog10, double exptForLog3)
            throws SQLException {
        List<Expression> expressionsLn = Lists.newArrayList((Expression) literal);
        List<Expression> expressionsLog10 = Lists.newArrayList(literal2, DEFAULT_VALUE);
        List<Expression> expressionsLog3 = Lists.newArrayList(literal3, THREE);

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();

        Expression lnFunction = new LnFunction(expressionsLn);
        boolean retLn = lnFunction.evaluate(null, ptr);
        if (retLn) {
            Double result =
                    (Double) lnFunction.getDataType().toObject(ptr, lnFunction.getSortOrder());
            assertTrue(twoDoubleEquals(result.doubleValue(), exptForLn));
        }

        Expression log10Function = new LogFunction(expressionsLog10);
        boolean retLog10 = log10Function.evaluate(null, ptr);
        if (retLog10) {
            Double result =
                    (Double) log10Function.getDataType()
                            .toObject(ptr, log10Function.getSortOrder());
            assertTrue(twoDoubleEquals(result.doubleValue(), exptForLog10));
        }
        assertEquals(retLn, retLog10);

        Expression log3Function = new LogFunction(expressionsLog3);
        boolean retLog3 = log3Function.evaluate(null, ptr);
        if (retLog3) {
            Double result =
                    (Double) log3Function.getDataType().toObject(ptr, log3Function.getSortOrder());
            assertTrue(twoDoubleEquals(result.doubleValue(), exptForLog3));
        }
        assertEquals(retLn, retLog3);
        return retLn;
    }

    private static void test(Number value, PNumericType dataType, double exptForLn,
            double exptForLog10, double exptForLog3) throws SQLException {
        LiteralExpression literal, literal2, literal3;
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.ASC);
        literal2 = LiteralExpression.newConstant(value, dataType, SortOrder.ASC);
        literal3 = LiteralExpression.newConstant(value, dataType, SortOrder.ASC);
        boolean ret1 =
                testExpression(literal, literal2, literal3, exptForLn, exptForLog10, exptForLog3);
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.DESC);
        literal2 = LiteralExpression.newConstant(value, dataType, SortOrder.DESC);
        literal3 = LiteralExpression.newConstant(value, dataType, SortOrder.DESC);
        boolean ret2 =
                testExpression(literal, literal2, literal3, exptForLn, exptForLog10, exptForLog3);
        assertEquals(ret1, ret2);
    }

    private static void testBatch(Number[] value, PNumericType dataType) throws SQLException {
        double[][] expected = new double[value.length][3];
        for (int i = 0; i < expected.length; ++i) {
            expected[i][0] = Math.log(value[i].doubleValue());
            expected[i][1] = Math.log10(value[i].doubleValue());
            expected[i][2] = Math.log10(value[i].doubleValue()) / Math.log10(3);
        }
        assertEquals(value.length, expected.length);
        for (int i = 0; i < value.length; ++i) {
            test(value[i], dataType, expected[i][0], expected[i][1], expected[i][2]);
        }
    }

    @Test
    public void testLnLogFunction() throws Exception {
        Random random = new Random();

        testBatch(
            new BigDecimal[] { BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.0),
                    BigDecimal.valueOf(-1.0), BigDecimal.valueOf(123.1234),
                    BigDecimal.valueOf(-123.1234), BigDecimal.valueOf(random.nextDouble()),
                    BigDecimal.valueOf(random.nextDouble()) }, PDecimal.INSTANCE);

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
