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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.SignFunction;
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
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link SignFunction}
 * @since 4.3.0
 */
public class SignFunctionTest {

    private static void testExpression(LiteralExpression literal, Integer expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) literal);
        Expression signFunction = new SignFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        signFunction.evaluate(null, ptr);
        Integer result = (Integer) signFunction.getDataType().toObject(ptr);
        assertTrue(result.compareTo(expected) == 0);
    }

    private static void test(Number value, PNumericType dataType, int expected) throws SQLException {
        LiteralExpression literal;
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.ASC);
        testExpression(literal, expected);
        literal = LiteralExpression.newConstant(value, dataType, SortOrder.DESC);
        testExpression(literal, expected);
    }

    private static void testBatch(Number[] value, PNumericType dataType, int[] expected)
            throws SQLException {
        assertEquals(value.length, expected.length);
        for (int i = 0; i < value.length; ++i) {
            test(value[i], dataType, expected[i]);
        }
    }

    @Test
    public void testSignFunction() throws Exception {
        testBatch(
            new BigDecimal[] { BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.0),
                    BigDecimal.valueOf(-1.0), BigDecimal.valueOf(123.1234),
                    BigDecimal.valueOf(-123.1234) }, PDecimal.INSTANCE,
            new int[] { 1, 0, -1, 1, -1 });

        testBatch(new Float[] { 1.0f, 0.0f, -1.0f, Float.MAX_VALUE, Float.MIN_VALUE,
                -Float.MAX_VALUE, -Float.MIN_VALUE, 123.1234f, -123.1234f }, PFloat.INSTANCE,
            new int[] { 1, 0, -1, 1, 1, -1, -1, 1, -1 });

        testBatch(new Float[] { 1.0f, 0.0f, Float.MAX_VALUE, Float.MIN_VALUE, 123.1234f },
            PUnsignedFloat.INSTANCE, new int[] { 1, 0, 1, 1, 1 });

        testBatch(new Double[] { 1.0, 0.0, -1.0, Double.MAX_VALUE, Double.MIN_VALUE,
                -Double.MAX_VALUE, -Double.MIN_VALUE, 123.1234, -123.1234 }, PDouble.INSTANCE,
            new int[] { 1, 0, -1, 1, 1, -1, -1, 1, -1 });

        testBatch(new Double[] { 1.0, 0.0, Double.MAX_VALUE, Double.MIN_VALUE, 123.1234 },
            PUnsignedDouble.INSTANCE, new int[] { 1, 0, 1, 1, 1 });

        testBatch(new Long[] { (long) 1, (long) 0, (long) -1, Long.MAX_VALUE, Long.MIN_VALUE,
                (long) 123, (long) -123 }, PLong.INSTANCE, new int[] { 1, 0, -1, 1, -1, 1, -1 });

        testBatch(new Long[] { (long) 1, (long) 0, Long.MAX_VALUE, (long) 123 }, PLong.INSTANCE,
            new int[] { 1, 0, 1, 1 });

        testBatch(new Integer[] { 1, 0, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 123, -123 },
            PInteger.INSTANCE, new int[] { 1, 0, -1, 1, -1, 1, -1 });

        testBatch(new Integer[] { 1, 0, Integer.MAX_VALUE, 123 }, PUnsignedInt.INSTANCE, new int[] {
                1, 0, 1, 1 });

        testBatch(new Short[] { (short) 1, (short) 0, (short) -1, Short.MAX_VALUE, Short.MIN_VALUE,
                (short) 123, (short) -123 }, PSmallint.INSTANCE,
            new int[] { 1, 0, -1, 1, -1, 1, -1 });

        testBatch(new Short[] { (short) 1, (short) 0, Short.MAX_VALUE, (short) 123 },
            PSmallint.INSTANCE, new int[] { 1, 0, 1, 1 });

        testBatch(new Byte[] { (byte) 1, (byte) 0, (byte) -1, Byte.MAX_VALUE, Byte.MIN_VALUE,
                (byte) 123, (byte) -123 }, PTinyint.INSTANCE, new int[] { 1, 0, -1, 1, -1, 1, -1 });

        testBatch(new Byte[] { (byte) 1, (byte) 0, Byte.MAX_VALUE, (byte) 123 }, PTinyint.INSTANCE,
            new int[] { 1, 0, 1, 1 });
    }
}
