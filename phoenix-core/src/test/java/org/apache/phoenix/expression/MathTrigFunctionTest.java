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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.CosFunction;
import org.apache.phoenix.expression.function.SinFunction;
import org.apache.phoenix.expression.function.TanFunction;
import org.apache.phoenix.query.BaseTest;
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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
/**
 * Unit tests for {@link SinFunction}
 * Unit tests for {@link CosFunction}
 * Unit tests for {@link TanFunction}
 */

@RunWith(Parameterized.class)
public class MathTrigFunctionTest {

    private Number[] value;
    private PNumericType dataType;

    public MathTrigFunctionTest(Number[] value, PNumericType dataType) {
        this.value = value;
        this.dataType = dataType;
    }

    @Parameters(name = "{0} {1}")
    public static synchronized Collection<Object> data() {
        return Arrays.asList((Object[]) new Object[][]{
            {
                new BigDecimal[]{BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.0),
                    BigDecimal.valueOf(-1.0), BigDecimal.valueOf(123.1234),
                    BigDecimal.valueOf(-123.1234)},
                PDecimal.INSTANCE
            },
            {
                new Float[]{1.0f, 0.0f, -1.0f, Float.MAX_VALUE, Float.MIN_VALUE,
                    -Float.MAX_VALUE, -Float.MIN_VALUE, 123.1234f, -123.1234f},
                PFloat.INSTANCE
            },
            {
                new Float[]{1.0f, 0.0f, Float.MAX_VALUE, Float.MIN_VALUE, 123.1234f},
                PUnsignedFloat.INSTANCE
            },
            {
                new Double[]{1.0, 0.0, -1.0, Double.MAX_VALUE, Double.MIN_VALUE,
                    -Double.MAX_VALUE, -Double.MIN_VALUE, 123.1234, -123.1234},
                PDouble.INSTANCE
            },
            {
                new Double[]{1.0, 0.0, Double.MAX_VALUE, Double.MIN_VALUE, 123.1234},
                PUnsignedDouble.INSTANCE
            },
            {
                new Long[]{(long) 1, (long) 0, (long) -1, Long.MAX_VALUE,
                    Long.MIN_VALUE, (long) 123, (long) -123},
                PLong.INSTANCE
            },
            {
                new Long[]{(long) 1, (long) 0, Long.MAX_VALUE, (long) 123},
                PUnsignedLong.INSTANCE
            },
            {
                new Integer[]{1, 0, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 123, -123},
                PInteger.INSTANCE
            },
            {
                new Integer[]{1, 0, Integer.MAX_VALUE, 123},
                PUnsignedInt.INSTANCE
            },
            {
                new Short[]{(short) 1, (short) 0, (short) -1, Short.MAX_VALUE,
                    Short.MIN_VALUE, (short) 123, (short) -123},
                PSmallint.INSTANCE
            },
            {
                new Short[]{(short) 1, (short) 0, Short.MAX_VALUE, (short) 123},
                PSmallint.INSTANCE
            },
            {
                new Byte[]{(byte) 1, (byte) 0, (byte) -1, Byte.MAX_VALUE,
                    Byte.MIN_VALUE, (byte) 123, (byte) -123},
                PTinyint.INSTANCE
            },
            {
                new Byte[]{(byte) 1, (byte) 0, Byte.MAX_VALUE, (byte) 123},
                PTinyint.INSTANCE
            }
    });
    }

    private boolean testExpression(LiteralExpression literal, double expectedResult,
                                          String testedFunction) throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) literal);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Expression mathFunction = null;

        if (testedFunction.equals("SIN")) {
            mathFunction = new SinFunction(expressions);
        } else if (testedFunction.equals("COS")) {
            mathFunction = new CosFunction(expressions);
        } else if (testedFunction.equals("TAN")) {
            mathFunction = new TanFunction(expressions);
        }

        boolean ret = mathFunction.evaluate(null, ptr);
        if (ret) {
            Double result =
                    (Double) mathFunction.getDataType().toObject(ptr, mathFunction.getSortOrder());
            assertTrue(BaseTest.twoDoubleEquals(result.doubleValue(), expectedResult));
        }

        return ret;
    }

    private void test(Number value, PNumericType dataType, double expectedResult,
                             String testedFunction)
            throws SQLException {
        LiteralExpression literal = LiteralExpression.newConstant(value, dataType, SortOrder.ASC);
        boolean ret1 = testExpression(literal, expectedResult, testedFunction);

        literal = LiteralExpression.newConstant(value, dataType, SortOrder.DESC);
        boolean ret2 = testExpression(literal, expectedResult, testedFunction);
        assertEquals(ret1, ret2);
    }

    @Test
    public void testBatch()
            throws SQLException {
        for (int i = 0; i < value.length; ++i) {
            test(value[i], dataType, Math.sin(value[i].doubleValue()), "SIN");
            test(value[i], dataType, Math.cos(value[i].doubleValue()), "COS");
            test(value[i], dataType, Math.tan(value[i].doubleValue()), "TAN");
        }
    }
}
