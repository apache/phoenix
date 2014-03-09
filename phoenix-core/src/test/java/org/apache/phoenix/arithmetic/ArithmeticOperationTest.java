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
package org.apache.phoenix.arithmetic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.ValueTypeIncompatibleException;
import org.apache.phoenix.expression.DecimalAddExpression;
import org.apache.phoenix.expression.DecimalDivideExpression;
import org.apache.phoenix.expression.DecimalMultiplyExpression;
import org.apache.phoenix.expression.DecimalSubtractExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.PDataType;
import org.junit.Test;


public class ArithmeticOperationTest {

    // Addition
    // result scale should be: max(ls, rs)
    // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
    @Test
    public void testDecimalAddition() throws Exception {
        LiteralExpression op1, op2, op3;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalAddExpression e;
        boolean evaluated;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL, 31, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL, 5, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1234567890123456789012345691246"), ptr);

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("12468.45"), ptr);

        // Exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123"), PDataType.DECIMAL, 3, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
        }

        // Pass since we roll out imposing precisioin and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123"), PDataType.DECIMAL, 3, 0);
        op3 = LiteralExpression.newConstant(new BigDecimal("-123"), PDataType.DECIMAL, 3, 0);
        children = Arrays.<Expression>asList(op1, op2, op3);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("99999999999999999999999999999999999999"), ptr);

        // Exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("9999.1"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("1.1111"), PDataType.DECIMAL, 5, 4);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("10000.2111"), ptr);
    }

    @Test
    public void testIntPlusDecimal() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalAddExpression e;
        boolean evaluated;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234.111"), PDataType.DECIMAL);
        assertEquals(Integer.valueOf(3),op1.getScale());
        op2 = LiteralExpression.newConstant(1, PDataType.INTEGER);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1235.111"), ptr);
    }

    // Subtraction
    // result scale should be: max(ls, rs)
    // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
    @Test
    public void testDecimalSubtraction() throws Exception {
        LiteralExpression op1, op2, op3;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalSubtractExpression e;
        boolean evaluated;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDataType.DECIMAL, 31, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL, 5, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1234567890123456789012345666556"), ptr);

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("12221.55"), ptr);

        // Excceds precision
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("-123"), PDataType.DECIMAL, 3, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
        }

        // Pass since we roll up precision and scale imposing.
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("-123"), PDataType.DECIMAL, 3, 0);
        op3 = LiteralExpression.newConstant(new BigDecimal("123"), PDataType.DECIMAL, 3, 0);
        children = Arrays.<Expression>asList(op1, op2, op3);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("99999999999999999999999999999999999999"), ptr);

        // Exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("1111.1"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("1.1111"), PDataType.DECIMAL, 5, 4);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1109.9889"), ptr);
    }

    // Multiplication
    // result scale should be: ls + rs
    // result precision should be: lp + rp
    @Test
    public void testDecimalMultiplication() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalMultiplyExpression e;
        boolean evaluated;

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDataType.DECIMAL, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1523990.25"), ptr);

        // Value too big, exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL, 5, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
        }

        // Values exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("1.45"), PDataType.DECIMAL, 3, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("1111.1"), PDataType.DECIMAL);
        assertEquals(Integer.valueOf(1),op1.getScale());
        op2 = LiteralExpression.newConstant(new BigDecimal("1.1111"), PDataType.DECIMAL, 5, 4);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1234.54321"), ptr);
    }

    // Division
    // result scale should be: 31 - lp + ls - rs
    // result precision should be: lp - ls + rp + scale
    @Test
    public void testDecimalDivision() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        ImmutableBytesWritable ptr;
        DecimalDivideExpression e;
        boolean evaluated;

        // The value should be 1234500.0000...00 because we set to scale to be 24. However, in
        // PhoenixResultSet.getBigDecimal, the case to (BigDecimal) actually cause the scale to be eradicated. As
        // a result, the resulting value does not have the right form.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDataType.DECIMAL, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("0.01"), PDataType.DECIMAL, 2, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("1.2345E+6"), ptr);

        // Exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDataType.DECIMAL, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("0.01"), PDataType.DECIMAL, 2, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        ptr = new ImmutableBytesWritable();
        try {
            evaluated = e.evaluate(null, ptr);
            fail("Evaluation should have failed");
        } catch (ValueTypeIncompatibleException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("10"), PDataType.DECIMAL);
        op2 = LiteralExpression.newConstant(new BigDecimal("3"), PDataType.DECIMAL, 5, 4);
        assertEquals(Integer.valueOf(4),op2.getScale());
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        ptr = new ImmutableBytesWritable();
        evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEqualValue(PDataType.DECIMAL, new BigDecimal("3.3333333333333333333333333333333333333"), ptr);
    }

    private static void assertEqualValue(PDataType type, Object value, ImmutableBytesWritable ptr) {
        assertEquals(value, type.toObject(ptr.get()));
    }
}
