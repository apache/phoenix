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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.expression.function.RandomFunction;
import org.apache.phoenix.expression.visitor.CloneExpressionVisitor;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;


public class ArithmeticOperationTest {

    // Addition
    // result scale should be: max(ls, rs)
    // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
    @Test
    public void testDecimalAddition() throws Exception {
        LiteralExpression op1, op2, op3;
        List<Expression> children;
        DecimalAddExpression e;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDecimal.INSTANCE, 31, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDecimal.INSTANCE, 5, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("1234567890123456789012345691246"));

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDecimal.INSTANCE, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDecimal.INSTANCE, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("12468.45"));

        // Exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123"), PDecimal.INSTANCE, 3, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        try {
            e.evaluate(null, new ImmutableBytesWritable());
            fail("Evaluation should have failed");
        } catch (DataExceedsCapacityException ex) {
        }

        // Pass since we roll out imposing precisioin and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123"), PDecimal.INSTANCE, 3, 0);
        op3 = LiteralExpression.newConstant(new BigDecimal("-123"), PDecimal.INSTANCE, 3, 0);
        children = Arrays.<Expression>asList(op1, op2, op3);
        e = new DecimalAddExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("99999999999999999999999999999999999999"));

        // Exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDecimal.INSTANCE, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        try {
            e.evaluate(null, new ImmutableBytesWritable());
            fail("Evaluation should have failed");
        } catch (DataExceedsCapacityException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("9999.1"), PDecimal.INSTANCE);
        op2 = LiteralExpression.newConstant(new BigDecimal("1.1111"), PDecimal.INSTANCE, 5, 4);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("10000.2111"));
    }

    @Test
    public void testIntPlusDecimal() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        DecimalAddExpression e;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234.111"), PDecimal.INSTANCE);
        assertNull(op1.getScale());
        op2 = LiteralExpression.newConstant(1, PInteger.INSTANCE);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalAddExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("1235.111"));
    }

    // Subtraction
    // result scale should be: max(ls, rs)
    // result precision should be: max(lp - ls, rp - rs) + 1 + max(ls, rs)
    @Test
    public void testDecimalSubtraction() throws Exception {
        LiteralExpression op1, op2, op3;
        List<Expression> children;
        DecimalSubtractExpression e;

        op1 = LiteralExpression.newConstant(new BigDecimal("1234567890123456789012345678901"), PDecimal.INSTANCE, 31, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDecimal.INSTANCE, 5, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("1234567890123456789012345666556"));

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDecimal.INSTANCE, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDecimal.INSTANCE, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("12221.55"));

        // Excceds precision
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("-123"), PDecimal.INSTANCE, 3, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        try {
            e.evaluate(null, new ImmutableBytesWritable());
            fail("Evaluation should have failed");
        } catch (DataExceedsCapacityException ex) {
        }

        // Pass since we roll up precision and scale imposing.
        op1 = LiteralExpression.newConstant(new BigDecimal("99999999999999999999999999999999999999"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("-123"), PDecimal.INSTANCE, 3, 0);
        op3 = LiteralExpression.newConstant(new BigDecimal("123"), PDecimal.INSTANCE, 3, 0);
        children = Arrays.<Expression>asList(op1, op2, op3);
        e = new DecimalSubtractExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("99999999999999999999999999999999999999"));

        // Exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDecimal.INSTANCE, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        try {
            e.evaluate(null, new ImmutableBytesWritable());
            fail("Evaluation should have failed");
        } catch (DataExceedsCapacityException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("1111.1"), PDecimal.INSTANCE);
        op2 = LiteralExpression.newConstant(new BigDecimal("1.1111"), PDecimal.INSTANCE, 5, 4);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalSubtractExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("1109.9889"));
    }

    // Multiplication
    // result scale should be: ls + rs
    // result precision should be: lp + rp
    @Test
    public void testDecimalMultiplication() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        DecimalMultiplyExpression e;

        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDecimal.INSTANCE, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("123.45"), PDecimal.INSTANCE, 5, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("1523990.25"));

        // Value too big, exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("12345"), PDecimal.INSTANCE, 5, 0);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        try {
            e.evaluate(null, new ImmutableBytesWritable());
            fail("Evaluation should have failed");
        } catch (DataExceedsCapacityException ex) {
        }

        // Values exceeds scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("1.45"), PDecimal.INSTANCE, 3, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        try {
            e.evaluate(null, new ImmutableBytesWritable());
            fail("Evaluation should have failed");
        } catch (DataExceedsCapacityException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("1111.1"), PDecimal.INSTANCE);
        assertNull(op1.getScale());
        op2 = LiteralExpression.newConstant(new BigDecimal("1.1111"), PDecimal.INSTANCE, 5, 4);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalMultiplyExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("1234.54321"));
    }

    // Division
    // result scale should be: 31 - lp + ls - rs
    // result precision should be: lp - ls + rp + scale
    @Test
    public void testDecimalDivision() throws Exception {
        LiteralExpression op1, op2;
        List<Expression> children;
        DecimalDivideExpression e;

        // The value should be 1234500.0000...00 because we set to scale to be 24. However, in
        // PhoenixResultSet.getBigDecimal, the case to (BigDecimal) actually cause the scale to be eradicated. As
        // a result, the resulting value does not have the right form.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345"), PDecimal.INSTANCE, 5, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("0.01"), PDecimal.INSTANCE, 2, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("1.2345E+6"));

        // Exceeds precision.
        op1 = LiteralExpression.newConstant(new BigDecimal("12345678901234567890123456789012345678"), PDecimal.INSTANCE, 38, 0);
        op2 = LiteralExpression.newConstant(new BigDecimal("0.01"), PDecimal.INSTANCE, 2, 2);
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        try {
            e.evaluate(null, new ImmutableBytesWritable());
            fail("Evaluation should have failed");
        } catch (DataExceedsCapacityException ex) {
        }
        
        // Decimal with no precision and scale.
        op1 = LiteralExpression.newConstant(new BigDecimal("10"), PDecimal.INSTANCE);
        op2 = LiteralExpression.newConstant(new BigDecimal("3"), PDecimal.INSTANCE, 5, 4);
        assertEquals(Integer.valueOf(4),op2.getScale());
        children = Arrays.<Expression>asList(op1, op2);
        e = new DecimalDivideExpression(children);
        assertEqualValue(e, PDecimal.INSTANCE, new BigDecimal("3.3333333333333333333333333333333333333"));
    }

    @Test
    public void testPerInvocationClone() throws Exception {
        LiteralExpression op1, op2, op3, op4;
        List<Expression> children;
        Expression e1, e2, e3, e4;
        ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
        ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();

        op1 = LiteralExpression.newConstant(5.0);
        op2 = LiteralExpression.newConstant(3.0);
        op3 = LiteralExpression.newConstant(2.0);
        op4 = LiteralExpression.newConstant(1.0);
        children = Arrays.<Expression>asList(op1, op2);
        e1 = new DoubleAddExpression(children);
        children = Arrays.<Expression>asList(op3, op4);
        e2 = new DoubleSubtractExpression(children);
        e3 = new DoubleAddExpression(Arrays.<Expression>asList(e1, e2));
        e4 = new DoubleAddExpression(Arrays.<Expression>asList(new RandomFunction(Arrays.<Expression>asList(LiteralExpression.newConstant(null))), e3));
        CloneExpressionVisitor visitor = new CloneExpressionVisitor();
        Expression clone = e4.accept(visitor);
        assertTrue(clone != e4);
        e4.evaluate(null, ptr1);
        clone.evaluate(null, ptr2);
        assertNotEquals(ptr1, ptr2);
        
        e4 = new DoubleAddExpression(Arrays.<Expression>asList(new RandomFunction(Arrays.<Expression>asList(LiteralExpression.newConstant(1))), e3));
        visitor = new CloneExpressionVisitor();
        clone = e4.accept(visitor);
        assertTrue(clone == e4);
        e4.evaluate(null, ptr1);
        clone.evaluate(null, ptr2);
        assertEquals(ptr1, ptr2);
    }

    private static void assertEqualValue(Expression e, PDataType type, Object value) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEquals(value, type.toObject(ptr.get()));
        CloneExpressionVisitor visitor = new CloneExpressionVisitor();
        Expression clone = e.accept(visitor);
        evaluated = clone.evaluate(null, ptr);
        assertTrue(evaluated);
        assertEquals(value, type.toObject(ptr.get()));
    }
}
