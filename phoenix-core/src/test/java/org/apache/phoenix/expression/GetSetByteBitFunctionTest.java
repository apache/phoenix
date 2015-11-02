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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.GetBitFunction;
import org.apache.phoenix.expression.function.GetByteFunction;
import org.apache.phoenix.expression.function.SetBitFunction;
import org.apache.phoenix.expression.function.SetByteFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBinaryBase;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link GetByteFunction} {@link SetByteFunction} {@link GetBitFunction}
 * {@link SetBitFunction}
 */
public class GetSetByteBitFunctionTest {
    private void testGetByteExpression(Expression data, Expression offset, int expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList(data, offset);
        Expression getByteFunction = new GetByteFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        getByteFunction.evaluate(null, ptr);
        Integer result =
                (Integer) getByteFunction.getDataType().toObject(ptr,
                    getByteFunction.getSortOrder());
        assertEquals(expected, result.intValue());
    }

    private void testSetByteExpression(Expression data, Expression offset, Expression newValue,
            byte[] expected) throws SQLException {
        List<Expression> expressions = Lists.newArrayList(data, offset, newValue);
        Expression setByteFunction = new SetByteFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        setByteFunction.evaluate(null, ptr);
        byte[] result =
                (byte[]) setByteFunction.getDataType()
                        .toObject(ptr, setByteFunction.getSortOrder());
        assertArrayEquals(expected, result);
    }

    private void testGetByte(byte[] bytes, int offset, PBinaryBase dataType, int expected)
            throws SQLException {
        LiteralExpression dataExpr, offsetExpr;
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.ASC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.ASC);
        testGetByteExpression(dataExpr, offsetExpr, expected);
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.DESC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.DESC);
        testGetByteExpression(dataExpr, offsetExpr, expected);
    }

    private void testSetByte(byte[] bytes, int offset, int newValue, PBinaryBase dataType,
            byte[] expected) throws SQLException {
        LiteralExpression dataExpr, offsetExpr, newValueExpr;
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.ASC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.ASC);
        newValueExpr = LiteralExpression.newConstant(newValue, PInteger.INSTANCE, SortOrder.ASC);
        testSetByteExpression(dataExpr, offsetExpr, newValueExpr, expected);
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.DESC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.DESC);
        newValueExpr = LiteralExpression.newConstant(newValue, PInteger.INSTANCE, SortOrder.DESC);
        testSetByteExpression(dataExpr, offsetExpr, newValueExpr, expected);
    }

    @Test
    public void testByteBatch() throws SQLException {
        byte[] bytes = new byte[256];
        int sum = 0;
        for (int i = 0; i < 256; ++i) {
            bytes[i] = (byte) (i & 0xff);
            sum += bytes[i];
        }
        assertEquals(-128, sum);
        for (int offset = 0; offset < 256; ++offset) {
            testGetByte(bytes, offset, PBinary.INSTANCE, bytes[offset]);
            testGetByte(bytes, offset, PVarbinary.INSTANCE, bytes[offset]);
        }
        for (int offset = 0; offset < 256; ++offset)
            for (int tmp = Byte.MIN_VALUE; tmp <= Byte.MAX_VALUE; ++tmp) {
                byte[] expected = new byte[bytes.length];
                System.arraycopy(bytes, 0, expected, 0, bytes.length);
                expected[offset] = (byte) (tmp & 0xff);
                testSetByte(bytes, offset, tmp, PBinary.INSTANCE, expected);
            }
    }

    private void testGetBitExpression(Expression data, Expression offset, int expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList(data, offset);
        Expression getBitFunction = new GetBitFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        getBitFunction.evaluate(null, ptr);
        Integer result =
                (Integer) getBitFunction.getDataType().toObject(ptr, getBitFunction.getSortOrder());
        assertEquals(expected, result.intValue());
    }

    private void testSetBitExpression(Expression data, Expression offset, Expression newValue,
            byte[] expected) throws SQLException {
        List<Expression> expressions = Lists.newArrayList(data, offset, newValue);
        Expression setBitFunction = new SetBitFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        setBitFunction.evaluate(null, ptr);
        byte[] result =
                (byte[]) setBitFunction.getDataType().toObject(ptr, setBitFunction.getSortOrder());
        assertArrayEquals(expected, result);
    }

    private void testGetBit(byte[] bytes, int offset, PBinaryBase dataType, int expected)
            throws SQLException {
        LiteralExpression dataExpr, offsetExpr;
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.ASC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.ASC);
        testGetBitExpression(dataExpr, offsetExpr, expected);
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.DESC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.DESC);
        testGetBitExpression(dataExpr, offsetExpr, expected);
    }

    private void testSetBit(byte[] bytes, int offset, int newValue, PBinaryBase dataType,
            byte[] expected) throws SQLException {
        LiteralExpression dataExpr, offsetExpr, newValueExpr;
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.ASC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.ASC);
        newValueExpr = LiteralExpression.newConstant(newValue, PInteger.INSTANCE, SortOrder.ASC);
        testSetBitExpression(dataExpr, offsetExpr, newValueExpr, expected);
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.DESC);
        offsetExpr = LiteralExpression.newConstant(offset, PInteger.INSTANCE, SortOrder.DESC);
        newValueExpr = LiteralExpression.newConstant(newValue, PInteger.INSTANCE, SortOrder.DESC);
        testSetBitExpression(dataExpr, offsetExpr, newValueExpr, expected);
    }

    @Test
    public void testGetBitBatch() throws SQLException {
        byte[] bytes = new byte[256];
        int sum = 0;
        for (int i = 0; i < 256; ++i) {
            bytes[i] = (byte) (i & 0xff);
            sum += bytes[i];
        }
        assertEquals(-128, sum);
        for (int offset = 0; offset < 256 * Byte.SIZE; ++offset) {
            byte expected =
                    (bytes[offset / Byte.SIZE] & (1 << (offset % Byte.SIZE))) != 0 ? (byte) 1
                            : (byte) 0;
            testGetBit(bytes, offset, PBinary.INSTANCE, expected);
            testGetBit(bytes, offset, PVarbinary.INSTANCE, expected);
        }
        for (int offset = 0; offset < 256 * Byte.SIZE; ++offset)
            for (int tmp = 0; tmp <= 1; ++tmp) {
                byte[] expected = new byte[bytes.length];
                System.arraycopy(bytes, 0, expected, 0, bytes.length);
                if (tmp != 0) {
                    expected[offset / Byte.SIZE] |= (byte) (1 << (offset % Byte.SIZE));
                } else {
                    expected[offset / Byte.SIZE] &= (byte) (~(1 << (offset % Byte.SIZE)));
                }
                testSetBit(bytes, offset, tmp, PBinary.INSTANCE, expected);
            }
    }
}
