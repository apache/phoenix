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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.DelegateExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.SingleCellConstructorExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class ImmutableStorageSchemeTest {
    
    protected static final LiteralExpression CONSTANT_EXPRESSION = LiteralExpression.newConstant(QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    protected static final byte[] BYTE_ARRAY1 = new byte[]{1,2,3,4,5};
    protected static final byte[] BYTE_ARRAY2 = new byte[]{6,7,8};
    protected Expression FALSE_EVAL_EXPRESSION = new DelegateExpression(LiteralExpression.newConstant(null)) {
        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            return false;
        }
    };
    private ImmutableStorageScheme immutableStorageScheme;
    
    @Parameters(name="ImmutableStorageSchemeTest_immutableStorageScheme={0}}") // name is used by failsafe as file name in reports
    public static ImmutableStorageScheme[] data() {
        ImmutableStorageScheme[] values = ImmutableStorageScheme.values();
        // skip ONE_CELL_PER_COLUMN
        return Arrays.copyOfRange(values, 1, values.length);
    }
    
    public ImmutableStorageSchemeTest(ImmutableStorageScheme immutableStorageScheme) {
        this.immutableStorageScheme = immutableStorageScheme;
    }

    @Test
    public void testWithExpressionsThatEvaluatetoFalse() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        children.add(CONSTANT_EXPRESSION);
        children.add(FALSE_EVAL_EXPRESSION);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(FALSE_EVAL_EXPRESSION);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        singleCellConstructorExpression.evaluate(null, ptr);
        
        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, 0));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, 1));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 2));
        assertArrayEquals(BYTE_ARRAY1, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, 3));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 4));
        assertArrayEquals(BYTE_ARRAY2, ptrCopy.copyBytesIfNecessary());
    }
    
    @Test
    public void testWithMaxOffsetLargerThanShortMax() throws Exception {
        int numElements = Short.MAX_VALUE+2;
        List<Expression> children = Lists.newArrayListWithExpectedSize(numElements);
        for (int i=0; i<numElements; ++i) {
            children.add(CONSTANT_EXPRESSION);
        }
        SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        singleCellConstructorExpression.evaluate(null, ptr);

        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, 0));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 14999));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, numElements-1));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
    }
    
    @Test
    public void testWithMaxOffsetSmallerThanShortMin() throws Exception {
        int numElements = Short.MAX_VALUE+2;
        List<Expression> children = Lists.newArrayListWithExpectedSize(numElements);
        for (int i=0; i<=numElements; i+=2) {
            children.add(CONSTANT_EXPRESSION);
            children.add(FALSE_EVAL_EXPRESSION);
        }
        SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        singleCellConstructorExpression.evaluate(null, ptr);

        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, 0));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, 1));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, numElements-1));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, numElements));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
    }
    
    @Test
    public void testLeadingNulls() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(nullExpression);
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        singleCellConstructorExpression.evaluate(null, ptr);
        
        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, 0));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 1));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 2));
        assertArrayEquals(BYTE_ARRAY1, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 3));
        assertArrayEquals(BYTE_ARRAY2, ptrCopy.copyBytesIfNecessary());
    }
    
}
