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

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ArrayConstructorExpressionTest {
    
    protected static final LiteralExpression CONSTANT_EXPRESSION = LiteralExpression.newConstant(QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    protected static final byte[] BYTE_ARRAY1 = new byte[]{1,2,3,4,5};
    protected static final byte[] BYTE_ARRAY2 = new byte[]{6,7,8};
    protected Expression FALSE_EVAL_EXPRESSION = new DelegateExpression(LiteralExpression.newConstant(null)) {
        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            return false;
        }
    };
    
    @Test
    public void testLeadingNulls() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(nullExpression);
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        ArrayConstructorExpression arrayConstructorExpression = new ArrayConstructorExpression(children, PVarbinary.INSTANCE, false);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        
        ArrayElemRefExpression arrayElemRefExpression = new ArrayElemRefExpression(Lists.<Expression>newArrayList(arrayConstructorExpression));
        arrayElemRefExpression.setIndex(1);
        arrayElemRefExpression.evaluate(null, ptr);
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptr.copyBytesIfNecessary());
        arrayElemRefExpression.setIndex(2);
        arrayElemRefExpression.evaluate(null, ptr);
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptr.copyBytesIfNecessary());
        arrayElemRefExpression.setIndex(3);
        arrayElemRefExpression.evaluate(null, ptr);
        assertArrayEquals(BYTE_ARRAY1, ptr.copyBytesIfNecessary());
        arrayElemRefExpression.setIndex(4);
        arrayElemRefExpression.evaluate(null, ptr);
        assertArrayEquals(BYTE_ARRAY2, ptr.copyBytesIfNecessary());
    }
    
}
