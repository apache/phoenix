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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.OctetLengthFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBinaryBase;
import org.apache.phoenix.schema.types.PVarbinary;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link OctetLengthFunction}
 */
public class OctetLengthFunctionTest {
    private void testOctetLengthExpression(Expression data, int expected) throws SQLException {
        List<Expression> expressions = Lists.newArrayList(data);
        Expression octetLengthFunction = new OctetLengthFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        octetLengthFunction.evaluate(null, ptr);
        Integer result =
                (Integer) octetLengthFunction.getDataType().toObject(ptr,
                    octetLengthFunction.getSortOrder());
        assertEquals(expected, result.intValue());
    }

    private void testOctetLength(byte[] bytes, PBinaryBase dataType, int expected)
            throws SQLException {
        LiteralExpression dataExpr;
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.ASC);
        testOctetLengthExpression(dataExpr, expected);
        dataExpr = LiteralExpression.newConstant(bytes, dataType, SortOrder.DESC);
        testOctetLengthExpression(dataExpr, expected);
    }

    @Test
    public void testByteBatch() throws SQLException {
        for (int len = 0; len < 300; ++len) {
            byte[] bytes = new byte[len];
            testOctetLength(bytes, PBinary.INSTANCE, bytes.length);
            testOctetLength(bytes, PVarbinary.INSTANCE, bytes.length);
        }
    }
}
