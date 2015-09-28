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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ByteBasedRegexpSplitFunction;
import org.apache.phoenix.expression.function.StringBasedRegexpSplitFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RegexpSplitFunctionTest {
    private final static PVarchar TYPE = PVarchar.INSTANCE;

    private String[] evalExp(Expression exp) throws SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean eval = exp.evaluate(null, ptr);
        assertTrue(eval);
        PhoenixArray evalRes = (PhoenixArray) exp.getDataType().toObject(ptr);
        String[] res = (String[]) evalRes.getArray();
        return res;
    }

    private String[] testExpression(String srcStr, String patternStr, SortOrder sortOrder)
            throws SQLException {
        Expression srcExp, patternExp;
        srcExp = LiteralExpression.newConstant(srcStr, TYPE, sortOrder);
        patternExp = LiteralExpression.newConstant(patternStr, TYPE, sortOrder);
        List<Expression> expressions = Lists.newArrayList(srcExp, patternExp);
        String[] res1, res2;
        res1 = evalExp(new ByteBasedRegexpSplitFunction(expressions));
        res2 = evalExp(new StringBasedRegexpSplitFunction(expressions));
        testEqual(res2, res1);
        return res1;
    }

    private String[] testExpression(String srcStr, String patternStr) throws SQLException {
        String[] result1 = testExpression(srcStr, patternStr, SortOrder.ASC);
        String[] result2 = testExpression(srcStr, patternStr, SortOrder.DESC);
        testEqual(result1, result2);
        return result1;
    }

    private void testEqual(String[] expectedStr, String[] result) {
        if (result == null ^ expectedStr == null) return;
        if (expectedStr == null) return;
        assertEquals(expectedStr.length, result.length);
        for (int i = 0; i < expectedStr.length; ++i)
            assertEquals(expectedStr[i], result[i]);
    }

    private void testExpression(String srcStr, String patternStr, String[] expectedStr)
            throws SQLException {
        String[] result = testExpression(srcStr, patternStr);
        testEqual(expectedStr, result);
    }

    @Test
    public void test() throws Exception {
        String[] res = new String[] { "ONE", "TWO", "THREE" };
        testExpression("ONE:TWO:THREE", ":", res);
        testExpression("ONE,TWO,THREE", ",", res);
        testExpression("12ONE34TWO56THREE78", "[0-9]+", new String[] { null, "ONE", "TWO", "THREE",
                null });
        testExpression("ONE34TWO56THREE78", "[0-9]+", new String[] { "ONE", "TWO", "THREE", null });
        testExpression("123ONE34TWO56THREE", "[0-9]+", new String[] { null, "ONE", "TWO", "THREE" });
        testExpression("123", "[0-9]+", new String[] { null, null });
        testExpression("ONE", "[0-9]+", new String[] { "ONE" });
    }
}
