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
import org.apache.phoenix.expression.function.ByteBasedRegexpReplaceFunction;
import org.apache.phoenix.expression.function.StringBasedRegexpReplaceFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RegexpReplaceFunctionTest {
    private final static PVarchar TYPE = PVarchar.INSTANCE;

    private String evalExp(Expression exp) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean eval = exp.evaluate(null, ptr);
        assertTrue(eval);
        String res = (String) exp.getDataType().toObject(ptr);
        return res;
    }

    private String testExpression(String srcStr, String patternStr, String replaceStr,
            SortOrder sortOrder) throws SQLException {
        Expression srcExp, patternExp, replaceExp;
        srcExp = LiteralExpression.newConstant(srcStr, TYPE, sortOrder);
        patternExp = LiteralExpression.newConstant(patternStr, TYPE, sortOrder);
        replaceExp = LiteralExpression.newConstant(replaceStr, TYPE, sortOrder);
        List<Expression> expressions = Lists.newArrayList(srcExp, patternExp, replaceExp);
        String res1, res2;
        res1 = evalExp(new ByteBasedRegexpReplaceFunction(expressions));
        res2 = evalExp(new StringBasedRegexpReplaceFunction(expressions));
        assertEquals(res1, res2);
        return res1;
    }

    private String testExpression(String srcStr, String patternStr, String replaceStr)
            throws SQLException {
        String result1 = testExpression(srcStr, patternStr, replaceStr, SortOrder.ASC);
        String result2 = testExpression(srcStr, patternStr, replaceStr, SortOrder.DESC);
        assertEquals(result1, result2);
        return result1;
    }

    private void testExpression(String srcStr, String patternStr, String replaceStr,
            String expectedStr) throws SQLException {
        String result = testExpression(srcStr, patternStr, replaceStr);
        assertEquals(expectedStr, result);
    }

    @Test
    public void test() throws Exception {
        testExpression("aa11bb22cc33dd44ee", "[0-9]+", "*", "aa*bb*cc*dd*ee");
        testExpression("aa11bb22cc33dd44ee", "[0-9]+", "", "aabbccddee");
        testExpression("aa11bb22cc33dd44ee", "[a-z][0-9]", "", "a1b2c3d4ee");
        testExpression("aa11bb22cc33dd44ee", "[a-z0-9]+", "", (String) null);
    }
}
