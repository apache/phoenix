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
import org.apache.phoenix.expression.function.ByteBasedRegexpSubstrFunction;
import org.apache.phoenix.expression.function.StringBasedRegexpSubstrFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RegexpSubstrFunctionTest {
    private final static PVarchar TYPE = PVarchar.INSTANCE;

    private String evalExp(Expression exp) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean eval = exp.evaluate(null, ptr);
        assertTrue(eval);
        String res = (String) exp.getDataType().toObject(ptr);
        return res;
    }

    private String testExpression(String srcStr, String patternStr, int offset, SortOrder sortOrder) throws SQLException {
        Expression srcExp, patternExp, offsetExp;
        srcExp = LiteralExpression.newConstant(srcStr, TYPE, sortOrder);
        patternExp = LiteralExpression.newConstant(patternStr, TYPE, sortOrder);
        offsetExp = LiteralExpression.newConstant(offset, PInteger.INSTANCE, sortOrder);
        List<Expression> expressions = Lists.newArrayList(srcExp, patternExp, offsetExp);
        String res1, res2;
        res1 = evalExp(new ByteBasedRegexpSubstrFunction(expressions));
        res2 = evalExp(new StringBasedRegexpSubstrFunction(expressions));
        assertEquals(res1, res2);
        return res1;
    }

    private String testExpression(String srcStr, String patternStr, int offset) throws SQLException {
        String result1 = testExpression(srcStr, patternStr, offset, SortOrder.ASC);
        String result2 = testExpression(srcStr, patternStr, offset, SortOrder.DESC);
        assertEquals(result1, result2);
        return result1;
    }

    private void testExpression(String srcStr, String patternStr, int offset, String expectedStr)
            throws SQLException {
        String result = testExpression(srcStr, patternStr, offset);
        assertEquals(expectedStr, result);
    }

    @Test
    public void test() throws Exception {
        testExpression("Report1?1", "[^\\\\?]+", 1, "Report1");
        testExpression("Report1?2", "[^\\\\?]+", 1, "Report1");
        testExpression("Report2?1", "[^\\\\?]+", 1, "Report2");
        testExpression("Report3?2", "[^\\\\?]+", 1, "Report3");
        testExpression("Report3?2", "[4-9]+", 0, (String) null);
        testExpression("Report3?2", "[^\\\\?]+", 2, "eport3");
        testExpression("Report3?2", "[^\\\\?]+", -5, "rt3");
    }
}
