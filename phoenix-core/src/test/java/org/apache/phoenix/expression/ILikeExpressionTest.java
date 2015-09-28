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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class ILikeExpressionTest {
    private boolean testExpression (String value, String expression, SortOrder sortorder)
            throws SQLException {
      LiteralExpression v = LiteralExpression.newConstant(value, PVarchar.INSTANCE, sortorder);
      LiteralExpression p = LiteralExpression.newConstant(expression, PVarchar.INSTANCE, sortorder);
      List<Expression> children = Arrays.<Expression>asList(v,p);
      LikeExpression e1 = ByteBasedLikeExpression.create(children, LikeType.CASE_INSENSITIVE);
      LikeExpression e2 = StringBasedLikeExpression.create(children, LikeType.CASE_INSENSITIVE);
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      boolean evaluated1 = e1.evaluate(null, ptr);
      Boolean result1 = (Boolean)e1.getDataType().toObject(ptr);
      assertTrue(evaluated1);
      boolean evaluated2 = e2.evaluate(null, ptr);
      Boolean result2 = (Boolean)e2.getDataType().toObject(ptr);
      assertTrue(evaluated2);
      assertEquals(result1, result2);
      return result1;
    }

    private boolean testExpression(String value, String expression) throws SQLException {
        boolean result1 = testExpression(value, expression, SortOrder.ASC);
        boolean result2 = testExpression(value, expression, SortOrder.DESC);
        assertEquals(result1, result2);
        return result1;
    }

    @Test
    public void testStartWildcard() throws Exception {
        assertEquals(Boolean.FALSE, testExpression ("149na7-app1-2-", "%-w"));
        assertEquals(Boolean.TRUE, testExpression ("149na7-app1-2-", "%-2%"));
        assertEquals(Boolean.TRUE, testExpression ("149na7-app1-2-", "%4%7%2%"));
        assertEquals(Boolean.FALSE, testExpression ("149na7-app1-2-", "%9%4%2%"));
    }

    @Test
    public void testCaseSensitive() throws Exception {
        assertEquals(Boolean.TRUE, testExpression ("test", "test"));
        assertEquals(Boolean.TRUE, testExpression ("test", "teSt"));
    }

    @Test
    public void testStartWildcardAndCaseInsensitive() throws Exception {
        assertEquals(Boolean.TRUE, testExpression ("test", "%s%"));
        assertEquals(Boolean.TRUE, testExpression ("test", "%S%"));
    }
}
