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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.junit.Test;

public class LikeExpressionTest {
    public boolean testExpression (String value, String expression) {
      LiteralExpression v = LiteralExpression.newConstant(value);
      LiteralExpression p = LiteralExpression.newConstant(expression);
      List<Expression> children = Arrays.<Expression>asList(v,p);
      LikeExpression e = LikeExpression.create(children, LikeType.CASE_SENSITIVE);
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      boolean evaluated = e.evaluate(null, ptr);
      Boolean result = (Boolean)e.getDataType().toObject(ptr);
      assertTrue(evaluated);
      return result;
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
        assertEquals(Boolean.FALSE, testExpression ("test", "teSt"));
    }

    @Test
    public void testStartWildcardAndCaseInsensitive() throws Exception {
        assertEquals(Boolean.TRUE, testExpression ("test", "%s%"));
        assertEquals(Boolean.FALSE, testExpression ("test", "%S%"));
    }
 }
