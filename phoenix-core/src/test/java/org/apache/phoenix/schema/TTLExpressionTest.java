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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.junit.Test;

public class TTLExpressionTest {

    @Test
    public void testLiteralExpression() {
        int ttl = 100;
        LiteralTTLExpression literal = new LiteralTTLExpression(ttl);
        assertEquals(literal, TTLExpression.create(ttl));
        assertEquals(literal, TTLExpression.create(String.valueOf(ttl)));
    }

    @Test
    public void testForever() {
        assertEquals(TTLExpression.TTL_EXPRESSION_FORVER,
                TTLExpression.create(PhoenixDatabaseMetaData.FOREVER_TTL));
        assertEquals(TTLExpression.TTL_EXPRESSION_FORVER,
                TTLExpression.create(HConstants.FOREVER));
    }

    @Test
    public void testNone() {
        assertEquals(TTLExpression.TTL_EXPRESSION_NOT_DEFINED,
                TTLExpression.create(PhoenixDatabaseMetaData.NONE_TTL));
        assertEquals(TTLExpression.TTL_EXPRESSION_NOT_DEFINED,
                TTLExpression.create(PhoenixDatabaseMetaData.TTL_NOT_DEFINED));
        assertNull(TTLExpression.TTL_EXPRESSION_NOT_DEFINED.getTTLForScanAttribute());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLiteral() {
        TTLExpression.create(-1);
    }

    @Test
    public void testConditionalExpression() {
        String ttl = "PK1 = 5 AND COL1 > 'abc'";
        ConditionTTLExpression expected = new ConditionTTLExpression(ttl);
        TTLExpression actual = TTLExpression.create(ttl);
        assertEquals(expected, actual);
        assertEquals(ttl, expected.getTTLExpression());
        assertNull(actual.getTTLForScanAttribute());
    }
}
