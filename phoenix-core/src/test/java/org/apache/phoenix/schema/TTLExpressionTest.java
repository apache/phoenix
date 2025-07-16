/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_FOREVER;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TTLExpressionTest {

  @Mock
  private PhoenixConnection pconn;
  @Mock
  private PTable table;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testLiteralExpression() {
    int ttl = 100;
    LiteralTTLExpression literal = new LiteralTTLExpression(ttl);
    assertEquals(literal, TTLExpressionFactory.create(ttl));
    assertEquals(literal, TTLExpressionFactory.create(String.valueOf(ttl)));
  }

  @Test
  public void testForever() {
    assertEquals(TTL_EXPRESSION_FOREVER,
      TTLExpressionFactory.create(PhoenixDatabaseMetaData.FOREVER_TTL));
    assertEquals(TTL_EXPRESSION_FOREVER, TTLExpressionFactory.create(HConstants.FOREVER));
  }

  @Test
  public void testTableTTLInTableDescriptor() {
    assertEquals(TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR,
      TTLExpressionFactory.create(PhoenixDatabaseMetaData.TTL_DEFINED_IN_TABLE_DESCRIPTOR));
  }

  @Test
  public void testNone() throws SQLException {
    assertEquals(TTL_EXPRESSION_NOT_DEFINED,
      TTLExpressionFactory.create(PhoenixDatabaseMetaData.NONE_TTL));
    assertEquals(TTL_EXPRESSION_NOT_DEFINED,
      TTLExpressionFactory.create(PhoenixDatabaseMetaData.TTL_NOT_DEFINED));
    assertNull(TTL_EXPRESSION_NOT_DEFINED.serialize());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLiteral() {
    TTLExpressionFactory.create(-10);
  }

  @Test
  public void testConditionalExpression() throws SQLException {
    String ttl = "PK1 = 5 AND COL1 > 'abc'";
    ConditionalTTLExpression expected = new ConditionalTTLExpression(ttl);
    TTLExpression actual = TTLExpressionFactory.create(ttl);
    assertEquals(expected, actual);
    assertEquals(ttl, expected.getTTLExpression());
  }
}
