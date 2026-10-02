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
package org.apache.phoenix.expression.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.BeforeClass;
import org.junit.Test;

public class BsonValueFunctionTest extends BaseConnectionlessQueryTest {

  private static final String TABLE = "t_bson_value_args";

  @BeforeClass
  public static synchronized void createTable() throws Exception {
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + TABLE + " (pk VARCHAR NOT NULL PRIMARY KEY, doc BSON)");
    }
  }

  private static BsonValueFunction compile(String projection) throws Exception {
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      SelectStatement select =
        new SQLParser("SELECT " + projection + " FROM " + TABLE).parseQuery();
      ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn);
      StatementContext context = new StatementContext(new PhoenixStatement(conn), resolver);
      Expression expression =
        select.getSelect().get(0).getNode().accept(new ExpressionCompiler(context));
      assertTrue("Expected a BSON_VALUE expression but got " + expression.getClass(),
        expression instanceof BsonValueFunction);
      return (BsonValueFunction) expression;
    }
  }

  private static Object literalValue(Expression argument) {
    assertTrue("Expected a literal argument but got " + argument.getClass(),
      argument instanceof LiteralExpression);
    return ((LiteralExpression) argument).getValue();
  }

  @Test
  public void testTypeArgumentDefaultsToVarchar() throws Exception {
    BsonValueFunction function = compile("BSON_VALUE(doc, 'category')");
    assertEquals(PVarchar.INSTANCE, function.getDataType());
    assertEquals("VARCHAR", literalValue(function.getChildren().get(2)));
    assertNull(literalValue(function.getChildren().get(3)));
  }

  @Test
  public void testExplicitTypeArgumentStillHonoured() throws Exception {
    BsonValueFunction function = compile("BSON_VALUE(doc, 'rating', 'INTEGER')");
    assertEquals(PInteger.INSTANCE, function.getDataType());
    assertEquals("INTEGER", literalValue(function.getChildren().get(2)));
    assertNull(literalValue(function.getChildren().get(3)));
  }

  @Test
  public void testExplicitTypeAndAbsentFieldDefault() throws Exception {
    BsonValueFunction function = compile("BSON_VALUE(doc, 'category', 'VARCHAR', 'unknown')");
    assertEquals(PVarchar.INSTANCE, function.getDataType());
    assertEquals("unknown", literalValue(function.getChildren().get(3)));
  }
}
