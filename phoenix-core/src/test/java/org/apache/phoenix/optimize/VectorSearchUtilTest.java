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
package org.apache.phoenix.optimize;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.function.CosineDistanceFunction;
import org.apache.phoenix.expression.function.InnerProductDistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceSquaredFunction;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ArrayConstructorNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

/** Unit tests that find vector search queries and extract their descriptors. */
public class VectorSearchUtilTest extends BaseConnectionlessQueryTest {

  private static boolean isQueryLiteral(VectorSearchDescriptor d) {
    return d.getQueryExpression() instanceof LiteralParseNode
      || d.getQueryExpression() instanceof ArrayConstructorNode;
  }

  private SelectStatement parse(String sql) throws Exception {
    SQLParser parser = new SQLParser(sql);
    return (SelectStatement) parser.parseStatement();
  }

  @Test
  public void testL2DistanceFunctionLiteralArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ARRAY[1.0,0.0,0.0]) ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertEquals(L2DistanceFunction.NAME, d.getDistanceFunctionName());
    assertTrue("Source should be a plain column", d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertTrue("ARRAY literal should be a query literal", isQueryLiteral(d));
    assertFalse(d.getQueryExpression() instanceof BindParseNode);
    assertEquals(Integer.valueOf(10), d.getLimit());
  }

  @Test
  public void testL2DistanceSquaredFunctionBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE_SQUARED(v, ?) ASC LIMIT 5";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertEquals(L2DistanceSquaredFunction.NAME, d.getDistanceFunctionName());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertTrue(d.getQueryExpression() instanceof BindParseNode);
    assertFalse(isQueryLiteral(d));
    assertEquals(Integer.valueOf(5), d.getLimit());
  }

  @Test
  public void testCosineDistanceFunctionBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY COSINE_DISTANCE(v, ?) ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.COSINE, d.getMetric());
    assertEquals(CosineDistanceFunction.NAME, d.getDistanceFunctionName());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertTrue(d.getQueryExpression() instanceof BindParseNode);
    assertEquals(Integer.valueOf(10), d.getLimit());
  }

  @Test
  public void testInnerProductFunctionBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY INNER_PRODUCT(v, ?) ASC LIMIT 25";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.INNER_PRODUCT, d.getMetric());
    assertEquals(InnerProductDistanceFunction.NAME, d.getDistanceFunctionName());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertEquals(Integer.valueOf(25), d.getLimit());
  }

  @Test
  public void testL2OperatorLiteralArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY v <-> ARRAY[1.0,0.0,0.0] ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertEquals(Integer.valueOf(10), d.getLimit());
  }

  @Test
  public void testCosineOperatorBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY v <=> ? ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.COSINE, d.getMetric());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertEquals(Integer.valueOf(10), d.getLimit());
  }

  @Test
  public void testInnerProductOperatorBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY v <#> ? ASC LIMIT 15";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.INNER_PRODUCT, d.getMetric());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertEquals(Integer.valueOf(15), d.getLimit());
  }

  @Test
  public void testSwappedArguments_QueryVectorFirst() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(?, v) ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertEquals("V", d.getSourceColumnName());
    assertTrue(d.getQueryExpression() instanceof BindParseNode);
  }

  @Test
  public void testDefaultSortIsAscending() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull("Default ASC should qualify as vector search", d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertEquals("V", d.getSourceColumnName());
    assertEquals(Integer.valueOf(10), d.getLimit());
  }

  @Test
  public void testDescendingSortReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) DESC LIMIT 10";
    SelectStatement select = parse(sql);

    assertFalse("Descending vector distance order is not a vector search",
      VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testOrdinalResolvesProjectedDistance() throws Exception {
    VectorSearchDescriptor d = VectorSearchUtil
      .getVectorSearchDescriptor(parse("SELECT id, L2_DISTANCE(v, ?) FROM t ORDER BY 2 LIMIT 10"));
    assertNotNull("An ordinal referencing a projected distance is a vector search", d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertEquals("V", d.getSourceColumnName());
    assertEquals(Integer.valueOf(10), d.getLimit());
    assertNull(VectorSearchUtil
      .getVectorSearchDescriptor(parse("SELECT id, L2_DISTANCE(v, ?) FROM t ORDER BY 1 LIMIT 10")));
    assertNull(VectorSearchUtil
      .getVectorSearchDescriptor(parse("SELECT id, L2_DISTANCE(v, ?) FROM t ORDER BY 3 LIMIT 10")));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(
      parse("SELECT id, L2_DISTANCE(v, ?) FROM t ORDER BY 2 DESC LIMIT 10")));
    // The position of an ordinal after a wildcard is known only after the projection expands.
    assertNotNull(VectorSearchUtil.getVectorSearchDescriptor(
      parse("SELECT L2_DISTANCE(v, ?), \"0\".* FROM t ORDER BY 1 LIMIT 10")));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(
      parse("SELECT \"0\".*, L2_DISTANCE(v, ?) FROM t ORDER BY 2 LIMIT 10")));
  }

  private static boolean isNullsLast(PhoenixConnection conn, String sql) throws Exception {
    OrderBy orderBy = conn.prepareStatement(sql)
      .unwrap(org.apache.phoenix.jdbc.PhoenixPreparedStatement.class).optimizeQuery().getOrderBy();
    assertEquals(sql, 1, orderBy.getOrderByExpressions().size());
    return orderBy.getOrderByExpressions().get(0).isNullsLast();
  }

  @Test
  public void testAscendingDistanceDefaultsToNullsLast() throws Exception {
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE t_nulls (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3), ts INTEGER, "
          + "w VECTOR(FLOAT, 3))");
      String q = "ARRAY[1.0, 0.0, 0.0]";
      // Without a NULLS clause, an ascending distance sorts rows without a vector last. This
      // applies to a distance written directly, as an operator, or through an alias or an ordinal.
      // It also applies to a distance in a flattened subquery, through an alias of the subquery,
      // and to a distance group key, directly or through an ordinal.
      for (String sql : new String[] {
        "SELECT pk FROM t_nulls ORDER BY L2_DISTANCE(v, " + q + ") LIMIT 5",
        "SELECT pk FROM t_nulls ORDER BY v <=> " + q + " ASC LIMIT 5",
        "SELECT pk, INNER_PRODUCT(v, " + q + ") AS d FROM t_nulls ORDER BY d LIMIT 5",
        "SELECT pk, L2_DISTANCE_SQUARED(v, " + q + ") FROM t_nulls ORDER BY 2 LIMIT 5",
        "SELECT pk FROM (SELECT pk, v FROM t_nulls) ORDER BY v <-> " + q + " LIMIT 5",
        "SELECT * FROM (SELECT pk, L2_DISTANCE(v, " + q + ") d FROM t_nulls) ORDER BY d LIMIT 5",
        "SELECT L2_DISTANCE(v, w), COUNT(*) FROM t_nulls GROUP BY L2_DISTANCE(v, w) "
          + "ORDER BY L2_DISTANCE(v, w)",
        "SELECT L2_DISTANCE(v, w), COUNT(*) FROM t_nulls GROUP BY L2_DISTANCE(v, w) ORDER BY 1",
        "SELECT pk FROM t_nulls ORDER BY L2_DISTANCE(v, " + q + ") NULLS LAST LIMIT 5" }) {
        assertTrue(sql, isNullsLast(conn, sql));
      }
      // An explicit NULLS FIRST, a descending distance and all other orderings keep nulls first.
      // This includes an ordinal that a wildcard before it moves to another column, here ts.
      for (String sql : new String[] {
        "SELECT pk FROM t_nulls ORDER BY L2_DISTANCE(v, " + q + ") NULLS FIRST LIMIT 5",
        "SELECT pk, L2_DISTANCE(v, " + q + ") AS d FROM t_nulls ORDER BY d NULLS FIRST LIMIT 5",
        "SELECT pk, L2_DISTANCE(v, " + q + ") FROM t_nulls ORDER BY 2 NULLS FIRST LIMIT 5",
        "SELECT * FROM (SELECT pk, L2_DISTANCE(v, " + q
          + ") d FROM t_nulls) ORDER BY d NULLS FIRST LIMIT 5",
        "SELECT pk FROM t_nulls ORDER BY L2_DISTANCE(v, " + q + ") DESC LIMIT 5",
        "SELECT pk FROM t_nulls ORDER BY ts LIMIT 5",
        "SELECT \"0\".*, L2_DISTANCE(v, " + q + ") FROM t_nulls ORDER BY 2 LIMIT 5",
        "SELECT ts, COUNT(*) FROM t_nulls GROUP BY ts ORDER BY 2" }) {
        assertFalse(sql, isNullsLast(conn, sql));
      }
      // SQL that toString() regenerates from a statement with an explicit NULLS FIRST keeps
      // NULLS FIRST.
      String regenerated =
        parse("SELECT pk FROM t_nulls ORDER BY L2_DISTANCE(v, " + q + ") NULLS FIRST LIMIT 5")
          .toString();
      assertFalse(regenerated, isNullsLast(conn, regenerated));
    }
  }

  @Test
  public void testDistanceRewrittenToIndexColumnKeepsNullsLast() throws Exception {
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      String q = "ARRAY[1.0, 0.0, 0.0]";
      conn.createStatement()
        .execute("CREATE TABLE t_fidx (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");
      conn.createStatement().execute("CREATE INDEX i_fidx ON t_fidx (L2_DISTANCE(v, " + q + "))");
      // The functional index rewrites the distance to an index column, whether the query orders
      // by the distance, by an alias, or by an ordinal. The index plan must still sort rows that
      // have no vector last, as the data plan does. The row key order of the index puts them first.
      for (String hint : new String[] { "/*+ INDEX(t_fidx i_fidx) */", "/*+ NO_INDEX */" }) {
        for (String sql : new String[] {
          "SELECT " + hint + " pk FROM t_fidx ORDER BY L2_DISTANCE(v, " + q + ") LIMIT 5",
          "SELECT " + hint + " pk, L2_DISTANCE(v, " + q + ") d FROM t_fidx ORDER BY d LIMIT 5",
          "SELECT " + hint + " pk, L2_DISTANCE(v, " + q + ") FROM t_fidx ORDER BY 2 LIMIT 5" }) {
          org.apache.phoenix.compile.QueryPlan plan = conn.prepareStatement(sql)
            .unwrap(org.apache.phoenix.jdbc.PhoenixPreparedStatement.class).optimizeQuery();
          assertEquals(sql, hint.contains("NO_INDEX") ? "T_FIDX" : "I_FIDX",
            plan.getTableRef().getTable().getTableName().getString());
          assertEquals(sql, 1, plan.getOrderBy().getOrderByExpressions().size());
          assertTrue(sql, plan.getOrderBy().getOrderByExpressions().get(0).isNullsLast());
        }
      }
    }
  }

  @Test
  public void testLimitAsBindVariable() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT ?";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertNull("Bind-variable limit has no parse-time integer value", d.getLimit());
  }

  @Test
  public void testLargeLimit() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 1000000";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(Integer.valueOf(1_000_000), d.getLimit());
  }

  @Test
  public void testMissingLimitReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC";
    SelectStatement select = parse(sql);

    assertFalse("Unbounded distance sort is not a vector search",
      VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testZeroLimitReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 0";
    SelectStatement select = parse(sql);

    assertFalse("LIMIT 0 returns no rows; not a useful vector search",
      VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testTableQualifiedColumn() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(t.v, ?) ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals("V", d.getSourceColumnName());
    assertEquals("T", ((ColumnParseNode) d.getSourceExpression()).getTableName());
    assertNull("No schema qualifier expected",
      ((ColumnParseNode) d.getSourceExpression()).getSchemaName());
  }

  @Test
  public void testSchemaAndTableQualifiedColumn() throws Exception {
    String sql = "SELECT * FROM s.t ORDER BY L2_DISTANCE(s.t.v, ?) ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals("V", d.getSourceColumnName());
    assertEquals("T", ((ColumnParseNode) d.getSourceExpression()).getTableName());
    assertEquals("S", ((ColumnParseNode) d.getSourceExpression()).getSchemaName());
  }

  @Test
  public void testBsonVectorPathExpression() throws Exception {
    String sql =
      "SELECT * FROM t ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'search.embedding', 128), ?)"
        + " ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertFalse("BSON expression is not a bare column", d.isSourceColumn());
    assertNotNull(d.getSourceExpression());
    assertTrue(d.getQueryExpression() instanceof BindParseNode);
    assertEquals(Integer.valueOf(10), d.getLimit());
  }

  @Test
  public void testNoOrderByReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t LIMIT 10";
    SelectStatement select = parse(sql);

    assertFalse(VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testNonDistanceOrderByReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY created_at ASC LIMIT 10";
    SelectStatement select = parse(sql);

    assertFalse(VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testMultipleDistanceExpressionsReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v1, ?), L2_DISTANCE(v2, ?) LIMIT 10";
    SelectStatement select = parse(sql);

    assertFalse("Multiple ORDER BY items disqualify the query",
      VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testDistanceMixedWithOtherOrderByReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?), created_at LIMIT 10";
    SelectStatement select = parse(sql);

    assertFalse(VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testTwoColumnsInDistanceReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v1, v2) ASC LIMIT 10";
    SelectStatement select = parse(sql);

    assertFalse("Column-to-column distance is not a nearest-neighbor search",
      VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testTwoLiteralsInDistanceReturnsFalse() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(ARRAY[1.0], ARRAY[2.0]) ASC LIMIT 10";
    SelectStatement select = parse(sql);

    assertFalse("Both-literal distance has no indexed column to search",
      VectorSearchUtil.isVectorSearchQuery(select));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(select));
  }

  @Test
  public void testNullStatementReturnsFalse() {
    assertFalse(VectorSearchUtil.isVectorSearchQuery(null));
    assertNull(VectorSearchUtil.getVectorSearchDescriptor(null));
  }

  @Test
  public void testDistanceMetricForFunction() {
    assertSame(DistanceMetric.L2, DistanceMetric.forFunction(L2DistanceFunction.NAME));
    assertSame(DistanceMetric.L2, DistanceMetric.forFunction(L2DistanceSquaredFunction.NAME));
    assertSame(DistanceMetric.COSINE, DistanceMetric.forFunction(CosineDistanceFunction.NAME));
    assertSame(DistanceMetric.INNER_PRODUCT,
      DistanceMetric.forFunction(InnerProductDistanceFunction.NAME));
    assertNull(DistanceMetric.forFunction(null));
    assertNull(DistanceMetric.forFunction("MANHATTAN_DISTANCE"));
  }

  @Test
  public void testDistanceMetricFromString() {
    assertSame(DistanceMetric.L2, DistanceMetric.fromString("L2"));
    assertSame(DistanceMetric.L2, DistanceMetric.fromString(" l2 "));
    assertSame(DistanceMetric.COSINE, DistanceMetric.fromString("cosine"));
    assertSame(DistanceMetric.INNER_PRODUCT, DistanceMetric.fromString("Inner_Product"));
    // Null, blank, function names and other unknown names are not metric names.
    for (String other : new String[] { null, "  ", "EUCLIDEAN", "DOT_PRODUCT", "IP", "L2_DISTANCE",
      "HAMMING" }) {
      assertNull("'" + other + "' is not a metric name", DistanceMetric.fromString(other));
    }
  }

  @Test
  public void testHasColumnParseNode_directColumn() throws Exception {
    SelectStatement stmt = parse("SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 1");
    ParseNode distanceNode = stmt.getOrderBy().get(0).getNode();
    ParseNode columnArg = distanceNode.getChildren().get(0);

    assertTrue("Direct ColumnParseNode should be detected", columnArg instanceof ColumnParseNode);
    assertTrue(VectorSearchUtil.hasColumnParseNode(columnArg));
  }

  @Test
  public void testHasColumnParseNode_deeplyNestedColumn() throws Exception {
    SelectStatement stmt = parse(
      "SELECT * FROM t ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'emb', 3), ?) ASC LIMIT 1");
    ParseNode distanceNode = stmt.getOrderBy().get(0).getNode();
    ParseNode bsonArg = distanceNode.getChildren().get(0);

    assertTrue("Nested column inside BSON call must be found recursively",
      VectorSearchUtil.hasColumnParseNode(bsonArg));
  }

  @Test
  public void testHasColumnParseNode_bindAndNullHaveNoColumn() throws Exception {
    SelectStatement stmt = parse("SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 1");
    ParseNode distanceNode = stmt.getOrderBy().get(0).getNode();
    ParseNode bindArg = distanceNode.getChildren().get(1);

    assertFalse("Bind parameter has no column child", VectorSearchUtil.hasColumnParseNode(bindArg));
    assertFalse("null input should return false", VectorSearchUtil.hasColumnParseNode(null));
  }

  @Test
  public void testIsDistanceFunctionNode_distanceFunctions() throws Exception {
    for (String sql : new String[] { "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 1",
      "SELECT * FROM t ORDER BY L2_DISTANCE_SQUARED(v, ?) ASC LIMIT 1",
      "SELECT * FROM t ORDER BY COSINE_DISTANCE(v, ?) ASC LIMIT 1",
      "SELECT * FROM t ORDER BY INNER_PRODUCT(v, ?) ASC LIMIT 1", }) {
      ParseNode node = parse(sql).getOrderBy().get(0).getNode();
      assertTrue("Expected distance node for: " + sql,
        VectorSearchUtil.isDistanceFunctionNode(node));
    }
  }

  @Test
  public void testIsDistanceFunctionNode_distanceOperators() throws Exception {
    for (String sql : new String[] { "SELECT * FROM t ORDER BY v <-> ? ASC LIMIT 1",
      "SELECT * FROM t ORDER BY v <=> ? ASC LIMIT 1",
      "SELECT * FROM t ORDER BY v <#> ? ASC LIMIT 1", }) {
      ParseNode node = parse(sql).getOrderBy().get(0).getNode();
      assertTrue("Expected distance operator node for: " + sql,
        VectorSearchUtil.isDistanceFunctionNode(node));
    }
  }

  @Test
  public void testIsDistanceFunctionNode_nonDistanceAndNull() throws Exception {
    ParseNode colNode =
      parse("SELECT * FROM t ORDER BY v ASC LIMIT 1").getOrderBy().get(0).getNode();
    assertFalse("Plain column is not a distance function",
      VectorSearchUtil.isDistanceFunctionNode(colNode));
    assertFalse("null should return false", VectorSearchUtil.isDistanceFunctionNode(null));
  }

  @Test
  public void testCompiledIsVectorSearch_emptyAndNullOrderByReturnsFalse() {
    assertFalse(VectorSearchUtil.isVectorSearch(OrderBy.EMPTY_ORDER_BY, 10));
    assertFalse(VectorSearchUtil.isVectorSearch((OrderBy) null, 10));
  }

  @Test
  public void testCompiledIsVectorSearch_nullOrZeroLimitReturnsFalse() throws Exception {
    // Two vector columns give both operands a type at compile time without bind values.
    String ddl = "CREATE TABLE t_compiled"
      + " (pk INTEGER PRIMARY KEY, v1 VECTOR(FLOAT, 3), v2 VECTOR(FLOAT, 3))";
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);

      String sql = "SELECT pk FROM t_compiled ORDER BY L2_DISTANCE(v1, v2) ASC LIMIT 5";
      SelectStatement select = parse(sql);
      PhoenixStatement stmt = new PhoenixStatement(conn);
      ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn);
      StatementContext context = new StatementContext(stmt, resolver);

      OrderBy orderBy = OrderByCompiler.compile(context, select, GroupBy.EMPTY_GROUP_BY, 5,
        org.apache.phoenix.compile.CompiledOffset.EMPTY_COMPILED_OFFSET,
        RowProjector.EMPTY_PROJECTOR, null, null);

      assertTrue("Compiled OrderBy with LIMIT 5 should be a vector search",
        VectorSearchUtil.isVectorSearch(orderBy, 5));

      assertFalse("null limit must be rejected", VectorSearchUtil.isVectorSearch(orderBy, null));
      assertFalse("zero limit must be rejected", VectorSearchUtil.isVectorSearch(orderBy, 0));
    }
  }

  @Test
  public void testCompiledIsVectorSearch_allDistanceFunctions() throws Exception {
    // Two vector columns let each distance function compile without a literal query vector.
    String ddl = "CREATE TABLE t_all_dist"
      + " (pk INTEGER PRIMARY KEY, v1 VECTOR(FLOAT, 3), v2 VECTOR(FLOAT, 3))";
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);

      String[] sqls = { "SELECT pk FROM t_all_dist ORDER BY L2_DISTANCE(v1, v2) ASC LIMIT 3",
        "SELECT pk FROM t_all_dist ORDER BY L2_DISTANCE_SQUARED(v1, v2) ASC LIMIT 3",
        "SELECT pk FROM t_all_dist ORDER BY COSINE_DISTANCE(v1, v2) ASC LIMIT 3",
        "SELECT pk FROM t_all_dist ORDER BY INNER_PRODUCT(v1, v2) ASC LIMIT 3", };

      for (String sql : sqls) {
        SelectStatement select = parse(sql);
        PhoenixStatement stmt = new PhoenixStatement(conn);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn);
        StatementContext context = new StatementContext(stmt, resolver);

        OrderBy orderBy = OrderByCompiler.compile(context, select, GroupBy.EMPTY_GROUP_BY, 3,
          org.apache.phoenix.compile.CompiledOffset.EMPTY_COMPILED_OFFSET,
          RowProjector.EMPTY_PROJECTOR, null, null);

        assertTrue("VectorSearchUtil.isVectorSearch for: " + sql,
          VectorSearchUtil.isVectorSearch(orderBy, 3));
      }
    }
  }

  @Test
  public void testCompiledIsVectorSearch_nonDistanceOrderByReturnsFalse() throws Exception {
    String ddl = "CREATE TABLE t_no_dist (pk INTEGER PRIMARY KEY, ts INTEGER)";
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);

      String sql = "SELECT pk FROM t_no_dist ORDER BY ts ASC LIMIT 10";
      SelectStatement select = parse(sql);
      PhoenixStatement stmt = new PhoenixStatement(conn);
      ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn);
      StatementContext context = new StatementContext(stmt, resolver);

      OrderBy orderBy = OrderByCompiler.compile(context, select, GroupBy.EMPTY_GROUP_BY, 10,
        org.apache.phoenix.compile.CompiledOffset.EMPTY_COMPILED_OFFSET,
        RowProjector.EMPTY_PROJECTOR, null, null);

      assertFalse("Non-distance ORDER BY must not be detected as vector search",
        VectorSearchUtil.isVectorSearch(orderBy, 10));
    }
  }

  @Test
  public void testGetDistanceMetricFromOrderBy() throws Exception {
    String ddl =
      "CREATE TABLE t_metric_dist (pk INTEGER PRIMARY KEY, v1 VECTOR(FLOAT, 3), v2 VECTOR(FLOAT, 3), ts INTEGER)";
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);

      // A null or empty ORDER BY has no metric
      assertNull(VectorSearchUtil.getDistanceMetric(null));
      assertNull(VectorSearchUtil.getDistanceMetric(OrderBy.EMPTY_ORDER_BY));

      // Each supported distance function gives its metric
      String[] sqls = { "SELECT pk FROM t_metric_dist ORDER BY L2_DISTANCE(v1, v2) LIMIT 5",
        "SELECT pk FROM t_metric_dist ORDER BY L2_DISTANCE_SQUARED(v1, v2) LIMIT 5",
        "SELECT pk FROM t_metric_dist ORDER BY COSINE_DISTANCE(v1, v2) LIMIT 5",
        "SELECT pk FROM t_metric_dist ORDER BY INNER_PRODUCT(v1, v2) LIMIT 5" };
      DistanceMetric[] expected = { DistanceMetric.L2, DistanceMetric.L2, DistanceMetric.COSINE,
        DistanceMetric.INNER_PRODUCT };

      for (int i = 0; i < sqls.length; i++) {
        SelectStatement select = parse(sqls[i]);
        PhoenixStatement stmt = new PhoenixStatement(conn);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn);
        StatementContext context = new StatementContext(stmt, resolver);
        OrderBy orderBy = OrderByCompiler.compile(context, select, GroupBy.EMPTY_GROUP_BY, 5,
          org.apache.phoenix.compile.CompiledOffset.EMPTY_COMPILED_OFFSET,
          RowProjector.EMPTY_PROJECTOR, null, null);
        assertEquals("Metric mismatch for " + sqls[i], expected[i],
          VectorSearchUtil.getDistanceMetric(orderBy));
      }

      // An ORDER BY expression that is not a distance has no metric
      SelectStatement nonDistSelect = parse("SELECT pk FROM t_metric_dist ORDER BY ts LIMIT 5");
      PhoenixStatement stmt1 = new PhoenixStatement(conn);
      ColumnResolver resolver1 = FromCompiler.getResolverForQuery(nonDistSelect, conn);
      StatementContext context1 = new StatementContext(stmt1, resolver1);
      OrderBy nonDistOrderBy = OrderByCompiler.compile(context1, nonDistSelect,
        GroupBy.EMPTY_GROUP_BY, 5, org.apache.phoenix.compile.CompiledOffset.EMPTY_COMPILED_OFFSET,
        RowProjector.EMPTY_PROJECTOR, null, null);
      assertNull("Non-distance expression must return null metric",
        VectorSearchUtil.getDistanceMetric(nonDistOrderBy));

      // An ORDER BY with more than one expression has no metric
      SelectStatement multiSelect =
        parse("SELECT pk FROM t_metric_dist ORDER BY L2_DISTANCE(v1, v2), ts LIMIT 5");
      PhoenixStatement stmt2 = new PhoenixStatement(conn);
      ColumnResolver resolver2 = FromCompiler.getResolverForQuery(multiSelect, conn);
      StatementContext context2 = new StatementContext(stmt2, resolver2);
      OrderBy multiOrderBy = OrderByCompiler.compile(context2, multiSelect, GroupBy.EMPTY_GROUP_BY,
        5, org.apache.phoenix.compile.CompiledOffset.EMPTY_COMPILED_OFFSET,
        RowProjector.EMPTY_PROJECTOR, null, null);
      assertNull("Multiple order by expressions must return null metric",
        VectorSearchUtil.getDistanceMetric(multiOrderBy));
    }
  }

}
