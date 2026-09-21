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
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

/** Tests parsing, static query analysis, and descriptor extraction for vector search. */
public class VectorSearchUtilTest extends BaseConnectionlessQueryTest {

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
    assertEquals(DistanceFunctionType.L2_DISTANCE, d.getDistanceFunctionType());
    assertEquals(L2DistanceFunction.NAME, d.getDistanceFunctionName());
    assertTrue("Source should be a plain column", d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertTrue("ARRAY literal should be a query literal", d.isQueryLiteral());
    assertFalse(d.isQueryBindVariable());
    assertEquals(Integer.valueOf(10), d.getLimit());
    assertTrue(d.hasLimit());
  }

  @Test
  public void testL2DistanceSquaredFunctionBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE_SQUARED(v, ?) ASC LIMIT 5";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.L2, d.getMetric());
    assertEquals(DistanceFunctionType.L2_DISTANCE_SQUARED, d.getDistanceFunctionType());
    assertEquals(L2DistanceSquaredFunction.NAME, d.getDistanceFunctionName());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertTrue(d.isQueryBindVariable());
    assertFalse(d.isQueryLiteral());
    assertEquals(Integer.valueOf(5), d.getLimit());
  }

  @Test
  public void testCosineDistanceFunctionBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY COSINE_DISTANCE(v, ?) ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.COSINE, d.getMetric());
    assertEquals(DistanceFunctionType.COSINE_DISTANCE, d.getDistanceFunctionType());
    assertEquals(CosineDistanceFunction.NAME, d.getDistanceFunctionName());
    assertTrue(d.isSourceColumn());
    assertEquals("V", d.getSourceColumnName());
    assertTrue(d.isQueryBindVariable());
    assertEquals(Integer.valueOf(10), d.getLimit());
  }

  @Test
  public void testInnerProductFunctionBindArg() throws Exception {
    String sql = "SELECT * FROM t ORDER BY INNER_PRODUCT(v, ?) ASC LIMIT 25";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals(DistanceMetric.INNER_PRODUCT, d.getMetric());
    assertEquals(DistanceFunctionType.INNER_PRODUCT, d.getDistanceFunctionType());
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
    assertTrue(d.isQueryBindVariable());
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
  public void testLimitAsBindVariable() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT ?";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertTrue("LimitNode must be present even when value is a bind variable", d.hasLimit());
    assertNull("Bind-variable limit has no parse-time integer value", d.getLimit());
    assertNotNull(d.getLimitNode());
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
    assertEquals("T", d.getTableName());
    assertNull("No schema qualifier expected", d.getSchemaName());
  }

  @Test
  public void testSchemaAndTableQualifiedColumn() throws Exception {
    String sql = "SELECT * FROM s.t ORDER BY L2_DISTANCE(s.t.v, ?) ASC LIMIT 10";
    VectorSearchDescriptor d = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d);
    assertEquals("V", d.getSourceColumnName());
    assertEquals("T", d.getTableName());
    assertEquals("S", d.getSchemaName());
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
    assertTrue(d.isQueryBindVariable());
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
  public void testDistanceFunctionTypeFromFunctionName_allFunctions() {
    assertSame(DistanceFunctionType.L2_DISTANCE,
      DistanceFunctionType.fromFunctionName(L2DistanceFunction.NAME));
    assertSame(DistanceFunctionType.L2_DISTANCE_SQUARED,
      DistanceFunctionType.fromFunctionName(L2DistanceSquaredFunction.NAME));
    assertSame(DistanceFunctionType.COSINE_DISTANCE,
      DistanceFunctionType.fromFunctionName(CosineDistanceFunction.NAME));
    assertSame(DistanceFunctionType.INNER_PRODUCT,
      DistanceFunctionType.fromFunctionName(InnerProductDistanceFunction.NAME));
  }

  @Test
  public void testDistanceFunctionTypeFromFunctionName_caseInsensitive() {
    assertSame(DistanceFunctionType.L2_DISTANCE,
      DistanceFunctionType.fromFunctionName("l2_distance"));
    assertSame(DistanceFunctionType.COSINE_DISTANCE,
      DistanceFunctionType.fromFunctionName("Cosine_Distance"));
    assertSame(DistanceFunctionType.INNER_PRODUCT,
      DistanceFunctionType.fromFunctionName("inner_product"));
  }

  @Test
  public void testDistanceFunctionTypeFromFunctionName_nullAndUnknown() {
    assertNull("null input should return null", DistanceFunctionType.fromFunctionName(null));
    assertNull("unknown name should return null",
      DistanceFunctionType.fromFunctionName("MANHATTAN_DISTANCE"));
    assertNull("empty-after-trim name should return null",
      DistanceFunctionType.fromFunctionName("   "));
  }

  @Test
  public void testDistanceFunctionTypeMetricMapping() {
    assertEquals(DistanceMetric.L2, DistanceFunctionType.L2_DISTANCE.getMetric());
    assertEquals(DistanceMetric.L2, DistanceFunctionType.L2_DISTANCE_SQUARED.getMetric());
    assertEquals(DistanceMetric.COSINE, DistanceFunctionType.COSINE_DISTANCE.getMetric());
    assertEquals(DistanceMetric.INNER_PRODUCT, DistanceFunctionType.INNER_PRODUCT.getMetric());
  }

  @Test
  public void testDistanceMetricFromString_L2aliases() {
    for (String alias : new String[] { "L2", "L2_DISTANCE", "L2_DISTANCE_SQUARED", "EUCLIDEAN",
      "l2", "Euclidean" }) {
      assertEquals("Alias '" + alias + "' should resolve to L2", DistanceMetric.L2,
        DistanceMetric.fromString(alias));
    }
  }

  @Test
  public void testDistanceMetricFromString_cosineAliases() {
    for (String alias : new String[] { "COSINE", "COSINE_DISTANCE", "cosine_distance" }) {
      assertEquals("Alias '" + alias + "' should resolve to COSINE", DistanceMetric.COSINE,
        DistanceMetric.fromString(alias));
    }
  }

  @Test
  public void testDistanceMetricFromString_innerProductAliases() {
    for (String alias : new String[] { "INNER_PRODUCT", "INNER_PRODUCT_DISTANCE", "DOT_PRODUCT",
      "IP", "dot_product", "ip" }) {
      assertEquals("Alias '" + alias + "' should resolve to INNER_PRODUCT",
        DistanceMetric.INNER_PRODUCT, DistanceMetric.fromString(alias));
    }
  }

  @Test
  public void testDistanceMetricFromString_nullAndUnknown() {
    assertNull("null input should return null", DistanceMetric.fromString(null));
    assertNull("whitespace-only should return null", DistanceMetric.fromString("  "));
    assertNull("unknown metric name should return null",
      DistanceMetric.fromString("HAMMING_DISTANCE"));
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
  public void testDescriptorEquality_sameQuery() throws Exception {
    String sql = "SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 10";
    VectorSearchDescriptor d1 = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));
    VectorSearchDescriptor d2 = VectorSearchUtil.getVectorSearchDescriptor(parse(sql));

    assertNotNull(d1);
    assertNotNull(d2);
    assertEquals("Descriptors from identical SQL must be equal", d1, d2);
    assertEquals("Equal descriptors must have identical hash codes", d1.hashCode(), d2.hashCode());
  }

  @Test
  public void testDescriptorInequality_differentMetric() throws Exception {
    VectorSearchDescriptor dL2 = VectorSearchUtil
      .getVectorSearchDescriptor(parse("SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 10"));
    VectorSearchDescriptor dCosine = VectorSearchUtil.getVectorSearchDescriptor(
      parse("SELECT * FROM t ORDER BY COSINE_DISTANCE(v, ?) ASC LIMIT 10"));

    assertNotNull(dL2);
    assertNotNull(dCosine);
    assertFalse("Descriptors with different metrics must not be equal", dL2.equals(dCosine));
  }

  @Test
  public void testDescriptorInequality_differentLimit() throws Exception {
    VectorSearchDescriptor d10 = VectorSearchUtil
      .getVectorSearchDescriptor(parse("SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 10"));
    VectorSearchDescriptor d20 = VectorSearchUtil
      .getVectorSearchDescriptor(parse("SELECT * FROM t ORDER BY L2_DISTANCE(v, ?) ASC LIMIT 20"));

    assertNotNull(d10);
    assertNotNull(d20);
    assertFalse("Descriptors with different limits must not be equal", d10.equals(d20));
  }

  @Test
  public void testCompiledIsVectorSearch_emptyAndNullOrderByReturnsFalse() {
    assertFalse(VectorSearchUtil.isVectorSearch(OrderBy.EMPTY_ORDER_BY, 10));
    assertFalse(VectorSearchUtil.isVectorSearch(null, 10));
  }

  @Test
  public void testCompiledIsVectorSearch_nullOrZeroLimitReturnsFalse() throws Exception {
    // Use two vector columns so type resolution succeeds without execution bind context.
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
      assertTrue(orderBy.isVectorDistanceOrder());
      assertTrue(orderBy.isVectorSearch(5));

      assertFalse("null limit must be rejected", VectorSearchUtil.isVectorSearch(orderBy, null));
      assertFalse("zero limit must be rejected", VectorSearchUtil.isVectorSearch(orderBy, 0));
      assertFalse(orderBy.isVectorSearch(null));
      assertFalse(orderBy.isVectorSearch(0));
    }
  }

  @Test
  public void testCompiledIsVectorSearch_allDistanceFunctions() throws Exception {
    // Use resolved vector columns to avoid untyped literal array coercion during compilation.
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

        assertTrue("isVectorDistanceOrder() for: " + sql, orderBy.isVectorDistanceOrder());
        assertTrue("isVectorSearch(3) for: " + sql, orderBy.isVectorSearch(3));
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
        orderBy.isVectorDistanceOrder());
      assertFalse(VectorSearchUtil.isVectorSearch(orderBy, 10));
    }
  }

  @Test
  public void testGetDistanceMetricFromOrderBy() throws Exception {
    String ddl =
      "CREATE TABLE t_metric_dist (pk INTEGER PRIMARY KEY, v1 VECTOR(FLOAT, 3), v2 VECTOR(FLOAT, 3), ts INTEGER)";
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);

      // Null / empty
      assertNull(VectorSearchUtil.getDistanceMetric(null));
      assertNull(VectorSearchUtil.getDistanceMetric(OrderBy.EMPTY_ORDER_BY));

      // Each of the 4 functions
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

      // Non-distance order by
      SelectStatement nonDistSelect = parse("SELECT pk FROM t_metric_dist ORDER BY ts LIMIT 5");
      PhoenixStatement stmt1 = new PhoenixStatement(conn);
      ColumnResolver resolver1 = FromCompiler.getResolverForQuery(nonDistSelect, conn);
      StatementContext context1 = new StatementContext(stmt1, resolver1);
      OrderBy nonDistOrderBy = OrderByCompiler.compile(context1, nonDistSelect,
        GroupBy.EMPTY_GROUP_BY, 5, org.apache.phoenix.compile.CompiledOffset.EMPTY_COMPILED_OFFSET,
        RowProjector.EMPTY_PROJECTOR, null, null);
      assertNull("Non-distance expression must return null metric",
        VectorSearchUtil.getDistanceMetric(nonDistOrderBy));

      // Multiple order by expressions
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

  @Test
  public void testIsMetricCompatible() {
    // 3x3 combinations
    assertTrue(VectorSearchUtil.isMetricCompatible(DistanceMetric.L2, "L2"));
    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.L2, "COSINE"));
    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.L2, "INNER_PRODUCT"));

    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.COSINE, "L2"));
    assertTrue(VectorSearchUtil.isMetricCompatible(DistanceMetric.COSINE, "COSINE"));
    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.COSINE, "INNER_PRODUCT"));

    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.INNER_PRODUCT, "L2"));
    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.INNER_PRODUCT, "COSINE"));
    assertTrue(VectorSearchUtil.isMetricCompatible(DistanceMetric.INNER_PRODUCT, "INNER_PRODUCT"));

    // Whitespace and case tolerance
    assertTrue(VectorSearchUtil.isMetricCompatible(DistanceMetric.L2, " l2 "));
    assertTrue(VectorSearchUtil.isMetricCompatible(DistanceMetric.COSINE, " cosine\t"));
    assertTrue(VectorSearchUtil.isMetricCompatible(DistanceMetric.INNER_PRODUCT, "Inner_Product "));

    // Null checks
    assertFalse(VectorSearchUtil.isMetricCompatible(null, "L2"));
    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.L2, null));
    assertFalse(VectorSearchUtil.isMetricCompatible(null, null));
    assertFalse(VectorSearchUtil.isMetricCompatible(DistanceMetric.L2, "UNKNOWN"));
  }

  @Test
  public void testIsColumnCoveredAndGetDataColumn() throws Exception {
    String dataTableDdl = "CREATE TABLE T_COVERED_TEST (" + "ID VARCHAR NOT NULL PRIMARY KEY, "
      + "V1 VECTOR(FLOAT, 3), " + "V2 VECTOR(FLOAT, 3), " + "CATEGORY VARCHAR, "
      + "UNCOVERED_COL VARCHAR)";
    String indexDdl =
      "CREATE VECTOR INDEX IDX_COVERED_TEST ON T_COVERED_TEST (V1) INCLUDE (CATEGORY) "
        + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)";

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(dataTableDdl);
      conn.createStatement().execute(indexDdl);

      org.apache.phoenix.schema.PTable dataTable = conn.getTableNoCache("T_COVERED_TEST");
      org.apache.phoenix.schema.PTable indexTable = conn.getTableNoCache("IDX_COVERED_TEST");

      org.apache.phoenix.schema.PColumn pkCol = dataTable.getColumnForColumnName("ID");
      org.apache.phoenix.schema.PColumn v1Col = dataTable.getColumnForColumnName("V1");
      org.apache.phoenix.schema.PColumn v2Col = dataTable.getColumnForColumnName("V2");
      org.apache.phoenix.schema.PColumn catCol = dataTable.getColumnForColumnName("CATEGORY");
      org.apache.phoenix.schema.PColumn uncCol = dataTable.getColumnForColumnName("UNCOVERED_COL");

      // Primary key column is covered by index row key
      assertTrue("PK column must be covered",
        VectorSearchUtil.isColumnCovered(indexTable, dataTable, pkCol));

      // Indexed vector column is covered
      assertTrue("Indexed vector V1 must be covered",
        VectorSearchUtil.isColumnCovered(indexTable, dataTable, v1Col));

      // Included column is covered
      assertTrue("Included column CATEGORY must be covered",
        VectorSearchUtil.isColumnCovered(indexTable, dataTable, catCol));

      // Uncovered data column is not covered
      assertFalse("UNCOVERED_COL must not be covered",
        VectorSearchUtil.isColumnCovered(indexTable, dataTable, uncCol));

      // Vector column not included in index definition is not covered
      assertFalse("Second vector column V2 not included must not be covered",
        VectorSearchUtil.isColumnCovered(indexTable, dataTable, v2Col));

      // Null handling
      assertFalse(VectorSearchUtil.isColumnCovered(null, dataTable, pkCol));
      assertFalse(VectorSearchUtil.isColumnCovered(indexTable, dataTable, null));

      // getDataColumn tests
      assertEquals(catCol, VectorSearchUtil.getDataColumn(dataTable, "CATEGORY"));
      assertEquals(catCol, VectorSearchUtil.getDataColumn(dataTable, "0:CATEGORY"));
      assertEquals(pkCol, VectorSearchUtil.getDataColumn(dataTable, ":ID"));
      assertEquals(pkCol, VectorSearchUtil.getDataColumn(dataTable, "ID"));
      assertNull(VectorSearchUtil.getDataColumn(dataTable, "NOPE"));
      assertNull(VectorSearchUtil.getDataColumn(dataTable, null));
      assertNull(VectorSearchUtil.getDataColumn(null, "CATEGORY"));
    }
  }

  @Test
  public void testHasUncoveredFilterAndProjectionColumns() throws Exception {
    String dataTableDdl = "CREATE TABLE T_UNCOV_FILTER_TEST (" + "ID VARCHAR NOT NULL PRIMARY KEY, "
      + "V1 VECTOR(FLOAT, 3), " + "CATEGORY VARCHAR, " + "DESCRIPTION VARCHAR)";
    String indexDdl =
      "CREATE VECTOR INDEX IDX_UNCOV_FILTER_TEST ON T_UNCOV_FILTER_TEST (V1) INCLUDE (CATEGORY) "
        + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)";

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(dataTableDdl);
      conn.createStatement().execute(indexDdl);

      org.apache.phoenix.schema.PTable dataTable = conn.getTableNoCache("T_UNCOV_FILTER_TEST");
      org.apache.phoenix.schema.PTable indexTable = conn.getTableNoCache("IDX_UNCOV_FILTER_TEST");

      // Filter referencing only covered columns does not require data table scan
      SelectStatement s1 = parse("SELECT ID FROM T_UNCOV_FILTER_TEST WHERE CATEGORY = 'A' LIMIT 5");
      assertFalse(VectorSearchUtil.hasUncoveredFilterColumns(indexTable, dataTable, s1));

      // Filter referencing an uncovered column requires data table access
      SelectStatement s2 =
        parse("SELECT ID FROM T_UNCOV_FILTER_TEST WHERE DESCRIPTION = 'B' LIMIT 5");
      assertTrue(VectorSearchUtil.hasUncoveredFilterColumns(indexTable, dataTable, s2));

      // Conjunction containing both covered and uncovered column references
      SelectStatement s3 = parse(
        "SELECT ID FROM T_UNCOV_FILTER_TEST WHERE CATEGORY = 'A' AND DESCRIPTION = 'B' LIMIT 5");
      assertTrue(VectorSearchUtil.hasUncoveredFilterColumns(indexTable, dataTable, s3));

      // Filter containing constant expressions without column references
      SelectStatement s4 = parse("SELECT ID FROM T_UNCOV_FILTER_TEST WHERE 1 = 1 LIMIT 5");
      assertFalse(VectorSearchUtil.hasUncoveredFilterColumns(indexTable, dataTable, s4));

      // Query without a WHERE clause
      SelectStatement s5 = parse("SELECT ID FROM T_UNCOV_FILTER_TEST LIMIT 5");
      assertFalse(VectorSearchUtil.hasUncoveredFilterColumns(indexTable, dataTable, s5));

      // Projection referencing only covered columns
      SelectStatement p1 = parse("SELECT ID, CATEGORY FROM T_UNCOV_FILTER_TEST LIMIT 5");
      assertFalse(VectorSearchUtil.hasUncoveredProjectionColumns(indexTable, dataTable, p1));

      // Projection referencing an uncovered column
      SelectStatement p2 = parse("SELECT ID, DESCRIPTION FROM T_UNCOV_FILTER_TEST LIMIT 5");
      assertTrue(VectorSearchUtil.hasUncoveredProjectionColumns(indexTable, dataTable, p2));

      // Wildcard projection over entire data table
      SelectStatement p3 = parse("SELECT * FROM T_UNCOV_FILTER_TEST LIMIT 5");
      assertTrue(VectorSearchUtil.hasUncoveredProjectionColumns(indexTable, dataTable, p3));

      // Table-qualified wildcard projection
      SelectStatement p4 = parse("SELECT t.* FROM T_UNCOV_FILTER_TEST t LIMIT 5");
      assertTrue(VectorSearchUtil.hasUncoveredProjectionColumns(indexTable, dataTable, p4));

      // Expression evaluating uncovered column
      SelectStatement p5 = parse("SELECT UPPER(DESCRIPTION) FROM T_UNCOV_FILTER_TEST LIMIT 5");
      assertTrue(VectorSearchUtil.hasUncoveredProjectionColumns(indexTable, dataTable, p5));

      // Expression evaluating covered column
      SelectStatement p6 = parse("SELECT UPPER(CATEGORY) FROM T_UNCOV_FILTER_TEST LIMIT 5");
      assertFalse(VectorSearchUtil.hasUncoveredProjectionColumns(indexTable, dataTable, p6));
    }
  }
}
