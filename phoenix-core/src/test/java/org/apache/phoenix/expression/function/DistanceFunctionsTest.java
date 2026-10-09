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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.DistanceFunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

public class DistanceFunctionsTest extends BaseConnectionlessQueryTest {

  private static final double DELTA = 1e-6;

  private static <T extends DistanceFunction> T withBound(T function, double bound) {
    function.setDistanceUpperBound(bound);
    return function;
  }

  private static BuiltInFunctionInfo lookup(String name) {
    Collection<BuiltInFunctionInfo> infos = ParseNodeFactory.getBuiltInFunctionMultimap().get(name);
    return infos.isEmpty() ? null : infos.iterator().next();
  }

  private static void assertDistanceNode(String name, ParseNode node) {
    assertTrue("Expected DistanceFunctionParseNode, got " + node.getClass().getSimpleName(),
      node instanceof DistanceFunctionParseNode);
    assertEquals(name, ((FunctionParseNode) node).getName());
  }

  private static double evaluateDistance(DistanceFunction function) {
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    boolean evaluated = function.evaluate(null, ptr);
    assertTrue("Distance function should evaluate successfully", evaluated);
    assertNotNull("Pointer should not be null", ptr.get());
    Object obj = PDouble.INSTANCE.toObject(ptr);
    assertNotNull("Decoded distance should not be null", obj);
    return ((Number) obj).doubleValue();
  }

  @Test
  public void testL2DistanceCorrectness() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE);
    L2DistanceFunction func = new L2DistanceFunction(Arrays.asList(v1, v2));

    double dist = evaluateDistance(func);
    assertEquals(5.0, dist, DELTA);
  }

  @Test
  public void testL2DistanceSquaredCorrectness() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE);
    L2DistanceSquaredFunction func = new L2DistanceSquaredFunction(Arrays.asList(v1, v2));

    double dist = evaluateDistance(func);
    assertEquals(25.0, dist, DELTA);
  }

  @Test
  public void testCosineDistanceOrthogonalVectors() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 1.0f, 0.0f }, PVectorFloat.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 1.0f }, PVectorFloat.INSTANCE);
    CosineDistanceFunction func = new CosineDistanceFunction(Arrays.asList(v1, v2));

    double dist = evaluateDistance(func);
    assertEquals(1.0, dist, DELTA);
  }

  @Test
  public void testCosineDistanceIdenticalAndOpposite() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f, 3.0f }, PVectorFloat.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f, 3.0f }, PVectorFloat.INSTANCE);
    CosineDistanceFunction funcIdentical = new CosineDistanceFunction(Arrays.asList(v1, v2));
    assertEquals(0.0, evaluateDistance(funcIdentical), DELTA);

    Expression vOpposite =
      LiteralExpression.newConstant(new float[] { -1.0f, -2.0f, -3.0f }, PVectorFloat.INSTANCE);
    CosineDistanceFunction funcOpposite = new CosineDistanceFunction(Arrays.asList(v1, vOpposite));
    assertEquals(2.0, evaluateDistance(funcOpposite), DELTA);
  }

  @Test
  public void testCosineDistanceZeroVector() throws Exception {
    Expression vZero =
      LiteralExpression.newConstant(new float[] { 0.0f, 0.0f }, PVectorFloat.INSTANCE);
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 1.0f, 0.0f }, PVectorFloat.INSTANCE);
    CosineDistanceFunction func = new CosineDistanceFunction(Arrays.asList(vZero, v1));
    assertEquals(1.0, evaluateDistance(func), DELTA);
  }

  @Test
  public void testInnerProductDistance() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 2.0f, 3.0f }, PVectorFloat.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 4.0f, 5.0f }, PVectorFloat.INSTANCE);
    InnerProductDistanceFunction func = new InnerProductDistanceFunction(Arrays.asList(v1, v2));

    double dist = evaluateDistance(func);
    assertEquals(-23.0, dist, DELTA);
  }

  @Test
  public void testVectorDoubleSupport() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new double[] { 3.0, 0.0 }, PVectorDouble.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new double[] { 0.0, 4.0 }, PVectorDouble.INSTANCE);

    L2DistanceFunction l2 = new L2DistanceFunction(Arrays.asList(v1, v2));
    assertEquals(5.0, evaluateDistance(l2), DELTA);

    L2DistanceSquaredFunction l2Sq = new L2DistanceSquaredFunction(Arrays.asList(v1, v2));
    assertEquals(25.0, evaluateDistance(l2Sq), DELTA);

    Expression d1 =
      LiteralExpression.newConstant(new double[] { 2.0, 3.0 }, PVectorDouble.INSTANCE);
    Expression d2 =
      LiteralExpression.newConstant(new double[] { 4.0, 5.0 }, PVectorDouble.INSTANCE);
    InnerProductDistanceFunction ip = new InnerProductDistanceFunction(Arrays.asList(d1, d2));
    assertEquals(-23.0, evaluateDistance(ip), DELTA);

    Expression o1 =
      LiteralExpression.newConstant(new double[] { 1.0, 0.0 }, PVectorDouble.INSTANCE);
    Expression o2 =
      LiteralExpression.newConstant(new double[] { 0.0, 1.0 }, PVectorDouble.INSTANCE);
    CosineDistanceFunction cosine = new CosineDistanceFunction(Arrays.asList(o1, o2));
    assertEquals(1.0, evaluateDistance(cosine), DELTA);
  }

  @Test
  public void testDescendingSortOrder() throws Exception {
    Expression v1 = LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE,
      SortOrder.DESC);
    Expression v2 = LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE,
      SortOrder.DESC);

    L2DistanceFunction l2 = new L2DistanceFunction(Arrays.asList(v1, v2));
    assertEquals(5.0, evaluateDistance(l2), DELTA);

    Expression v3 = LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE,
      SortOrder.ASC);
    L2DistanceFunction l2Mixed = new L2DistanceFunction(Arrays.asList(v1, v3));
    assertEquals(5.0, evaluateDistance(l2Mixed), DELTA);
  }

  @Test
  public void testDimensionMismatchCompileTime() throws Exception {
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f }, PVectorFloat.INSTANCE);
    Expression v3 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f, 3.0f }, PVectorFloat.INSTANCE);
    try {
      DistanceFunction.validateChildren(Arrays.asList(v2, v3));
      fail("Expected SQLException for dimension mismatch");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.VECTOR_DIMENSION_MISMATCH.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testDimensionMismatchAtEvaluation() throws Exception {
    // Operands with different dimensions must fail at row evaluation
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f }, PVectorFloat.INSTANCE);
    Expression v3 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f, 3.0f }, PVectorFloat.INSTANCE);
    for (DistanceFunction func : new DistanceFunction[] {
      new L2DistanceFunction(Arrays.asList(v2, v3)),
      new L2DistanceSquaredFunction(Arrays.asList(v2, v3)),
      new CosineDistanceFunction(Arrays.asList(v2, v3)),
      new InnerProductDistanceFunction(Arrays.asList(v2, v3)) }) {
      try {
        func.evaluate(null, new ImmutableBytesWritable());
        fail("Expected IllegalDataException for " + func.getName());
      } catch (IllegalDataException e) {
        assertEquals(SQLExceptionCode.VECTOR_DIMENSION_MISMATCH.getErrorCode(),
          ((SQLException) e.getCause()).getErrorCode());
      }
    }
  }

  @Test
  public void testNullPropagation() throws Exception {
    Expression nullVec = LiteralExpression.newConstant(null, PVectorFloat.INSTANCE);
    Expression validVec =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f }, PVectorFloat.INSTANCE);

    DistanceFunction[] funcs =
      new DistanceFunction[] { new L2DistanceFunction(Arrays.asList(nullVec, validVec)),
        new L2DistanceFunction(Arrays.asList(validVec, nullVec)),
        new L2DistanceFunction(Arrays.asList(nullVec, nullVec)),
        new L2DistanceSquaredFunction(Arrays.asList(nullVec, validVec)),
        new CosineDistanceFunction(Arrays.asList(nullVec, validVec)),
        new InnerProductDistanceFunction(Arrays.asList(nullVec, validVec)) };

    for (DistanceFunction func : funcs) {
      ImmutableBytesWritable ptr = new ImmutableBytesWritable(new byte[] { 1 });
      assertTrue("A null operand is evaluable for " + func.getName(), func.evaluate(null, ptr));
      assertEquals("Expected SQL NULL for " + func.getName(), 0, ptr.getLength());
    }
  }

  @Test
  public void testEarlyTerminationUpperBound() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE);

    // A squared L2 distance above the upper bound must return Double.MAX_VALUE
    L2DistanceSquaredFunction l2SqExceeded =
      withBound(new L2DistanceSquaredFunction(Arrays.asList(v1, v2)), 10.0);
    assertEquals(Double.MAX_VALUE, evaluateDistance(l2SqExceeded), DELTA);

    L2DistanceSquaredFunction l2SqMet =
      withBound(new L2DistanceSquaredFunction(Arrays.asList(v1, v2)), 30.0);
    assertEquals(25.0, evaluateDistance(l2SqMet), DELTA);

    // An L2 distance above the upper bound must return Double.MAX_VALUE
    L2DistanceFunction l2Exceeded = withBound(new L2DistanceFunction(Arrays.asList(v1, v2)), 4.0);
    assertEquals(Double.MAX_VALUE, evaluateDistance(l2Exceeded), DELTA);

    L2DistanceFunction l2Met = withBound(new L2DistanceFunction(Arrays.asList(v1, v2)), 6.0);
    assertEquals(5.0, evaluateDistance(l2Met), DELTA);
  }

  @Test
  public void testParseNodeCreation() throws Exception {
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f }, PVectorFloat.INSTANCE);
    Expression v3 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f, 3.0f }, PVectorFloat.INSTANCE);
    Expression v2b =
      LiteralExpression.newConstant(new float[] { 3.0f, 4.0f }, PVectorFloat.INSTANCE);
    List<ParseNode> dummyNodes = Collections.emptyList();
    for (int i = 0; i < DISTANCE_FUNCTION_NAMES.length; i++) {
      String name = DISTANCE_FUNCTION_NAMES[i];
      DistanceFunctionParseNode node =
        new DistanceFunctionParseNode(name, dummyNodes, lookup(name));
      try {
        node.create(Arrays.asList(v2, v3), null);
        fail("Expected SQLException on dimension mismatch in parse node for " + name);
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_DIMENSION_MISMATCH.getErrorCode(), e.getErrorCode());
      }
      assertTrue(
        DISTANCE_FUNCTION_CLASSES[i].isInstance(node.create(Arrays.asList(v2, v2b), null)));
    }
  }

  @Test
  public void testExpressionTypeFullSerializationRoundTrip() throws Exception {
    // Each function must survive a round trip through expression serialization
    LiteralExpression child1 =
      LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE, 2, null);
    LiteralExpression child2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE, 2, null);

    double[] expectedValues = { 5.0, 25.0, 1.0, 0.0 };
    List<DistanceFunction> functions =
      Arrays.asList(new L2DistanceFunction(Arrays.asList(child1, child2)),
        new L2DistanceSquaredFunction(Arrays.asList(child1, child2)),
        new CosineDistanceFunction(Arrays.asList(child1, child2)),
        new InnerProductDistanceFunction(Arrays.asList(child1, child2)));

    for (int i = 0; i < functions.size(); i++) {
      DistanceFunction func = functions.get(i);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      WritableUtils.writeVInt(out, ExpressionType.valueOf(func).ordinal());
      func.write(out);
      out.close();

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
      int ordinal = WritableUtils.readVInt(in);
      Expression deserialized = ExpressionType.values()[ordinal].newInstance();
      deserialized.readFields(in);

      assertTrue(func.getClass().isInstance(deserialized));
      DistanceFunction deserializedDistance = (DistanceFunction) deserialized;
      assertEquals(Double.MAX_VALUE, deserializedDistance.getDistanceUpperBound(), 0.0);
      assertEquals(2, deserializedDistance.getChildren().size());

      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      assertTrue(deserializedDistance.evaluate(null, ptr));
      double val = (Double) PDouble.INSTANCE.toObject(ptr);
      assertEquals(func.getClass().getSimpleName() + " returned unexpected distance",
        expectedValues[i], val, DELTA);
    }
  }

  private static final String[] DISTANCE_FUNCTION_NAMES =
    { L2DistanceFunction.NAME, L2DistanceSquaredFunction.NAME, CosineDistanceFunction.NAME,
      InnerProductDistanceFunction.NAME };

  private static final Class<?>[] DISTANCE_FUNCTION_CLASSES =
    { L2DistanceFunction.class, L2DistanceSquaredFunction.class, CosineDistanceFunction.class,
      InnerProductDistanceFunction.class };

  @Test
  public void testFunctionLookupByName() {
    for (int i = 0; i < DISTANCE_FUNCTION_NAMES.length; i++) {
      String name = DISTANCE_FUNCTION_NAMES[i];
      BuiltInFunctionInfo info = lookup(name);
      assertNotNull("Function lookup failed for: " + name, info);
      assertEquals(name, info.getName());
      assertEquals(2, info.getRequiredArgCount());
      assertEquals(2, info.getArgs().length);
      assertEquals(DistanceFunctionParseNode.class, info.getNodeCtor().getDeclaringClass());
      assertEquals(DISTANCE_FUNCTION_CLASSES[i], info.getFunc());

      // Each argument must accept both float and double vectors
      for (FunctionParseNode.BuiltInFunctionArgInfo arg : info.getArgs()) {
        List<Class<?>> allowedTypes = Arrays.asList(arg.getAllowedTypes());
        assertTrue(name + " arg should allow PVectorFloat",
          allowedTypes.contains(PVectorFloat.class));
        assertTrue(name + " arg should allow PVectorDouble",
          allowedTypes.contains(PVectorDouble.class));
      }
    }
  }

  @Test
  public void testFunctionLookupWithChildrenList() {
    ParseNode c1 = new ColumnParseNode(null, "V1", null);
    ParseNode c2 = new ColumnParseNode(null, "V2", null);
    List<ParseNode> children2 = Arrays.asList(c1, c2);
    List<ParseNode> children1 = Arrays.asList(c1);

    for (String name : DISTANCE_FUNCTION_NAMES) {
      BuiltInFunctionInfo info = ParseNodeFactory.get(name, children2);
      assertNotNull("Lookup with 2 children failed for: " + name, info);
      assertEquals(name, info.getName());

      assertNull("Lookup with 1 child should be null for: " + name,
        ParseNodeFactory.get(name, children1));
    }
  }

  @Test
  public void testFunctionUniquenessAndNoCollisions() {
    Multimap<String, BuiltInFunctionInfo> multimap = ParseNodeFactory.getBuiltInFunctionMultimap();

    for (String name : DISTANCE_FUNCTION_NAMES) {
      assertTrue("Multimap should contain: " + name, multimap.containsKey(name));
      Collection<BuiltInFunctionInfo> infos = multimap.get(name);
      assertEquals("Expected exactly 1 registered function for " + name, 1, infos.size());
      BuiltInFunctionInfo info = infos.iterator().next();
      assertEquals(name, info.getName());
    }

    // A name that is not registered must not resolve
    assertFalse(multimap.containsKey("NON_EXISTENT_DISTANCE_FUNC"));
  }

  @Test
  public void testParseNodeFactoryDirectMethods() {
    ParseNodeFactory factory = new ParseNodeFactory();
    ParseNode c1 = new ColumnParseNode(null, "V1", null);
    ParseNode c2 = new ColumnParseNode(null, "V2", null);
    List<ParseNode> children = Arrays.asList(c1, c2);

    assertDistanceNode(L2DistanceFunction.NAME, factory.l2Distance(children));
    assertDistanceNode(CosineDistanceFunction.NAME, factory.cosineDistance(children));
    assertDistanceNode(InnerProductDistanceFunction.NAME, factory.innerProductDistance(children));
    for (String name : DISTANCE_FUNCTION_NAMES) {
      assertDistanceNode(name, factory.function(name, children));
    }
  }

  @Test
  public void testSQLParsingDistanceFunctions() throws Exception {
    for (String name : DISTANCE_FUNCTION_NAMES) {
      for (String spelling : new String[] { name, name.toLowerCase() }) {
        SelectStatement select =
          (SelectStatement) new SQLParser("SELECT " + spelling + "(v1, v2) FROM t")
            .parseStatement();
        ParseNode node = select.getSelect().get(0).getNode();
        assertDistanceNode(name, node);
        assertEquals(2, node.getChildren().size());
      }
    }
  }

  @Test
  public void testEndToEndSQLCompilation() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE t_vector_func_test (" + "pk INTEGER PRIMARY KEY, " + "v1 VECTOR(FLOAT, 3), "
          + "v2 VECTOR(FLOAT, 3), " + "vd1 VECTOR(DOUBLE, 4), " + "vd2 VECTOR(DOUBLE, 4))");

      String[] queries = { "SELECT L2_DISTANCE(v1, v2) FROM t_vector_func_test",
        "SELECT L2_DISTANCE_SQUARED(v1, v2) FROM t_vector_func_test",
        "SELECT COSINE_DISTANCE(v1, v2) FROM t_vector_func_test",
        "SELECT INNER_PRODUCT(v1, v2) FROM t_vector_func_test",
        "SELECT L2_DISTANCE(vd1, vd2) FROM t_vector_func_test",
        "SELECT L2_DISTANCE_SQUARED(vd1, vd2) FROM t_vector_func_test",
        "SELECT COSINE_DISTANCE(vd1, vd2) FROM t_vector_func_test",
        "SELECT INNER_PRODUCT(vd1, vd2) FROM t_vector_func_test",
        "SELECT pk FROM t_vector_func_test WHERE L2_DISTANCE(v1, v2) < 2.5",
        "SELECT pk FROM t_vector_func_test ORDER BY COSINE_DISTANCE(v1, v2) LIMIT 10",
        "SELECT L2_DISTANCE(v1, v2) + 1.0 AS dist FROM t_vector_func_test" };

      for (String sql : queries) {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          PhoenixPreparedStatement pStmt = stmt.unwrap(PhoenixPreparedStatement.class);
          QueryPlan plan = pStmt.compileQuery();
          assertNotNull("Plan should not be null for query: " + sql, plan);
        }
      }
    }
  }

  @Test
  public void testCompilationFailsOnDimensionMismatch() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE t_dim_mismatch (" + "pk INTEGER PRIMARY KEY, "
        + "v3 VECTOR(FLOAT, 3), " + "v4 VECTOR(FLOAT, 4))");

      try (PreparedStatement stmt =
        conn.prepareStatement("SELECT L2_DISTANCE(v3, v4) FROM t_dim_mismatch")) {
        PhoenixPreparedStatement pStmt = stmt.unwrap(PhoenixPreparedStatement.class);
        pStmt.compileQuery();
        fail("Expected compilation to fail due to dimension mismatch");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_DIMENSION_MISMATCH.getErrorCode(), e.getErrorCode());
      }
    }
  }

  @Test
  public void testCompilationFailsOnLiteralDimensionMismatch() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE t_literal_dim_mismatch ("
        + "pk INTEGER PRIMARY KEY, " + "embedding VECTOR(FLOAT, 3))");

      try (PreparedStatement stmt =
        conn.prepareStatement("SELECT pk FROM t_literal_dim_mismatch ORDER BY "
          + "L2_DISTANCE(embedding, ARRAY[1.0, 2.0]) LIMIT 10")) {
        PhoenixPreparedStatement pStmt = stmt.unwrap(PhoenixPreparedStatement.class);
        pStmt.compileQuery();
        fail("Expected compilation to fail due to dimension mismatch between the column and "
          + "the literal query vector");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_DIMENSION_MISMATCH.getErrorCode(), e.getErrorCode());
      }
    }
  }

  @Test
  public void testCompilationFailsOnNonVectorArgument() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE t_non_vector (" + "pk INTEGER PRIMARY KEY, " + "v VECTOR(FLOAT, 3))");

      try (PreparedStatement stmt =
        conn.prepareStatement("SELECT L2_DISTANCE(pk, v) FROM t_non_vector")) {
        PhoenixPreparedStatement pStmt = stmt.unwrap(PhoenixPreparedStatement.class);
        pStmt.compileQuery();
        fail("Expected compilation to fail due to non-vector argument");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
      }
    }
  }

  @Test
  public void testCompilationFailsOnUnknownFunction() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE t_unknown_func (" + "pk INTEGER PRIMARY KEY, "
        + "v1 VECTOR(FLOAT, 3), " + "v2 VECTOR(FLOAT, 3))");

      try (PreparedStatement stmt =
        conn.prepareStatement("SELECT UNKNOWN_DISTANCE_FUNC(v1, v2) FROM t_unknown_func")) {
        PhoenixPreparedStatement pStmt = stmt.unwrap(PhoenixPreparedStatement.class);
        pStmt.compileQuery();
        fail("Expected compilation to fail for unknown function");
      } catch (SQLException e) {
        // Compilation fails because the function name does not resolve
        assertNotNull(e.getMessage());
      }
    }
  }

  @Test
  public void testExplainTopKMerge() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE t_explain_topk (pk INTEGER PRIMARY KEY, "
        + "v VECTOR(FLOAT, 3)) SALT_BUCKETS=4");
      String query = "SELECT pk FROM t_explain_topk ORDER BY L2_DISTANCE(v, ARRAY[1.0, 0.0, 0.0])";

      String plan = QueryUtil
        .getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + query + " LIMIT 5"));
      assertTrue(plan, plan.contains("SERVER TOP-5 BY L2_DISTANCE"));
      assertTrue(plan, plan.contains("CLIENT MERGE SORT TOP-5"));
      assertFalse(plan, plan.contains("CLIENT LIMIT"));

      // With an OFFSET, the plan keeps separate merge, offset and limit steps.
      plan = QueryUtil.getExplainPlan(
        conn.createStatement().executeQuery("EXPLAIN " + query + " LIMIT 5 OFFSET 2"));
      assertFalse(plan, plan.contains("CLIENT MERGE SORT TOP-"));
      int merge = plan.indexOf("CLIENT MERGE SORT");
      int offset = plan.indexOf("CLIENT OFFSET 2");
      int limit = plan.indexOf("CLIENT LIMIT 5");
      assertTrue(plan, merge >= 0 && merge < offset && offset < limit);
    }
  }

  /** A vector expression that returns the next vector from a fixed list on each evaluate call. */
  private static class VectorSequenceExpression extends BaseTerminalExpression {
    private final List<float[]> vectors;
    private int next = 0;

    VectorSequenceExpression(List<float[]> vectors) {
      this.vectors = vectors;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
      ptr.set(PVectorFloat.INSTANCE.toBytes(vectors.get(next++)));
      return true;
    }

    @Override
    public PDataType getDataType() {
      return PVectorFloat.INSTANCE;
    }

    @Override
    public Integer getMaxLength() {
      return vectors.get(0).length;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
      return null;
    }
  }

  /**
   * Successive evaluate() calls must return different output buffers. A stateful result iterator
   * can keep an earlier result, and a shared buffer would change that result.
   */
  @Test
  public void testEvaluateDoesNotAliasOutputBufferAcrossCalls() throws Exception {
    Expression query =
      LiteralExpression.newConstant(new float[] { 0.0f, 0.0f }, PVectorFloat.INSTANCE);
    Expression rows = new VectorSequenceExpression(
      Arrays.asList(new float[] { 3.0f, 4.0f }, new float[] { 6.0f, 8.0f }));
    L2DistanceFunction func = new L2DistanceFunction(Arrays.asList(rows, query));

    ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
    ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();
    assertTrue(func.evaluate(null, ptr1));
    assertTrue(func.evaluate(null, ptr2));

    assertEquals(5.0, ((Number) PDouble.INSTANCE.toObject(ptr1)).doubleValue(), DELTA);
    assertEquals(10.0, ((Number) PDouble.INSTANCE.toObject(ptr2)).doubleValue(), DELTA);
    assertFalse("Successive evaluations must not share an output buffer", ptr1.get() == ptr2.get());
  }

  @Test
  public void testDistanceFunctionsWithDim128RandomAgainstBruteForce() throws Exception {
    Random rng = new Random(12345);
    int[] dimensions = new int[] { 128, 129 };
    int numPairs = 20;

    for (int dim : dimensions) {
      for (int pair = 0; pair < numPairs; pair++) {
        float[] a = new float[dim];
        float[] b = new float[dim];
        for (int i = 0; i < dim; i++) {
          a[i] = (rng.nextFloat() - 0.5f) * 20.0f;
          b[i] = (rng.nextFloat() - 0.5f) * 20.0f;
        }

        // Reference distances in double precision
        double sumSq = 0.0;
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < dim; i++) {
          double ai = (double) a[i];
          double bi = (double) b[i];
          double diff = ai - bi;
          sumSq += diff * diff;
          dot += ai * bi;
          normA += ai * ai;
          normB += bi * bi;
        }
        double expectedL2Sq = sumSq;
        double expectedL2 = Math.sqrt(sumSq);
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        double cosSim = (denom == 0.0) ? 1.0 : (dot / denom);
        if (cosSim > 1.0) {
          cosSim = 1.0;
        } else if (cosSim < -1.0) {
          cosSim = -1.0;
        }
        double expectedCos = 1.0 - cosSim;
        double expectedIp = -dot;

        Expression exprA = LiteralExpression.newConstant(a, PVectorFloat.INSTANCE, dim, null);
        Expression exprB = LiteralExpression.newConstant(b, PVectorFloat.INSTANCE, dim, null);

        // L2DistanceFunction
        L2DistanceFunction l2 = new L2DistanceFunction(Arrays.asList(exprA, exprB));
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(l2.evaluate(null, ptr));
        double actualL2 = ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue();
        assertEquals("L2 mismatch at dim " + dim + " pair " + pair, expectedL2, actualL2,
          1e-4 * Math.max(1.0, Math.abs(expectedL2)));

        // L2DistanceSquaredFunction
        L2DistanceSquaredFunction l2Sq = new L2DistanceSquaredFunction(Arrays.asList(exprA, exprB));
        ptr = new ImmutableBytesWritable();
        assertTrue(l2Sq.evaluate(null, ptr));
        double actualL2Sq = ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue();
        assertEquals("L2Sq mismatch at dim " + dim + " pair " + pair, expectedL2Sq, actualL2Sq,
          1e-4 * Math.max(1.0, Math.abs(expectedL2Sq)));

        // CosineDistanceFunction
        CosineDistanceFunction cos = new CosineDistanceFunction(Arrays.asList(exprA, exprB));
        ptr = new ImmutableBytesWritable();
        assertTrue(cos.evaluate(null, ptr));
        double actualCos = ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue();
        assertEquals("Cosine mismatch at dim " + dim + " pair " + pair, expectedCos, actualCos,
          1e-4 * Math.max(1.0, Math.abs(expectedCos)));

        // InnerProductDistanceFunction
        InnerProductDistanceFunction ip =
          new InnerProductDistanceFunction(Arrays.asList(exprA, exprB));
        ptr = new ImmutableBytesWritable();
        assertTrue(ip.evaluate(null, ptr));
        double actualIp = ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue();
        assertEquals("IP mismatch at dim " + dim + " pair " + pair, expectedIp, actualIp,
          1e-4 * Math.max(1.0, Math.abs(expectedIp)));
      }
    }
  }
}
