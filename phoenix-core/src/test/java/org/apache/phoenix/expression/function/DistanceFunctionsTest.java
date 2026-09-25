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
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.CosineDistanceParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.InnerProductDistanceParseNode;
import org.apache.phoenix.parse.L2DistanceParseNode;
import org.apache.phoenix.parse.L2DistanceSquaredParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

public class DistanceFunctionsTest extends BaseConnectionlessQueryTest {

  private static final double DELTA = 1e-6;

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
    List<Expression> mismatched = Arrays.asList(v2, v3);

    try {
      new L2DistanceFunction(mismatched);
      fail("Expected IllegalArgumentException for L2DistanceFunction dimension mismatch");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("dimension mismatch"));
    }

    try {
      new L2DistanceSquaredFunction(mismatched);
      fail("Expected IllegalArgumentException for L2DistanceSquaredFunction dimension mismatch");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("dimension mismatch"));
    }

    try {
      new CosineDistanceFunction(mismatched);
      fail("Expected IllegalArgumentException for CosineDistanceFunction dimension mismatch");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("dimension mismatch"));
    }

    try {
      new InnerProductDistanceFunction(mismatched);
      fail("Expected IllegalArgumentException for InnerProductDistanceFunction dimension mismatch");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("dimension mismatch"));
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

    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    for (DistanceFunction func : funcs) {
      boolean evaluated = func.evaluate(null, ptr);
      assertFalse("Expected SQL NULL (evaluate=false) when child is null for " + func.getName(),
        evaluated);
    }
  }

  @Test
  public void testEarlyTerminationUpperBound() throws Exception {
    Expression v1 =
      LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE);
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE);

    // Verify squared L2 distance upper bound pruning
    L2DistanceSquaredFunction l2SqExceeded =
      new L2DistanceSquaredFunction(Arrays.asList(v1, v2), 10.0);
    assertEquals(Double.MAX_VALUE, evaluateDistance(l2SqExceeded), DELTA);

    L2DistanceSquaredFunction l2SqMet = new L2DistanceSquaredFunction(Arrays.asList(v1, v2), 30.0);
    assertEquals(25.0, evaluateDistance(l2SqMet), DELTA);

    // Verify L2 distance upper bound pruning
    L2DistanceFunction l2Exceeded = new L2DistanceFunction(Arrays.asList(v1, v2), 4.0);
    assertEquals(Double.MAX_VALUE, evaluateDistance(l2Exceeded), DELTA);

    L2DistanceFunction l2Met = new L2DistanceFunction(Arrays.asList(v1, v2), 6.0);
    assertEquals(5.0, evaluateDistance(l2Met), DELTA);
  }

  @Test
  public void testParseNodeCreation() throws Exception {
    Expression v2 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f }, PVectorFloat.INSTANCE);
    Expression v3 =
      LiteralExpression.newConstant(new float[] { 1.0f, 2.0f, 3.0f }, PVectorFloat.INSTANCE);

    List<ParseNode> dummyNodes = Collections.emptyList();
    L2DistanceParseNode l2Node = new L2DistanceParseNode("L2_DISTANCE", dummyNodes, null);

    try {
      l2Node.create(Arrays.asList(v2, v3), null);
      fail("Expected SQLException on dimension mismatch in parse node");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("dimension mismatch"));
    }

    Expression v2b =
      LiteralExpression.newConstant(new float[] { 3.0f, 4.0f }, PVectorFloat.INSTANCE);
    FunctionExpression created = l2Node.create(Arrays.asList(v2, v2b), null);
    assertTrue(created instanceof L2DistanceFunction);

    L2DistanceSquaredParseNode l2SqNode =
      new L2DistanceSquaredParseNode("L2_DISTANCE_SQUARED", dummyNodes, null);
    assertTrue(l2SqNode.create(Arrays.asList(v2, v2b), null) instanceof L2DistanceSquaredFunction);

    CosineDistanceParseNode cosineNode =
      new CosineDistanceParseNode("COSINE_DISTANCE", dummyNodes, null);
    assertTrue(cosineNode.create(Arrays.asList(v2, v2b), null) instanceof CosineDistanceFunction);

    InnerProductDistanceParseNode ipNode =
      new InnerProductDistanceParseNode("INNER_PRODUCT", dummyNodes, null);
    assertTrue(ipNode.create(Arrays.asList(v2, v2b), null) instanceof InnerProductDistanceFunction);
  }

  @Test
  public void testExpressionTypeVIntSerializationRoundTrip() throws Exception {
    // Verify ExpressionType resolution and VInt serialization round-trip for each distance
    // function.
    List<Class<? extends Expression>> functionClasses =
      Arrays.asList(L2DistanceFunction.class, L2DistanceSquaredFunction.class,
        CosineDistanceFunction.class, InnerProductDistanceFunction.class);

    for (Class<? extends Expression> clazz : functionClasses) {
      Expression expression = clazz.getDeclaredConstructor().newInstance();
      ExpressionType expectedType = ExpressionType.valueOf(expression);
      assertEquals(clazz, expectedType.getExpressionClass());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      WritableUtils.writeVInt(out, expectedType.ordinal());
      out.close();

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
      int decoded = WritableUtils.readVInt(in);
      Expression resolved = ExpressionType.values()[decoded].newInstance();
      assertTrue(clazz.getName() + " should round-trip via VInt ordinal",
        clazz.isInstance(resolved));
    }
  }

  @Test
  public void testExpressionTypeFullSerializationRoundTrip() throws Exception {
    // Verify full expression serialization and evaluated results against orthogonal test vectors.
    LiteralExpression child1 =
      LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE, 2, null);
    LiteralExpression child2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE, 2, null);

    double[] expectedValues = { 5.0, 25.0, 1.0, 0.0 };
    List<DistanceFunction> functions =
      Arrays.asList(new L2DistanceFunction(Arrays.asList(child1, child2), 10.0),
        new L2DistanceSquaredFunction(Arrays.asList(child1, child2), 30.0),
        new CosineDistanceFunction(Arrays.asList(child1, child2), 2.0),
        new InnerProductDistanceFunction(Arrays.asList(child1, child2), 5.0));

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
      assertEquals(func.getDistanceUpperBound(), deserializedDistance.getDistanceUpperBound(),
        DELTA);
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

  private static final Class<?>[] DISTANCE_PARSE_NODE_CLASSES =
    { L2DistanceParseNode.class, L2DistanceSquaredParseNode.class, CosineDistanceParseNode.class,
      InnerProductDistanceParseNode.class };

  @Test
  public void testFunctionLookupByName() {
    for (int i = 0; i < DISTANCE_FUNCTION_NAMES.length; i++) {
      String name = DISTANCE_FUNCTION_NAMES[i];
      BuiltInFunctionInfo info = ParseNodeFactory.getBuiltInFunction(name);
      assertNotNull("Function lookup failed for: " + name, info);
      assertEquals(name, info.getName());
      assertEquals(2, info.getRequiredArgCount());
      assertEquals(2, info.getArgs().length);
      assertEquals(DISTANCE_PARSE_NODE_CLASSES[i], info.getNodeCtor().getDeclaringClass());
      assertEquals(DISTANCE_FUNCTION_CLASSES[i], info.getFunc());

      // Validate allowed argument types are PVectorFloat and PVectorDouble
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
  public void testFunctionLookupWithArgCount() {
    for (String name : DISTANCE_FUNCTION_NAMES) {
      BuiltInFunctionInfo info = ParseNodeFactory.get(name, 2);
      assertNotNull("Lookup with argCount=2 failed for: " + name, info);
      assertEquals(name, info.getName());

      // Lookup with invalid argument count returns null
      assertNull("Lookup with argCount=1 should be null for: " + name,
        ParseNodeFactory.get(name, 1));
      assertNull("Lookup with argCount=3 should be null for: " + name,
        ParseNodeFactory.get(name, 3));
      assertNull("Lookup with argCount=0 should be null for: " + name,
        ParseNodeFactory.get(name, 0));
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
  public void testCaseInsensitiveResolution() {
    for (String name : DISTANCE_FUNCTION_NAMES) {
      String lower = name.toLowerCase();
      BuiltInFunctionInfo info1 = ParseNodeFactory.getBuiltInFunction(lower);
      assertNotNull("Case-insensitive getBuiltInFunction failed for: " + lower, info1);
      assertEquals(name, info1.getName());

      BuiltInFunctionInfo info2 = ParseNodeFactory.get(lower, 2);
      assertNotNull("Case-insensitive get(name, 2) failed for: " + lower, info2);
      assertEquals(name, info2.getName());
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

    // Verify nonexistent function lookup returns null
    assertNull(ParseNodeFactory.getBuiltInFunction("NON_EXISTENT_DISTANCE_FUNC"));
    assertNull(ParseNodeFactory.get("NON_EXISTENT_DISTANCE_FUNC", 2));
  }

  @Test
  public void testParseNodeFactoryDirectMethods() {
    ParseNodeFactory factory = new ParseNodeFactory();
    ParseNode c1 = new ColumnParseNode(null, "V1", null);
    ParseNode c2 = new ColumnParseNode(null, "V2", null);
    List<ParseNode> children = Arrays.asList(c1, c2);

    FunctionParseNode l2 = factory.l2Distance(children);
    assertTrue(l2 instanceof L2DistanceParseNode);
    assertEquals(L2DistanceFunction.NAME, l2.getName());

    FunctionParseNode l2Sq = factory.l2DistanceSquared(children);
    assertTrue(l2Sq instanceof L2DistanceSquaredParseNode);
    assertEquals(L2DistanceSquaredFunction.NAME, l2Sq.getName());

    FunctionParseNode cosine = factory.cosineDistance(children);
    assertTrue(cosine instanceof CosineDistanceParseNode);
    assertEquals(CosineDistanceFunction.NAME, cosine.getName());

    FunctionParseNode inner = factory.innerProductDistance(children);
    assertTrue(inner instanceof InnerProductDistanceParseNode);
    assertEquals(InnerProductDistanceFunction.NAME, inner.getName());

    // Generic factory.function dispatch
    assertTrue(factory.function(L2DistanceFunction.NAME, children) instanceof L2DistanceParseNode);
    assertTrue(factory.function(L2DistanceSquaredFunction.NAME,
      children) instanceof L2DistanceSquaredParseNode);
    assertTrue(
      factory.function(CosineDistanceFunction.NAME, children) instanceof CosineDistanceParseNode);
    assertTrue(factory.function(InnerProductDistanceFunction.NAME,
      children) instanceof InnerProductDistanceParseNode);
  }

  @Test
  public void testSQLParsingDistanceFunctions() throws Exception {
    SQLParser parser;
    SelectStatement select;
    ParseNode node;

    parser = new SQLParser("SELECT L2_DISTANCE(v1, v2) FROM t");
    select = (SelectStatement) parser.parseStatement();
    node = select.getSelect().get(0).getNode();
    assertTrue("Expected L2DistanceParseNode", node instanceof L2DistanceParseNode);
    assertEquals(2, node.getChildren().size());

    parser = new SQLParser("SELECT L2_DISTANCE_SQUARED(v1, v2) FROM t");
    select = (SelectStatement) parser.parseStatement();
    node = select.getSelect().get(0).getNode();
    assertTrue("Expected L2DistanceSquaredParseNode", node instanceof L2DistanceSquaredParseNode);
    assertEquals(2, node.getChildren().size());

    parser = new SQLParser("SELECT COSINE_DISTANCE(v1, v2) FROM t");
    select = (SelectStatement) parser.parseStatement();
    node = select.getSelect().get(0).getNode();
    assertTrue("Expected CosineDistanceParseNode", node instanceof CosineDistanceParseNode);
    assertEquals(2, node.getChildren().size());

    parser = new SQLParser("SELECT INNER_PRODUCT(v1, v2) FROM t");
    select = (SelectStatement) parser.parseStatement();
    node = select.getSelect().get(0).getNode();
    assertTrue("Expected InnerProductDistanceParseNode",
      node instanceof InnerProductDistanceParseNode);
    assertEquals(2, node.getChildren().size());

    // Verify case-insensitive resolution in SQL parser
    parser = new SQLParser("SELECT l2_distance(v1, v2) FROM t");
    select = (SelectStatement) parser.parseStatement();
    node = select.getSelect().get(0).getNode();
    assertTrue("Expected L2DistanceParseNode for lowercase call",
      node instanceof L2DistanceParseNode);
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
        assertTrue("Error message should mention dimension mismatch: " + e.getMessage(),
          e.getMessage().contains("Vector dimension mismatch"));
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
        assertTrue("Error message should mention vector type mismatch: " + e.getMessage(),
          e.getMessage().contains("VECTOR") || e.getMessage().contains("must be a vector type"));
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
        // Unregistered function names trigger resolution failure
        assertNotNull(e.getMessage());
      }
    }
  }

  /**
   * Terminal expression that emits successive vector values across evaluations to simulate
   * row-by-row column scanning.
   */
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
   * Execution iterators such as OrderedResultIterator and MergeSortTopNResultIterator retain the
   * ImmutableBytesWritable instances produced by evaluate() across rows. Consecutive evaluations
   * must therefore produce independent byte buffers to preserve comparator correctness.
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

        // Compute reference distances using double precision
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
