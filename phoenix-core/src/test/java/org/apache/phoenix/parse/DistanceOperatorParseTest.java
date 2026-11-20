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
package org.apache.phoenix.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

/**
 * Parser tests for the infix vector distance operators (<->, <=>, <#>). Each operator must parse to
 * the same parse node as its distance function. The tests also examine the precedence and
 * associativity of the operators relative to arithmetic, comparison, and logical operators.
 */
public class DistanceOperatorParseTest {

  private static boolean isDistance(ParseNode node, String name) {
    return node instanceof DistanceFunctionParseNode
      && name.equals(((FunctionParseNode) node).getName());
  }

  @Test
  public void testParseL2DistanceOperator() throws Exception {
    String sql = "SELECT v1 <-> v2 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    assertEquals(1, select.getSelect().size());
    ParseNode node = select.getSelect().get(0).getNode();
    assertTrue("Expected L2_DISTANCE node, got: " + node.getClass().getSimpleName(),
      isDistance(node, "L2_DISTANCE"));
    assertEquals(2, node.getChildren().size());
    assertTrue(node.getChildren().get(0) instanceof ColumnParseNode);
    assertTrue(node.getChildren().get(1) instanceof ColumnParseNode);
    assertEquals("V1", ((ColumnParseNode) node.getChildren().get(0)).getName());
    assertEquals("V2", ((ColumnParseNode) node.getChildren().get(1)).getName());
  }

  @Test
  public void testParseCosineDistanceOperator() throws Exception {
    String sql = "SELECT v1 <=> v2 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    assertEquals(1, select.getSelect().size());
    ParseNode node = select.getSelect().get(0).getNode();
    assertTrue("Expected COSINE_DISTANCE node, got: " + node.getClass().getSimpleName(),
      isDistance(node, "COSINE_DISTANCE"));
    assertEquals(2, node.getChildren().size());
    assertTrue(node.getChildren().get(0) instanceof ColumnParseNode);
    assertTrue(node.getChildren().get(1) instanceof ColumnParseNode);
    assertEquals("V1", ((ColumnParseNode) node.getChildren().get(0)).getName());
    assertEquals("V2", ((ColumnParseNode) node.getChildren().get(1)).getName());
  }

  @Test
  public void testParseInnerProductDistanceOperator() throws Exception {
    String sql = "SELECT v1 <#> v2 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    assertEquals(1, select.getSelect().size());
    ParseNode node = select.getSelect().get(0).getNode();
    assertTrue("Expected INNER_PRODUCT node, got: " + node.getClass().getSimpleName(),
      isDistance(node, "INNER_PRODUCT"));
    assertEquals(2, node.getChildren().size());
    assertTrue(node.getChildren().get(0) instanceof ColumnParseNode);
    assertTrue(node.getChildren().get(1) instanceof ColumnParseNode);
    assertEquals("V1", ((ColumnParseNode) node.getChildren().get(0)).getName());
    assertEquals("V2", ((ColumnParseNode) node.getChildren().get(1)).getName());
  }

  @Test
  public void testOperatorFunctionEquivalence() throws Exception {
    // The <-> operator gives the same parse node as L2_DISTANCE.
    SelectStatement opL2 =
      (SelectStatement) new SQLParser("SELECT v1 <-> v2 FROM t").parseStatement();
    SelectStatement fnL2 =
      (SelectStatement) new SQLParser("SELECT L2_DISTANCE(v1, v2) FROM t").parseStatement();
    ParseNode opL2Node = opL2.getSelect().get(0).getNode();
    ParseNode fnL2Node = fnL2.getSelect().get(0).getNode();
    assertTrue(isDistance(opL2Node, "L2_DISTANCE"));
    assertEquals(fnL2Node.getClass(), opL2Node.getClass());
    assertEquals(fnL2Node, opL2Node);

    // The <=> operator gives the same parse node as COSINE_DISTANCE.
    SelectStatement opCos =
      (SelectStatement) new SQLParser("SELECT v1 <=> v2 FROM t").parseStatement();
    SelectStatement fnCos =
      (SelectStatement) new SQLParser("SELECT COSINE_DISTANCE(v1, v2) FROM t").parseStatement();
    ParseNode opCosNode = opCos.getSelect().get(0).getNode();
    ParseNode fnCosNode = fnCos.getSelect().get(0).getNode();
    assertTrue(isDistance(opCosNode, "COSINE_DISTANCE"));
    assertEquals(fnCosNode.getClass(), opCosNode.getClass());
    assertEquals(fnCosNode, opCosNode);

    // The <#> operator gives the same parse node as INNER_PRODUCT.
    SelectStatement opIp =
      (SelectStatement) new SQLParser("SELECT v1 <#> v2 FROM t").parseStatement();
    SelectStatement fnIp =
      (SelectStatement) new SQLParser("SELECT INNER_PRODUCT(v1, v2) FROM t").parseStatement();
    ParseNode opIpNode = opIp.getSelect().get(0).getNode();
    ParseNode fnIpNode = fnIp.getSelect().get(0).getNode();
    assertTrue(isDistance(opIpNode, "INNER_PRODUCT"));
    assertEquals(fnIpNode.getClass(), opIpNode.getClass());
    assertEquals(fnIpNode, opIpNode);
  }

  @Test
  public void testPrecedenceDistanceVsComparison() throws Exception {
    // Distance operators have a higher precedence than comparison operators.
    String sql = "SELECT v1 <-> v2 < 0.5 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode root = select.getSelect().get(0).getNode();
    assertTrue("Expected LessThanParseNode, got: " + root.getClass().getSimpleName(),
      root instanceof LessThanParseNode);
    assertEquals(2, root.getChildren().size());
    ParseNode lhs = root.getChildren().get(0);
    ParseNode rhs = root.getChildren().get(1);
    assertTrue("LHS should be L2_DISTANCE node, got: " + lhs.getClass().getSimpleName(),
      isDistance(lhs, "L2_DISTANCE"));
    assertTrue("RHS should be LiteralParseNode, got: " + rhs.getClass().getSimpleName(),
      rhs instanceof LiteralParseNode);
    assertEquals(2, lhs.getChildren().size());
    assertEquals("V1", ((ColumnParseNode) lhs.getChildren().get(0)).getName());
    assertEquals("V2", ((ColumnParseNode) lhs.getChildren().get(1)).getName());
  }

  @Test
  public void testPrecedenceArithmeticVsDistance() throws Exception {
    // Additive and multiplicative operators have a higher precedence than distance operators.
    String sql = "SELECT v1 + v2 <-> v3 * v4 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode root = select.getSelect().get(0).getNode();
    assertTrue("Expected L2_DISTANCE node, got: " + root.getClass().getSimpleName(),
      isDistance(root, "L2_DISTANCE"));
    assertEquals(2, root.getChildren().size());
    ParseNode lhs = root.getChildren().get(0);
    ParseNode rhs = root.getChildren().get(1);
    assertTrue("LHS should be AddParseNode, got: " + lhs.getClass().getSimpleName(),
      lhs instanceof AddParseNode);
    assertTrue("RHS should be MultiplyParseNode, got: " + rhs.getClass().getSimpleName(),
      rhs instanceof MultiplyParseNode);
  }

  @Test
  public void testParenthesesOverridePrecedence() throws Exception {
    // Parentheses change the default precedence.
    String sql = "SELECT (v1 <-> v2) + 1.0 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode root = select.getSelect().get(0).getNode();
    assertTrue("Expected AddParseNode, got: " + root.getClass().getSimpleName(),
      root instanceof AddParseNode);
    assertEquals(2, root.getChildren().size());
    ParseNode lhs = root.getChildren().get(0);
    assertTrue("LHS should be L2_DISTANCE node, got: " + lhs.getClass().getSimpleName(),
      isDistance(lhs, "L2_DISTANCE"));
  }

  @Test
  public void testWhereClauseWithDistanceOperator() throws Exception {
    String sql = "SELECT * FROM t WHERE v1 <-> v2 < 0.5";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode where = select.getWhere();
    assertNotNull("WHERE clause should not be null", where);
    assertTrue("WHERE should be LessThanParseNode", where instanceof LessThanParseNode);
    ParseNode lhs = where.getChildren().get(0);
    assertTrue("LHS of WHERE comparison should be L2_DISTANCE node",
      isDistance(lhs, "L2_DISTANCE"));
  }

  @Test
  public void testOrderByWithDistanceOperator() throws Exception {
    String sql = "SELECT doc_id FROM documents ORDER BY embedding <=> :1 LIMIT 10";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    List<OrderByNode> orderBy = select.getOrderBy();
    assertNotNull(orderBy);
    assertEquals(1, orderBy.size());
    ParseNode orderNode = orderBy.get(0).getNode();
    assertTrue("ORDER BY node should be COSINE_DISTANCE node",
      isDistance(orderNode, "COSINE_DISTANCE"));
    assertEquals(2, orderNode.getChildren().size());
    assertTrue(orderNode.getChildren().get(0) instanceof ColumnParseNode);
    assertTrue(orderNode.getChildren().get(1) instanceof BindParseNode);
  }

  @Test
  public void testNoSpaceDistanceOperator() throws Exception {
    String sql = "SELECT v1<->v2, v1<=>v2, v1<#>v2 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    assertEquals(3, select.getSelect().size());
    assertTrue(isDistance(select.getSelect().get(0).getNode(), "L2_DISTANCE"));
    assertTrue(isDistance(select.getSelect().get(1).getNode(), "COSINE_DISTANCE"));
    assertTrue(isDistance(select.getSelect().get(2).getNode(), "INNER_PRODUCT"));
  }

  @Test
  public void testComplexLogicalPrecedence() throws Exception {
    // The precedence order, from highest to lowest, is: distance, comparison, NOT, AND, OR.
    String sql =
      "SELECT * FROM t WHERE v1 <-> v2 < 0.5 AND NOT v3 <=> v4 >= 0.8 OR v1 <#> v2 < -10.0";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode where = select.getWhere();
    assertNotNull(where);
    assertTrue("Root WHERE should be OrParseNode, got: " + where.getClass().getSimpleName(),
      where instanceof OrParseNode);
    assertEquals(2, where.getChildren().size());

    ParseNode andNode = where.getChildren().get(0);
    assertTrue("Left of OR should be AndParseNode", andNode instanceof AndParseNode);
    assertEquals(2, andNode.getChildren().size());

    ParseNode leftComp = andNode.getChildren().get(0);
    assertTrue("Left of AND should be LessThanParseNode", leftComp instanceof LessThanParseNode);
    assertTrue("LHS of < should be L2_DISTANCE node",
      isDistance(leftComp.getChildren().get(0), "L2_DISTANCE"));
    assertTrue("RHS of < should be LiteralParseNode",
      leftComp.getChildren().get(1) instanceof LiteralParseNode);

    ParseNode notNode = andNode.getChildren().get(1);
    assertTrue("Right of AND should be NotParseNode", notNode instanceof NotParseNode);
    ParseNode notChild = notNode.getChildren().get(0);
    assertTrue("Child of NOT should be GreaterThanOrEqualParseNode",
      notChild instanceof GreaterThanOrEqualParseNode);
    assertTrue("LHS of >= should be COSINE_DISTANCE node",
      isDistance(notChild.getChildren().get(0), "COSINE_DISTANCE"));
    assertTrue("RHS of >= should be LiteralParseNode",
      notChild.getChildren().get(1) instanceof LiteralParseNode);

    ParseNode rightComp = where.getChildren().get(1);
    assertTrue("Right of OR should be LessThanParseNode", rightComp instanceof LessThanParseNode);
    assertTrue("LHS of < should be INNER_PRODUCT node",
      isDistance(rightComp.getChildren().get(0), "INNER_PRODUCT"));
  }

  @Test
  public void testFullArithmeticSpectrumWithDistance() throws Exception {
    // The precedence order, from highest to lowest, is: multiplicative, additive, distance,
    // comparison.
    String sql =
      "SELECT (v1 + v2 * 2.0) <-> (v3 - v4 / 2.0 % 1.0) < (threshold + 0.1) * 2.0 FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode root = select.getSelect().get(0).getNode();
    assertTrue("Expected LessThanParseNode, got: " + root.getClass().getSimpleName(),
      root instanceof LessThanParseNode);

    ParseNode distNode = root.getChildren().get(0);
    assertTrue("LHS of < should be L2_DISTANCE node", isDistance(distNode, "L2_DISTANCE"));
    assertEquals(2, distNode.getChildren().size());

    ParseNode op1 = distNode.getChildren().get(0);
    assertTrue("Operand 1 of <-> should be AddParseNode", op1 instanceof AddParseNode);
    assertTrue("Add child 0 should be ColumnParseNode",
      op1.getChildren().get(0) instanceof ColumnParseNode);
    assertTrue("Add child 1 should be MultiplyParseNode",
      op1.getChildren().get(1) instanceof MultiplyParseNode);

    ParseNode op2 = distNode.getChildren().get(1);
    assertTrue("Operand 2 of <-> should be SubtractParseNode", op2 instanceof SubtractParseNode);
    assertTrue("Subtract child 0 should be ColumnParseNode",
      op2.getChildren().get(0) instanceof ColumnParseNode);
    ParseNode modNode = op2.getChildren().get(1);
    assertTrue("Subtract child 1 should be ModulusParseNode", modNode instanceof ModulusParseNode);
    assertTrue("Modulus child 0 should be DivideParseNode",
      modNode.getChildren().get(0) instanceof DivideParseNode);

    ParseNode rhs = root.getChildren().get(1);
    assertTrue("RHS of < should be MultiplyParseNode", rhs instanceof MultiplyParseNode);
    assertTrue("Multiply child 0 should be AddParseNode",
      rhs.getChildren().get(0) instanceof AddParseNode);
  }

  @Test
  public void testBetweenClauseWithDistanceExpressions() throws Exception {
    // A distance expression can be the operand of BETWEEN.
    String sql1 = "SELECT * FROM t WHERE v1 <-> v2 BETWEEN 0.1 AND 0.9";
    SelectStatement select1 = (SelectStatement) new SQLParser(sql1).parseStatement();
    ParseNode where1 = select1.getWhere();
    assertTrue("WHERE should be BetweenParseNode", where1 instanceof BetweenParseNode);
    assertEquals(3, where1.getChildren().size());
    assertTrue("BETWEEN target should be L2_DISTANCE node",
      isDistance(where1.getChildren().get(0), "L2_DISTANCE"));
    assertTrue("BETWEEN lower bound should be LiteralParseNode",
      where1.getChildren().get(1) instanceof LiteralParseNode);
    assertTrue("BETWEEN upper bound should be LiteralParseNode",
      where1.getChildren().get(2) instanceof LiteralParseNode);

    // Distance expressions can be the range limits of BETWEEN.
    String sql2 = "SELECT * FROM t WHERE target_dist BETWEEN v1 <-> v2 AND v3 <=> v4";
    SelectStatement select2 = (SelectStatement) new SQLParser(sql2).parseStatement();
    ParseNode where2 = select2.getWhere();
    assertTrue("WHERE should be BetweenParseNode", where2 instanceof BetweenParseNode);
    assertEquals(3, where2.getChildren().size());
    assertTrue("BETWEEN target should be ColumnParseNode",
      where2.getChildren().get(0) instanceof ColumnParseNode);
    assertTrue("BETWEEN lower bound should be L2_DISTANCE node",
      isDistance(where2.getChildren().get(1), "L2_DISTANCE"));
    assertTrue("BETWEEN upper bound should be COSINE_DISTANCE node",
      isDistance(where2.getChildren().get(2), "COSINE_DISTANCE"));
  }

  @Test
  public void testInListWithDistanceExpression() throws Exception {
    // A distance expression can be the operand of IN.
    String sql = "SELECT * FROM t WHERE v1 <-> v2 IN (0.1, 0.2, 0.3)";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode where = select.getWhere();
    assertTrue("WHERE should be InListParseNode", where instanceof InListParseNode);
    List<ParseNode> children = where.getChildren();
    assertEquals(4, children.size());
    assertTrue("IN target should be L2_DISTANCE node", isDistance(children.get(0), "L2_DISTANCE"));
    assertTrue("IN element 1 should be LiteralParseNode",
      children.get(1) instanceof LiteralParseNode);
    assertTrue("IN element 2 should be LiteralParseNode",
      children.get(2) instanceof LiteralParseNode);
    assertTrue("IN element 3 should be LiteralParseNode",
      children.get(3) instanceof LiteralParseNode);
  }

  @Test
  public void testCaseWhenWithDistanceExpressions() throws Exception {
    // Distance expressions can be in the WHEN conditions and in the results of CASE.
    String sql = "SELECT CASE " + "WHEN v1 <=> v2 < 0.2 THEN v1 <-> v2 "
      + "WHEN v1 <=> v2 < 0.5 THEN (v1 <-> v2) * 1.5 " + "ELSE v1 <#> v2 END FROM t";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    ParseNode root = select.getSelect().get(0).getNode();
    assertTrue("Expected CaseParseNode", root instanceof CaseParseNode);
    List<ParseNode> children = root.getChildren();
    assertEquals(5, children.size());

    assertTrue("THEN 1 should be L2_DISTANCE node", isDistance(children.get(0), "L2_DISTANCE"));
    assertTrue("WHEN 1 should be LessThanParseNode", children.get(1) instanceof LessThanParseNode);
    assertTrue("WHEN 1 LHS should be COSINE_DISTANCE node",
      isDistance(children.get(1).getChildren().get(0), "COSINE_DISTANCE"));

    assertTrue("THEN 2 should be MultiplyParseNode", children.get(2) instanceof MultiplyParseNode);
    assertTrue("THEN 2 child 0 should be L2_DISTANCE node",
      isDistance(children.get(2).getChildren().get(0), "L2_DISTANCE"));
    assertTrue("WHEN 2 should be LessThanParseNode", children.get(3) instanceof LessThanParseNode);
    assertTrue("WHEN 2 LHS should be COSINE_DISTANCE node",
      isDistance(children.get(3).getChildren().get(0), "COSINE_DISTANCE"));

    assertTrue("ELSE should be INNER_PRODUCT node", isDistance(children.get(4), "INNER_PRODUCT"));
  }

  @Test
  public void testChainedDistanceOperatorsAssociativity() throws Exception {
    // A chain of the same distance operator associates from left to right.
    String sql1 = "SELECT v1 <-> v2 <-> v3 FROM t";
    SelectStatement select1 = (SelectStatement) new SQLParser(sql1).parseStatement();
    ParseNode root1 = select1.getSelect().get(0).getNode();
    assertTrue("Root should be L2_DISTANCE node", isDistance(root1, "L2_DISTANCE"));
    assertEquals(2, root1.getChildren().size());
    assertTrue("LHS should be L2_DISTANCE node",
      isDistance(root1.getChildren().get(0), "L2_DISTANCE"));
    assertTrue("RHS should be ColumnParseNode",
      root1.getChildren().get(1) instanceof ColumnParseNode);
    assertEquals("V3", ((ColumnParseNode) root1.getChildren().get(1)).getName());

    // A chain of different distance operators also associates from left to right.
    String sql2 = "SELECT v1 <-> v2 <=> v3 <#> v4 FROM t";
    SelectStatement select2 = (SelectStatement) new SQLParser(sql2).parseStatement();
    ParseNode root2 = select2.getSelect().get(0).getNode();
    assertTrue("Root should be INNER_PRODUCT node", isDistance(root2, "INNER_PRODUCT"));
    assertEquals(2, root2.getChildren().size());

    ParseNode cosNode = root2.getChildren().get(0);
    assertTrue("LHS of <#> should be COSINE_DISTANCE node", isDistance(cosNode, "COSINE_DISTANCE"));
    assertTrue("LHS of <=> should be L2_DISTANCE node",
      isDistance(cosNode.getChildren().get(0), "L2_DISTANCE"));
    assertEquals("V3", ((ColumnParseNode) cosNode.getChildren().get(1)).getName());
    assertEquals("V4", ((ColumnParseNode) root2.getChildren().get(1)).getName());
  }

  @Test
  public void testUnaryMinusPrecedenceWithDistance() throws Exception {
    // Unary minus on a vector operand has a higher precedence than a distance operator.
    String sql1 = "SELECT -v1 <-> v2 FROM t";
    SelectStatement select1 = (SelectStatement) new SQLParser(sql1).parseStatement();
    ParseNode root1 = select1.getSelect().get(0).getNode();
    assertTrue("Root should be L2_DISTANCE node", isDistance(root1, "L2_DISTANCE"));
    ParseNode lhs1 = root1.getChildren().get(0);
    assertTrue("Operand with unary minus should be MultiplyParseNode (negate representation)",
      lhs1 instanceof MultiplyParseNode);
    assertTrue(root1.getChildren().get(1) instanceof ColumnParseNode);

    // With parentheses, unary minus applies to the result of the distance expression.
    String sql2 = "SELECT -(v1 <-> v2) FROM t";
    SelectStatement select2 = (SelectStatement) new SQLParser(sql2).parseStatement();
    ParseNode root2 = select2.getSelect().get(0).getNode();
    assertTrue("Root should be MultiplyParseNode (negate representation)",
      root2 instanceof MultiplyParseNode);
    assertTrue("Child of negate should be L2_DISTANCE node",
      isDistance(root2.getChildren().get(0), "L2_DISTANCE"));
  }

  @Test
  public void testNestedFunctionsAndComplexOrderBy() throws Exception {
    // A distance expression can be an argument of a scalar function.
    String sql1 = "SELECT ROUND(v1 <-> v2, 2) > ROUND(v3 <=> v4, 2) FROM t";
    SelectStatement select1 = (SelectStatement) new SQLParser(sql1).parseStatement();
    ParseNode root1 = select1.getSelect().get(0).getNode();
    assertTrue("Expected GreaterThanParseNode", root1 instanceof GreaterThanParseNode);
    ParseNode leftFn = root1.getChildren().get(0);
    ParseNode rightFn = root1.getChildren().get(1);
    assertTrue("LHS should be FunctionParseNode", leftFn instanceof FunctionParseNode);
    assertTrue("RHS should be FunctionParseNode", rightFn instanceof FunctionParseNode);
    assertTrue("LHS function arg should be L2_DISTANCE node",
      isDistance(leftFn.getChildren().get(0), "L2_DISTANCE"));
    assertTrue("RHS function arg should be COSINE_DISTANCE node",
      isDistance(rightFn.getChildren().get(0), "COSINE_DISTANCE"));

    // ORDER BY can use a weighted sum of two distance metrics.
    String sql2 = "SELECT doc_id FROM documents "
      + "ORDER BY (embedding <=> :1) * 0.7 + (embedding <-> :2) * 0.3 ASC, doc_id DESC";
    SelectStatement select2 = (SelectStatement) new SQLParser(sql2).parseStatement();
    List<OrderByNode> orderBy = select2.getOrderBy();
    assertNotNull(orderBy);
    assertEquals(2, orderBy.size());

    OrderByNode order1 = orderBy.get(0);
    assertTrue("First ORDER BY should be ascending", order1.isAscending());
    ParseNode orderNode1 = order1.getNode();
    assertTrue("Weighted combination should be AddParseNode", orderNode1 instanceof AddParseNode);
    assertEquals(2, orderNode1.getChildren().size());

    ParseNode term1 = orderNode1.getChildren().get(0);
    assertTrue("Term 1 should be MultiplyParseNode", term1 instanceof MultiplyParseNode);
    assertTrue("Term 1 child 0 should be COSINE_DISTANCE node",
      isDistance(term1.getChildren().get(0), "COSINE_DISTANCE"));

    ParseNode term2 = orderNode1.getChildren().get(1);
    assertTrue("Term 2 should be MultiplyParseNode", term2 instanceof MultiplyParseNode);
    assertTrue("Term 2 child 0 should be L2_DISTANCE node",
      isDistance(term2.getChildren().get(0), "L2_DISTANCE"));

    OrderByNode order2 = orderBy.get(1);
    assertTrue("Second ORDER BY should be descending", !order2.isAscending());
    assertTrue("Second ORDER BY node should be ColumnParseNode",
      order2.getNode() instanceof ColumnParseNode);
  }

  @Test
  public void testIsNullWithDistanceExpression() throws Exception {
    // A distance expression can be the operand of IS NULL.
    String sql1 = "SELECT * FROM t WHERE v1 <-> v2 IS NULL";
    SelectStatement select1 = (SelectStatement) new SQLParser(sql1).parseStatement();
    ParseNode where1 = select1.getWhere();
    assertTrue("WHERE should be IsNullParseNode", where1 instanceof IsNullParseNode);
    assertTrue("Should be IS NULL (not negated)", !((IsNullParseNode) where1).isNegate());
    assertTrue("Child of IS NULL should be L2_DISTANCE node",
      isDistance(where1.getChildren().get(0), "L2_DISTANCE"));

    // A distance expression can be the operand of IS NOT NULL.
    String sql2 = "SELECT * FROM t WHERE v1 <=> v2 IS NOT NULL";
    SelectStatement select2 = (SelectStatement) new SQLParser(sql2).parseStatement();
    ParseNode where2 = select2.getWhere();
    assertTrue("WHERE should be IsNullParseNode", where2 instanceof IsNullParseNode);
    assertTrue("Should be IS NOT NULL (negated)", ((IsNullParseNode) where2).isNegate());
    assertTrue("Child of IS NOT NULL should be COSINE_DISTANCE node",
      isDistance(where2.getChildren().get(0), "COSINE_DISTANCE"));
  }

  @Test
  public void testInterleavedArithmeticAndDistancePrecedence() throws Exception {
    // Without parentheses, + binds before the distance operators, which associate from left to
    // right.
    String sql1 = "SELECT v1 <-> v2 + v3 <=> v4 FROM t";
    SelectStatement select1 = (SelectStatement) new SQLParser(sql1).parseStatement();
    ParseNode root1 = select1.getSelect().get(0).getNode();
    assertTrue("Root should be COSINE_DISTANCE node", isDistance(root1, "COSINE_DISTANCE"));
    assertEquals(2, root1.getChildren().size());

    ParseNode lhsCos = root1.getChildren().get(0);
    assertTrue("LHS of <=> should be L2_DISTANCE node", isDistance(lhsCos, "L2_DISTANCE"));
    ParseNode rhsCos = root1.getChildren().get(1);
    assertTrue("RHS of <=> should be ColumnParseNode", rhsCos instanceof ColumnParseNode);
    assertEquals("V4", ((ColumnParseNode) rhsCos).getName());

    assertEquals("V1", ((ColumnParseNode) lhsCos.getChildren().get(0)).getName());
    ParseNode rhsL2 = lhsCos.getChildren().get(1);
    assertTrue("RHS of <-> should be AddParseNode", rhsL2 instanceof AddParseNode);
    assertEquals("V2", ((ColumnParseNode) rhsL2.getChildren().get(0)).getName());
    assertEquals("V3", ((ColumnParseNode) rhsL2.getChildren().get(1)).getName());

    // Parentheses change the default precedence, so + applies to the two distance results.
    String sql2 = "SELECT (v1 <-> v2) + (v3 <=> v4) FROM t";
    SelectStatement select2 = (SelectStatement) new SQLParser(sql2).parseStatement();
    ParseNode root2 = select2.getSelect().get(0).getNode();
    assertTrue("Root should be AddParseNode", root2 instanceof AddParseNode);
    assertEquals(2, root2.getChildren().size());
    assertTrue("LHS of + should be L2_DISTANCE node",
      isDistance(root2.getChildren().get(0), "L2_DISTANCE"));
    assertTrue("RHS of + should be COSINE_DISTANCE node",
      isDistance(root2.getChildren().get(1), "COSINE_DISTANCE"));
  }

  /**
   * Comparison operators that start with {@code <} must not parse as distance operators. For
   * example, {@code a<-1} is a less-than comparison with a negative literal.
   */
  @Test
  public void testLessThanComparisonsUnaffected() throws Exception {
    String[][] cases =
      { { "a<-1", "<" }, { "a < -1", "<" }, { "a<=b", "<=" }, { "a<>b", "!=" }, { "a<b", "<" } };
    for (String[] c : cases) {
      SelectStatement select =
        (SelectStatement) new SQLParser("SELECT * FROM t WHERE " + c[0]).parseStatement();
      ParseNode where = select.getWhere();
      assertTrue(c[0] + " should parse as a comparison", where instanceof ComparisonParseNode);
      assertEquals(c[0], c[1], QueryUtil.toSQL(((ComparisonParseNode) where).getFilterOp()));
    }
  }
}
