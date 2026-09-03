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
package org.apache.phoenix.compile.keyspace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class ExpressionNormalizerTest {

  /**
   * A bare-bones stand-in for a PK column that gives ComparisonExpression.create an LHS with
   * a data type but no constant value, so constant folding is skipped.
   */
  private static final class TestColumn implements PDatum {
    private final PDataType type;

    TestColumn(PDataType type) {
      this.type = type;
    }

    @Override
    public boolean isNullable() {
      return true;
    }

    @Override
    public PDataType getDataType() {
      return type;
    }

    @Override
    public Integer getMaxLength() {
      return null;
    }

    @Override
    public Integer getScale() {
      return null;
    }

    @Override
    public SortOrder getSortOrder() {
      return SortOrder.ASC;
    }
  }

  private static RowKeyColumnExpression col(int position) {
    return new RowKeyColumnExpression(new TestColumn(PVarchar.INSTANCE),
      new RowKeyValueAccessor(Arrays.<PDatum>asList(new TestColumn(PVarchar.INSTANCE),
        new TestColumn(PVarchar.INSTANCE), new TestColumn(PVarchar.INSTANCE)), position));
  }

  private static Expression lit(String s) throws Exception {
    return LiteralExpression.newConstant(s, PVarchar.INSTANCE);
  }

  private static Expression cmp(CompareOperator op, Expression lhs, Expression rhs) throws Exception {
    return ComparisonExpression.create(op, Arrays.asList(lhs, rhs), new ImmutableBytesWritable(), true);
  }

  private static Expression rvc(Expression... children) {
    return new RowValueConstructorExpression(new ArrayList<>(Arrays.asList(children)), false);
  }

  @Test
  public void rvcEqualityIsNotRewrittenHere() throws Exception {
    // ComparisonExpression.create already expanded equality RVCs; the normalizer should
    // leave the result alone.
    Expression e = cmp(CompareOperator.EQUAL, rvc(col(0), col(1)), rvc(lit("a"), lit("b")));
    // ComparisonExpression.create returns AndExpression directly for RVC equality.
    assertTrue(e instanceof AndExpression);
    Expression normalized = ExpressionNormalizer.normalize(e);
    assertEquals(e, normalized);
  }

  @Test
  public void rvcGreaterRewritesToLexicographicOr() throws Exception {
    //   (c1, c2) > (a, b)
    //     => (c1 > a) OR (c1 = a AND c2 > b)
    Expression rvcCmp = new ComparisonExpression(
      Arrays.<Expression>asList(rvc(col(0), col(1)), rvc(lit("a"), lit("b"))),
      CompareOperator.GREATER);
    Expression normalized = ExpressionNormalizer.normalize(rvcCmp);
    assertTrue(normalized instanceof OrExpression);
    List<Expression> orKids = normalized.getChildren();
    assertEquals(2, orKids.size());
    assertTrue(orKids.get(0) instanceof ComparisonExpression);
    assertEquals(CompareOperator.GREATER,
      ((ComparisonExpression) orKids.get(0)).getFilterOp());
    assertTrue(orKids.get(1) instanceof AndExpression);
    List<Expression> andKids = orKids.get(1).getChildren();
    assertEquals(CompareOperator.EQUAL,
      ((ComparisonExpression) andKids.get(0)).getFilterOp());
    assertEquals(CompareOperator.GREATER,
      ((ComparisonExpression) andKids.get(1)).getFilterOp());
  }

  @Test
  public void rvcGreaterOrEqualKeepsInclusiveOnFinalTerm() throws Exception {
    // (c1, c2, c3) >= (a, b, c)
    //   => (c1 > a) OR (c1 = a AND c2 > b) OR (c1 = a AND c2 = b AND c3 >= c)
    Expression rvcCmp = new ComparisonExpression(
      Arrays.<Expression>asList(rvc(col(0), col(1), col(2)), rvc(lit("a"), lit("b"), lit("c"))),
      CompareOperator.GREATER_OR_EQUAL);
    Expression normalized = ExpressionNormalizer.normalize(rvcCmp);
    assertTrue(normalized instanceof OrExpression);
    List<Expression> orKids = normalized.getChildren();
    assertEquals(3, orKids.size());
    assertEquals(CompareOperator.GREATER,
      ((ComparisonExpression) orKids.get(0)).getFilterOp());
    List<Expression> lastAnd = orKids.get(2).getChildren();
    assertEquals(CompareOperator.GREATER_OR_EQUAL,
      ((ComparisonExpression) lastAnd.get(lastAnd.size() - 1)).getFilterOp());
  }

  @Test
  public void rvcLessRewritesSymmetrically() throws Exception {
    Expression rvcCmp = new ComparisonExpression(
      Arrays.<Expression>asList(rvc(col(0), col(1)), rvc(lit("a"), lit("b"))),
      CompareOperator.LESS);
    Expression normalized = ExpressionNormalizer.normalize(rvcCmp);
    assertTrue(normalized instanceof OrExpression);
    ComparisonExpression first = (ComparisonExpression) normalized.getChildren().get(0);
    assertEquals(CompareOperator.LESS, first.getFilterOp());
  }

  @Test
  public void scalarInListIsNotRewritten() throws Exception {
    // The normalizer previously expanded scalar IN lists to OR-of-equalities. That
    // changed the tree shape (OrExpression replaced InListExpression) and added
    // TO_VARCHAR coercions around literals via ComparisonExpression.create — both
    // broke callers that inspect the WHERE tree (HavingCompiler, WhereCompilerTest,
    // PhoenixResultSetMetadataTest, NullValueTest). The visitor now handles scalar
    // IN directly, so the normalizer leaves the node alone.
    List<Expression> children = new ArrayList<>();
    children.add(col(0));
    children.add(lit("a"));
    children.add(lit("b"));
    children.add(lit("c"));
    Expression in = new InListExpression(children, true);

    Expression normalized = ExpressionNormalizer.normalize(in);
    assertSame(in, normalized);
  }

  @Test
  public void nestedInListAndRvcInequalityInsideAnd() throws Exception {
    List<Expression> inChildren = new ArrayList<>();
    inChildren.add(col(0));
    inChildren.add(lit("a"));
    inChildren.add(lit("b"));
    Expression inExpr = new InListExpression(inChildren, true);

    Expression rvcCmp = new ComparisonExpression(
      Arrays.<Expression>asList(rvc(col(1), col(2)), rvc(lit("x"), lit("y"))),
      CompareOperator.GREATER);

    Expression andNode = AndExpression.create(
      new ArrayList<Expression>(Arrays.asList(inExpr, rvcCmp)));
    Expression normalized = ExpressionNormalizer.normalize(andNode);

    assertTrue(normalized instanceof AndExpression);
    List<Expression> andKids = normalized.getChildren();
    // The scalar IN list is left alone; only the RVC inequality is rewritten to an
    // OR-of-ANDs.
    assertTrue(andKids.get(0) instanceof InListExpression);
    assertTrue(andKids.get(1) instanceof OrExpression);
  }

  @Test
  public void plainScalarComparisonIsUnchanged() throws Exception {
    Expression scalar = cmp(CompareOperator.GREATER, col(0), lit("x"));
    Expression normalized = ExpressionNormalizer.normalize(scalar);
    assertSame(scalar, normalized);
  }
}
