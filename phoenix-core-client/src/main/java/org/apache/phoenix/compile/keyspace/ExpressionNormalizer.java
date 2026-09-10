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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;

/**
 * Rewrites a WHERE {@link Expression} into the canonical AND/OR form the v2 key-space model
 * operates on. One rewrite is applied bottom-up:
 * <ul>
 * <li><b>RVC inequality</b>: {@code (c1,...,cK) OP (v1,...,vK)} for OP in
 * {@code <, ≤, >, ≥} expands to the lexicographic OR of ANDs. Example:
 * {@code (c1,c2,c3) > (v1,v2,v3)} becomes
 * {@code (c1>v1) OR (c1=v1 AND c2>v2) OR (c1=v1 AND c2=v2 AND c3>v3)}. Equality is already
 * expanded upstream by {@link ComparisonExpression#create}.</li>
 * </ul>
 * This rewrite is load-bearing: by replacing RVC inequality with an equivalent AND/OR tree
 * we guarantee that per-dim intersection composes correctly with any other scalar predicate,
 * matching the design's key-space model.
 * <p>
 * <b>IN lists are not rewritten here.</b> Scalar {@code a IN (v1,...,vK)} and RVC
 * {@code (c1,...,cK) IN ((r11,...,r1K), ...)} are both left intact; the visitor consumes
 * them directly via
 * {@link KeySpaceExpressionVisitor#visitLeave(InListExpression, java.util.List)}.
 * An earlier draft rewrote scalar IN into an OR chain of equalities here, but that
 * reshaped the Expression tree in ways downstream consumers (HavingCompiler, WhereCompiler
 * tree-equality assertions) didn't expect and wrapped literals in spurious TO_VARCHAR
 * coercions. See {@code rewriteScalarInList}'s comment for the history.
 * <p>
 * BETWEEN is lowered at parse time by
 * {@link org.apache.phoenix.compile.StatementNormalizer} and does not reach this pass.
 */
public final class ExpressionNormalizer {

  private ExpressionNormalizer() {
  }

  public static Expression normalize(Expression root) throws SQLException {
    if (root == null) {
      return null;
    }
    // Fast-path: scan the tree once looking for any node that would actually be rewritten
    // (RVC inequalities and scalar IN lists). If none exist, skip the full rewrite walk
    // entirely — avoids the cost of rebuilding the tree for scalar ORs and simple
    // comparisons that are the vast majority of real-world queries.
    if (!needsRewrite(root)) {
      return root;
    }
    return rewrite(root);
  }

  /**
   * Cheap predicate: returns true iff the tree contains at least one RVC inequality
   * {@code (a,b) OP (v,w)} or a scalar {@link InListExpression}. These are the only two
   * node shapes rewriteNode transforms; anything else is a pass-through.
   */
  private static boolean needsRewrite(Expression e) {
    if (e instanceof ComparisonExpression) {
      ComparisonExpression cmp = (ComparisonExpression) e;
      if (isInequality(cmp.getFilterOp())) {
        java.util.List<Expression> kids = cmp.getChildren();
        if (kids.size() >= 2
          && kids.get(0) instanceof RowValueConstructorExpression
          && kids.get(1) instanceof RowValueConstructorExpression) {
          return true;
        }
      }
    }
    if (e instanceof InListExpression) {
      InListExpression in = (InListExpression) e;
      if (!(in.getChildren().get(0) instanceof RowValueConstructorExpression)) {
        return true;
      }
    }
    java.util.List<Expression> children = e.getChildren();
    if (children == null) {
      return false;
    }
    for (int i = 0; i < children.size(); i++) {
      if (needsRewrite(children.get(i))) {
        return true;
      }
    }
    return false;
  }

  private static Expression rewrite(Expression e) throws SQLException {
    List<Expression> children = e.getChildren();
    if (children == null || children.isEmpty()) {
      return e;
    }
    List<Expression> newChildren = null;
    for (int i = 0; i < children.size(); i++) {
      Expression orig = children.get(i);
      Expression rewritten = rewrite(orig);
      if (rewritten != orig) {
        if (newChildren == null) {
          newChildren = new ArrayList<>(children);
        }
        newChildren.set(i, rewritten);
      }
    }
    Expression withNewChildren = (newChildren == null) ? e : cloneWithChildren(e, newChildren);
    return rewriteNode(withNewChildren);
  }

  private static Expression rewriteNode(Expression e) throws SQLException {
    if (e instanceof ComparisonExpression) {
      Expression rewritten = rewriteRvcInequality((ComparisonExpression) e);
      if (rewritten != null) {
        return rewritten;
      }
    }
    if (e instanceof InListExpression) {
      Expression rewritten = rewriteScalarInList((InListExpression) e);
      if (rewritten != null) {
        return rewritten;
      }
    }
    return e;
  }

  /**
   * Expand {@code (c1,...,cK) OP (v1,...,vK)} for strict/non-strict inequalities into the
   * lexicographic OR-of-ANDs form. Returns {@code null} if the node is not an RVC
   * inequality, so the caller keeps the original.
   */
  private static Expression rewriteRvcInequality(ComparisonExpression cmp) throws SQLException {
    CompareOperator op = cmp.getFilterOp();
    if (!isInequality(op)) {
      return null;
    }
    List<Expression> operands = cmp.getChildren();
    Expression lhs = operands.get(0);
    Expression rhs = operands.get(1);
    if (!(lhs instanceof RowValueConstructorExpression)
      || !(rhs instanceof RowValueConstructorExpression)) {
      return null;
    }
    List<Expression> lhsCols = lhs.getChildren();
    List<Expression> rhsCols = rhs.getChildren();
    int k = Math.min(lhsCols.size(), rhsCols.size());
    if (k == 0) {
      return null;
    }
    if (k == 1) {
      return makeCompare(op, lhsCols.get(0), rhsCols.get(0));
    }

    boolean strict = op == CompareOperator.GREATER || op == CompareOperator.LESS;
    CompareOperator strictOp = (op == CompareOperator.GREATER
      || op == CompareOperator.GREATER_OR_EQUAL) ? CompareOperator.GREATER : CompareOperator.LESS;
    CompareOperator finalOp = strict ? strictOp
      : (strictOp == CompareOperator.GREATER ? CompareOperator.GREATER_OR_EQUAL
        : CompareOperator.LESS_OR_EQUAL);

    List<Expression> orTerms = new ArrayList<>(k);
    for (int i = 0; i < k; i++) {
      List<Expression> andTerms = new ArrayList<>(i + 1);
      for (int j = 0; j < i; j++) {
        andTerms.add(makeCompare(CompareOperator.EQUAL, lhsCols.get(j), rhsCols.get(j)));
      }
      CompareOperator tailOp = (i == k - 1) ? finalOp : strictOp;
      andTerms.add(makeCompare(tailOp, lhsCols.get(i), rhsCols.get(i)));
      orTerms.add(andTerms.size() == 1 ? andTerms.get(0) : AndExpression.create(andTerms));
    }
    return new OrExpression(orTerms);
  }

  /**
   * Previously this rewrote scalar IN lists to OR chains of equalities so that the
   * existing equality/OR visitor paths could handle them. That rewrite changed the
   * Expression tree shape — callers that inspect the WHERE clause (e.g. HavingCompiler
   * "HAVING IN → WHERE IN" lowering, WhereCompiler assertions) saw an OrExpression
   * where they expected an InListExpression, breaking many tests that assert on tree
   * equality. Plus, the rewrite wrapped literals in TO_VARCHAR coercions via
   * ComparisonExpression.create, distorting the tree further.
   * <p>
   * Now the visitor handles scalar {@code InListExpression} directly (see
   * {@link KeySpaceExpressionVisitor#visitLeave(InListExpression, List)}), so there's
   * no need to rewrite here. This method is kept as a documented no-op to make the
   * normalization boundary explicit.
   */
  private static Expression rewriteScalarInList(InListExpression in) throws SQLException {
    return null;
  }

  private static Expression makeCompare(CompareOperator op, Expression lhs, Expression rhs)
    throws SQLException {
    return ComparisonExpression.create(op, Arrays.asList(lhs, rhs),
      new ImmutableBytesWritable(), true);
  }

  private static boolean isInequality(CompareOperator op) {
    return op == CompareOperator.GREATER || op == CompareOperator.GREATER_OR_EQUAL
      || op == CompareOperator.LESS || op == CompareOperator.LESS_OR_EQUAL;
  }

  private static Expression cloneWithChildren(Expression original, List<Expression> newChildren)
    throws SQLException {
    if (original instanceof AndExpression) {
      return AndExpression.create(newChildren);
    }
    if (original instanceof OrExpression) {
      return new OrExpression(newChildren);
    }
    if (original instanceof ComparisonExpression) {
      return new ComparisonExpression(newChildren, ((ComparisonExpression) original).getFilterOp());
    }
    return original;
  }
}
