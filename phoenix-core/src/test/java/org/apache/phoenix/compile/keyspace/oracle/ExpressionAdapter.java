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
package org.apache.phoenix.compile.keyspace.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.keyspace.ExpressionNormalizer;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;

/**
 * Converts a Phoenix {@link Expression} tree into an {@link AbstractExpression} the oracle
 * can ingest. The adapter aims to be <em>complete</em>: if a sub-expression can't be mapped
 * precisely to a PK-keyable predicate, it becomes {@link AbstractExpression#unknown(String)}
 * rather than throwing. Unknowns are safe over-approximations that let the harness still
 * run a useful soundness check on the rest of the tree.
 * <p>
 * Supported shapes:
 * <ul>
 * <li>{@link AndExpression} / {@link OrExpression} — recurse on children.</li>
 * <li>{@link ComparisonExpression} on a PK column with a {@link LiteralExpression} RHS —
 * mapped to a {@link AbstractExpression.Pred} with the column's decoded value.</li>
 * <li>{@link ComparisonExpression} between two {@link org.apache.phoenix.expression.
 * RowValueConstructorExpression}s (RVC inequality) — lex-expanded via
 * {@link ExpressionNormalizer#normalize}, then the result is re-visited.</li>
 * <li>{@link InListExpression} on a PK column — expanded to an {@link AbstractExpression.Or}
 * of {@link AbstractExpression#pred} equalities.</li>
 * <li>{@link LikeExpression} {@code col LIKE 'prefix%'} — mapped to
 * {@code (col &gt;= prefix) AND (col &lt; nextKey(prefix))}.</li>
 * </ul>
 * Shapes mapped to {@link AbstractExpression.Unknown}:
 * <ul>
 * <li>Non-PK column predicates.</li>
 * <li>Scalar functions / {@link org.apache.phoenix.expression.CoerceExpression} on LHS.</li>
 * <li>{@link IsNullExpression} (the abstract model doesn't carry null yet).</li>
 * <li>{@code NOT_EQUAL} comparisons.</li>
 * <li>LIKE with leading wildcard, case-insensitive LIKE, LIKE on non-PK, non-literal RHS.</li>
 * <li>Any other shape.</li>
 * </ul>
 * The semantic guarantee: {@code rows(originalExpr) ⊆ rows(adapter.convert(originalExpr))} —
 * the adapter's output never rejects a row the original accepts. That's what makes the
 * Unknown-as-true over-approximation safe for the harness soundness check.
 */
public final class ExpressionAdapter {

  private final PTable table;
  private final int nPk;

  public ExpressionAdapter(PTable table) {
    this.table = table;
    this.nPk = table.getPKColumns().size();
  }

  /**
   * Entry point: normalize the expression (lex-expands RVC inequalities and other shapes
   * V2 handles) before converting.
   */
  public AbstractExpression convert(Expression expr) {
    try {
      Expression normalized = ExpressionNormalizer.normalize(expr);
      return convertNode(normalized == null ? expr : normalized);
    } catch (SQLException e) {
      return AbstractExpression.unknown("normalize failed: " + e.getMessage());
    }
  }

  private AbstractExpression convertNode(Expression expr) {
    if (expr instanceof AndExpression) {
      List<Expression> kids = expr.getChildren();
      List<AbstractExpression> out = new ArrayList<>(kids.size());
      for (Expression k : kids) {
        out.add(convertNode(k));
      }
      return AbstractExpression.And.of(out);
    }
    if (expr instanceof OrExpression) {
      List<Expression> kids = expr.getChildren();
      List<AbstractExpression> out = new ArrayList<>(kids.size());
      for (Expression k : kids) {
        out.add(convertNode(k));
      }
      return AbstractExpression.Or.of(out);
    }
    if (expr instanceof ComparisonExpression) {
      return convertComparison((ComparisonExpression) expr);
    }
    if (expr instanceof InListExpression) {
      return convertInList((InListExpression) expr);
    }
    if (expr instanceof LikeExpression) {
      return convertLike((LikeExpression) expr);
    }
    if (expr instanceof IsNullExpression) {
      // The abstract model doesn't carry NULL as a distinguishable value yet. Treat IS
      // [NOT] NULL as Unknown so the oracle doesn't narrow based on it. Safe
      // over-approximation.
      return AbstractExpression.unknown("IS [NOT] NULL not modeled");
    }
    return AbstractExpression.unknown(
      "unsupported node type: " + expr.getClass().getSimpleName());
  }

  private AbstractExpression convertComparison(ComparisonExpression cmp) {
    Expression lhs = cmp.getChildren().get(0);
    Expression rhs = cmp.getChildren().get(1);
    if (!(lhs instanceof RowKeyColumnExpression)) {
      return AbstractExpression.unknown("comparison LHS not a bare PK column: " + lhs);
    }
    if (cmp.getFilterOp() == CompareOperator.NOT_EQUAL) {
      return AbstractExpression.unknown("NOT_EQUAL not keyable");
    }
    int pkPos = ((RowKeyColumnExpression) lhs).getPosition();
    if (pkPos < 0 || pkPos >= nPk) {
      return AbstractExpression.unknown("LHS PK position out of range: " + pkPos);
    }
    Comparable<?> value = evaluateLiteral(rhs);
    if (value == null) {
      return AbstractExpression.unknown("could not evaluate RHS literal/stateless expr: " + rhs);
    }
    return AbstractExpression.pred(pkPos, mapOp(cmp.getFilterOp()), value);
  }

  private AbstractExpression convertInList(InListExpression in) {
    Expression lhs = in.getChildren().get(0);
    if (lhs instanceof RowValueConstructorExpression) {
      return convertRvcInList(in, (RowValueConstructorExpression) lhs);
    }
    if (!(lhs instanceof RowKeyColumnExpression)) {
      // function-of-PK IN is out of scope.
      return AbstractExpression.unknown("IN list LHS not a bare PK column: " + lhs);
    }
    int pkPos = ((RowKeyColumnExpression) lhs).getPosition();
    if (pkPos < 0 || pkPos >= nPk) {
      return AbstractExpression.unknown("IN list LHS PK position out of range: " + pkPos);
    }
    List<AbstractExpression> branches = new ArrayList<>(in.getKeyExpressions().size());
    for (Expression v : in.getKeyExpressions()) {
      Comparable<?> value = evaluateLiteral(v);
      if (value == null) {
        return AbstractExpression.unknown("IN list value not evaluable: " + v);
      }
      branches.add(AbstractExpression.pred(pkPos, AbstractExpression.Op.EQ, value));
    }
    if (branches.isEmpty()) {
      // Empty IN list never matches — model as a contradiction via (d = x AND d != x).
      // Simpler: Unknown would over-approximate; here we want to match reality. Use an
      // impossible predicate: col < MIN && col > MAX... but we don't have "MIN/MAX". Just
      // fall back to Unknown; the planner typically short-circuits empty IN-lists anyway.
      return AbstractExpression.unknown("empty IN list");
    }
    return AbstractExpression.Or.of(branches);
  }

  /**
   * Convert {@code (c1, ..., cK) IN ((v1a, ..., vKa), (v1b, ..., vKb), ...)} to an OR of
   * per-row AND chains of equalities. Requires every LHS child to be a bare PK column
   * {@link RowKeyColumnExpression} and each RHS row-value to be an
   * {@link RowValueConstructorExpression} of literals. Phoenix may also pack row values
   * as {@link LiteralExpression}s of concatenated bytes after its sort-and-coerce pass; in
   * that case we fall back to Unknown.
   */
  private AbstractExpression convertRvcInList(InListExpression in,
    RowValueConstructorExpression lhsRvc) {
    int k = lhsRvc.getChildren().size();
    int[] pkPositions = new int[k];
    for (int i = 0; i < k; i++) {
      Expression child = lhsRvc.getChildren().get(i);
      if (!(child instanceof RowKeyColumnExpression)) {
        return AbstractExpression.unknown("RVC IN LHS child not bare PK col: " + child);
      }
      int pos = ((RowKeyColumnExpression) child).getPosition();
      if (pos < 0 || pos >= nPk) {
        return AbstractExpression.unknown("RVC IN LHS PK pos out of range: " + pos);
      }
      pkPositions[i] = pos;
    }
    List<AbstractExpression> branches = new ArrayList<>(in.getKeyExpressions().size());
    for (Expression rv : in.getKeyExpressions()) {
      if (!(rv instanceof RowValueConstructorExpression)) {
        return AbstractExpression.unknown("RVC IN row value not RVC: " + rv);
      }
      RowValueConstructorExpression rvc = (RowValueConstructorExpression) rv;
      List<AbstractExpression> conjuncts = new ArrayList<>(k);
      int rvcChildren = Math.min(k, rvc.getChildren().size());
      for (int i = 0; i < rvcChildren; i++) {
        Comparable<?> value = evaluateLiteral(rvc.getChildren().get(i));
        if (value == null) {
          return AbstractExpression.unknown("RVC IN value not evaluable");
        }
        conjuncts.add(AbstractExpression.pred(pkPositions[i], AbstractExpression.Op.EQ, value));
      }
      branches.add(AbstractExpression.And.of(conjuncts));
    }
    if (branches.isEmpty()) {
      return AbstractExpression.unknown("empty RVC IN list");
    }
    return AbstractExpression.Or.of(branches);
  }

  private AbstractExpression convertLike(LikeExpression like) {
    if (like.getLikeType() == LikeType.CASE_INSENSITIVE) {
      return AbstractExpression.unknown("case-insensitive LIKE");
    }
    Expression lhs = like.getChildren().get(0);
    Expression rhs = like.getChildren().get(1);
    if (!(lhs instanceof RowKeyColumnExpression)) {
      return AbstractExpression.unknown("LIKE LHS not a bare PK column: " + lhs);
    }
    if (!(rhs instanceof LiteralExpression)) {
      return AbstractExpression.unknown("LIKE RHS not a bare literal: " + rhs);
    }
    if (like.startsWithWildcard()) {
      return AbstractExpression.unknown("LIKE pattern starts with wildcard");
    }
    int pkPos = ((RowKeyColumnExpression) lhs).getPosition();
    if (pkPos < 0 || pkPos >= nPk) {
      return AbstractExpression.unknown("LIKE LHS PK position out of range: " + pkPos);
    }
    String prefix = like.getLiteralPrefix();
    if (prefix == null || prefix.isEmpty()) {
      return AbstractExpression.unknown("LIKE has empty prefix");
    }
    // Construct `col >= prefix AND col < nextString(prefix)`. nextString bumps the last
    // character up by 1; for strings it's the lex successor.
    String upper = nextString(prefix);
    AbstractExpression lower =
      AbstractExpression.pred(pkPos, AbstractExpression.Op.GE, prefix);
    if (upper == null) {
      // Bump overflowed — upper is unbounded. Just lower bound alone.
      return lower;
    }
    AbstractExpression upperBound =
      AbstractExpression.pred(pkPos, AbstractExpression.Op.LT, upper);
    return AbstractExpression.and(lower, upperBound);
  }

  /** Returns the lex-successor of {@code s}, or {@code null} if the string is at the top. */
  private static String nextString(String s) {
    StringBuilder sb = new StringBuilder(s);
    for (int i = sb.length() - 1; i >= 0; i--) {
      char c = sb.charAt(i);
      if (c < Character.MAX_VALUE) {
        sb.setCharAt(i, (char) (c + 1));
        return sb.substring(0, i + 1);
      }
      sb.setLength(i); // all-max chars — trim and retry at higher position
    }
    return null;
  }

  /**
   * Evaluate a "literal-like" RHS to a typed Comparable. Unwraps Phoenix's common
   * function wrappers around literal values (e.g. {@code TO_BIGINT(5)} which Phoenix
   * compiles as a {@link org.apache.phoenix.expression.CoerceExpression} or similar
   * when the LHS's declared type differs from the integer literal's parsed type).
   * These wrappers are semantically literals — they evaluate to a fixed value with no
   * row context — so {@code rhs.evaluate(null, ptr)} succeeds and we can decode the
   * resulting bytes regardless of whether {@code rhs} is a bare
   * {@link LiteralExpression} or a wrapper around one.
   */
  private Comparable<?> evaluateLiteral(Expression rhs) {
    if (rhs == null) return null;
    // Accept any expression that is stateless (evaluates without row context) — this
    // includes LiteralExpression and any CoerceExpression / arithmetic-of-literals chain.
    if (!rhs.isStateless()) return null;
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    if (!rhs.evaluate(null, ptr) || ptr.getLength() == 0) {
      return null;
    }
    Object v = rhs.getDataType().toObject(ptr, rhs.getDataType(), SortOrder.ASC);
    return (v instanceof Comparable) ? (Comparable<?>) v : null;
  }

  private static AbstractExpression.Op mapOp(CompareOperator op) {
    switch (op) {
      case EQUAL: return AbstractExpression.Op.EQ;
      case LESS: return AbstractExpression.Op.LT;
      case LESS_OR_EQUAL: return AbstractExpression.Op.LE;
      case GREATER: return AbstractExpression.Op.GT;
      case GREATER_OR_EQUAL: return AbstractExpression.Op.GE;
      default: throw new IllegalStateException("unexpected op " + op);
    }
  }

  public int nPk() {
    return nPk;
  }

  public PTable table() {
    return table;
  }
}
