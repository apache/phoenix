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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;

/**
 * V2 variant of {@link org.apache.phoenix.compile.WhereOptimizer.RemoveExtractedNodesVisitor}. It
 * removes AND conjuncts that the scan applies, and it drops TRUE literals from the AND that it
 * rebuilds. An {@link OrExpression} stays whole or goes away as a unit, as in the v1 visitor.
 */
final class RemoveExtractedNodesVisitorV2 extends StatelessTraverseNoExpressionVisitor<Expression> {
  private final Set<Expression> nodesToRemove;

  RemoveExtractedNodesVisitorV2(Set<Expression> nodesToRemove) {
    this.nodesToRemove = nodesToRemove;
  }

  @Override
  public Expression defaultReturn(Expression node, List<Expression> e) {
    return nodesToRemove.contains(node) ? null : node;
  }

  @Override
  public Iterator<Expression> visitEnter(AndExpression node) {
    return node.getChildren().iterator();
  }

  @Override
  public Expression visit(LiteralExpression node) {
    return nodesToRemove.contains(node) ? null : node;
  }

  @Override
  public Expression visitLeave(AndExpression node, List<Expression> l) {
    if (!l.equals(node.getChildren())) {
      List<Expression> filtered = removeTrue(l);
      if (filtered.isEmpty()) {
        // AND of nothing (or all TRUE) is TRUE. Return the literal so upstream
        // {@code setScanFilter} logic recognizes it via
        // {@code ExpressionUtil.evaluatesToTrue} and skips attaching a filter.
        return LiteralExpression.newConstant(true, Determinism.ALWAYS);
      }
      if (filtered.size() == 1) {
        return filtered.get(0);
      }
      try {
        return AndExpression.create(filtered);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return node;
  }

  /**
   * The consumed set matches nodes by equals(). Thus it can hold a leaf that is equal to a branch
   * of this OR. For example, the leaf can come from a tautology OR or from an AND conjunct. Removal
   * of that branch makes the OR stricter and drops rows. Thus the OR goes away only when the OR
   * node itself is consumed.
   */
  @Override
  public Expression visitLeave(OrExpression node, List<Expression> l) {
    return nodesToRemove.contains(node)
      ? LiteralExpression.newConstant(true, Determinism.ALWAYS)
      : node;
  }

  private static List<Expression> removeTrue(List<Expression> l) {
    List<Expression> out = new java.util.ArrayList<>(l.size());
    for (Expression e : l) {
      if (!LiteralExpression.isTrue(e)) {
        out.add(e);
      }
    }
    return out;
  }

}
