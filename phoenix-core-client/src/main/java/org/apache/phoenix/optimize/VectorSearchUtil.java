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

import java.lang.reflect.Array;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.DelegateQueryPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.HashJoinPlan.SubPlan;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.DistanceFunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.LimitNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVectorDataType;
import org.apache.phoenix.util.CostUtil;

/**
 * Finds vector similarity search queries and extracts their parameters.
 * <p>
 * A vector similarity search has a LIMIT and one ascending ORDER BY item. A literal LIMIT must be
 * positive. The ORDER BY item is a distance between a source vector expression that references a
 * column and a constant or bind parameter query vector. The item can also be an ordinal that refers
 * to such a distance in the projection.
 */
public final class VectorSearchUtil {

  /** The maximum number of elements that EXPLAIN shows for a literal query vector. */
  private static final int EXPLAIN_VECTOR_ELEMENTS = 8;

  private VectorSearchUtil() {
  }

  /**
   * Returns the expression that an ORDER BY item ranks by. For an ordinal, this is the projected
   * expression that the ordinal refers to. If the position of the ordinal is not known before
   * compilation, this is the item itself.
   */
  static ParseNode getOrderByExpression(SelectStatement statement, OrderByNode orderByNode) {
    ParseNode selectNode = orderByNode.getOrdinalSelectNode(statement.getSelect());
    return selectNode != null ? selectNode : orderByNode.getNode();
  }

  /** Returns true if the statement is a vector similarity search. */
  public static boolean isVectorSearchQuery(SelectStatement statement) {
    return getVectorSearchDescriptor(statement) != null;
  }

  /**
   * Returns a {@link VectorSearchDescriptor} for the parsed statement, or null if the statement is
   * not a vector similarity search.
   */
  public static VectorSearchDescriptor getVectorSearchDescriptor(SelectStatement statement) {
    if (statement == null) {
      return null;
    }

    LimitNode limitNode = statement.getLimit();
    if (limitNode == null) {
      return null;
    }
    Integer limitValue = null;
    ParseNode limitParseNode = limitNode.getLimitParseNode();
    if (limitParseNode instanceof LiteralParseNode) {
      Object value = ((LiteralParseNode) limitParseNode).getValue();
      if (value instanceof Number) {
        int l = ((Number) value).intValue();
        if (l <= 0) {
          return null;
        }
        limitValue = l;
      }
    }

    List<OrderByNode> orderByNodes = statement.getOrderBy();
    if (orderByNodes == null || orderByNodes.size() != 1) {
      return null;
    }
    OrderByNode orderByNode = orderByNodes.get(0);

    if (!orderByNode.isAscending()) {
      return null;
    }

    ParseNode exprNode = getOrderByExpression(statement, orderByNode);
    if (!isDistanceFunctionNode(exprNode)) {
      return null;
    }

    List<ParseNode> children = exprNode.getChildren();
    if (children == null || children.size() < 2) {
      return null;
    }

    ParseNode arg0 = children.get(0);
    ParseNode arg1 = children.get(1);

    ParseNode sourceExpr;
    ParseNode queryExpr;

    boolean arg0Source = isSourceVectorNode(arg0);
    boolean arg0Query = isQueryVectorNode(arg0);
    boolean arg1Source = isSourceVectorNode(arg1);
    boolean arg1Query = isQueryVectorNode(arg1);

    if (arg0Source && arg1Query && !arg0Query) {
      sourceExpr = arg0;
      queryExpr = arg1;
    } else if (arg1Source && arg0Query && !arg1Query) {
      sourceExpr = arg1;
      queryExpr = arg0;
    } else {
      return null;
    }

    DistanceMetric metric = getMetric(exprNode);
    return new VectorSearchDescriptor(metric, (FunctionParseNode) exprNode, sourceExpr, queryExpr,
      limitValue);
  }

  /** Returns true if the parse node is a vector distance function or distance operator. */
  public static boolean isDistanceFunctionNode(ParseNode node) {
    return node instanceof DistanceFunctionParseNode;
  }

  /** Returns the {@link DistanceMetric} of a distance parse node, or null for any other node. */
  public static DistanceMetric getMetric(ParseNode distanceNode) {
    return isDistanceFunctionNode(distanceNode)
      ? DistanceMetric.forFunction(((FunctionParseNode) distanceNode).getName())
      : null;
  }

  /**
   * Returns true if the parse node can be the source vector. The node must reference a column and
   * must not be stateless.
   */
  public static boolean isSourceVectorNode(ParseNode node) {
    if (node == null || node.isStateless()) {
      return false;
    }
    return hasColumnParseNode(node);
  }

  /**
   * Returns true if the parse node can be the query vector. The node must be a literal, a bind
   * parameter, or a stateless expression that references no column.
   */
  public static boolean isQueryVectorNode(ParseNode node) {
    if (node == null) {
      return false;
    }
    if (node instanceof BindParseNode || node instanceof LiteralParseNode) {
      return true;
    }
    return node.isStateless() && !hasColumnParseNode(node);
  }

  /** Returns true if the parse node tree contains a column reference. */
  public static boolean hasColumnParseNode(ParseNode node) {
    if (node == null) {
      return false;
    }
    if (node instanceof ColumnParseNode) {
      return true;
    }
    List<ParseNode> children = node.getChildren();
    if (children != null) {
      for (ParseNode child : children) {
        if (hasColumnParseNode(child)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns true if the compiled {@link OrderBy} and row limit make a vector similarity search. See
   * {@link #isVectorSearch(List, Integer)}.
   */
  public static boolean isVectorSearch(OrderBy orderBy, Integer limit) {
    return orderBy != null && isVectorSearch(orderBy.getOrderByExpressions(), limit);
  }

  /**
   * Returns true if the compiled ORDER BY expressions and row limit make a vector similarity
   * search. The limit must be positive, and the only ORDER BY expression must be an ascending
   * {@link DistanceFunction}.
   */
  public static boolean isVectorSearch(List<OrderByExpression> orderByExpressions, Integer limit) {
    if (
      limit == null || limit <= 0 || orderByExpressions == null || orderByExpressions.size() != 1
    ) {
      return false;
    }
    OrderByExpression orderByExpression = orderByExpressions.get(0);
    return orderByExpression.isAscending()
      && orderByExpression.getExpression() instanceof DistanceFunction;
  }

  /**
   * Returns the distance metric of the ORDER BY distance function. Returns null if the ORDER BY is
   * not a single distance function.
   */
  public static DistanceMetric getDistanceMetric(OrderBy orderBy) {
    if (orderBy == null || orderBy.getOrderByExpressions().size() != 1) {
      return null;
    }
    Expression expression = orderBy.getOrderByExpressions().get(0).getExpression();
    return expression instanceof DistanceFunction
      ? DistanceMetric.forFunction(((DistanceFunction) expression).getName())
      : null;
  }

  /**
   * Returns the {@link VectorIndexScanPlan} in a plan tree, or null if there is none. The search
   * goes into the sub plans of a hash join and into the plan that a delegate plan wraps.
   */
  public static VectorIndexScanPlan getVectorIndexScan(QueryPlan plan) {
    if (plan instanceof VectorIndexScanPlan) {
      return (VectorIndexScanPlan) plan;
    }
    if (plan instanceof HashJoinPlan) {
      for (SubPlan subPlan : ((HashJoinPlan) plan).getSubPlans()) {
        VectorIndexScanPlan scan = getVectorIndexScan(subPlan.getInnerPlan());
        if (scan != null) {
          return scan;
        }
      }
    }
    return plan instanceof DelegateQueryPlan
      ? getVectorIndexScan(((DelegateQueryPlan) plan).getDelegate())
      : null;
  }

  /** Indicates whether the query execution plan incorporates a vector index scan. */
  public static boolean usesVectorIndex(QueryPlan plan) {
    return getVectorIndexScan(plan) != null;
  }

  /**
   * Returns the rank of a plan by its data table lookups, where a lower rank does fewer lookups. A
   * covering vector index plan gets 0 because it does no lookups. A deferred projection gets 1
   * because it looks up only the top-k rows. An uncovered vector index plan gets 2 because it looks
   * up every candidate row at filter time. A plan that does not read a vector index gets 0.
   */
  public static int getLookupRank(QueryPlan plan) {
    VectorIndexScanPlan scan = getVectorIndexScan(plan);
    if (scan == null || scan == plan) {
      return scan != null && scan.getContext().isUncoveredIndex() ? 2 : 0;
    }
    return 1;
  }

  /**
   * Returns the cost of a query plan for comparison with the other candidate plans. For a deferred
   * projection, the cost is the vector index scan cost plus point lookups for LIMIT + OFFSET rows.
   * This cost does not include a full data table scan. For other plans, the cost is
   * {@link QueryPlan#getCost()}. A deferred projection gets {@link Cost#UNKNOWN} if the scan cost
   * is unknown or the scan has no limit.
   */
  public static Cost getCost(QueryPlan plan) {
    VectorIndexScanPlan scan = getVectorIndexScan(plan);
    if (scan == null || scan == plan) {
      return plan.getCost();
    }
    Cost scanCost = scan.getCost();
    if (scanCost.isUnknown() || scan.getLimit() == null) {
      return Cost.UNKNOWN;
    }
    long rows = scan.getLimit() + (scan.getOffset() == null ? 0 : scan.getOffset());
    return scanCost.plus(CostUtil.estimateLookupCost(rows, plan.getTableRef().getTable()));
  }

  /**
   * Formats a distance expression for EXPLAIN output. A literal query vector shows only its type,
   * its dimension, and its first elements, so that a large vector does not make the plan too long.
   */
  public static String toExplainString(Expression distance) {
    if (!(distance instanceof DistanceFunction)) {
      return distance.toString();
    }
    StringBuilder buf = new StringBuilder(((DistanceFunction) distance).getName()).append('(');
    List<Expression> children = distance.getChildren();
    for (int i = 0; i < children.size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      Expression child = children.get(i);
      PDataType type = child.getDataType();
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      if (
        child.isStateless() && type instanceof PVectorDataType && child.evaluate(null, ptr)
          && ptr.getLength() > 0
      ) {
        Object elements = type.toObject(ptr);
        int dim = Array.getLength(elements);
        buf.append("VECTOR(").append(((PVectorDataType<?>) type).getElementType().getSqlTypeName())
          .append(", ").append(dim).append(")[");
        for (int j = 0; j < Math.min(dim, EXPLAIN_VECTOR_ELEMENTS); j++) {
          buf.append(j > 0 ? ", " : "").append(Array.get(elements, j));
        }
        buf.append(dim > EXPLAIN_VECTOR_ELEMENTS ? ", ...]" : "]");
      } else {
        buf.append(child);
      }
    }
    return buf.append(')').toString();
  }
}
