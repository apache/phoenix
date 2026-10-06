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
 * Utilities for identifying and extracting vector similarity search queries.
 * <p>
 * Recognizes queries ordering ascending by a distance expression between a persistent vector column
 * and a constant query vector with an explicit row limit.
 */
public final class VectorSearchUtil {

  /** Maximum vector dimensions to display in EXPLAIN plan output before truncation. */
  private static final int EXPLAIN_VECTOR_ELEMENTS = 8;

  private VectorSearchUtil() {
  }

  /** Returns true if the statement matches the vector similarity search query pattern. */
  public static boolean isVectorSearchQuery(SelectStatement statement) {
    return getVectorSearchDescriptor(statement) != null;
  }

  /**
   * Extracts a {@link VectorSearchDescriptor} from the parsed statement, or null if the query does
   * not qualify as a vector similarity search.
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

    ParseNode exprNode = orderByNode.getNode();
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

  /** Checks whether the parse node is a vector distance function or operator. */
  public static boolean isDistanceFunctionNode(ParseNode node) {
    return node instanceof DistanceFunctionParseNode;
  }

  /** Resolves the {@link DistanceMetric} associated with a distance parse node, or null. */
  public static DistanceMetric getMetric(ParseNode distanceNode) {
    return isDistanceFunctionNode(distanceNode)
      ? DistanceMetric.forFunction(((FunctionParseNode) distanceNode).getName())
      : null;
  }

  /** Checks if the parse node represents a source vector expression backed by a column. */
  public static boolean isSourceVectorNode(ParseNode node) {
    if (node == null || node.isStateless()) {
      return false;
    }
    return hasColumnParseNode(node);
  }

  /** Checks if the parse node represents a constant or parameterized query vector. */
  public static boolean isQueryVectorNode(ParseNode node) {
    if (node == null) {
      return false;
    }
    if (node instanceof BindParseNode || node instanceof LiteralParseNode) {
      return true;
    }
    return node.isStateless() && !hasColumnParseNode(node);
  }

  /** Checks whether a parse node tree references any table column. */
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
   * Checks whether the compiled {@link OrderBy} and row limit qualify as a vector similarity
   * search.
   */
  public static boolean isVectorSearch(OrderBy orderBy, Integer limit) {
    return orderBy != null && isVectorSearch(orderBy.getOrderByExpressions(), limit);
  }

  /**
   * Checks whether the compiled {@link OrderBy} expressions and row limit qualify as a vector
   * similarity search.
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

  /** Resolves the distance metric configured for the compiled ORDER BY expression. */
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
   * Resolves the {@link VectorIndexScanPlan} from the query execution tree, unwrapping joins and
   * delegates if present.
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
   * Returns the lookup tier for a vector index plan based on base table access overhead: 0 for
   * covering indexes (no lookups), 1 for deferred projection (lookups for top-k rows only), and 2
   * for filter-time joins (lookups for all index candidates).
   */
  public static int getLookupRank(QueryPlan plan) {
    VectorIndexScanPlan scan = getVectorIndexScan(plan);
    if (scan == null || scan == plan) {
      return scan != null && scan.getContext().isUncoveredIndex() ? 2 : 0;
    }
    return 1;
  }

  /**
   * Computes the effective execution cost of a query plan. For plans utilizing deferred projection,
   * calculates cost as the vector index scan plus point lookups for the top-k result rows rather
   * than a full data table scan.
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
   * Formats a distance expression for EXPLAIN output, abbreviating literal query vectors to their
   * type, dimension, and initial elements.
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
