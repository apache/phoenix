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

import java.util.List;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.CosineDistanceFunction;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.expression.function.InnerProductDistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceSquaredFunction;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.CosineDistanceParseNode;
import org.apache.phoenix.parse.DistanceFunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.InnerProductDistanceParseNode;
import org.apache.phoenix.parse.L2DistanceParseNode;
import org.apache.phoenix.parse.L2DistanceSquaredParseNode;
import org.apache.phoenix.parse.LimitNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SelectStatement;

/**
 * Static analysis utilities for detecting and extracting vector similarity search queries.
 * <p>
 * A vector search query is recognized when:
 * <ol>
 * <li>The ORDER BY clause contains exactly one expression that is a distance function or
 * operator.</li>
 * <li>One argument of the distance expression resolves to a table column or path expression (the
 * indexed vector), and the other resolves to a constant, bind variable, or literal (the query
 * vector).</li>
 * <li>The sort direction is ascending (smaller distance = closer match).</li>
 * <li>The query includes a LIMIT clause.</li>
 * </ol>
 */
public final class VectorSearchUtil {

  private VectorSearchUtil() {
  }

  /**
   * Determines whether the given parsed SELECT statement represents a vector similarity query.
   * @param statement the select statement to inspect
   * @return true if the statement is a vector similarity search query, false otherwise
   */
  public static boolean isVectorSearchQuery(SelectStatement statement) {
    return getVectorSearchDescriptor(statement) != null;
  }

  /**
   * Extracts a {@link VectorSearchDescriptor} from the given parsed SELECT statement, or returns
   * {@code null} if the statement does not qualify as a vector similarity query.
   * @param statement the select statement to analyze
   * @return the descriptor if valid vector search query, null otherwise
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
    if (metric == null) {
      return null;
    }
    DistanceFunctionType functionType = getDistanceFunctionType(exprNode);

    return new VectorSearchDescriptor(metric, functionType, exprNode, sourceExpr, queryExpr,
      limitNode, limitValue);
  }

  /**
   * Checks whether the given parse node corresponds to a vector distance function or operator.
   */
  public static boolean isDistanceFunctionNode(ParseNode node) {
    if (node instanceof DistanceFunctionParseNode) {
      return true;
    }
    if (node instanceof FunctionParseNode) {
      String name = ((FunctionParseNode) node).getName();
      if (name != null) {
        String upper = name.trim().toUpperCase();
        return upper.equals(L2DistanceFunction.NAME) || upper.equals(L2DistanceSquaredFunction.NAME)
          || upper.equals(CosineDistanceFunction.NAME)
          || upper.equals(InnerProductDistanceFunction.NAME);
      }
    }
    return false;
  }

  /**
   * Maps a distance function parse node to its {@link DistanceMetric}.
   */
  public static DistanceMetric getMetric(ParseNode distanceNode) {
    if (
      distanceNode instanceof L2DistanceParseNode
        || distanceNode instanceof L2DistanceSquaredParseNode
    ) {
      return DistanceMetric.L2;
    }
    if (distanceNode instanceof CosineDistanceParseNode) {
      return DistanceMetric.COSINE;
    }
    if (distanceNode instanceof InnerProductDistanceParseNode) {
      return DistanceMetric.INNER_PRODUCT;
    }
    if (distanceNode instanceof FunctionParseNode) {
      String name = ((FunctionParseNode) distanceNode).getName();
      if (name != null) {
        String upper = name.trim().toUpperCase();
        if (upper.equals(L2DistanceFunction.NAME) || upper.equals(L2DistanceSquaredFunction.NAME)) {
          return DistanceMetric.L2;
        }
        if (upper.equals(CosineDistanceFunction.NAME)) {
          return DistanceMetric.COSINE;
        }
        if (upper.equals(InnerProductDistanceFunction.NAME)) {
          return DistanceMetric.INNER_PRODUCT;
        }
      }
    }
    return null;
  }

  /**
   * Maps a distance function parse node to its {@link DistanceFunctionType}.
   */
  public static DistanceFunctionType getDistanceFunctionType(ParseNode distanceNode) {
    if (distanceNode instanceof L2DistanceParseNode) {
      return DistanceFunctionType.L2_DISTANCE;
    }
    if (distanceNode instanceof L2DistanceSquaredParseNode) {
      return DistanceFunctionType.L2_DISTANCE_SQUARED;
    }
    if (distanceNode instanceof CosineDistanceParseNode) {
      return DistanceFunctionType.COSINE_DISTANCE;
    }
    if (distanceNode instanceof InnerProductDistanceParseNode) {
      return DistanceFunctionType.INNER_PRODUCT;
    }
    if (distanceNode instanceof FunctionParseNode) {
      String name = ((FunctionParseNode) distanceNode).getName();
      return DistanceFunctionType.fromFunctionName(name);
    }
    return null;
  }

  /**
   * Checks if the parse node represents a source vector candidate (e.g., a table column or BSON
   * extraction function referencing a column).
   */
  public static boolean isSourceVectorNode(ParseNode node) {
    if (node == null || node.isStateless()) {
      return false;
    }
    return hasColumnParseNode(node);
  }

  /**
   * Checks if the parse node represents a query vector candidate (e.g., a bind variable, literal
   * array, or stateless constant expression).
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

  /**
   * Recursively checks whether a parse node tree contains any {@link ColumnParseNode}.
   */
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
   * Checks whether the compiled {@link OrderBy} and row limit represent a vector similarity search.
   * @param orderBy the compiled ORDER BY clause
   * @param limit   the row limit, or null if unconstrained
   * @return true if the compiled plan is an exact vector search
   */
  public static boolean isVectorSearch(OrderBy orderBy, Integer limit) {
    if (limit == null || limit <= 0 || orderBy == null || orderBy.isEmpty()) {
      return false;
    }
    List<OrderByExpression> expressions = orderBy.getOrderByExpressions();
    if (expressions.size() != 1) {
      return false;
    }
    OrderByExpression orderByExpression = expressions.get(0);
    if (!orderByExpression.isAscending()) {
      return false;
    }
    Expression expr = orderByExpression.getExpression();
    return expr instanceof DistanceFunction;
  }
}
