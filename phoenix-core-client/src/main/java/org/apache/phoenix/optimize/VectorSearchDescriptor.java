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

import java.util.Objects;
import org.apache.phoenix.parse.ArrayConstructorNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.LimitNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNode;

/**
 * Descriptor capturing the criteria of a vector similarity search query:
 * <ul>
 * <li>The distance metric (e.g. L2, COSINE, INNER_PRODUCT) and distance function type</li>
 * <li>The source vector expression (column reference or BSON vector extraction expression)</li>
 * <li>The query vector expression (literal, bind variable, or array constructor)</li>
 * <li>The search limit (top-K)</li>
 * </ul>
 */
public class VectorSearchDescriptor {

  private final DistanceMetric metric;
  private final DistanceFunctionType functionType;
  private final ParseNode distanceNode;
  private final ParseNode sourceExpression;
  private final ParseNode queryExpression;
  private final LimitNode limitNode;
  private final Integer limit;

  public VectorSearchDescriptor(DistanceMetric metric, DistanceFunctionType functionType,
    ParseNode distanceNode, ParseNode sourceExpression, ParseNode queryExpression,
    LimitNode limitNode, Integer limit) {
    this.metric = Objects.requireNonNull(metric, "metric cannot be null");
    this.functionType = functionType;
    this.distanceNode = distanceNode;
    this.sourceExpression =
      Objects.requireNonNull(sourceExpression, "sourceExpression cannot be null");
    this.queryExpression =
      Objects.requireNonNull(queryExpression, "queryExpression cannot be null");
    this.limitNode = limitNode;
    this.limit = limit;
  }

  public DistanceMetric getMetric() {
    return metric;
  }

  public DistanceFunctionType getDistanceFunctionType() {
    return functionType;
  }

  public String getDistanceFunctionName() {
    if (functionType != null) {
      return functionType.getFunctionName();
    }
    if (distanceNode instanceof FunctionParseNode) {
      return ((FunctionParseNode) distanceNode).getName();
    }
    return null;
  }

  public ParseNode getDistanceNode() {
    return distanceNode;
  }

  public ParseNode getSourceExpression() {
    return sourceExpression;
  }

  public String getSourceColumnName() {
    if (sourceExpression instanceof ColumnParseNode) {
      return ((ColumnParseNode) sourceExpression).getName();
    }
    return null;
  }

  public String getTableName() {
    if (sourceExpression instanceof ColumnParseNode) {
      return ((ColumnParseNode) sourceExpression).getTableName();
    }
    return null;
  }

  public String getSchemaName() {
    if (sourceExpression instanceof ColumnParseNode) {
      return ((ColumnParseNode) sourceExpression).getSchemaName();
    }
    return null;
  }

  public boolean isSourceColumn() {
    return sourceExpression instanceof ColumnParseNode;
  }

  public ParseNode getQueryExpression() {
    return queryExpression;
  }

  public boolean isQueryBindVariable() {
    return queryExpression instanceof BindParseNode;
  }

  public boolean isQueryLiteral() {
    return queryExpression instanceof LiteralParseNode
      || queryExpression instanceof ArrayConstructorNode;
  }

  public LimitNode getLimitNode() {
    return limitNode;
  }

  public Integer getLimit() {
    return limit;
  }

  public boolean hasLimit() {
    return limitNode != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VectorSearchDescriptor that = (VectorSearchDescriptor) o;
    return metric == that.metric && functionType == that.functionType
      && Objects.equals(distanceNode, that.distanceNode)
      && Objects.equals(sourceExpression, that.sourceExpression)
      && Objects.equals(queryExpression, that.queryExpression)
      && Objects.equals(limitNode, that.limitNode) && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metric, functionType, distanceNode, sourceExpression, queryExpression,
      limitNode, limit);
  }

  @Override
  public String toString() {
    return "VectorSearchDescriptor{" + "metric=" + metric + ", functionType=" + functionType
      + ", sourceExpression=" + sourceExpression + ", queryExpression=" + queryExpression
      + ", limit=" + limit + '}';
  }
}
