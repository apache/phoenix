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
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ParseNode;

/**
 * Query descriptor capturing parsed vector similarity search parameters, including target metric,
 * source expression, query vector, and result limit.
 */
public class VectorSearchDescriptor {

  private final DistanceMetric metric;
  private final FunctionParseNode distanceNode;
  private final ParseNode sourceExpression;
  private final ParseNode queryExpression;
  private final Integer limit;

  public VectorSearchDescriptor(DistanceMetric metric, FunctionParseNode distanceNode,
    ParseNode sourceExpression, ParseNode queryExpression, Integer limit) {
    this.metric = Objects.requireNonNull(metric, "metric cannot be null");
    this.distanceNode = Objects.requireNonNull(distanceNode, "distanceNode cannot be null");
    this.sourceExpression =
      Objects.requireNonNull(sourceExpression, "sourceExpression cannot be null");
    this.queryExpression =
      Objects.requireNonNull(queryExpression, "queryExpression cannot be null");
    this.limit = limit;
  }

  public DistanceMetric getMetric() {
    return metric;
  }

  public FunctionParseNode getDistanceNode() {
    return distanceNode;
  }

  public String getDistanceFunctionName() {
    return distanceNode.getName();
  }

  public ParseNode getSourceExpression() {
    return sourceExpression;
  }

  public boolean isSourceColumn() {
    return sourceExpression instanceof ColumnParseNode;
  }

  /** Returns the source column name if the source expression references a table column, or null. */
  public String getSourceColumnName() {
    return isSourceColumn() ? ((ColumnParseNode) sourceExpression).getName() : null;
  }

  public ParseNode getQueryExpression() {
    return queryExpression;
  }

  /** Returns the literal row limit, or null if unbound or parameterized. */
  public Integer getLimit() {
    return limit;
  }

  @Override
  public String toString() {
    return "VectorSearchDescriptor{function=" + distanceNode.getName() + ", metric=" + metric
      + ", sourceExpression=" + sourceExpression + ", queryExpression=" + queryExpression
      + ", limit=" + limit + '}';
  }
}
