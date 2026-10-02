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

import org.apache.phoenix.expression.function.CosineDistanceFunction;
import org.apache.phoenix.expression.function.InnerProductDistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceSquaredFunction;

/**
 * Enumerates vector distance function expressions and maps them to their underlying distance
 * metric.
 */
public enum DistanceFunctionType {
  L2_DISTANCE(L2DistanceFunction.NAME, DistanceMetric.L2),
  L2_DISTANCE_SQUARED(L2DistanceSquaredFunction.NAME, DistanceMetric.L2),
  COSINE_DISTANCE(CosineDistanceFunction.NAME, DistanceMetric.COSINE),
  INNER_PRODUCT(InnerProductDistanceFunction.NAME, DistanceMetric.INNER_PRODUCT);

  private final String functionName;
  private final DistanceMetric metric;

  DistanceFunctionType(String functionName, DistanceMetric metric) {
    this.functionName = functionName;
    this.metric = metric;
  }

  public String getFunctionName() {
    return functionName;
  }

  public DistanceMetric getMetric() {
    return metric;
  }

  public static DistanceFunctionType fromFunctionName(String name) {
    if (name == null) {
      return null;
    }
    String upper = name.trim().toUpperCase();
    for (DistanceFunctionType type : values()) {
      if (type.functionName.equals(upper)) {
        return type;
      }
    }
    return null;
  }
}
