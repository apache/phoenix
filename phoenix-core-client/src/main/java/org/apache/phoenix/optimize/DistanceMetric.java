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

/** Supported vector distance metrics. */
public enum DistanceMetric {
  L2,
  COSINE,
  INNER_PRODUCT;

  /** ` Parses a metric name, ignoring case and whitespace. */
  public static DistanceMetric fromString(String name) {
    if (name == null) {
      return null;
    }
    try {
      return valueOf(name.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /** Maps a distance function to its underlying ranking metric. */
  public static DistanceMetric forFunction(String functionName) {
    if (functionName == null) {
      return null;
    }
    switch (functionName) {
      case L2DistanceFunction.NAME:
      case L2DistanceSquaredFunction.NAME:
        return L2;
      case CosineDistanceFunction.NAME:
        return COSINE;
      case InnerProductDistanceFunction.NAME:
        return INNER_PRODUCT;
      default:
        return null;
    }
  }
}
