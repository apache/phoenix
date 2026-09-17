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

/** Distance metrics supported for vector similarity search and vector indexing. */
public enum DistanceMetric {
  L2,
  COSINE,
  INNER_PRODUCT;

  public static DistanceMetric fromString(String name) {
    if (name == null) {
      return null;
    }
    String normalized = name.trim().toUpperCase();
    switch (normalized) {
      case "L2":
      case "L2_DISTANCE":
      case "L2_DISTANCE_SQUARED":
      case "EUCLIDEAN":
        return L2;
      case "COSINE":
      case "COSINE_DISTANCE":
        return COSINE;
      case "INNER_PRODUCT":
      case "INNER_PRODUCT_DISTANCE":
      case "DOT_PRODUCT":
      case "IP":
        return INNER_PRODUCT;
      default:
        return null;
    }
  }
}
