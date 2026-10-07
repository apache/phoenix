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
package org.apache.phoenix.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum VectorIndexType {
  IVF,
  HNSW;

  private static final Map<String, VectorIndexType> VECTOR_TYPE;

  static {
    Map<String, VectorIndexType> map = new HashMap<>(values().length);
    for (VectorIndexType type : values()) {
      map.put(type.name().toUpperCase(Locale.ROOT), type);
    }
    VECTOR_TYPE = Collections.unmodifiableMap(map);
  }

  /**
   * Resolves the vector index type for the specified algorithm name.
   * @param algorithm algorithm name
   * @return matching VectorIndexType, or null if unrecognized
   */
  public static VectorIndexType fromAlgorithm(String algorithm) {
    if (algorithm == null) {
      return null;
    }
    return VECTOR_TYPE.get(algorithm.trim().toUpperCase(Locale.ROOT));
  }
}
