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
package org.apache.phoenix.query.explain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.regex.Pattern;

/**
 * Elides cluster- and connection-specific fields from the JSON view of
 * {@code ExplainPlanAttributes} so the comparison is invariant under environment differences.
 */
public final class ExplainJsonNormalizer {

  private static final Pattern WAY_COUNT = Pattern.compile("\\b\\d+-WAY\\b");

  /**
   * Recursively normalize the given attributes-shaped JSON node.
   * @return the same node, for fluent chaining.
   */
  public JsonNode normalize(JsonNode node) {
    if (node == null || node.isNull() || !node.isObject()) {
      return node;
    }
    ObjectNode obj = (ObjectNode) node;

    if (obj.has("regionLocations")) {
      obj.set("regionLocations", NullNode.getInstance());
    }
    if (obj.has("numRegionLocationLookups")) {
      obj.put("numRegionLocationLookups", 0);
    }
    if (obj.has("splitsChunk")) {
      obj.set("splitsChunk", NullNode.getInstance());
    }
    if (obj.has("estimatedRows")) {
      obj.set("estimatedRows", NullNode.getInstance());
    }
    if (obj.has("estimatedSizeInBytes")) {
      obj.set("estimatedSizeInBytes", NullNode.getInstance());
    }

    JsonNode iter = obj.get("iteratorTypeAndScanSize");
    if (iter != null && iter.isTextual()) {
      obj.put("iteratorTypeAndScanSize", WAY_COUNT.matcher(iter.asText()).replaceAll("<N>-WAY"));
    }

    JsonNode rhs = obj.get("rhsJoinQueryExplainPlan");
    if (rhs != null && rhs.isObject()) {
      normalize(rhs);
    }

    return obj;
  }
}
