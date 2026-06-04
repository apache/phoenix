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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Elides cluster- and connection-specific details from the {@code List<String>} returned by
 * {@code ExplainPlan.getPlanSteps()} so the EXPLAIN text can be compared across environments.
 */
public final class ExplainTextNormalizer {

  // CLIENT 5-CHUNK -> CLIENT <N>-CHUNK ; matches any non-negative integer immediately before
  // -CHUNK.
  private static final Pattern CHUNK_COUNT = Pattern.compile("\\b\\d+-CHUNK\\b");

  // PARALLEL 400-WAY -> PARALLEL <N>-WAY ; matches the iterator parallelism count.
  private static final Pattern WAY_COUNT = Pattern.compile("\\b\\d+-WAY\\b");

  // 1234 ROWS 5678 BYTES (stats-row-count gated; we strip when present).
  private static final Pattern ROWS_BYTES = Pattern.compile("\\d+ ROWS \\d+ BYTES\\s*");

  // " (region locations = [...]) " emitted via planSteps.add(regionLocationPlan); the line always
  // begins with the leading-space form of ExplainTable.REGION_LOCATIONS.
  private static final String REGION_LOCATIONS_PREFIX = " (region locations = ";

  /**
   * @param raw the result of {@code ExplainPlan.getPlanSteps()}
   * @return a new list with cluster/connection-specific detail elided. The original list is not
   *         mutated.
   */
  public List<String> normalize(List<String> raw) {
    List<String> out = new ArrayList<>(raw.size());
    for (String line : raw) {
      if (line == null) {
        out.add(null);
        continue;
      }
      // Drop region-location lines outright
      if (line.startsWith(REGION_LOCATIONS_PREFIX) || line.contains(REGION_LOCATIONS_PREFIX)) {
        continue;
      }
      String normalized = line;
      normalized = CHUNK_COUNT.matcher(normalized).replaceAll("<N>-CHUNK");
      normalized = WAY_COUNT.matcher(normalized).replaceAll("<N>-WAY");
      normalized = ROWS_BYTES.matcher(normalized).replaceAll("");
      out.add(normalized);
    }
    return out;
  }
}
