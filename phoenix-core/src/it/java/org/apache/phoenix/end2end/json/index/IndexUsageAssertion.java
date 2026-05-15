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
package org.apache.phoenix.end2end.json.index;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Helpers that classify Phoenix EXPLAIN PLAN output and assert whether a
 * specified index name appears in the plan.
 */
public final class IndexUsageAssertion {

  /** Two-tier expectation an IT pins to each query. */
  public enum Expectation {
    INDEX,           // plan must reference indexName
    FULL_SCAN        // plan must NOT reference indexName
  }

  private IndexUsageAssertion() {}

  /** Captures the EXPLAIN output for the given SQL using the given Connection. */
  public static String explain(Connection conn, String sql) throws Exception {
    StringBuilder sb = new StringBuilder();
    try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + sql)) {
      while (rs.next()) {
        sb.append(rs.getString(1)).append('\n');
      }
    }
    return sb.toString();
  }

  /** True if the explain plan uses indexName (RANGE SCAN OVER / FULL SCAN OVER indexName). */
  public static boolean planUsesIndex(String explainPlan, String indexName) {
    if (explainPlan == null || indexName == null) return false;
    // Phoenix EXPLAIN renders index hits as "OVER <SCHEMA.>NAME" — substring match is sufficient.
    return explainPlan.contains(indexName);
  }

  /** Coarse classifier for the report. */
  public static String classify(String explainPlan, String indexName) {
    if (planUsesIndex(explainPlan, indexName)) {
      return explainPlan.contains("RANGE SCAN") ? "INDEX_RANGE_SCAN" : "INDEX_FULL_SCAN";
    }
    return "DATA_FULL_SCAN";
  }

  /**
   * Throws AssertionError if observed usage does not match expected.
   * The thrown message embeds the entire EXPLAIN plan to make debugging trivial.
   */
  public static void assertExpectation(Expectation expected, String explainPlan,
      String indexName, String queryLabel) {
    boolean used = planUsesIndex(explainPlan, indexName);
    boolean ok = (expected == Expectation.INDEX && used)
        || (expected == Expectation.FULL_SCAN && !used);
    if (!ok) {
      throw new AssertionError("Index-usage expectation failed for query [" + queryLabel
          + "]; expected=" + expected + ", indexName=" + indexName
          + "\n--- EXPLAIN ---\n" + explainPlan + "---");
    }
  }
}
