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
package org.apache.phoenix.compile.keyspace.algebra;

import java.util.ArrayList;
import java.util.List;

/**
 * Row-enumeration-based assertions for comparing two {@link AbstractKeySpaceList}s and the
 * original expression. Used by the differential harness to check that V2's output is:
 * <ul>
 * <li><b>Sound</b> — every row matching the expression is contained in V2's emitted list
 * (no false negatives). This is the primary correctness property; a violation is a
 * production bug.</li>
 * <li><b>Not overly wide</b> — V2's list does not contain rows that fall outside the
 * oracle's list. Equivalently, V2 ⊆ oracle. Violations are performance concerns, not
 * correctness bugs — the residual filter still rejects the extras.</li>
 * </ul>
 */
public final class HarnessAssertions {

  private HarnessAssertions() {}

  /** One enumerated test row with the values for every PK column. */
  public static final class Row {
    public final List<Object> values;
    public Row(List<Object> values) { this.values = values; }
    @Override public String toString() { return values.toString(); }
  }

  /**
   * Enumerate all rows in the cartesian product of {@code perDimValues}. Each inner list
   * is the set of candidate values for that PK column. The total row count is the product
   * of the inner sizes, so keep those small (≤10) to avoid explosion.
   */
  public static List<Row> enumerateRows(List<List<Object>> perDimValues) {
    List<Row> out = new ArrayList<>();
    Object[] current = new Object[perDimValues.size()];
    build(perDimValues, 0, current, out);
    return out;
  }

  private static void build(List<List<Object>> perDim, int idx, Object[] current, List<Row> out) {
    if (idx == perDim.size()) {
      out.add(new Row(new ArrayList<>(java.util.Arrays.asList(current.clone()))));
      return;
    }
    for (Object v : perDim.get(idx)) {
      current[idx] = v;
      build(perDim, idx + 1, current, out);
    }
  }

  /** Result of a soundness check. */
  public static final class Report {
    public final int totalRows;
    public final int exprMatches;
    public final int oracleContains;
    public final int v2Contains;
    public final List<Row> soundnessViolations;  // matched expr but NOT in V2
    public final List<Row> wideningViolations;   // in V2 but NOT in oracle
    public final List<Row> oracleMissesExprMatch; // matched expr but NOT in oracle (oracle bug!)

    public Report(int totalRows, int exprMatches, int oracleContains, int v2Contains,
      List<Row> soundnessViolations, List<Row> wideningViolations,
      List<Row> oracleMissesExprMatch) {
      this.totalRows = totalRows;
      this.exprMatches = exprMatches;
      this.oracleContains = oracleContains;
      this.v2Contains = v2Contains;
      this.soundnessViolations = soundnessViolations;
      this.wideningViolations = wideningViolations;
      this.oracleMissesExprMatch = oracleMissesExprMatch;
    }

    public boolean v2Sound() { return soundnessViolations.isEmpty(); }
    public boolean oracleSound() { return oracleMissesExprMatch.isEmpty(); }
    public boolean v2SubsetOfOracle() { return wideningViolations.isEmpty(); }

    @Override
    public String toString() {
      return String.format(
        "Report[rows=%d, exprMatches=%d, oracleContains=%d, v2Contains=%d, "
          + "v2Sound=%s, oracleSound=%s, v2SubsetOfOracle=%s]",
        totalRows, exprMatches, oracleContains, v2Contains,
        v2Sound(), oracleSound(), v2SubsetOfOracle());
    }
  }

  /**
   * Enumerate every row in {@code domain} and classify it under each of:
   * {@code expr.evaluate(row)}, {@code oracle.matches(row)}, {@code v2.matches(row)}.
   * Collect any row that violates soundness (expr → V2) or V2's subset-of-oracle property.
   */
  public static Report evaluate(AbstractExpression expr, AbstractKeySpaceList oracle,
    AbstractKeySpaceList v2, List<Row> domain) {
    int exprMatches = 0;
    int oracleContains = 0;
    int v2Contains = 0;
    List<Row> soundnessViolations = new ArrayList<>();
    List<Row> wideningViolations = new ArrayList<>();
    List<Row> oracleMissesExprMatch = new ArrayList<>();

    for (Row row : domain) {
      boolean matchesExpr = expr.evaluate(row.values);
      boolean inOracle = oracle.matches(row.values);
      boolean inV2 = v2.matches(row.values);

      if (matchesExpr) exprMatches++;
      if (inOracle) oracleContains++;
      if (inV2) v2Contains++;

      if (matchesExpr && !inV2) soundnessViolations.add(row);
      if (matchesExpr && !inOracle) oracleMissesExprMatch.add(row);
      if (inV2 && !inOracle) wideningViolations.add(row);
    }
    return new Report(domain.size(), exprMatches, oracleContains, v2Contains,
      soundnessViolations, wideningViolations, oracleMissesExprMatch);
  }
}
