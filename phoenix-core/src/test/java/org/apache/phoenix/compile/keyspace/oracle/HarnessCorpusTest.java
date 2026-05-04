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
package org.apache.phoenix.compile.keyspace.oracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

/**
 * Drives the harness over a corpus of hand-picked SQL queries that exercise the scan-range
 * optimizer's interesting shapes. Each entry is a DDL + query pair; the harness automates
 * compilation, adapter conversion, decoding, oracle comparison, and soundness evaluation.
 * <p>
 * Failures:
 * <ul>
 * <li>A V2 soundness violation (row matching the expression NOT in V2's emission) fails
 * the test immediately — that's a correctness bug.</li>
 * <li>Widening (V2 contains rows outside the oracle's view) is logged for visibility but
 * does not fail the test — it's a performance concern, not correctness.</li>
 * <li>Skipped queries (unsupported shape in adapter or decoder) are logged; many skips
 * indicate harness coverage is insufficient.</li>
 * </ul>
 */
public class HarnessCorpusTest extends BaseConnectionlessQueryTest {

  private static final long GRID_SIZE_CAP = 5000;

  /** One corpus entry: the DDL to set up the table + the SELECT to exercise. */
  private static final class Case {
    final String tableName;
    final String ddl;
    final String query;
    /**
     * When non-null, this query is <em>expected</em> to show scan-range widening vs the
     * oracle and the reason is documented here. Used to distinguish unfixable
     * scan-primitive limitations (e.g. SkipScanFilter can't express cross-dim OR) from
     * real regressions. Any widening not annotated here fails the test.
     */
    final String expectedWideningReason;
    Case(String tableName, String ddl, String query) {
      this(tableName, ddl, query, null);
    }
    Case(String tableName, String ddl, String query, String expectedWideningReason) {
      this.tableName = tableName;
      this.ddl = ddl;
      this.query = query;
      this.expectedWideningReason = expectedWideningReason;
    }
  }

  @Test
  public void runCorpus() {
    List<Case> cases = corpus();
    int sound = 0, expectedWidening = 0, skipped = 0, violations = 0;
    List<HarnessRunner.Report> violationReports = new ArrayList<>();
    List<String> unexpectedWidening = new ArrayList<>();
    List<String> unexpectedTightening = new ArrayList<>();
    for (Case c : cases) {
      HarnessRunner.Report rep = HarnessRunner.run(getUrl(),
        Arrays.asList("DROP TABLE IF EXISTS " + c.tableName, c.ddl), c.query, GRID_SIZE_CAP);
      if (rep.skipped) {
        skipped++;
        System.err.println("SKIP: " + c.query + " — " + rep.skipReason);
        continue;
      }
      System.err.println("RUN : " + c.query);
      System.err.println("    expr=" + rep.expr);
      System.err.println("    oracleView=" + rep.oracleView);
      System.err.println("    v2View=" + rep.v2View);
      System.err.println("    " + rep.assertions);
      if (!rep.assertions.v2Sound()) {
        violations++;
        violationReports.add(rep);
      } else {
        sound++;
      }
      boolean actuallyWider = !rep.assertions.v2SubsetOfOracle();
      boolean expectedWider = c.expectedWideningReason != null;
      if (actuallyWider && expectedWider) {
        expectedWidening++;
        System.err.println("  EXPECTED WIDENING (" + c.expectedWideningReason + ")");
      } else if (actuallyWider) {
        unexpectedWidening.add(c.query
          + "\n      violations: " + rep.assertions.wideningViolations);
      } else if (expectedWider) {
        unexpectedTightening.add(c.query
          + "\n      documented reason no longer applies: " + c.expectedWideningReason);
      }
    }
    System.err.println("============================================");
    System.err.println("Corpus: " + cases.size() + " total, " + sound + " sound, "
      + violations + " violations, " + expectedWidening + " expected-widening, "
      + unexpectedWidening.size() + " unexpected-widening, "
      + unexpectedTightening.size() + " unexpected-tightening, " + skipped + " skipped");

    if (violations > 0) {
      StringBuilder sb = new StringBuilder(violations + " V2 soundness violation(s):\n");
      for (HarnessRunner.Report r : violationReports) {
        sb.append("  ").append(r.query).append("\n")
          .append("    rows: ").append(r.assertions.soundnessViolations).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
    if (!unexpectedWidening.isEmpty()) {
      StringBuilder sb = new StringBuilder(unexpectedWidening.size()
        + " unexpected V2 widening(s) — either fix V2 or annotate with "
        + "expectedWideningReason:\n");
      for (String s : unexpectedWidening) sb.append("  ").append(s).append("\n");
      throw new AssertionError(sb.toString());
    }
    if (!unexpectedTightening.isEmpty()) {
      StringBuilder sb = new StringBuilder(unexpectedTightening.size()
        + " V2 now tighter than the documented expected widening — "
        + "remove the expectedWideningReason annotation:\n");
      for (String s : unexpectedTightening) sb.append("  ").append(s).append("\n");
      throw new AssertionError(sb.toString());
    }
  }

  private static List<Case> corpus() {
    List<Case> out = new ArrayList<>();

    // Simple leading-PK range.
    out.add(new Case("C_SIMPLE_RANGE",
      "CREATE TABLE C_SIMPLE_RANGE (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_SIMPLE_RANGE WHERE a >= 5 AND a < 10"));

    // Equality on leading PK.
    out.add(new Case("C_LEAD_EQ",
      "CREATE TABLE C_LEAD_EQ (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_LEAD_EQ WHERE a = 5"));

    // OR on leading PK.
    out.add(new Case("C_OR_LEAD",
      "CREATE TABLE C_OR_LEAD (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_OR_LEAD WHERE a = 3 OR a = 7"));

    // IN list on leading PK.
    out.add(new Case("C_IN_LIST",
      "CREATE TABLE C_IN_LIST (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_IN_LIST WHERE a IN (3, 5, 7)"));

    // Degenerate predicate on non-leading PK (PHOENIX-6669).
    out.add(new Case("C_DEGEN",
      "CREATE TABLE C_DEGEN (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b, c))",
      "SELECT * FROM C_DEGEN WHERE a = 1 AND b >= 10 AND b < 5"));

    // Leading EQ + trailing range.
    out.add(new Case("C_LEADEQ_TRAILRANGE",
      "CREATE TABLE C_LEADEQ_TRAILRANGE (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_LEADEQ_TRAILRANGE WHERE a = 5 AND b >= 10 AND b <= 20"));

    // RVC inequality — the classic bug-finder.
    out.add(new Case("C_RVC_INEQ",
      "CREATE TABLE C_RVC_INEQ (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b, c))",
      "SELECT * FROM C_RVC_INEQ WHERE a = 5 AND (a, b, c) > (5, 10, 100)"));

    // RVC inequality with category prefix (the testRVCScanBoundaries1 shape).
    out.add(new Case("C_RVC_BOUNDARIES",
      "CREATE TABLE C_RVC_BOUNDARIES (category VARCHAR NOT NULL, score BIGINT NOT NULL, "
        + "pk VARCHAR NOT NULL, sk BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (category, score, pk, sk))",
      "SELECT * FROM C_RVC_BOUNDARIES WHERE category = 'cat0' AND score <= 100"
        + " AND (score, pk, sk) > (50, 'pk_5', 50)"));

    // OR of two disjoint point predicates on same dim.
    out.add(new Case("C_POINT_OR",
      "CREATE TABLE C_POINT_OR (a BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_POINT_OR WHERE a = 3 OR a = 5"));

    // AND of two IN lists on different PK cols.
    out.add(new Case("C_AND_INS",
      "CREATE TABLE C_AND_INS (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_AND_INS WHERE a IN (1, 2) AND b IN (3, 4)"));

    // Tautology.
    out.add(new Case("C_TAUTO",
      "CREATE TABLE C_TAUTO (a BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_TAUTO WHERE a >= 5 OR a < 5"));

    // Non-PK predicate — oracle marks unknown, V2 should emit as residual.
    out.add(new Case("C_NONPK",
      "CREATE TABLE C_NONPK (a BIGINT NOT NULL, b BIGINT, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_NONPK WHERE a = 5 AND b = 7"));

    // LIKE with prefix on a varchar PK.
    out.add(new Case("C_LIKE_PREFIX",
      "CREATE TABLE C_LIKE_PREFIX (a VARCHAR NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_LIKE_PREFIX WHERE a LIKE 'pre%'"));

    // BETWEEN on leading PK (StatementNormalizer lowers to >= AND <=).
    out.add(new Case("C_BETWEEN",
      "CREATE TABLE C_BETWEEN (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_BETWEEN WHERE a BETWEEN 3 AND 8"));

    // Range on non-leading PK (gap at leading) — V2's behaviour can fall back to
    // everything depending on handling; harness confirms soundness either way.
    out.add(new Case("C_NONLEAD_RANGE",
      "CREATE TABLE C_NONLEAD_RANGE (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_NONLEAD_RANGE WHERE b = 7"));

    // Conjunction of OR with AND shape (testAndOrExpression-style).
    out.add(new Case("C_AND_OR_TWO_DIMS",
      "CREATE TABLE C_AND_OR_TWO_DIMS (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_AND_OR_TWO_DIMS WHERE (a = 1 AND b = 3) OR (a = 2 AND b = 4)"));

    // Equality chain on all PK cols — full point lookup.
    out.add(new Case("C_FULL_POINT",
      "CREATE TABLE C_FULL_POINT (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b, c))",
      "SELECT * FROM C_FULL_POINT WHERE a = 1 AND b = 2 AND c = 3"));

    // Mixed equality + IN — combines single-dim EQ with single-dim OR.
    out.add(new Case("C_MIXED_EQ_IN",
      "CREATE TABLE C_MIXED_EQ_IN (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_MIXED_EQ_IN WHERE a = 5 AND b IN (10, 20)"));

    // Negative values.
    out.add(new Case("C_NEGATIVE",
      "CREATE TABLE C_NEGATIVE (a BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_NEGATIVE WHERE a = -5 OR a = -10"));

    // Range covering entire domain (tautology via disjoint-adjacent).
    out.add(new Case("C_DISJOINT_ADJ",
      "CREATE TABLE C_DISJOINT_ADJ (a BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_DISJOINT_ADJ WHERE a < 5 OR a >= 5"));

    // VARCHAR equality.
    out.add(new Case("C_VARCHAR_EQ",
      "CREATE TABLE C_VARCHAR_EQ (a VARCHAR NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_VARCHAR_EQ WHERE a = 'hello'"));

    // Multiple ANDs on the same column (redundant but common from user code).
    out.add(new Case("C_REDUNDANT_AND",
      "CREATE TABLE C_REDUNDANT_AND (a BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_REDUNDANT_AND WHERE a >= 5 AND a <= 15 AND a >= 3"));

    // Contradictory predicates that should fold to unsatisfiable.
    out.add(new Case("C_CONTRADICTION",
      "CREATE TABLE C_CONTRADICTION (a BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_CONTRADICTION WHERE a = 5 AND a = 7"));

    // Nested OR across different dims. Expected widening: SkipScanFilter applies "AND
    // across slots, OR within a slot" — it cannot express cross-dim OR like
    // `(a=5, any b) OR (any a, b=10)`. V1 and V2 both fall back to a full-table scan
    // with the predicate in the residual filter. Client sees correct results.
    out.add(new Case("C_CROSS_DIM_OR",
      "CREATE TABLE C_CROSS_DIM_OR (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_CROSS_DIM_OR WHERE a = 5 OR b = 10",
      "cross-dim OR cannot be expressed as a single SkipScanFilter; V1-identical"));

    // RVC equality (point lookup via RVC syntax).
    out.add(new Case("C_RVC_EQ",
      "CREATE TABLE C_RVC_EQ (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_RVC_EQ WHERE (a, b) = (5, 10)"));

    // RVC IN.
    out.add(new Case("C_RVC_IN",
      "CREATE TABLE C_RVC_IN (a BIGINT NOT NULL, b BIGINT NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY (a, b))",
      "SELECT * FROM C_RVC_IN WHERE (a, b) IN ((1, 2), (3, 4), (5, 6))"));

    // Leading PK range with trailing non-PK filter (tests residual plumbing).
    out.add(new Case("C_LEAD_RANGE_NONPK",
      "CREATE TABLE C_LEAD_RANGE_NONPK (a BIGINT NOT NULL, b BIGINT, "
        + "CONSTRAINT pk PRIMARY KEY (a))",
      "SELECT * FROM C_LEAD_RANGE_NONPK WHERE a >= 3 AND a <= 7 AND b = 99"));

    return out;
  }
}
