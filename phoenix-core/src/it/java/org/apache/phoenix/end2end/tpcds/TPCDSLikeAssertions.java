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
package org.apache.phoenix.end2end.tpcds;

import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.phoenix.query.explain.ExplainPlanTestUtil;
import org.apache.phoenix.query.explain.ExplainPlanTestUtil.ExplainPlanAssert;

/**
 * Result and EXPLAIN assertion helpers for the TPC-DS derived ITs.
 * <p>
 * {@link #assertResult(Connection, String, String, String[][])} compares the full, ordered result
 * set of {@code sql} against an embedded {@code expected} array captured from the deterministic
 * {@link TPCDSLikeFixture}. Each adapted query carries a total {@code ORDER BY} so the row order is
 * stable. Cell values are compared via {@link ResultSet#getString(int)}.
 * <p>
 * <b>Regenerating expected arrays.</b> Run either IT with
 * {@code -D}{@value #REGENERATE_PROPERTY}{@code =true}. In that mode {@code assertResult} prints a
 * paste-ready literal to stdout for each query. Paste the printed blocks back into the IT and
 * commit.
 */
public final class TPCDSLikeAssertions {

  public static final String REGENERATE_PROPERTY = "phoenix.tpcds.regenerate";

  public static final String REGENERATE_ENV = "PHOENIX_TPCDS_REGENERATE";

  private static final boolean REGENERATE = Boolean.getBoolean(REGENERATE_PROPERTY)
    || "true".equalsIgnoreCase(System.getenv(REGENERATE_ENV));

  private TPCDSLikeAssertions() {
  }

  public static boolean isRegenerating() {
    return REGENERATE;
  }

  /**
   * Execute {@code sql} and either assert its rows equal {@code expected} (normal mode) or print a
   * paste-ready expected-array literal (regenerate mode).
   * @param conn     a Phoenix connection
   * @param label    the query label, e.g. {@code "Q03"}; only used to name the printed literal
   * @param sql      the query to run
   * @param expected the embedded expected rows (ignored in capture mode)
   * @return {@code true} if the call captured/printed instead of asserting (regenerate mode, or an
   *         empty {@code expected} that has not yet been populated); {@code false} if it asserted
   */
  public static boolean assertResult(Connection conn, String label, String sql, String[][] expected)
    throws SQLException {
    List<String[]> actual = run(conn, sql);
    // Capture mode: either forced via the flag, or bootstrapping an as-yet-unpopulated query.
    if (REGENERATE || expected == null || expected.length == 0) {
      System.out.println((expected == null || expected.length == 0
        ? "// NOTE: " + label + " has no embedded expected rows yet; paste the block below.\n"
        : "") + format(label, actual));
      return true;
    }
    String mismatch = diff(expected, actual);
    if (mismatch != null) {
      fail("Result mismatch for " + label + ": " + mismatch + "\nRe-run with -D"
        + REGENERATE_PROPERTY + "=true (or env " + REGENERATE_ENV
        + "=true) to capture the current output.\nFull query:\n" + sql);
    }
    return false;
  }

  public static String captureLiteral(Connection conn, String label, String sql)
    throws SQLException {
    return format(label, run(conn, sql));
  }

  /** Begin EXPLAIN-plan assertions for {@code sql} (delegates to {@link ExplainPlanTestUtil}). */
  public static ExplainPlanAssert assertPlan(Connection conn, String sql) throws SQLException {
    return ExplainPlanTestUtil.assertPlan(conn, sql);
  }

  /** Assert the rendered EXPLAIN plan text for {@code sql} contains every {@code needle}. */
  public static void assertPlanContains(Connection conn, String sql, String... needles)
    throws SQLException {
    List<String> steps = ExplainPlanTestUtil.getPlanSteps(conn, sql);
    String text = String.join("\n", steps);
    for (String needle : needles) {
      if (!text.contains(needle)) {
        fail("EXPLAIN plan missing expected fragment:\n  expected to contain: " + needle
          + "\nactual plan:\n" + text + "\nquery:\n" + sql);
      }
    }
  }

  /**
   * Assert the rendered EXPLAIN plan text for {@code sql} contains at least one of {@code needles}.
   */
  public static void assertPlanContainsAny(Connection conn, String sql, String... needles)
    throws SQLException {
    List<String> steps = ExplainPlanTestUtil.getPlanSteps(conn, sql);
    String text = String.join("\n", steps);
    for (String needle : needles) {
      if (text.contains(needle)) {
        return;
      }
    }
    fail("EXPLAIN plan missing all expected fragments " + Arrays.toString(needles) + ":\n" + text
      + "\nquery:\n" + sql);
  }

  private static List<String[]> run(Connection conn, String sql) throws SQLException {
    List<String[]> rows = new ArrayList<>();
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
      ResultSetMetaData md = rs.getMetaData();
      int n = md.getColumnCount();
      while (rs.next()) {
        String[] row = new String[n];
        for (int i = 1; i <= n; i++) {
          row[i - 1] = canonical(rs.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /**
   * Canonicalize a JDBC value into a stable locale independent string. Numbers are normalized via
   * {@link BigDecimal} with trailing zeros stripped (so {@code 37.00 -> "37"}, {@code 1081.50 ->
   * "1081.5"}, {@code 0 -> "0"}). Everything else uses {@code toString()}.
   */
  private static String canonical(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      BigDecimal b = new BigDecimal(value.toString());
      return b.signum() == 0 ? "0" : b.stripTrailingZeros().toPlainString();
    }
    return value.toString();
  }

  private static String diff(String[][] expected, List<String[]> actual) {
    if (expected == null) {
      expected = new String[0][];
    }
    if (expected.length != actual.size()) {
      return "row count expected=" + expected.length + " actual=" + actual.size();
    }
    for (int r = 0; r < expected.length; r++) {
      String[] e = expected[r];
      String[] a = actual.get(r);
      if (e.length != a.length) {
        return "row " + r + " column count expected=" + e.length + " actual=" + a.length;
      }
      for (int c = 0; c < e.length; c++) {
        if (!eq(e[c], a[c])) {
          return "row " + r + " col " + c + " expected=" + lit(e[c]) + " actual=" + lit(a[c])
            + "\n  expectedRow=" + Arrays.toString(e) + "\n  actualRow=  " + Arrays.toString(a);
        }
      }
    }
    return null;
  }

  private static boolean eq(String a, String b) {
    return a == null ? b == null : a.equals(b);
  }

  private static String lit(String s) {
    return s == null ? "null" : "\"" + s + "\"";
  }

  private static String format(String label, List<String[]> rows) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n  // ---- ").append(label).append(" (").append(rows.size())
      .append(" rows) ----\n");
    sb.append("  private static final String[][] ").append(label).append("_EXPECTED = {\n");
    for (String[] row : rows) {
      sb.append("    { ");
      for (int c = 0; c < row.length; c++) {
        if (c > 0) {
          sb.append(", ");
        }
        sb.append(lit(row[c]));
      }
      sb.append(" },\n");
    }
    sb.append("  };\n");
    return sb.toString();
  }
}
