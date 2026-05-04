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

import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.EQ;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.GE;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.GT;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.LE;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.LT;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.and;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.or;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.pred;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Differential test harness. Each case supplies:
 * <ul>
 * <li>A CREATE TABLE statement (simple unsalted single-tenant tables only).</li>
 * <li>A SELECT query with a WHERE clause.</li>
 * <li>A hand-authored {@link AbstractExpression} equivalent to the WHERE.</li>
 * <li>A set of candidate values per PK column for row enumeration.</li>
 * </ul>
 * The harness:
 * <ol>
 * <li>Compiles the query with V2 enabled → captures {@link ScanRanges}.</li>
 * <li>Decodes {@link ScanRanges} → {@link AbstractKeySpaceList} (V2 view).</li>
 * <li>Runs {@link Oracle#extract} on the hand-authored expression → oracle view.</li>
 * <li>Enumerates every row in the candidate grid and checks:
 *   <ul>
 *   <li><b>Oracle soundness</b>: every row the expression matches is in the oracle's
 *   list (oracle bug if violated).</li>
 *   <li><b>V2 soundness</b>: every row the expression matches is in V2's list
 *   (production bug if violated).</li>
 *   <li><b>Widening</b>: every row in V2's list is also in the oracle's list. The
 *   harness reports if V2 is wider than the oracle (not a bug, but noteworthy).</li>
 *   </ul>
 * </li>
 * </ol>
 */
public class DifferentialHarnessTest extends BaseConnectionlessQueryTest {

  /**
   * testRVCScanBoundaries1's first case, run through the harness.
   * Query: {@code category = 'category_0' AND score <= 5000
   * AND (score, pk, sk) > (4990, 'pk_90', 4990)}.
   */
  @Test
  public void rvcScanBoundaries1_firstCase() throws SQLException {
    String tableName = "T_RVC1";
    String ddl = "CREATE TABLE " + tableName + " ("
      + "category VARCHAR NOT NULL, score DECIMAL NOT NULL, "
      + "pk VARCHAR NOT NULL, sk BIGINT NOT NULL, val VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (category, score, pk, sk))";
    String query = "SELECT * FROM " + tableName
      + " WHERE category = 'category_0' AND score <= 5000"
      + " AND (score, pk, sk) > (4990, 'pk_90', 4990)";

    // Hand-authored equivalent. Values use Java types that match PDataType.toObject
    // outputs for DECIMAL (BigDecimal), VARCHAR (String), BIGINT (Long).
    AbstractExpression expr = and(
      pred(0, EQ, "category_0"),
      pred(1, LE, new java.math.BigDecimal("5000")),
      or(
        pred(1, GT, new java.math.BigDecimal("4990")),
        and(pred(1, EQ, new java.math.BigDecimal("4990")), pred(2, GT, "pk_90")),
        and(pred(1, EQ, new java.math.BigDecimal("4990")), pred(2, EQ, "pk_90"),
          pred(3, GT, 4990L))
      ));

    // Enumeration domain: 3 categories × 4 scores × 3 pk × 3 sk = 108 rows.
    List<List<Object>> perDim = Arrays.asList(
      Arrays.<Object>asList("category_0", "category_1", "category_2"),
      Arrays.<Object>asList(
        new java.math.BigDecimal("4989"),
        new java.math.BigDecimal("4990"),
        new java.math.BigDecimal("4991"),
        new java.math.BigDecimal("5001")),
      Arrays.<Object>asList("pk_89", "pk_90", "pk_91"),
      Arrays.<Object>asList(4989L, 4990L, 4991L));

    run(ddl, tableName, query, expr, perDim);
  }

  /** Simple leading-PK range. */
  @Test
  public void simpleLeadingRange() throws SQLException {
    String tableName = "T_SIMPLE";
    String ddl = "CREATE TABLE " + tableName
      + " (a BIGINT NOT NULL, b BIGINT NOT NULL, CONSTRAINT pk PRIMARY KEY (a, b))";
    String query = "SELECT * FROM " + tableName + " WHERE a >= 5 AND a < 10";
    AbstractExpression expr = and(pred(0, GE, 5L), pred(0, LT, 10L));
    List<List<Object>> perDim = Arrays.asList(
      longs(0L, 3L, 5L, 7L, 10L, 15L),
      longs(0L, 1L, 2L));
    run(ddl, tableName, query, expr, perDim);
  }

  /** OR on leading PK — two disjoint ranges. */
  @Test
  public void orOnLeadingPk() throws SQLException {
    String tableName = "T_OR";
    String ddl = "CREATE TABLE " + tableName
      + " (a BIGINT NOT NULL, b BIGINT NOT NULL, CONSTRAINT pk PRIMARY KEY (a, b))";
    String query = "SELECT * FROM " + tableName + " WHERE a = 3 OR a = 7";
    AbstractExpression expr = or(pred(0, EQ, 3L), pred(0, EQ, 7L));
    List<List<Object>> perDim = Arrays.asList(
      longs(0L, 3L, 5L, 7L, 10L),
      longs(0L, 1L, 2L));
    run(ddl, tableName, query, expr, perDim);
  }

  /** Degeneracy on a non-leading PK col (PHOENIX-6669). */
  @Test
  public void degeneracyOnNonLeadingPk() throws SQLException {
    String tableName = "T_DEGEN";
    String ddl = "CREATE TABLE " + tableName
      + " (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, "
      + "CONSTRAINT pk PRIMARY KEY (a, b, c))";
    String query = "SELECT * FROM " + tableName
      + " WHERE a = 1 AND b >= 10 AND b < 5";
    AbstractExpression expr = and(
      pred(0, EQ, 1L),
      pred(1, GE, 10L),
      pred(1, LT, 5L));
    List<List<Object>> perDim = Arrays.asList(
      longs(0L, 1L, 2L),
      longs(0L, 3L, 7L, 10L, 15L),
      longs(0L, 5L));
    // Expression is unsatisfiable by construction (testing PHOENIX-6669).
    run(ddl, tableName, query, expr, perDim, false);
  }

  /** Leading equality + trailing range. */
  @Test
  public void leadingEqTrailingRange() throws SQLException {
    String tableName = "T_LEQTR";
    String ddl = "CREATE TABLE " + tableName
      + " (a BIGINT NOT NULL, b BIGINT NOT NULL, CONSTRAINT pk PRIMARY KEY (a, b))";
    String query = "SELECT * FROM " + tableName + " WHERE a = 5 AND b >= 10 AND b <= 20";
    AbstractExpression expr = and(pred(0, EQ, 5L), pred(1, GE, 10L), pred(1, LE, 20L));
    List<List<Object>> perDim = Arrays.asList(
      longs(0L, 5L, 7L),
      longs(5L, 10L, 15L, 20L, 25L));
    run(ddl, tableName, query, expr, perDim);
  }

  // ---------- machinery ----------

  private void run(String ddl, String tableName, String query, AbstractExpression expr,
    List<List<Object>> perDim) throws SQLException {
    run(ddl, tableName, query, expr, perDim, true);
  }

  private void run(String ddl, String tableName, String query, AbstractExpression expr,
    List<List<Object>> perDim, boolean expectMatches) throws SQLException {
    Properties props = new Properties();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Drop if table already exists from a prior run, then create.
      try {
        conn.createStatement().executeUpdate("DROP TABLE " + tableName);
      } catch (SQLException ignore) { /* table didn't exist */ }
      conn.createStatement().execute(ddl);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
      QueryPlan plan = pstmt.compileQuery();
      ScanRanges sr = plan.getContext().getScanRanges();
      PTable table = plan.getContext().getCurrentTable().getTable();

      AbstractKeySpaceList v2View;
      try {
        v2View = ScanRangesDecoder.decode(sr, table);
        System.err.println(tableName + " v2View=" + v2View);
      } catch (ScanRangesDecoder.UnsupportedEncodingShape e) {
        // Skip cases whose shape the decoder can't handle yet (e.g. salted).
        System.err.println("SKIP " + tableName + ": " + e.getMessage());
        return;
      }

      AbstractKeySpaceList oracleView = Oracle.extract(expr, perDim.size());

      List<HarnessAssertions.Row> rows = HarnessAssertions.enumerateRows(perDim);
      HarnessAssertions.Report report =
        HarnessAssertions.evaluate(expr, oracleView, v2View, rows);

      System.err.println(tableName + " " + report);
      if (!report.oracleSound()) {
        fail("Oracle soundness violated for " + tableName
          + "; missing rows: " + report.oracleMissesExprMatch);
      }
      if (!report.v2Sound()) {
        fail("V2 soundness violated for " + tableName
          + "; missing rows: " + report.soundnessViolations);
      }
      // Widening is informational, not a failure.
      if (!report.v2SubsetOfOracle()) {
        System.err.println("  V2 wider than oracle on rows: " + report.wideningViolations);
      }
      if (expectMatches) {
        assertTrue("expression matched at least one row (sanity)", report.exprMatches > 0);
      }
    }
  }

  private static List<Object> longs(Long... vs) {
    return Collections.<Object>unmodifiableList(Arrays.<Object>asList(vs));
  }
}
