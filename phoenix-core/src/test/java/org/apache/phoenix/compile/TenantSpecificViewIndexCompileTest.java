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
package org.apache.phoenix.compile;

import static org.apache.phoenix.query.explain.ExplainPlanTestUtil.assertPlan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.phoenix.optimize.OptimizerReasons;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.explain.ExplainPlanTestUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

public class TenantSpecificViewIndexCompileTest extends BaseConnectionlessQueryTest {

  @Test
  public void testOrderByOptimizedOut() throws Exception {
    Properties props = new Properties();
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement()
      .execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR,"
        + " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");

    String tenantId = "me";
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
    conn = DriverManager.getConnection(getUrl(), props);
    conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k1 = 'a'");
    conn.createStatement().execute("CREATE INDEX i1 ON v(v2) INCLUDE(v1)");

    assertPlan(conn, "SELECT v1,v2 FROM v WHERE v2 > 'a' ORDER BY v2")
      .iteratorType("PARALLEL 1-WAY").scanType("RANGE SCAN").table("_IDX_T")
      .keyRanges(" [-9223372036854775808,'me','a'] - [-9223372036854775808,'me',*]")
      .indexRule(OptimizerReasons.RULE_MORE_BOUND_PK_COLUMNS);
  }

  @Test
  public void testOrderByOptimizedOutWithoutPredicateInView() throws Exception {

    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement().execute(
      "CREATE TABLE t(t_id CHAR(15) NOT NULL, k1 CHAR(3) NOT NULL, k2 CHAR(15) NOT NULL, k3 DATE NOT NULL, v1 VARCHAR,"
        + " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2, k3)) multi_tenant=true");
    conn.createStatement().execute("CREATE VIEW v1  AS SELECT * FROM t");

    conn = createTenantSpecificConnection();

    // Query without predicate ordered by full row key
    String sql = "SELECT * FROM v1 ORDER BY k1, k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Predicate with valid partial PK
    sql = "SELECT * FROM v1 WHERE k1 = 'xyz' ORDER BY k1, k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    sql = "SELECT * FROM v1 WHERE k1 > 'xyz' ORDER BY k1, k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789','xy{'] - ['tenant123456789',*]");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    String datePredicate = createStaticDate();
    sql = "SELECT * FROM v1 WHERE k1 = 'xyz' AND k2 = '123456789012345' AND k3 < TO_DATE('"
      + datePredicate + "') ORDER BY k1, k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz','123456789012345',*] - "
      + "['tenant123456789','xyz','123456789012345','2015-01-01 08:00:00.000']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Predicate without valid partial PK
    sql = "SELECT * FROM v1 WHERE k2 < 'abcde1234567890' ORDER BY k1, k2, k3";
    assertRangeScanWithFilter(conn, sql, " ['tenant123456789']",
      "SERVER FILTER BY K2 < 'abcde1234567890'");
    assertOrderByHasBeenOptimizedOut(conn, sql);
  }

  @Test
  public void testOrderByOptimizedOutWithPredicateInView() throws Exception {
    // Arrange
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement().execute(
      "CREATE TABLE t(t_id CHAR(15) NOT NULL, k1 CHAR(3) NOT NULL, k2 CHAR(15) NOT NULL, k3 DATE NOT NULL, v1 VARCHAR,"
        + " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2, k3)) multi_tenant=true");
    conn.createStatement().execute("CREATE VIEW v1  AS SELECT * FROM t WHERE k1 = 'xyz'");
    conn = createTenantSpecificConnection();

    // Query without predicate ordered by full row key
    String sql = "SELECT * FROM v1 ORDER BY k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Query without predicate ordered by full row key, but without column view predicate
    sql = "SELECT * FROM v1 ORDER BY k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Predicate with valid partial PK
    sql = "SELECT * FROM v1 WHERE k1 = 'xyz' ORDER BY k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    sql = "SELECT * FROM v1 WHERE k2 < 'abcde1234567890' ORDER BY k2, k3";
    assertRangeScan(conn, sql,
      " ['tenant123456789','xyz',*] - ['tenant123456789','xyz','abcde1234567890']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Predicate with full PK
    String datePredicate = createStaticDate();
    sql = "SELECT * FROM v1 WHERE k2 = '123456789012345' AND k3 < TO_DATE('" + datePredicate
      + "') ORDER BY k2, k3";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz','123456789012345',*] - "
      + "['tenant123456789','xyz','123456789012345','2015-01-01 08:00:00.000']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Predicate with valid partial PK
    sql = "SELECT * FROM v1 WHERE k3 < TO_DATE('" + datePredicate + "') ORDER BY k2, k3";
    assertRangeScanWithFilter(conn, sql, " ['tenant123456789','xyz']",
      "SERVER FILTER BY K3 < DATE '" + datePredicate + "'");
    assertOrderByHasBeenOptimizedOut(conn, sql);
  }

  @Test
  public void testOrderByOptimizedOutWithMultiplePredicatesInView() throws Exception {
    // Arrange
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement().execute(
      "CREATE TABLE t(t_id CHAR(15) NOT NULL, k1 CHAR(3) NOT NULL, k2 CHAR(5) NOT NULL, k3 DATE NOT NULL, v1 VARCHAR,"
        + " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2, k3 DESC)) multi_tenant=true");
    conn.createStatement()
      .execute("CREATE VIEW v1  AS SELECT * FROM t WHERE k1 = 'xyz' AND k2='abcde'");
    conn = createTenantSpecificConnection();

    // Query without predicate ordered by full row key
    String sql = "SELECT * FROM v1 ORDER BY k3 DESC";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz','abcde']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Query without predicate ordered by full row key, but without column view predicate
    sql = "SELECT * FROM v1 ORDER BY k3 DESC";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz','abcde']");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Query with predicate ordered by full row key
    sql = "SELECT * FROM v1 WHERE k3 <= TO_DATE('" + createStaticDate() + "') ORDER BY k3 DESC";
    assertRangeScan(conn, sql, " ['tenant123456789','xyz','abcde',~'2015-01-01 08:00:00.000'] - "
      + "['tenant123456789','xyz','abcde',*]");
    assertOrderByHasBeenOptimizedOut(conn, sql);

    // Query with predicate ordered by full row key with date in reverse order
    sql = "SELECT * FROM v1 WHERE k3 <= TO_DATE('" + createStaticDate() + "') ORDER BY k3";
    assertPlan(conn, sql).iteratorType("PARALLEL 1-WAY").scanType("RANGE SCAN").table("T")
      .clientSortedBy("REVERSE")
      .keyRanges(" ['tenant123456789','xyz','abcde',~'2015-01-01 08:00:00.000'] - "
        + "['tenant123456789','xyz','abcde',*]")
      .indexRule(OptimizerReasons.RULE_DATA_TABLE).indexRejectedNone();
    assertOrderByHasBeenOptimizedOut(conn, sql);

  }

  @Test
  public void testViewConstantsOptimizedOut() throws Exception {
    Properties props = new Properties();
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement()
      .execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR,"
        + " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");

    String tenantId = "me";
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
    conn = DriverManager.getConnection(getUrl(), props);
    conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k2 = 'a'");
    conn.createStatement().execute("CREATE INDEX i1 ON v(v2)");

    assertPlan(conn, "SELECT v2 FROM v WHERE v2 > 'a' and k2 = 'a' ORDER BY v2,k2")
      .iteratorType("PARALLEL 1-WAY").scanType("RANGE SCAN").table("_IDX_T")
      .keyRanges(" [-9223372036854775808,'me','a'] - [-9223372036854775808,'me',*]")
      .serverFirstKeyOnlyProjection(true).indexRule(OptimizerReasons.RULE_MORE_BOUND_PK_COLUMNS);

    // Won't use index b/c v1 is not in index, but should optimize out k2 still from the order by
    // K2 will still be referenced in the filter, as these are automatically tacked on to the where
    // clause.
    // The index i1 is rejected because it does not cover v1, leaving the data table as the only
    // surviving candidate.
    assertPlan(conn, "SELECT v1 FROM v WHERE v2 > 'a' ORDER BY k2").iteratorType("PARALLEL 1-WAY")
      .scanType("RANGE SCAN").table("T").keyRanges(" ['me']")
      .serverWhereFilter("SERVER FILTER BY (V2 > 'a' AND K2 = 'a')")
      .indexRule(OptimizerReasons.RULE_ONLY_CANDIDATE).indexRejectedCount(1);

    // If we match K2 against a constant not equal to it's view constant, we should get a degenerate
    // plan. The DEGENERATE SCAN ExplainPlan does not populate structured attributes, so this
    // single-literal check on the plan-steps text is retained.
    List<String> degenerateSteps = ExplainPlanTestUtil.getPlanSteps(conn,
      "SELECT v1 FROM v WHERE v2 > 'a' and k2='b' ORDER BY k2");
    assertEquals(1, degenerateSteps.size());
    assertTrue("expected DEGENERATE SCAN OVER V, got " + degenerateSteps,
      degenerateSteps.get(0).contains("DEGENERATE SCAN OVER V"));
  }

  @Test
  public void testViewConstantsOptimizedOutOnReadOnlyView() throws Exception {
    Properties props = new Properties();
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement()
      .execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR,"
        + " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");

    String tenantId = "me";
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
    conn = DriverManager.getConnection(getUrl(), props);
    conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k2 = 'a'");
    conn.createStatement().execute("CREATE VIEW v2(v3 VARCHAR) AS SELECT * FROM v WHERE k1 > 'a'");
    conn.createStatement().execute("CREATE INDEX i2 ON v2(v3) include(v2)");

    // Confirm that a read-only view on an updatable view still optimizes out the read-only parts of
    // the updatable view
    assertPlan(conn, "SELECT v2 FROM v2 WHERE v3 > 'a' and k2 = 'a' ORDER BY v3,k2")
      .iteratorType("PARALLEL 1-WAY").scanType("RANGE SCAN").table("_IDX_T")
      .keyRanges(" [-9223372036854775808,'me','a'] - [-9223372036854775808,'me',*]")
      .indexRule(OptimizerReasons.RULE_MORE_BOUND_PK_COLUMNS);
  }

  // -----------------------------------------------------------------
  // Private Helper Methods
  // -----------------------------------------------------------------

  private Connection createTenantSpecificConnection() throws SQLException {
    Connection conn;
    Properties props = new Properties();
    String tenantId = "tenant123456789";
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
    conn = DriverManager.getConnection(getUrl(), props);
    return conn;
  }

  private void assertRangeScan(Connection conn, String sql, String keyRanges) throws SQLException {
    assertPlan(conn, sql).iteratorType("PARALLEL 1-WAY").scanType("RANGE SCAN").table("T")
      .keyRanges(keyRanges).indexRule(OptimizerReasons.RULE_DATA_TABLE).indexRejectedNone();
  }

  private void assertRangeScanWithFilter(Connection conn, String sql, String keyRanges,
    String serverWhereFilter) throws SQLException {
    assertPlan(conn, sql).iteratorType("PARALLEL 1-WAY").scanType("RANGE SCAN").table("T")
      .keyRanges(keyRanges).serverWhereFilter(serverWhereFilter)
      .indexRule(OptimizerReasons.RULE_DATA_TABLE).indexRejectedNone();
  }

  private void assertOrderByHasBeenOptimizedOut(Connection conn, String sql) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(sql);
    QueryPlan plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);
    assertEquals(0, plan.getOrderBy().getOrderByExpressions().size());
  }

  /**
   * Returns the default String representation of 1/1/2015 00:00:00
   */
  private String createStaticDate() {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_YEAR, 1);
    cal.set(Calendar.YEAR, 2015);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    cal.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
    return DateUtil.DEFAULT_DATE_FORMATTER.format(cal.getTime());
  }

}
