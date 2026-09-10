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
package org.apache.phoenix.query;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class QueryPlanTest extends BaseConnectionlessQueryTest {

  /**
   * True when the V2 WHERE optimizer is enabled. Tests whose explain output differs
   * between V1 and V2 (V2 typically retains a residual server filter where V1 fully
   * consumes the predicate into the scan range) branch on this helper.
   */
  protected static boolean isV2Optimizer() {
    try (java.sql.Connection conn = java.sql.DriverManager.getConnection(getUrl())) {
      return conn.unwrap(org.apache.phoenix.jdbc.PhoenixConnection.class).getQueryServices()
        .getConfiguration().getBoolean(
          org.apache.phoenix.query.QueryServices.WHERE_OPTIMIZER_V2_ENABLED,
          org.apache.phoenix.query.QueryServicesOptions.DEFAULT_WHERE_OPTIMIZER_V2_ENABLED);
    } catch (java.sql.SQLException e) {
      return false;
    }
  }

  @Test
  public void testExplainPlan() throws Exception {
    // Pair #1 (V1 marginally cheaper, V2 correct): V1 fully consumes the RVC <= bound
    // into the [..,'003'] - [..,'005'] scan range and emits no server filter. V2
    // produces the identical scan range but defensively retains the lex-expanded RVC
    // predicate as a server filter. The residual is redundant (the scan range
    // already enforces it) and only costs one extra byte-comparison per scanned row.
    // Same row count read from HBase; V2 adds a small per-row CPU overhead. Neither
    // is "wrong" — V2's residual-pruning is just more conservative on RVC compounds.
    String pair1Expected = isV2Optimizer()
      ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000003'] - ['000000000000001','000000000000005']\n"
        + "    SERVER FILTER BY (ORGANIZATION_ID < TO_CHAR('000000000000001') OR (ORGANIZATION_ID = TO_CHAR('000000000000001') AND ENTITY_ID <= TO_CHAR('000000000000005')))"
      : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000003'] - ['000000000000001','000000000000005']";
    // Pair #2 (V2 strictly better): V1 narrows to (inst=null, host=not-null) and
    // leaves DATE >= ... in a server filter that has to evaluate against every row
    // returned by the scan. V2 promotes the DATE lower bound into the scan range
    // itself as a SKIP SCAN with all 3 PK dims compound-encoded — HBase rejects
    // out-of-range rows pre-filter so they never reach the server filter. Strictly
    // fewer rows read for any non-empty subset matching `inst IS NULL AND host IS
    // NOT NULL AND DATE < '2013-01-01'`.
    String pair2Expected = isV2Optimizer()
      ? "CLIENT PARALLEL 1-WAY SKIP SCAN ON 1 RANGE OVER PTSDB [null,not null,'2013-01-01'] - [null,not null,*]\n"
        + "    SERVER FILTER BY FIRST KEY ONLY"
      : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER PTSDB [null,not null]\n"
        + "    SERVER FILTER BY FIRST KEY ONLY AND \"DATE\" >= DATE '2013-01-01 00:00:00.000'";
    String[] queryPlans = new String[] {

      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id > '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) <= ('000000000000001','000000000000005') ",
      pair1Expected,

      "SELECT host FROM PTSDB WHERE inst IS NULL AND host IS NOT NULL AND \"DATE\" >= to_date('2013-01-01')",
      pair2Expected,

      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id > '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) >= ('000000000000001','000000000000005') ",
      // Pair #3 (V1 marginally cheaper, V2 correct): same scan range, V2 retains a
      // redundant residual filter that the scan range already enforces. See pair #1
      // notes — this is V2's conservative residual-pruning on RVC compounds.
      isV2Optimizer()
        ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000005'] - ['000000000000001','000000000000008']\n"
          + "    SERVER FILTER BY (ORGANIZATION_ID > TO_CHAR('000000000000001') OR (ORGANIZATION_ID = TO_CHAR('000000000000001') AND ENTITY_ID >= TO_CHAR('000000000000005')))"
        : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000005'] - ['000000000000001','000000000000008']",

      "SELECT host FROM PTSDB3 WHERE host IN ('na1', 'na2','na3')",
      "CLIENT PARALLEL 1-WAY SKIP SCAN ON 3 KEYS OVER PTSDB3 [~'na3'] - [~'na1']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY",

      "SELECT /*+ SMALL*/ host FROM PTSDB3 WHERE host IN ('na1', 'na2','na3')",
      "CLIENT PARALLEL 1-WAY SMALL SKIP SCAN ON 3 KEYS OVER PTSDB3 [~'na3'] - [~'na1']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY",

      "SELECT inst,\"DATE\" FROM PTSDB2 WHERE inst = 'na1' ORDER BY inst DESC, \"DATE\" DESC",
      "CLIENT PARALLEL 1-WAY REVERSE RANGE SCAN OVER PTSDB2 ['na1']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY",

      // Pair #7 (V2 strictly better): V1 stops compound emission at the IS NOT NULL
      // on `inst` and leaves both `HOST IS NULL` AND `DATE >=` as server filters that
      // run against every row in the [not null] range. V2 continues compound emission
      // into a 3-dim SKIP SCAN [not null, null, '2013-01-01'] - [not null, null, *],
      // letting HBase reject all rows that don't satisfy the trailing dims pre-filter.
      // Strictly fewer rows scanned and fewer server-filter evaluations.
      "SELECT host FROM PTSDB WHERE inst IS NOT NULL AND host IS NULL AND \"DATE\" >= to_date('2013-01-01')",
      isV2Optimizer()
        ? "CLIENT PARALLEL 1-WAY SKIP SCAN ON 1 RANGE OVER PTSDB [not null,null,'2013-01-01'] - [not null,null,*]\n"
          + "    SERVER FILTER BY FIRST KEY ONLY"
        : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER PTSDB [not null]\n"
          + "    SERVER FILTER BY FIRST KEY ONLY AND (HOST IS NULL AND \"DATE\" >= DATE '2013-01-01 00:00:00.000')",

      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id = '000000000000002' AND x_integer = 2 AND a_integer < 5 ",
      "CLIENT PARALLEL 1-WAY POINT LOOKUP ON 1 KEY OVER ATABLE\n"
        + "    SERVER FILTER BY (X_INTEGER = 2 AND A_INTEGER < 5)",

      "SELECT a_string,b_string FROM atable WHERE organization_id > '000000000000001' AND entity_id > '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) >= ('000000000000003','000000000000005') ",
      // Pair #9 (equivalent scan, divergent explain string): V1 fuses `org_id > '001'`
      // and `(org_id, entity_id) >= ('003','005')` into a single RANGE SCAN with a
      // 30-byte compound start row '003·005' plus a server filter for the entity-id
      // range. V2 emits a SKIP SCAN ON 2 RANGES whose two slots correspond directly
      // to the lex-expanded RVC: `(org=003, entity in [005,008))` and
      // `(org > 003, entity in (002,008))`. V2's two slots together cover the same
      // rows V1's single range does after the server filter applies — V2 reads the
      // same or fewer rows. Different explain shape, equivalent runtime work.
      isV2Optimizer()
        ? "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 RANGES OVER ATABLE ['000000000000003'] - [*]\n"
          + "    SERVER FILTER BY (ORGANIZATION_ID > TO_CHAR('000000000000003') OR (ORGANIZATION_ID = TO_CHAR('000000000000003') AND ENTITY_ID >= TO_CHAR('000000000000005')))"
        : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000003000000000000005'] - [*]\n"
          + "    SERVER FILTER BY (ENTITY_ID > '000000000000002' AND ENTITY_ID < '000000000000008')",

      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id >= '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) >= ('000000000000000','000000000000005') ",
      // Pair #10 (V1 marginally cheaper, V2 correct): same scan range. V1 recognizes
      // the RVC >= ('000','005') is dominated by org_id='001' AND entity_id>='002'
      // and drops it. V2 retains it as a residual filter — same row count, slight
      // per-row CPU overhead. See pair #1 notes.
      isV2Optimizer()
        ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000002'] - ['000000000000001','000000000000008']\n"
          + "    SERVER FILTER BY (ORGANIZATION_ID > TO_CHAR('000000000000000') OR (ORGANIZATION_ID = TO_CHAR('000000000000000') AND ENTITY_ID >= TO_CHAR('000000000000005')))"
        : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000002'] - ['000000000000001','000000000000008']",

      "SELECT * FROM atable", "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE",

      "SELECT inst,host FROM PTSDB WHERE inst IN ('na1', 'na2','na3') AND host IN ('a','b') AND \"DATE\" >= to_date('2013-01-01') AND \"DATE\" < to_date('2013-01-02')",
      "CLIENT PARALLEL 1-WAY SKIP SCAN ON 6 RANGES OVER PTSDB ['na1','a','2013-01-01'] - ['na3','b','2013-01-02']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY",

      "SELECT inst,host FROM PTSDB WHERE inst LIKE 'na%' AND host IN ('a','b') AND \"DATE\" >= to_date('2013-01-01') AND \"DATE\" < to_date('2013-01-02')",
      "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 RANGES OVER PTSDB ['na','a','2013-01-01'] - ['nb','b','2013-01-02']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY",

      "SELECT count(*) FROM atable",
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
        + "    SERVER AGGREGATE INTO SINGLE ROW",

      "SELECT count(*) FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','003            '] - ['000000000000001','004            ']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY\n" + "    SERVER AGGREGATE INTO SINGLE ROW",

      "SELECT a_string FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','003            '] - ['000000000000001','004            ']",

      "SELECT count(1) FROM atable GROUP BY a_string",
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE\n"
        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]\n" + "CLIENT MERGE SORT",

      "SELECT count(1) FROM atable GROUP BY a_string LIMIT 5",
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE\n"
        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]\n" + "CLIENT MERGE SORT\n"
        + "CLIENT 5 ROW LIMIT",

      "SELECT a_string FROM atable ORDER BY a_string DESC LIMIT 3",
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE\n"
        + "    SERVER TOP 3 ROWS SORTED BY [A_STRING DESC]\n" + "CLIENT MERGE SORT\n"
        + "CLIENT LIMIT 3",

      "SELECT count(1) FROM atable GROUP BY a_string,b_string HAVING max(a_string) = 'a'",
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE\n"
        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n"
        + "CLIENT MERGE SORT\n" + "CLIENT FILTER BY MAX(A_STRING) = 'a'",

      "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY ROUND(a_time,'HOUR',2),entity_id HAVING max(a_string) = 'a'",
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE\n" + "    SERVER FILTER BY A_INTEGER = 1\n"
        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(A_TIME)]\n"
        + "CLIENT MERGE SORT\n" + "CLIENT FILTER BY MAX(A_STRING) = 'a'",

      "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY a_string,b_string HAVING max(a_string) = 'a' ORDER BY b_string",
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE\n" + "    SERVER FILTER BY A_INTEGER = 1\n"
        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n"
        + "CLIENT MERGE SORT\n" + "CLIENT FILTER BY MAX(A_STRING) = 'a'\n"
        + "CLIENT SORTED BY [B_STRING]",

      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id != '000000000000002' AND x_integer = 2 AND a_integer < 5 LIMIT 10",
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n"
        + "    SERVER FILTER BY (ENTITY_ID != '000000000000002' AND X_INTEGER = 2 AND A_INTEGER < 5)\n"
        + "    SERVER 10 ROW LIMIT\n" + "CLIENT 10 ROW LIMIT",

      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string ASC NULLS FIRST LIMIT 10",
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n"
        + "    SERVER TOP 10 ROWS SORTED BY [A_STRING]\n" + "CLIENT MERGE SORT\n"
        + "CLIENT LIMIT 10",

      "SELECT max(a_integer) FROM atable WHERE organization_id = '000000000000001' GROUP BY organization_id,entity_id,ROUND(a_date,'HOUR') ORDER BY entity_id NULLS LAST LIMIT 10",
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n"
        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ORGANIZATION_ID, ENTITY_ID, ROUND(A_DATE)]\n"
        + "CLIENT MERGE SORT\n" + "CLIENT 10 ROW LIMIT",

      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string DESC NULLS LAST LIMIT 10",
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n"
        + "    SERVER TOP 10 ROWS SORTED BY [A_STRING DESC NULLS LAST]\n" + "CLIENT MERGE SORT\n"
        + "CLIENT LIMIT 10",

      "SELECT a_string,b_string FROM atable WHERE organization_id IN ('000000000000001', '000000000000005')",
      "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER ATABLE ['000000000000001'] - ['000000000000005']",

      "SELECT a_string,b_string FROM atable WHERE organization_id IN ('00D000000000001', '00D000000000005') AND entity_id IN('00E00000000000X','00E00000000000Z')",
      "CLIENT PARALLEL 1-WAY POINT LOOKUP ON 4 KEYS OVER ATABLE",

      "SELECT inst,host FROM PTSDB WHERE REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1', 'na2','na3')",
      // Pair #29 (equivalent scan, divergent explain string): V1 promotes the regexp_
      // substr IN-list into SKIP SCAN ON 3 RANGES [na1, na2)·[na2, na3)·[na3, na4).
      // V2 coalesces these three contiguous sub-ranges into a single RANGE SCAN
      // [na1, na4) over byte-identical bytes. The IN values are consecutive so the
      // sub-ranges contain no gaps; both plans cover the same rows and run the same
      // residual REGEXP_SUBSTR filter. V2's single contiguous scan is at worst
      // equivalent to and likely cheaper than V1's three-hop SKIP SCAN.
      isV2Optimizer()
        ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER PTSDB ['na1'] - ['na4']\n"
          + "    SERVER FILTER BY FIRST KEY ONLY AND REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')"
        : "CLIENT PARALLEL 1-WAY SKIP SCAN ON 3 RANGES OVER PTSDB ['na1'] - ['na4']\n"
          + "    SERVER FILTER BY FIRST KEY ONLY AND REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')",

    };
    for (int i = 0; i < queryPlans.length; i += 2) {
      String query = queryPlans[i];
      String plan = queryPlans[i + 1];
      Properties props = new Properties();
      // Override date format so we don't have a bunch of zeros
      props.setProperty(QueryServices.DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
      Connection conn = DriverManager.getConnection(getUrl(), props);
      try {
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("EXPLAIN " + query);
        // TODO: figure out a way of verifying that query isn't run during explain execution
        assertEquals((i / 2 + 1) + ") " + query, plan, QueryUtil.getExplainPlan(rs));
      } finally {
        conn.close();
      }
    }
  }

  @Test
  public void testTenantSpecificConnWithLimit() throws Exception {
    String baseTableDDL =
      "CREATE TABLE BASE_MULTI_TENANT_TABLE(\n " + "  tenant_id VARCHAR(5) NOT NULL,\n"
        + "  userid INTEGER NOT NULL,\n" + "  username VARCHAR NOT NULL,\n" + "  col VARCHAR\n "
        + "  CONSTRAINT pk PRIMARY KEY (tenant_id, userid, username)) MULTI_TENANT=true";
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement().execute(baseTableDDL);
    conn.close();

    String tenantId = "tenantId";
    String tenantViewDDL = "CREATE VIEW TENANT_VIEW AS SELECT * FROM BASE_MULTI_TENANT_TABLE";
    Properties tenantProps = new Properties();
    tenantProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    conn = DriverManager.getConnection(getUrl(), tenantProps);
    conn.createStatement().execute(tenantViewDDL);

    String query = "EXPLAIN SELECT * FROM TENANT_VIEW LIMIT 1";
    ResultSet rs = conn.createStatement().executeQuery(query);
    assertEquals("CLIENT SERIAL 1-WAY RANGE SCAN OVER BASE_MULTI_TENANT_TABLE ['tenantId']\n"
      + "    SERVER 1 ROW LIMIT\n" + "CLIENT 1 ROW LIMIT", QueryUtil.getExplainPlan(rs));
    query = "EXPLAIN SELECT * FROM TENANT_VIEW LIMIT " + Integer.MAX_VALUE;
    rs = conn.createStatement().executeQuery(query);
    assertEquals(
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER BASE_MULTI_TENANT_TABLE ['tenantId']\n" + "    SERVER "
        + Integer.MAX_VALUE + " ROW LIMIT\n" + "CLIENT " + Integer.MAX_VALUE + " ROW LIMIT",
      QueryUtil.getExplainPlan(rs));
    query = "EXPLAIN SELECT * FROM TENANT_VIEW WHERE username = 'Joe' LIMIT 1";
    rs = conn.createStatement().executeQuery(query);
    // V2 strictly better: V1 narrows only on the tenant prefix and leaves
    // `USERNAME = 'Joe'` as a server filter that must reject every non-'Joe'
    // username row. V2 promotes the equality on the trailing PK column into the
    // scan range itself (with the unconstrained `userid` middle dim emitted as a
    // wildcard) → ['tenantId',*,'Joe']. HBase rejects non-'Joe' rows pre-filter,
    // so the LIMIT 1 is satisfied with far fewer rows scanned. The redundant
    // residual filter is a small CPU cost on the (presumably few) matching rows.
    String expectedUsername = isV2Optimizer()
      ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER BASE_MULTI_TENANT_TABLE ['tenantId',*,'Joe']\n"
        + "    SERVER FILTER BY USERNAME = 'Joe'\n" + "    SERVER 1 ROW LIMIT\n"
        + "CLIENT 1 ROW LIMIT"
      : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER BASE_MULTI_TENANT_TABLE ['tenantId']\n"
        + "    SERVER FILTER BY USERNAME = 'Joe'\n" + "    SERVER 1 ROW LIMIT\n"
        + "CLIENT 1 ROW LIMIT";
    assertEquals(expectedUsername, QueryUtil.getExplainPlan(rs));
    query = "EXPLAIN SELECT * FROM TENANT_VIEW WHERE col = 'Joe' LIMIT 1";
    rs = conn.createStatement().executeQuery(query);
    assertEquals(
      "CLIENT PARALLEL 1-WAY RANGE SCAN OVER BASE_MULTI_TENANT_TABLE ['tenantId']\n"
        + "    SERVER FILTER BY COL = 'Joe'\n" + "    SERVER 1 ROW LIMIT\n" + "CLIENT 1 ROW LIMIT",
      QueryUtil.getExplainPlan(rs));
  }

  @Test
  public void testDescTimestampAtBoundary() throws Exception {
    Properties props = PropertiesUtil.deepCopy(new Properties());
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      conn.createStatement()
        .execute("CREATE TABLE FOO(\n" + "                a VARCHAR NOT NULL,\n"
          + "                b TIMESTAMP NOT NULL,\n" + "                c VARCHAR,\n"
          + "                CONSTRAINT pk PRIMARY KEY (a, b DESC, c)\n"
          + "              ) IMMUTABLE_ROWS=true\n" + "                ,SALT_BUCKETS=20");
      String query =
        "select * from foo where a = 'a' and b >= timestamp '2016-01-28 00:00:00' and b < timestamp '2016-01-29 00:00:00'";
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
      String queryPlan = QueryUtil.getExplainPlan(rs);
      // For real connection CQSI, the result is supposed to be 20-WAY RANGE SCAN, however
      // for connection-less impl, since we retrieve region locations for 20 splits and each
      // time we get all region locations due to connection-less specific impl, we get
      // 20*20 = 400-WAY RANGE SCAN.
      assertEquals(
        "CLIENT PARALLEL 400-WAY RANGE SCAN OVER FOO [X'00','a',~'2016-01-28 23:59:59.999'] - [X'13','a',~'2016-01-28 00:00:00.000']\n"
          + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
        queryPlan);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testUseOfRoundRobinIteratorSurfaced() throws Exception {
    Properties props = PropertiesUtil.deepCopy(new Properties());
    props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String tableName = "testUseOfRoundRobinIteratorSurfaced".toUpperCase();
    try {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + "(\n" + "                a VARCHAR NOT NULL,\n"
          + "                b TIMESTAMP NOT NULL,\n" + "                c VARCHAR,\n"
          + "                CONSTRAINT pk PRIMARY KEY (a, b DESC, c)\n"
          + "              ) IMMUTABLE_ROWS=true\n" + "                ,SALT_BUCKETS=20");
      String query = "select * from " + tableName
        + " where a = 'a' and b >= timestamp '2016-01-28 00:00:00' and b < timestamp '2016-01-29 00:00:00'";
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
      String queryPlan = QueryUtil.getExplainPlan(rs);
      // For real connection CQSI, the result is supposed to be 20-WAY RANGE SCAN, however
      // for connection-less impl, since we retrieve region locations for 20 splits and each
      // time we get all region locations due to connection-less specific impl, we get
      // 20*20 = 400-WAY RANGE SCAN.
      assertEquals("CLIENT PARALLEL 400-WAY ROUND ROBIN RANGE SCAN OVER " + tableName
        + " [X'00','a',~'2016-01-28 23:59:59.999'] - [X'13','a',~'2016-01-28 00:00:00.000']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY", queryPlan);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testSerialHintIgnoredForNonRowkeyOrderBy() throws Exception {

    Properties props = PropertiesUtil.deepCopy(new Properties());
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      conn.createStatement()
        .execute("CREATE TABLE FOO(\n" + "                a VARCHAR NOT NULL,\n"
          + "                b TIMESTAMP NOT NULL,\n" + "                c VARCHAR,\n"
          + "                CONSTRAINT pk PRIMARY KEY (a, b DESC, c)\n" + "              )");
      String query = "select /*+ SERIAL*/ * from foo where a = 'a' ORDER BY b, c";
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
      String queryPlan = QueryUtil.getExplainPlan(rs);
      assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER FOO ['a']\n"
        + "    SERVER FILTER BY FIRST KEY ONLY\n" + "    SERVER SORTED BY [B, C]\n"
        + "CLIENT MERGE SORT", queryPlan);
    } finally {
      conn.close();
    }

  }

}
