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

import static org.apache.phoenix.query.QueryServices.AUTO_COMMIT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.DATE_FORMAT_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Backward compatibility tests for Phoenix EXPLAIN output.
 * <p>
 * Each corpus {@code @Test} method compiles a representative query against a connectionless Phoenix
 * driver, builds the expected normalized plan-steps text and JSON attributes inline, and hands both
 * to the {@link ExplainOracle} for a tolerant comparison. The corpus covers every EXPLAIN grammar
 * branch reachable without a connection.
 */
public class ExplainPlanTest extends BaseConnectionlessQueryTest {

  private static final String SALTED = "EO_SALTED";
  private static final String SEQ = "EO_SEQ";
  private static final String MT_BASE = "EO_MT_BASE";
  private static final String MT_VIEW = "EO_MT_VIEW";
  private static final String TENANT_ID = "tenant42";

  private static ExplainOracle oracle;
  private static ObjectMapper mapper;

  @BeforeClass
  public static synchronized void setUp() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + SALTED
        + " (k VARCHAR NOT NULL PRIMARY KEY, v INTEGER) SALT_BUCKETS=4");
      conn.createStatement().execute("CREATE SEQUENCE IF NOT EXISTS " + SEQ);
      conn.createStatement()
        .execute("CREATE TABLE IF NOT EXISTS " + MT_BASE + " (" + "  tenant_id VARCHAR(8) NOT NULL,"
          + "  userid INTEGER NOT NULL," + "  username VARCHAR NOT NULL," + "  col VARCHAR"
          + "  CONSTRAINT pk PRIMARY KEY (tenant_id, userid, username)) MULTI_TENANT=true");
    }
    Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    tenantProps.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    tenantProps.setProperty(TENANT_ID_ATTRIB, TENANT_ID);
    try (Connection conn = DriverManager.getConnection(getUrl(), tenantProps)) {
      conn.createStatement()
        .execute("CREATE VIEW IF NOT EXISTS " + MT_VIEW + " AS SELECT * FROM " + MT_BASE);
    }

    oracle = new ExplainOracle();
    mapper = oracle.mapper();
  }

  @Test
  public void testPointLookup() throws Exception {
    verifyQuery("pointLookup",
      "SELECT a_string, b_string FROM atable"
        + " WHERE organization_id = '00D000000000001' AND entity_id = '00E00000000001'"
        + " AND x_integer = 2 AND a_integer < 5",
      text("CLIENT PARALLEL <N>-WAY POINT LOOKUP ON 1 KEY OVER ATABLE",
        "    SERVER FILTER BY (X_INTEGER = 2 AND A_INTEGER < 5)"),
      scanAttrs("POINT LOOKUP ON 1 KEY ", "ATABLE", null).put("serverWhereFilter",
        "SERVER FILTER BY (X_INTEGER = 2 AND A_INTEGER < 5)"));
  }

  @Test
  public void testPointLookupMultiKey() throws Exception {
    verifyQuery("pointLookupMultiKey",
      "SELECT a_string, b_string FROM atable"
        + " WHERE organization_id IN ('00D000000000001', '00D000000000005')"
        + " AND entity_id IN ('00E00000000000X','00E00000000000Z')",
      text("CLIENT PARALLEL <N>-WAY POINT LOOKUP ON 4 KEYS OVER ATABLE"),
      scanAttrs("POINT LOOKUP ON 4 KEYS ", "ATABLE", null));
  }

  @Test
  public void testRangeScan() throws Exception {
    verifyQuery("rangeScan",
      "SELECT a_string FROM atable WHERE organization_id = '00D000000000001'"
        + " AND entity_id > '00E00000000002' AND entity_id < '00E00000000008'",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE"
        + " ['00D000000000001','00E00000000002!'] - ['00D000000000001','00E00000000008 ']"),
      scanAttrs("RANGE SCAN ", "ATABLE",
        " ['00D000000000001','00E00000000002!'] - ['00D000000000001','00E00000000008 ']"));
  }

  @Test
  public void testSkipScanKeys() throws Exception {
    verifyQuery("skipScanKeys", "SELECT host FROM ptsdb3 WHERE host IN ('na1','na2','na3')",
      text("CLIENT PARALLEL <N>-WAY SKIP SCAN ON 3 KEYS OVER PTSDB3 [~'na3'] - [~'na1']",
        "    SERVER FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 3 KEYS ", "PTSDB3", " [~'na3'] - [~'na1']").put("serverWhereFilter",
        "SERVER FILTER BY FIRST KEY ONLY"));
  }

  @Test
  public void testSkipScanRanges() throws Exception {
    verifyQuery("skipScanRanges",
      "SELECT inst,host FROM ptsdb WHERE inst IN ('na1','na2','na3')"
        + " AND host IN ('a','b') AND \"DATE\" >= to_date('2013-01-01')"
        + " AND \"DATE\" < to_date('2013-01-02')",
      text(
        "CLIENT PARALLEL <N>-WAY SKIP SCAN ON 6 RANGES OVER PTSDB"
          + " ['na1','a','2013-01-01'] - ['na3','b','2013-01-02']",
        "    SERVER FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 6 RANGES ", "PTSDB",
        " ['na1','a','2013-01-01'] - ['na3','b','2013-01-02']").put("serverWhereFilter",
          "SERVER FILTER BY FIRST KEY ONLY"));
  }

  @Test
  public void testFullScan() throws Exception {
    verifyQuery("fullScan", "SELECT * FROM atable",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE"), scanAttrs("FULL SCAN ", "ATABLE", ""));
  }

  @Test
  public void testReverseScan() throws Exception {
    verifyQuery("reverseScan",
      "SELECT inst,\"DATE\" FROM ptsdb2 WHERE inst = 'na1' ORDER BY inst DESC, \"DATE\" DESC",
      text("CLIENT PARALLEL <N>-WAY REVERSE RANGE SCAN OVER PTSDB2 ['na1']",
        "    SERVER FILTER BY FIRST KEY ONLY"),
      scanAttrs("RANGE SCAN ", "PTSDB2", " ['na1']")
        .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY")
        .put("clientSortedBy", "REVERSE"));
  }

  @Test
  public void testSmallHint() throws Exception {
    verifyQuery("smallHint",
      "SELECT /*+ SMALL */ host FROM ptsdb3 WHERE host IN ('na1','na2','na3')",
      text("CLIENT PARALLEL <N>-WAY SMALL SKIP SCAN ON 3 KEYS OVER PTSDB3 [~'na3'] - [~'na1']",
        "    SERVER FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 3 KEYS ", "PTSDB3", " [~'na3'] - [~'na1']").put("hint", "SMALL")
        .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY"));
  }

  @Test
  public void testAggregateSingleRow() throws Exception {
    verifyQuery("aggregateSingleRow", "SELECT count(*) FROM atable",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    SERVER FILTER BY FIRST KEY ONLY",
        "    SERVER AGGREGATE INTO SINGLE ROW"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY")
        .put("serverAggregate", "SERVER AGGREGATE INTO SINGLE ROW"));
  }

  @Test
  public void testAggregateOrderedDistinct() throws Exception {
    verifyQuery("aggregateOrderedDistinct", "SELECT count(1) FROM atable GROUP BY a_string",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]", "CLIENT MERGE SORT"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]")
        .put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testAggregateHashDistinct() throws Exception {
    verifyQuery("aggregateHashDistinct",
      "SELECT count(1) FROM atable WHERE a_integer = 1"
        + " GROUP BY ROUND(a_time,'HOUR',2), entity_id",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    SERVER FILTER BY A_INTEGER = 1",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(A_TIME)]",
        "CLIENT MERGE SORT"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverWhereFilter", "SERVER FILTER BY A_INTEGER = 1")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(A_TIME)]")
        .put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testTopNSortedBy() throws Exception {
    verifyQuery("topNSortedBy", "SELECT a_string FROM atable ORDER BY a_string DESC LIMIT 3",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    SERVER TOP 3 ROWS SORTED BY [A_STRING DESC]", "CLIENT MERGE SORT", "CLIENT LIMIT 3"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("serverSortedBy", "[A_STRING DESC]")
        .put("serverRowLimit", 3).put("clientRowLimit", 3)
        .put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testClientFilterByMax() throws Exception {
    verifyQuery("clientFilterByMax",
      "SELECT count(1) FROM atable GROUP BY a_string, b_string HAVING max(a_string) = 'a'",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]", "CLIENT MERGE SORT",
        "CLIENT FILTER BY MAX(A_STRING) = 'a'"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]")
        .put("clientFilterBy", "MAX(A_STRING) = 'a'").put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testClientLimit() throws Exception {
    verifyQuery("clientLimit",
      "SELECT a_string, b_string FROM atable"
        + " WHERE organization_id = '00D000000000001' AND entity_id != '00E00000000002'"
        + " AND x_integer = 2 AND a_integer < 5 LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "    SERVER FILTER BY (ENTITY_ID != '00E00000000002' AND X_INTEGER = 2 AND A_INTEGER < 5)",
        "    SERVER 10 ROW LIMIT", "CLIENT 10 ROW LIMIT"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']")
        .put("serverWhereFilter",
          "SERVER FILTER BY (ENTITY_ID != '00E00000000002' AND X_INTEGER = 2 AND A_INTEGER < 5)")
        .put("serverRowLimit", 10).put("clientRowLimit", 10));
  }

  @Test
  public void testArrayElementProjection() throws Exception {
    verifyQuery("arrayElementProjection", "SELECT a_string_array[1] FROM table_with_array",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER TABLE_WITH_ARRAY",
        "    SERVER ARRAY ELEMENT PROJECTION"),
      scanAttrs("FULL SCAN ", "TABLE_WITH_ARRAY", "").put("serverArrayElementProjection", true));
  }

  @Test
  public void testRangeScanCompositeRvcUpperBound() throws Exception {
    verifyQuery("rangeScanCompositeRvcUpper",
      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001'"
        + " AND entity_id > '000000000000002' AND entity_id < '000000000000008'"
        + " AND (organization_id,entity_id) <= ('000000000000001','000000000000005')",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE"
        + " ['000000000000001','000000000000003'] - ['000000000000001','000000000000005']"),
      scanAttrs("RANGE SCAN ", "ATABLE",
        " ['000000000000001','000000000000003'] - ['000000000000001','000000000000005']"));
  }

  @Test
  public void testRangeScanCompositeRvcOpenUpperBound() throws Exception {
    verifyQuery("rangeScanCompositeRvcOpen",
      "SELECT a_string,b_string FROM atable WHERE organization_id > '000000000000001'"
        + " AND entity_id > '000000000000002' AND entity_id < '000000000000008'"
        + " AND (organization_id,entity_id) >= ('000000000000003','000000000000005')",
      text(
        "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE"
          + " ['000000000000003000000000000005'] - [*]",
        "    SERVER FILTER BY (ENTITY_ID > '000000000000002' AND ENTITY_ID < '000000000000008')"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['000000000000003000000000000005'] - [*]").put(
        "serverWhereFilter",
        "SERVER FILTER BY (ENTITY_ID > '000000000000002' AND ENTITY_ID < '000000000000008')"));
  }

  @Test
  public void testRangeScanNullNotNull() throws Exception {
    verifyQuery("rangeScanNullNotNull",
      "SELECT host FROM PTSDB WHERE inst IS NULL AND host IS NOT NULL"
        + " AND \"DATE\" >= to_date('2013-01-01')",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER PTSDB [null,not null]",
        "    SERVER FILTER BY FIRST KEY ONLY AND \"DATE\" >= DATE '2013-01-01 00:00:00.000'"),
      scanAttrs("RANGE SCAN ", "PTSDB", " [null,not null]").put("serverWhereFilter",
        "SERVER FILTER BY FIRST KEY ONLY AND \"DATE\" >= DATE '2013-01-01 00:00:00.000'"));
  }

  @Test
  public void testRangeScanNotNull() throws Exception {
    verifyQuery("rangeScanNotNull",
      "SELECT host FROM PTSDB WHERE inst IS NOT NULL AND host IS NULL"
        + " AND \"DATE\" >= to_date('2013-01-01')",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER PTSDB [not null]",
        "    SERVER FILTER BY FIRST KEY ONLY AND (HOST IS NULL"
          + " AND \"DATE\" >= DATE '2013-01-01 00:00:00.000')"),
      scanAttrs("RANGE SCAN ", "PTSDB", " [not null]").put("serverWhereFilter",
        "SERVER FILTER BY FIRST KEY ONLY AND (HOST IS NULL"
          + " AND \"DATE\" >= DATE '2013-01-01 00:00:00.000')"));
  }

  @Test
  public void testSkipScanLikeRanges() throws Exception {
    verifyQuery("skipScanLikeRanges",
      "SELECT inst,host FROM PTSDB WHERE inst LIKE 'na%' AND host IN ('a','b')"
        + " AND \"DATE\" >= to_date('2013-01-01') AND \"DATE\" < to_date('2013-01-02')",
      text(
        "CLIENT PARALLEL <N>-WAY SKIP SCAN ON 2 RANGES OVER PTSDB"
          + " ['na','a','2013-01-01'] - ['nb','b','2013-01-02']",
        "    SERVER FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 2 RANGES ", "PTSDB",
        " ['na','a','2013-01-01'] - ['nb','b','2013-01-02']").put("serverWhereFilter",
          "SERVER FILTER BY FIRST KEY ONLY"));
  }

  @Test
  public void testSkipScanRegexpRanges() throws Exception {
    verifyQuery("skipScanRegexpRanges",
      "SELECT inst,host FROM PTSDB WHERE REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1', 'na2','na3')",
      text("CLIENT PARALLEL <N>-WAY SKIP SCAN ON 3 RANGES OVER PTSDB ['na1'] - ['na4']",
        "    SERVER FILTER BY FIRST KEY ONLY AND"
          + " REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')"),
      scanAttrs("SKIP SCAN ON 3 RANGES ", "PTSDB", " ['na1'] - ['na4']").put("serverWhereFilter",
        "SERVER FILTER BY FIRST KEY ONLY AND REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')"));
  }

  @Test
  public void testRangeScanSubstrBounds() throws Exception {
    verifyQuery("rangeScanSubstrBounds",
      "SELECT a_string FROM atable WHERE organization_id='000000000000001'"
        + " AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE"
        + " ['000000000000001','003            '] - ['000000000000001','004            ']"),
      scanAttrs("RANGE SCAN ", "ATABLE",
        " ['000000000000001','003            '] - ['000000000000001','004            ']"));
  }

  @Test
  public void testSkipScanTwoKeys() throws Exception {
    verifyQuery("skipScanTwoKeys",
      "SELECT a_string,b_string FROM atable"
        + " WHERE organization_id IN ('000000000000001', '000000000000005')",
      text("CLIENT PARALLEL <N>-WAY SKIP SCAN ON 2 KEYS OVER ATABLE"
        + " ['000000000000001'] - ['000000000000005']"),
      scanAttrs("SKIP SCAN ON 2 KEYS ", "ATABLE", " ['000000000000001'] - ['000000000000005']"));
  }

  @Test
  public void testGroupByClientLimit() throws Exception {
    verifyQuery("groupByClientLimit", "SELECT count(1) FROM atable GROUP BY a_string LIMIT 5",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]", "CLIENT MERGE SORT",
        "CLIENT 5 ROW LIMIT"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]")
        .put("clientRowLimit", 5).put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testTopNAscNullsFirstLimit() throws Exception {
    verifyQuery("topNAscNullsFirstLimit",
      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001'"
        + " ORDER BY a_string ASC NULLS FIRST LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['000000000000001']",
        "    SERVER TOP 10 ROWS SORTED BY [A_STRING]", "CLIENT MERGE SORT", "CLIENT LIMIT 10"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['000000000000001']").put("serverSortedBy", "[A_STRING]")
        .put("serverRowLimit", 10).put("clientRowLimit", 10)
        .put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testTopNDescNullsLastLimit() throws Exception {
    verifyQuery("topNDescNullsLastLimit",
      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001'"
        + " ORDER BY a_string DESC NULLS LAST LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['000000000000001']",
        "    SERVER TOP 10 ROWS SORTED BY [A_STRING DESC NULLS LAST]", "CLIENT MERGE SORT",
        "CLIENT LIMIT 10"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['000000000000001']")
        .put("serverSortedBy", "[A_STRING DESC NULLS LAST]").put("serverRowLimit", 10)
        .put("clientRowLimit", 10).put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testAggregateOrderByClientLimit() throws Exception {
    verifyQuery("aggregateOrderByClientLimit",
      "SELECT max(a_integer) FROM atable WHERE organization_id = '000000000000001'"
        + " GROUP BY organization_id,entity_id,ROUND(a_date,'HOUR')"
        + " ORDER BY entity_id NULLS LAST LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['000000000000001']",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY"
          + " [ORGANIZATION_ID, ENTITY_ID, ROUND(A_DATE)]",
        "CLIENT MERGE SORT", "CLIENT 10 ROW LIMIT"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['000000000000001']")
        .put("serverAggregate",
          "SERVER AGGREGATE INTO DISTINCT ROWS BY [ORGANIZATION_ID, ENTITY_ID, ROUND(A_DATE)]")
        .put("clientRowLimit", 10).put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testClientSortedByHaving() throws Exception {
    verifyQuery("clientSortedByHaving",
      "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY a_string,b_string"
        + " HAVING max(a_string) = 'a' ORDER BY b_string",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    SERVER FILTER BY A_INTEGER = 1",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]", "CLIENT MERGE SORT",
        "CLIENT FILTER BY MAX(A_STRING) = 'a'", "CLIENT SORTED BY [B_STRING]"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverWhereFilter", "SERVER FILTER BY A_INTEGER = 1")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]")
        .put("clientFilterBy", "MAX(A_STRING) = 'a'").put("clientSortedBy", "[B_STRING]")
        .put("clientOffset", 0).put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testSortMergeJoin() throws Exception {
    ObjectNode rhs = scanAttrs("FULL SCAN ", "ATABLE", "");
    verifyQuery("sortMergeJoin",
      "SELECT /*+ USE_SORT_MERGE_JOIN */ a.a_string, b.a_string FROM atable a"
        + " JOIN atable b ON a.organization_id = b.organization_id"
        + " WHERE a.organization_id = '00D000000000001'",
      text("SORT-MERGE-JOIN (INNER) TABLES",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']", "AND",
        "    CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']")
        .put("abstractExplainPlan", "SORT-MERGE-JOIN (INNER)").set("rhsJoinQueryExplainPlan", rhs));
  }

  @Test
  public void testHashJoinInner() throws Exception {
    // HashJoinPlan root attributes come from the delegate scan. Each hash/skip-scan child
    // is recorded under subPlans with its join header on abstractExplainPlan.
    ObjectNode child = scanAttrs("FULL SCAN ", "ATABLE", "").put("abstractExplainPlan",
      "PARALLEL INNER-JOIN TABLE 0");
    ObjectNode expected = scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']");
    expected.set("subPlans", mapper.createArrayNode().add(child));
    expected.put("dynamicServerFilter",
      "DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)");
    verifyQuery("hashJoinInner",
      "SELECT a.a_string, b.a_string FROM atable a"
        + " JOIN atable b ON a.organization_id = b.organization_id"
        + " WHERE a.organization_id = '00D000000000001'",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "    PARALLEL INNER-JOIN TABLE 0", "        CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)"),
      expected);
  }

  @Test
  public void testHashJoinSemiInSubquery() throws Exception {
    ObjectNode child =
      scanAttrs("FULL SCAN ", "ATABLE", "").put("abstractExplainPlan", "SKIP-SCAN-JOIN TABLE 0")
        .put("serverWhereFilter", "SERVER FILTER BY A_INTEGER = 1")
        .put("serverAggregate", "SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [ORGANIZATION_ID]");
    ObjectNode expected = scanAttrs("FULL SCAN ", "ATABLE", "");
    expected.set("subPlans", mapper.createArrayNode().add(child));
    expected.put("dynamicServerFilter",
      "DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($1.$3)");
    verifyQuery("hashJoinSemiInSubquery",
      "SELECT a_string FROM atable"
        + " WHERE organization_id IN (SELECT organization_id FROM atable WHERE a_integer = 1)",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    SKIP-SCAN-JOIN TABLE 0",
        "        CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "            SERVER FILTER BY A_INTEGER = 1",
        "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [ORGANIZATION_ID]",
        "    DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($1.$3)"),
      expected);
  }

  @Test
  public void testUnionAll() throws Exception {
    ObjectNode rhs = scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000002']");
    verifyQuery("unionAll",
      "SELECT a_string FROM atable WHERE organization_id = '00D000000000001'" + " UNION ALL"
        + " SELECT a_string FROM atable WHERE organization_id = '00D000000000002'",
      text("UNION ALL OVER 2 QUERIES",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000002']"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']")
        .put("abstractExplainPlan", "UNION ALL OVER 2 QUERIES")
        .set("rhsJoinQueryExplainPlan", rhs));
  }

  @Test
  public void testPutSingleRow() throws Exception {
    verifyMutation("putSingleRow",
      "UPSERT INTO atable (organization_id, entity_id, a_string)"
        + " VALUES ('00D000000000001','00E00000000001','x')",
      false, text("PUT SINGLE ROW"), defaultAttrs());
  }

  @Test
  public void testUpsertSelectClient() throws Exception {
    verifyMutation("upsertSelectClient",
      "UPSERT INTO atable (organization_id, entity_id, a_string)"
        + " SELECT organization_id, entity_id, a_string FROM atable"
        + " WHERE organization_id = '00D000000000001'",
      false,
      text("UPSERT SELECT", "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']").put("abstractExplainPlan",
        "UPSERT SELECT"));
  }

  @Test
  public void testUpsertSelectServer() throws Exception {
    verifyMutation("upsertSelectServer",
      "UPSERT INTO atable (organization_id, entity_id, a_string)"
        + " SELECT organization_id, entity_id, a_string FROM atable"
        + " WHERE organization_id = '00D000000000001'",
      true,
      text("UPSERT ROWS", "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']").put("abstractExplainPlan",
        "UPSERT ROWS"));
  }

  @Test
  public void testDeleteSingleRow() throws Exception {
    verifyMutation("deleteSingleRow",
      "DELETE FROM atable WHERE organization_id = '00D000000000001'"
        + " AND entity_id = '00E00000000001'",
      true, text("DELETE SINGLE ROW"),
      defaultAttrs().put("abstractExplainPlan", "DELETE SINGLE ROW"));
  }

  @Test
  public void testDeleteServer() throws Exception {
    verifyMutation("deleteServer", "DELETE FROM atable WHERE entity_id = 'abc'", true,
      text("DELETE ROWS SERVER SELECT", "CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    SERVER FILTER BY FIRST KEY ONLY AND ENTITY_ID = 'abc'"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("abstractExplainPlan", "DELETE ROWS SERVER SELECT")
        .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY AND ENTITY_ID = 'abc'"));
  }

  @Test
  public void testDeleteClient() throws Exception {
    verifyMutation("deleteClient", "DELETE FROM atable WHERE entity_id = 'abc'", false,
      text("DELETE ROWS CLIENT SELECT", "CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    SERVER FILTER BY FIRST KEY ONLY AND ENTITY_ID = 'abc'"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("abstractExplainPlan", "DELETE ROWS CLIENT SELECT")
        .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY AND ENTITY_ID = 'abc'"));
  }

  @Test
  public void testSequenceNextValue() throws Exception {
    verifyQuery("sequenceNextValue", "SELECT NEXT VALUE FOR " + SEQ + " FROM atable",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    SERVER FILTER BY FIRST KEY ONLY",
        "CLIENT RESERVE VALUES FROM 1 SEQUENCE"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY").put("clientSequenceCount", 1));
  }

  @Test
  public void testSaltedTableScan() throws Exception {
    verifyQuery("saltedTableScan", "SELECT * FROM " + SALTED + " WHERE v = 7",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER EO_SALTED", "    SERVER FILTER BY V = 7",
        "CLIENT MERGE SORT"),
      scanAttrs("FULL SCAN ", "EO_SALTED", "").put("serverWhereFilter", "SERVER FILTER BY V = 7")
        .put("clientSortAlgo", "CLIENT MERGE SORT"));
  }

  @Test
  public void testMultiTenantView() throws Exception {
    Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    tenantProps.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    tenantProps.setProperty(TENANT_ID_ATTRIB, TENANT_ID);
    verifyQuery("multiTenantView", "SELECT * FROM " + MT_VIEW + " LIMIT 1", tenantProps,
      text("CLIENT SERIAL <N>-WAY RANGE SCAN OVER EO_MT_BASE ['tenant42']",
        "    SERVER 1 ROW LIMIT", "CLIENT 1 ROW LIMIT"),
      attrs().put("iteratorTypeAndScanSize", "SERIAL <N>-WAY").put("consistency", "STRONG")
        .put("explainScanType", "RANGE SCAN ").put("tableName", "EO_MT_BASE")
        .put("keyRanges", " ['tenant42']").put("serverRowLimit", 1).put("clientRowLimit", 1));
  }

  @Test
  public void testTextNormalizerCollapsesWayCount() {
    assertEquals(Collections.singletonList("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE"),
      new ExplainTextNormalizer()
        .normalize(Arrays.asList("CLIENT PARALLEL 400-WAY FULL SCAN OVER ATABLE")));
  }

  @Test
  public void testTextNormalizerCollapsesChunkCount() {
    assertEquals(
      Collections.singletonList("CLIENT <N>-CHUNK PARALLEL <N>-WAY FULL SCAN OVER ATABLE"),
      new ExplainTextNormalizer()
        .normalize(Arrays.asList("CLIENT 5-CHUNK PARALLEL 16-WAY FULL SCAN OVER ATABLE")));
  }

  @Test
  public void testTextNormalizerStripsRowsBytes() {
    assertEquals(
      Collections.singletonList("CLIENT <N>-CHUNK PARALLEL <N>-WAY FULL SCAN OVER ATABLE"),
      new ExplainTextNormalizer().normalize(
        Arrays.asList("CLIENT 1-CHUNK 100 ROWS 2048 BYTES PARALLEL 1-WAY FULL SCAN OVER ATABLE")));
  }

  @Test
  public void testTextNormalizerDropsRegionLocationsLine() {
    assertEquals(
      Arrays.asList("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    SERVER FILTER BY FIRST KEY ONLY"),
      new ExplainTextNormalizer().normalize(Arrays.asList(
        "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE", "    SERVER FILTER BY FIRST KEY ONLY",
        " (region locations = [{startKey=\\x00, endKey=, server=foo,1234}])")));
  }

  @Test
  public void testTextNormalizerPreservesAllGrammar() {
    List<String> in = Arrays.asList("CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['a','b']",
      "    SERVER FILTER BY (X = 1 AND Y = 'z')",
      "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [Y]", "CLIENT MERGE SORT",
      "CLIENT 3 ROW LIMIT");
    assertEquals(Arrays.asList("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['a','b']",
      "    SERVER FILTER BY (X = 1 AND Y = 'z')",
      "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [Y]", "CLIENT MERGE SORT",
      "CLIENT 3 ROW LIMIT"), new ExplainTextNormalizer().normalize(in));
  }

  @Test
  public void testJsonNormalizerErasesClusterFields() {
    ObjectNode root = mapper.createObjectNode();
    root.put("iteratorTypeAndScanSize", "PARALLEL 16-WAY");
    root.put("splitsChunk", 4);
    root.put("estimatedRows", 1234L);
    root.put("estimatedSizeInBytes", 9876L);
    root.put("numRegionLocationLookups", 7);
    root.set("regionLocations", mapper.createArrayNode().add("anything"));

    new ExplainJsonNormalizer().normalize(root);

    assertEquals("PARALLEL <N>-WAY", root.get("iteratorTypeAndScanSize").asText());
    assertTrue(root.get("splitsChunk").isNull());
    assertTrue(root.get("estimatedRows").isNull());
    assertTrue(root.get("estimatedSizeInBytes").isNull());
    assertTrue(root.get("regionLocations").isNull());
    assertEquals(0, root.get("numRegionLocationLookups").asInt());
  }

  @Test
  public void testJsonNormalizerRecursesIntoRhsJoinQueryExplainPlan() {
    ObjectNode root = mapper.createObjectNode();
    root.put("iteratorTypeAndScanSize", "PARALLEL 5-WAY");
    root.put("numRegionLocationLookups", 1);
    ObjectNode rhs = mapper.createObjectNode();
    rhs.put("iteratorTypeAndScanSize", "PARALLEL 12-WAY");
    rhs.put("numRegionLocationLookups", 99);
    rhs.set("regionLocations", mapper.createArrayNode().add(1));
    root.set("rhsJoinQueryExplainPlan", rhs);

    new ExplainJsonNormalizer().normalize(root);

    assertEquals("PARALLEL <N>-WAY", root.get("iteratorTypeAndScanSize").asText());
    assertEquals(0, root.get("numRegionLocationLookups").asInt());
    JsonNode nestedRhs = root.get("rhsJoinQueryExplainPlan");
    assertEquals("PARALLEL <N>-WAY", nestedRhs.get("iteratorTypeAndScanSize").asText());
    assertEquals(0, nestedRhs.get("numRegionLocationLookups").asInt());
    assertTrue(nestedRhs.get("regionLocations").isNull());
  }

  @Test
  public void testJacksonFieldOrderMatchesPropertyOrderAnnotation() throws Exception {
    // The serialized field order must exactly follow the @JsonPropertyOrder declaration. Deriving
    // the expected order from the annotation keeps this test correct across future reorderings.
    String[] expectedOrder =
      ExplainPlanAttributes.class.getAnnotation(JsonPropertyOrder.class).value();
    String json = mapper.writeValueAsString(new ExplainPlanAttributesBuilder().build());
    int prevIdx = -1;
    String prevName = null;
    for (String name : expectedOrder) {
      int idx = json.indexOf("\"" + name + "\"");
      assertTrue(name + " present in serialized JSON", idx >= 0);
      assertTrue(name + " must serialize after " + prevName, idx > prevIdx);
      prevIdx = idx;
      prevName = name;
    }
  }

  @Test
  public void testRegionLocationsSerializerRendersTriple() throws Exception {
    HRegionLocation loc = new HRegionLocation(
      RegionInfoBuilder.newBuilder(TableName.valueOf("FOO")).setStartKey(new byte[] { 0x01, 0x02 })
        .setEndKey(new byte[] { 0x03, 0x04 }).build(),
      ServerName.valueOf("rs.example.com", 16020, 1234567890L));
    ExplainPlanAttributes a = new ExplainPlanAttributesBuilder()
      .setRegionLocations(Collections.singletonList(loc)).setNumRegionLocationLookups(1).build();
    JsonNode tree = mapper.readTree(mapper.writeValueAsString(a));
    JsonNode entry = tree.get("regionLocations").get(0);
    assertEquals("\\x01\\x02", entry.get("startKey").asText());
    assertEquals("\\x03\\x04", entry.get("endKey").asText());
    assertTrue(entry.get("server").asText().contains("rs.example.com"));
  }

  @Test
  public void testServerMergeColumnsSerializerEmitsSortedNames() throws Exception {
    Set<PColumn> cols = new HashSet<>(
      Arrays.asList(column("CF", "B_COL"), column("CF", "A_COL"), column("CF", "C_COL")));
    ExplainPlanAttributes a =
      new ExplainPlanAttributesBuilder().setServerMergeColumns(cols).build();
    JsonNode tree = mapper.readTree(mapper.writeValueAsString(a));
    JsonNode array = tree.get("serverMergeColumns");
    assertEquals(3, array.size());
    // PColumn.toString() uses QueryConstants.NAME_SEPARATOR (".") between family and name.
    assertEquals("CF.A_COL", array.get(0).asText());
    assertEquals("CF.B_COL", array.get(1).asText());
    assertEquals("CF.C_COL", array.get(2).asText());
  }

  @Test
  public void testIdentityRuleChainPasses() throws Exception {
    ExplainPlan plan = samplePlan("PARALLEL 4-WAY", "RANGE SCAN ");
    List<String> expectedText =
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER T", "    SERVER FILTER BY FIRST KEY ONLY");
    // samplePlan() sets only four attributes on the builder; consistency stays null.
    ObjectNode expectedJson = defaultAttrs().put("iteratorTypeAndScanSize", "PARALLEL <N>-WAY")
      .put("explainScanType", "RANGE SCAN ").put("tableName", "T")
      .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY");
    // Identity rule: returns inputs unchanged.
    new ExplainOracle(Collections.singletonList(new ExplainChangeRule() {
    })).verify("identity", plan, expectedText, expectedJson);
  }

  @Test
  public void testChangeRuleRewritesText() throws Exception {
    // Today's plan emits "SERVER FILTER BY FIRST KEY ONLY".
    ExplainPlanAttributes todayAttrs = new ExplainPlanAttributesBuilder()
      .setIteratorTypeAndScanSize("PARALLEL 1-WAY").setExplainScanType("FULL SCAN ")
      .setTableName("T").setServerWhereFilter("SERVER FILTER BY FIRST KEY ONLY").build();
    ExplainPlan todayPlan = new ExplainPlan(Arrays.asList("CLIENT PARALLEL 1-WAY FULL SCAN OVER T",
      "    SERVER FILTER BY FIRST KEY ONLY"), todayAttrs);
    List<String> todayExpectedText =
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER T", "    SERVER FILTER BY FIRST KEY ONLY");
    // samplePlan-style builder leaves consistency null and key-ranges unset.
    ObjectNode todayExpectedJson = defaultAttrs().put("iteratorTypeAndScanSize", "PARALLEL <N>-WAY")
      .put("explainScanType", "FULL SCAN ").put("tableName", "T")
      .put("serverWhereFilter", "SERVER FILTER BY FIRST KEY ONLY");

    // Sanity: with no rules, today's plan compares against today's expected.
    new ExplainOracle().verify("today", todayPlan, todayExpectedText, todayExpectedJson);

    // Future plan emits the new form. The embedded baseline stays unchanged and a rule transforms
    // the baseline into the new shape so it passes the comparison.
    ExplainPlanAttributes futureAttrs = new ExplainPlanAttributesBuilder()
      .setIteratorTypeAndScanSize("PARALLEL 1-WAY").setExplainScanType("FULL SCAN ")
      .setTableName("T").setServerWhereFilter("SERVER PROJECTION FILTER BY FIRST KEY ONLY").build();
    ExplainPlan futurePlan = new ExplainPlan(Arrays.asList("CLIENT PARALLEL 1-WAY FULL SCAN OVER T",
      "    SERVER PROJECTION FILTER BY FIRST KEY ONLY"), futureAttrs);

    ExplainChangeRule rename = new ExplainChangeRule() {
      @Override
      public List<String> applyText(String caseId, List<String> goldenText) {
        List<String> out = new ArrayList<>(goldenText.size());
        for (String s : goldenText) {
          out.add(s.replace("SERVER FILTER BY FIRST KEY ONLY",
            "SERVER PROJECTION FILTER BY FIRST KEY ONLY"));
        }
        return out;
      }

      @Override
      public JsonNode applyJson(String caseId, JsonNode goldenJson) {
        if (goldenJson.isObject()) {
          ObjectNode obj = (ObjectNode) goldenJson;
          JsonNode swf = obj.get("serverWhereFilter");
          if (
            swf != null && swf.isTextual() && swf.asText().equals("SERVER FILTER BY FIRST KEY ONLY")
          ) {
            obj.put("serverWhereFilter", "SERVER PROJECTION FILTER BY FIRST KEY ONLY");
          }
        }
        return goldenJson;
      }
    };

    new ExplainOracle(Collections.singletonList(rename)).verify("today", futurePlan,
      todayExpectedText, todayExpectedJson);
  }

  @Test
  public void testDiffMessageShowsExpectedAndActualForTextMismatch() {
    ExplainPlan plan = samplePlan("PARALLEL 1-WAY", "FULL SCAN ");
    // Caller's "expected" disagrees with what the plan actually emits.
    List<String> divergentExpectedText =
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER T", "    SERVER FILTER BY (X = 9)");
    ObjectNode divergentExpectedJson = defaultAttrs()
      .put("iteratorTypeAndScanSize", "PARALLEL <N>-WAY").put("explainScanType", "FULL SCAN ")
      .put("tableName", "T").put("serverWhereFilter", "SERVER FILTER BY (X = 9)");
    try {
      new ExplainOracle().verify("x", plan, divergentExpectedText, divergentExpectedJson);
      fail("Expected AssertionError for diverged plan");
    } catch (AssertionError expected) {
      String msg = expected.getMessage();
      assertTrue(msg.contains("Text mismatch for case 'x'"));
      assertTrue(msg.contains("SERVER FILTER BY FIRST KEY ONLY"));
      assertTrue(msg.contains("SERVER FILTER BY (X = 9)"));
    } catch (Exception e) {
      fail("Unexpected exception type: " + e);
    }
  }

  private void verifyQuery(String caseId, String query, List<String> expectedText,
    JsonNode expectedJson) throws Exception {
    verifyQuery(caseId, query, defaultProps(), expectedText, expectedJson);
  }

  private void verifyQuery(String caseId, String query, Properties props, List<String> expectedText,
    JsonNode expectedJson) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ExplainPlan plan = ExplainPlanTestUtil.getExplainPlan(conn, query);
      oracle.verify(caseId, plan, expectedText, expectedJson);
    }
  }

  private void verifyMutation(String caseId, String query, boolean autoCommit,
    List<String> expectedText, JsonNode expectedJson) throws Exception {
    Properties props = defaultProps();
    if (autoCommit) {
      props.setProperty(AUTO_COMMIT_ATTRIB, "true");
    }
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ExplainPlan plan = compileMutation(conn, query);
      oracle.verify(caseId, plan, expectedText, expectedJson);
    }
  }

  private ExplainPlan compileMutation(Connection conn, String query) throws SQLException {
    return ExplainPlanTestUtil.getMutationExplainPlan(conn, query);
  }

  private static Properties defaultProps() {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    return props;
  }

  private static List<String> text(String... lines) {
    return Arrays.asList(lines);
  }

  private static ObjectNode defaultAttrs() {
    ObjectNode n = mapper.createObjectNode();
    n.putNull("abstractExplainPlan");
    n.putNull("splitsChunk");
    n.putNull("estimatedRows");
    n.putNull("estimatedSizeInBytes");
    n.putNull("iteratorTypeAndScanSize");
    n.putNull("samplingRate");
    n.put("useRoundRobinIterator", false);
    n.putNull("hexStringRVCOffset");
    n.putNull("consistency");
    n.putNull("hint");
    n.putNull("serverSortedBy");
    n.putNull("explainScanType");
    n.putNull("tableName");
    n.putNull("keyRanges");
    n.putNull("scanTimeRangeMin");
    n.putNull("scanTimeRangeMax");
    n.putNull("serverWhereFilter");
    n.putNull("serverDistinctFilter");
    n.putNull("serverOffset");
    n.putNull("serverRowLimit");
    n.put("serverArrayElementProjection", false);
    n.putNull("serverAggregate");
    n.putNull("clientFilterBy");
    n.putNull("clientAggregate");
    n.putNull("clientSortedBy");
    n.putNull("clientAfterAggregate");
    n.putNull("clientDistinctFilter");
    n.putNull("clientOffset");
    n.putNull("clientRowLimit");
    n.putNull("clientSequenceCount");
    n.putNull("clientCursorName");
    n.putNull("clientSortAlgo");
    n.putNull("rhsJoinQueryExplainPlan");
    n.putNull("serverMergeColumns");
    n.putNull("regionLocations");
    n.put("numRegionLocationLookups", 0);
    n.putNull("subPlans");
    n.putNull("serverGroupByLimit");
    n.putNull("dynamicServerFilter");
    n.putNull("afterJoinFilter");
    n.putNull("joinScannerLimit");
    n.put("sortMergeSkipMerge", false);
    return n;
  }

  /**
   * Convenience method that builds {@link #defaultAttrs()}.
   * @param scanType the {@code explainScanType} string (with its trailing space, e.g.
   *                 {@code "FULL SCAN "})
   * @param table    the {@code tableName} value
   * @param keys     the {@code keyRanges} string (may be {@code null} or empty)
   */
  private static ObjectNode scanAttrs(String scanType, String table, String keys) {
    ObjectNode n = defaultAttrs();
    n.put("iteratorTypeAndScanSize", "PARALLEL <N>-WAY");
    n.put("consistency", "STRONG");
    n.put("explainScanType", scanType);
    n.put("tableName", table);
    if (keys != null) {
      n.put("keyRanges", keys);
    }
    return n;
  }

  /** A plain {@link ObjectNode} alias for clarity in tests that don't use {@link #scanAttrs}. */
  private static ObjectNode attrs() {
    return defaultAttrs();
  }

  private static ExplainPlan samplePlan(String way, String scanType) {
    ExplainPlanAttributes a = new ExplainPlanAttributesBuilder().setIteratorTypeAndScanSize(way)
      .setExplainScanType(scanType).setTableName("T")
      .setServerWhereFilter("SERVER FILTER BY FIRST KEY ONLY").build();
    return new ExplainPlan(Arrays.asList("CLIENT " + way + " " + scanType.trim() + " OVER T",
      "    SERVER FILTER BY FIRST KEY ONLY"), a);
  }

  private static PColumn column(String family, String name) {
    PName fName = PNameFactory.newName(family);
    PName cName = PNameFactory.newName(name);
    return new PColumnImpl(cName, fName, PInteger.INSTANCE, null, null, false, 0, SortOrder.ASC, 0,
      null, false, "expression", false, false, name.getBytes(), 0L);
  }
}
