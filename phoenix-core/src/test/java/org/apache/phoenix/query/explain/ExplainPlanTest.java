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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
import org.apache.phoenix.iterate.ExplainTable;
import org.apache.phoenix.optimize.OptimizerReasons;
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
  private static final String JSON_TBL = "EO_JSON";
  private static final String BSON_TBL = "EO_BSON";

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
      conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + JSON_TBL
        + " (pk VARCHAR NOT NULL PRIMARY KEY, jsoncol JSON)");
      conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + BSON_TBL
        + " (pk VARCHAR NOT NULL PRIMARY KEY, payload BSON)");
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
        "    INDEX ATABLE  /* point lookup */", "    REGIONS PLANNED <N>",
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
      text("CLIENT PARALLEL <N>-WAY POINT LOOKUP ON 4 KEYS OVER ATABLE",
        "    INDEX ATABLE  /* point lookup */", "    REGIONS PLANNED <N>"),
      scanAttrs("POINT LOOKUP ON 4 KEYS ", "ATABLE", null));
  }

  @Test
  public void testRangeScan() throws Exception {
    verifyQuery("rangeScan",
      "SELECT a_string FROM atable WHERE organization_id = '00D000000000001'"
        + " AND entity_id > '00E00000000002' AND entity_id < '00E00000000008'",
      text(
        "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE"
          + " ['00D000000000001','00E00000000002!'] - ['00D000000000001','00E00000000008 ']",
        "    INDEX ATABLE", "    REGIONS PLANNED <N>"),
      scanAttrs("RANGE SCAN ", "ATABLE",
        " ['00D000000000001','00E00000000002!'] - ['00D000000000001','00E00000000008 ']"));
  }

  @Test
  public void testSkipScanKeys() throws Exception {
    verifyQuery("skipScanKeys", "SELECT host FROM ptsdb3 WHERE host IN ('na1','na2','na3')",
      text("CLIENT PARALLEL <N>-WAY SKIP SCAN ON 3 KEYS OVER PTSDB3 [~'na3'] - [~'na1']",
        "    INDEX PTSDB3", "    REGIONS PLANNED <N>",
        "    SERVER PROJECTION FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 3 KEYS ", "PTSDB3", " [~'na3'] - [~'na1']")
        .put("serverFirstKeyOnlyProjection", true));
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
        "    INDEX PTSDB", "    REGIONS PLANNED <N>",
        "    SERVER PROJECTION FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 6 RANGES ", "PTSDB",
        " ['na1','a','2013-01-01'] - ['na3','b','2013-01-02']").put("serverFirstKeyOnlyProjection",
          true));
  }

  @Test
  public void testFullScan() throws Exception {
    verifyQuery("fullScan", "SELECT * FROM atable",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>"),
      scanAttrs("FULL SCAN ", "ATABLE", ""));
  }

  @Test
  public void testReverseScan() throws Exception {
    verifyQuery("reverseScan",
      "SELECT inst,\"DATE\" FROM ptsdb2 WHERE inst = 'na1' ORDER BY inst DESC, \"DATE\" DESC",
      text("CLIENT PARALLEL <N>-WAY REVERSE RANGE SCAN OVER PTSDB2 ['na1']", "    INDEX PTSDB2",
        "    REGIONS PLANNED <N>", "    SERVER PROJECTION FILTER BY FIRST KEY ONLY"),
      scanAttrs("RANGE SCAN ", "PTSDB2", " ['na1']").put("serverFirstKeyOnlyProjection", true)
        .put("clientSortedBy", "REVERSE")
        .set("rewrites", rewriteList("REVERSE SCAN SUBSTITUTION")));
  }

  @Test
  public void testSmallHint() throws Exception {
    verifyQuery("smallHint",
      "SELECT /*+ SMALL */ host FROM ptsdb3 WHERE host IN ('na1','na2','na3')",
      text("CLIENT PARALLEL <N>-WAY SMALL SKIP SCAN ON 3 KEYS OVER PTSDB3 [~'na3'] - [~'na1']",
        "    INDEX PTSDB3", "    REGIONS PLANNED <N>",
        "    SERVER PROJECTION FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 3 KEYS ", "PTSDB3", " [~'na3'] - [~'na1']").put("hint", "SMALL")
        .put("serverFirstKeyOnlyProjection", true));
  }

  @Test
  public void testAggregateSingleRow() throws Exception {
    verifyQuery("aggregateSingleRow", "SELECT count(*) FROM atable",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER PROJECTION FILTER BY FIRST KEY ONLY",
        "    SERVER AGGREGATE INTO SINGLE ROW"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("serverFirstKeyOnlyProjection", true)
        .put("serverAggregate", "SERVER AGGREGATE INTO SINGLE ROW"));
  }

  @Test
  public void testAggregateOrderedDistinct() throws Exception {
    verifyQuery("aggregateOrderedDistinct", "SELECT count(1) FROM atable GROUP BY a_string",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]",
        "CLIENT MERGE SORT"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]")
        .put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT")));
  }

  @Test
  public void testAggregateHashDistinct() throws Exception {
    verifyQuery("aggregateHashDistinct",
      "SELECT count(1) FROM atable WHERE a_integer = 1"
        + " GROUP BY ROUND(a_time,'HOUR',2), entity_id",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER FILTER BY A_INTEGER = 1",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(A_TIME)]",
        "CLIENT MERGE SORT"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverWhereFilter", "SERVER FILTER BY A_INTEGER = 1")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(A_TIME)]")
        .put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT")));
  }

  @Test
  public void testTopNSortedBy() throws Exception {
    verifyQuery("topNSortedBy", "SELECT a_string FROM atable ORDER BY a_string DESC LIMIT 3",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER TOP 3 ROWS SORTED BY [A_STRING DESC]",
        "CLIENT MERGE SORT", "CLIENT LIMIT 3"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("serverSortedBy", "[A_STRING DESC]")
        .put("serverRowLimit", 3).put("clientRowLimit", 3)
        .put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT", "CLIENT LIMIT 3")));
  }

  @Test
  public void testClientFilterByMax() throws Exception {
    verifyQuery("clientFilterByMax",
      "SELECT count(1) FROM atable GROUP BY a_string, b_string HAVING max(a_string) = 'a'",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]", "CLIENT MERGE SORT",
        "CLIENT FILTER BY MAX(A_STRING) = 'a'"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]")
        .put("clientFilterBy", "MAX(A_STRING) = 'a'").put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps",
          clientSteps("CLIENT MERGE SORT", "CLIENT FILTER BY MAX(A_STRING) = 'a'")));
  }

  @Test
  public void testClientLimit() throws Exception {
    verifyQuery("clientLimit",
      "SELECT a_string, b_string FROM atable"
        + " WHERE organization_id = '00D000000000001' AND entity_id != '00E00000000002'"
        + " AND x_integer = 2 AND a_integer < 5 LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>",
        "    SERVER FILTER BY (ENTITY_ID != '00E00000000002' AND X_INTEGER = 2 AND A_INTEGER < 5)",
        "    SERVER 10 ROW LIMIT", "CLIENT 10 ROW LIMIT"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']")
        .put("serverWhereFilter",
          "SERVER FILTER BY (ENTITY_ID != '00E00000000002' AND X_INTEGER = 2 AND A_INTEGER < 5)")
        .put("serverRowLimit", 10).put("clientRowLimit", 10)
        .set("clientSteps", clientSteps("CLIENT 10 ROW LIMIT")));
  }

  @Test
  public void testArrayElementProjection() throws Exception {
    ObjectNode arrayBucket = mapper.createObjectNode();
    arrayBucket.set("ARRAY", mapper.createArrayNode().add("ARRAY_ELEM(A_STRING_ARRAY, 1)"));
    verifyQuery("arrayElementProjection", "SELECT a_string_array[1] FROM table_with_array",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER TABLE_WITH_ARRAY", "    INDEX TABLE_WITH_ARRAY",
        "    REGIONS PLANNED <N>", "    SERVER ARRAY PROJECTION 1",
        "        ARRAY_ELEM(A_STRING_ARRAY, 1)"),
      scanAttrs("FULL SCAN ", "TABLE_WITH_ARRAY", "").set("serverParsedProjections", arrayBucket));
  }

  @Test
  public void testJsonFunctionProjection() throws Exception {
    ObjectNode jsonBucket = mapper.createObjectNode();
    jsonBucket.set("JSON", mapper.createArrayNode().add("JSON_VALUE(JSONCOL, '$.type')"));
    verifyQuery("jsonFunctionProjection", "SELECT JSON_VALUE(jsoncol, '$.type') FROM " + JSON_TBL,
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER " + JSON_TBL, "    INDEX " + JSON_TBL,
        "    REGIONS PLANNED <N>", "    SERVER JSON PROJECTION 1",
        "        JSON_VALUE(JSONCOL, '$.type')"),
      scanAttrs("FULL SCAN ", JSON_TBL, "").set("serverParsedProjections", jsonBucket));
  }

  @Test
  public void testBsonValueProjection() throws Exception {
    ObjectNode bsonBucket = mapper.createObjectNode();
    bsonBucket.set("BSON",
      mapper.createArrayNode().add("BSON_VALUE(PAYLOAD, 'user.id', 'VARCHAR', )"));
    verifyQuery("bsonValueProjection",
      "SELECT BSON_VALUE(payload, 'user.id', 'VARCHAR') FROM " + BSON_TBL,
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER " + BSON_TBL, "    INDEX " + BSON_TBL,
        "    REGIONS PLANNED <N>", "    SERVER BSON PROJECTION 1",
        "        BSON_VALUE(PAYLOAD, 'user.id', 'VARCHAR', )"),
      scanAttrs("FULL SCAN ", BSON_TBL, "").set("serverParsedProjections", bsonBucket));
  }

  @Test
  public void testRangeScanCompositeRvcUpperBound() throws Exception {
    verifyQuery("rangeScanCompositeRvcUpper",
      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001'"
        + " AND entity_id > '000000000000002' AND entity_id < '000000000000008'"
        + " AND (organization_id,entity_id) <= ('000000000000001','000000000000005')",
      text(
        "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE"
          + " ['000000000000001','000000000000003'] - ['000000000000001','000000000000005']",
        "    INDEX ATABLE", "    REGIONS PLANNED <N>"),
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
        "    INDEX ATABLE", "    REGIONS PLANNED <N>",
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
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER PTSDB [null,not null]", "    INDEX PTSDB",
        "    REGIONS PLANNED <N>", "    SERVER PROJECTION FILTER BY FIRST KEY ONLY",
        "    SERVER FILTER BY \"DATE\" >= DATE '2013-01-01 00:00:00.000'"),
      scanAttrs("RANGE SCAN ", "PTSDB", " [null,not null]")
        .put("serverFirstKeyOnlyProjection", true)
        .put("serverWhereFilter", "SERVER FILTER BY \"DATE\" >= DATE '2013-01-01 00:00:00.000'"));
  }

  @Test
  public void testRangeScanNotNull() throws Exception {
    verifyQuery("rangeScanNotNull",
      "SELECT host FROM PTSDB WHERE inst IS NOT NULL AND host IS NULL"
        + " AND \"DATE\" >= to_date('2013-01-01')",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER PTSDB [not null]", "    INDEX PTSDB",
        "    REGIONS PLANNED <N>", "    SERVER PROJECTION FILTER BY FIRST KEY ONLY",
        "    SERVER FILTER BY (HOST IS NULL" + " AND \"DATE\" >= DATE '2013-01-01 00:00:00.000')"),
      scanAttrs("RANGE SCAN ", "PTSDB", " [not null]").put("serverFirstKeyOnlyProjection", true)
        .put("serverWhereFilter",
          "SERVER FILTER BY (HOST IS NULL" + " AND \"DATE\" >= DATE '2013-01-01 00:00:00.000')"));
  }

  @Test
  public void testSkipScanLikeRanges() throws Exception {
    verifyQuery("skipScanLikeRanges",
      "SELECT inst,host FROM PTSDB WHERE inst LIKE 'na%' AND host IN ('a','b')"
        + " AND \"DATE\" >= to_date('2013-01-01') AND \"DATE\" < to_date('2013-01-02')",
      text(
        "CLIENT PARALLEL <N>-WAY SKIP SCAN ON 2 RANGES OVER PTSDB"
          + " ['na','a','2013-01-01'] - ['nb','b','2013-01-02']",
        "    INDEX PTSDB", "    REGIONS PLANNED <N>",
        "    SERVER PROJECTION FILTER BY FIRST KEY ONLY"),
      scanAttrs("SKIP SCAN ON 2 RANGES ", "PTSDB",
        " ['na','a','2013-01-01'] - ['nb','b','2013-01-02']").put("serverFirstKeyOnlyProjection",
          true));
  }

  @Test
  public void testSkipScanRegexpRanges() throws Exception {
    verifyQuery("skipScanRegexpRanges",
      "SELECT inst,host FROM PTSDB WHERE REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1', 'na2','na3')",
      text("CLIENT PARALLEL <N>-WAY SKIP SCAN ON 3 RANGES OVER PTSDB ['na1'] - ['na4']",
        "    INDEX PTSDB", "    REGIONS PLANNED <N>",
        "    SERVER PROJECTION FILTER BY FIRST KEY ONLY",
        "    SERVER FILTER BY REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')"),
      scanAttrs("SKIP SCAN ON 3 RANGES ", "PTSDB", " ['na1'] - ['na4']")
        .put("serverFirstKeyOnlyProjection", true).put("serverWhereFilter",
          "SERVER FILTER BY REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')"));
  }

  @Test
  public void testRangeScanSubstrBounds() throws Exception {
    verifyQuery("rangeScanSubstrBounds",
      "SELECT a_string FROM atable WHERE organization_id='000000000000001'"
        + " AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
      text(
        "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE"
          + " ['000000000000001','003            '] - ['000000000000001','004            ']",
        "    INDEX ATABLE", "    REGIONS PLANNED <N>"),
      scanAttrs("RANGE SCAN ", "ATABLE",
        " ['000000000000001','003            '] - ['000000000000001','004            ']"));
  }

  @Test
  public void testSkipScanTwoKeys() throws Exception {
    verifyQuery("skipScanTwoKeys",
      "SELECT a_string,b_string FROM atable"
        + " WHERE organization_id IN ('000000000000001', '000000000000005')",
      text(
        "CLIENT PARALLEL <N>-WAY SKIP SCAN ON 2 KEYS OVER ATABLE"
          + " ['000000000000001'] - ['000000000000005']",
        "    INDEX ATABLE", "    REGIONS PLANNED <N>"),
      scanAttrs("SKIP SCAN ON 2 KEYS ", "ATABLE", " ['000000000000001'] - ['000000000000005']"));
  }

  @Test
  public void testGroupByClientLimit() throws Exception {
    verifyQuery("groupByClientLimit", "SELECT count(1) FROM atable GROUP BY a_string LIMIT 5",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]",
        "CLIENT MERGE SORT", "CLIENT 5 ROW LIMIT"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]")
        .put("clientRowLimit", 5).put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT", "CLIENT 5 ROW LIMIT")));
  }

  @Test
  public void testTopNAscNullsFirstLimit() throws Exception {
    verifyQuery("topNAscNullsFirstLimit",
      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001'"
        + " ORDER BY a_string ASC NULLS FIRST LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['000000000000001']", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER TOP 10 ROWS SORTED BY [A_STRING]",
        "CLIENT MERGE SORT", "CLIENT LIMIT 10"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['000000000000001']").put("serverSortedBy", "[A_STRING]")
        .put("serverRowLimit", 10).put("clientRowLimit", 10)
        .put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT", "CLIENT LIMIT 10")));
  }

  @Test
  public void testTopNDescNullsLastLimit() throws Exception {
    verifyQuery("topNDescNullsLastLimit",
      "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001'"
        + " ORDER BY a_string DESC NULLS LAST LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['000000000000001']", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER TOP 10 ROWS SORTED BY [A_STRING DESC NULLS LAST]",
        "CLIENT MERGE SORT", "CLIENT LIMIT 10"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['000000000000001']")
        .put("serverSortedBy", "[A_STRING DESC NULLS LAST]").put("serverRowLimit", 10)
        .put("clientRowLimit", 10).put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT", "CLIENT LIMIT 10")));
  }

  @Test
  public void testAggregateOrderByClientLimit() throws Exception {
    verifyQuery("aggregateOrderByClientLimit",
      "SELECT max(a_integer) FROM atable WHERE organization_id = '000000000000001'"
        + " GROUP BY organization_id,entity_id,ROUND(a_date,'HOUR')"
        + " ORDER BY entity_id NULLS LAST LIMIT 10",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['000000000000001']", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY"
          + " [ORGANIZATION_ID, ENTITY_ID, ROUND(A_DATE)]",
        "CLIENT MERGE SORT", "CLIENT 10 ROW LIMIT"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['000000000000001']")
        .put("serverAggregate",
          "SERVER AGGREGATE INTO DISTINCT ROWS BY [ORGANIZATION_ID, ENTITY_ID, ROUND(A_DATE)]")
        .put("clientRowLimit", 10).put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT", "CLIENT 10 ROW LIMIT")));
  }

  @Test
  public void testClientSortedByHaving() throws Exception {
    verifyQuery("clientSortedByHaving",
      "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY a_string,b_string"
        + " HAVING max(a_string) = 'a' ORDER BY b_string",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER FILTER BY A_INTEGER = 1",
        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]", "CLIENT MERGE SORT",
        "CLIENT FILTER BY MAX(A_STRING) = 'a'", "CLIENT SORTED BY [B_STRING]"),
      scanAttrs("FULL SCAN ", "ATABLE", "")
        .put("serverWhereFilter", "SERVER FILTER BY A_INTEGER = 1")
        .put("serverAggregate", "SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]")
        .put("clientFilterBy", "MAX(A_STRING) = 'a'").put("clientSortedBy", "[B_STRING]")
        .put("clientOffset", 0).put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT", "CLIENT FILTER BY MAX(A_STRING) = 'a'",
          "CLIENT SORTED BY [B_STRING]")));
  }

  @Test
  public void testSortMergeJoin() throws Exception {
    // In a SMJ the root is a synthetic node carrying the join header and the two operand plans as
    // lhs and rhs. Join operand leaves are compiled through the join path, not optimizer index
    // selection, so they carry no decision rule.
    ObjectNode lhs =
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']").putNull("indexRule");
    ObjectNode rhs = scanAttrs("FULL SCAN ", "ATABLE", "").putNull("indexRule");
    ObjectNode expected = defaultAttrs();
    expected.put("abstractExplainPlan", "SORT-MERGE-JOIN (INNER)");
    expected.set("lhsJoinQueryExplainPlan", lhs);
    expected.set("rhsJoinQueryExplainPlan", rhs);
    verifyQuery("sortMergeJoin",
      "SELECT /*+ USE_SORT_MERGE_JOIN */ a.a_string, b.a_string FROM atable a"
        + " JOIN atable b ON a.organization_id = b.organization_id"
        + " WHERE a.organization_id = '00D000000000001'",
      text("SORT-MERGE-JOIN (INNER) TABLES  /* SORT_MERGE */",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "        INDEX ATABLE", "        REGIONS PLANNED <N>", "AND",
        "    CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "        INDEX ATABLE",
        "        REGIONS PLANNED <N>"),
      expected);
  }

  @Test
  public void testHashJoinInner() throws Exception {
    // HashJoinPlan root attributes come from the delegate scan. Each hash/skip-scan child
    // is recorded under subPlans with its join header on abstractExplainPlan.
    // Hash joins are compiled through the join path, not optimizer index selection, so they carry
    // no decision rule.
    ObjectNode child = scanAttrs("FULL SCAN ", "ATABLE", "")
      .put("abstractExplainPlan", "PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .putNull("indexRule");
    ObjectNode expected =
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']").putNull("indexRule");
    expected.set("subPlans", mapper.createArrayNode().add(child));
    expected.put("dynamicServerFilter",
      "DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)");
    verifyQuery("hashJoinInner",
      "SELECT a.a_string, b.a_string FROM atable a"
        + " JOIN atable b ON a.organization_id = b.organization_id"
        + " WHERE a.organization_id = '00D000000000001'",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */",
        "        CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "            INDEX ATABLE",
        "            REGIONS PLANNED <N>",
        "    DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)"),
      expected);
  }

  @Test
  public void testHashJoinSemiInSubquery() throws Exception {
    // The behavior under test is the IN-subquery -> semi-join rewrite breadcrumb, asserted via
    // attributes below. The optimizer's behavior is nondeterministic.
    // WhereOptimizer.getKeyExpressionCombination probes the row key using
    // PDataType.getSampleValue, which for CHAR/VARCHAR row keys returns a value from an
    // unseeded ThreadLocal Random. The choice flips between SKIP-SCAN-JOIN and PARALLEL
    // SEMI-JOIN ... SKIP MERGE from run to run. Every other rewrite breadcrumb is stable and
    // asserted exactly. Phoenix temp aliases are renamed by first appearance so the dynamic
    // server filter asserts on the canonical "$1.$2" form.
    String query = "SELECT a_string FROM atable"
      + " WHERE organization_id IN (SELECT organization_id FROM atable WHERE a_integer = 1)";
    String skipScanJoin = "    SKIP-SCAN-JOIN TABLE 0  /* HASH BUILD RIGHT */";
    String parallelSemiJoin = "    PARALLEL SEMI-JOIN TABLE 0  /* HASH BUILD RIGHT, SKIP MERGE */";
    // Index 3 is the unstable join-operator line. null marks it as "tolerated".
    List<String> expectedStable = text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
      "    INDEX ATABLE", "    REGIONS PLANNED <N>", null,
      "        CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "            INDEX ATABLE",
      "            REGIONS PLANNED <N>", "            SERVER FILTER BY A_INTEGER = 1",
      "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [ORGANIZATION_ID]",
      "    DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($1.$2)");
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlan plan = ExplainPlanTestUtil.getExplainPlan(conn, query);
      List<String> actual = oracle.normalizeText(plan.getPlanSteps());
      assertEquals("plan line count, was " + actual, expectedStable.size(), actual.size());
      for (int i = 0; i < expectedStable.size(); i++) {
        if (expectedStable.get(i) == null) {
          assertTrue("unexpected join-strategy line: " + actual.get(i),
            actual.get(i).equals(skipScanJoin) || actual.get(i).equals(parallelSemiJoin));
        } else {
          assertEquals("@" + i, expectedStable.get(i), actual.get(i));
        }
      }
      // The dynamic server filter line is normalized above.
      ExplainPlanTestUtil.assertPlan(plan.getPlanStepsAsAttributes())
        .rewriteContains("IN SUBQUERY AS SEMI JOIN");
    }
  }

  @Test
  public void testUnionAll() throws Exception {
    // The union composes each branch recursively from its own explain plan into subPlans.
    ObjectNode lhs = scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']");
    ObjectNode rhs = scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000002']");
    ObjectNode expected = defaultAttrs();
    expected.put("abstractExplainPlan", "UNION ALL OVER 2 QUERIES");
    expected.set("subPlans", mapper.createArrayNode().add(lhs).add(rhs));
    verifyQuery("unionAll",
      "SELECT a_string FROM atable WHERE organization_id = '00D000000000001'" + " UNION ALL"
        + " SELECT a_string FROM atable WHERE organization_id = '00D000000000002'",
      text("UNION ALL OVER 2 QUERIES",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "        INDEX ATABLE", "        REGIONS PLANNED <N>",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000002']",
        "        INDEX ATABLE", "        REGIONS PLANNED <N>"),
      expected);
  }

  @Test
  public void testUnionAllOfHashJoins() throws Exception {
    // A UNION ALL whose branches are each hash joins. Exercises recursive explain composition end
    // to end and confirms each branch carries its own subPlans and dynamicServerFilter.
    // Every node in a union of hash joins is compiled through the join path, so none carry a
    // decision rule.
    ObjectNode joinChild1 = scanAttrs("FULL SCAN ", "ATABLE", "")
      .put("abstractExplainPlan", "PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .putNull("indexRule");
    ObjectNode branch1 =
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']").putNull("indexRule");
    branch1.set("subPlans", mapper.createArrayNode().add(joinChild1));
    branch1.put("dynamicServerFilter",
      "DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)");
    ObjectNode joinChild2 = scanAttrs("FULL SCAN ", "ATABLE", "")
      .put("abstractExplainPlan", "PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .putNull("indexRule");
    ObjectNode branch2 =
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000002']").putNull("indexRule");
    branch2.set("subPlans", mapper.createArrayNode().add(joinChild2));
    branch2.put("dynamicServerFilter",
      "DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)");
    ObjectNode expected = defaultAttrs();
    expected.put("abstractExplainPlan", "UNION ALL OVER 2 QUERIES");
    expected.set("subPlans", mapper.createArrayNode().add(branch1).add(branch2));
    verifyQuery("unionAllOfHashJoins",
      "SELECT a.a_string AS s1, b.a_string AS s2 FROM atable a"
        + " JOIN atable b ON a.organization_id = b.organization_id"
        + " WHERE a.organization_id = '00D000000000001'" + " UNION ALL"
        + " SELECT a.a_string AS s1, b.a_string AS s2 FROM atable a"
        + " JOIN atable b ON a.organization_id = b.organization_id"
        + " WHERE a.organization_id = '00D000000000002'",
      text("UNION ALL OVER 2 QUERIES",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "        INDEX ATABLE", "        REGIONS PLANNED <N>",
        "        PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */",
        "            CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "                INDEX ATABLE",
        "                REGIONS PLANNED <N>",
        "        DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)",
        "    CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000002']",
        "        INDEX ATABLE", "        REGIONS PLANNED <N>",
        "        PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */",
        "            CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "                INDEX ATABLE",
        "                REGIONS PLANNED <N>",
        "        DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)"),
      expected);
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
      text("UPSERT SELECT", "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "    INDEX ATABLE", "    REGIONS PLANNED <N>"),
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
      text("UPSERT ROWS", "CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "    INDEX ATABLE", "    REGIONS PLANNED <N>"),
      scanAttrs("RANGE SCAN ", "ATABLE", " ['00D000000000001']")
        .put("abstractExplainPlan", "UPSERT ROWS").putNull("indexRule"));
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
        "    INDEX ATABLE", "    REGIONS PLANNED <N>",
        "    SERVER PROJECTION FILTER BY FIRST KEY ONLY", "    SERVER FILTER BY ENTITY_ID = 'abc'"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("abstractExplainPlan", "DELETE ROWS SERVER SELECT")
        .put("serverFirstKeyOnlyProjection", true)
        .put("serverWhereFilter", "SERVER FILTER BY ENTITY_ID = 'abc'").putNull("indexRule"));
  }

  @Test
  public void testDeleteClient() throws Exception {
    verifyMutation("deleteClient", "DELETE FROM atable WHERE entity_id = 'abc'", false,
      text("DELETE ROWS CLIENT SELECT", "CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    INDEX ATABLE", "    REGIONS PLANNED <N>",
        "    SERVER PROJECTION FILTER BY FIRST KEY ONLY", "    SERVER FILTER BY ENTITY_ID = 'abc'"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("abstractExplainPlan", "DELETE ROWS CLIENT SELECT")
        .put("serverFirstKeyOnlyProjection", true)
        .put("serverWhereFilter", "SERVER FILTER BY ENTITY_ID = 'abc'"));
  }

  @Test
  public void testSequenceNextValue() throws Exception {
    verifyQuery("sequenceNextValue", "SELECT NEXT VALUE FOR " + SEQ + " FROM atable",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    INDEX ATABLE",
        "    REGIONS PLANNED <N>", "    SERVER PROJECTION FILTER BY FIRST KEY ONLY",
        "CLIENT RESERVE VALUES FROM 1 SEQUENCE"),
      scanAttrs("FULL SCAN ", "ATABLE", "").put("serverFirstKeyOnlyProjection", true)
        .put("clientSequenceCount", 1)
        .set("clientSteps", clientSteps("CLIENT RESERVE VALUES FROM 1 SEQUENCE")));
  }

  @Test
  public void testSaltedTableScan() throws Exception {
    verifyQuery("saltedTableScan", "SELECT * FROM " + SALTED + " WHERE v = 7",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER EO_SALTED", "    INDEX EO_SALTED",
        "    SALT BUCKETS 4", "    REGIONS PLANNED <N>", "    SERVER FILTER BY V = 7",
        "CLIENT MERGE SORT"),
      scanAttrs("FULL SCAN ", "EO_SALTED", "").put("saltBuckets", 4)
        .put("serverWhereFilter", "SERVER FILTER BY V = 7")
        .put("clientSortAlgo", "CLIENT MERGE SORT")
        .set("clientSteps", clientSteps("CLIENT MERGE SORT")));
  }

  @Test
  public void testMultiTenantView() throws Exception {
    Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    tenantProps.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    tenantProps.setProperty(TENANT_ID_ATTRIB, TENANT_ID);
    verifyQuery("multiTenantView", "SELECT * FROM " + MT_VIEW + " LIMIT 1", tenantProps,
      text("CLIENT SERIAL <N>-WAY RANGE SCAN OVER EO_MT_BASE ['tenant42']", "    INDEX EO_MT_VIEW",
        "    REGIONS PLANNED <N>", "    SERVER 1 ROW LIMIT", "CLIENT 1 ROW LIMIT"),
      attrs().put("tenantId", TENANT_ID).put("viewName", MT_VIEW).put("viewBaseName", MT_BASE)
        .put("iteratorTypeAndScanSize", "SERIAL <N>-WAY").put("consistency", "STRONG")
        .put("explainScanType", "RANGE SCAN ").put("tableName", "EO_MT_BASE")
        .put("indexName", "EO_MT_VIEW").put("indexRule", "data table")
        .put("keyRanges", " ['tenant42']").put("serverRowLimit", 1).put("clientRowLimit", 1)
        .set("clientSteps", clientSteps("CLIENT 1 ROW LIMIT")));
  }

  @Test
  public void testIndexKindGlobal() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps());
      java.sql.Statement stmt = conn.createStatement()) {
      String base = generateUniqueName();
      String idx = generateUniqueName();
      stmt.execute("CREATE TABLE " + base + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
      stmt.execute("CREATE INDEX " + idx + " ON " + base + " (v1) INCLUDE (v2)");
      String query = "SELECT v1, v2 FROM " + base + " WHERE v1 = 'x'";
      ExplainPlanTestUtil.assertPlan(conn, query).table(idx).indexName(idx).indexKind("GLOBAL")
        .indexRule(OptimizerReasons.RULE_MORE_BOUND_PK_COLUMNS).indexRejectedNone();
      assertPlanContainsLine(conn, query,
        "    INDEX " + idx + " GLOBAL  /* more bound PK columns */");
    }
  }

  @Test
  public void testIndexKindLocal() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps());
      java.sql.Statement stmt = conn.createStatement()) {
      String base = generateUniqueName();
      String idx = generateUniqueName();
      stmt.execute("CREATE TABLE " + base + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
      stmt.execute("CREATE LOCAL INDEX " + idx + " ON " + base + " (v1)");
      // The OVER line decorates a local index as <idx>(<phys>); the INDEX line prints just <idx>.
      String query =
        "SELECT /*+ INDEX(" + base + " " + idx + ") */ k, v1 FROM " + base + " WHERE v1 = 'x'";
      ExplainPlanTestUtil.assertPlan(conn, query).indexName(idx).indexKind("LOCAL")
        .indexRule(OptimizerReasons.RULE_HINT).indexRejectedNone();
      assertPlanContainsLine(conn, query, "    INDEX " + idx + " LOCAL  /* hint */");
    }
  }

  @Test
  public void testIndexKindUncoveredGlobal() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps());
      java.sql.Statement stmt = conn.createStatement()) {
      String base = generateUniqueName();
      String idx = generateUniqueName();
      stmt.execute("CREATE TABLE " + base + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
      stmt.execute("CREATE UNCOVERED INDEX " + idx + " ON " + base + " (v1)");
      String query =
        "SELECT /*+ INDEX(" + base + " " + idx + ") */ k, v2 FROM " + base + " WHERE v1 = 'x'";
      ExplainPlanTestUtil.assertPlan(conn, query).indexName(idx).indexKind("UNCOVERED GLOBAL")
        .indexRule(OptimizerReasons.RULE_HINT).indexRejectedNone();
      assertPlanContainsLine(conn, query, "    INDEX " + idx + " UNCOVERED GLOBAL  /* hint */");
    }
  }

  /**
   * A full scan over a table with no indexes records the {@code "data table"} default rule, which
   * is suppressed. The INDEX line must be bare and carry no {@code !INDEX} rejection notes.
   */
  @Test
  public void testNoCandidateDataTableBareIndexLine() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps());
      java.sql.Statement stmt = conn.createStatement()) {
      String base = generateUniqueName();
      stmt.execute("CREATE TABLE " + base + " (k VARCHAR PRIMARY KEY, v1 VARCHAR)");
      String query = "SELECT v1 FROM " + base;
      ExplainPlanTestUtil.assertPlan(conn, query).indexName(base).indexKind(null)
        .indexRule(OptimizerReasons.RULE_DATA_TABLE).indexRejectedNone();
      assertPlanContainsLine(conn, query, "    INDEX " + base);
      assertNoPlanLineContains(conn, query, "/*");
      assertNoPlanLineContains(conn, query, "!INDEX");
    }
  }

  /**
   * A query whose {@code ORDER BY} can be served by a covered global index picks that index by the
   * {@code "non-local preferred"} rule, which is non default and renders a rule comment on the
   * INDEX line.
   */
  @Test
  public void testIndexRuleNonLocalPreferred() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps());
      java.sql.Statement stmt = conn.createStatement()) {
      String base = generateUniqueName();
      String idx = generateUniqueName();
      stmt.execute("CREATE TABLE " + base + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
      stmt.execute("CREATE INDEX " + idx + " ON " + base + " (v1) INCLUDE (v2)");
      String query = "SELECT v1, v2 FROM " + base + " ORDER BY v1";
      ExplainPlanTestUtil.assertPlan(conn, query).indexName(idx).indexKind("GLOBAL")
        .indexRule(OptimizerReasons.RULE_NON_LOCAL_PREFERRED);
      assertPlanContainsLine(conn, query,
        "    INDEX " + idx + " GLOBAL  /* non-local preferred */");
    }
  }

  /**
   * A {@code GROUP BY} on a column with a LOCAL index chooses the data table by the
   * {@code "more bound PK columns"} rule, which is non default and renders a rule comment.
   */
  @Test
  public void testIndexRejectedRendered() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps());
      java.sql.Statement stmt = conn.createStatement()) {
      String base = generateUniqueName();
      String idx = base + "_IDX";
      stmt
        .execute("CREATE TABLE " + base + " (rowkey VARCHAR PRIMARY KEY, c1 VARCHAR, c2 VARCHAR)");
      stmt.execute("CREATE LOCAL INDEX " + idx + " ON " + base + " (c1)");
      String query =
        "SELECT c1, max(rowkey), max(c2) FROM " + base + " WHERE rowkey <= 'z' GROUP BY c1";
      ExplainPlanTestUtil.assertPlan(conn, query).indexName(base)
        .indexRule(OptimizerReasons.RULE_MORE_BOUND_PK_COLUMNS).indexRejectedCount(1)
        .indexRejected(0, idx, OptimizerReasons.REASON_NO_PK_PREFIX_BOUND);
      assertPlanContainsLine(conn, query, "    INDEX " + base + "  /* more bound PK columns */");
      assertPlanContainsLine(conn, query, "    /* !INDEX " + idx + " -- no PK prefix bound */");
    }
  }

  @Test
  public void testRewriteReverseScanSubstitution() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil.assertPlan(conn, "SELECT inst,\"DATE\" FROM ptsdb2 WHERE inst = 'na1'"
        + " ORDER BY inst DESC, \"DATE\" DESC").rewriteContains("REVERSE SCAN SUBSTITUTION");
    }
  }

  @Test
  public void testRewriteInSubqueryAsSemiJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil
        .assertPlan(conn,
          "SELECT a_string FROM atable"
            + " WHERE organization_id IN (SELECT organization_id FROM atable WHERE a_integer = 1)")
        .rewriteContains("IN SUBQUERY AS SEMI JOIN");
    }
  }

  @Test
  public void testRewriteNotInSubqueryAsAntiJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil.assertPlan(conn, "SELECT a_string FROM atable"
        + " WHERE organization_id NOT IN (SELECT organization_id FROM atable WHERE a_integer = 1)")
        .rewriteContains("NOT IN SUBQUERY AS ANTI JOIN");
    }
  }

  @Test
  public void testRewriteExistsSubqueryAsSemiJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil.assertPlan(conn,
        "SELECT a_string FROM atable a WHERE EXISTS"
          + " (SELECT 1 FROM atable b WHERE b.organization_id = a.organization_id"
          + " AND b.a_integer = 1)")
        .rewriteContains("EXISTS SUBQUERY AS SEMI JOIN");
    }
  }

  @Test
  public void testRewriteNotExistsSubqueryAsAntiJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil.assertPlan(conn,
        "SELECT a_string FROM atable a WHERE NOT EXISTS"
          + " (SELECT 1 FROM atable b WHERE b.organization_id = a.organization_id"
          + " AND b.a_integer = 1)")
        .rewriteContains("NOT EXISTS SUBQUERY AS ANTI JOIN");
    }
  }

  @Test
  public void testRewriteScalarSubqueryAsInnerJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil
        .assertPlan(conn,
          "SELECT a_string FROM atable a WHERE a_integer ="
            + " (SELECT max(a_integer) FROM atable b WHERE b.organization_id = a.organization_id)")
        .rewriteContains("SCALAR SUBQUERY AS INNER JOIN");
    }
  }

  @Test
  public void testRewriteHavingPredicateAsWhere() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil
        .assertPlan(conn, "SELECT count(1) FROM atable GROUP BY a_string HAVING a_string = 'a'")
        .rewriteContains("HAVING PREDICATE AS WHERE");
    }
  }

  @Test
  public void testRewriteDerivedTableFlattened() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil
        .assertPlan(conn, "SELECT a_string FROM (SELECT a_string FROM atable) WHERE a_string = 'a'")
        .rewriteContains("DERIVED TABLE FLATTENED 1");
    }
  }

  @Test
  public void testRewriteUnionOrderByMerge() throws Exception {
    // The UNION ORDER BY merge optimization only fires when a union is the inner select of an
    // outer query whose GROUP BY / ORDER BY is order-preserving over the union output.
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
    try (Connection conn = DriverManager.getConnection(getUrl(), props);
      java.sql.Statement stmt = conn.createStatement()) {
      String t1 = generateUniqueName();
      String t2 = generateUniqueName();
      stmt.execute("CREATE TABLE " + t1 + " (fuid UNSIGNED_LONG NOT NULL,"
        + " fstatsdate UNSIGNED_LONG NOT NULL, faid_1 UNSIGNED_LONG NOT NULL,"
        + " clk_pv_1 UNSIGNED_LONG CONSTRAINT pk PRIMARY KEY (fuid, fstatsdate, faid_1))");
      stmt.execute("CREATE TABLE " + t2 + " (fuid UNSIGNED_LONG NOT NULL,"
        + " fstatsdate UNSIGNED_LONG NOT NULL, faid_2 UNSIGNED_LONG NOT NULL,"
        + " clk_pv_2 UNSIGNED_LONG CONSTRAINT pk PRIMARY KEY (fuid, fstatsdate, faid_2))");
      String unionSql = "(SELECT fuid AS advertiser_id, faid_1 AS adgroup_id,"
        + " fstatsdate AS date, SUM(clk_pv_1) AS clicks FROM " + t1
        + " GROUP BY fuid, faid_1, fstatsdate UNION ALL"
        + " SELECT fuid AS advertiser_id, faid_2 AS adgroup_id,"
        + " fstatsdate AS date, SUM(clk_pv_2) AS clicks FROM " + t2
        + " GROUP BY fuid, faid_2, fstatsdate)";
      String sql = "SELECT advertiser_id, adgroup_id, date, SUM(clicks) AS clicks FROM " + unionSql
        + " GROUP BY advertiser_id, adgroup_id, date ORDER BY advertiser_id, adgroup_id, date";
      ExplainPlanTestUtil.assertPlan(conn, sql).rewriteContains("UNION ORDER BY MERGE");
    }
  }

  @Test
  public void testRewriteStarJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil
        .assertPlan(conn,
          "SELECT a.a_string, b.a_string, c.a_string FROM atable a"
            + " JOIN atable b ON a.organization_id = b.organization_id"
            + " JOIN atable c ON a.organization_id = c.organization_id"
            + " WHERE a.organization_id = '00D000000000001'")
        .rewriteContains("STAR JOIN ON 2 RIGHT LEGS");
    }
  }

  @Test
  public void testRewriteRightJoinAsLeftJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil
        .assertPlan(conn,
          "SELECT a.a_string, b.a_string FROM atable a"
            + " RIGHT JOIN atable b ON a.organization_id = b.organization_id"
            + " WHERE b.organization_id = '00D000000000001'")
        .rewriteContains("RIGHT JOIN AS LEFT JOIN");
    }
  }

  @Test
  public void testDisclosureTenantAndView() throws Exception {
    Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    tenantProps.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    tenantProps.setProperty(TENANT_ID_ATTRIB, TENANT_ID);
    try (Connection conn = DriverManager.getConnection(getUrl(), tenantProps)) {
      ExplainPlanTestUtil.assertPlan(conn, "SELECT * FROM " + MT_VIEW + " LIMIT 1")
        .tenant(TENANT_ID).view(MT_VIEW, MT_BASE);
    }
  }

  @Test
  public void testDisclosureNoneOnPlainScan() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      ExplainPlanTestUtil.assertPlan(conn, "SELECT * FROM atable").tenantNone().viewNone()
        .cdcScopesNone().txnProviderNone().rewritesNone();
    }
  }

  /**
   * End-to-end check that the disclosure block is prepended to the real JDBC {@code EXPLAIN} result
   * set (the {@link org.apache.phoenix.jdbc.PhoenixStatement} path that invokes
   * {@link ExplainTable#renderTopOfPlanText}), with {@code TENANT} then {@code VIEW} ahead of the
   * first operator line.
   */
  @Test
  public void testTopOfPlanTextTenantAndView() throws Exception {
    Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    tenantProps.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    tenantProps.setProperty(TENANT_ID_ATTRIB, TENANT_ID);
    try (Connection conn = DriverManager.getConnection(getUrl(), tenantProps)) {
      List<String> rows = explainViaJdbc(conn, "SELECT * FROM " + MT_VIEW + " LIMIT 1");
      assertEquals("TENANT '" + TENANT_ID + "'", rows.get(0));
      assertEquals("VIEW " + MT_VIEW + " OVER " + MT_BASE, rows.get(1));
      assertTrue("operator should follow the disclosure block: " + rows.get(2),
        rows.get(2).startsWith("CLIENT"));
    }
  }

  /**
   * End-to-end check that a {@code REWRITE} breadcrumb is prepended ahead of the first operator.
   */
  @Test
  public void testTopOfPlanTextRewriteAtTop() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      List<String> rows =
        explainViaJdbc(conn, "SELECT a.a_string, b.a_string FROM atable a RIGHT JOIN atable b"
          + " ON a.organization_id = b.organization_id WHERE b.organization_id = '00D000000000001'");
      assertEquals("REWRITE RIGHT JOIN AS LEFT JOIN", rows.get(0));
      assertTrue("operator should follow the REWRITE line: " + rows.get(1),
        rows.get(1).startsWith("CLIENT"));
    }
  }

  /** A view with no resolvable base table renders {@code VIEW <name>} without {@code OVER}. */
  @Test
  public void testRenderTopOfPlanTextViewWithoutBase() {
    ExplainPlanAttributes attrs = new ExplainPlanAttributesBuilder().setViewName("V").build();
    List<String> steps = new java.util.ArrayList<>(Arrays.asList("CLIENT FOO"));
    ExplainTable.renderTopOfPlanText(steps, attrs);
    assertEquals(Arrays.asList("VIEW V", "CLIENT FOO"), steps);
  }

  /** {@link ExplainTable#renderTopOfPlanText} leaves the steps untouched. */
  @Test
  public void testRenderTopOfPlanTextNoopWhenNoDisclosure() {
    ExplainPlanAttributes attrs = new ExplainPlanAttributesBuilder().build();
    List<String> steps = new java.util.ArrayList<>(
      Arrays.asList("CLIENT PARALLEL 1-WAY FULL SCAN OVER T", "    SERVER FILTER BY X"));
    ExplainTable.renderTopOfPlanText(steps, attrs);
    assertEquals(Arrays.asList("CLIENT PARALLEL 1-WAY FULL SCAN OVER T", "    SERVER FILTER BY X"),
      steps);
  }

  private static List<String> explainViaJdbc(Connection conn, String query) throws SQLException {
    List<String> rows = new java.util.ArrayList<>();
    try (java.sql.Statement stmt = conn.createStatement();
      java.sql.ResultSet rs = stmt.executeQuery("EXPLAIN " + query)) {
      while (rs.next()) {
        rows.add(rs.getString(1));
      }
    }
    return rows;
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
  public void testTextNormalizerRenumbersTempAliasesByFirstAppearance() {
    // Two distinct aliases ($7, $9) with $7 appearing first → $1, $9 → $2.
    assertEquals(
      Collections.singletonList("    DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($1.$2)"),
      new ExplainTextNormalizer().normalize(Collections
        .singletonList("    DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($7.$9)")));
  }

  @Test
  public void testTextNormalizerSharesAliasStateAcrossLines() {
    // The same alias ($5) appearing in two different lines gets the same renumbered token. A
    // distinct alias ($8) on the second line gets the next number.
    List<String> in = Arrays.asList("CLIENT PARALLEL 1-WAY FULL SCAN OVER ($5)",
      "    DYNAMIC SERVER FILTER BY T.X IN ($5.$8)");
    assertEquals(Arrays.asList("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ($1)",
      "    DYNAMIC SERVER FILTER BY T.X IN ($1.$2)"), new ExplainTextNormalizer().normalize(in));
  }

  @Test
  public void testTextNormalizerLeavesNonAliasDollarTokensAlone() {
    // The "$<N>" form below is the canonical renumbered form, not a temp alias, so the token
    // pattern only matches "$<digits>". A literal dollar followed by a non-digit is preserved.
    List<String> in = Collections.singletonList("CLIENT FILTER BY price > $5.50 AND tag = '$abc'");
    assertEquals(Collections.singletonList("CLIENT FILTER BY price > $1.50 AND tag = '$abc'"),
      new ExplainTextNormalizer().normalize(in));
  }

  @Test
  public void testJsonNormalizerRenumbersTempAliasesAcrossFields() {
    ObjectNode root = mapper.createObjectNode();
    // First-appearance order is field-insertion order: $7 → $1, $9 → $2, $7 reused → $1.
    root.put("serverWhereFilter", "SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($7.$9)");
    root.put("clientFilterBy", "X = $7");
    new ExplainJsonNormalizer().normalize(root);
    assertEquals("SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($1.$2)",
      root.get("serverWhereFilter").asText());
    assertEquals("X = $1", root.get("clientFilterBy").asText());
  }

  @Test
  public void testJsonNormalizerSharesAliasStateWithRhsRecursion() {
    ObjectNode root = mapper.createObjectNode();
    root.put("serverWhereFilter", "X IN ($3)");
    ObjectNode rhs = mapper.createObjectNode();
    rhs.put("serverWhereFilter", "Y = $3 AND Z = $5");
    root.set("rhsJoinQueryExplainPlan", rhs);
    new ExplainJsonNormalizer().normalize(root);
    assertEquals("X IN ($1)", root.get("serverWhereFilter").asText());
    assertEquals("Y = $1 AND Z = $2",
      root.get("rhsJoinQueryExplainPlan").get("serverWhereFilter").asText());
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
    root.put("scanEstimatedRows", 1234L);
    root.put("scanEstimatedSizeInBytes", 9876L);
    root.put("estimatedRows", 4321L);
    root.put("estimatedSizeInBytes", 6789L);
    root.put("estimateInfoTs", 1748880000000L);
    root.put("numRegionLocationLookups", 7);
    root.set("regionLocations", mapper.createArrayNode().add("anything"));
    new ExplainJsonNormalizer().normalize(root);
    assertEquals("PARALLEL <N>-WAY", root.get("iteratorTypeAndScanSize").asText());
    assertTrue(root.get("splitsChunk").isNull());
    assertTrue(root.get("scanEstimatedRows").isNull());
    assertTrue(root.get("scanEstimatedSizeInBytes").isNull());
    assertTrue(root.get("estimatedRows").isNull());
    assertTrue(root.get("estimatedSizeInBytes").isNull());
    assertTrue(root.get("estimateInfoTs").isNull());
    assertTrue(root.get("regionLocations").isNull());
    assertEquals(0, root.get("numRegionLocationLookups").asInt());
  }

  @Test
  public void testJsonNormalizerRecursesIntoLhsJoinQueryExplainPlan() {
    ObjectNode root = mapper.createObjectNode();
    root.put("iteratorTypeAndScanSize", "PARALLEL 5-WAY");
    root.put("numRegionLocationLookups", 1);
    ObjectNode lhs = mapper.createObjectNode();
    lhs.put("iteratorTypeAndScanSize", "PARALLEL 7-WAY");
    lhs.put("numRegionLocationLookups", 42);
    lhs.set("regionLocations", mapper.createArrayNode().add(1));
    root.set("lhsJoinQueryExplainPlan", lhs);
    new ExplainJsonNormalizer().normalize(root);
    assertEquals("PARALLEL <N>-WAY", root.get("iteratorTypeAndScanSize").asText());
    assertEquals(0, root.get("numRegionLocationLookups").asInt());
    JsonNode nestedLhs = root.get("lhsJoinQueryExplainPlan");
    assertEquals("PARALLEL <N>-WAY", nestedLhs.get("iteratorTypeAndScanSize").asText());
    assertEquals(0, nestedLhs.get("numRegionLocationLookups").asInt());
    assertTrue(nestedLhs.get("regionLocations").isNull());
  }

  @Test
  public void testJsonNormalizerSharesAliasStateWithLhsRecursion() {
    ObjectNode root = mapper.createObjectNode();
    root.put("serverWhereFilter", "X IN ($3)");
    ObjectNode lhs = mapper.createObjectNode();
    lhs.put("serverWhereFilter", "Y = $3 AND Z = $5");
    root.set("lhsJoinQueryExplainPlan", lhs);
    new ExplainJsonNormalizer().normalize(root);
    assertEquals("X IN ($1)", root.get("serverWhereFilter").asText());
    assertEquals("Y = $1 AND Z = $2",
      root.get("lhsJoinQueryExplainPlan").get("serverWhereFilter").asText());
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
      assertTrue(msg.contains("SERVER PROJECTION FILTER BY FIRST KEY ONLY"));
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

  /** Assert that the optimized plan-steps text for {@code query} contains {@code expectedLine}. */
  private static void assertPlanContainsLine(Connection conn, String query, String expectedLine)
    throws SQLException {
    List<String> steps = ExplainPlanTestUtil.getPlanSteps(conn, query);
    assertTrue("expected plan to contain line '" + expectedLine + "' but was " + steps,
      steps.contains(expectedLine));
  }

  /** Assert that no plan-steps text line for {@code query} contains {@code needle}. */
  private static void assertNoPlanLineContains(Connection conn, String query, String needle)
    throws SQLException {
    List<String> steps = ExplainPlanTestUtil.getPlanSteps(conn, query);
    for (String s : steps) {
      assertFalse(
        "expected no plan line containing '" + needle + "' but found '" + s + "' in " + steps,
        s.contains(needle));
    }
  }

  private static Properties defaultProps() {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty(DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
    return props;
  }

  private static List<String> text(String... lines) {
    return Arrays.asList(lines);
  }

  /**
   * Returns a fresh {@link ObjectNode} populated with the JSON shape that
   * {@link ExplainPlanAttributes#getDefaultExplainPlan()} serializes.
   */
  private static ObjectNode defaultAttrs() {
    ObjectNode n = mapper.createObjectNode();
    n.putNull("tenantId");
    n.putNull("viewName");
    n.putNull("viewBaseName");
    n.putNull("cdcScopes");
    n.putNull("txnProvider");
    n.putNull("rewrites");
    n.putNull("estimatedRows");
    n.putNull("estimatedSizeInBytes");
    n.putNull("estimateInfoTs");
    n.putNull("abstractExplainPlan");
    n.putNull("splitsChunk");
    n.putNull("scanEstimatedRows");
    n.putNull("scanEstimatedSizeInBytes");
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
    n.putNull("indexName");
    n.putNull("indexKind");
    n.putNull("indexRule");
    n.putNull("indexRejected");
    n.putNull("saltBuckets");
    n.putNull("regionsPlanned");
    n.putNull("scanTimeRangeMin");
    n.putNull("scanTimeRangeMax");
    n.putNull("serverWhereFilter");
    n.putNull("serverDistinctFilter");
    n.putNull("serverOffset");
    n.putNull("serverRowLimit");
    n.putNull("serverParsedProjections");
    n.put("serverFirstKeyOnlyProjection", false);
    n.put("serverEmptyColumnOnlyProjection", false);
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
    n.putNull("clientSteps");
    n.putNull("lhsJoinQueryExplainPlan");
    n.putNull("rhsJoinQueryExplainPlan");
    n.putNull("serverMergeColumns");
    n.putNull("regionLocations");
    n.putNull("regionLocationsTotalSize");
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
   * Convenience wrapper that builds {@link #defaultAttrs()} for scans.
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
    // For a data table scan the per scan INDEX line names the same entity as tableName. View and
    // index scans that diverge override indexName on the returned node.
    n.put("indexName", table);
    // A data table scan that participated in optimizer index selection records its decision rule.
    // A point lookup short-circuits to the point lookup rule. Index targets set their own rule.
    n.put("indexRule", scanType.trim().startsWith("POINT LOOKUP") ? "point lookup" : "data table");
    if (keys != null) {
      n.put("keyRanges", keys);
    }
    return n;
  }

  /** A plain {@link ObjectNode} alias for clarity in tests that don't use {@link #scanAttrs}. */
  private static ObjectNode attrs() {
    return defaultAttrs();
  }

  /** Build a {@code clientSteps} JSON array for embedding into an expected attributes object. */
  private static ArrayNode clientSteps(String... steps) {
    ArrayNode arr = mapper.createArrayNode();
    for (String s : steps) {
      arr.add(s);
    }
    return arr;
  }

  /** Build a {@code rewrites} JSON array for embedding into an expected attributes object. */
  private static ArrayNode rewriteList(String... rewrites) {
    ArrayNode arr = mapper.createArrayNode();
    for (String s : rewrites) {
      arr.add(s);
    }
    return arr;
  }

  private static ExplainPlan samplePlan(String way, String scanType) {
    ExplainPlanAttributes a = new ExplainPlanAttributesBuilder().setIteratorTypeAndScanSize(way)
      .setExplainScanType(scanType).setTableName("T").setServerFirstKeyOnlyProjection(true).build();
    return new ExplainPlan(Arrays.asList("CLIENT " + way + " " + scanType.trim() + " OVER T",
      "    SERVER PROJECTION FILTER BY FIRST KEY ONLY"), a);
  }

  private static PColumn column(String family, String name) {
    PName fName = PNameFactory.newName(family);
    PName cName = PNameFactory.newName(name);
    return new PColumnImpl(cName, fName, PInteger.INSTANCE, null, null, false, 0, SortOrder.ASC, 0,
      null, false, "expression", false, false, name.getBytes(), 0L);
  }
}
