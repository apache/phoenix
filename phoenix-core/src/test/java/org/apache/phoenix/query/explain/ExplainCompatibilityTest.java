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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
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
public class ExplainCompatibilityTest extends BaseConnectionlessQueryTest {

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
    verifyQuery("hashJoinInner",
      "SELECT a.a_string, b.a_string FROM atable a"
        + " JOIN atable b ON a.organization_id = b.organization_id"
        + " WHERE a.organization_id = '00D000000000001'",
      text("CLIENT PARALLEL <N>-WAY RANGE SCAN OVER ATABLE ['00D000000000001']",
        "    PARALLEL INNER-JOIN TABLE 0", "        CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "    DYNAMIC SERVER FILTER BY A.ORGANIZATION_ID IN (B.ORGANIZATION_ID)"),
      // HashJoinPlan uses the List<String>-only ExplainPlan constructor, which installs the
      // default attributes (all-null/empty). Freeze that baseline.
      defaultAttrs());
  }

  @Test
  public void testHashJoinSemiInSubquery() throws Exception {
    // Phoenix temp aliases are renamed by first appearance so this case asserts on the canonical
    // "$1.$2" form.
    verifyQuery("hashJoinSemiInSubquery",
      "SELECT a_string FROM atable"
        + " WHERE organization_id IN (SELECT organization_id FROM atable WHERE a_integer = 1)",
      text("CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE", "    SKIP-SCAN-JOIN TABLE 0",
        "        CLIENT PARALLEL <N>-WAY FULL SCAN OVER ATABLE",
        "            SERVER FILTER BY A_INTEGER = 1",
        "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [ORGANIZATION_ID]",
        "    DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($1.$2)"),
      defaultAttrs());
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
    verifyMutation("deleteSingleRow", "DELETE FROM atable WHERE organization_id = '00D000000000001'"
      + " AND entity_id = '00E00000000001'", true, text("DELETE SINGLE ROW"), defaultAttrs());
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
  public void testTextNormalizerRenumbersTempAliasesByFirstAppearance() {
    // Two distinct aliases ($7, $9) with $7 appearing first → $1, $9 → $2.
    assertEquals(
      Collections.singletonList("    DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($1.$2)"),
      new ExplainTextNormalizer()
        .normalize(Collections.singletonList(
          "    DYNAMIC SERVER FILTER BY ATABLE.ORGANIZATION_ID IN ($7.$9)")));
  }

  @Test
  public void testTextNormalizerSharesAliasStateAcrossLines() {
    // The same alias ($5) appearing in two different lines gets the same renumbered token. A
    // distinct alias ($8) on the second line gets the next number.
    List<String> in = Arrays.asList(
      "CLIENT PARALLEL 1-WAY FULL SCAN OVER ($5)",
      "    DYNAMIC SERVER FILTER BY T.X IN ($5.$8)");
    assertEquals(Arrays.asList(
      "CLIENT PARALLEL <N>-WAY FULL SCAN OVER ($1)",
      "    DYNAMIC SERVER FILTER BY T.X IN ($1.$2)"),
      new ExplainTextNormalizer().normalize(in));
  }

  @Test
  public void testTextNormalizerLeavesNonAliasDollarTokensAlone() {
    // The "$<N>" form below is the canonical renumbered form, not a temp alias, so the token
    // pattern only matches "$<digits>". A literal dollar followed by a non-digit is preserved.
    List<String> in = Collections.singletonList("CLIENT FILTER BY price > $5.50 AND tag = '$abc'");
    assertEquals(
      Collections.singletonList("CLIENT FILTER BY price > $1.50 AND tag = '$abc'"),
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
    ExplainPlanAttributes a = new ExplainPlanAttributesBuilder()
      .setIteratorTypeAndScanSize("PARALLEL 1-WAY").setTableName("T").build();
    String json = mapper.writeValueAsString(a);
    int iAbstract = json.indexOf("\"abstractExplainPlan\"");
    int iIter = json.indexOf("\"iteratorTypeAndScanSize\"");
    int iTable = json.indexOf("\"tableName\"");
    int iRhs = json.indexOf("\"rhsJoinQueryExplainPlan\"");
    int iMerge = json.indexOf("\"serverMergeColumns\"");
    int iRegions = json.indexOf("\"regionLocations\"");
    int iLookups = json.indexOf("\"numRegionLocationLookups\"");
    assertTrue("abstractExplainPlan first", iAbstract >= 0 && iAbstract < iIter);
    assertTrue("iteratorTypeAndScanSize before tableName", iIter < iTable);
    assertTrue("rhsJoinQueryExplainPlan before serverMergeColumns", iRhs < iMerge);
    assertTrue("serverMergeColumns before regionLocations", iMerge < iRegions);
    assertTrue("regionLocations before numRegionLocationLookups", iRegions < iLookups);
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
      ExplainPlan plan = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)
        .optimizeQuery().getExplainPlan();
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
    PhoenixPreparedStatement ps =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
    return ps.compileMutation().getExplainPlan();
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
    return n;
  }

  /**
   * Convenience wrapper that builds {@link #defaultAttrs()} and sets the five fields every
   * connection backed scan emits via {@code ExplainTable.explain}: {@code iteratorTypeAndScanSize},
   * {@code consistency}, {@code explainScanType}, {@code tableName}, and {@code keyRanges}.
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
