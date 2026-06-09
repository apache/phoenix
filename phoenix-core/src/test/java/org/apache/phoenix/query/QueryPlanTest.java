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

import static org.apache.phoenix.query.explain.ExplainPlanTestUtil.assertPlan;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/** Verifies query plan details via {@link org.apache.phoenix.compile.ExplainPlanAttributes}. */
public class QueryPlanTest extends BaseConnectionlessQueryTest {

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

    // LIMIT 1 uses a SERIAL iterator and pushes the limit to both server and client.
    assertPlan(conn, "SELECT * FROM TENANT_VIEW LIMIT 1").iteratorType("SERIAL")
      .scanType("RANGE SCAN").table("BASE_MULTI_TENANT_TABLE").keyRanges(" ['tenantId']")
      .serverRowLimit(1L).clientRowLimit(1);

    // A very large limit falls back to a PARALLEL iterator.
    assertPlan(conn, "SELECT * FROM TENANT_VIEW LIMIT " + Integer.MAX_VALUE)
      .iteratorType("PARALLEL").scanType("RANGE SCAN").table("BASE_MULTI_TENANT_TABLE")
      .keyRanges(" ['tenantId']").serverRowLimit((long) Integer.MAX_VALUE)
      .clientRowLimit(Integer.MAX_VALUE);

    assertPlan(conn, "SELECT * FROM TENANT_VIEW WHERE username = 'Joe' LIMIT 1")
      .scanType("RANGE SCAN").table("BASE_MULTI_TENANT_TABLE").keyRanges(" ['tenantId']")
      .serverWhereFilter("SERVER FILTER BY USERNAME = 'Joe'").serverRowLimit(1L).clientRowLimit(1);

    assertPlan(conn, "SELECT * FROM TENANT_VIEW WHERE col = 'Joe' LIMIT 1").scanType("RANGE SCAN")
      .table("BASE_MULTI_TENANT_TABLE").keyRanges(" ['tenantId']")
      .serverWhereFilter("SERVER FILTER BY COL = 'Joe'").serverRowLimit(1L).clientRowLimit(1);
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
      String query = "select * from foo where a = 'a' and b >= timestamp '2016-01-28 00:00:00'"
        + " and b < timestamp '2016-01-29 00:00:00'";
      // The salient detail is the DESC-timestamp key range.
      assertPlan(conn, query).scanType("RANGE SCAN").table("FOO")
        .keyRanges(
          " [X'00','a',~'2016-01-28 23:59:59.999'] -" + " [X'13','a',~'2016-01-28 00:00:00.000']")
        .serverFirstKeyOnlyProjection(true).clientSortAlgo("CLIENT MERGE SORT");
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
      String query =
        "select * from " + tableName + " where a = 'a' and b >= timestamp '2016-01-28 00:00:00'"
          + " and b < timestamp '2016-01-29 00:00:00'";
      // The round-robin iterator is surfaced as a dedicated attribute.
      assertPlan(conn, query).useRoundRobinIterator(true).scanType("RANGE SCAN").table(tableName)
        .keyRanges(
          " [X'00','a',~'2016-01-28 23:59:59.999'] -" + " [X'13','a',~'2016-01-28 00:00:00.000']")
        .serverFirstKeyOnlyProjection(true);
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
      // The SERIAL hint is ignored for a non-rowkey ORDER BY, so the iterator stays PARALLEL and a
      // server sort + client merge sort are planned.
      assertPlan(conn, query).iteratorType("PARALLEL").scanType("RANGE SCAN").table("FOO")
        .keyRanges(" ['a']").serverFirstKeyOnlyProjection(true).serverSortedBy("[B, C]")
        .clientSortAlgo("CLIENT MERGE SORT");
    } finally {
      conn.close();
    }
  }
}
