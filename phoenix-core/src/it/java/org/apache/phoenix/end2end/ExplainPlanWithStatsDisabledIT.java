/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.end2end.ExplainPlanWithStatsEnabledIT.getByteRowEstimates;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.end2end.ExplainPlanWithStatsEnabledIT.Estimate;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are disabled.
 */
@Category(ParallelStatsDisabledTest.class)
public class ExplainPlanWithStatsDisabledIT extends ParallelStatsDisabledIT {

    private static void initData(Connection conn, String tableName) throws Exception {
        conn.createStatement().execute("CREATE TABLE " + tableName
                + " ( k INTEGER, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k)) GUIDE_POSTS_WIDTH = 0");
        conn.createStatement().execute("upsert into " + tableName + " values (100,1,3)");
        conn.createStatement().execute("upsert into " + tableName + " values (101,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (102,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (103,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (104,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (105,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (106,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (107,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (108,2,4)");
        conn.createStatement().execute("upsert into " + tableName + " values (109,2,4)");
        conn.commit();
        // Because the guide post width is set to 0, no guide post will be collected
        // effectively disabling stats collection.
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
    }

    @Test
    public void testBytesRowsForSelect() throws Exception {
        String tableA = generateUniqueName();
        String sql = "SELECT * FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUnion() throws Exception {
        String tableA = generateUniqueName();
        String tableB = generateUniqueName();
        String sql = "SELECT * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            initData(conn, tableB);
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForHashJoin() throws Exception {
        String tableA = generateUniqueName();
        String tableB = generateUniqueName();
        String sql =
                "SELECT ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN " + tableB
                        + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            initData(conn, tableB);
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForSortMergeJoin() throws Exception {
        String tableA = generateUniqueName();
        String tableB = generateUniqueName();
        String sql =
                "SELECT /*+ USE_SORT_MERGE_JOIN */ ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN "
                        + tableB + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            initData(conn, tableB);
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForAggregateQuery() throws Exception {
        String tableA = generateUniqueName();
        String sql = "SELECT count(*) FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectServerSide() throws Exception {
        String tableA = generateUniqueName();
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(true);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectClientSide() throws Exception {
        String tableA = generateUniqueName();
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertValues() throws Exception {
        String tableA = generateUniqueName();
        String sql = "UPSERT INTO " + tableA + " VALUES (?, ?, ?)";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        binds.add(99);
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            assertEstimatesAreZero(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForDeleteServerSide() throws Exception {
        String tableA = generateUniqueName();
        String sql = "DELETE FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(true);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForDeleteClientSideExecutedSerially() throws Exception {
        String tableA = generateUniqueName();
        String sql = "DELETE FROM " + tableA + " where k >= ? LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertTrue(info.estimatedRows > 0);
        }
    }

    @Test
    public void testBytesRowsForPointDelete() throws Exception {
        String tableA = generateUniqueName();
        String sql = "DELETE FROM " + tableA + " where k = ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(100);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            assertEstimatesAreZero(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForSelectExecutedSerially() throws Exception {
        String tableA = generateUniqueName();
        String sql = "SELECT * FROM " + tableA + " LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertTrue(info.estimatedRows > 0);
        }
    }

    @Test
    public void testEstimatesForUnionWithTablesWithNullAndLargeGpWidth() throws Exception {
        String tableA = generateUniqueName();
        String tableWithLargeGPWidth = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            // create a table with 1 MB guidepost width
            long guidePostWidth = 1000000;
            conn.createStatement()
                    .execute("CREATE TABLE " + tableWithLargeGPWidth
                            + " ( k INTEGER, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k)) GUIDE_POSTS_WIDTH="
                            + guidePostWidth);
            conn.createStatement()
                    .execute("upsert into " + tableWithLargeGPWidth + " values (100,1,3)");
            conn.createStatement()
                    .execute("upsert into " + tableWithLargeGPWidth + " values (101,2,4)");
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableWithLargeGPWidth);
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql =
                    "SELECT * FROM " + tableA + " UNION ALL SELECT * FROM " + tableWithLargeGPWidth;
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testDescTimestampAtBoundary() throws Exception {
        Properties props = PropertiesUtil.deepCopy(new Properties());
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(
                "CREATE TABLE FOO(\n" + "                a VARCHAR NOT NULL,\n"
                    + "                b TIMESTAMP NOT NULL,\n" + "                c VARCHAR,\n"
                    + "                CONSTRAINT pk PRIMARY KEY (a, b DESC, c)\n"
                    + "              ) IMMUTABLE_ROWS=true\n" + "                ,SALT_BUCKETS=20");
            String query =
                "select * from foo where a = 'a' and b >= timestamp '2016-01-28 00:00:00' and b < timestamp '2016-01-29 00:00:00'";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String queryPlan = QueryUtil.getExplainPlan(rs);
            assertEquals(
                "CLIENT PARALLEL 20-WAY RANGE SCAN OVER FOO [X'00','a',~'2016-01-28 23:59:59.999'] - [X'13','a',~'2016-01-28 00:00:00.000']\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT", queryPlan);
        }
    }

    @Test
    public void testUseOfRoundRobinIteratorSurfaced() throws Exception {
        Properties props = PropertiesUtil.deepCopy(new Properties());
        props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = "testUseOfRoundRobinIteratorSurfaced".toUpperCase();
            conn.createStatement().execute(
                "CREATE TABLE " + tableName + "(\n" + "                a VARCHAR NOT NULL,\n"
                    + "                b TIMESTAMP NOT NULL,\n" + "                c VARCHAR,\n"
                    + "                CONSTRAINT pk PRIMARY KEY (a, b DESC, c)\n"
                    + "              ) IMMUTABLE_ROWS=true\n" + "                ,SALT_BUCKETS=20");
            String query = "select * from " + tableName
                + " where a = 'a' and b >= timestamp '2016-01-28 00:00:00' and b < timestamp '2016-01-29 00:00:00'";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String queryPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 20-WAY ROUND ROBIN RANGE SCAN OVER " + tableName
                + " [X'00','a',~'2016-01-28 23:59:59.999'] - [X'13','a',~'2016-01-28 00:00:00.000']\n"
                + "    SERVER FILTER BY FIRST KEY ONLY", queryPlan);
        }
    }

    @Test
    public void testRangeScanWithMetadataLookup() throws Exception {
        final String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(new Properties());
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(
                "CREATE TABLE " + tableName + "(PK1 VARCHAR NOT NULL, "
                    + "PK2 VARCHAR, COL1 VARCHAR"
                    + " CONSTRAINT pk PRIMARY KEY (PK1, PK2)) SPLIT ON ('b', 'c', 'd')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('0123A', 'pk20', 'col10')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('#0123A', 'pk20', 'col10')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('_0123A', 'pk20', 'col10')");
            for (int i = 0; i < 25; i++) {
                String pk1Val = "a" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2a', 'col10a')");
                pk1Val = "ab" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2ab', 'col10ab')");
            }
            for (int i = 0; i < 25; i++) {
                String pk1Val = "b" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2b', 'col10b')");
                pk1Val = "bc" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2bc', 'col10bc')");
            }
            for (int i = 0; i < 25; i++) {
                String pk1Val = "c" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2c', 'col10c')");
                pk1Val = "cd" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2cd', 'col10cd')");
            }
            for (int i = 0; i < 25; i++) {
                String pk1Val = "d" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2d', 'col10d')");
                pk1Val = "de" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2de', 'col10de')");
            }
            for (int i = 0; i < 25; i++) {
                String pk1Val = "e" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2e', 'col10e')");
                pk1Val = "ef" + i;
                conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('" + pk1Val + "', 'pk2ef', 'col10ef')");
            }
            conn.commit();

            String query = "select count(*) from " + tableName
                + " where PK1 <= 'b'";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(53, rs.getInt(1));
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String queryPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [*] - ['b']\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "    SERVER AGGREGATE INTO SINGLE ROW",
                queryPlan);
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class)
                .optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
            assertEquals(2, planAttributes.getNumRegionLocationLookups());

            query = "select count(*) from " + tableName
                + " where PK1 <= 'cd'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(128, rs.getInt(1));
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            queryPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [*] - ['cd']\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "    SERVER AGGREGATE INTO SINGLE ROW",
                queryPlan);
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class)
                .optimizeQuery()
                .getExplainPlan();
            planAttributes = plan.getPlanStepsAsAttributes();
            assertEquals(3, planAttributes.getNumRegionLocationLookups());

            query = "select count(*) from " + tableName
                + " where PK1 LIKE 'ef%'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(25, rs.getInt(1));
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            queryPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['ef'] - ['eg']\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "    SERVER AGGREGATE INTO SINGLE ROW",
                queryPlan);
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class)
                .optimizeQuery()
                .getExplainPlan();
            planAttributes = plan.getPlanStepsAsAttributes();
            assertEquals(1, planAttributes.getNumRegionLocationLookups());

            query = "select count(*) from " + tableName
                + " where PK1 > 'de'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(75, rs.getInt(1));
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            queryPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['de'] - [*]\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "    SERVER AGGREGATE INTO SINGLE ROW",
                queryPlan);
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class)
                .optimizeQuery()
                .getExplainPlan();
            planAttributes = plan.getPlanStepsAsAttributes();
            assertEquals(1, planAttributes.getNumRegionLocationLookups());
        }
    }

    @Test
    public void testMultiTenantWithMetadataLookup() throws Exception {
        final String tableName = generateUniqueName();
        final String view01 = generateUniqueName();
        final String view02 = generateUniqueName();
        final String view03 = generateUniqueName();
        final String view04 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(new Properties());
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(
                "CREATE TABLE " + tableName + "(TENANT_ID VARCHAR NOT NULL, "
                    + "PK2 VARCHAR, COL1 VARCHAR"
                    + " CONSTRAINT pk PRIMARY KEY (TENANT_ID, PK2)) MULTI_TENANT = true"
                    + " SPLIT ON ('b', 'c', 'd')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('0123A', 'pk20', 'col10')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('#0123A', 'pk20', 'col10')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('_0123A', 'pk20', 'col10')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('bcde', 'pk20', 'col10')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('cdef', 'pk20', 'col10')");
            conn.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES ('defg', 'pk20', 'col10')");
            conn.commit();

            try (Connection tenantConn = getTenantConnection("ab12")) {
                tenantConn.createStatement().execute("CREATE VIEW " + view01
                    + " (COL2 VARCHAR) AS SELECT * FROM " + tableName);
                for (int i = 0; i < 25; i++) {
                    String pk2Val = "012" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view01 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col101', 'col201')");
                    pk2Val = "ab" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view01 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col1010', 'col2010')");
                }
                tenantConn.commit();
            }

            try (Connection tenantConn = getTenantConnection("bc12")) {
                tenantConn.createStatement().execute("CREATE VIEW " + view02
                    + " (COL2 VARCHAR) AS SELECT * FROM " + tableName);
                for (int i = 0; i < 25; i++) {
                    String pk2Val = "012" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view02 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col101', 'col201')");
                    pk2Val = "ab" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view02 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col1010', 'col2010')");
                }
                tenantConn.commit();
            }

            try (Connection tenantConn = getTenantConnection("cd12")) {
                tenantConn.createStatement().execute("CREATE VIEW " + view03
                    + " (COL2 VARCHAR) AS SELECT * FROM " + tableName);
                for (int i = 0; i < 25; i++) {
                    String pk2Val = "012" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view03 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col101', 'col201')");
                    pk2Val = "ab" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view03 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col1010', 'col2010')");
                }
                tenantConn.commit();
            }

            try (Connection tenantConn = getTenantConnection("de12")) {
                tenantConn.createStatement().execute("CREATE VIEW " + view04
                    + " (COL2 VARCHAR) AS SELECT * FROM " + tableName);
                for (int i = 0; i < 25; i++) {
                    String pk2Val = "012" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view04 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col101', 'col201')");
                    pk2Val = "ab" + i;
                    tenantConn.createStatement().execute(
                        "UPSERT INTO " + view04 + "(PK2, COL1, COL2) VALUES ('" + pk2Val
                            + "', 'col1010', 'col2010')");
                }
                tenantConn.commit();
            }

            try (Connection tenantConn = getTenantConnection("ab12")) {
                String query = "select count(*) from " + view01;
                ResultSet rs = tenantConn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1));

                rs = tenantConn.createStatement().executeQuery("EXPLAIN " + query);
                String queryPlan = QueryUtil.getExplainPlan(rs);
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['ab12']\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "    SERVER AGGREGATE INTO SINGLE ROW",
                    queryPlan);
                ExplainPlan plan = tenantConn.prepareStatement(query)
                    .unwrap(PhoenixPreparedStatement.class)
                    .optimizeQuery()
                    .getExplainPlan();
                ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
                assertEquals(1, planAttributes.getNumRegionLocationLookups());
            }

            try (Connection tenantConn = getTenantConnection("cd12")) {
                String query = "select * from " + view03 + " order by col2";

                ResultSet rs = tenantConn.createStatement().executeQuery("EXPLAIN " + query);
                String queryPlan = QueryUtil.getExplainPlan(rs);
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['cd12']\n"
                        + "    SERVER SORTED BY [COL2]\n"
                        + "CLIENT MERGE SORT",
                    queryPlan);
                ExplainPlan plan = tenantConn.prepareStatement(query)
                    .unwrap(PhoenixPreparedStatement.class)
                    .optimizeQuery()
                    .getExplainPlan();
                ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
                assertEquals(1, planAttributes.getNumRegionLocationLookups());
            }

            try (Connection tenantConn = getTenantConnection("de12")) {
                String query = "select * from " + view04 + " where col1='col101'";
                ResultSet rs = tenantConn.createStatement().executeQuery(query);
                int c = 0;
                while (rs.next()) {
                    c++;
                }
                assertEquals(25, c);

                rs = tenantConn.createStatement().executeQuery("EXPLAIN " + query);
                String queryPlan = QueryUtil.getExplainPlan(rs);
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['de12']\n"
                        + "    SERVER FILTER BY COL1 = 'col101'",
                    queryPlan);
                ExplainPlan plan = tenantConn.prepareStatement(query)
                    .unwrap(PhoenixPreparedStatement.class)
                    .optimizeQuery()
                    .getExplainPlan();
                ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
                assertEquals(1, planAttributes.getNumRegionLocationLookups());
            }
        }
    }

    private Connection getTenantConnection(final String tenantId) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }

    public static void assertEstimatesAreNull(String sql, List<Object> binds, Connection conn)
            throws Exception {
        Estimate info = getByteRowEstimates(conn, sql, binds);
        assertNull(info.estimatedBytes);
        assertNull(info.estimatedRows);
        assertNull(info.estimateInfoTs);
    }

    private void assertEstimatesAreZero(String sql, List<Object> binds, Connection conn)
            throws Exception {
        Estimate info = getByteRowEstimates(conn, sql, binds);
        assertEquals((Long) 0l, info.estimatedBytes);
        assertEquals((Long) 0l, info.estimatedRows);
        assertEquals((Long) 0l, info.estimateInfoTs);
    }
}
