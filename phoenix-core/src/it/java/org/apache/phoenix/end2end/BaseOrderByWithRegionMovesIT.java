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

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertResultSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public abstract class BaseOrderByWithRegionMovesIT extends ParallelStatsDisabledWithRegionMovesIT {

    @Before
    public void setUp() throws Exception {
        hasTestStarted = true;
    }

    @After
    public void tearDown() throws Exception {
        countOfDummyResults = 0;
        TABLE_NAMES.clear();
        hasTestStarted = false;
    }

    @Test
    public void testMultiOrderByExpr() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());
        TABLE_NAMES.add(tableName);
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                        Lists.newArrayList("ENTITY_ID", "B_STRING"))
                .setFullTableName(tableName)
                .setOrderByClause("B_STRING, ENTITY_ID");

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW1, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW4, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW2, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW5, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW8, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW6, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));

            assertFalse(rs.next());
        }
    }

    @Test
    public void testDescMultiOrderByExpr() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, null, getUrl());
        TABLE_NAMES.add(tableName);
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                        Lists.newArrayList("ENTITY_ID", "B_STRING"))
                .setFullTableName(tableName)
                .setOrderByClause("B_STRING || ENTITY_ID DESC");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW9, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW6, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW5, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW2, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW4, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testDescMultiOrderByExprWithSplits() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());
        TABLE_NAMES.add(tableName);
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                        Lists.newArrayList("ENTITY_ID", "B_STRING"))
                .setFullTableName(tableName)
                .setOrderByClause("B_STRING || ENTITY_ID DESC");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW9, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW6, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW5, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW2, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW4, rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testOrderByDifferentColumns() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String tableName = generateUniqueName();
            TABLE_NAMES.add(tableName);
            String ddl = "CREATE TABLE " + tableName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 40);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            QueryBuilder queryBuilder = new QueryBuilder()
                    .setSelectColumns(
                            Lists.newArrayList("COL1"))
                    .setFullTableName(tableName)
                    .setSelectExpression("count(*)")
                    .setGroupByClause("COL1")
                    .setOrderByClause("COL1");
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(1, rs.getLong(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(1, rs.getLong(1));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals(1, rs.getLong(1));
            assertFalse(rs.next());

            queryBuilder = new QueryBuilder();
            queryBuilder.setSelectColumns(
                    Lists.newArrayList("A_STRING", "COL1"));
            queryBuilder.setFullTableName(tableName);
            queryBuilder.setOrderByClause("A_STRING");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals("a", rs.getString(1));
            assertEquals(40, rs.getInt(2));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals("b", rs.getString(1));
            assertEquals(20, rs.getInt(2));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals("c", rs.getString(1));
            assertEquals(30, rs.getInt(2));
            assertFalse(rs.next());

            queryBuilder.setSelectColumns(
                    Lists.newArrayList("A_STRING", "COL1"));
            queryBuilder.setFullTableName(tableName);
            queryBuilder.setOrderByClause("COL1");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals("b", rs.getString(1));
            assertEquals(20, rs.getInt(2));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals("c", rs.getString(1));
            assertEquals(30, rs.getInt(2));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals("a", rs.getString(1));
            assertEquals(40, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testAggregateOrderBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);
        String ddl = "create table " + tableName +
                " (ID VARCHAR NOT NULL PRIMARY KEY, VAL1 VARCHAR, VAL2 INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("upsert into " + tableName + " values ('ABC','aa123', 11)");
        conn.createStatement().execute("upsert into " + tableName + " values ('ABD','ba124', 1)");
        conn.createStatement().execute("upsert into " + tableName + " values ('ABE','cf125', 13)");
        conn.createStatement().execute("upsert into " + tableName + " values ('ABF','dan126', 4)");
        conn.createStatement().execute("upsert into " + tableName + " values ('ABG','elf127', 15)");
        conn.createStatement().execute("upsert into " + tableName + " values ('ABH','fan128', 6)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAA','get211', 100)");
        conn.createStatement().execute("upsert into " + tableName + " values ('AAB','hat212', 7)");
        conn.createStatement().execute("upsert into " + tableName + " values ('AAC','aap12', 2)");
        conn.createStatement().execute("upsert into " + tableName + " values ('AAD','ball12', 3)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAE','inn2110', 13)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAF','key2112', 40)");
        conn.commit();

        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                        Lists.newArrayList("VAL1", "VAL2"))
                .setFullTableName(tableName)
                .setOrderByClause("VAL1")
                .setSelectExpression("DISTINCT(ID)")
                .setSelectExpressionColumns(Lists.newArrayList("ID"))
                .setWhereClause(
                        "ID in ('ABC','ABD','ABE','ABF','ABG','ABH','AAA', 'AAB', 'AAC','AAD','AAE','AAF')");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("ABC", rs.getString(1));
        assertEquals("aa123", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("aap12", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ba124", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ball12", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("cf125", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("dan126", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("elf127", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("fan128", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("get211", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("hat212", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("inn2110", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("AAF", rs.getString(1));
        assertEquals("key2112", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testAggregateOptimizedOutOrderBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);
        String ddl = "create table " + tableName +
                " (K1 VARCHAR NOT NULL, K2 VARCHAR NOT NULL, VAL1 VARCHAR, VAL2 INTEGER, CONSTRAINT pk PRIMARY KEY(K1,K2))";
        conn.createStatement().execute(ddl);

        conn.createStatement()
                .execute("upsert into " + tableName + " values ('ABC','ABC','aa123', 11)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('ABD','ABC','ba124', 1)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('ABE','ABC','cf125', 13)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('ABF','ABC','dan126', 4)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('ABG','ABC','elf127', 15)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('ABH','ABC','fan128', 6)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAA','ABC','get211', 100)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAB','ABC','hat212', 7)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAC','ABC','aap12', 2)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAD','ABC','ball12', 3)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAE','ABC','inn2110', 13)");
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('AAF','ABC','key2112', 40)");
        conn.commit();

        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                        Lists.newArrayList("VAL1", "VAL2"))
                .setFullTableName(tableName)
                .setOrderByClause("VAL1")
                .setSelectExpressionColumns(Lists.newArrayList("K2"))
                .setSelectExpression("DISTINCT(K2)")
                .setWhereClause("K2 = 'ABC'");

        // verify that the phoenix query plan doesn't contain an order by
        ExplainPlan plan = conn.prepareStatement(queryBuilder.build())
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals("SERVER FILTER BY K2 = 'ABC'",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals("SERVER AGGREGATE INTO DISTINCT ROWS BY [K2, VAL1, VAL2]",
                explainPlanAttributes.getServerAggregate());
        assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
        assertNull(explainPlanAttributes.getClientSortedBy());
        assertNull(explainPlanAttributes.getServerSortedBy());

        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("ABC", rs.getString(1));
        assertEquals("aa123", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("aap12", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ba124", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ball12", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("cf125", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("dan126", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("elf127", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("fan128", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("get211", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("hat212", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("inn2110", rs.getString(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("ABC", rs.getString(1));
        assertEquals("key2112", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testNullsLastWithDesc() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            TABLE_NAMES.add(tableName);
            String sql = "CREATE TABLE " + tableName + " ( " +
                    "ORGANIZATION_ID VARCHAR," +
                    "CONTAINER_ID VARCHAR," +
                    "ENTITY_ID VARCHAR NOT NULL," +
                    "CONSTRAINT TEST_PK PRIMARY KEY ( " +
                    "ORGANIZATION_ID DESC," +
                    "CONTAINER_ID DESC," +
                    "ENTITY_ID" +
                    "))";
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a',null,'11')");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (null,'2','22')");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('c','3','33')");
            conn.commit();

            //-----ORGANIZATION_ID

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID ASC NULLS FIRST";
            QueryBuilder queryBuilder = new QueryBuilder()
                    .setSelectColumns(
                            Lists.newArrayList("CONTAINER_ID", "ORGANIZATION_ID"))
                    .setFullTableName(tableName)
                    .setOrderByClause("ORGANIZATION_ID ASC NULLS FIRST");
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new String[][]{{"2", null}, {null, "a"}, {"3", "c"},}, tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new String[][]{{null, "a"}, {"3", "c"}, {"2", null}},
                    tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new String[][]{{"2", null}, {"3", "c"}, {null, "a"}},
                    tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new String[][]{{"3", "c"}, {null, "a"}, {"2", null}},
                    tableName);

            //----CONTAINER_ID

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS FIRST";
            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, "a"}, {"2", null}, {"3", "c"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2", null}, {"3", "c"}, {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new String[][]{{null, "a"}, {"3", "c"}, {"2", null}},
                    tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3", "c"}, {"2", null}, {null, "a"}});

            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (null,null,'44')");
            conn.commit();

            //-----ORGANIZATION_ID ASC  CONTAINER_ID ASC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS FIRST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new String[][]{{null, null}, {"2", null}, {null, "a"},
                            {"3", "c"}}, tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2", null}, {null, null}, {null, "a"},
                    {"3", "c"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS FIRST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, "a"}, {"3", "c"}, {null, null},
                    {"2", null}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, "a"}, {"3", "c"}, {"2", null},
                    {null, null}});


            //-----ORGANIZATION_ID ASC  CONTAINER_ID DESC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, null}, {"2", null}, {null, "a"},
                    {"3", "c"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new String[][]{{"2", null}, {null, null}, {null, "a"},
                            {"3", "c"}}, tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new String[][]{{null, "a"}, {"3", "c"}, {null, null},
                    {"2", null}}, tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, "a"}, {"3", "c"}, {"2", null},
                    {null, null}});

            //-----ORGANIZATION_ID DESC  CONTAINER_ID ASC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, null}, {"2", null}, {"3", "c"},
                    {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2", null}, {null, null}, {"3", "c"},
                    {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new String[][]{{"3", "c"}, {null, "a"}, {null, null},
                    {"2", null}}, tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new String[][]{{"3", "c"}, {null, "a"}, {"2", null},
                    {null, null}}, tableName);

            //-----ORGANIZATION_ID DESC  CONTAINER_ID DESC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, null}, {"2", null}, {"3", "c"},
                    {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2", null}, {null, null}, {"3", "c"},
                    {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3", "c"}, {null, "a"}, {null, null},
                    {"2", null}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3", "c"}, {null, "a"}, {"2", null}, {null, null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID ASC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS FIRST";
            queryBuilder.setOrderByClause("CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, null}, {null, "a"}, {"2", null}, {"3", "c"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS LAST";
            queryBuilder.setOrderByClause("CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, "a"}, {null, null}, {"2", null}, {"3", "c"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS FIRST";
            queryBuilder.setOrderByClause("CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new String[][]{{"2", null}, {"3", "c"}, {null, null}, {null,
                            "a"}}, tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS LAST";
            queryBuilder.setOrderByClause("CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2", null}, {"3", "c"}, {null, "a"}, {null, null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID DESC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, null}, {null, "a"}, {"2", null}, {"3", "c"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new String[][]{{null, "a"}, {null, null}, {"2", null}, {"3",
                            "c"}}, tableName);

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2", null}, {"3", "c"}, {null, null}, {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2", null}, {"3", "c"}, {null, "a"}, {null, null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID ASC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, null}, {null, "a"}, {"3", "c"}, {"2", null}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, "a"}, {null, null}, {"3", "c"}, {"2", null}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3", "c"}, {"2", null}, {null, null}, {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3", "c"}, {"2", null}, {null, "a"}, {null, null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID DESC

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, null}, {null, "a"}, {"3", "c"}, {"2", null}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null, "a"}, {null, null}, {"3", "c"}, {"2", null}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3", "c"}, {"2", null}, {null, null}, {null, "a"}});

//            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new String[][]{{"3", "c"}, {"2", null}, {null, "a"}, {null,
                            null}}, tableName);
        }
    }

    @Test
    public void testOrderByReverseOptimization1() throws Exception {
        for (boolean salted : new boolean[]{true, false}) {
            doTestOrderByReverseOptimization(salted, true, true, true);
            doTestOrderByReverseOptimization(salted, true, true, false);
            doTestOrderByReverseOptimization(salted, true, false, true);
            doTestOrderByReverseOptimization(salted, true, false, false);
        }
    }

    @Test
    public void testOrderByReverseOptimization2() throws Exception {
        for (boolean salted : new boolean[]{true, false}) {
            doTestOrderByReverseOptimization(salted, false, true, true);
            doTestOrderByReverseOptimization(salted, false, true, false);
            doTestOrderByReverseOptimization(salted, false, false, true);
            doTestOrderByReverseOptimization(salted, false, false, false);
        }
    }

    private void doTestOrderByReverseOptimization(boolean salted, boolean desc1, boolean desc2,
                                                  boolean desc3) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            TABLE_NAMES.add(tableName);
            String sql = "CREATE TABLE " + tableName + " ( " +
                    "ORGANIZATION_ID INTEGER NOT NULL," +
                    "CONTAINER_ID INTEGER NOT NULL," +
                    "SCORE INTEGER NOT NULL," +
                    "ENTITY_ID INTEGER NOT NULL," +
                    "CONSTRAINT TEST_PK PRIMARY KEY ( " +
                    "ORGANIZATION_ID" + (desc1 ? " DESC" : "") + "," +
                    "CONTAINER_ID" + (desc2 ? " DESC" : "") + "," +
                    "SCORE" + (desc3 ? " DESC" : "") + "," +
                    "ENTITY_ID" +
                    ")) " + (salted ? "SALT_BUCKETS =4" : "split on(4)");
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1,1,1,1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (2,2,2,2)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3,3,3,3)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4,4,4,4)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (5,5,5,5)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (6,6,6,6)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1,1,1,11)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (2,2,2,22)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3,3,3,33)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4,4,4,44)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (5,5,5,55)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (6,6,6,66)");
            conn.commit();

            QueryBuilder queryBuilder = new QueryBuilder()
                    .setSelectColumns(
                            Lists.newArrayList("CONTAINER_ID", "ORGANIZATION_ID"))
                    .setFullTableName(tableName)
                    .setGroupByClause("ORGANIZATION_ID, CONTAINER_ID")
                    .setOrderByClause("ORGANIZATION_ID ASC, CONTAINER_ID ASC");
            //groupBy orderPreserving orderBy asc asc
//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC, CONTAINER_ID ASC";
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new Object[][]{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5,
                    5}, {6, 6}}, tableName);

            //groupBy orderPreserving orderBy asc desc
//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC, CONTAINER_ID desc";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC, CONTAINER_ID DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}});

            //groupBy orderPreserving orderBy desc asc
//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC, CONTAINER_ID ASC";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC, CONTAINER_ID ASC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{6, 6}, {5, 5}, {4, 4}, {3, 3}, {2, 2}, {1, 1}});

            //groupBy orderPreserving orderBy desc desc
//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC, CONTAINER_ID DESC";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC, CONTAINER_ID DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{6, 6}, {5, 5}, {4, 4}, {3, 3}, {2,
                    2}, {1, 1}});

            //groupBy not orderPreserving orderBy asc asc
//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC, SCORE ASC";
            queryBuilder.setSelectColumns(
                    Lists.newArrayList("ORGANIZATION_ID", "SCORE"));
            queryBuilder.setGroupByClause("ORGANIZATION_ID, SCORE");
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC, SCORE ASC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}});

            //groupBy not orderPreserving orderBy asc desc
//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC, SCORE DESC";
            queryBuilder.setGroupByClause("ORGANIZATION_ID, SCORE");
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC, SCORE DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}});

            //groupBy not orderPreserving orderBy desc asc
//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC, SCORE ASC";
            queryBuilder.setGroupByClause("ORGANIZATION_ID, SCORE");
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC, SCORE ASC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new Object[][]{{6, 6}, {5, 5}, {4, 4}, {3, 3}, {2,
                    2}, {1, 1}}, tableName);

            //groupBy not orderPreserving orderBy desc desc
//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC, SCORE DESC";
            queryBuilder.setGroupByClause("ORGANIZATION_ID, SCORE");
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC, SCORE DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{6, 6}, {5, 5}, {4, 4}, {3, 3}, {2, 2}, {1, 1}});
        }
    }

    @Test
    public void testOrderByReverseOptimizationWithNullsLast1() throws Exception {
        for (boolean salted : new boolean[]{true, false}) {
            doTestOrderByReverseOptimizationWithNullsLast(salted, true, true, true);
            doTestOrderByReverseOptimizationWithNullsLast(salted, true, true, false);
        }
    }

    @Test
    public void testOrderByReverseOptimizationWithNullsLast2() throws Exception {
        for (boolean salted : new boolean[]{true, false}) {
            doTestOrderByReverseOptimizationWithNullsLast(salted, true, false, true);
            doTestOrderByReverseOptimizationWithNullsLast(salted, true, false, false);
        }
    }

    @Test
    public void testOrderByReverseOptimizationWithNullsLast3() throws Exception {
        for (boolean salted : new boolean[]{true, false}) {
            doTestOrderByReverseOptimizationWithNullsLast(salted, false, true, true);
            doTestOrderByReverseOptimizationWithNullsLast(salted, false, true, false);
        }
    }

    @Test
    public void testOrderByReverseOptimizationWithNullsLast4() throws Exception {
        for (boolean salted : new boolean[]{true, false}) {
            doTestOrderByReverseOptimizationWithNullsLast(salted, false, false, true);
            doTestOrderByReverseOptimizationWithNullsLast(salted, false, false, false);
        }
    }

    private void doTestOrderByReverseOptimizationWithNullsLast(boolean salted, boolean desc1,
                                                               boolean desc2, boolean desc3)
            throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            TABLE_NAMES.add(tableName);
            String sql = "CREATE TABLE " + tableName + " ( " +
                    "ORGANIZATION_ID VARCHAR," +
                    "CONTAINER_ID VARCHAR," +
                    "SCORE VARCHAR," +
                    "ENTITY_ID VARCHAR NOT NULL," +
                    "CONSTRAINT TEST_PK PRIMARY KEY ( " +
                    "ORGANIZATION_ID" + (desc1 ? " DESC" : "") + "," +
                    "CONTAINER_ID" + (desc2 ? " DESC" : "") + "," +
                    "SCORE" + (desc3 ? " DESC" : "") + "," +
                    "ENTITY_ID" +
                    ")) " + (salted ? "SALT_BUCKETS =4" : "split on('4')");
            conn.createStatement().execute(sql);

            for (int i = 1; i <= 6; i++) {
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES (null,'" + i + "','" + i + "','" + i +
                                "')");
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES (null,'" + i + "',null,'" + i + "')");
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES (null,null,'" + i + "','" + i + "')");
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES (null,null,null,'" + i + "')");
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES ('" + i + "','" + i + "','" + i +
                                "','" + i + "')");
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES ('" + i + "','" + i + "',null,'" + i +
                                "')");
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES ('" + i + "',null,'" + i + "','" + i +
                                "')");
                conn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES ('" + i + "',null,null,'" + i + "')");
            }
            conn.createStatement()
                    .execute("UPSERT INTO " + tableName + " VALUES (null,null,null,'66')");
            conn.commit();

            //groupBy orderPreserving orderBy asc asc

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST";
            QueryBuilder queryBuilder = new QueryBuilder()
                    .setSelectColumns(
                            Lists.newArrayList("ORGANIZATION_ID", "CONTAINER_ID"))
                    .setFullTableName(tableName)
                    .setGroupByClause("ORGANIZATION_ID, CONTAINER_ID")
                    .setOrderByClause(
                            "ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST");
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new Object[][]{{null, null}, {null, "1"}, {null, "2"}, {null, "3"}, {null, "4"},
                            {null, "5"}, {null, "6"}, {"1", null}, {"1", "1"}, {"2", null},
                            {"2", "2"}, {"3", null}, {"3", "3"}, {"4", null}, {"4", "4"},
                            {"5", null}, {"5", "5"}, {"6", null}, {"6", "6"}}, tableName);

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, "1"}, {null, "2"}, {null, "3"}, {null, "4"}, {null, "5"},
                            {null, "6"}, {null, null}, {"1", "1"}, {"1", null}, {"2", "2"},
                            {"2", null}, {"3", "3"}, {"3", null}, {"4", "4"}, {"4", null},
                            {"5", "5"}, {"5", null}, {"6", "6"}, {"6", null}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, "1"}, {null, "2"}, {null, "3"}, {null, "4"}, {null, "5"},
                            {null, "6"}, {null, null}, {"1", "1"}, {"1", null}, {"2", "2"},
                            {"2", null}, {"3", "3"}, {"3", null}, {"4", "4"}, {"4", null},
                            {"5", "5"}, {"5", null}, {"6", "6"}, {"6", null}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"1", "1"}, {"1", null}, {"2", "2"}, {"2", null}, {"3", "3"},
                            {"3", null}, {"4", "4"}, {"4", null}, {"5", "5"}, {"5", null},
                            {"6", "6"}, {"6", null}, {null, "1"}, {null, "2"}, {null, "3"},
                            {null, "4"}, {null, "5"}, {null, "6"}, {null, null}});

            //groupBy orderPreserving orderBy asc desc

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, null}, {null, "6"}, {null, "5"}, {null, "4"}, {null, "3"},
                            {null, "2"}, {null, "1"}, {"1", null}, {"1", "1"}, {"2", null},
                            {"2", "2"}, {"3", null}, {"3", "3"}, {"4", null}, {"4", "4"},
                            {"5", null}, {"5", "5"}, {"6", null}, {"6", "6"}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new Object[][]{{null, "6"}, {null, "5"}, {null, "4"}, {null, "3"}, {null, "2"},
                            {null, "1"}, {null, null}, {"1", "1"}, {"1", null}, {"2", "2"},
                            {"2", null}, {"3", "3"}, {"3", null}, {"4", "4"}, {"4", null},
                            {"5", "5"}, {"5", null}, {"6", "6"}, {"6", null}}, tableName);

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"1", null}, {"1", "1"}, {"2", null}, {"2", "2"}, {"3", null},
                            {"3", "3"}, {"4", null}, {"4", "4"}, {"5", null}, {"5", "5"},
                            {"6", null}, {"6", "6"}, {null, null}, {null, "6"}, {null, "5"},
                            {null, "4"}, {null, "3"}, {null, "2"}, {null, "1"}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"1", "1"}, {"1", null}, {"2", "2"}, {"2", null}, {"3", "3"},
                            {"3", null}, {"4", "4"}, {"4", null}, {"5", "5"}, {"5", null},
                            {"6", "6"}, {"6", null}, {null, "6"}, {null, "5"}, {null, "4"},
                            {null, "3"}, {null, "2"}, {null, "1"}, {null, null}});

            //groupBy orderPreserving orderBy desc asc

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, null}, {null, "1"}, {null, "2"}, {null, "3"}, {null, "4"},
                            {null, "5"}, {null, "6"}, {"6", null}, {"6", "6"}, {"5", null},
                            {"5", "5"}, {"4", null}, {"4", "4"}, {"3", null}, {"3", "3"},
                            {"2", null}, {"2", "2"}, {"1", null}, {"1", "1"}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, "1"}, {null, "2"}, {null, "3"}, {null, "4"}, {null, "5"},
                            {null, "6"}, {null, null}, {"6", "6"}, {"6", null}, {"5", "5"},
                            {"5", null}, {"4", "4"}, {"4", null}, {"3", "3"}, {"3", null},
                            {"2", "2"}, {"2", null}, {"1", "1"}, {"1", null}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"6", null}, {"6", "6"}, {"5", null}, {"5", "5"}, {"4", null},
                            {"4", "4"}, {"3", null}, {"3", "3"}, {"2", null}, {"2", "2"},
                            {"1", null}, {"1", "1"}, {null, null}, {null, "1"}, {null, "2"},
                            {null, "3"}, {null, "4"}, {null, "5"}, {null, "6"}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"6", "6"}, {"6", null}, {"5", "5"}, {"5", null}, {"4", "4"},
                            {"4", null}, {"3", "3"}, {"3", null}, {"2", "2"}, {"2", null},
                            {"1", "1"}, {"1", null}, {null, "1"}, {null, "2"}, {null, "3"},
                            {null, "4"}, {null, "5"}, {null, "6"}, {null, null}});

            //groupBy orderPreserving orderBy desc desc

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, null}, {null, "6"}, {null, "5"}, {null, "4"}, {null, "3"},
                            {null, "2"}, {null, "1"}, {"6", null}, {"6", "6"}, {"5", null},
                            {"5", "5"}, {"4", null}, {"4", "4"}, {"3", null}, {"3", "3"},
                            {"2", null}, {"2", "2"}, {"1", null}, {"1", "1"}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, "6"}, {null, "5"}, {null, "4"}, {null, "3"}, {null, "2"},
                            {null, "1"}, {null, null}, {"6", "6"}, {"6", null}, {"5", "5"},
                            {"5", null}, {"4", "4"}, {"4", null}, {"3", "3"}, {"3", null},
                            {"2", "2"}, {"2", null}, {"1", "1"}, {"1", null}});

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new Object[][]{{"6", null}, {"6", "6"}, {"5", null}, {"5", "5"}, {"4", null},
                            {"4", "4"}, {"3", null}, {"3", "3"}, {"2", null}, {"2", "2"},
                            {"1", null}, {"1", "1"}, {null, null}, {null, "6"}, {null, "5"},
                            {null, "4"}, {null, "3"}, {null, "2"}, {null, "1"}}, tableName);

//            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"6", "6"}, {"6", null}, {"5", "5"}, {"5", null}, {"4", "4"},
                            {"4", null}, {"3", "3"}, {"3", null}, {"2", "2"}, {"2", null},
                            {"1", "1"}, {"1", null}, {null, "6"}, {null, "5"}, {null, "4"},
                            {null, "3"}, {null, "2"}, {null, "1"}, {null, null}});

            //-----groupBy not orderPreserving--

            //groupBy not orderPreserving orderBy asc asc

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS FIRST";
            queryBuilder.setSelectColumns(
                            Lists.newArrayList("ORGANIZATION_ID", "SCORE"))
                    .setFullTableName(tableName)
                    .setGroupByClause("ORGANIZATION_ID, SCORE")
                    .setOrderByClause("ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, null}, {null, "1"}, {null, "2"}, {null, "3"}, {null, "4"},
                            {null, "5"}, {null, "6"}, {"1", null}, {"1", "1"}, {"2", null},
                            {"2", "2"}, {"3", null}, {"3", "3"}, {"4", null}, {"4", "4"},
                            {"5", null}, {"5", "5"}, {"6", null}, {"6", "6"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, "1"}, {null, "2"}, {null, "3"}, {null, "4"}, {null, "5"},
                            {null, "6"}, {null, null}, {"1", "1"}, {"1", null}, {"2", "2"},
                            {"2", null}, {"3", "3"}, {"3", null}, {"4", "4"}, {"4", null},
                            {"5", "5"}, {"5", null}, {"6", "6"}, {"6", null}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS FIRST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"1", null}, {"1", "1"}, {"2", null}, {"2", "2"}, {"3", null},
                            {"3", "3"}, {"4", null}, {"4", "4"}, {"5", null}, {"5", "5"},
                            {"6", null}, {"6", "6"}, {null, null}, {null, "1"}, {null, "2"},
                            {null, "3"}, {null, "4"}, {null, "5"}, {null, "6"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"1", "1"}, {"1", null}, {"2", "2"}, {"2", null}, {"3", "3"},
                            {"3", null}, {"4", "4"}, {"4", null}, {"5", "5"}, {"5", null},
                            {"6", "6"}, {"6", null}, {null, "1"}, {null, "2"}, {null, "3"},
                            {null, "4"}, {null, "5"}, {null, "6"}, {null, null}});

            //groupBy not orderPreserving orderBy asc desc

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, null}, {null, "6"}, {null, "5"}, {null, "4"}, {null, "3"},
                            {null, "2"}, {null, "1"}, {"1", null}, {"1", "1"}, {"2", null},
                            {"2", "2"}, {"3", null}, {"3", "3"}, {"4", null}, {"4", "4"},
                            {"5", null}, {"5", "5"}, {"6", null}, {"6", "6"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, "6"}, {null, "5"}, {null, "4"}, {null, "3"}, {null, "2"},
                            {null, "1"}, {null, null}, {"1", "1"}, {"1", null}, {"2", "2"},
                            {"2", null}, {"3", "3"}, {"3", null}, {"4", "4"}, {"4", null},
                            {"5", "5"}, {"5", null}, {"6", "6"}, {"6", null}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS FIRST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"1", null}, {"1", "1"}, {"2", null}, {"2", "2"}, {"3", null},
                            {"3", "3"}, {"4", null}, {"4", "4"}, {"5", null}, {"5", "5"},
                            {"6", null}, {"6", "6"}, {null, null}, {null, "6"}, {null, "5"},
                            {null, "4"}, {null, "3"}, {null, "2"}, {null, "1"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"1", "1"}, {"1", null}, {"2", "2"}, {"2", null}, {"3", "3"},
                            {"3", null}, {"4", "4"}, {"4", null}, {"5", "5"}, {"5", null},
                            {"6", "6"}, {"6", null}, {null, "6"}, {null, "5"}, {null, "4"},
                            {null, "3"}, {null, "2"}, {null, "1"}, {null, null}});

            //groupBy not orderPreserving orderBy desc asc

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, null}, {null, "1"}, {null, "2"}, {null, "3"}, {null, "4"},
                            {null, "5"}, {null, "6"}, {"6", null}, {"6", "6"}, {"5", null},
                            {"5", "5"}, {"4", null}, {"4", "4"}, {"3", null}, {"3", "3"},
                            {"2", null}, {"2", "2"}, {"1", null}, {"1", "1"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs,
                    new Object[][]{{null, "1"}, {null, "2"}, {null, "3"}, {null, "4"}, {null, "5"},
                            {null, "6"}, {null, null}, {"6", "6"}, {"6", null}, {"5", "5"},
                            {"5", null}, {"4", "4"}, {"4", null}, {"3", "3"}, {"3", null},
                            {"2", "2"}, {"2", null}, {"1", "1"}, {"1", null}}, tableName);

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS FIRST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"6", null}, {"6", "6"}, {"5", null}, {"5", "5"}, {"4", null},
                            {"4", "4"}, {"3", null}, {"3", "3"}, {"2", null}, {"2", "2"},
                            {"1", null}, {"1", "1"}, {null, null}, {null, "1"}, {null, "2"},
                            {null, "3"}, {null, "4"}, {null, "5"}, {null, "6"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"6", "6"}, {"6", null}, {"5", "5"}, {"5", null}, {"4", "4"},
                            {"4", null}, {"3", "3"}, {"3", null}, {"2", "2"}, {"2", null},
                            {"1", "1"}, {"1", null}, {null, "1"}, {null, "2"}, {null, "3"},
                            {null, "4"}, {null, "5"}, {null, "6"}, {null, null}});

            //groupBy not orderPreserving orderBy desc desc

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, null}, {null, "6"}, {null, "5"}, {null, "4"}, {null, "3"},
                            {null, "2"}, {null, "1"}, {"6", null}, {"6", "6"}, {"5", null},
                            {"5", "5"}, {"4", null}, {"4", "4"}, {"3", null}, {"3", "3"},
                            {"2", null}, {"2", "2"}, {"1", null}, {"1", "1"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS LAST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{null, "6"}, {null, "5"}, {null, "4"}, {null, "3"}, {null, "2"},
                            {null, "1"}, {null, null}, {"6", "6"}, {"6", null}, {"5", "5"},
                            {"5", null}, {"4", "4"}, {"4", null}, {"3", "3"}, {"3", null},
                            {"2", "2"}, {"2", null}, {"1", "1"}, {"1", null}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS FIRST";
            queryBuilder.setOrderByClause(
                    "ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"6", null}, {"6", "6"}, {"5", null}, {"5", "5"}, {"4", null},
                            {"4", "4"}, {"3", null}, {"3", "3"}, {"2", null}, {"2", "2"},
                            {"1", null}, {"1", "1"}, {null, null}, {null, "6"}, {null, "5"},
                            {null, "4"}, {null, "3"}, {null, "2"}, {null, "1"}});

//            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS LAST";
            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs,
                    new Object[][]{{"6", "6"}, {"6", null}, {"5", "5"}, {"5", null}, {"4", "4"},
                            {"4", null}, {"3", "3"}, {"3", null}, {"2", "2"}, {"2", null},
                            {"1", "1"}, {"1", null}, {null, "6"}, {null, "5"}, {null, "4"},
                            {null, "3"}, {null, "2"}, {null, "1"}, {null, null}});

            //-------test only one return column----------------------------------

//            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE ASC NULLS FIRST";
            queryBuilder.setSelectColumns(
                            Lists.newArrayList("SCORE"))
                    .setFullTableName(tableName)
                    .setGroupByClause("SCORE")
                    .setOrderByClause("SCORE ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{null}, {"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}});

//            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE ASC NULLS LAST";
            queryBuilder.setOrderByClause("SCORE ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSetWithRegionMoves(rs, new Object[][]{{"1"}, {"2"}, {"3"}, {"4"}, {"5"},
                    {"6"}, {null}}, tableName);

//            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE DESC NULLS FIRST";
            queryBuilder.setOrderByClause("SCORE DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{null}, {"6"}, {"5"}, {"4"}, {"3"}, {"2"}, {"1"}});

//            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE DESC NULLS LAST";
            queryBuilder.setOrderByClause("SCORE DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{"6"}, {"5"}, {"4"}, {"3"}, {"2"}, {"1"}, {null}});
        }
    }

    @Test
    public void testOrderByNullable() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String sql = "CREATE TABLE IF NOT EXISTS us_population (state CHAR(2) NOT NULL," +
                    "city VARCHAR NOT NULL, population BIGINT CONSTRAINT my_pk PRIMARY KEY" +
                    " (state, city))";
            conn.createStatement().execute(sql);
            TABLE_NAMES.add("US_POPULATION");

            sql = "select ORDINAL_POSITION from SYSTEM.CATALOG where TABLE_NAME = 'US_POPULATION'";
            ResultSet rs = conn.createStatement().executeQuery(sql);
            int expected = 0;
            while (rs.next()) {
                expected += 1;
            }

            QueryBuilder queryBuilder = new QueryBuilder()
                    .setSelectColumns(Lists.newArrayList("*"))
                    .setFullTableName("SYSTEM.CATALOG")
                    .setWhereClause("TABLE_NAME = 'US_POPULATION'")
                    .setOrderByClause("ORDINAL_POSITION");
            rs = executeQuery(conn, queryBuilder);
            int linesCount = 0;
            while (rs.next()) {
                linesCount += 1;
            }
            assertEquals(expected, linesCount);

            queryBuilder = new QueryBuilder()
                    .setSelectColumns(Lists.newArrayList("COLUMN_NAME"))
                    .setFullTableName("SYSTEM.CATALOG")
                    .setWhereClause("TABLE_NAME = 'US_POPULATION'")
                    .setOrderByClause("ORDINAL_POSITION");
            rs = executeQuery(conn, queryBuilder);
            linesCount = 0;
            while (rs.next()) {
                linesCount += 1;
            }
            assertEquals(expected, linesCount);
        }
    }

    @Test
    public void testPhoenix6999() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String descTableName = "TBL_" + generateUniqueName();

        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullDescTableName =
                SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, descTableName);
        TABLE_NAMES.add(fullTableName);
        TABLE_NAMES.add(fullDescTableName);

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            String ddl =
                    "CREATE TABLE " + fullTableName
                            + "(k1 varchar primary key, v1 varchar, v2 varchar)";
            stmt.execute(ddl);
            ddl =
                    "CREATE TABLE " + fullDescTableName
                            + "(k1 varchar primary key desc, v1 varchar, v2 varchar)";
            stmt.execute(ddl);
            stmt.execute("upsert into " + fullTableName + " values ('a','a','a')");
            stmt.execute("upsert into " + fullTableName + " values ('b','b','b')");
            stmt.execute("upsert into " + fullTableName + " values ('c','c','c')");
            stmt.execute("upsert into " + fullDescTableName + " values ('a','a','a')");
            stmt.execute("upsert into " + fullDescTableName + " values ('b','b','b')");
            stmt.execute("upsert into " + fullDescTableName + " values ('c','c','c')");
            conn.commit();

            String query = "SELECT  *  from " + fullTableName + " where k1='b' order by k1 asc";
            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));

            query = "SELECT  *  from " + fullTableName + " where k1='b' order by k1 desc";
            rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));

            query = "SELECT  *  from " + fullDescTableName + " where k1='b' order by k1 asc";
            rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));

            query = "SELECT  *  from " + fullDescTableName + " where k1='b' order by k1 desc";
            rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
        }
    }

}