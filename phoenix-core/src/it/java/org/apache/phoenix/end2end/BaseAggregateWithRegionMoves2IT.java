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
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class BaseAggregateWithRegionMoves2IT extends ParallelStatsDisabledWithRegionMovesIT {

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
    public void testGroupByArray() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);
        conn.createStatement().execute("CREATE TABLE " + tableName + "(\n" +
                "  a VARCHAR NOT NULL,\n" +
                "  b VARCHAR,\n" +
                "  c INTEGER,\n" +
                "  d VARCHAR,\n" +
                "  e VARCHAR ARRAY,\n" +
                "  f BIGINT,\n" +
                "  g BIGINT,\n" +
                "  CONSTRAINT pk PRIMARY KEY(a)\n" +
                ")");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " VALUES('1', 'val', 100, 'a', ARRAY ['b'], 1, 2)");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " VALUES('2', 'val', 100, 'a', ARRAY ['b'], 3, 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " VALUES('3', 'val', 100, 'a', ARRAY ['b','c'], 5, 6)");
        conn.commit();

        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("C, SUM(F + G) AS SUMONE, D, E")
                .setSelectExpressionColumns(Lists.newArrayList("A", "B", "C", "F", "G", "D", "E"))
                .setWhereClause("B = 'val' AND A IN ('1','2','3')")
                .setFullTableName(tableName)
                .setGroupByClause("C, D, E")
                .setOrderByClause("SUMONE desc");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals(100, rs.getInt(1));
        assertEquals(11, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertEquals(10, rs.getLong(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testGroupByOrderPreserving() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.FALSE.toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);

        conn.createStatement()
                .execute("CREATE TABLE " + tableName + "(ORGANIZATION_ID char(15) not null, \n" +
                        "JOURNEY_ID char(15) not null, \n" +
                        "DATASOURCE SMALLINT not null, \n" +
                        "MATCH_STATUS TINYINT not null, \n" +
                        "EXTERNAL_DATASOURCE_KEY varchar(30), \n" +
                        "ENTITY_ID char(15) not null, \n" +
                        "CONSTRAINT PK PRIMARY KEY (\n" +
                        "    ORGANIZATION_ID, \n" +
                        "    JOURNEY_ID, \n" +
                        "    DATASOURCE, \n" +
                        "    MATCH_STATUS,\n" +
                        "    EXTERNAL_DATASOURCE_KEY,\n" +
                        "    ENTITY_ID))");
        conn.createStatement().execute("UPSERT INTO " + tableName
                + " VALUES('000001111122222', '333334444455555', 0, 0, 'abc', '666667777788888')");
        conn.createStatement().execute("UPSERT INTO " + tableName
                + " VALUES('000001111122222', '333334444455555', 0, 0, 'abcd', '666667777788889')");
        conn.createStatement().execute("UPSERT INTO " + tableName
                + " VALUES('000001111122222', '333334444455555', 0, 0, 'abc', '666667777788899')");
        conn.commit();
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("COUNT(1), EXTERNAL_DATASOURCE_KEY As DUP_COUNT")
                .setSelectExpressionColumns(
                        Lists.newArrayList("EXTERNAL_DATASOURCE_KEY", "MATCH_STATUS",
                                "JOURNEY_ID", "DATASOURCE", "ORGANIZATION_ID"))
                .setWhereClause(
                        "JOURNEY_ID='333334444455555' AND DATASOURCE=0 AND MATCH_STATUS <= 1 and ORGANIZATION_ID='000001111122222'")
                .setFullTableName(tableName)
                .setGroupByClause("MATCH_STATUS, EXTERNAL_DATASOURCE_KEY")
                .setHavingClause("COUNT(1) > 1");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals(2, rs.getLong(1));
        assertEquals("abc", rs.getString(2));
        assertFalse(rs.next());

        ExplainPlan plan = conn.prepareStatement(queryBuilder.build())
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("RANGE SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals(
                " ['000001111122222','333334444455555',0,*] - ['000001111122222','333334444455555',0,1]",
                explainPlanAttributes.getKeyRanges());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals(
                "SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [MATCH_STATUS, EXTERNAL_DATASOURCE_KEY]",
                explainPlanAttributes.getServerAggregate());
        assertEquals("COUNT(1) > 1", explainPlanAttributes.getClientFilterBy());
    }

    @Test
    public void testGroupByOrderPreservingDescSort() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName + " CASCADE");
        conn.createStatement().execute(
                "CREATE TABLE " + tableName + " (k1 char(1) not null, k2 char(1) not null," +
                        " constraint pk primary key (k1,k2)) split on ('ac','jc','nc')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 'a')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 'b')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 'c')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 'd')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 'a')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 'b')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 'c')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 'd')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 'a')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 'b')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 'c')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 'd')");
        conn.commit();
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("K1,COUNT(*)")
                .setSelectColumns(Lists.newArrayList("K1"))
                .setFullTableName(tableName)
                .setGroupByClause("K1")
                .setOrderByClause("K1 DESC");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals("n", rs.getString(1));
        assertEquals(4, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("j", rs.getString(1));
        assertEquals(4, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals(4, rs.getLong(2));
        assertFalse(rs.next());
        ExplainPlan plan = conn.prepareStatement(queryBuilder.build())
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("REVERSE", explainPlanAttributes.getClientSortedBy());
        assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [K1]",
                explainPlanAttributes.getServerAggregate());
    }

    @Test
    public void testSumGroupByOrderPreservingDesc() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName +
                " (k1 char(1) not null, k2 integer not null, constraint pk primary key (k1,k2)) split on (?,?,?)");
        stmt.setBytes(1,
                ByteUtil.concat(PChar.INSTANCE.toBytes("a"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(2,
                ByteUtil.concat(PChar.INSTANCE.toBytes("j"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(3,
                ByteUtil.concat(PChar.INSTANCE.toBytes("n"), PInteger.INSTANCE.toBytes(3)));
        stmt.execute();
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('b', 5)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 4)");
        conn.commit();
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("K1,SUM(K2)")
                .setSelectExpressionColumns(Lists.newArrayList("K1", "K2"))
                .setFullTableName(tableName)
                .setGroupByClause("K1")
                .setOrderByClause("K1 DESC");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("n", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("j", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getLong(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("a", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertFalse(rs.next());
        ExplainPlan plan = conn.prepareStatement(queryBuilder.build())
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("REVERSE", explainPlanAttributes.getClientSortedBy());
        assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [K1]",
                explainPlanAttributes.getServerAggregate());
    }

    @Test
    public void testSumGroupByOrderPreservingDescWithoutSplit() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName +
                " (k1 char(1) not null, k2 integer not null, constraint pk primary key (k1,k2))");
        stmt.execute();
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('b', 5)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 4)");
        conn.commit();
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("K1,SUM(K2)")
                .setSelectExpressionColumns(Lists.newArrayList("K1", "K2"))
                .setFullTableName(tableName)
                .setWhereClause("K1 >= 'a' AND K1 < 'o'")
                .setGroupByClause("K1")
                .setOrderByClause("K1 DESC");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("n", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("j", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertFalse(rs.next());
        ExplainPlan plan = conn.prepareStatement(queryBuilder.build())
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("REVERSE", explainPlanAttributes.getClientSortedBy());
        assertEquals("RANGE SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
    }

    @Test
    public void testSumGroupByOrderPreservingAsc() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName +
                " (k1 char(1) not null, k2 integer not null, constraint pk primary key (k1,k2)) split on (?,?,?)");
        stmt.setBytes(1,
                ByteUtil.concat(PChar.INSTANCE.toBytes("a"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(2,
                ByteUtil.concat(PChar.INSTANCE.toBytes("j"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(3,
                ByteUtil.concat(PChar.INSTANCE.toBytes("n"), PInteger.INSTANCE.toBytes(3)));
        stmt.execute();
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('b', 5)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 4)");
        conn.commit();
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("K1,SUM(K2)")
                .setSelectExpressionColumns(Lists.newArrayList("K1", "K2"))
                .setFullTableName(tableName)
                .setGroupByClause("K1")
                .setOrderByClause("K1 ASC");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("a", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getLong(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("j", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("n", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSumGroupByOrderPreservingAscWithoutSplits() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        TABLE_NAMES.add(tableName);

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName +
                " (k1 char(1) not null, k2 integer not null, constraint pk primary key (k1,k2))");
        stmt.execute();
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('b', 5)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 4)");
        conn.commit();
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("K1,SUM(K2)")
                .setSelectExpressionColumns(Lists.newArrayList("K1", "K2"))
                .setFullTableName(tableName)
                .setGroupByClause("K1")
                .setOrderByClause("K1 ASC");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("a", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getLong(2));
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("j", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("n", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertFalse(rs.next());
    }

    @Test
    public void testAvgGroupByOrderPreservingWithNoStats() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        initAvgGroupTable(conn, tableName, "");
        testAvgGroupByOrderPreserving(conn, tableName, 4);
    }

    protected void initAvgGroupTable(Connection conn, String tableName, String tableProps)
            throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName +
                " (k1 char(1) not null, k2 integer not null, constraint pk primary key (k1,k2)) " +
                tableProps + " split on (?,?,?)");
        TABLE_NAMES.add(tableName);
        stmt.setBytes(1,
                ByteUtil.concat(PChar.INSTANCE.toBytes("a"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(2,
                ByteUtil.concat(PChar.INSTANCE.toBytes("j"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(3,
                ByteUtil.concat(PChar.INSTANCE.toBytes("n"), PInteger.INSTANCE.toBytes(3)));
        stmt.execute();
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a', 6)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('b', 5)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('j', 10)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('n', 2)");
        conn.commit();
    }

    protected void testAvgGroupByOrderPreserving(Connection conn, String tableName, int splitSize)
            throws SQLException, IOException {
        TABLE_NAMES.add(tableName);
        QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("K1, AVG(K2)")
                .setSelectExpressionColumns(Lists.newArrayList("K1", "K2"))
                .setFullTableName(tableName)
                .setGroupByClause("K1")
                .setOrderByClause("K1");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals(3, rs.getDouble(2), 1e-6);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getDouble(2), 1e-6);
        assertTrue(rs.next());
        assertEquals("j", rs.getString(1));
        assertEquals(4, rs.getDouble(2), 1e-6);
        assertTrue(rs.next());
        moveRegionsOfTable(tableName);
        assertEquals("n", rs.getString(1));
        assertEquals(2, rs.getDouble(2), 1e-6);
        assertFalse(rs.next());
        ExplainPlan plan = conn.prepareStatement(queryBuilder.build())
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [K1]",
                explainPlanAttributes.getServerAggregate());
        TestUtil.analyzeTable(conn, tableName);
        List<KeyRange> splits = TestUtil.getAllSplits(conn, tableName);
        // nGuideposts when stats are enabled, 4 when disabled
        assertEquals(splitSize, splits.size());
    }

    @Test
    public void testGroupByOrderByDescBug3451() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            String sql = "CREATE TABLE " + tableName + " (\n" +
                    "            ORGANIZATION_ID CHAR(15) NOT NULL,\n" +
                    "            CONTAINER_ID CHAR(15) NOT NULL,\n" +
                    "            ENTITY_ID CHAR(15) NOT NULL,\n" +
                    "            SCORE DOUBLE,\n" +
                    "            CONSTRAINT TEST_PK PRIMARY KEY (\n" +
                    "               ORGANIZATION_ID,\n" +
                    "               CONTAINER_ID,\n" +
                    "               ENTITY_ID\n" +
                    "             )\n" +
                    "         )";
            conn.createStatement().execute(sql);
            String indexName = generateUniqueName();
            TABLE_NAMES.add(tableName);
            TABLE_NAMES.add(indexName);
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName +
                    "(ORGANIZATION_ID,CONTAINER_ID, SCORE DESC, ENTITY_ID DESC)");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('org2','container2','entityId6',1.1)");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('org2','container1','entityId5',1.2)");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('org2','container2','entityId4',1.3)");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('org2','container1','entityId5',1.2)");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('org2','container1','entityId3',1.4)");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('org2','container3','entityId7',1.35)");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " VALUES ('org2','container3','entityId8',1.45)");
            conn.commit();
            QueryBuilder queryBuilder = new QueryBuilder()
                    .setDistinct(true)
                    .setSelectColumns(Lists.newArrayList("ENTITY_ID", "SCORE", "ORGANIZATION_ID",
                            "CONTAINER_ID"))
                    .setFullTableName(tableName)
                    .setWhereClause(
                            "ORGANIZATION_ID = 'org2' AND CONTAINER_ID IN ('container1','container2','container3')")
                    .setOrderByClause("SCORE DESC")
                    .setLimit(2);
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            assertEquals("entityId8", rs.getString(1));
            assertEquals(1.45, rs.getDouble(2), 0.001);
            assertTrue(rs.next());
            assertEquals("entityId3", rs.getString(1));
            assertEquals(1.4, rs.getDouble(2), 0.001);
            assertFalse(rs.next());

            String expectedPhoenixPlan = "";
            validateQueryPlan(conn, queryBuilder, expectedPhoenixPlan, null);
        }
    }

    protected static void assertResultSet(ResultSet rs, Object[][] rows, String tableName)
            throws Exception {
        boolean switchToRegionMove = true;
        for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            assertTrue("rowIndex:[" + rowIndex + "] rs.next error!", rs.next());
            if (switchToRegionMove) {
                moveRegionsOfTable(tableName);
                switchToRegionMove = false;
            } else {
                switchToRegionMove = true;
            }
            for (int columnIndex = 1; columnIndex <= rows[rowIndex].length; columnIndex++) {
                Object realValue = rs.getObject(columnIndex);
                Object expectedValue = rows[rowIndex][columnIndex - 1];
                if (realValue == null) {
                    assertNull("rowIndex:[" + rowIndex + "],columnIndex:[" + columnIndex + "]",
                            expectedValue);
                } else {
                    assertEquals("rowIndex:[" + rowIndex + "],columnIndex:[" + columnIndex + "]",
                            expectedValue,
                            realValue
                    );
                }
            }
        }
        assertFalse(rs.next());
    }
}
