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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.phoenix.util.TestUtil.assertResultSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public abstract class BaseAggregateIT extends ParallelStatsDisabledIT {

    private static void initData(Connection conn, String tableName) throws SQLException {
        conn.createStatement().execute("create table " + tableName +
                "   (id varchar not null primary key,\n" +
                "    uri varchar, appcpu integer)");
        insertRow(conn, tableName, "Report1", 10, 1);
        insertRow(conn, tableName, "Report2", 10, 2);
        insertRow(conn, tableName, "Report3", 30, 3);
        insertRow(conn, tableName, "Report4", 30, 4);
        insertRow(conn, tableName, "SOQL1", 10, 5);
        insertRow(conn, tableName, "SOQL2", 10, 6);
        insertRow(conn, tableName, "SOQL3", 30, 7);
        insertRow(conn, tableName, "SOQL4", 30, 8);
        conn.commit();
    }

    private static void insertRow(Connection conn, String tableName, String uri, int appcpu, int id) throws SQLException {
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO " + tableName + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, "id" + id);
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
    }

    @Test
    public void testDuplicateTrailingAggExpr() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.FALSE.toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();

        conn.createStatement().execute("create table " + tableName +
                "   (nam VARCHAR(20), address VARCHAR(20), id BIGINT "
                + "constraint my_pk primary key (id))");
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO " + tableName + "(nam, address, id) values (?,?,?)");
        statement.setString(1, "pulkit");
        statement.setString(2, "badaun");
        statement.setInt(3, 1);
        statement.executeUpdate();
        conn.commit();

        QueryBuilder queryBuilder = new QueryBuilder()
                .setDistinct(true)
                .setSelectExpression("'harshit' as TEST_COLUMN, trim(NAM), trim(NAM)")
                .setSelectExpressionColumns(Lists.newArrayList("NAM"))
                .setFullTableName(tableName);

        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals("harshit", rs.getString(1));
        assertEquals("pulkit", rs.getString(2));
        assertEquals("pulkit", rs.getString(3));
        conn.close();
    }

    @Test
    public void testExpressionInGroupBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "create table " + tableName + "(tgb_id integer NOT NULL,utc_date_epoch integer NOT NULL,tgb_name varchar(40),ack_success_count integer" +
                ",ack_success_one_ack_count integer, CONSTRAINT pk_tgb_counter PRIMARY KEY(tgb_id, utc_date_epoch))";

        createTestTable(getUrl(), ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES(?,?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setInt(2, 1000);
        stmt.setString(3, "aaa");
        stmt.setInt(4, 1);
        stmt.setInt(5, 1);
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setInt(2, 2000);
        stmt.setString(3, "bbb");
        stmt.setInt(4, 2);
        stmt.setInt(5, 2);
        stmt.execute();
        conn.commit();

        QueryBuilder queryBuilder = new QueryBuilder()
            .setSelectExpression("TGB_ID, TGB_NAME, (UTC_DATE_EPOCH/10)*10 AS UTC_EPOCH_HOUR,SUM(ACK_SUCCESS_COUNT + " +
                    "ACK_SUCCESS_ONE_ACK_COUNT) AS ACK_TX_SUM")
            .setSelectExpressionColumns(Lists.newArrayList("TGB_ID", "TGB_NAME",
                "UTC_DATE_EPOCH", "ACK_SUCCESS_COUNT", "ACK_SUCCESS_ONE_ACK_COUNT"))
            .setGroupByClause("TGB_ID, TGB_NAME, UTC_EPOCH_HOUR")
            .setFullTableName(tableName)
            .setOrderByClause("TGB_ID, UTC_EPOCH_HOUR");

        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals("aaa",rs.getString(2));
        assertEquals(1000,rs.getDouble(3), 1e-6);
        assertEquals(2,rs.getLong(4));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertEquals("bbb",rs.getString(2));
        assertEquals(2000,rs.getDouble(3), 1e-6);
        assertEquals(4,rs.getLong(4));
        assertFalse(rs.next());
        rs.close();
        conn.close();
    }
    
    @Test
    public void testBooleanInGroupBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(id varchar primary key,v1 boolean, v2 integer, v3 integer)";

        createTestTable(getUrl(), ddl);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + "(id,v2,v3) VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setInt(2, 1);
        stmt.setInt(3, 1);
        stmt.execute();
        stmt.close();
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?,?)");
        stmt.setString(1, "b");
        stmt.setBoolean(2, false);
        stmt.setInt(3, 2);
        stmt.setInt(4, 2);
        stmt.execute();
        stmt.setString(1, "c");
        stmt.setBoolean(2, true);
        stmt.setInt(3, 3);
        stmt.setInt(4, 3);
        stmt.execute();
        conn.commit();

        String[] gbs = {"V1,V2,V3","V1,V3,V2","V2,V1,V3"};
        for (String gb : gbs) {
            QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(Lists.newArrayList("V1", "V2", "V3"))
                .setFullTableName(tableName)
                .setGroupByClause(gb);

            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(false,rs.getBoolean("v1"));
            assertTrue(rs.wasNull());
            assertEquals(1,rs.getInt("v2"));
            assertEquals(1,rs.getInt("v3"));
            assertTrue(rs.next());
            assertEquals(false,rs.getBoolean("v1"));
            assertFalse(rs.wasNull());
            assertEquals(2,rs.getInt("v2"));
            assertEquals(2,rs.getInt("v3"));
            assertTrue(rs.next());
            assertEquals(true,rs.getBoolean("v1"));
            assertEquals(3,rs.getInt("v2"));
            assertEquals(3,rs.getInt("v3"));
            assertFalse(rs.next());
            rs.close();
        }
        conn.close();
    }
    
    @Test
    public void testScanUri() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        initData(conn, tableName);
        Statement stmt = conn.createStatement();
        QueryBuilder queryBuilder = new QueryBuilder()
            .setSelectColumns(Lists.newArrayList("URI"))
            .setFullTableName(tableName);

        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals("Report1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report3", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report4", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL3", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL4", rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testCount() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        initData(conn, tableName);
        Statement stmt = conn.createStatement();
        QueryBuilder queryBuilder = new QueryBuilder()
            .setSelectExpression("count(1)")
            .setFullTableName(tableName);
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals(8, rs.getLong(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testGroupByCase() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.FALSE.toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        initData(conn, tableName);
        QueryBuilder queryBuilder = new QueryBuilder()
            .setSelectExpression("CASE WHEN URI LIKE 'REPORT%' THEN 'REPORTS' ELSE 'OTHER' END CATEGORY, AVG(APPCPU)")
            .setSelectExpressionColumns(Lists.newArrayList("URI", "APPCPU"))
            .setFullTableName(tableName)
            .setGroupByClause("CATEGORY");
        executeQuery(conn, queryBuilder);

        queryBuilder.setSelectExpression(
                "CASE URI WHEN 'REPORT%' THEN 'REPORTS' ELSE 'OTHER' END CATEGORY, AVG(APPCPU)")
            .setSelectExpressionColumns(Lists.newArrayList("URI", "APPCPU"))
            .setFullTableName(tableName)
            .setGroupByClause("APPCPU, CATEGORY");
        executeQuery(conn, queryBuilder);

        queryBuilder.setSelectExpression(
                "CASE URI WHEN 'Report%' THEN 'Reports' ELSE 'Other' END CATEGORY, AVG(APPCPU)")
            .setSelectColumns(Lists.newArrayList("URI", "APPCPU"))
            .setFullTableName(tableName)
            .setGroupByClause("AVG(APPCPU), CATEGORY");
        executeQueryThrowsException(conn, queryBuilder, "Aggregate expressions may not be used in GROUP BY", "");
        conn.close();
    }


    @Test
    public void testGroupByArray() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        String tableName = generateUniqueName();
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
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('1', 'val', 100, 'a', ARRAY ['b'], 1, 2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('2', 'val', 100, 'a', ARRAY ['b'], 3, 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('3', 'val', 100, 'a', ARRAY ['b','c'], 5, 6)");
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

        conn.createStatement().execute("CREATE TABLE " + tableName + "(ORGANIZATION_ID char(15) not null, \n" + 
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
            .setSelectExpressionColumns(Lists.newArrayList("EXTERNAL_DATASOURCE_KEY", "MATCH_STATUS",
                "JOURNEY_ID", "DATASOURCE", "ORGANIZATION_ID"))
            .setWhereClause(
            "JOURNEY_ID='333334444455555' AND DATASOURCE=0 AND MATCH_STATUS <= 1 and ORGANIZATION_ID='000001111122222'")
            .setFullTableName(tableName)
            .setGroupByClause("MATCH_STATUS, EXTERNAL_DATASOURCE_KEY")
            .setHavingClause("COUNT(1) > 1");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals(2,rs.getLong(1));
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
        assertEquals(" ['000001111122222','333334444455555',0,*] - ['000001111122222','333334444455555',0,1]",
            explainPlanAttributes.getKeyRanges());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
            explainPlanAttributes.getServerWhereFilter());
        assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [MATCH_STATUS, EXTERNAL_DATASOURCE_KEY]",
            explainPlanAttributes.getServerAggregate());
        assertEquals("COUNT(1) > 1", explainPlanAttributes.getClientFilterBy());
    }
    
    @Test
    public void testGroupByOrderPreservingDescSort() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 char(1) not null, k2 char(1) not null," +
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

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName + " (k1 char(1) not null, k2 integer not null, constraint pk primary key (k1,k2)) split on (?,?,?)");
        stmt.setBytes(1, ByteUtil.concat(PChar.INSTANCE.toBytes("a"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(2, ByteUtil.concat(PChar.INSTANCE.toBytes("j"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(3, ByteUtil.concat(PChar.INSTANCE.toBytes("n"), PInteger.INSTANCE.toBytes(3)));
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
        assertEquals("n", rs.getString(1));
        assertEquals(10, rs.getLong(2));
        assertTrue(rs.next());
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
        assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
            explainPlanAttributes.getServerWhereFilter());
        assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [K1]",
            explainPlanAttributes.getServerAggregate());
    }

    @Test
    public void testAvgGroupByOrderPreservingWithNoStats() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        initAvgGroupTable(conn, tableName, "");
        testAvgGroupByOrderPreserving(conn, tableName, 4);
    }
    
    protected void initAvgGroupTable(Connection conn, String tableName, String tableProps) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName + " (k1 char(1) not null, k2 integer not null, constraint pk primary key (k1,k2)) " + tableProps + " split on (?,?,?)");
        stmt.setBytes(1, ByteUtil.concat(PChar.INSTANCE.toBytes("a"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(2, ByteUtil.concat(PChar.INSTANCE.toBytes("j"), PInteger.INSTANCE.toBytes(3)));
        stmt.setBytes(3, ByteUtil.concat(PChar.INSTANCE.toBytes("n"), PInteger.INSTANCE.toBytes(3)));
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
    
    protected void testAvgGroupByOrderPreserving(Connection conn, String tableName, int nGuidePosts) throws SQLException, IOException {
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
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getDouble(2), 1e-6);
        assertTrue(rs.next());
        assertEquals("j", rs.getString(1));
        assertEquals(4, rs.getDouble(2), 1e-6);
        assertTrue(rs.next());
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
        assertEquals(nGuidePosts, splits.size());
    }
    
    @Test
    public void testDistinctGroupByBug3452WithoutMultiTenant() throws Exception {
        doTestDistinctGroupByBug3452("");
    }

    @Test
    public void testDistinctGroupByBug3452WithMultiTenant() throws Exception {
        doTestDistinctGroupByBug3452("VERSIONS=1, MULTI_TENANT=TRUE, REPLICATION_SCOPE=1, TTL=31536000");
    }

    private void doTestDistinctGroupByBug3452(String options) throws Exception {
        Connection conn=null;
        try {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);

            String tableName=generateUniqueName();
            conn.createStatement().execute("DROP TABLE if exists "+tableName);
            String sql="CREATE TABLE "+ tableName +" ( "+
                    "ORGANIZATION_ID CHAR(15) NOT NULL,"+
                    "CONTAINER_ID CHAR(15) NOT NULL,"+
                    "ENTITY_ID CHAR(15) NOT NULL,"+
                    "SCORE DOUBLE,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "ORGANIZATION_ID,"+
                    "CONTAINER_ID,"+
                    "ENTITY_ID"+
                    ")) "+options;
            conn.createStatement().execute(sql);

            String indexTableName=generateUniqueName();
            conn.createStatement().execute("DROP INDEX IF EXISTS "+indexTableName+" ON "+tableName);
            conn.createStatement().execute("CREATE INDEX "+indexTableName+" ON "+tableName+" (CONTAINER_ID, SCORE DESC, ENTITY_ID DESC)");

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('org1','container1','entityId6',1.1)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('org1','container1','entityId5',1.2)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('org1','container1','entityId4',1.3)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('org1','container1','entityId3',1.4)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('org1','container1','entityId2',1.5)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('org1','container1','entityId1',1.6)");
            conn.commit();

            QueryBuilder queryBuilder = new QueryBuilder()
                .setDistinct(true)
                .setSelectColumns(Lists.newArrayList("ENTITY_ID", "SCORE", "ORGANIZATION_ID", "CONTAINER_ID"))
                .setFullTableName(tableName)
                .setWhereClause("ORGANIZATION_ID = 'org1' AND CONTAINER_ID = 'container1'")
                .setOrderByClause("SCORE DESC");
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).equals("entityId1"));
            assertEquals(rs.getDouble(2),1.6,0.0001);

            assertTrue(rs.next());
            assertTrue(rs.getString(1).equals("entityId2"));
            assertEquals(rs.getDouble(2),1.5,0.0001);

            assertTrue(rs.next());
            assertTrue(rs.getString(1).equals("entityId3"));
            assertEquals(rs.getDouble(2),1.4,0.0001);

            assertTrue(rs.next());
            assertTrue(rs.getString(1).equals("entityId4"));
            assertEquals(rs.getDouble(2),1.3,0.0001);

            assertTrue(rs.next());
            assertTrue(rs.getString(1).equals("entityId5"));
            assertEquals(rs.getDouble(2),1.2,0.0001);

            assertTrue(rs.next());
            assertTrue(rs.getString(1).equals("entityId6"));
            assertEquals(rs.getDouble(2),1.1,0.0001);
            assertTrue(!rs.next());
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testGroupByOrderByDescBug3451() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName=generateUniqueName();
            String sql="CREATE TABLE " + tableName + " (\n" + 
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
            String indexName=generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(ORGANIZATION_ID,CONTAINER_ID, SCORE DESC, ENTITY_ID DESC)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('org2','container2','entityId6',1.1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('org2','container1','entityId5',1.2)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('org2','container2','entityId4',1.3)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('org2','container1','entityId5',1.2)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('org2','container1','entityId3',1.4)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('org2','container3','entityId7',1.35)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('org2','container3','entityId8',1.45)");
            conn.commit();
            QueryBuilder queryBuilder = new QueryBuilder()
                .setDistinct(true)
                .setSelectColumns(Lists.newArrayList("ENTITY_ID", "SCORE", "ORGANIZATION_ID", "CONTAINER_ID"))
                .setFullTableName(tableName)
                .setWhereClause(
                    "ORGANIZATION_ID = 'org2' AND CONTAINER_ID IN ('container1','container2','container3')")
                .setOrderByClause("SCORE DESC")
                .setLimit(2);
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals("entityId8", rs.getString(1));
            assertEquals(1.45, rs.getDouble(2),0.001);
            assertTrue(rs.next());
            assertEquals("entityId3", rs.getString(1));
            assertEquals(1.4, rs.getDouble(2),0.001);
            assertFalse(rs.next());

            String expectedPhoenixPlan = "";
            validateQueryPlan(conn, queryBuilder, expectedPhoenixPlan, null);
       }
    }
    
    @Test
    public void testGroupByDescColumnWithNullsLastBug3452() throws Exception {

        Connection conn=null;
        try
        {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);

            String tableName=generateUniqueName();
            String sql="CREATE TABLE "+tableName+" ( "+
                    "ORGANIZATION_ID VARCHAR,"+
                    "CONTAINER_ID VARCHAR,"+
                    "ENTITY_ID VARCHAR NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "ORGANIZATION_ID DESC,"+
                    "CONTAINER_ID DESC,"+
                    "ENTITY_ID"+
                    "))";
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a',null,'11')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,'2','22')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('c','3','33')");
            conn.commit();

            //-----ORGANIZATION_ID

            QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                        Lists.newArrayList("CONTAINER_ID", "ORGANIZATION_ID"))
                .setFullTableName(tableName)
                .setGroupByClause("ORGANIZATION_ID, CONTAINER_ID")
                .setOrderByClause("ORGANIZATION_ID ASC NULLS FIRST");
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{null,"a"},{"3","c"},});

            queryBuilder.setOrderByClause("ORGANIZATION_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null}});

            //----CONTAINER_ID

            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{"2",null},{"3","c"}});

            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"}});

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,null,'44')");
            conn.commit();

            //-----ORGANIZATION_ID ASC  CONTAINER_ID ASC

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{null,"a"},{"3","c"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{null,"a"},{"3","c"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{null,null},{"2",null}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID ASC  CONTAINER_ID DESC

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{null,"a"},{"3","c"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{null,"a"},{"3","c"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{null,null},{"2",null}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID DESC  CONTAINER_ID ASC

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{"3","c"},{null,"a"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{"3","c"},{null,"a"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{null,null},{"2",null}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID DESC  CONTAINER_ID DESC

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{"3","c"},{null,"a"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{"3","c"},{null,"a"}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{null,null},{"2",null}});

            queryBuilder.setOrderByClause("ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null},{null,null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID ASC

            queryBuilder.setOrderByClause("CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"2",null},{"3","c"}});

            queryBuilder.setOrderByClause("CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"2",null},{"3","c"}});

            queryBuilder.setOrderByClause("CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,null},{null,"a"}});

            queryBuilder.setOrderByClause("CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"},{null,null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID DESC

            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"2",null},{"3","c"}});

            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"2",null},{"3","c"}});

            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,null},{null,"a"}});

            queryBuilder.setOrderByClause("CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"},{null,null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID ASC

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"3","c"},{"2",null}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"3","c"},{"2",null}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,null},{null,"a"}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"},{null,null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID DESC

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"3","c"},{"2",null}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"3","c"},{"2",null}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,null},{null,"a"}});

            queryBuilder.setOrderByClause("CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"},{null,null}});
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testCountNullInEncodedNonEmptyKeyValueCF() throws Exception {
        testCountNullInNonEmptyKeyValueCF(1);
    }
    
    @Test
    public void testCountNullInNonEncodedNonEmptyKeyValueCF() throws Exception {
        testCountNullInNonEmptyKeyValueCF(0);
    }

    protected abstract void testCountNullInNonEmptyKeyValueCF(int columnEncodedBytes) throws Exception;

    @Test
    public void testGroupByOrderMatchPkColumnOrderBug4690() throws Exception {
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(false, false);
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(false, true);
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(true, false);
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(true, true);
    }

    private void doTestGroupByOrderMatchPkColumnOrderBug4690(boolean desc ,boolean salted) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            String tableName = generateUniqueName();
            String sql = "create table " + tableName + "( "+
                    " pk1 integer not null , " +
                    " pk2 integer not null, " +
                    " pk3 integer not null," +
                    " pk4 integer not null,"+
                    " v integer, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                       "pk1 "+(desc ? "desc" : "")+", "+
                       "pk2 "+(desc ? "desc" : "")+", "+
                       "pk3 "+(desc ? "desc" : "")+", "+
                       "pk4 "+(desc ? "desc" : "")+
                    " )) "+(salted ? "SALT_BUCKETS =4" : "split on(2)");
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,8,10,20,30)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,8,11,21,31)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,9,5 ,22,32)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,9,6 ,12,33)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,9,6 ,13,34)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,9,7 ,8,35)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2,3,15,25,35)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2,7,16,26,36)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2,7,17,27,37)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (3,2,18,28,38)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (3,2,19,29,39)");
            conn.commit();

            QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("PK2,PK1,COUNT(V)")
                .setSelectExpressionColumns(Lists.newArrayList("PK1", "PK2", "V"))
                .setFullTableName(tableName)
                .setGroupByClause("PK2, PK1")
                .setOrderByClause("PK2, PK1");
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{2,3,2L},{3,2,1L},{7,2,2L},{8,1,2L},{9,1,4L}});

            queryBuilder.setSelectExpression("PK1, PK2, COUNT(V)");
            queryBuilder.setOrderByClause("PK1, PK2");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{1,8,2L},{1,9,4L},{2,3,1L},{2,7,2L},{3,2,2L}});

            queryBuilder.setSelectExpression("PK2,PK1,COUNT(V)");
            queryBuilder.setOrderByClause("PK2 DESC,PK1 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{9,1,4L},{8,1,2L},{7,2,2L},{3,2,1L},{2,3,2L}});

            queryBuilder.setSelectExpression("PK1,PK2,COUNT(V)");
            queryBuilder.setOrderByClause("PK1 DESC,PK2 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{3,2,2L},{2,7,2L},{2,3,1L},{1,9,4L},{1,8,2L}});


            queryBuilder.setSelectExpression("PK3,PK2,COUNT(V)");
            queryBuilder.setSelectExpressionColumns(Lists.newArrayList("PK1", "PK2", "PK3", "V"));
            queryBuilder.setFullTableName(tableName);
            queryBuilder.setGroupByClause("PK3,PK2");
            queryBuilder.setOrderByClause("PK3,PK2");
            queryBuilder.setWhereClause("PK1=1");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{5,9,1L},{6,9,2L},{7,9,1L},{10,8,1L},{11,8,1L}});

            queryBuilder.setSelectExpression("PK2,PK3,COUNT(V)");
            queryBuilder.setOrderByClause("PK2,PK3");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{8,10,1L},{8,11,1L},{9,5,1L},{9,6,2L},{9,7,1L}});

            queryBuilder.setSelectExpression("PK3,PK2,COUNT(V)");
            queryBuilder.setOrderByClause("PK3 DESC,PK2 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{11,8,1L},{10,8,1L},{7,9,1L},{6,9,2L},{5,9,1L}});

            queryBuilder.setSelectExpression("PK2,PK3,COUNT(V)");
            queryBuilder.setOrderByClause("PK2 DESC,PK3 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{9,7,1L},{9,6,2L},{9,5,1L},{8,11,1L},{8,10,1L}});


            queryBuilder.setSelectExpression("PK4,PK3,PK1,COUNT(V)");
            queryBuilder.setSelectExpressionColumns(Lists.newArrayList("PK1", "PK2", "PK3", "PK4", "V"));
            queryBuilder.setFullTableName(tableName);
            queryBuilder.setWhereClause("PK2=9 ");
            queryBuilder.setGroupByClause("PK4,PK3,PK1");
            queryBuilder.setOrderByClause("PK4,PK3,PK1");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{8,7,1,1L},{12,6,1,1L},{13,6,1,1L},{22,5,1,1L}});

            queryBuilder.setSelectExpression("PK1,PK3,PK4,COUNT(V)");
            queryBuilder.setOrderByClause("PK1,PK3,PK4");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{1,5,22,1L},{1,6,12,1L},{1,6,13,1L},{1,7,8,1L}});

            queryBuilder.setSelectExpression("PK4,PK3,PK1,COUNT(V)");
            queryBuilder.setOrderByClause("PK4 DESC,PK3 DESC,PK1 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{22,5,1,1L},{13,6,1,1L},{12,6,1,1L},{8,7,1,1L}});

            queryBuilder.setSelectExpression("PK1,PK3,PK4,COUNT(V)");
            queryBuilder.setOrderByClause("PK1 DESC,PK3 DESC,PK4 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][]{{1,7,8,1L},{1,6,13,1L},{1,6,12,1L},{1,5,22,1L}});
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}
