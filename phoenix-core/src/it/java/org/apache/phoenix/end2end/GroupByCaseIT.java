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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class GroupByCaseIT extends ParallelStatsDisabledIT {
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
        conn.close();
    }

    private static void insertRow(Connection conn, String tableName, String uri, int appcpu, int id) throws SQLException {
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO " + tableName + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, "id" + id);
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
    }

    @Test
    public void testExpressionInGroupBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(tgb_id integer NOT NULL,utc_date_epoch integer NOT NULL,tgb_name varchar(40),ack_success_count integer" +
                ",ack_success_one_ack_count integer, CONSTRAINT pk_tgb_counter PRIMARY KEY(tgb_id, utc_date_epoch))";
        String query = "SELECT tgb_id, tgb_name, (utc_date_epoch/10)*10 AS utc_epoch_hour,SUM(ack_success_count + ack_success_one_ack_count) AS ack_tx_sum" +
                " FROM " + tableName + " GROUP BY tgb_id, tgb_name, utc_epoch_hour";

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

        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals("aaa",rs.getString(2));
        assertEquals(1000,rs.getInt(3));
        assertEquals(2,rs.getInt(4));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertEquals("bbb",rs.getString(2));
        assertEquals(2000,rs.getInt(3));
        assertEquals(4,rs.getInt(4));
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

        String[] gbs = {"v1,v2,v3","v1,v3,v2","v2,v1,v3"};
        for (String gb : gbs) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT v1, v2, v3 from " + tableName + " group by " + gb);
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
        ResultSet rs = stmt.executeQuery("select uri from " + tableName);
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
        ResultSet rs = stmt.executeQuery("select count(1) from " + tableName);
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testGroupByCase() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String groupBy1 = "select " +
                "case when uri LIKE 'Report%' then 'Reports' else 'Other' END category" +
                ", avg(appcpu) from " + tableName +
                " group by category";

        String groupBy2 = "select " +
                "case uri when 'Report%' then 'Reports' else 'Other' END category" +
                ", avg(appcpu) from " + tableName +
                " group by appcpu, category";
        
        String groupBy3 = "select " +
                "case uri when 'Report%' then 'Reports' else 'Other' END category" +
                ", avg(appcpu) from " + tableName +
                " group by avg(appcpu), category";
        initData(conn, tableName);
        conn.createStatement().executeQuery(groupBy1);
        conn.createStatement().executeQuery(groupBy2);
        // TODO: validate query results
        try {
            conn.createStatement().executeQuery(groupBy3);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Aggregate expressions may not be used in GROUP BY"));
        }
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
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT c, SUM(f + g) AS sumone, d, e\n" + 
                "FROM " + tableName + "\n" + 
                "WHERE b = 'val'\n" + 
                "  AND a IN ('1','2','3')\n" + 
                "GROUP BY c, d, e\n" + 
                "ORDER BY sumone DESC");
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
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('000001111122222', '333334444455555', 0, 0, 'abc', '666667777788888')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('000001111122222', '333334444455555', 0, 0, 'abcd', '666667777788889')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('000001111122222', '333334444455555', 0, 0, 'abc', '666667777788899')");
        conn.commit();
        String query =
                "SELECT COUNT(1), EXTERNAL_DATASOURCE_KEY As DUP_COUNT\n" + 
                "    FROM " + tableName + " \n" + 
                "   WHERE JOURNEY_ID='333334444455555' AND \n" + 
                "                 DATASOURCE=0 AND MATCH_STATUS <= 1 and \n" + 
                "                 ORGANIZATION_ID='000001111122222' \n" + 
                "    GROUP BY MATCH_STATUS, EXTERNAL_DATASOURCE_KEY \n" + 
                "    HAVING COUNT(1) > 1";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertEquals("abc", rs.getString(2));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['000001111122222','333334444455555',0,*] - ['000001111122222','333334444455555',0,1]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [MATCH_STATUS, EXTERNAL_DATASOURCE_KEY]\n" + 
                "CLIENT FILTER BY COUNT(1) > 1",QueryUtil.getExplainPlan(rs));
    }
    
    @Test
    public void testGroupByOrderPreservingDescSort() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName(); 
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 char(1) not null, k2 char(1) not null, constraint pk primary key (k1,k2)) split on ('ac','jc','nc')");
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
        String query = "SELECT k1,count(*) FROM " + tableName + " GROUP BY k1 ORDER BY k1 DESC";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("n", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("j", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(
                "CLIENT PARALLEL 1-WAY REVERSE FULL SCAN OVER " + tableName + "\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [K1]", QueryUtil.getExplainPlan(rs));
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
        String query = "SELECT k1,sum(k2) FROM " + tableName + " GROUP BY k1 ORDER BY k1 DESC";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("n", rs.getString(1));
        assertEquals(10, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("j", rs.getString(1));
        assertEquals(10, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals(10, rs.getInt(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(
                "CLIENT PARALLEL 1-WAY REVERSE FULL SCAN OVER " + tableName + "\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [K1]", QueryUtil.getExplainPlan(rs));
    }

    @Test
    public void testAvgGroupByOrderPreservingWithStats() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + "\"SYSTEM\".\"STATS\"" + " WHERE " + PhoenixDatabaseMetaData.PHYSICAL_NAME + " ='" + tableName + "'");
        assertTrue(rs.next());
        assertEquals(0,rs.getInt(1));
        initAvgGroupTable(conn, tableName, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=20 ");
        testAvgGroupByOrderPreserving(conn, tableName, 13);
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + "\"SYSTEM\".\"STATS\"" + " WHERE " + PhoenixDatabaseMetaData.PHYSICAL_NAME + " ='" + tableName + "'");
        assertTrue(rs.next());
        assertEquals(13,rs.getInt(1));
        conn.setAutoCommit(true);
        conn.createStatement().execute("DELETE FROM " + "\"SYSTEM\".\"STATS\"");
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + "\"SYSTEM\".\"STATS\"" + " WHERE " + PhoenixDatabaseMetaData.PHYSICAL_NAME + " ='" + tableName + "'");
        assertTrue(rs.next());
        assertEquals(0,rs.getInt(1));
        TestUtil.doMajorCompaction(conn, tableName);
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + "\"SYSTEM\".\"STATS\"" + " WHERE " + PhoenixDatabaseMetaData.PHYSICAL_NAME + " ='" + tableName + "'");
        assertTrue(rs.next());
        assertEquals(13,rs.getInt(1));
        testAvgGroupByOrderPreserving(conn, tableName, 13);
        conn.createStatement().execute("ALTER TABLE " + tableName + " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=100");
        testAvgGroupByOrderPreserving(conn, tableName, 6);
        conn.createStatement().execute("ALTER TABLE " + tableName + " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=null");
        testAvgGroupByOrderPreserving(conn, tableName, 4);
    }
    
    @Test
    public void testAvgGroupByOrderPreservingWithNoStats() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        initAvgGroupTable(conn, tableName, "");
        testAvgGroupByOrderPreserving(conn, tableName, 4);
    }
    
    private void initAvgGroupTable(Connection conn, String tableName, String tableProps) throws SQLException {
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
    
    private void testAvgGroupByOrderPreserving(Connection conn, String tableName, int nGuidePosts) throws SQLException, IOException {
        String query = "SELECT k1,avg(k2) FROM " + tableName + " GROUP BY k1";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("j", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("n", rs.getString(1));
        assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName + "\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [K1]", QueryUtil.getExplainPlan(rs));
        TestUtil.analyzeTable(conn, tableName);
        List<KeyRange> splits = TestUtil.getAllSplits(conn, tableName);
        assertEquals(nGuidePosts, splits.size());
    }
    
    @Test
    public void testGroupByWithAliasWithSameColumnName() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String tableName3 = generateUniqueName();
        String ddl = "create table " + tableName1 + " (pk integer primary key, col integer)";
        conn.createStatement().execute(ddl);
        ddl = "create table " + tableName2 + " (pk integer primary key, col integer)";
        conn.createStatement().execute(ddl);
        ddl = "create table " + tableName3 + " (notPk integer primary key, col integer)";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + tableName1 + " VALUES (1,2)");
        conn.createStatement().execute("UPSERT INTO " + tableName2 + " VALUES (1,2)");
        conn.createStatement().execute("UPSERT INTO " + tableName3 + " VALUES (1,2)");
        conn.createStatement().executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " group by pk");
        conn.createStatement().executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " group by " + tableName1 + ".pk");
        conn.createStatement().executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " as t group by t.pk");
        conn.createStatement().executeQuery("select " + tableName1 + ".col as pk from " + tableName1);
        conn.createStatement()
                .executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " join " + tableName3 + " on (" + tableName1 + ".pk=" + tableName3 + ".notPk) group by pk");
        try {
            conn.createStatement().executeQuery("select " + tableName1 + ".col as pk from " + tableName1 + " group by pk");
            fail();
        } catch (AmbiguousColumnException e) {}
        try {
            conn.createStatement().executeQuery("select col as pk from " + tableName1 + " group by pk");
            fail();
        } catch (AmbiguousColumnException e) {}
        try {
            conn.createStatement()
                    .executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " join " + tableName2 + " on (" + tableName1 + ".pk=" + tableName2 + ".pk) group by pk");
            fail();
        } catch (AmbiguousColumnException e) {}
        conn.close();
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

            sql="SELECT DISTINCT entity_id,score FROM "+tableName+" WHERE organization_id = 'org1' AND container_id = 'container1' ORDER BY score DESC";
            ResultSet rs=conn.createStatement().executeQuery(sql);
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
            String query = "SELECT DISTINCT entity_id, score\n" + 
                    "    FROM " + tableName + "\n" +
                    "    WHERE organization_id = 'org2'\n" + 
                    "    AND container_id IN ( 'container1','container2','container3' )\n" + 
                    "    ORDER BY score DESC\n" + 
                    "    LIMIT 2";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertEquals(indexName, plan.getContext().getCurrentTable().getTable().getName().getString());
            assertFalse(plan.getOrderBy().getOrderByExpressions().isEmpty());
            assertTrue(rs.next());
            assertEquals("entityId8", rs.getString(1));
            assertEquals(1.45, rs.getDouble(2),0.001);
            assertTrue(rs.next());
            assertEquals("entityId3", rs.getString(1));
            assertEquals(1.4, rs.getDouble(2),0.001);
            assertFalse(rs.next());
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

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS FIRST";
            ResultSet rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,"a"},{"3","c"},});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null}});

            //----CONTAINER_ID

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"}});

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,null,'44')");
            conn.commit();

            //-----ORGANIZATION_ID ASC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID ASC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID DESC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID DESC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null},{null,null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"},{null,null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID  order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"},{null,null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"},{null,null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"},{null,null}});
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testGroupByCoerceExpressionBug3453() throws Exception {
        final Connection conn = DriverManager.getConnection(getUrl());
        try {
            //Type is INT
            String intTableName=generateUniqueName();
            String sql="CREATE TABLE "+ intTableName +"("+
                    "ENTITY_ID INTEGER NOT NULL,"+
                    "CONTAINER_ID INTEGER NOT NULL,"+
                    "SCORE INTEGER NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY (ENTITY_ID DESC,CONTAINER_ID DESC,SCORE DESC))";

            conn.createStatement().execute(sql);
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (1,1,1)");
            conn.commit();

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+intTableName+" limit 1)";
            ResultSet rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1}});

            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (2,2,2)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (3,3,3)");
            conn.commit();

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+intTableName+" limit 3) order by entity_id";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1},{2,2},{3,3}});

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+intTableName+" limit 3) order by entity_id desc";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{3,3},{2,2},{1,1}});

            //Type is CHAR
            String charTableName=generateUniqueName();
            sql="CREATE TABLE "+ charTableName +"("+
                    "ENTITY_ID CHAR(15) NOT NULL,"+
                    "CONTAINER_ID INTEGER NOT NULL,"+
                    "SCORE INTEGER NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY (ENTITY_ID DESC,CONTAINER_ID DESC,SCORE DESC))";

            conn.createStatement().execute(sql);
            conn.createStatement().execute("UPSERT INTO "+charTableName+" VALUES ('entity1',1,1)");
            conn.createStatement().execute("UPSERT INTO "+charTableName+" VALUES ('entity2',2,2)");
            conn.createStatement().execute("UPSERT INTO "+charTableName+" VALUES ('entity3',3,3)");
            conn.commit();

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+charTableName+" limit 3) order by entity_id";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"entity1",1},{"entity2",2},{"entity3",3}});

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+charTableName+" limit 3) order by entity_id desc";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"entity3",3},{"entity2",2},{"entity1",1}});
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    private void assertResultSet(ResultSet rs,Object[][] rows) throws Exception {
        for(int rowIndex=0;rowIndex<rows.length;rowIndex++) {
            assertTrue(rs.next());
            for(int columnIndex=1;columnIndex<= rows[rowIndex].length;columnIndex++) {
                Object realValue=rs.getObject(columnIndex);
                Object expectedValue=rows[rowIndex][columnIndex-1];
                if(realValue==null) {
                    assertTrue(expectedValue==null);
                }
                else {
                    assertTrue(realValue.equals(expectedValue));
                }
            }
        }
        assertTrue(!rs.next());
    }
}
