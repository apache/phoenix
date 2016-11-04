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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class UnionAllIT extends ParallelStatsDisabledIT {

    @Test
    public void testUnionAllSelects() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar(10) not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string char(20) not null, col1 bigint" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select * from " + tableName1 + " union all select * from " + tableName2 + " union all select * from " + tableName1;
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1).trim());
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1).trim());
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next());  
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAggregate() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string char(5) not null, col1 tinyint" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "d");
            stmt.setInt(2, 40);
            stmt.execute();
            stmt.setString(1, "e");
            stmt.setInt(2, 50);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            String aggregate = "select count(*) from " + tableName1 + " union all select count(*) from " + tableName2 + " union all select count(*) from " + tableName1;
            ResultSet rs = conn.createStatement().executeQuery(aggregate);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
            assertFalse(rs.next());  
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGroupBy() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            String aggregate = "select count(*), col1 from " + tableName1 + " group by col1 union all select count(*), col1 from " + tableName2 + " group by col1";
            ResultSet rs = conn.createStatement().executeQuery(aggregate);
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1)); 
            assertFalse(rs.next());  
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByLimit() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "f");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            stmt.setString(1, "d");
            stmt.setInt(2, 30);
            stmt.execute();
            stmt.setString(1, "e");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            String aggregate = "select count(*), col1 from " + tableName2 + " group by col1 union all select count(*), col1 from " + tableName1 + " group by col1 order by col1";
            ResultSet rs = conn.createStatement().executeQuery(aggregate);
            assertTrue(rs.next());
            assertEquals(2,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));  
            assertFalse(rs.next());  

            String limit = "select count(*), col1 x from " + tableName1 + " group by col1 union all select count(*), col1 x from " + tableName2 + " group by col1 order by x limit 2";
            rs = conn.createStatement().executeQuery(limit);
            assertTrue(rs.next());
            assertEquals(2,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertFalse(rs.next());  

            String limitOnly = "select * from " + tableName1 + " union all select * from " + tableName2 + " limit 2";
            rs = conn.createStatement().executeQuery(limitOnly);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("f",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next());  
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectDiff() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "select a_string, col1, col1 from " + tableName1 + " union all select * from " + tableName2 + " union all select a_string, col1 from " + tableName1;
            conn.createStatement().executeQuery(ddl);
            fail();
        }  catch (SQLException e) {
            assertEquals(SQLExceptionCode.SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testJoinInUnionAll() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);


            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select x.a_string, y.col1  from " + tableName1 + " x, " + tableName2 + " y where x.a_string=y.a_string union all " +
                    "select t.a_string, s.col1 from " + tableName1 + " s, " + tableName2 + " t where s.a_string=t.a_string"; 
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next()); 

            ddl = "select x.a_string, y.col1  from " + tableName1 + " x join " + tableName2 + " y on x.a_string=y.a_string union all " +
                    "select t.a_string, s.col1 from " + tableName1 + " s inner join " + tableName2 + " t on s.a_string=t.a_string"; 
            rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next()); 

            ddl = "select x.a_string, y.col1  from " + tableName1 + " x left join " + tableName2 + " y on x.a_string=y.a_string union all " +
                    "select t.a_string, s.col1 from " + tableName1 + " s inner join " + tableName2 + " t on s.a_string=t.a_string union all " +
                    "select y.a_string, x.col1 from " + tableName2 + " x right join " + tableName1 + " y on x.a_string=y.a_string";
            rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2)); 
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2)); 
            assertFalse(rs.next()); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDerivedTable() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select * from (select x.a_string, y.col1  from " + tableName1 + " x, " + tableName2 + " y where x.a_string=y.a_string) union all " +
                    "select * from (select t.a_string, s.col1 from " + tableName1 + " s, " + tableName2 + " t where s.a_string=t.a_string)"; 
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next()); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUnionAllInDerivedTable() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col2 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 30);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 60);
            stmt.execute();
            conn.commit();

            String query = "select a_string from " +
                    "(select a_string, col1 from " + tableName1 + " union all select a_string, col2 from " + tableName2 + " order by a_string)";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertFalse(rs.next());
            
            query = "select c from " +
                    "(select a_string, col1 c from " + tableName1 + " union all select a_string, col2 c from " + tableName2 + " order by c)";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(60, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUnionAllInSubquery() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 30);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 60);
            stmt.execute();
            conn.commit();

            String[] queries = new String[2];
            queries[0] = "select a_string, col1 from " + tableName1 + " where a_string in " +
                    "(select a_string aa from " + tableName2 + " where a_string != 'a' union all select a_string bb from " + tableName2 + ")";
            queries[1] = "select a_string, col1 from " + tableName1 + " where a_string in (select a_string from  " +
                    "(select a_string from " + tableName2 + " where a_string != 'a' union all select a_string from " + tableName2 + "))";
            for (String query : queries) {
                ResultSet rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals(10, rs.getInt(2));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUnionAllWithBindParam() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select a_string, col1 from " + tableName2 + " where col1=? union all select a_string, col1 from " + tableName1 + " where col1=? ";
            stmt = conn.prepareStatement(ddl);
            stmt.setInt(1, 20);
            stmt.setInt(2, 10);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next()); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testExplainUnionAll() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "CREATE TABLE " + tableName2 + " " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "explain select a_string, col1 from " + tableName1 + " union all select a_string, col1 from " + tableName2 + " order by col1 limit 1";
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertEquals(
                    "UNION ALL OVER 2 QUERIES\n" +
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName1 + "\n" + 
                    "        SERVER TOP 1 ROW SORTED BY [COL1]\n" + 
                    "    CLIENT MERGE SORT\n" + 
                    "    CLIENT LIMIT 1\n" + 
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName2 + "\n" + 
                    "        SERVER TOP 1 ROW SORTED BY [COL1]\n" + 
                    "    CLIENT MERGE SORT\n" +
                    "    CLIENT LIMIT 1\n" + 
                    "CLIENT MERGE SORT\nCLIENT LIMIT 1", QueryUtil.getExplainPlan(rs)); 
            
            String limitPlan = 
                    "UNION ALL OVER 2 QUERIES\n" + 
                    "    CLIENT SERIAL 1-WAY FULL SCAN OVER " + tableName1 + "\n" + 
                    "        SERVER 2 ROW LIMIT\n" + 
                    "    CLIENT 2 ROW LIMIT\n" + 
                    "    CLIENT SERIAL 1-WAY FULL SCAN OVER " + tableName2 + "\n" + 
                    "        SERVER 2 ROW LIMIT\n" + 
                    "    CLIENT 2 ROW LIMIT\n" + 
                    "CLIENT 2 ROW LIMIT";
            ddl = "explain select a_string, col1 from " + tableName1 + " union all select a_string, col1 from " + tableName2;
            rs = conn.createStatement().executeQuery(ddl + " limit 2");
            assertEquals(limitPlan, QueryUtil.getExplainPlan(rs));
            Statement stmt = conn.createStatement();
            stmt.setMaxRows(2);
            rs = stmt.executeQuery(ddl);
            assertEquals(limitPlan, QueryUtil.getExplainPlan(rs));
            
            ddl = "explain select a_string, col1 from " + tableName1 + " union all select a_string, col1 from " + tableName2;
            rs = conn.createStatement().executeQuery(ddl);
            assertEquals(
                    "UNION ALL OVER 2 QUERIES\n" + 
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName1 + "\n" + 
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName2, QueryUtil.getExplainPlan(rs)); 
        } finally {
            conn.close();
        }
    } 

    @Test
    public void testBug2295() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String itableName1 = generateUniqueName();
        String itableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + tableName1 + "(" +
                    "id BIGINT, col1 VARCHAR, col2 integer, CONSTRAINT pk PRIMARY KEY (id)) IMMUTABLE_ROWS=true";
            createTestTable(getUrl(), ddl);

            ddl = "CREATE TABLE " + tableName2 + "(" +
                    "id BIGINT, col1 VARCHAR, col2 integer, CONSTRAINT pk PRIMARY KEY (id)) IMMUTABLE_ROWS=true";
            createTestTable(getUrl(), ddl);

            ddl = "CREATE index " + itableName1 + " on " + tableName1 + "(col1)";
            createTestTable(getUrl(), ddl);

            ddl = "CREATE index " + itableName2 + " on " + tableName2 + "(col1)";
            createTestTable(getUrl(), ddl);

            ddl = "Explain SELECT /*+ INDEX(" + tableName1 + " " + itableName1 + ") */ col1, col2 from " + tableName1 + " where col1='123' " +
                    "union all SELECT /*+ INDEX(" + tableName2 + " " + itableName2 + ") */ col1, col2 from " + tableName2 + " where col1='123'"; 
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testParameterMetaDataNotNull() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
    
        String ddl = "CREATE TABLE " + tableName1 + " " +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
        createTestTable(getUrl(), ddl);
        String dml = "UPSERT INTO " + tableName1 + " VALUES(?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, "a");
        stmt.setInt(2, 10);
        stmt.execute();
        conn.commit();

        ddl = "CREATE TABLE " + tableName2 + " " +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
        createTestTable(getUrl(), ddl);
        dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
        stmt = conn.prepareStatement(dml);
        stmt.setString(1, "b");
        stmt.setInt(2, 20);
        stmt.execute();
        conn.commit();

        String query = "select * from " + tableName1 + " union all select * from " + tableName2;

        try{
            PreparedStatement pstmt = conn.prepareStatement(query);
            assertTrue(pstmt.getParameterMetaData() != null);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertFalse(rs.next()); 
        } finally {
            conn.close();
        }
    } 

    @Test
    public void testDiffDataTypes() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String tableName3 = generateUniqueName();
        String tableName4 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        String ddl = "create table " + tableName1 + " ( id bigint not null primary key, " +
                "firstname varchar(10), lastname varchar(10) )";
        createTestTable(getUrl(), ddl);
        String dml = "upsert into " + tableName1 + " values (?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "john");
        stmt.setString(3, "doe");
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "jane");
        stmt.setString(3, "doe");
        stmt.execute();
        conn.commit();

        ddl = "create table " + tableName2 + " ( id integer not null primary key, firstname char(12)," +
                " lastname varchar(12) )";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName2 + " values (?, ?, ?)";
        stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "sam");
        stmt.setString(3, "johnson");
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "ann");
        stmt.setString(3, "wiely");
        stmt.execute();
        conn.commit();

        ddl = "create table " + tableName3 + " ( id varchar(20) not null primary key)";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName3 + " values ('abcd')";
        stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
        ddl = "create table " + tableName4 + " ( id char(50) not null primary key)";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName4 + " values ('xyz')";
        stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
        String query = "select id, 'foo' firstname, lastname from " + tableName1 + " union all" +
                " select * from " + tableName2;
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("foo", rs.getString(2));
            assertEquals("doe", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("foo", rs.getString(2));
            assertEquals("doe", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("sam", rs.getString(2).trim());
            assertEquals("johnson", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("ann", rs.getString(2).trim());
            assertEquals("wiely", rs.getString(3));
            assertFalse(rs.next());

            pstmt = conn.prepareStatement("select * from " + tableName3 + " union all select * from " + tableName4);
            rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("xyz", rs.getString(1).trim());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDiffScaleSortOrder() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String tableName3 = generateUniqueName();
        String tableName4 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        String ddl = "create table " + tableName1 + " ( id bigint not null primary key desc, " +
                "firstname char(10), lastname varchar(10) )";
        createTestTable(getUrl(), ddl);
        String dml = "upsert into " + tableName1 + " values (?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "john");
        stmt.setString(3, "doe");
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "jane");
        stmt.setString(3, "doe");
        stmt.execute();
        conn.commit();

        ddl = "create table " + tableName2 + " ( id integer not null primary key asc, " +
                "firstname varchar(12), lastname varchar(10) )";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName2 + " values (?, ?, ?)";
        stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "sam");
        stmt.setString(3, "johnson");
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "ann");
        stmt.setString(3, "wiely");
        stmt.execute();
        conn.commit();

        ddl = "create table " + tableName3 + " ( id varchar(20) not null primary key, col1 decimal)";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName3 + " values ('abcd', 234.23)";
        stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
        ddl = "create table " + tableName4 + " ( id char(50) not null primary key, col1 decimal(12,4))";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName4 + " values ('xyz', 1342.1234)";
        stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();

        String query = "select * from " + tableName2 + " union all select * from " + tableName1;
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("sam", rs.getString(2));
            assertEquals("johnson", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("ann", rs.getString(2));
            assertEquals("wiely", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("jane", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("john", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3));
            assertFalse(rs.next());

            pstmt = conn.prepareStatement("select * from " + tableName3 + " union all select * from " + tableName4);
            rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertEquals(BigDecimal.valueOf(234.2300), rs.getBigDecimal(2));
            assertTrue(rs.next());
            assertEquals("xyz", rs.getString(1).trim());
            assertEquals(BigDecimal.valueOf(1342.1234), rs.getBigDecimal(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarcharChar() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        String ddl = "create table " + tableName2 + " ( id integer not null primary key asc, " +
                "firstname char(8), lastname varchar )";
        createTestTable(getUrl(), ddl);
        String dml = "upsert into " + tableName2 + " values (?, ?, ?)";
        PreparedStatement  stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "sam");
        stmt.setString(3, "johnson");
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "ann");
        stmt.setString(3, "wiely");
        stmt.execute();
        conn.commit();

        ddl = "create table " + tableName1 + " ( id bigint not null primary key desc, " +
                "firstname varchar(10), lastname char(10) )";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName1 + " values (?, ?, ?)";
        stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "john");
        stmt.setString(3, "doe");
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "jane");
        stmt.setString(3, "doe");
        stmt.execute();
        conn.commit();

        String query = "select id, 'baa' firstname, lastname from " + tableName2 + " " +
                "union all select * from " + tableName1;
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("baa", rs.getString(2));
            assertEquals("johnson", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("baa", rs.getString(2));
            assertEquals("wiely", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("jane", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3).trim());
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("john", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3).trim());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCoerceExpr() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        String ddl = "create table " + tableName2 + " ( id integer not null primary key desc, " +
                "firstname char(8), lastname varchar, sales double)";
        createTestTable(getUrl(), ddl);
        String dml = "upsert into " + tableName2 + " values (?, ?, ?, ?)";
        PreparedStatement  stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "sam");
        stmt.setString(3, "johnson");
        stmt.setDouble(4, 100.6798);
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "ann");
        stmt.setString(3, "wiely");
        stmt.setDouble(4, 10.67);
        stmt.execute();
        conn.commit();

        ddl = "create table " + tableName1 + " (id bigint not null primary key, " +
                "firstname char(10), lastname varchar(10), sales decimal)";
        createTestTable(getUrl(), ddl);
        dml = "upsert into " + tableName1 + " values (?, ?, ?, ?)";
        stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "john");
        stmt.setString(3, "doe");
        stmt.setBigDecimal(4, BigDecimal.valueOf(467.894745));
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setString(2, "jane");
        stmt.setString(3, "doe");
        stmt.setBigDecimal(4, BigDecimal.valueOf(88.89474501));
        stmt.execute();
        conn.commit();

        String query = "select id, cast('foo' as char(10)) firstname, lastname, sales " +
                "from " + tableName1 + " union all select * from " + tableName2;
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("foo", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3).trim());
            assertEquals(BigDecimal.valueOf(467.894745), rs.getBigDecimal(4));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("foo", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3).trim());
            assertEquals(BigDecimal.valueOf(88.89474501), rs.getBigDecimal(4));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("ann", rs.getString(2).trim());
            assertEquals("wiely", rs.getString(3).trim());
            assertEquals(BigDecimal.valueOf(10.67), rs.getBigDecimal(4));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("sam", rs.getString(2).trim());
            assertEquals("johnson", rs.getString(3).trim());
            assertEquals(BigDecimal.valueOf(100.6798), rs.getBigDecimal(4));
            assertFalse(rs.next());

            query = "select id, cast('foo' as char(10)) firstname, lastname, sales from " + tableName1;
            pstmt = conn.prepareStatement(query);
            rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("foo", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3));
            assertEquals(BigDecimal.valueOf(467.894745), rs.getBigDecimal(4));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("foo", rs.getString(2).trim());
            assertEquals("doe", rs.getString(3));
            assertEquals(BigDecimal.valueOf(88.89474501), rs.getBigDecimal(4));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
