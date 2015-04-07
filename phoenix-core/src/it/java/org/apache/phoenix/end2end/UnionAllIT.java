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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;


public class UnionAllIT extends BaseOwnClusterHBaseManagedTimeIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testUnionAllSelects() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select * from test_table union all select * from b_table union all select * from test_table";
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table VALUES(?, ?)";
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

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            String aggregate = "select count(*) from test_table union all select count(*) from b_table union all select count(*) from test_table";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            String aggregate = "select count(*), col1 from test_table group by col1 union all select count(*), col1 from b_table group by col1";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table1 " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table1 VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "f");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table1 " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO b_table1 VALUES(?, ?)";
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

            String aggregate = "select count(*), col1 from b_table1 group by col1 union all select count(*), col1 from test_table1 group by col1 order by col1";
            ResultSet rs = conn.createStatement().executeQuery(aggregate);
            assertTrue(rs.next());
            assertEquals(2,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));  
            assertFalse(rs.next());  

            String limit = "select count(*), col1 x from test_table1 group by col1 union all select count(*), col1 x from b_table1 group by col1 order by x limit 2";
            rs = conn.createStatement().executeQuery(limit);
            assertTrue(rs.next());
            assertEquals(2,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertFalse(rs.next());  

            String limitOnly = "select * from test_table1 union all select * from b_table1 limit 2";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "select a_string, col1, col1 from test_table union all select * from b_table union all select a_string, col1 from test_table";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);


            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select x.a_string, y.col1  from test_table x, b_table y where x.a_string=y.a_string union all " +
                    "select t.a_string, s.col1 from test_table s, b_table t where s.a_string=t.a_string"; 
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next()); 

            ddl = "select x.a_string, y.col1  from test_table x join b_table y on x.a_string=y.a_string union all " +
                    "select t.a_string, s.col1 from test_table s inner join b_table t on s.a_string=t.a_string"; 
            rs = conn.createStatement().executeQuery(ddl);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertFalse(rs.next()); 

            ddl = "select x.a_string, y.col1  from test_table x left join b_table y on x.a_string=y.a_string union all " +
                    "select t.a_string, s.col1 from test_table s inner join b_table t on s.a_string=t.a_string union all " +
                    "select y.a_string, x.col1 from b_table x right join test_table y on x.a_string=y.a_string";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select * from (select x.a_string, y.col1  from test_table x, b_table y where x.a_string=y.a_string) union all " +
                    "select * from (select t.a_string, s.col1 from test_table s, b_table t where s.a_string=t.a_string)"; 
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col2 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 30);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 60);
            stmt.execute();
            conn.commit();

            String query = "select a_string from " +
                    "(select a_string, col1 from test_table union all select a_string, col2 from b_table order by a_string)";
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
                    "(select a_string, col1 c from test_table union all select a_string, col2 c from b_table order by c)";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO test_table VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 30);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 60);
            stmt.execute();
            conn.commit();

            String[] queries = new String[2];
            queries[0] = "select a_string, col1 from test_table where a_string in " +
                    "(select a_string aa from b_table where a_string != 'a' union all select a_string bb from b_table)";
            queries[1] = "select a_string, col1 from test_table where a_string in (select a_string from  " +
                    "(select a_string from b_table where a_string != 'a' union all select a_string from b_table))";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            String dml = "UPSERT INTO test_table VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 10);
            stmt.execute();
            conn.commit();

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            dml = "UPSERT INTO b_table VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            conn.commit();

            ddl = "select a_string, col1 from b_table where col1=? union all select a_string, col1 from test_table where col1=? ";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "CREATE TABLE b_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            ddl = "explain select a_string, col1 from test_table union all select a_string, col1 from b_table order by col1 limit 1";
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertEquals(
                    "UNION ALL OVER 2 QUERIES\n" +
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER TEST_TABLE\n" + 
                    "        SERVER TOP 1 ROW SORTED BY [COL1]\n" + 
                    "    CLIENT MERGE SORT\n" + 
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER B_TABLE\n" + 
                    "        SERVER TOP 1 ROW SORTED BY [COL1]\n" + 
                    "    CLIENT MERGE SORT\n" + 
                    "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs)); 
            
            String limitPlan = 
                    "UNION ALL OVER 2 QUERIES\n" + 
                    "    CLIENT SERIAL 1-WAY FULL SCAN OVER TEST_TABLE\n" + 
                    "        SERVER 2 ROW LIMIT\n" + 
                    "    CLIENT 2 ROW LIMIT\n" + 
                    "    CLIENT SERIAL 1-WAY FULL SCAN OVER B_TABLE\n" + 
                    "        SERVER 2 ROW LIMIT\n" + 
                    "    CLIENT 2 ROW LIMIT\n" + 
                    "CLIENT 2 ROW LIMIT";
            ddl = "explain select a_string, col1 from test_table union all select a_string, col1 from b_table";
            rs = conn.createStatement().executeQuery(ddl + " limit 2");
            assertEquals(limitPlan, QueryUtil.getExplainPlan(rs));
            Statement stmt = conn.createStatement();
            stmt.setMaxRows(2);
            rs = stmt.executeQuery(ddl);
            assertEquals(limitPlan, QueryUtil.getExplainPlan(rs));
            
            ddl = "explain select a_string, col1 from test_table union all select a_string, col1 from b_table";
            rs = conn.createStatement().executeQuery(ddl);
            assertEquals(
                    "UNION ALL OVER 2 QUERIES\n" + 
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER TEST_TABLE\n" + 
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER B_TABLE", QueryUtil.getExplainPlan(rs)); 
        } finally {
            conn.close();
        }
    } 
}
