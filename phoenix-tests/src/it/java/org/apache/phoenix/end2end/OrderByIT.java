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
import static org.apache.phoenix.util.TestUtil.assertResultSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.containsString;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class OrderByIT extends BaseOrderByIT {

    @Test
    public void testOrderByWithPosition() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String tableName = generateUniqueName();
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

            String query = "select count(*), col1 from " + tableName + " group by col1 order by 2";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertFalse(rs.next());

            query = "select a_string x, col1 y from " + tableName + " order by x";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(30,rs.getInt(2));
            assertFalse(rs.next());

            query = "select * from " + tableName + " order by 2";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(30,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByWithJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String tableName1 = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName1 +
                    "  (a_string varchar not null, cf1.a integer, cf1.b varchar, col1 integer, cf2.c varchar, cf2.d integer " +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            String dml = "UPSERT INTO " + tableName1 + " VALUES(?,?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 40);
            stmt.setString(3, "aa");
            stmt.setInt(4, 10);
            stmt.setString(5, "bb");
            stmt.setInt(6, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.setString(3, "cc");
            stmt.setInt(4, 50);
            stmt.setString(5, "dd");
            stmt.setInt(6, 60);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 40);
            stmt.setString(3, "bb");
            stmt.setInt(4, 5);
            stmt.setString(5, "aa");
            stmt.setInt(6, 80);
            stmt.execute();
            conn.commit();

            String tableName2 = generateUniqueName();
            ddl = "CREATE TABLE " + tableName2 +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
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

            String query = "select t1.* from " + tableName1 + " t1 join " + tableName2 + " t2 on t1.a_string = t2.a_string order by 3";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertEquals("aa",rs.getString(3));
            assertEquals(10,rs.getInt(4));
            assertEquals("bb",rs.getString(5));
            assertEquals(20,rs.getInt(6));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertEquals("bb",rs.getString(3));
            assertEquals(5,rs.getInt(4));
            assertEquals("aa",rs.getString(5));
            assertEquals(80,rs.getInt(6));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(30,rs.getInt(2));
            assertEquals("cc",rs.getString(3));
            assertEquals(50,rs.getInt(4));
            assertEquals("dd",rs.getString(5));
            assertEquals(60,rs.getInt(6));
            assertFalse(rs.next());

            query = "select t1.a_string, t2.col1 from " + tableName1 + " t1 join " + tableName2 + " t2 on t1.a_string = t2.a_string order by 2";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(30,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByWithUnionAll() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String tableName1 = generateUniqueName();
            String ddl = "CREATE TABLE  " + tableName1 +
                    "  (a_string varchar not null, cf1.a integer, cf1.b varchar, col1 integer, cf2.c varchar, cf2.d integer " +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            String dml = "UPSERT INTO " + tableName1 + " VALUES(?,?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 40);
            stmt.setString(3, "aa");
            stmt.setInt(4, 10);
            stmt.setString(5, "bb");
            stmt.setInt(6, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.setString(3, "cc");
            stmt.setInt(4, 50);
            stmt.setString(5, "dd");
            stmt.setInt(6, 60);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 40);
            stmt.setString(3, "bb");
            stmt.setInt(4, 5);
            stmt.setString(5, "aa");
            stmt.setInt(6, 80);
            stmt.execute();
            conn.commit();

            String tableName2 = generateUniqueName();
            ddl = "CREATE TABLE " + tableName2 +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            dml = "UPSERT INTO " + tableName2 + " VALUES(?, ?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "aa");
            stmt.setInt(2, 40);
            stmt.execute();
            stmt.setString(1, "bb");
            stmt.setInt(2, 10);
            stmt.execute();
            stmt.setString(1, "cc");
            stmt.setInt(2, 30);
            stmt.execute();
            conn.commit();

            String query = "select a_string, cf2.d from " + tableName1 + " union all select * from " + tableName2 + " order by 2";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bb",rs.getString(1));
            assertEquals(10,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(20,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("cc",rs.getString(1));
            assertEquals(30,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("aa",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(60,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(80,rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByWithExpression() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName +
                    "  (a_string varchar not null, col1 integer, col2 integer, col3 timestamp, col4 varchar" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);

            Date date = new Date(System.currentTimeMillis());
            String dml = "UPSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 40);
            stmt.setInt(3, 20);
            stmt.setDate(4, new Date(date.getTime()));
            stmt.setString(5, "xxyy");
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 50);
            stmt.setInt(3, 30);
            stmt.setDate(4, new Date(date.getTime()-500));
            stmt.setString(5, "yyzz");
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 60);
            stmt.setInt(3, 20);
            stmt.setDate(4, new Date(date.getTime()-300));
            stmt.setString(5, "ddee");
            stmt.execute();
            conn.commit();

            String query = "SELECT col1+col2, col4, a_string FROM " + tableName + " ORDER BY 1, 2";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(3));
            assertFalse(rs.next());
        } catch (SQLException e) {
        } finally {
            conn.close();
        }
    }


    @Test
    public void testOrderByRVC() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "create table " + tableName + " (testpk varchar not null primary key, l_quantity decimal(15,2), l_discount decimal(15,2))";
        conn.createStatement().execute(ddl);

        PreparedStatement stmt = conn.prepareStatement("upsert into " + tableName + " values ('a',0.1,0.9)");
        stmt.execute();
        stmt = conn.prepareStatement(" upsert into " + tableName + " values ('b',0.5,0.5)");
        stmt.execute();
        stmt = conn.prepareStatement(" upsert into " + tableName + " values ('c',0.9,0.1)");
        stmt.execute();
        conn.commit();

        ResultSet rs;
        stmt = conn.prepareStatement("select l_discount,testpk from " + tableName + " order by (l_discount,l_quantity)");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(0.1, rs.getDouble(1), 0.01);
        assertEquals("c", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(0.5, rs.getDouble(1), 0.01);
        assertEquals("b", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(0.9, rs.getDouble(1), 0.01);
        assertEquals("a", rs.getString(2));
    }

    @Test
    public void testColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName +
                    "  (a_string varchar not null, cf1.a integer, cf1.b varchar, col1 integer, cf2.c varchar, cf2.d integer, col2 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            createTestTable(getUrl(), ddl);
            String dml = "UPSERT INTO " + tableName + " VALUES(?,?,?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 40);
            stmt.setString(3, "aa");
            stmt.setInt(4, 10);
            stmt.setString(5, "bb");
            stmt.setInt(6, 20);
            stmt.setInt(7, 1);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.setString(3, "cc");
            stmt.setInt(4, 50);
            stmt.setString(5, "dd");
            stmt.setInt(6, 60);
            stmt.setInt(7, 3);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 40);
            stmt.setString(3, "bb");
            stmt.setInt(4, 5);
            stmt.setString(5, "aa");
            stmt.setInt(6, 80);
            stmt.setInt(7, 2);
            stmt.execute();
            conn.commit();

            String query = "select * from " + tableName + " order by 2, 5";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(30,rs.getInt(2));
            assertEquals("cc",rs.getString(3));
            assertEquals(50,rs.getInt(4));
            assertEquals("dd",rs.getString(5));
            assertEquals(60,rs.getInt(6));
            assertEquals(3,rs.getInt(7));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertEquals("bb",rs.getString(3));
            assertEquals(5,rs.getInt(4));
            assertEquals("aa",rs.getString(5));
            assertEquals(80,rs.getInt(6));
            assertEquals(2,rs.getInt(7));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertEquals("aa",rs.getString(3));
            assertEquals(10,rs.getInt(4));
            assertEquals("bb",rs.getString(5));
            assertEquals(20,rs.getInt(6));
            assertEquals(1,rs.getInt(7));
            assertFalse(rs.next());

            query = "select * from " + tableName + " order by 7";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertEquals("aa",rs.getString(3));
            assertEquals(10,rs.getInt(4));
            assertEquals("bb",rs.getString(5));
            assertEquals(20,rs.getInt(6));
            assertEquals(1,rs.getInt(7));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals(40,rs.getInt(2));
            assertEquals("bb",rs.getString(3));
            assertEquals(5,rs.getInt(4));
            assertEquals("aa",rs.getString(5));
            assertEquals(80,rs.getInt(6));
            assertEquals(2,rs.getInt(7));
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(30,rs.getInt(2));
            assertEquals("cc",rs.getString(3));
            assertEquals(50,rs.getInt(4));
            assertEquals("dd",rs.getString(5));
            assertEquals(60,rs.getInt(6));
            assertEquals(3,rs.getInt(7));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByWithClientMemoryLimit() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB, Integer.toString(1));
        props.put(QueryServices.CLIENT_ORDERBY_SPOOLING_ENABLED_ATTRIB,
            Boolean.toString(Boolean.FALSE));

        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String tableName = generateUniqueName();
            String ddl =
                    "CREATE TABLE " + tableName + "  (a_string varchar not null, col1 integer"
                            + "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
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

            String query = "select count(*), col1 from " + tableName + " group by col1 order by 2";
            ResultSet rs = conn.createStatement().executeQuery(query);
            try {
                rs.next();
                fail("Expected PhoenixIOException due to IllegalStateException");
            } catch (PhoenixIOException e) {
                assertThat(e.getMessage(), containsString("java.lang.IllegalStateException: "
                        + "Queue full. Consider increasing memory threshold or spooling to disk"));
            }
        }
    }

    @Test
    public void testOrderPreservingOptimizeBug5148() throws Exception {
        doTestOrderPreservingOptimizeBug5148(false,false);
        doTestOrderPreservingOptimizeBug5148(false,true);
        doTestOrderPreservingOptimizeBug5148(true,false);
        doTestOrderPreservingOptimizeBug5148(true,true);
    }

    private void doTestOrderPreservingOptimizeBug5148(boolean desc ,boolean salted) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            String tableName = generateUniqueName();
            String sql = "create table " + tableName + "( "+
                    " pk1 varchar not null , " +
                    " pk2 varchar not null, " +
                    " pk3 varchar not null," +
                    " v1 varchar, " +
                    " v2 varchar, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "pk1 "+(desc ? "desc" : "")+", "+
                    "pk2 "+(desc ? "desc" : "")+", "+
                    "pk3 "+(desc ? "desc" : "")+
                    " )) "+(salted ? "SALT_BUCKETS =4" : "split on('b')");
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a11','a12','a13','a14','a15')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a21','a22','a23','a24','a25')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a31','a32','a33','a34','a35')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b11','b12','b13','b14','b15')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b21','b22','b23','b24','b25')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','b32','b33','b34','b35')");
            conn.commit();
            //TestOrderByOrderPreserving
            sql = "select pk3 from (select v1,v2,pk3 from " + tableName + " t where pk1 > 'a10' order by t.v2,t.pk3,t.v1 limit 10) a order by v2,pk3";
            ResultSet rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            sql = "select pk3 from (select v1,v2,pk3 from "+tableName+" t where pk1 > 'a10' order by t.v2 desc,t.pk3 desc,t.v1 desc limit 10) a order by v2 desc ,pk3 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select sub from (select substr(pk2,0,3) sub,cast (count(pk3) as bigint) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3),t.pk2 limit 10) a order by cnt,sub";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a12"},{"a22"},{"a32"},{"b12"},{"b22"},{"b32"}});

            sql = "select sub from (select substr(pk2,0,3) sub,count(pk3) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3),t.pk2 limit 10) a "+
                  "order by cast(cnt as bigint),substr(sub,0,3)";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a12"},{"a22"},{"a32"},{"b12"},{"b22"},{"b32"}});

            sql = "select sub from (select substr(pk2,0,3) sub,cast (count(pk3) as bigint) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3) desc,t.pk2 desc limit 10) a "+
                  "order by cnt desc,sub desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});

            sql = "select sub from (select substr(pk2,0,3) sub,count(pk3) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3) desc,t.pk2 desc limit 10) a "+
                  "order by cast(cnt as bigint) desc,substr(sub,0,3) desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});

            sql = "select sub,pk2 from (select substr(v2,0,3) sub,pk2 from "+tableName+" t where pk1 > 'a10' group by pk2,v2 limit 10) a order by pk2,sub";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a15","a12"},{"a25","a22"},{"a35","a32"},{"b15","b12"},{"b25","b22"},{"b35","b32"}});

            sql = "select sub,pk2 from (select substr(v2,0,3) sub,pk2 from "+tableName+" t where pk1 > 'a10' group by pk2,v2 limit 10) a order by pk2 desc,sub";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b35","b32"},{"b25","b22"},{"b15","b12"},{"a35","a32"},{"a25","a22"},{"a15","a12"}});

            //test innerQueryPlan is ordered by rowKey
            sql = "select pk3 from (select pk3,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1,t.pk2 limit 10) a where pk3 > 'a10' order by pk1,pk2,pk3";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            sql = "select sub from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1,t.pk2 limit 10) a where sub > 'a10' order by pk1,pk2,sub";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            sql = "select pk3 from (select pk3,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1 desc,t.pk2 desc limit 10) a where pk3 > 'a10' order by pk1 desc ,pk2 desc ,pk3 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select sub from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1 desc,t.pk2 desc limit 10) a where sub > 'a10' order by pk1 desc,pk2 desc,sub desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            //TestGroupByOrderPreserving
            sql = "select pk2 from (select v1,pk2,pk1 from "+tableName+" t where pk1 > 'a10' order by t.pk2,t.v1,t.pk1 limit 10) a group by pk2, v1 order by pk2,v1";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a12"},{"a22"},{"a32"},{"b12"},{"b22"},{"b32"}});

            sql = "select pk2 from (select v1,pk2,pk1 from "+tableName+" t where pk1 > 'a10' order by t.pk2 desc,t.v1 desc,t.pk1 limit 10) a group by pk2, v1 order by pk2 desc,v1 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});

            sql = "select sub from (select substr(pk2,0,3) sub,cast (count(pk3) as bigint) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3),t.pk2 limit 10) a "+
                  "group by cnt,sub order by cnt,sub";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a12"},{"a22"},{"a32"},{"b12"},{"b22"},{"b32"}});

            sql = "select substr(sub,0,3) from (select substr(pk2,0,3) sub,count(pk3) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3),t.pk2 limit 10) a "+
                  "group by cast(cnt as bigint),substr(sub,0,3) order by cast(cnt as bigint),substr(sub,0,3)";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a12"},{"a22"},{"a32"},{"b12"},{"b22"},{"b32"}});

            sql = "select sub from (select substr(pk2,0,3) sub,cast (count(pk3) as bigint) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3) desc,t.pk2 desc limit 10) a "+
                  "group by cnt,sub order by cnt desc,sub desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});

            sql = "select substr(sub,0,3) from (select substr(pk2,0,3) sub,count(pk3) cnt from "+tableName+" t where pk1 > 'a10' group by v1 ,pk2 order by count(pk3) desc,t.pk2 desc limit 10) a "+
                  "group by cast(cnt as bigint),substr(sub,0,3) order by cast(cnt as bigint) desc,substr(sub,0,3) desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});

            sql = "select pk2 from (select substr(v2,0,3) sub,pk2 from "+tableName+" t where pk1 > 'a10' group by pk2,v2 limit 10) a group by pk2,sub order by pk2,sub";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a12"},{"a22"},{"a32"},{"b12"},{"b22"},{"b32"}});

            sql = "select pk2 from (select substr(v2,0,3) sub,pk2 from "+tableName+" t where pk1 > 'a10' group by pk2,v2 limit 10) a group by pk2,sub order by pk2 desc,sub";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});

            //test innerQueryPlan is ordered by rowKey
            sql = "select pk3 from (select pk3,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1,t.pk2 limit 10) a where pk3 > 'a10' group by pk1,pk2,pk3 order by pk1,pk2,pk3";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            sql = "select sub from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1,t.pk2 limit 10) a where sub > 'a10' group by pk1,pk2,sub order by pk1,pk2";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            sql = "select pk3 from (select pk3,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1 desc,t.pk2 desc limit 10) a where pk3 > 'a10' group by pk1, pk2, pk3 order by pk1 desc ,pk2 desc ,pk3 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select sub from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 > 'a10' order by t.pk1 desc,t.pk2 desc limit 10) a where sub > 'a10' group by pk1,pk2,sub order by pk1 desc,pk2 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}
