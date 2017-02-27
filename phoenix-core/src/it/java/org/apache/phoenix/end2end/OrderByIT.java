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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class OrderByIT extends ParallelStatsDisabledIT {

    @Test
    public void testMultiOrderByExpr() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());
        String query = "SELECT entity_id FROM " + tableName + " ORDER BY b_string, entity_id";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }


    @Test
    public void testDescMultiOrderByExpr() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());
        String query = "SELECT entity_id FROM " + tableName + " ORDER BY b_string || entity_id desc";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

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
    public void testNullsLastWithDesc() throws Exception {
        Connection conn=null;
        try {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);

            String tableName=generateUniqueName();
            conn.createStatement().execute("DROP TABLE if exists "+tableName);
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

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID ASC NULLS FIRST";
            ResultSet rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,"a"},{"3","c"},});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null}});

            //----CONTAINER_ID

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"}});

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,null,'44')");
            conn.commit();

            //-----ORGANIZATION_ID ASC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null},{null,null}});


            //-----ORGANIZATION_ID ASC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{null,"a"},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{"3","c"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID DESC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null},{null,null}});

            //-----ORGANIZATION_ID DESC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{"2",null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{null,null},{"3","c"},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{null,null},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{null,"a"},{"2",null},{null,null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"},{null,null}});

            //-----CONTAINER_ID ASC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"2",null},{"3","c"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"2",null},{"3","c"},{null,"a"},{null,null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"},{null,null}});

            //-----CONTAINER_ID DESC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,null},{null,"a"},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{null,"a"},{null,null},{"3","c"},{"2",null}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,null},{null,"a"}});

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM "+tableName+" order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new String[][]{{"3","c"},{"2",null},{null,"a"},{null,null}});
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderByReverseOptimizationBug3491() throws Exception {
        for(boolean salted: new boolean[]{true,false}) {
            doTestOrderByReverseOptimizationBug3491(salted,true,true,true);
            doTestOrderByReverseOptimizationBug3491(salted,true,true,false);
            doTestOrderByReverseOptimizationBug3491(salted,true,false,true);
            doTestOrderByReverseOptimizationBug3491(salted,true,false,false);
            doTestOrderByReverseOptimizationBug3491(salted,false,true,true);
            doTestOrderByReverseOptimizationBug3491(salted,false,true,false);
            doTestOrderByReverseOptimizationBug3491(salted,false,false,true);
            doTestOrderByReverseOptimizationBug3491(salted,false,false,false);
        }
    }

    private void doTestOrderByReverseOptimizationBug3491(boolean salted,boolean desc1,boolean desc2,boolean desc3) throws Exception {
        Connection conn = null;
        try {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String tableName=generateUniqueName();
            conn.createStatement().execute("DROP TABLE if exists "+tableName);
            String sql="CREATE TABLE "+tableName+" ( "+
                    "ORGANIZATION_ID INTEGER NOT NULL,"+
                    "CONTAINER_ID INTEGER NOT NULL,"+
                    "SCORE INTEGER NOT NULL,"+
                    "ENTITY_ID INTEGER NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "ORGANIZATION_ID" +(desc1 ? " DESC" : "" )+","+
                    "CONTAINER_ID"+(desc2 ? " DESC" : "" )+","+
                    "SCORE"+(desc3 ? " DESC" : "" )+","+
                    "ENTITY_ID"+
                    ")) "+(salted ? "SALT_BUCKETS =4" : "split on(4)");
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,1,1,1)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2,2,2,2)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (3,3,3,3)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (4,4,4,4)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (5,5,5,5)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (6,6,6,6)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (1,1,1,11)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2,2,2,22)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (3,3,3,33)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (4,4,4,44)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (5,5,5,55)");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (6,6,6,66)");
            conn.commit();

            //groupBy orderPreserving orderBy asc asc
            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC, CONTAINER_ID ASC";
            ResultSet rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1},{2,2},{3,3},{4,4},{5,5},{6,6}});

            //groupBy orderPreserving orderBy asc desc
            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC, CONTAINER_ID desc";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1},{2,2},{3,3},{4,4},{5,5},{6,6}});

            //groupBy orderPreserving orderBy desc asc
            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC, CONTAINER_ID DESC";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{6,6},{5,5},{4,4},{3,3},{2,2},{1,1}});

            //groupBy orderPreserving orderBy desc desc
            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC, CONTAINER_ID DESC";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{6,6},{5,5},{4,4},{3,3},{2,2},{1,1}});

            //groupBy not orderPreserving orderBy asc asc
            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC, SCORE ASC";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1},{2,2},{3,3},{4,4},{5,5},{6,6}});

            //groupBy not orderPreserving orderBy asc desc
            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC, SCORE ASC";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1},{2,2},{3,3},{4,4},{5,5},{6,6}});

            //groupBy not orderPreserving orderBy desc asc
            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC, SCORE DESC";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{6,6},{5,5},{4,4},{3,3},{2,2},{1,1}});

            //groupBy not orderPreserving orderBy desc desc
            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC, SCORE DESC";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{6,6},{5,5},{4,4},{3,3},{2,2},{1,1}});
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderByReverseOptimizationWithNUllsLastBug3491() throws Exception{
        for(boolean salted: new boolean[]{true,false}) {
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,true,true);
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,true,false);
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,false,true);
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,false,false);
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,true,true);
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,true,false);
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,false,true);
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,false,false);
        }
    }

    private void doTestOrderByReverseOptimizationWithNUllsLastBug3491(boolean salted,boolean desc1,boolean desc2,boolean desc3) throws Exception {
        Connection conn = null;
        try {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String tableName=generateUniqueName();
            conn.createStatement().execute("DROP TABLE if exists "+tableName);
            String sql="CREATE TABLE "+tableName+" ( "+
                    "ORGANIZATION_ID VARCHAR,"+
                    "CONTAINER_ID VARCHAR,"+
                    "SCORE VARCHAR,"+
                    "ENTITY_ID VARCHAR NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "ORGANIZATION_ID" +(desc1 ? " DESC" : "" )+","+
                    "CONTAINER_ID"+(desc2 ? " DESC" : "" )+","+
                    "SCORE"+(desc3 ? " DESC" : "" )+","+
                    "ENTITY_ID"+
                    ")) "+(salted ? "SALT_BUCKETS =4" : "split on('4')");
            conn.createStatement().execute(sql);

            for(int i=1;i<=6;i++) {
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,'"+i+"','"+i+"','"+i+"')");
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,'"+i+"',null,'"+i+"')");
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,null,'"+i+"','"+i+"')");
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,null,null,'"+i+"')");
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('"+i+"','"+i+"','"+i+"','"+i+"')");
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('"+i+"','"+i+"',null,'"+i+"')");
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('"+i+"',null,'"+i+"','"+i+"')");
                conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('"+i+"',null,null,'"+i+"')");
            }
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (null,null,null,'66')");
            conn.commit();

            //groupBy orderPreserving orderBy asc asc

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST";
            ResultSet rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null},{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"},{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null}});

            //groupBy orderPreserving orderBy asc desc

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null},{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"},{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null}});

            //groupBy orderPreserving orderBy desc asc

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null},{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"},{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null}});

            //groupBy orderPreserving orderBy desc desc

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null},{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"},{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"}});

            sql="SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null}});

            //-----groupBy not orderPreserving--

            //groupBy not orderPreserving orderBy asc asc

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null},{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"},{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null}});

            //groupBy not orderPreserving orderBy asc desc

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null},{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1",null},{"1","1"},{"2",null},{"2","2"},{"3",null},{"3","3"},{"4",null},{"4","4"},{"5",null},{"5","5"},{"6",null},{"6","6"},{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1","1"},{"1",null},{"2","2"},{"2",null},{"3","3"},{"3",null},{"4","4"},{"4",null},{"5","5"},{"5",null},{"6","6"},{"6",null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null}});

            //groupBy not orderPreserving orderBy desc asc

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null},{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"},{null,null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null},{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,"5"},{null,"6"},{null,null}});

            //groupBy not orderPreserving orderBy desc desc

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null},{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6",null},{"6","6"},{"5",null},{"5","5"},{"4",null},{"4","4"},{"3",null},{"3","3"},{"2",null},{"2","2"},{"1",null},{"1","1"},{null,null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"}});

            sql="SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6","6"},{"6",null},{"5","5"},{"5",null},{"4","4"},{"4",null},{"3","3"},{"3",null},{"2","2"},{"2",null},{"1","1"},{"1",null},{null,"6"},{null,"5"},{null,"4"},{null,"3"},{null,"2"},{null,"1"},{null,null}});

            //-------test only one return column----------------------------------

            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE ASC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null},{"1"},{"2"},{"3"},{"4"},{"5"},{"6"}});

            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE ASC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"1"},{"2"},{"3"},{"4"},{"5"},{"6"},{null}});

            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE DESC NULLS FIRST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{null},{"6"},{"5"},{"4"},{"3"},{"2"},{"1"}});

            sql="SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE DESC NULLS LAST";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"6"},{"5"},{"4"},{"3"},{"2"},{"1"},{null}});
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