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


public class OrderByIT extends BaseHBaseManagedTimeTableReuseIT {

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
            String tableName = generateRandomString();
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
            String tableName = generateRandomString();
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
            String tableName1 = generateRandomString();
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

            String tableName2 = generateRandomString();
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
        } catch (SQLException e) {
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
            String tableName1 = generateRandomString();
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

            String tableName2 = generateRandomString();
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
            String tableName = generateRandomString();
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
        String tableName = generateRandomString();
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
}