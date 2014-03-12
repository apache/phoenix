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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

public class SkipScanQueryIT extends BaseHBaseManagedTimeIT {
    
    private void initIntInTable(Connection conn, List<Integer> data) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS inTest (" + 
                     "  i INTEGER NOT NULL PRIMARY KEY)";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO inTest VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (Integer i : data) {
            stmt.setInt(1, i);
            stmt.execute();
        }
        conn.commit();
    }
    
    private void initVarCharCrossProductInTable(Connection conn, List<String> c1, List<String> c2) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS inVarTest (" + 
                     "  s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (s1,s2))";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO inVarTest VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (String s1 : c1) {
            for (String s2 : c2) {
                stmt.setString(1, s1);
                stmt.setString(2, s2);
                stmt.execute();
            }
        }
        conn.commit();
    }
    
    private void initVarCharParallelListInTable(Connection conn, List<String> c1, List<String> c2) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS inVarTest (" + 
                     "  s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (s1,s2))";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO inVarTest VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (int i = 0; i < c1.size(); i++) {
            stmt.setString(1, c1.get(i));
            stmt.setString(2, i < c2.size() ? c2.get(i) : null);
            stmt.execute();
        }
        conn.commit();
    }
    
    private static final String UPSERT_SELECT_AFTER_UPSERT_STATEMENTS = 
    		"upsert into table1(c1, c2, c3, c4, v1, v2) values('1001', '91', 's1', '2013-09-26', 28397, 23541);\n" + 
    		"upsert into table1(c1, c2, c3, c4, v1, v2) values('1001', '91', 's2', '2013-09-23', 3369, null);\n";
    private void initSelectAfterUpsertTable(Connection conn) throws Exception {
        String ddl = "create table if not exists table1("
                + "c1 VARCHAR NOT NULL," + "c2 VARCHAR NOT NULL,"
                + "c3 VARCHAR NOT NULL," + "c4 VARCHAR NOT NULL,"
                + "v1 integer," + "v2 integer "
                + "CONSTRAINT PK PRIMARY KEY (c1, c2, c3, c4)" + ")";
        conn.createStatement().execute(ddl);

        // Test upsert correct values
        StringReader reader = new StringReader(UPSERT_SELECT_AFTER_UPSERT_STATEMENTS);
        PhoenixRuntime.executeStatements(conn, reader, Collections.emptyList());
        reader.close();
        conn.commit();
    }
    
    @Test
    public void testSelectAfterUpsertInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initSelectAfterUpsertTable(conn);
        try {
            String query;
            query = "SELECT case when sum(v2)*1.0/sum(v1) is null then 0 else sum(v2)*1.0/sum(v1) END AS val FROM table1 " +
            		"WHERE c1='1001' AND c2 = '91' " +
            		"AND c3 IN ('s1','s2') AND c4='2013-09-24'";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        } finally {
            conn.close();
        }
    }
    @Test
    public void testInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initIntInTable(conn,Arrays.asList(2,7,10));
        try {
            String query;
            query = "SELECT i FROM inTest WHERE i IN (1,2,4,5,7,8,10)";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarCharParallelListInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initVarCharParallelListInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM inVarTest WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z')";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testVarCharXInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initVarCharCrossProductInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM inVarTest WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z')";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("m", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("db", rs.getString(1));
            assertEquals("m", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("db", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarCharXIntInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        initVarCharCrossProductInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM inVarTest " +
                    "WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z') " +
                    "AND s1 > 'd' AND s1 < 'db' AND s2 > 'm'";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("da", rs.getString(1));
            assertEquals("mc", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testPreSplitCompositeFixedKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("create table test(key_1 char(3) not null, key_2 char(4) not null, v varchar(8)  CONSTRAINT pk PRIMARY KEY (key_1,key_2)) split on('000','100','200')");
            conn.setAutoCommit(true);
            conn.createStatement().execute("upsert into test values('000','aaaa','value_1')");
            conn.createStatement().execute("upsert into test values('000','aabb','value_2')");
            conn.createStatement().execute("upsert into test values('100','aacc','value_3')");
            conn.createStatement().execute("upsert into test values('100','aadd','value_4')");
            conn.createStatement().execute("upsert into test values('200','aaee','value_5')");
            conn.createStatement().execute("upsert into test values('201','aaff','value_6')");
            ResultSet rs = conn.createStatement().executeQuery("select * from test where key_1>='000' and key_1<'200' and key_2>='aabb' and key_2<'aadd'");
            assertTrue(rs.next());
            assertEquals("000", rs.getString(1));
            assertEquals("aabb", rs.getString(2));
            assertEquals("value_2", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("100", rs.getString(1));
            assertEquals("aacc", rs.getString(2));
            assertEquals("value_3", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    
    @Test
    public void testInWithDescKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("create table test(key_1 char(3) not null, key_2 char(4) not null, v varchar(8)  CONSTRAINT pk PRIMARY KEY (key_1,key_2 desc))");
            conn.setAutoCommit(true);
            conn.createStatement().execute("upsert into test values('000','aaaa','value_1')");
            conn.createStatement().execute("upsert into test values('000','aabb','value_2')");
            conn.createStatement().execute("upsert into test values('100','aacc','value_3')");
            conn.createStatement().execute("upsert into test values('100','aadd','value_4')");
            conn.createStatement().execute("upsert into test values('200','aaee','value_5')");
            conn.createStatement().execute("upsert into test values('201','aaff','value_6')");
            ResultSet rs = conn.createStatement().executeQuery("select * from test where key_1>='000' and key_1<'200' and key_2>='aabb' and key_2<'aadd'");
            assertTrue(rs.next());
            assertEquals("000", rs.getString(1));
            assertEquals("aabb", rs.getString(2));
            assertEquals("value_2", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("100", rs.getString(1));
            assertEquals("aacc", rs.getString(2));
            assertEquals("value_3", rs.getString(3));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("select * from test where (key_1,key_2) in (('100','aacc'),('100','aadd'))");
            assertTrue(rs.next());
            assertEquals("100", rs.getString(1));
            assertEquals("aadd", rs.getString(2));
            assertEquals("value_4", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("100", rs.getString(1));
            assertEquals("aacc", rs.getString(2));
            assertEquals("value_3", rs.getString(3));
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    
}
