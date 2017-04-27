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

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;


public class SkipScanQueryIT extends ParallelStatsDisabledIT {
    
    private String initIntInTable(Connection conn, List<Integer> data) throws SQLException {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + 
                     "  i INTEGER NOT NULL PRIMARY KEY)";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO " + tableName + " VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (Integer i : data) {
            stmt.setInt(1, i);
            stmt.execute();
        }
        conn.commit();
        return tableName;
    }
    
    private String initVarCharCrossProductInTable(Connection conn, List<String> c1, List<String> c2) throws SQLException {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                     "  s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (s1,s2))";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO " + tableName + " VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (String s1 : c1) {
            for (String s2 : c2) {
                stmt.setString(1, s1);
                stmt.setString(2, s2);
                stmt.execute();
            }
        }
        conn.commit();
        return tableName;
    }
    
    private String initVarCharParallelListInTable(Connection conn, List<String> c1, List<String> c2) throws SQLException {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + 
                     "  s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (s1,s2))";
        conn.createStatement().executeUpdate(ddl);
        
        // Test upsert correct values 
        String query = "UPSERT INTO " + tableName + " VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (int i = 0; i < c1.size(); i++) {
            stmt.setString(1, c1.get(i));
            stmt.setString(2, i < c2.size() ? c2.get(i) : null);
            stmt.execute();
        }
        conn.commit();
        return tableName;
    }
    
    private static String UPSERT_SELECT_AFTER_UPSERT_STATEMENTS =
    		"upsert into %s(c1, c2, c3, c4, v1, v2) values('1001', '91', 's1', '2013-09-26', 28397, 23541);\n" +
    		"upsert into %s(c1, c2, c3, c4, v1, v2) values('1001', '91', 's2', '2013-09-23', 3369, null);\n";
    private String initSelectAfterUpsertTable(Connection conn) throws Exception {
        String tableName = generateUniqueName();
        String ddl = "create table if not exists  " + tableName + " ("
                + "c1 VARCHAR NOT NULL," + "c2 VARCHAR NOT NULL,"
                + "c3 VARCHAR NOT NULL," + "c4 VARCHAR NOT NULL,"
                + "v1 integer," + "v2 integer "
                + "CONSTRAINT PK PRIMARY KEY (c1, c2, c3, c4)" + ")";
        conn.createStatement().execute(ddl);

        // Test upsert correct values
        StringReader reader = new StringReader(String.format(UPSERT_SELECT_AFTER_UPSERT_STATEMENTS, tableName, tableName));
        PhoenixRuntime.executeStatements(conn, reader, Collections.emptyList());
        reader.close();
        conn.commit();
        return tableName;
    }
    
    @Test
    public void testSkipScanFilterQuery() throws Exception {
        String tableName = generateUniqueName();
        String createTableDDL = "CREATE TABLE " + tableName + "(col1 VARCHAR," + "col2 VARCHAR," + "col3 VARCHAR,"
             + "col4 VARCHAR," + "CONSTRAINT pk  " + "PRIMARY KEY (col1,col2,col3,col4))";
        String upsertQuery = "upsert into  " + tableName + "  values(?,?,?,?)";
        String query = "SELECT col1, col2, col3, col4 FROM " + tableName + " WHERE col1 IN ('a','e','f') AND col2 = 'b' AND col4 = '1' ";
        String[] col1Values = { "a", "e.f", "f" };
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        createTestTable(getUrl(), createTableDDL);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertQuery);
            for (String col1Value : col1Values) {
                statement.setString(1, col1Value);
                statement.setString(2, "b");
                statement.setString(3, "");
                statement.setString(4, "1");
                statement.execute();
            }
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "a");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "f");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectAfterUpsertInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initSelectAfterUpsertTable(conn);
        try {
            String query;
            query = "SELECT case when sum(v2)*1.0/sum(v1) is null then 0 else sum(v2)*1.0/sum(v1) END AS val FROM  " + tableName +
            		" WHERE c1='1001' AND c2 = '91' " +
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
        String tableName = initIntInTable(conn,Arrays.asList(2,7,10));
        try {
            String query;
            query = "SELECT i FROM " + tableName + " WHERE i IN (1,2,4,5,7,8,10)";
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
        String tableName = initVarCharParallelListInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM " + tableName + " WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z')";
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
        String tableName = initVarCharCrossProductInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM " + tableName + " WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z')";
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
        String tableName = initVarCharCrossProductInTable(conn,Arrays.asList("d","da","db"),Arrays.asList("m","mc","tt"));
        try {
            String query;
            query = "SELECT s1,s2 FROM " + tableName +
                    " WHERE s1 IN ('a','b','da','db') AND s2 IN ('c','ma','m','mc','ttt','z') " +
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
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("create table " + tableName + "(key_1 char(3) not null, key_2 char(4) not null, v varchar(8)  CONSTRAINT pk PRIMARY KEY (key_1,key_2)) split on('000','100','200')");
            conn.setAutoCommit(true);
            conn.createStatement().execute("upsert into " + tableName + " values('000','aaaa','value_1')");
            conn.createStatement().execute("upsert into " + tableName + " values('000','aabb','value_2')");
            conn.createStatement().execute("upsert into " + tableName + " values('100','aacc','value_3')");
            conn.createStatement().execute("upsert into " + tableName + " values('100','aadd','value_4')");
            conn.createStatement().execute("upsert into " + tableName + " values('200','aaee','value_5')");
            conn.createStatement().execute("upsert into " + tableName + " values('201','aaff','value_6')");
            ResultSet rs = conn.createStatement().executeQuery("select * from " + tableName + " where key_1>='000' and key_1<'200' and key_2>='aabb' and key_2<'aadd'");
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
        String tableName = generateUniqueName();
        try {
            conn.createStatement().execute("create table " + tableName + "(key_1 char(3) not null, key_2 char(4) not null, v varchar(8)  CONSTRAINT pk PRIMARY KEY (key_1,key_2 desc))");
            conn.setAutoCommit(true);
            conn.createStatement().execute("upsert into " + tableName + " values('000','aaaa','value_1')");
            conn.createStatement().execute("upsert into " + tableName + " values('000','aabb','value_2')");
            conn.createStatement().execute("upsert into " + tableName + " values('100','aacc','value_3')");
            conn.createStatement().execute("upsert into " + tableName + " values('100','aadd','value_4')");
            conn.createStatement().execute("upsert into " + tableName + " values('200','aaee','value_5')");
            conn.createStatement().execute("upsert into " + tableName + " values('201','aaff','value_6')");
            ResultSet rs = conn.createStatement().executeQuery("select * from " + tableName + " where key_1>='000' and key_1<'200' and key_2>='aabb' and key_2<'aadd'");
            assertTrue(rs.next());
            assertEquals("000", rs.getString(1));
            assertEquals("aabb", rs.getString(2));
            assertEquals("value_2", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("100", rs.getString(1));
            assertEquals("aacc", rs.getString(2));
            assertEquals("value_3", rs.getString(3));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("select * from " + tableName + " where (key_1,key_2) in (('100','aacc'),('100','aadd'))");
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
    
    @Test
    public void testSkipScanIntersectionAtEnd() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        conn.createStatement()
                .execute(
                    "create table "
                            + tableName
                            + "(pk1 UNSIGNED_TINYINT NOT NULL, pk2 UNSIGNED_TINYINT NOT NULL, pk3 UNSIGNED_TINYINT NOT NULL, kv VARCHAR "
                            + "CONSTRAINT pk PRIMARY KEY (pk1, pk2, pk3)) SPLIT ON ('"
                            + Bytes.toString(new byte[] { 1, 1 }) + "', '"
                            + Bytes.toString(new byte[] { 2, 1 }) + "', '"
                            + Bytes.toString(new byte[] { 3, 1 }) + "')");
        
        conn.createStatement().execute("upsert into " + tableName + " values (0, 1, 1, 'a')");
        conn.createStatement().execute("upsert into " + tableName + " values (1, 1, 1, 'a')");
        conn.createStatement().execute("upsert into " + tableName + " values (2, 1, 1, 'a')");
        conn.createStatement().execute("upsert into " + tableName + " values (3, 1, 1, 'a')");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery("select count(kv) from " + tableName + " where pk1 in (0, 1, 2, 3) AND pk2 = 1");
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void testSkipScanFilterWhenTableHasMultipleColumnFamilies() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String tableName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        try {
            TestUtil.createMultiCFTestTable(conn , fullTableName, null);
            populateMultiCFTestTable(fullTableName);
            String upsert = "UPSERT INTO " + fullTableName
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "varchar4");
            stmt.setString(2, "char1");
            stmt.setInt(3, 1);
            stmt.setLong(4, 1L);
            stmt.setBigDecimal(5, new BigDecimal("1.1"));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 2);
            stmt.setLong(9, 2L);
            stmt.setBigDecimal(10, new BigDecimal("2.1"));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 3);
            stmt.setLong(14, 3L);
            stmt.setBigDecimal(15, new BigDecimal("3.1"));
            stmt.setDate(16, null);
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar5");
            stmt.setString(2, "char2");
            stmt.setInt(3, 2);
            stmt.setLong(4, 2L);
            stmt.setBigDecimal(5, new BigDecimal("2.2"));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 3);
            stmt.setLong(9, 3L);
            stmt.setBigDecimal(10, new BigDecimal("3.2"));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 4);
            stmt.setLong(14, 4L);
            stmt.setBigDecimal(15, new BigDecimal("4.2"));
            stmt.setDate(16, null);
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar6");
            stmt.setString(2, "char3");
            stmt.setInt(3, 3);
            stmt.setLong(4, 3L);
            stmt.setBigDecimal(5, new BigDecimal("3.3"));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 4);
            stmt.setLong(9, 4L);
            stmt.setBigDecimal(10, new BigDecimal("4.3"));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 5);
            stmt.setLong(14, 5L);
            stmt.setBigDecimal(15, new BigDecimal("5.3"));
            stmt.setDate(16, null);
            stmt.executeUpdate();
            conn.commit();
            String query = "SELECT char_col1, int_col1, long_col2 from " + fullTableName + " where varchar_pk in ('varchar3','varchar6')";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }    
    
    @Test
    public void testOrPKWithAndNonPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        try {
            conn.createStatement().execute("create table " + tableName + "(ID varchar primary key,company varchar)");
            conn.setAutoCommit(true);
            conn.createStatement().execute("upsert into " + tableName + " values('i1','c1')");
            conn.createStatement().execute("upsert into " + tableName + " values('i2','c2')");
            conn.createStatement().execute("upsert into " + tableName + " values('i3','c3')");
            ResultSet rs = conn.createStatement().executeQuery("select * from " + tableName + " where ID = 'i1' or (ID = 'i2' and company = 'c3')");
            assertTrue(rs.next());
            assertEquals("i1", rs.getString(1));
            assertEquals("c1", rs.getString(2));
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNullInfiniteLoop() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            conn.setAutoCommit(true);
            conn.createStatement().execute(
              "create table " + tableName +
              "("+
                     "CREATETIME VARCHAR,"+
                     "ACCOUNTID VARCHAR,"+
                     "SERVICENAME VARCHAR,"+
                     "SPAN.APPID VARCHAR,"+
                     "CONSTRAINT pk PRIMARY KEY(CREATETIME,ACCOUNTID,SERVICENAME)"+
              ")");

            conn.createStatement().execute("upsert into " + tableName + "(CREATETIME,SERVICENAME,SPAN.APPID) values('20160116141006','servlet','android')");
            conn.createStatement().execute("upsert into " + tableName + "(CREATETIME,ACCOUNTID,SERVICENAME,SPAN.APPID) values('20160116151006','2404787','jdbc','ios')");
            ResultSet rs = conn.createStatement().executeQuery("select * from " + tableName + " where CREATETIME>='20160116121006' and  CREATETIME<='20160116181006' and ACCOUNTID='2404787'");
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }
}
