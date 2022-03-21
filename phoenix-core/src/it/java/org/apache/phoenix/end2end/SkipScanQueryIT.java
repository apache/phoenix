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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
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

    @Test
    public void testSkipScanQueryWhenSplitKeyIsSmaller() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            StringBuffer buf =
                    new StringBuffer("CREATE TABLE IF NOT EXISTS " + tableName
                            + "(ORGANIZATION_ID CHAR(15) NOT NULL,"
                            + "FEED_ITEM_ID CHAR(15) NOT NULL," + "EXTENSION VARCHAR(128) NOT NULL,"
                            + "CREATED_TIME TIMESTAMP," + "LAST_UPDATE TIMESTAMP,"
                            + "LAST_ACCESSED TIMESTAMP," + "VERSION INTEGER,"
                            + "DATA.PAYLOAD VARCHAR(512000)" + "CONSTRAINT PK PRIMARY KEY" + "("
                            + "        ORGANIZATION_ID," + "        FEED_ITEM_ID,"
                            + "        EXTENSION" + ")" + ")");
            conn.createStatement().execute(buf.toString());
            String upsert =
                    "UPSERT INTO " + tableName
                            + " (ORGANIZATION_ID, FEED_ITEM_ID, EXTENSION) VALUES (?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002MK5Uu");
            stmt.setString(3, "FI");
            stmt.executeUpdate();
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002MK5Uu");
            stmt.setString(3, "T0");
            stmt.executeUpdate();
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002QWbP0");
            stmt.setString(3, "FI");
            stmt.executeUpdate();
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002QWbP0");
            stmt.setString(3, "T0");
            stmt.executeUpdate();
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002QXXL2");
            stmt.setString(3, "FI");
            stmt.executeUpdate();
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002QXXL2");
            stmt.setString(3, "T0");
            stmt.executeUpdate();
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002RhvtQ");
            stmt.setString(3, "FI");
            stmt.executeUpdate();
            stmt.setString(1, "00Do0000000a8w1");
            stmt.setString(2, "0D5o000002RhvtQ");
            stmt.setString(3, "T0");
            stmt.executeUpdate();
            conn.commit();
            try (Admin admin =
                    conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                /*
                 * The split key is 27 bytes instead of at least 30 bytes (CHAR(15) + CHAR(15)).
                 * Note that we cannot use the phoenix way of giving split points in the ddl because
                 * it ends up padding the split point bytes to 30.
                 */
                byte[] smallSplitKey = Bytes.toBytes("00Do0000000a8w10D5o000002Rhv");
                admin.split(TableName.valueOf(tableName), smallSplitKey);
            }
            ResultSet rs =
                    conn.createStatement().executeQuery("SELECT EXTENSION FROM " + tableName
                            + " WHERE " + "ORGANIZATION_ID = '00Do0000000a8w1' AND "
                            + "FEED_ITEM_ID IN "
                            + "('0D5o000002MK5Uu','0D5o000002QWbP0','0D5o000002QXXL2','0D5o000002RhvtQ') ORDER BY ORGANIZATION_ID, FEED_ITEM_ID, EXTENSION");
            assertTrue(rs.next());
            assertEquals("FI", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("T0", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("FI", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("T0", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("FI", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("T0", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("FI", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("T0", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSkipScanJoinOptimization() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            String viewName = generateUniqueName();
            String idxName = "IDX_" + tableName;
            conn.setAutoCommit(true);
            conn.createStatement().execute(
                    "create table " + tableName + " (PK1 INTEGER NOT NULL, PK2 INTEGER NOT NULL, " +
                            " ID1 INTEGER, ID2 INTEGER CONSTRAINT PK PRIMARY KEY(PK1 , PK2))SALT_BUCKETS = 4");
            conn.createStatement().execute("upsert into " + tableName + " values (1,1,1,1)");
            conn.createStatement().execute("upsert into " + tableName + " values (2,2,2,2)");
            conn.createStatement().execute("upsert into " + tableName + " values (2,3,1,2)");
            conn.createStatement().execute("create view " + viewName + " as select * from " +
                    tableName + " where PK1 in (1,2)");
            conn.createStatement().execute("create index " + idxName + " on " + viewName + " (ID1)");
            ResultSet rs = conn.createStatement().executeQuery("select /*+ INDEX(" + viewName + " " + idxName + ") */ * from " + viewName + " where ID1 = 1 ");
            assertTrue(rs.next());
        }
    }

    @Test
    public void testOrWithMixedOrderPKs() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName +
                    " (COL1 VARCHAR, COL2 VARCHAR CONSTRAINT PK PRIMARY KEY (COL1 DESC, COL2)) ");

            // this is the order the rows will be stored on disk
            stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('8', 'a')");
            stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('6', 'a')");
            stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('23', 'b')");
            stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('23', 'bb')");
            stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('2', 'a')");
            stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('17', 'a')");


            // test values in the skip scan filter which are prefixes of another value, eg 1,12 and 2,23
            String sql = "select COL1, COL2 from " + tableName + " where COL1='1' OR COL1='2' OR COL1='3' OR COL1='4' " +
                    "OR COL1='5' OR COL1='6' OR COL1='8' OR COL1='17' OR COL1='12' OR COL1='23'";

            ResultSet rs = stmt.executeQuery(sql);
            assertTrue(rs.next());

            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertEquals("Expected a single scan ", 1, plan.getScans().size());
            assertEquals("Expected a single scan ", 1, plan.getScans().get(0).size());
            Scan scan = plan.getScans().get(0).get(0);
            FilterList filterList = (FilterList)scan.getFilter();
            boolean skipScanFilterFound = false;
            for (Filter f : filterList.getFilters()) {
                if (f instanceof SkipScanFilter) {
                    skipScanFilterFound = true;
                    SkipScanFilter skipScanFilter = (SkipScanFilter) f;
                    assertEquals("Expected a single slot ", skipScanFilter.getSlots().size(), 1);
                    assertEquals("Number of key ranges should match number of or filters ",
                            skipScanFilter.getSlots().get(0).size(), 10);
                }
            }
            assertTrue("Should use skip scan filter", skipScanFilterFound);

            assertEquals("8", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("6", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("23", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("23", rs.getString(1));
            assertEquals("bb", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("17", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testRVCClipBug5753() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();

            String sql = "CREATE TABLE "+tableName+" (" +
                         " pk1 INTEGER NOT NULL , " +
                         " pk2 INTEGER NOT NULL, " +
                         " pk3 INTEGER NOT NULL, " +
                         " pk4 INTEGER NOT NULL, " +
                         " pk5 INTEGER NOT NULL, " +
                         " pk6 INTEGER NOT NULL, " +
                         " pk7 INTEGER NOT NULL, " +
                         " pk8 INTEGER NOT NULL, " +
                         " v INTEGER, CONSTRAINT PK PRIMARY KEY(pk1,pk2,pk3 desc,pk4,pk5,pk6 desc,pk7,pk8))";;

            stmt.execute(sql);

            stmt.execute(
                    "UPSERT INTO " + tableName + " (pk1,pk2,pk3,pk4,pk5,pk6,pk7,pk8,v) "+
                    "VALUES (1,3,4,10,2,6,7,9,1)");

            sql = "select pk1,pk2,pk3,pk4 from " + tableName +
                 " where (pk1 >=1 and pk1<=2) and (pk2>=3 and pk2<=4) and (pk3,pk4) < (5,7) order by pk1,pk2,pk3";

            ResultSet rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,3,4,10}});

            sql = "select * from " + tableName +
                    " where (pk1 >=1 and pk1<=2) and (pk2>=2 and pk2<=3) and (pk3,pk4) < (5,7) and "+
                    " (pk5,pk6,pk7) < (5,6,7) and pk8 > 8 order by pk1,pk2,pk3";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,3,4,10}});

            stmt.execute(
                    "UPSERT INTO " + tableName + " (pk1,pk2,pk3,pk4,pk5,pk6,pk7,pk8,v) "+
                    "VALUES (1,3,2,10,5,4,3,9,1)");
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,3,2,10},{1,3,4,10}});

            stmt.execute(
                    "UPSERT INTO " + tableName + " (pk1,pk2,pk3,pk4,pk5,pk6,pk7,pk8,v) "+
                    "VALUES (1,3,5,6,4,7,8,9,1)");
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,3,2,10},{1,3,4,10},{1,3,5,6}});

            sql = "select * from " + tableName +
                    " where (pk1 >=1 and pk1<=2) and (pk2>=2 and pk2<=3) and (pk3,pk4) in ((5,6),(2,10)) and "+
                    " (pk5,pk6,pk7) in ((4,7,8),(5,4,3)) and pk8 > 8 order by pk1,pk2,pk3";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,3,2,10},{1,3,5,6}});
        }
    }

    @Test
    public void testRVCBug6659() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, PK2 INTEGER NOT NULL, " +
                "PK3 BIGINT NOT NULL CONSTRAINT PK PRIMARY KEY (PK1,PK2,PK3))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0,1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',1,1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',2,1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',3,1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',3,2)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',4,1)");
            conn.commit();
            String query = "SELECT * FROM " + tableName +
                    " WHERE (PK1 = 'a') AND (PK1,PK2,PK3) <= ('a',3,1)";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            int rowCnt = 0;
            do {
                rowCnt++;
            } while (rs.next());
            assertEquals(4, rowCnt);
        }
    }

    @Test
    public void testRVCBug6659_Delete() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (PK1 DOUBLE NOT NULL, PK2 INTEGER NOT NULL, " +
                " CONSTRAINT PK PRIMARY KEY (PK1,PK2))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(10.0,10)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(20.0,20)");
            conn.commit();
            String query = "DELETE FROM " + tableName +
                    " WHERE (PK1,PK2) IN ((10.0, 10),(20.0, 20))";
            conn.createStatement().execute(query);
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testRVCBug6659_AllVarchar() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, " +
                "PK3 BIGINT NOT NULL CONSTRAINT PK PRIMARY KEY (PK1,PK2,PK3))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a','0',1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a','1',1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a','3',1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a','3',2)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a','4',1)");
            conn.commit();
            String query = "SELECT * FROM " + tableName +
                    " WHERE (PK1 = 'a') AND (PK1,PK2,PK3) <= ('a','3',1)";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            int rowCnt = 0;
            do {
                rowCnt++;
            } while (rs.next());
            assertEquals(3, rowCnt);
        }
    }

    @Test
    public void testRVCBug6659_IntVarcharBigint() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (PK1 INTEGER NOT NULL, PK2 VARCHAR NOT NULL, " +
                "PK3 BIGINT NOT NULL CONSTRAINT PK PRIMARY KEY (PK1,PK2,PK3))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,'0',1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,'1',1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,'3',1)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,'3',2)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,'4',1)");
            conn.commit();
            String query = "SELECT * FROM " + tableName +
                    " WHERE (PK1 = 1) AND (PK1,PK2,PK3) <= (1,'3',1)";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            int rowCnt = 0;
            do {
                rowCnt++;
            } while (rs.next());
            assertEquals(3, rowCnt);
        }
    }

    @Test
    public void testRvcInListDelete() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String viewName = "VW_" + generateUniqueName();
        String baseTableDDL = "CREATE TABLE IF NOT EXISTS "+ tableName + " (\n" +
                "    ORG_ID CHAR(15) NOT NULL,\n" +
                "    KEY_PREFIX CHAR(3) NOT NULL,\n" +
                "    DATE1 DATE,\n" +
                "    CONSTRAINT PK PRIMARY KEY (\n" +
                "        ORG_ID,\n" +
                "        KEY_PREFIX\n" +
                "    )\n" +
                ") VERSIONS=1, MULTI_TENANT=true, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0";
        String globalViewDDL = "CREATE VIEW  " + viewName +  "(\n" +
                "    DOUBLE1 DECIMAL(12, 3) NOT NULL,\n" +
                "    INT1 BIGINT NOT NULL,\n" +
                "    DATE_TIME1 DATE,\n" +
                "    COS_ID CHAR(15),\n" +
                "    TEXT1 VARCHAR,\n" +
                "    CONSTRAINT PKVIEW PRIMARY KEY\n" +
                "    (\n" +
                "        DOUBLE1 DESC, INT1\n" +
                "    )\n" +
                ")\n" +
                "AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = '08K'";
        String tenantViewName = tableName + ".\"08K\"";
        String tenantViewDDL = "CREATE VIEW " + tenantViewName + " AS SELECT * FROM " + viewName;
        createTestTable(getUrl(), baseTableDDL);
        createTestTable(getUrl(), globalViewDDL);

        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "tenant1");
        String upsertOne = "UPSERT INTO " + tenantViewName + " (DOUBLE1, INT1) VALUES (10.0,10)";
        String upsertTwo = "UPSERT INTO " + tenantViewName + " (DOUBLE1, INT1) VALUES (20.0,20)";
        String deletion = "DELETE FROM " + tenantViewName + " WHERE (DOUBLE1,INT1) IN ((10.0, 10),(20.0, 20))";
        String deletionNotPkOrder = "DELETE FROM " + tenantViewName + " WHERE (INT1,DOUBLE1) IN ((10, 10.0),(20, 20.0))";

        try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
            tenantConn.createStatement().execute(tenantViewDDL);
            tenantConn.createStatement().execute(upsertOne);
            tenantConn.createStatement().execute(upsertTwo);
            tenantConn.commit();

            ResultSet rs = tenantConn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tenantViewName);
            if (rs.next()) {
                int count = rs.getInt(1);
                assertEquals(2, count);
            } else {
                fail();
            }
            tenantConn.createStatement().execute(deletion);
            tenantConn.commit();
            rs = tenantConn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tenantViewName);
            if (rs.next()) {
                int count = rs.getInt(1);
                assertEquals(0, count);
            } else {
                fail();
            }

            tenantConn.createStatement().execute(upsertOne);
            tenantConn.createStatement().execute(upsertTwo);
            tenantConn.commit();
            tenantConn.createStatement().execute(deletionNotPkOrder);
            tenantConn.commit();
            rs = tenantConn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tenantViewName);
            if (rs.next()) {
                int count = rs.getInt(1);
                assertEquals(0, count);
            } else {
                fail();
            }
        }

    }

    @Test
    public void testRvcInListDeleteBothDesc() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String viewName = "VW_" + generateUniqueName();
        String baseTableDDL = "CREATE TABLE IF NOT EXISTS "+ tableName + " (\n" +
                "    ORG_ID CHAR(15) NOT NULL,\n" +
                "    KEY_PREFIX CHAR(3) NOT NULL,\n" +
                "    DATE1 DATE,\n" +
                "    CONSTRAINT PK PRIMARY KEY (\n" +
                "        ORG_ID,\n" +
                "        KEY_PREFIX\n" +
                "    )\n" +
                ") VERSIONS=1, MULTI_TENANT=true, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0";
        String globalViewDDL = "CREATE VIEW  " + viewName +  "(\n" +
                "    DOUBLE1 DECIMAL(12, 3) NOT NULL,\n" +
                "    INT1 BIGINT NOT NULL,\n" +
                "    DATE_TIME1 DATE,\n" +
                "    COS_ID CHAR(15),\n" +
                "    TEXT1 VARCHAR,\n" +
                "    CONSTRAINT PKVIEW PRIMARY KEY\n" +
                "    (\n" +
                "        DOUBLE1 DESC, INT1 DESC\n" +
                "    )\n" +
                ")\n" +
                "AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = '08K'";
        String tenantViewName = tableName + ".\"08K\"";
        String tenantViewDDL = "CREATE VIEW " + tenantViewName + " AS SELECT * FROM " + viewName;
        createTestTable(getUrl(), baseTableDDL);
        createTestTable(getUrl(), globalViewDDL);

        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "tenant1");
        String upsertOne = "UPSERT INTO " + tenantViewName + " (DOUBLE1, INT1) VALUES (10.0,10)";
        String upsertTwo = "UPSERT INTO " + tenantViewName + " (DOUBLE1, INT1) VALUES (20.0,20)";
        String deletion = "DELETE FROM " + tenantViewName + " WHERE (DOUBLE1,INT1) IN ((10.0, 10),(20.0, 20))";
        String deletionNotPkOrder = "DELETE FROM " + tenantViewName + " WHERE (INT1,DOUBLE1) IN ((10, 10.0),(20, 20.0))";

        try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
            tenantConn.createStatement().execute(tenantViewDDL);
            tenantConn.createStatement().execute(upsertOne);
            tenantConn.createStatement().execute(upsertTwo);
            tenantConn.commit();

            ResultSet rs = tenantConn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tenantViewName);
            if (rs.next()) {
                int count = rs.getInt(1);
                assertEquals(2, count);
            } else {
                fail();
            }
            tenantConn.createStatement().execute(deletion);
            tenantConn.commit();
            rs = tenantConn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tenantViewName);
            if (rs.next()) {
                int count = rs.getInt(1);
                assertEquals(0, count);
            } else {
                fail();
            }

            tenantConn.createStatement().execute(upsertOne);
            tenantConn.createStatement().execute(upsertTwo);
            tenantConn.commit();
            tenantConn.createStatement().execute(deletionNotPkOrder);
            tenantConn.commit();
            rs = tenantConn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tenantViewName);
            if (rs.next()) {
                int count = rs.getInt(1);
                assertEquals(0, count);
            } else {
                fail();
            }
        }

    }

    @Test
    public void testPHOENIX6669() throws SQLException {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS "+ tableName + " (\n" +
                "    PK1 VARCHAR NOT NULL,\n" +
                "    PK2 BIGINT NOT NULL,\n" +
                "    PK3 BIGINT NOT NULL,\n" +
                "    PK4 VARCHAR NOT NULL,\n" +
                "    COL1 BIGINT,\n" +
                "    COL2 INTEGER,\n" +
                "    COL3 VARCHAR,\n" +
                "    COL4 VARCHAR,    CONSTRAINT PK PRIMARY KEY\n" +
                "    (\n" +
                "        PK1,\n" +
                "        PK2,\n" +
                "        PK3,\n" +
                "        PK4\n" +
                "    )\n" +
                ")";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " (PK1, PK4, COL1, PK2, COL2, PK3, COL3, COL4)" +
                    "            VALUES ('xx', 'xid1', 0, 7, 7, 7, 'INSERT', null)");
            conn.commit();

            ResultSet rs = conn.createStatement().executeQuery("select PK2 from "+  tableName
                    +  " where (PK1 = 'xx') and (PK1, PK2, PK3) > ('xx', 5, 2) "
                    +  " and (PK1, PK2, PK3) <= ('xx', 5, 2)");
            assertFalse(rs.next());
        }
    }

    @Test
    public void testKeyRangesContainsAllValues() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                     " vdate VARCHAR, " +
                     " tab VARCHAR, " +
                     " dev TINYINT NOT NULL, " +
                     " app VARCHAR, " +
                     " target VARCHAR, " +
                     " channel VARCHAR, " +
                     " one VARCHAR, " +
                     " two VARCHAR, " +
                     " count1 INTEGER, " +
                     " count2 INTEGER, " +
                     " CONSTRAINT PK PRIMARY KEY (vdate,tab,dev,app,target,channel,one,two))";

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('2018-02-14','channel_agg',2,null,null,'A004',null,null,2,2)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('2018-02-14','channel_agg',2,null,null,null,null,null,2,2)");
            conn.commit();

            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + tableName +
                " WHERE dev = 2 AND vdate BETWEEN '2018-02-10' AND '2019-02-19'" +
                " AND tab = 'channel_agg' AND channel='A004'");

            assertTrue(rs.next());
            assertEquals("2018-02-14", rs.getString(1));
            assertFalse(rs.next());
        }
    }
}
