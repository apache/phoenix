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

import static org.apache.phoenix.util.TestUtil.closeStatement;
import static org.apache.phoenix.util.TestUtil.closeStmtAndConn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class UpsertValuesIT extends BaseClientManagedTimeIT {
    @Test
    public void testGroupByWithLimitOverRowKey() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),TestUtil.PTSDB_NAME,TestUtil.PTSDB_NAME, null, ts-2, null);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.PTSDB_NAME + " (inst,host,\"DATE\") VALUES(?,'b',CURRENT_DATE())");
        stmt.setString(1, "a");
        stmt.execute();
        stmt.execute();
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.execute();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select count(1) from " + TestUtil.PTSDB_NAME + " group by inst limit 1");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("select inst from " + TestUtil.PTSDB_NAME + " where inst > 'a' group by inst limit 1");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testUpsertDateValues() throws Exception {
        long ts = nextTimestamp();
        Date now = new Date(System.currentTimeMillis());
        ensureTableCreated(getUrl(),TestUtil.PTSDB_NAME,TestUtil.PTSDB_NAME,null, ts-2, null);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String dateString = "1999-01-01 02:00:00";
        PreparedStatement upsertStmt = conn.prepareStatement("upsert into ptsdb(inst,host,\"DATE\") values('aaa','bbb',to_date('" + dateString + "'))");
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsertStmt = conn.prepareStatement("upsert into ptsdb(inst,host,\"DATE\") values('ccc','ddd',current_date())");
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 1
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT \"DATE\",current_date() FROM ptsdb";
        ResultSet rs = conn.createStatement().executeQuery(select);
        Date then = new Date(System.currentTimeMillis());
        assertTrue(rs.next());
        Date date = DateUtil.parseDate(dateString);
        assertEquals(date,rs.getDate(1));
        assertTrue(rs.next());
        assertTrue(rs.getDate(1).after(now) && rs.getDate(1).before(then));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithExpression() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),"IntKeyTest","IntKeyTest", null, ts-2, null);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(-1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsert = "UPSERT INTO IntKeyTest VALUES(1+2)";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 1
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT i FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(-1,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithDate() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table UpsertDateTest (k VARCHAR not null primary key,\"DATE\" DATE)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into UpsertDateTest values ('a',to_date('2013-06-08 00:00:00'))");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k,to_char(\"DATE\") from UpsertDateTest");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("2013-06-08 00:00:00.000", rs.getString(2));
    }
    
    @Test
    public void testUpsertValuesWithDescDecimal() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table UpsertDecimalDescTest (k DECIMAL(12,3) NOT NULL PRIMARY KEY DESC)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into UpsertDecimalDescTest values (0.0)");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k from UpsertDecimalDescTest");
        assertTrue(rs.next());
        assertEquals(0.0, rs.getDouble(1), 0.001);
    }

    @Test
    public void testUpsertRandomValues() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table UpsertRandomTest (k UNSIGNED_DOUBLE not null primary key, v1 UNSIGNED_DOUBLE, v2 UNSIGNED_DOUBLE, v3 UNSIGNED_DOUBLE, v4 UNSIGNED_DOUBLE)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into UpsertRandomTest values (RAND(), RAND(), RAND(1), RAND(2), RAND(1))");
        conn.createStatement().execute("upsert into UpsertRandomTest values (RAND(), RAND(), RAND(1), RAND(2), RAND(1))");
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k,v1,v2,v3,v4 from UpsertRandomTest");
        assertTrue(rs.next());
        double rand0 = rs.getDouble(1);
        double rand1 = rs.getDouble(3);
        double rand2 = rs.getDouble(4);
        assertTrue(rs.getDouble(1) != rs.getDouble(2));
        assertTrue(rs.getDouble(2) != rs.getDouble(3));
        assertTrue(rand1 == rs.getDouble(5));
        assertTrue(rs.getDouble(4) != rs.getDouble(5));
        assertTrue(rs.next());
        assertTrue(rand0 != rs.getDouble(1));
        assertTrue(rand1 == rs.getDouble(3) && rand1 == rs.getDouble(5));
        assertTrue(rand2 == rs.getDouble(4));
        conn.close();
    }

    @Test
    public void testUpsertVarCharWithMaxLength() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table phoenix_uuid_mac (mac_md5 VARCHAR not null primary key,raw_mac VARCHAR)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into phoenix_uuid_mac values ('00000000591','a')");
        conn.createStatement().execute("upsert into phoenix_uuid_mac values ('000000005919','b')");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select max(mac_md5) from phoenix_uuid_mac");
        assertTrue(rs.next());
        assertEquals("000000005919", rs.getString(1));
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+15));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into phoenix_uuid_mac values ('000000005919adfasfasfsafdasdfasfdasdfdasfdsafaxxf1','b')");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+20));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("select max(mac_md5) from phoenix_uuid_mac");
        assertTrue(rs.next());
        assertEquals("000000005919adfasfasfsafdasdfasfdasdfdasfdsafaxxf1", rs.getString(1));
        conn.close();
    }
    
    @Test
    public void testUpsertValuesWithDescExpression() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table UpsertWithDesc (k VARCHAR not null primary key desc)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into UpsertWithDesc values (to_char(100))");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select to_number(k) from UpsertWithDesc");
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithMoreValuesThanNumColsInTable() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.createStatement();
            stmt.execute("create table UpsertWithDesc (k VARCHAR not null primary key desc)");
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.createStatement();
            stmt.execute("upsert into UpsertWithDesc values (to_char(100), to_char(100), to_char(100))");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH.getErrorCode(),e.getErrorCode());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testTimestampSerializedAndDeserializedCorrectly() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table UpsertTimestamp (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        Timestamp ts1 = new Timestamp(120055);
        ts1.setNanos(ts1.getNanos() + 60);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertTimestamp values (1, ?)");
            stmt.setTimestamp(1, ts1);
            stmt.executeUpdate();
            conn.commit();
         } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select t from UpsertTimestamp where t = ?");
            stmt.setTimestamp(1, ts1);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts1, rs.getTimestamp(1));
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testTimestampAddSubtractArithmetic() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table UpsertTimestamp (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        Timestamp ts1 = new Timestamp(120550);
        int extraNanos = 60;
        ts1.setNanos(ts1.getNanos() + extraNanos);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertTimestamp values (1, ?)");
            stmt.setTimestamp(1, ts1);
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select t from UpsertTimestamp LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts1, rs.getTimestamp(1));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(stmt, conn);
        }

        BigDecimal msInDay = BigDecimal.valueOf(1*24*60*60*1000);
        BigDecimal nanosInDay = BigDecimal.valueOf(1*24*60*60*1000).multiply(BigDecimal.valueOf(1000000));
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select 500.0/(1*24*60*60*1000) c1, 10.0/(1*24*60*60*1000*1000000) c2  from UpsertTimestamp LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal c1 = rs.getBigDecimal(1);
            BigDecimal rc1 = c1.multiply(msInDay).setScale(0,RoundingMode.HALF_UP);
            BigDecimal c2 = rs.getBigDecimal(2);
            BigDecimal rc2 = c2.multiply(nanosInDay).setScale(0,RoundingMode.HALF_UP);
            assertTrue(BigDecimal.valueOf(500).compareTo(rc1) == 0);
            assertTrue(BigDecimal.valueOf(10).compareTo(rc2) == 0);
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(stmt, conn);
        }

        Timestamp ts2 = new Timestamp(ts1.getTime() + 500);
        ts2.setNanos(ts2.getNanos() + extraNanos + 10); //setting the extra nanos as well as what spilled over from timestamp millis.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select (t + (500.0/(1*24*60*60*1000) + 10.0/(1*24*60*60*1000*1000000)))  from UpsertTimestamp LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts2, rs.getTimestamp(1));
        } finally {
            closeStatement(stmt);
        }
        
        ts2 = new Timestamp(ts1.getTime() - 250);
        ts2.setNanos(ts2.getNanos() + extraNanos - 30); //setting the extra nanos as well as what spilled over from timestamp millis.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            stmt = conn.prepareStatement("select (t - (250.0/(1*24*60*60*1000) + 30.0/(1*24*60*60*1000*1000000)))  from UpsertTimestamp LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts2, rs.getTimestamp(1));
        } finally {
            closeStatement(stmt);
        }
        
        ts2 = new Timestamp(ts1.getTime() + 250);
        ts2.setNanos(ts2.getNanos() + extraNanos);
        try {
            stmt = conn.prepareStatement("select t from UpsertTimestamp where t = ? - 250.0/(1*24*60*60*1000) LIMIT 1");
            stmt.setTimestamp(1, ts2);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts1, rs.getTimestamp(1));
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testUpsertIntoFloat() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table UpsertFloat (k varchar primary key, v float)");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertFloat values ('a', 0.0)");
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertFloat");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(Float.valueOf(0.0f).equals(rs.getFloat(2)));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(stmt, conn);
        }
    }
        
    
    @Test
    public void testBatchedUpsert() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement pstmt = null;
        String tableName = BaseTest.generateUniqueName();
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table " + tableName + " (k varchar primary key, v integer)");
        } finally {
            closeStmtAndConn(pstmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            pstmt = conn.prepareStatement("upsert into " + tableName + " values (?, ?)");
            pstmt.setString(1, "a");
            pstmt.setInt(2, 1);
            pstmt.addBatch();
            pstmt.setString(1, "b");
            pstmt.setInt(2, 2);
            pstmt.addBatch();
            pstmt.executeBatch();
            conn.commit();
        } finally {
             closeStmtAndConn(pstmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            pstmt = conn.prepareStatement("select * from " + tableName);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(pstmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 6));
        conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        try {
            stmt.addBatch("upsert into " + tableName + " values ('c', 3)");
            stmt.addBatch("select count(*) from " + tableName);
            stmt.addBatch("upsert into " + tableName + " values ('a', 4)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + tableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            int[] result = stmt.executeBatch();
            assertEquals(3,result.length);
            assertEquals(result[0], 1);
            assertEquals(result[1], -2);
            assertEquals(result[2], 1);
            conn.commit();
        } finally {
             closeStmtAndConn(pstmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 8));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            pstmt = conn.prepareStatement("select * from " + tableName);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(pstmt, conn);
        }
    }
    
    private static Date toDate(String dateString) {
        return DateUtil.parseDate(dateString);
    }
    
    @Test
    public void testUpsertDateIntoDescUnsignedDate() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table UpsertTimestamp (k varchar, v unsigned_date not null, constraint pk primary key (k,v desc))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        String dateStr = "2013-01-01 04:00:00";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertTimestamp(k,v) values ('a', to_date(?))");
            stmt.setString(1, dateStr);
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        Date date = toDate(dateStr);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertTimestamp");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(date, rs.getDate(2));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testUpsertDateString() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table UpsertDateVal (k varchar, v date not null, t timestamp" +
                    ", tt time constraint pk primary key (k,v desc))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        String dateStr = "2013-01-01";
        String timeStampStr = "2013-01-01 04:00:00.123456";
        String timeStr = "2013-01-01 04:00:00";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertDateVal(k,v,t,tt) values ('a', ?, ?, ?)");
            stmt.setString(1, dateStr);
            stmt.setString(2, timeStampStr);
            stmt.setString(3, timeStr);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        Date date = toDate(dateStr);
        Timestamp timeStamp = new Timestamp(toDate(timeStampStr).getTime());
        timeStamp.setNanos(Timestamp.valueOf(timeStampStr).getNanos());
        Time time = new Time(toDate(timeStr).getTime());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertDateVal");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(date, rs.getDate(2));
            assertEquals(timeStamp, rs.getTimestamp(3));
            assertEquals(time, rs.getTime(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testWithUpsertingRowTimestampColSpecified_desc() throws Exception {
        testWithUpsertingRowTimestampColSpecified(true);
    }
    
    @Test
    public void testWithUpsertingRowTimestampColSpecified_asc() throws Exception {
        testWithUpsertingRowTimestampColSpecified(false);
    }
    
    private void testWithUpsertingRowTimestampColSpecified(boolean desc) throws Exception {
        String tableName = "testUpsertingRowTimestampCol".toUpperCase();
        String indexName = "testUpsertingRowTimestampCol_idx".toUpperCase();
        long ts = nextTimestamp();
        String sortOrder = desc ? "DESC" : "";
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + tableName + " (PK1 VARCHAR NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 "+ sortOrder + " ROW_TIMESTAMP " + ")) ");
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON  " + tableName + "  (PK2, KV1) INCLUDE (KV2)");
        }
        ts = nextTimestamp();
        long rowTimestamp = ts + 10000;
        Date rowTimestampDate = new Date(rowTimestamp);
        try (Connection conn = getConnection(ts)) {
            // Upsert data with scn set on the connection. However, the timestamp of the put will be the value of the row_timestamp column.
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + "  (PK1, PK2, KV1, KV2) VALUES (?, ?, ?, ?)");
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            stmt.setString(3, "KV1");
            stmt.setString(4, "KV2");
            stmt.executeUpdate();
            conn.commit();
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            // Verify that the connection with the next time stamp isn't able to see the data inserted above. This 
            // is because the timestamp of the put was rowTimestamp and not connection scn.
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM  " + tableName + "  WHERE PK1 = ? AND PK2 = ?");
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            ResultSet rs = stmt.executeQuery();
            assertFalse(rs.next());
            
            // Same holds when querying the index table too
            stmt = conn.prepareStatement("SELECT KV1 FROM  " + tableName + "  WHERE PK2 = ?");
            stmt.setDate(1, rowTimestampDate);
            rs = stmt.executeQuery();
            assertFalse(rs.next());
        }

        // Verify now that if the connection is at an SCN beyond the rowtimestamp then we can indeed see the
        // data that we upserted above.
        try (Connection conn = getConnection(rowTimestamp + 5)) {
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM  " + tableName + "  WHERE PK1 = ? AND PK2 = ?");
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("PK1", rs.getString("PK1"));
            assertEquals(rowTimestampDate, rs.getDate("PK2"));
            assertEquals("KV1", rs.getString("KV1"));
            
            // Data visible when querying the index table too.
            stmt = conn.prepareStatement("SELECT KV2 FROM  " + tableName + "  WHERE PK2 = ? AND KV1 = ?");
            stmt.setDate(1, rowTimestampDate);
            stmt.setString(2, "KV1");
            rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(indexName));
            assertTrue(rs.next());
            assertEquals("KV2", rs.getString("KV2"));
        }
    }
    
    @Test
    public void testAutomaticallySettingRowtimestamp_desc() throws Exception {
        testAutomaticallySettingRowtimestamp("DESC");
    }
    
    @Test
    public void testAutomaticallySettingRowtimestamp_asc() throws Exception {
        testAutomaticallySettingRowtimestamp("ASC");
    }
    
    private void testAutomaticallySettingRowtimestamp(String sortOrder) throws Exception {
        String tableName = "testAutomaticallySettingRowtimestamp".toUpperCase();
        String indexName = "testAutomaticallySettingRowtimestamp_index".toUpperCase();
        long ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + tableName + " (PK1 VARCHAR NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 "+ sortOrder + " ROW_TIMESTAMP " + ")) ");
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON  " + tableName + "  (PK2, KV1) INCLUDE (KV2)");
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            // Upsert values where row_timestamp column PK2 is not set and the column names are specified
            // This should upsert data with the value for PK2 as new Date(ts);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (PK1, KV1, KV2) VALUES (?, ?, ?)");
            stmt.setString(1, "PK1");
            stmt.setString(2, "KV1");
            stmt.setString(3, "KV2");
            stmt.executeUpdate();
            conn.commit();
        }
        Date upsertedDate = new Date(ts);
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            // Now query for data that was upserted above. If the row key was generated correctly then we should be able to see
            // the data in this query.
            PreparedStatement stmt = conn.prepareStatement("SELECT KV1, KV2 FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?");
            stmt.setString(1, "PK1");
            stmt.setDate(2, upsertedDate);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertEquals("KV2", rs.getString(2));
            assertFalse(rs.next());
            
            // Verify now that the data was correctly added to the mutable index too.
            stmt = conn.prepareStatement("SELECT KV2 FROM " + tableName + " WHERE PK2 = ? AND KV1 = ?");
            stmt.setDate(1, upsertedDate);
            stmt.setString(2, "KV1");
            rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(indexName));
            assertTrue(rs.next());
            assertEquals("KV2", rs.getString(1));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testAutomaticallySettingRowTimestampForImmutableTableAndIndexes_desc() throws Exception {
        testAutomaticallySettingRowTimestampForImmutableTableAndIndexes("DESC");
    }
    
    @Test
    public void testAutomaticallySettingRowTimestampForImmutableTableAndIndexes_asc() throws Exception {
        testAutomaticallySettingRowTimestampForImmutableTableAndIndexes("ASC");
    }
    
    private void testAutomaticallySettingRowTimestampForImmutableTableAndIndexes(String sortOrder) throws Exception {
        String tableName = "testSettingRowTimestampForImmutableTableAndIndexes".toUpperCase();
        String indexName = "testSettingRowTimestampForImmutableTableAndIndexes_index".toUpperCase();
        long ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + tableName + " (PK1 VARCHAR NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 "+ sortOrder + " ROW_TIMESTAMP)) " + "  IMMUTABLE_ROWS=true");
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON  " + tableName + "  (PK2, KV1) INCLUDE (KV2)");
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            // Upsert values where row_timestamp column PK2 is not set and the column names are specified
            // This should upsert data with the value for PK2 as new Date(ts);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (PK1, KV1, KV2) VALUES (?, ?, ?)");
            stmt.setString(1, "PK1");
            stmt.setString(2, "KV1");
            stmt.setString(3, "KV2");
            stmt.executeUpdate();
            conn.commit();
        }
        Date upsertedDate = new Date(ts);
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            // Now query for data that was upserted above. If the row key was generated correctly then we should be able to see
            // the data in this query.
            PreparedStatement stmt = conn.prepareStatement("SELECT KV1, KV2 FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?");
            stmt.setString(1, "PK1");
            stmt.setDate(2, upsertedDate);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertEquals("KV2", rs.getString(2));
            assertFalse(rs.next());
            
            // Verify now that the data was correctly added to the immutable index too.
            stmt = conn.prepareStatement("SELECT KV2 FROM " + tableName + " WHERE PK2 = ? AND KV1 = ?");
            stmt.setDate(1, upsertedDate);
            stmt.setString(2, "KV1");
            rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(indexName));
            assertTrue(rs.next());
            assertEquals("KV2", rs.getString(1));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testComparisonOperatorsOnAscRowTimestampCol() throws Exception {
        testComparisonOperatorsOnRowTimestampCol("ASC");
    }
    
    @Test
    public void testComparisonOperatorsOnDescRowTimestampCol() throws Exception {
        testComparisonOperatorsOnRowTimestampCol("DESC");
    }
    
    private void testComparisonOperatorsOnRowTimestampCol(String sortOrder) throws Exception {
        String tableName = ("testComparisonOperatorsOnRowTimestampCol_" + sortOrder).toUpperCase();
        long ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + tableName + " (PK1 VARCHAR NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 "+ sortOrder + " ROW_TIMESTAMP)) " + "  IMMUTABLE_ROWS=true");
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "a");
            stmt.setDate(2, new Date(10));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            stmt.setString(1, "b");
            stmt.setDate(2, new Date(20));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            stmt.setString(1, "c");
            stmt.setDate(2, new Date(30));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            stmt.setString(1, "d");
            stmt.setDate(2, new Date(40));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            conn.commit();
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            assertNumRecords(3, "SELECT count(*) from " + tableName + " WHERE PK2 > ?", conn, new Date(10));
            assertNumRecords(1, "SELECT count(*) from " + tableName + " WHERE PK2 < ? AND PK2 > ?", conn, new Date(30), new Date(10));
            assertNumRecords(3, "SELECT count(*) from " + tableName + " WHERE PK2 <= ? AND PK2 >= ?", conn, new Date(30), new Date(10));
            assertNumRecords(2, "SELECT count(*) from " + tableName + " WHERE PK2 <= ? AND PK2 > ?", conn,  new Date(30), new Date(10));
            assertNumRecords(2, "SELECT count(*) from " + tableName + " WHERE PK2 < ? AND PK2 >= ?", conn, new Date(30), new Date(10));
            assertNumRecords(0, "SELECT count(*) from " + tableName + " WHERE PK2 < ?", conn, new Date(10));
            assertNumRecords(4, "SELECT count(*) from " + tableName, conn);
        }
    }
    
    private void assertNumRecords(int count, String sql, Connection conn, Date ... params) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(sql);
        int counter = 1;
        for (Date param : params) {
            stmt.setDate(counter++, param);
        }
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
    }
    
    @Test
    public void testDisallowNegativeValuesForRowTsColumn() throws Exception {
        String tableName = "testDisallowNegativeValuesForRowTsColumn".toUpperCase();
        long ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE TABLE " + tableName + " (PK1 DATE NOT NULL PRIMARY KEY ROW_TIMESTAMP, KV1 VARCHAR)");
        }
        ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            Date d = new Date(-100);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName +  " VALUES (?, ?)");
            stmt.setDate(1, d);
            stmt.setString(2, "KV1");
            stmt.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testAutoCastLongToBigDecimal() throws Exception {
        long ts = nextTimestamp();
        try (Connection conn = getConnection(ts)) {
            conn.createStatement().execute("CREATE TABLE LONG_BUG (NAME VARCHAR PRIMARY KEY, AMOUNT DECIMAL)");
        }
        try (Connection conn = getConnection(ts + 10)) {
            conn.createStatement().execute("UPSERT INTO LONG_BUG (NAME, AMOUNT) VALUES('HELLO1', -50000)");
            conn.commit();
        }
        try (Connection conn = getConnection(ts + 20)) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, AMOUNT FROM LONG_BUG");
            assertTrue(rs.next());
            assertEquals("HELLO1", rs.getString(1));
            assertTrue(new BigDecimal(-50000).compareTo(rs.getBigDecimal(2)) == 0);
            assertFalse(rs.next());
        }
    }
    
    public void testColumnQualifierForUpsertedValues() throws Exception {
        String schemaName = "A";
        String tableName = "TEST";
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String ddl = "create table " + fullTableName 
                + " (" 
                + " K varchar primary key,"
                + " CF1.V1 varchar, CF2.V2 VARCHAR, CF2.V3 VARCHAR)";
        try (Connection conn = getConnection(nextTimestamp())) {
            conn.createStatement().execute(ddl);
        }
        String dml = "UPSERT INTO " + fullTableName + " VALUES (?, ?, ?, ?)";
        try (Connection conn = getConnection(nextTimestamp())) {
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "KEY1");
            stmt.setString(2, "VALUE1");
            stmt.setString(3, "VALUE2");
            stmt.setString(4, "VALUE3");
            stmt.executeUpdate();
            conn.commit();
        }
        // Issue a raw hbase scan and assert that key values have the expected column qualifiers.
        try (Connection conn = getConnection(nextTimestamp())) {
            HTableInterface table = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName));
            ResultScanner scanner = table.getScanner(new Scan());
            Result next = scanner.next();
            assertTrue(next.containsColumn(Bytes.toBytes("CF1"), PInteger.INSTANCE.toBytes(1)));
            assertTrue(next.containsColumn(Bytes.toBytes("CF2"), PInteger.INSTANCE.toBytes(2)));
            assertTrue(next.containsColumn(Bytes.toBytes("CF2"), PInteger.INSTANCE.toBytes(3)));
        }
    }
    
    private static Connection getConnection(long ts) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        return DriverManager.getConnection(getUrl(), props);
    }
    
}
