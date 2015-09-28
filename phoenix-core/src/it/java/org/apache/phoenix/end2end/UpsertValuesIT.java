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
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class UpsertValuesIT extends BaseClientManagedTimeIT {
    @Test
    public void testGroupByWithLimitOverRowKey() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),TestUtil.PTSDB_NAME,null, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.PTSDB_NAME + " (inst,host,date) VALUES(?,'b',CURRENT_DATE())");
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
        ensureTableCreated(getUrl(),TestUtil.PTSDB_NAME,null, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String dateString = "1999-01-01 02:00:00";
        PreparedStatement upsertStmt = conn.prepareStatement("upsert into ptsdb(inst,host,date) values('aaa','bbb',to_date('" + dateString + "'))");
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsertStmt = conn.prepareStatement("upsert into ptsdb(inst,host,date) values('ccc','ddd',current_date())");
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 1
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT date,current_date() FROM ptsdb";
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
        ensureTableCreated(getUrl(),"IntKeyTest",null, ts-2);
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
        conn.createStatement().execute("create table UpsertDateTest (k VARCHAR not null primary key,date DATE)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into UpsertDateTest values ('a',to_date('2013-06-08 00:00:00'))");
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k,to_char(date) from UpsertDateTest");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("2013-06-08 00:00:00.000", rs.getString(2));
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
            stmt = conn.prepareStatement("create table UpsertTimestamp (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
            stmt.execute();
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
            stmt = conn.prepareStatement("create table UpsertTimestamp (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
            stmt.execute();
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
            stmt = conn.prepareStatement("create table UpsertFloat (k varchar primary key, v float)");
            stmt.execute();
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
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            pstmt = conn.prepareStatement("create table t (k varchar primary key, v integer)");
            pstmt.execute();
        } finally {
            closeStmtAndConn(pstmt, conn);
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            pstmt = conn.prepareStatement("upsert into t values (?, ?)");
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
            pstmt = conn.prepareStatement("select * from t");
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
            stmt.addBatch("upsert into t values ('c', 3)");
            stmt.addBatch("select count(*) from t");
            stmt.addBatch("upsert into t values ('a', 4)");
            ResultSet rs = stmt.executeQuery("select count(*) from t");
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
            pstmt = conn.prepareStatement("select * from t");
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
            stmt = conn.prepareStatement("create table UpsertTimestamp (k varchar, v unsigned_date not null, constraint pk primary key (k,v desc))");
            stmt.execute();
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
            stmt = conn.prepareStatement("create table UpsertDateVal (k varchar, v date not null, t timestamp" +
                    ", tt time constraint pk primary key (k,v desc))");
            stmt.execute();
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
    
}
