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

import static org.apache.phoenix.util.PhoenixRuntime.REQUEST_METRIC_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.closeStatement;
import static org.apache.phoenix.util.TestUtil.closeStmtAndConn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.function.ThrowingRunnable;


@Category(ParallelStatsDisabledTest.class)
public class UpsertValuesIT extends ParallelStatsDisabledIT {
    @Test
    public void testGroupByWithLimitOverRowKey() throws Exception {
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, TestUtil.PTSDB_NAME, null, null, null);
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (inst,host,\"DATE\") VALUES(?,'b',CURRENT_DATE())");
        stmt.setString(1, "a");
        stmt.execute();
        stmt.execute();
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.execute();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select count(1) from " + tableName + " group by inst limit 1");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("select inst from " + tableName + " where inst > 'a' group by inst limit 1");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testUpsertDateValues() throws Exception {
        String tableName = generateUniqueName();
        Date now = new Date(EnvironmentEdgeManager.currentTimeMillis());
        ensureTableCreated(getUrl(), tableName, TestUtil.PTSDB_NAME, null, null, null);
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String dateString = "1999-01-01 02:00:00";
        PreparedStatement upsertStmt = conn.prepareStatement("upsert into " + tableName + "(inst,host,\"DATE\") values('aaa','bbb',to_date('" + dateString + "'))");
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsertStmt = conn.prepareStatement("upsert into " + tableName + "(inst,host,\"DATE\") values('ccc','ddd',current_date())");
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT \"DATE\",current_date() FROM " + tableName;
        ResultSet rs = conn.createStatement().executeQuery(select);
        Date then = new Date(EnvironmentEdgeManager.currentTimeMillis());
        assertTrue(rs.next());
        Date date = DateUtil.parseDate(dateString);
        assertEquals(date,rs.getDate(1));
        assertTrue(rs.next());
        assertTrue(rs.getDate(1).after(now) && rs.getDate(1).before(then));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithExpression() throws Exception {
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, "IntKeyTest", null, null, null);
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO " + tableName + " VALUES(-1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsert = "UPSERT INTO " + tableName + " VALUES(1+2)";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT i FROM " + tableName;
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(-1,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithDate() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("create table " + tableName + " (k VARCHAR not null primary key,\"DATE\" DATE)");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into " + tableName + " values ('a',to_date('2013-06-08 00:00:00'))");
        conn.commit();
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k,to_char(\"DATE\") from " + tableName );
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("2013-06-08 00:00:00.000", rs.getString(2));
    }
    
    @Test
    public void testUpsertValuesWithDescDecimal() throws Exception {
        Properties props = new Properties();
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table " + tableName + " (k DECIMAL(12,3) NOT NULL PRIMARY KEY DESC)");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into " + tableName + " values (0.0)");
        conn.commit();
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k from " + tableName );
        assertTrue(rs.next());
        assertEquals(0.0, rs.getDouble(1), 0.001);
    }

    @Test
    public void testUpsertRandomValues() throws Exception {
        Properties props = new Properties();
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table " + tableName + " (k UNSIGNED_DOUBLE not null primary key, v1 UNSIGNED_DOUBLE, v2 UNSIGNED_DOUBLE, v3 UNSIGNED_DOUBLE, v4 UNSIGNED_DOUBLE)");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into " + tableName + " values (RAND(), RAND(), RAND(1), RAND(2), RAND(1))");
        conn.createStatement().execute("upsert into " + tableName + " values (RAND(), RAND(), RAND(1), RAND(2), RAND(1))");
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select k,v1,v2,v3,v4 from " + tableName);
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
        Properties props = new Properties();
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table " + tableName + " (mac_md5 VARCHAR not null primary key,raw_mac VARCHAR)");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into " + tableName + " values ('00000000591','a')");
        conn.createStatement().execute("upsert into " + tableName + " values ('000000005919','b')");
        conn.commit();
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select max(mac_md5) from " + tableName );
        assertTrue(rs.next());
        assertEquals("000000005919", rs.getString(1));
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into " + tableName + " values ('000000005919adfasfasfsafdasdfasfdasdfdasfdsafaxxf1','b')");
        conn.commit();
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("select max(mac_md5) from " + tableName);
        assertTrue(rs.next());
        assertEquals("000000005919adfasfasfsafdasdfasfdasdfdasfdsafaxxf1", rs.getString(1));
        conn.close();
    }
    
    @Test
    public void testUpsertValuesWithDescExpression() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table " + tableName + " (k VARCHAR not null primary key desc)");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into " + tableName + " values (to_char(100))");
        conn.commit();
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select to_number(k) from " + tableName);
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testUpsertValuesWithMoreValuesThanNumColsInTable() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.createStatement();
            stmt.execute("create table " + tableName + " (k VARCHAR not null primary key desc)");
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.createStatement();
            stmt.execute("upsert into " + tableName + " values (to_char(100), to_char(100), to_char(100))");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH.getErrorCode(),e.getErrorCode());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
    
    @Test
    public void testTimestampSerializedAndDeserializedCorrectly() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table " + tableName + " (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        Timestamp ts1 = new Timestamp(120055);
        ts1.setNanos(ts1.getNanos() + 60);
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into " + tableName + " values (1, ?)");
            stmt.setTimestamp(1, ts1);
            stmt.executeUpdate();
            conn.commit();
         } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select t from " + tableName + " where t = ?");
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
        String tableName = generateUniqueName();
        Properties props = new Properties();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table " + tableName + " (a integer NOT NULL, t timestamp NOT NULL CONSTRAINT pk PRIMARY KEY (a, t))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        Timestamp ts1 = new Timestamp(120550);
        int extraNanos = 60;
        ts1.setNanos(ts1.getNanos() + extraNanos);
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into " + tableName + " values (1, ?)");
            stmt.setTimestamp(1, ts1);
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select t from " + tableName + " LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts1, rs.getTimestamp(1));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(stmt, conn);
        }

        BigDecimal msInDay = BigDecimal.valueOf(1*24*60*60*1000);
        BigDecimal nanosInDay = BigDecimal.valueOf(1*24*60*60*1000).multiply(BigDecimal.valueOf(1000000));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select 500.0/(1*24*60*60*1000) c1, 10.0/(1*24*60*60*1000*1000000) c2  from " + tableName + " LIMIT 1");
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
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select (t + (500.0/(1*24*60*60*1000) + 10.0/(1*24*60*60*1000*1000000)))  from " + tableName + " LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts2, rs.getTimestamp(1));
        } finally {
            closeStatement(stmt);
        }
        
        ts2 = new Timestamp(ts1.getTime() - 250);
        ts2.setNanos(ts2.getNanos() + extraNanos - 30); //setting the extra nanos as well as what spilled over from timestamp millis.
        try {
            stmt = conn.prepareStatement("select (t - (250.0/(1*24*60*60*1000) + 30.0/(1*24*60*60*1000*1000000)))  from " + tableName + " LIMIT 1");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(ts2, rs.getTimestamp(1));
        } finally {
            closeStatement(stmt);
        }
        
        ts2 = new Timestamp(ts1.getTime() + 250);
        ts2.setNanos(ts2.getNanos() + extraNanos);
        try {
            stmt = conn.prepareStatement("select t from " + tableName + " where t = ? - 250.0/(1*24*60*60*1000) LIMIT 1");
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
        String tableName = generateUniqueName();
        Properties props = new Properties();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table " + tableName + " (k varchar primary key, v float)");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into " + tableName + " values ('a', 0.0)");
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from " + tableName);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(Float.valueOf(0.0f).equals(rs.getFloat(2)));
            assertFalse(rs.next());
        } finally {
             closeStmtAndConn(stmt, conn);
        }
    }

    private void testBatchedUpsert(boolean autocommit) throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(REQUEST_METRIC_ATTRIB, "true");
        props.setProperty(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
        Connection conn = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table " + tableName + " (k varchar primary key, v integer)");
        } finally {
            closeStmtAndConn(pstmt, conn);
        }

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(autocommit);
            pstmt = conn.prepareStatement("upsert into " + tableName + " values (?, ?)");
            pstmt.setString(1, "a");
            pstmt.setInt(2, 1);
            pstmt.addBatch();
            pstmt.setString(1, "b");
            pstmt.setInt(2, 2);
            pstmt.addBatch();
            pstmt.setString(1, "c");
            pstmt.setInt(2, 3);
            pstmt.addBatch();
            pstmt.executeBatch();
            if (!autocommit) {
                conn.commit();
            }
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            Map<String, Map<MetricType, Long>> mutationMetrics = pConn.getMutationMetrics();
            Assert.assertEquals(3, (long) mutationMetrics.get(tableName).get(MetricType.MUTATION_BATCH_SIZE));
            Assert.assertEquals(3, (long) mutationMetrics.get(tableName).get(MetricType.UPSERT_MUTATION_SQL_COUNTER));
            Assert.assertEquals(autocommit, conn.getAutoCommit());
        } finally {
            closeStmtAndConn(pstmt, conn);
        }

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
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(pstmt, conn);
        }

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(autocommit);
            stmt = conn.createStatement();
            stmt.addBatch("upsert into " + tableName + " values ('d', 4)");
            stmt.addBatch("upsert into " + tableName + " values ('a', 5)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + tableName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            int[] result = stmt.executeBatch();
            assertEquals(2, result.length);
            assertEquals(result[0], 1);
            assertEquals(result[1], 1);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(5, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(autocommit);
            stmt = conn.createStatement();
            stmt.addBatch("delete from " + tableName + " where v <= 4");
            stmt.addBatch("delete from " + tableName + " where v = 5");
            int[] result = stmt.executeBatch();
            assertEquals(2, result.length);
            assertEquals(result[0], 3);
            assertEquals(result[1], 1);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            pstmt = conn.prepareStatement("select count(*) from " + tableName);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        } finally {
            closeStmtAndConn(stmt, conn);
        }

    }

    @Test
    public void testBatchedUpsert() throws Exception {
        testBatchedUpsert(false);
    }

    @Test
    public void testBatchedUpsertAutoCommit() throws Exception {
        testBatchedUpsert(true);
    }

    @Test
    public void testBatchedUpsertMultipleBatches() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("create table " + tableName + " (k varchar primary key, v integer)");
            PreparedStatement pstmt = conn.prepareStatement("upsert into " + tableName + " values (?, ?)");
            pstmt.setString(1, "a");
            pstmt.setInt(2, 1);
            pstmt.addBatch();
            pstmt.executeBatch();
            pstmt.setString(1, "b");
            pstmt.setInt(2, 2);
            pstmt.addBatch();
            pstmt.executeBatch();
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + tableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    private void testBatchRollback(boolean autocommit) throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("create table " + tableName + " (k varchar primary key, v integer)");
            conn.setAutoCommit(autocommit);
            PreparedStatement pstmt = conn.prepareStatement("upsert into " + tableName + " values (?, ?)");
            pstmt.setString(1, "a");
            pstmt.setInt(2, 1);
            pstmt.addBatch();
            pstmt.executeBatch();
            conn.rollback();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + tableName);
            assertTrue(rs.next());
            assertEquals(autocommit ? 1 : 0, rs.getInt(1));
        }
    }

    @Test
    public void testBatchRollback() throws Exception {
        testBatchRollback(false);
    }

    @Test
    public void testBatchNoRollbackWithAutoCommit() throws Exception {
        testBatchRollback(true);
    }

    @Test
    public void testDQLFailsInBatch() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("create table " + tableName + " (k varchar primary key, v integer)");
            Statement stmt = conn.createStatement();
            stmt.addBatch("select * from " + tableName);
            BatchUpdateException ex = assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
            assertEquals("java.sql.BatchUpdateException: ERROR 1151 (XCL51): A batch operation can't include a statement that produces result sets.",
                    ex.getMessage());
        }
    }

    private static Date toDate(String dateString) {
        return DateUtil.parseDate(dateString);
    }
    
    @Test
    public void testUpsertDateIntoDescUnsignedDate() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table " + tableName + " (k varchar, v unsigned_date not null, constraint pk primary key (k,v desc))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        
        String dateStr = "2013-01-01 04:00:00";
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into " + tableName + "(k,v) values ('a', to_date(?))");
            stmt.setString(1, dateStr);
            stmt.executeUpdate();
            conn.commit();
        } finally {
             closeStmtAndConn(stmt, conn);
        }
        
        Date date = toDate(dateStr);
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from " + tableName);
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
        String tableName = generateUniqueName();
        Properties props = new Properties();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("create table " + tableName + " (k varchar, v date not null, t timestamp" +
                    ", tt time constraint pk primary key (k,v desc))");
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        String dateStr = "2013-01-01";
        String timeStampStr = "2013-01-01 04:00:00.123456";
        String timeStr = "2013-01-01 04:00:00";
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into " + tableName + "(k,v,t,tt) values ('a', ?, ?, ?)");
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
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from " + tableName);
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
    public void testAutoCastLongToBigDecimal() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE LONG_BUG (NAME VARCHAR PRIMARY KEY, AMOUNT DECIMAL)");
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("UPSERT INTO LONG_BUG (NAME, AMOUNT) VALUES('HELLO1', -50000)");
            conn.commit();
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
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
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
        }
        String dml = "UPSERT INTO " + fullTableName + " VALUES (?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "KEY1");
            stmt.setString(2, "VALUE1");
            stmt.setString(3, "VALUE2");
            stmt.setString(4, "VALUE3");
            stmt.executeUpdate();
            conn.commit();
        }
        // Issue a raw hbase scan and assert that key values have the expected column qualifiers.
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Table table = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName));
            ResultScanner scanner = table.getScanner(new Scan());
            Result next = scanner.next();
            assertTrue(next.containsColumn(Bytes.toBytes("CF1"), PInteger.INSTANCE.toBytes(1)));
            assertTrue(next.containsColumn(Bytes.toBytes("CF2"), PInteger.INSTANCE.toBytes(2)));
            assertTrue(next.containsColumn(Bytes.toBytes("CF2"), PInteger.INSTANCE.toBytes(3)));
        }
    }

    @Test
    public void testUpsertValueWithDiffSequenceAndConnections() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            PreparedStatement createTableStatement = conn.prepareStatement(String.format("CREATE TABLE IF NOT EXISTS " +
                    "%s (SERVICE VARCHAR NOT NULL, SEQUENCE_NUMBER BIGINT NOT NULL , " +
                    "CONSTRAINT PK PRIMARY KEY (SERVICE, SEQUENCE_NUMBER)) MULTI_TENANT = TRUE", tableName));
            createTableStatement.execute();
        }

        testGlobalSequenceUpsertWithTenantConnection(tableName);
        testGlobalSequenceUpsertWithGlobalConnection(tableName);
        testTenantSequenceUpsertWithSameTenantConnection(tableName);
        testTenantSequenceUpsertWithDifferentTenantConnection(tableName);
        testTenantSequenceUpsertWithGlobalConnection(tableName);

    }

    private void testTenantSequenceUpsertWithGlobalConnection(String tableName) throws Exception {
        String sequenceName = generateUniqueSequenceName();

        try (Connection conn = getTenantConnection("PHOENIX")) {
            conn.setAutoCommit(true);
            PreparedStatement createSequenceStatement = conn.prepareStatement(String.format("CREATE SEQUENCE " +
                    "IF NOT EXISTS %s", sequenceName));
            createSequenceStatement.execute();
        }

        try (Connection tenantConn = DriverManager.getConnection(getUrl())) {
            tenantConn.setAutoCommit(true);
            Statement executeUpdateStatement = tenantConn.createStatement();
            try {
                executeUpdateStatement.execute(String.format("UPSERT INTO %s ( SERVICE, SEQUENCE_NUMBER) VALUES " +
                        "( 'PHOENIX', NEXT VALUE FOR %s)", tableName, sequenceName));
                Assert.fail();
            } catch (SequenceNotFoundException e) {
                assertTrue(true);
            } catch (Exception e) {
                Assert.fail();
            }
        }
    }

    private void testTenantSequenceUpsertWithDifferentTenantConnection(String tableName) throws Exception {
        String sequenceName = generateUniqueSequenceName();

        try (Connection conn = getTenantConnection("PHOENIX")) {
            conn.setAutoCommit(true);
            PreparedStatement createSequenceStatement = conn.prepareStatement(String.format("CREATE SEQUENCE " +
                    "IF NOT EXISTS %s", sequenceName));
            createSequenceStatement.execute();
        }

        try (Connection tenantConn = getTenantConnection("HBASE")) {
            tenantConn.setAutoCommit(true);

            Statement executeUpdateStatement = tenantConn.createStatement();
            try {
                executeUpdateStatement.execute(String.format("UPSERT INTO %s ( SEQUENCE_NUMBER) VALUES " +
                        "( NEXT VALUE FOR %s)", tableName, sequenceName));
                Assert.fail();
            } catch (SequenceNotFoundException e) {
                assertTrue(true);
            } catch (Exception e) {
                Assert.fail();
            }
        }
    }

    private void testTenantSequenceUpsertWithSameTenantConnection(String tableName) throws Exception {
        String sequenceName = generateUniqueSequenceName();

        try (Connection conn = getTenantConnection("ZOOKEEPER")) {
            conn.setAutoCommit(true);
            PreparedStatement createSequenceStatement = conn.prepareStatement(String.format("CREATE SEQUENCE " +
                    "IF NOT EXISTS %s", sequenceName));
            createSequenceStatement.execute();
            Statement executeUpdateStatement = conn.createStatement();
            executeUpdateStatement.execute(String.format("UPSERT INTO %s ( SEQUENCE_NUMBER) VALUES " +
                    "( NEXT VALUE FOR %s)", tableName, sequenceName));
            ResultSet rs = executeUpdateStatement.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertFalse(rs.next());
        }

    }

    private void testGlobalSequenceUpsertWithGlobalConnection(String tableName) throws Exception {
        String sequenceName = generateUniqueSequenceName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            PreparedStatement createSequenceStatement = conn.prepareStatement(String.format("CREATE SEQUENCE " +
                    "IF NOT EXISTS %s", sequenceName));
            createSequenceStatement.execute();
            Statement executeUpdateStatement = conn.createStatement();
            executeUpdateStatement.execute(String.format("UPSERT INTO %s ( SERVICE, SEQUENCE_NUMBER) VALUES " +
                    "( 'PHOENIX', NEXT VALUE FOR %s)", tableName, sequenceName));
            ResultSet rs = executeUpdateStatement.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals("HBASE", rs.getString(1));
            assertEquals("1", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("PHOENIX", rs.getString(1));
            assertEquals("1", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    private void testGlobalSequenceUpsertWithTenantConnection(String tableName) throws Exception {
        String sequenceName = generateUniqueSequenceName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            PreparedStatement createSequenceStatement = conn.prepareStatement(String.format("CREATE SEQUENCE " +
                    "IF NOT EXISTS %s", sequenceName));
            createSequenceStatement.execute();
        }

        try (Connection tenantConn = getTenantConnection("HBASE")) {
            tenantConn.setAutoCommit(true);

            Statement executeUpdateStatement = tenantConn.createStatement();
            executeUpdateStatement.execute(String.format("UPSERT INTO %s ( SEQUENCE_NUMBER) VALUES " +
                    "( NEXT VALUE FOR %s)", tableName, sequenceName));

            ResultSet rs = executeUpdateStatement.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertFalse(rs.next());

        }
    }

    private static Connection getTenantConnection(String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }
}
