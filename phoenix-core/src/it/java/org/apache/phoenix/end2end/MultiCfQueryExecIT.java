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
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import org.apache.phoenix.util.PhoenixRuntime;


public class MultiCfQueryExecIT extends BaseClientManagedTimeIT {
    private static final String MULTI_CF = "MULTI_CF";
    
    protected static void initTableValues(long ts) throws Exception {
        ensureTableCreated(getUrl(),MULTI_CF,null, ts-2);
        
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "MULTI_CF(" +
                "    ID, " +
                "    TRANSACTION_COUNT, " +
                "    CPU_UTILIZATION, " +
                "    DB_CPU_UTILIZATION," +
                "    UNIQUE_USER_COUNT," +
                "    F.RESPONSE_TIME," +
                "    G.RESPONSE_TIME)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "000000000000001");
        stmt.setInt(2, 100);
        stmt.setBigDecimal(3, BigDecimal.valueOf(0.5));
        stmt.setBigDecimal(4, BigDecimal.valueOf(0.2));
        stmt.setInt(5, 1000);
        stmt.setLong(6, 11111);
        stmt.setLong(7, 11112);
        stmt.execute();
        stmt.setString(1, "000000000000002");
        stmt.setInt(2, 200);
        stmt.setBigDecimal(3, BigDecimal.valueOf(2.5));
        stmt.setBigDecimal(4, BigDecimal.valueOf(2.2));
        stmt.setInt(5, 2000);
        stmt.setLong(6, 2222);
        stmt.setLong(7, 22222);
        stmt.execute();
    }
    
    @Test
    public void testConstantCount() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT count(1) from multi_cf";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguateInSelectOnly1() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where ID = '000000000000002'";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguateInSelectOnly2() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where TRANSACTION_COUNT = 200";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguate1() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where F.RESPONSE_TIME = 2222";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguate2() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where G.RESPONSE_TIME-1 = F.RESPONSE_TIME";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(11111, rs.getLong(1));
            assertEquals(11112, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDefaultCFToDisambiguate() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts);
        
        String ddl = "ALTER TABLE multi_cf ADD response_time BIGINT";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3);
        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute(ddl);
        conn.close();
       
        String dml = "upsert into " +
        "MULTI_CF(" +
        "    ID, " +
        "    RESPONSE_TIME)" +
        "VALUES ('000000000000003', 333)";
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 4); // Run query at timestamp 5
        conn = DriverManager.getConnection(url);
        conn.createStatement().execute(dml);
        conn.commit();
        conn.close();

        String query = "SELECT ID,RESPONSE_TIME from multi_cf where RESPONSE_TIME = 333";
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("000000000000003", rs.getString(1));
            assertEquals(333, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testEssentialColumnFamilyForRowKeyFilter() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where SUBSTR(ID, 15) = '2'";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}
