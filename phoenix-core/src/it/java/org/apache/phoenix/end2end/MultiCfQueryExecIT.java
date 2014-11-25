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
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class MultiCfQueryExecIT extends BaseOwnClusterClientManagedTimeIT {
    private static final String MULTI_CF = "MULTI_CF";
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    protected static void initTableValues(long ts) throws Exception {
        ensureTableCreated(getUrl(),MULTI_CF,null, ts-2);
        
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            analyzeTable(getUrl(), ts + 3, "MULTI_CF");
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            analyzeTable(getUrl(), ts + 3, "MULTI_CF");
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            analyzeTable(getUrl(), ts + 3, "MULTI_CF");
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
    public void testGuidePostsForMultiCFs() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts);
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where F.RESPONSE_TIME = 2222";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            analyzeTable(getUrl(), ts + 3, "MULTI_CF");
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
            // Use E column family. Since the column family with the empty key value (the first one, A)
            // is always added to the scan, we never really use other guideposts (but this may change).
            List<KeyRange> splits = getAllSplits(conn, "MULTI_CF", "e.cpu_utilization IS NOT NULL");
            // Since the E column family is not populated, it won't have as many splits
            assertEquals(3, splits.size());
            // Same as above for G column family.
            splits = getAllSplits(conn, "MULTI_CF", "g.response_time IS NOT NULL");
            assertEquals(3, splits.size());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCFToDisambiguate2() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf where G.RESPONSE_TIME-1 = F.RESPONSE_TIME";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            analyzeTable(getUrl(), ts + 3, "MULTI_CF");
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
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 10);
        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute(ddl);
        conn.close();
        
        analyzeTable(getUrl(), ts + 15, "MULTI_CF");
       
        String dml = "upsert into " +
        "MULTI_CF(" +
        "    ID, " +
        "    RESPONSE_TIME)" +
        "VALUES ('000000000000003', 333)";
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 20); 
        conn = DriverManager.getConnection(url);
        conn.createStatement().execute(dml);
        conn.commit();
        conn.close();
        
        analyzeTable(getUrl(), ts + 25, "MULTI_CF");
        
        String query = "SELECT ID,RESPONSE_TIME from multi_cf where RESPONSE_TIME = 333";
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 30); // Run query at timestamp 5
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(ts);
            analyzeTable(getUrl(), ts + 3, "MULTI_CF");
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
