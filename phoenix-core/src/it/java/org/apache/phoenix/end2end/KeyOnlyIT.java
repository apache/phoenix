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

import static org.apache.phoenix.util.TestUtil.KEYONLY_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class KeyOnlyIT extends BaseOwnClusterClientManagedTimeIT {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(50));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(100));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testKeyOnly() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),KEYONLY_NAME,null, ts);
        initTableValues(ts+1);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+30));
        Connection conn3 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn3, KEYONLY_NAME);
        conn3.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+50));
        Connection conn5 = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT i1, i2 FROM KEYONLY";
        PreparedStatement statement = conn5.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        List<KeyRange> splits = getAllSplits(conn5, "KEYONLY");
        assertEquals(2, splits.size());
        conn5.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+60));
        Connection conn6 = DriverManager.getConnection(getUrl(), props);
        conn6.createStatement().execute("ALTER TABLE KEYONLY ADD s1 varchar");
        conn6.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+70));
        Connection conn7 = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn7.prepareStatement(
                "upsert into " +
                "KEYONLY VALUES (?, ?, ?)");
        stmt.setInt(1, 5);
        stmt.setInt(2, 6);
        stmt.setString(3, "foo");
        stmt.execute();
        conn7.commit();
        conn7.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+80));
        Connection conn8 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn8, KEYONLY_NAME);
        conn8.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+90));
        Connection conn9 = DriverManager.getConnection(getUrl(), props);
        query = "SELECT i1 FROM KEYONLY";
        statement = conn9.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertFalse(rs.next());
        
        query = "SELECT i1,s1 FROM KEYONLY";
        statement = conn9.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertEquals("foo", rs.getString(2));
        assertFalse(rs.next());

        conn9.close();
    }
    
    @Test
    public void testOr() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),KEYONLY_NAME,null, ts);
        initTableValues(ts+1);
        Properties props = new Properties();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+3));
        Connection conn3 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn3, KEYONLY_NAME);
        conn3.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        Connection conn5 = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT i1 FROM KEYONLY WHERE i1 < 2 or i1 = 3";
        PreparedStatement statement = conn5.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
        conn5.close();
    }
        
    @Test
    public void testQueryWithLimitAndStats() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),KEYONLY_NAME,null, ts);
        initTableValues(ts+1, 100);
        
        TestUtil.analyzeTable(getUrl(), ts+10, KEYONLY_NAME);
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+50));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT i1 FROM KEYONLY LIMIT 1";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT SERIAL 1-WAY FULL SCAN OVER KEYONLY\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER 1 ROW LIMIT\n" + 
                "CLIENT 1 ROW LIMIT", QueryUtil.getExplainPlan(rs));
        conn.close();
    }
    
    protected static void initTableValues(long ts) throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " +
            "KEYONLY VALUES (?, ?)");
        stmt.setInt(1, 1);
        stmt.setInt(2, 2);
        stmt.execute();
        
        stmt.setInt(1, 3);
        stmt.setInt(2, 4);
        stmt.execute();
        
        conn.commit();
        conn.close();
    }

    protected static void initTableValues(long ts, int nRows) throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " +
            "KEYONLY VALUES (?, ?)");
        for (int i = 0; i < nRows; i++) {
	        stmt.setInt(1, i);
	        stmt.setInt(2, i+1);
	        stmt.execute();
        }
        
        conn.commit();
        conn.close();
    }
}
