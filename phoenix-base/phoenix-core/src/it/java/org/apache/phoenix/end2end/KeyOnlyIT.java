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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;


public class KeyOnlyIT extends ParallelStatsEnabledIT {
    private String tableName;
    
    @Before
    public void createTable() throws SQLException {
        tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("create table " + tableName +
                "   (i1 integer not null, i2 integer not null\n" +
                "    CONSTRAINT pk PRIMARY KEY (i1,i2))");
        }
    }
    
    @Test
    public void testKeyOnly() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        initTableValues(conn);
        analyzeTable(conn, tableName);
        
        String query = "SELECT i1, i2 FROM " + tableName;
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        List<KeyRange> splits = getAllSplits(conn, tableName);
        assertEquals(3, splits.size());
        
        conn.createStatement().execute("ALTER TABLE " + tableName + " ADD s1 varchar");
        
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 5);
        stmt.setInt(2, 6);
        stmt.setString(3, "foo");
        stmt.execute();
        conn.commit();
        
        analyzeTable(conn, tableName);

        query = "SELECT i1 FROM " + tableName;
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertFalse(rs.next());
        
        query = "SELECT i1,s1 FROM " + tableName;
        statement = conn.prepareStatement(query);
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
    }
    
    @Test
    public void testOr() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        initTableValues(conn);
        analyzeTable(conn, tableName);
        
        String query = "SELECT i1 FROM " + tableName + " WHERE i1 < 2 or i1 = 3";
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
    }
        
    @Test
    public void testQueryWithLimitAndStats() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);        
        initTableValues(conn, 100);
        analyzeTable(conn, tableName);
        
        String query = "SELECT i1 FROM " + tableName + " LIMIT 1";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT SERIAL 1-WAY FULL SCAN OVER " + tableName + "\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER 1 ROW LIMIT\n" + 
                "CLIENT 1 ROW LIMIT", QueryUtil.getExplainPlan(rs));
    }
    
    private void initTableValues(Connection conn) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " +
            tableName + " VALUES (?, ?)");
        stmt.setInt(1, 1);
        stmt.setInt(2, 2);
        stmt.execute();
        
        stmt.setInt(1, 3);
        stmt.setInt(2, 4);
        stmt.execute();
        
        conn.commit();
    }

    private void initTableValues(Connection conn, int nRows) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " +
             tableName + " VALUES (?, ?)");
        for (int i = 0; i < nRows; i++) {
	        stmt.setInt(1, i);
	        stmt.setInt(2, i+1);
	        stmt.execute();
        }
        
        conn.commit();
    }
}
