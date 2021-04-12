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

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;


@Category(NeedsOwnMiniClusterTest.class)
public class PointInTimeQueryIT extends BaseQueryIT {

    @Parameters(name="PointInTimeQueryIT_{index},columnEncoded={1}")
    public static synchronized Collection<Object> data() {
        return BaseQueryIT.allIndexesWithEncodedAndKeepDeleted();
    }
    
    public PointInTimeQueryIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells)
            throws Exception {
        super(indexDDL, columnEncoded, keepDeletedCells);
        // For this class we specifically want to run each test method with fresh tables
        // it is expected to be slow
        initTables(indexDDL, columnEncoded, keepDeletedCells);
    }

    @Test
    public void testPointInTimeDeleteUngroupedAggregation() throws Exception {
        String updateStmt = 
            "upsert into " + tableName +
            " (" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_STRING) " +
            "VALUES (?, ?, ?)";
        
        // Override value that was set at creation time
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        // Remove column value at ts + 1 (i.e. equivalent to setting the value to null)
        try (Connection conn = DriverManager.getConnection(url, props)) {
            PreparedStatement stmt = conn.prepareStatement(updateStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW7);
            stmt.setString(3, null);
            stmt.execute();
            
            // Delete row 
            stmt = conn.prepareStatement("delete from " + tableName + " where organization_id=? and entity_id=?");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW5);
            stmt.execute();
            conn.commit();
        }
        long firstDeleteTime = System.currentTimeMillis();
        long timeDelta = 100; 
        Thread.sleep(timeDelta); 
        
        // Delete row at timestamp 3. This should not be seen by the query executing
        // Remove column value at ts + 1 (i.e. equivalent to setting the value to null)
        try (Connection futureConn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = futureConn.prepareStatement("delete from " + tableName + " where organization_id=? and entity_id=?");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW6);
            stmt.execute();
            futureConn.commit();
        }
        
        // query at a time which is beyong deleteTime1 but before the time at which above delete
        // happened
        long queryTime = firstDeleteTime + timeDelta / 2;

        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime)); // Execute at timestamp 2
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, B_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void TestPointInTimeGroupedAggregation() throws Exception {
        String updateStmt =
                "upsert into " + tableName + " VALUES ('" + tenantId + "','" + ROW5 + "','"
                        + C_VALUE + "')";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            Statement stmt = upsertConn.createStatement();
            stmt.execute(updateStmt); // should commit too
        }
        
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);

        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            updateStmt = "upsert into " + tableName + " VALUES (?, ?, ?)";
            // Insert all rows at ts
            PreparedStatement pstmt = upsertConn.prepareStatement(updateStmt);
            pstmt.setString(1, tenantId);
            pstmt.setString(2, ROW5);
            pstmt.setString(3, E_VALUE);
            pstmt.execute(); // should commit too
        }
        
        long queryTime = upsert1Time + timeDelta / 2;
        String query =
                "SELECT a_string, count(1) FROM " + tableName + " WHERE organization_id='"
                        + tenantId + "' GROUP BY a_string";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(B_VALUE, rs.getString(1));
            assertEquals(3, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals(C_VALUE, rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void TestPointInTimeUngroupedAggregation() throws Exception {
        // Override value that was set at creation time
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String updateStmt =
                "upsert into " + tableName + " (" + "    ORGANIZATION_ID, " + "    ENTITY_ID, "
                        + "    A_STRING) " + "VALUES (?, ?, ?)";
        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            // Insert all rows at ts
            PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW5);
            stmt.setString(3, null);
            stmt.execute();
            stmt.setString(3, C_VALUE);
            stmt.execute();
            stmt.setString(2, ROW7);
            stmt.setString(3, E_VALUE);
            stmt.execute();
            upsertConn.commit();
        }
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);

        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW6);
            stmt.setString(3, E_VALUE);
            stmt.execute();
        }
        
        long queryTime = upsert1Time + timeDelta / 2;
        String query =
                "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
        try (Connection conn = DriverManager.getConnection(url, props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, B_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void TestPointInTimeUngroupedLimitedAggregation() throws Exception {
        String updateStmt =
                "upsert into " + tableName + " (" + "    ORGANIZATION_ID, " + "    ENTITY_ID, "
                        + "    A_STRING) " + "VALUES (?, ?, ?)";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW6);
            stmt.setString(3, C_VALUE);
            stmt.execute();
            stmt.setString(3, E_VALUE);
            stmt.execute();
            stmt.setString(3, B_VALUE);
            stmt.execute();
            stmt.setString(3, B_VALUE);
            stmt.execute();
        }
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);
        
        try (Connection upsertConn = DriverManager.getConnection(url, props)) {
            upsertConn.setAutoCommit(true); // Test auto commit
            PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW6);
            stmt.setString(3, E_VALUE);
            stmt.execute();
        }
        long queryTime = upsert1Time + timeDelta / 2;
        String query =
                "SELECT count(1) FROM " + tableName
                        + " WHERE organization_id=? and a_string = ? LIMIT 3";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
        try (Connection conn = DriverManager.getConnection(url, props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, B_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1)); // LIMIT applied at end, so all rows would be counted
            assertFalse(rs.next());
        }
    }
}
