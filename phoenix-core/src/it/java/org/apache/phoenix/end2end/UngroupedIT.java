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

import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW3;
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
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class UngroupedIT extends BaseQueryIT {

    public UngroupedIT(String idxDdl, boolean columnEncoded)
            throws Exception {
        super(idxDdl, columnEncoded, false);
    }

    @Parameters(name="UngroupedIT_{index}") // name is used by failsafe as file name in reports
    public static Collection<Object> data() {
        return BaseQueryIT.allIndexes();
    }
    
    @Test
    public void testUngroupedAggregation() throws Exception {
        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, B_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
        conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, B_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUngroupedAggregationNoWhere() throws Exception {
        String query = "SELECT count(*) FROM " + tableName;
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(9, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPointInTimeUngroupedAggregation() throws Exception {
        // Override value that was set at creation time
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String updateStmt =
                "upsert into " + tableName + " (" + "    ORGANIZATION_ID, " + "    ENTITY_ID, "
                        + "    A_STRING) " + "VALUES (?, ?, ?)";
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
        upsertConn.close();
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);

        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, E_VALUE);
        stmt.execute();
        upsertConn.close();
        
        long queryTime = upsert1Time + timeDelta / 2;
        String query =
                "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        statement.setString(2, B_VALUE);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testPointInTimeUngroupedLimitedAggregation() throws Exception {
        String updateStmt =
                "upsert into " + tableName + " (" + "    ORGANIZATION_ID, " + "    ENTITY_ID, "
                        + "    A_STRING) " + "VALUES (?, ?, ?)";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
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
        upsertConn.close();
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);
        
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, E_VALUE);
        stmt.execute();
        upsertConn.close();

        long queryTime = upsert1Time + timeDelta / 2;
        String query =
                "SELECT count(1) FROM " + tableName
                        + " WHERE organization_id=? and a_string = ? LIMIT 3";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        statement.setString(2, B_VALUE);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getLong(1)); // LIMIT applied at end, so all rows would be counted
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testSumOverNullIntegerColumn() throws Exception {
        String query = "SELECT sum(a_integer) FROM " + tableName + " a";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (organization_id,entity_id,a_integer) VALUES('" + getOrganizationId() + "','" + ROW3 + "',NULL)");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        TestUtil.analyzeTable(conn1, tableName);
        conn1.close();
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(42, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (organization_id,entity_id,a_integer) SELECT organization_id, entity_id, CAST(null AS integer) FROM " + tableName);

        conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}
