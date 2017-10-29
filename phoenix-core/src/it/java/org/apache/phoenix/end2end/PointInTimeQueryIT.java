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
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW4;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

public class PointInTimeQueryIT extends BaseQueryIT {

    @Parameters(name="PointInTimeQueryIT_{index},columnEncoded={1}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean columnEncoded : new boolean[]{false,true}) {
                testCases.add(new Object[] { indexDDL, columnEncoded });
            }
        }
        return testCases;
    }
    
    public PointInTimeQueryIT(String idxDdl, boolean columnEncoded)
            throws Exception {
        // These queries fail without KEEP_DELETED_CELLS=true
        super(idxDdl, columnEncoded, false);
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
        Connection conn = DriverManager.getConnection(url, props);
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
        conn.close();
        long firstDeleteTime = System.currentTimeMillis();
        long timeDelta = 100; 
        Thread.sleep(timeDelta); 
        
        // Delete row at timestamp 3. This should not be seen by the query executing
        // Remove column value at ts + 1 (i.e. equivalent to setting the value to null)
        Connection futureConn = DriverManager.getConnection(getUrl());
        stmt = futureConn.prepareStatement("delete from " + tableName + " where organization_id=? and entity_id=?");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.execute();
        futureConn.commit();
        futureConn.close();
        
        // query at a time which is beyong deleteTime1 but before the time at which above delete
        // happened
        long queryTime = firstDeleteTime + timeDelta / 2;

        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime)); // Execute at timestamp 2
        conn = DriverManager.getConnection(getUrl(), props);
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
    public void testPointInTimeScan() throws Exception {
        // Override value that was set at creation time
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " + tableName +
            " (" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_INTEGER) " +
            "VALUES (?, ?, ?)";
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 5);
        stmt.execute(); // should commit too
        upsertConn.close();
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);
        
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 9);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        long queryTime = upsert1Time + timeDelta / 2;
        String query = "SELECT organization_id, a_string AS a FROM " + tableName + " WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(tenantId, rs.getString(1));
        assertEquals(A_VALUE, rs.getString("a"));
        assertTrue(rs.next());
        assertEquals(tenantId, rs.getString(1));
        assertEquals(B_VALUE, rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPointInTimeLimitedScan() throws Exception {
        // Override value that was set at creation time
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " + tableName +
            " (" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_INTEGER) " +
            "VALUES (?, ?, ?)";
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setInt(3, 6);
        stmt.execute(); // should commit too
        upsertConn.close();
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);

        url = getUrl();
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setInt(3, 0);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        long queryTime = upsert1Time + timeDelta  / 2;
        String query = "SELECT a_integer,b_string FROM " + tableName + " WHERE organization_id=? and a_integer <= 5 limit 2";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        List<List<Object>> expectedResultsA = Lists.newArrayList(
                Arrays.<Object>asList(2, C_VALUE),
                Arrays.<Object>asList( 3, E_VALUE));
        List<List<Object>> expectedResultsB = Lists.newArrayList(
                Arrays.<Object>asList( 5, C_VALUE),
                Arrays.<Object>asList(4, B_VALUE));
        // Since we're not ordering and we may be using a descending index, we don't
        // know which rows we'll get back.
        assertOneOfValuesEqualsResultSet(rs, expectedResultsA,expectedResultsB);
       conn.close();
    }
    
}
