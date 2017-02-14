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
import static org.apache.phoenix.util.TestUtil.ROW3;
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
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MutableQueryIT extends BaseQueryIT {
    
    @Parameters(name="indexDDL={0},mutable={1},columnEncoded={2}")
    @Shadower(classBeingShadowed = BaseQueryIT.class)
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean columnEncoded : new boolean[]{false,true}) {
                testCases.add(new Object[] { indexDDL, true, columnEncoded });
            }
        }
        return testCases;
    }
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseQueryIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.DEFAULT_KEEP_DELETED_CELLS_ATTRIB, Boolean.TRUE.toString());
        BaseQueryIT.doSetup(props);
    }

    public MutableQueryIT(String indexDDL, boolean mutable, boolean columnEncoded) {
        super(indexDDL, mutable, columnEncoded);
    }
    
    @Test
    public void testSumOverNullIntegerColumn() throws Exception {
        String query = "SELECT sum(a_integer) FROM " + tableName + " a";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (organization_id,entity_id,a_integer) VALUES('" + getOrganizationId() + "','" + ROW3 + "',NULL)");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, tableName);
        conn1.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 50));
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
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 70));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (organization_id,entity_id,a_integer) SELECT organization_id, entity_id, CAST(null AS integer) FROM " + tableName);

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 90));
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
    
    private void testNoStringValue(String value) throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 10);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(
                "upsert into " + tableName + " VALUES (?, ?, ?)"); // without specifying columns
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.setString(3, value);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, tableName);
        conn1.close();
        
        String query = "SELECT a_string, b_string FROM " + tableName + " WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(null, rs.getString(1));
            assertTrue(rs.wasNull());
            assertEquals(C_VALUE, rs.getString("B_string"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNullStringValue() throws Exception {
        testNoStringValue(null);
    }
    
    @Test
    public void testEmptyStringValue() throws Exception {
        testNoStringValue("");
    }
    
    @Test
    public void testUnfoundSingleColumnCaseStatement() throws Exception {
        String query = "SELECT entity_id, b_string FROM " + tableName + " WHERE organization_id=? and CASE WHEN a_integer = 0 or a_integer != 0 THEN 1 ELSE 0 END = 0";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        // Set ROW5.A_INTEGER to null so that we have one row
        // where the else clause of the CASE statement will
        // fire.
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " + tableName +
            " (" +
            "    ENTITY_ID, " +
            "    ORGANIZATION_ID, " +
            "    A_INTEGER) " +
            "VALUES ('" + ROW5 + "','" + tenantId + "', null)";
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(ROW5, rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testGroupByCondition() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement("SELECT count(*) FROM " + tableName + " WHERE organization_id=? GROUP BY a_integer=6");
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertValueEqualsResultSet(rs, Arrays.<Object>asList(1L,8L));
        try {
            statement = conn.prepareStatement("SELECT count(*),a_integer=6 FROM " + tableName + " WHERE organization_id=? and (a_integer IN (5,6) or a_integer is null) GROUP BY a_integer=6");
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            List<List<Object>> expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(1L,false),
                    Arrays.<Object>asList(1L,true));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }

        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            statement = conn.prepareStatement("UPSERT into " + tableName + " (organization_id,entity_id,a_integer) values(?,?,null)");
            statement.setString(1, tenantId);
            statement.setString(2, ROW3);
            statement.executeUpdate();
            conn.commit();
        } finally {
            conn.close();
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 60));
        conn = DriverManager.getConnection(getUrl(), props);
        statement = conn.prepareStatement("SELECT count(*) FROM " + tableName + " WHERE organization_id=? GROUP BY a_integer=6");
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertValueEqualsResultSet(rs, Arrays.<Object>asList(1L,1L,7L));
        statement = conn.prepareStatement("SELECT a_integer, entity_id FROM " + tableName + " WHERE organization_id=? and (a_integer IN (5,6) or a_integer is null)");
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        List<List<Object>> expectedResults = Lists.newArrayList(
                Arrays.<Object>asList(null,ROW3),
                Arrays.<Object>asList(5,ROW5),
                Arrays.<Object>asList(6,ROW6));
        assertValuesEqualsResultSet(rs, expectedResults);
        try {
            statement = conn.prepareStatement("SELECT count(*),a_integer=6 FROM " + tableName + " WHERE organization_id=? and (a_integer IN (5,6) or a_integer is null) GROUP BY a_integer=6");
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(1L,null),
                    Arrays.<Object>asList(1L,false),
                    Arrays.<Object>asList(1L,true));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
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
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
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
        
        // Delete row at timestamp 3. This should not be seen by the query executing
        // Remove column value at ts + 1 (i.e. equivalent to setting the value to null)
        Connection futureConn = DriverManager.getConnection(getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3), props);
        stmt = futureConn.prepareStatement("delete from " + tableName + " where organization_id=? and entity_id=?");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.execute();
        futureConn.commit();
        futureConn.close();

        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 10);
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
        stmt.setString(2, ROW4);
        stmt.setInt(3, 5);
        stmt.execute(); // should commit too
        
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 15);
        Connection conn1 = DriverManager.getConnection(url, props);
        analyzeTable(conn1, tableName);
        conn1.close();
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 30);
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 9);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT organization_id, a_string AS a FROM " + tableName + " WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
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
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
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

        // Override value again, but should be ignored since it's past the SCN
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3);
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setInt(3, 0);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT a_integer,b_string FROM " + tableName + " WHERE organization_id=? and a_integer <= 5 limit 2";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
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
