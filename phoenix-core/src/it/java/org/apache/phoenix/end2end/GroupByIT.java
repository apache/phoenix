/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
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
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


@RunWith(Parameterized.class)
public class GroupByIT extends BaseQueryIT {

    public GroupByIT(String indexDDL) {
        super(indexDDL);
    }
    
    @Parameters(name="{0}")
    public static Collection<Object> data() {
        return QueryIT.data();
    }
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseQueryIT.class)
    public static void doSetup() throws Exception {
    	Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
    	props.put(QueryServices.DEFAULT_KEEP_DELETED_CELLS_ATTRIB, "true");
    	BaseQueryIT.doSetup(props);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testGroupByCondition() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement("SELECT count(*) FROM aTable WHERE organization_id=? GROUP BY a_integer=6");
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertValueEqualsResultSet(rs, Arrays.<Object>asList(1L,8L));
        try {
            statement = conn.prepareStatement("SELECT count(*),a_integer=6 FROM aTable WHERE organization_id=? and (a_integer IN (5,6) or a_integer is null) GROUP BY a_integer=6");
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
            statement = conn.prepareStatement("UPSERT into aTable(organization_id,entity_id,a_integer) values(?,?,null)");
            statement.setString(1, tenantId);
            statement.setString(2, ROW3);
            statement.executeUpdate();
            conn.commit();
        } finally {
            conn.close();
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 60));
        conn = DriverManager.getConnection(getUrl(), props);
        statement = conn.prepareStatement("SELECT count(*) FROM aTable WHERE organization_id=? GROUP BY a_integer=6");
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertValueEqualsResultSet(rs, Arrays.<Object>asList(1L,1L,7L));
        statement = conn.prepareStatement("SELECT a_integer, entity_id FROM aTable WHERE organization_id=? and (a_integer IN (5,6) or a_integer is null)");
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        List<List<Object>> expectedResults = Lists.newArrayList(
                Arrays.<Object>asList(null,ROW3),
                Arrays.<Object>asList(5,ROW5),
                Arrays.<Object>asList(6,ROW6));
        assertValuesEqualsResultSet(rs, expectedResults);
        try {
            statement = conn.prepareStatement("SELECT count(*),a_integer=6 FROM aTable WHERE organization_id=? and (a_integer IN (5,6) or a_integer is null) GROUP BY a_integer=6");
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
    public void testNoWhereScan() throws Exception {
        String query = "SELECT y_integer FROM aTable";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            for (int i =0; i < 8; i++) {
                assertTrue (rs.next());
                assertEquals(0, rs.getInt(1));
                assertTrue(rs.wasNull());
            }
            assertTrue (rs.next());
            assertEquals(300, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
 // FIXME: this is flapping with an phoenix.memory.InsufficientMemoryException
    // in the GroupedAggregateRegionObserver. We can work around it by increasing
    // the amount of available memory in QueryServicesTestImpl, but we shouldn't
    // have to. I think something may no be being closed to reclaim the memory.
    @Test
    public void testGroupedAggregation() throws Exception {
        // Tests that you don't get an ambiguous column exception when using the same alias as the column name
        String query = "SELECT a_string as a_string, count(1), 'foo' FROM atable WHERE organization_id=? GROUP BY a_string";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 4L);
            assertEquals(rs.getString(3), "foo");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 4L);
            assertEquals(rs.getString(3), "foo");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), C_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctGroupedAggregation() throws Exception {
        String query = "SELECT DISTINCT a_string, count(1), 'foo' FROM atable WHERE organization_id=? GROUP BY a_string, b_string ORDER BY a_string, count(1)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), C_VALUE);
            assertEquals(rs.getLong(2), 1L);
            assertEquals(rs.getString(3), "foo");
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctLimitedGroupedAggregation() throws Exception {
        String query = "SELECT /*+ NO_INDEX */ DISTINCT a_string, count(1), 'foo' FROM atable WHERE organization_id=? GROUP BY a_string, b_string ORDER BY count(1) desc,a_string LIMIT 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            /*
            List<List<Object>> expectedResultsA = Lists.newArrayList(
                    Arrays.<Object>asList(A_VALUE, 2L, "foo"),
                    Arrays.<Object>asList(B_VALUE, 2L, "foo"));
            List<List<Object>> expectedResultsB = Lists.newArrayList(expectedResultsA);
            Collections.reverse(expectedResultsB);
            // Since we're not ordering and we may be using a descending index, we don't
            // know which rows we'll get back.
            assertOneOfValuesEqualsResultSet(rs, expectedResultsA,expectedResultsB);
            */

            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getLong(2), 2L);
            assertEquals(rs.getString(3), "foo");
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctUngroupedAggregation() throws Exception {
        String query = "SELECT DISTINCT count(1), 'foo' FROM atable WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(9L, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGroupedLimitedAggregation() throws Exception {
        String query = "SELECT a_string, count(1) FROM atable WHERE organization_id=? GROUP BY a_string LIMIT 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(4L, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(4L, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPointInTimeGroupedAggregation() throws Exception {
        String updateStmt = 
            "upsert into " +
            "ATABLE VALUES ('" + tenantId + "','" + ROW5 + "','" + C_VALUE +"')";
        // Override value that was set at creation time
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        Statement stmt = upsertConn.createStatement();
        stmt.execute(updateStmt); // should commit too
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        updateStmt = 
            "upsert into " +
            "ATABLE VALUES (?, ?, ?)";
        // Insert all rows at ts
        PreparedStatement pstmt = upsertConn.prepareStatement(updateStmt);
        pstmt.setString(1, tenantId);
        pstmt.setString(2, ROW5);
        pstmt.setString(3, E_VALUE);
        pstmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT a_string, count(1) FROM atable WHERE organization_id='" + tenantId + "' GROUP BY a_string";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        conn.close();
    }

    @Test
    public void testUngroupedAggregation() throws Exception {
        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ?";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5)); // Execute query at ts + 5
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
        // Run again to catch unintentianal deletion of rows during an ungrouped aggregation (W-1455633)
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 6)); // Execute at ts + 6
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
        String query = "SELECT count(*) FROM atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_STRING) " +
            "VALUES (?, ?, ?)";
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

        // Override value again, but should be ignored since it's past the SCN
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, E_VALUE);
        stmt.execute();
        upsertConn.close();
        
        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ?";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 2) + ";foo=bar"; // Run query at timestamp 2 
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
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_STRING) " +
            "VALUES (?, ?, ?)";
        // Override value that was set at creation time
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
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

        // Override value again, but should be ignored since it's past the SCN
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, E_VALUE);
        stmt.execute();
        upsertConn.close();

        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ? LIMIT 3";
        // Specify CurrentSCN on URL with extra stuff afterwards (which should be ignored)
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 2) + ";foo=bar"; // Run query at timestamp 2 
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
    public void testPointInTimeDeleteUngroupedAggregation() throws Exception {
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
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
        stmt = conn.prepareStatement("delete from atable where organization_id=? and entity_id=?");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.execute();
        conn.commit();
        conn.close();
        
        // Delete row at timestamp 3. This should not be seen by the query executing
        // Remove column value at ts + 1 (i.e. equivalent to setting the value to null)
        Connection futureConn = DriverManager.getConnection(getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3), props);
        stmt = futureConn.prepareStatement("delete from atable where organization_id=? and entity_id=?");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.execute();
        futureConn.commit();
        futureConn.close();

        String query = "SELECT count(1) FROM atable WHERE organization_id=? and a_string = ?";
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

}
