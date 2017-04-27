/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;


@RunWith(Parameterized.class)
public class GroupByIT extends BaseQueryIT {

    public GroupByIT(String indexDDL, boolean mutable, boolean columnEncoded) {
        super(indexDDL, mutable, columnEncoded);
    }
    
    @Parameters(name="GroupByIT_{index}") // name is used by failsafe as file name in reports
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
    
    @Test
    public void testNoWhereScan() throws Exception {
        String query = "SELECT y_integer FROM " + tableName;
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
        String query = "SELECT a_string as a_string, count(1), 'foo' FROM " + tableName + " WHERE organization_id=? GROUP BY a_string";
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
        String query = "SELECT DISTINCT a_string, count(1), 'foo' FROM " + tableName + " WHERE organization_id=? GROUP BY a_string, b_string ORDER BY a_string, count(1)";
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
        String query = "SELECT /*+ NO_INDEX */ DISTINCT a_string, count(1), 'foo' FROM " + tableName + " WHERE organization_id=? GROUP BY a_string, b_string ORDER BY count(1) desc,a_string LIMIT 2";
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
        String query = "SELECT DISTINCT count(1), 'foo' FROM " + tableName + " WHERE organization_id=?";
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
        String query = "SELECT a_string, count(1) FROM " + tableName + " WHERE organization_id=? GROUP BY a_string LIMIT 2";
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
            "upsert into " + tableName + 
            " VALUES ('" + tenantId + "','" + ROW5 + "','" + C_VALUE +"')";
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
            "upsert into " + tableName +
            " VALUES (?, ?, ?)";
        // Insert all rows at ts
        PreparedStatement pstmt = upsertConn.prepareStatement(updateStmt);
        pstmt.setString(1, tenantId);
        pstmt.setString(2, ROW5);
        pstmt.setString(3, E_VALUE);
        pstmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT a_string, count(1) FROM " + tableName + " WHERE organization_id='" + tenantId + "' GROUP BY a_string";
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
        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
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
        String query = "SELECT count(*) FROM " + tableName;
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
            "upsert into " + tableName + 
            " (" +
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
        
        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ?";
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
            "upsert into " + tableName +
            " (" +
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

        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id=? and a_string = ? LIMIT 3";
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
    public void testGroupByWithIntegerDivision1() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "create table test1(\"time\" integer not null, hostname varchar not null,usage float,period integer constraint pk PRIMARY KEY(\"time\", hostname))";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into test1 values(1439853462,'qa9',8.27,1439853462)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into test1 values(1439853461,'qa9',8.27,1439853362)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into test1 values(1439853461,'qa9',5.27,1439853461)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into test1 values(1439853451,'qa9',4.27,1439853451)");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select \"time\"/10 as tm, hostname, avg(usage) from test1 group by hostname, tm");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(143985345, rs.getInt(1));
        assertEquals(4.2699, rs.getDouble(3), 0.1);
        assertTrue(rs.next());
        assertEquals(143985346, rs.getInt(1));
        assertEquals(6.77, rs.getDouble(3), 0.1);
    }

    @Test
    public void testGroupByWithIntegerDivision2() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "create table test1(\"time\" integer not null, hostname varchar not null,usage float,period integer constraint pk PRIMARY KEY(\"time\", hostname))";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into test1 values(1439853462,'qa9',8.27,1439853462)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into test1 values(1439853461,'qa9',8.27,1439853362)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into test1 values(1439853461,'qa9',5.27,1439853461)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into test1 values(1439853451,'qa9',4.27,1439853451)");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select period/10 as tm, hostname, avg(usage) from test1 group by hostname, tm");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(143985345, rs.getInt(1));
        assertEquals(4.2699, rs.getDouble(3), 0.1);
        assertTrue(rs.next());
        assertEquals(143985346, rs.getInt(1));
        assertEquals(6.77, rs.getDouble(3), 0.1);
    }
}
