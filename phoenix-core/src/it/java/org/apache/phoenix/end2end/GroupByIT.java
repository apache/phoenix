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
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;


@RunWith(Parameterized.class)
public class GroupByIT extends BaseQueryIT {

    public GroupByIT(String indexDDL, boolean columnEncoded) throws Exception {
        super(indexDDL, columnEncoded, false);
    }
    
    @Parameters(name="GroupByIT_{index}") // name is used by failsafe as file name in reports
    public static Collection<Object> data() {
        return BaseQueryIT.allIndexes();
    }
    
    @Test
    public void testGroupedAggregation() throws Exception {
        // Tests that you don't get an ambiguous column exception when using the same alias as the column name
        String query = "SELECT a_string as a_string, count(1), 'foo' FROM " + tableName + " WHERE organization_id=? GROUP BY a_string";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    public void testGroupByWithIntegerDivision1() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table " + table + "(\"time\" integer not null, hostname varchar not null,usage float,period integer constraint pk PRIMARY KEY(\"time\", hostname))";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into " + table + " values(1439853462,'qa9',8.27,1439853462)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into " + table + " values(1439853461,'qa9',8.27,1439853362)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into " + table + " values(1439853461,'qa9',5.27,1439853461)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into " + table + " values(1439853451,'qa9',4.27,1439853451)");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select \"time\"/10 as tm, hostname, avg(usage) from " + table + " group by hostname, tm");
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table " + table + "(\"time\" integer not null, hostname varchar not null,usage float,period integer constraint pk PRIMARY KEY(\"time\", hostname))";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into " + table + " values(1439853462,'qa9',8.27,1439853462)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into " + table + " values(1439853461,'qa9',8.27,1439853362)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into " + table + " values(1439853461,'qa9',5.27,1439853461)");
        stmt.execute();
        stmt = conn.prepareStatement("upsert into " + table + " values(1439853451,'qa9',4.27,1439853451)");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select period/10 as tm, hostname, avg(usage) from " + table + " group by hostname, tm");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(143985345, rs.getInt(1));
        assertEquals(4.2699, rs.getDouble(3), 0.1);
        assertTrue(rs.next());
        assertEquals(143985346, rs.getInt(1));
        assertEquals(6.77, rs.getDouble(3), 0.1);
    }
    
    @Test
    public void testPointInTimeGroupedAggregation() throws Exception {
        String updateStmt =
                "upsert into " + tableName + " VALUES ('" + tenantId + "','" + ROW5 + "','"
                        + C_VALUE + "')";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        Statement stmt = upsertConn.createStatement();
        stmt.execute(updateStmt); // should commit too
        upsertConn.close();
        
        long upsert1Time = System.currentTimeMillis();
        long timeDelta = 100;
        Thread.sleep(timeDelta);

        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        updateStmt = "upsert into " + tableName + " VALUES (?, ?, ?)";
        // Insert all rows at ts
        PreparedStatement pstmt = upsertConn.prepareStatement(updateStmt);
        pstmt.setString(1, tenantId);
        pstmt.setString(2, ROW5);
        pstmt.setString(3, E_VALUE);
        pstmt.execute(); // should commit too
        upsertConn.close();
        
        long queryTime = upsert1Time + timeDelta / 2;
        String query =
                "SELECT a_string, count(1) FROM " + tableName + " WHERE organization_id='"
                        + tenantId + "' GROUP BY a_string";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(queryTime));
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

    @SuppressWarnings("unchecked")
    @Test
    public void testGroupByCondition() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    

}
