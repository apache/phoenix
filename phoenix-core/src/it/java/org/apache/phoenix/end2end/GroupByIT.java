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
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupByIT extends BaseQueryIT {

    public GroupByIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells) {
        super(indexDDL, columnEncoded, keepDeletedCells);
    }
    
    @Parameters(name="GroupByIT_{index}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object> data() {
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
    public void zTestGroupByWithIntegerDivision1() throws Exception {
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
    public void zTestGroupByWithIntegerDivision2() throws Exception {
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

    @Test
    public void testGroupByHavingWithAlias() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(false);
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName + " (a_string varchar not null, col1 integer"
              + " CONSTRAINT pk PRIMARY KEY (a_string))";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 40);
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setInt(2, 20);
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setInt(2, 30);
            stmt.execute();
            stmt.execute();
            conn.commit();

            String query = "SELECT a_string, sum(col1) as sumCol1 FROM " + tableName
              + " GROUP BY a_string HAVING sumCol1>20 ORDER BY sumCol1";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals(30, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(40, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGroupByHavingWithAlias2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(false);
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName + " (a_string varchar not null, col1 " +
                    "integer not null, col2 varchar, col3 integer"
                    + " CONSTRAINT pk PRIMARY KEY (a_string, col1))";
            createTestTable(getUrl(), ddl);

            String dml = "UPSERT INTO " + tableName + " VALUES(?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a1xyz");
            stmt.setInt(2, 40);
            stmt.setString(3, "col2xyz1");
            stmt.setInt(4, 50);
            stmt.execute();
            stmt.setString(1, "b1xyz");
            stmt.setInt(2, 20);
            stmt.setString(3, "col2xyz2");
            stmt.setInt(4, 60);
            stmt.execute();
            stmt.setString(1, "c1xyz");
            stmt.setInt(2, 30);
            stmt.setString(3, "col2xyz3");
            stmt.setInt(4, 70);
            stmt.execute();
            conn.commit();

            String query = "SELECT a_string, col1, sum(col1) as sumCol1 FROM " + tableName
                    + " GROUP BY a_string, col1 HAVING sumCol1 > 20 ORDER BY sumCol1";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c1xyz", rs.getString(1));
            assertEquals(30, rs.getInt(2));
            assertEquals(30, rs.getInt(3));
            assertTrue(rs.next());
            assertEquals("a1xyz", rs.getString(1));
            assertEquals(40, rs.getInt(2));
            assertEquals(40, rs.getInt(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

}
