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
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;


/**
 * 
 * Basic tests for Phoenix JDBC implementation
 *
 */
@Category(ParallelStatsDisabledTest.class)
public class QueryIT extends BaseQueryIT {
    
    @Parameters(name="QueryIT_{index}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object> data() {
        return BaseQueryIT.allIndexes();
    }    
    
    public QueryIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells) {
        super(indexDDL, columnEncoded, keepDeletedCells);
    }
    
    @Test
    public void testToDateOnString() throws Exception { // TODO: test more conversion combinations
        String query = "SELECT a_string FROM " + tableName + " WHERE organization_id=? and a_integer = 5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            rs.getDate(1);
            fail();
        } catch (SQLException e) { // Expected
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(),e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testColumnOnBothSides() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? and a_string = b_string";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testColumnAliasMapping() throws Exception {
        String query = "SELECT a.a_string, " + tableName + ".b_string FROM " + tableName + " a WHERE ?=organization_id and 5=a_integer ORDER BY a_string, b_string";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getString("B_string"), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAllScan() throws Exception {
        String query = "SELECT ALL a_string, b_string FROM " + tableName + " WHERE ?=organization_id and 5=a_integer";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getString("B_string"), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDistinctScan() throws Exception {
        String query = "SELECT DISTINCT a_string FROM " + tableName + " WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctLimitScan() throws Exception {
        String query = "SELECT DISTINCT a_string FROM " + tableName + " WHERE organization_id=? LIMIT 1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
