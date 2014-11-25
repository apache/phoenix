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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;



/**
 * 
 * Extended tests for Phoenix JDBC implementation
 * 
 */

public class ExtendedQueryExecIT extends BaseClientManagedTimeIT {

    @Test
    public void testToDateFunctionBind() throws Exception {
        long ts = nextTimestamp();
        Date date = new Date(1);
        String tenantId = getOrganizationId();

        initATableValues(tenantId, getDefaultSplits(tenantId),date, ts);
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT a_date FROM atable WHERE organization_id='" + tenantId + "' and a_date < TO_DATE(?)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, "1970-1-1 12:00:00");
            ResultSet rs = statement.executeQuery();
            verifyDateResultSet(rs, date, 3);
        } finally {
            conn.close();
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="RV_RETURN_VALUE_IGNORED",
            justification="Test code.")
    @Test
    public void testTypeMismatchToDateFunctionBind() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId),null, ts);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT a_date FROM atable WHERE organization_id='" + tenantId + "' and a_date < TO_DATE(?)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(2));
            statement.executeQuery();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch. expected: [VARCHAR] but was: DATE at TO_DATE"));
        } finally {
            conn.close();
        }
    }

    /**
     * Basic tests for date function
     * Related bug: W-1190856
     * @throws Exception
     */
    @Test
    public void testDateFunctions() throws Exception {
        long ts = nextTimestamp();
        Date date = new Date(1);
        String tenantId = getOrganizationId();

        initATableValues(tenantId, getDefaultSplits(tenantId),date, ts);
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            ResultSet rs;
            String queryPrefix = "SELECT a_date FROM atable WHERE organization_id='" + tenantId + "' and ";

            String queryDateArg = "a_date < TO_DATE('1970-1-1 12:00:00')";
            rs = getResultSet(conn, queryPrefix + queryDateArg);
            verifyDateResultSet(rs, date, 3);

            // TODO: Bug #1 - Result should be the same as the the case above
//          queryDateArg = "a_date < TO_DATE('70-1-1 12:0:0')";
//          rs = getResultSet(conn, queryPrefix + queryDateArg);
//          verifyDateResultSet(rs, date, 3);

            // TODO: Bug #2 - Exception should be generated for invalid date/time
//          queryDateArg = "a_date < TO_DATE('999-13-32 24:60:60')";
//          try {
//              getResultSet(conn, queryPrefix + queryDateArg);
//              fail("Expected SQLException");
//          } catch (SQLException ex) {
//              // expected
//          }
            
            queryDateArg = "a_date >= TO_DATE('1970-1-2 23:59:59') and a_date <= TO_DATE('1970-1-3 0:0:1')";
            rs = getResultSet(conn, queryPrefix + queryDateArg);
            verifyDateResultSet(rs, new Date(date.getTime() + (2*60*60*24*1000)), 3);

        } finally {
            conn.close();
        }
    }
    
    /**
     * aggregation - group by
     * @throws Exception
     */
    @Test
    public void testDateGroupBy() throws Exception {
        long ts = nextTimestamp();
        Date date = new Date(1);
        String tenantId = getOrganizationId();

        initATableValues(tenantId, getDefaultSplits(tenantId),date, ts);
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            ResultSet rs;
            String query = "SELECT a_date, count(1) FROM atable WHERE organization_id='" + tenantId + "' group by a_date";
            rs = getResultSet(conn, query);
            
            /* 3 rows in expected result:
             * 1969-12-31   3
             * 1970-01-01   3
             * 1970-01-02   3
             * */
                        
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1));
            assertEquals(3, rs.getInt(2));
            
            // the following assertions fails
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
            

        } finally {
            conn.close();
        }
    }
    
    private ResultSet getResultSet(Connection conn, String query) throws SQLException {
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        return rs;
    }
    
    private void verifyDateResultSet(ResultSet rs, Date date, int rowCount) throws SQLException {
        for (int i=0; i<rowCount; i++) {
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1));
        }
        assertFalse(rs.next());
    }
}
