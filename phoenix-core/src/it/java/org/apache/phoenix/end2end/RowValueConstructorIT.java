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

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID1;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID3;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID7;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTIDS;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_SALTED_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.PARENTID1;
import static org.apache.phoenix.util.TestUtil.PARENTID3;
import static org.apache.phoenix.util.TestUtil.PARENTID7;
import static org.apache.phoenix.util.TestUtil.PARENTIDS;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


public class RowValueConstructorIT extends BaseClientManagedTimeIT {
    
    @Test
    public void testRowValueConstructorInWhereWithEqualsExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) = (7, 5)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) == 7);
                assertTrue(rs.getInt(2) == 5);
                count++;
            }
            assertTrue(count == 1);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorInWhereWithGreaterThanExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) >= (4, 4)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) >= 4);
                assertTrue(rs.getInt(1) == 4 ? rs.getInt(2) >= 4 : rs.getInt(2) >= 0);
                count++;
            }
            // we have 6 values for a_integer present in the atable where a >= 4. x_integer is null for a_integer = 4. So the query should have returned 5 rows.
            assertEquals(5, count);   
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorInWhereWithUnEqualNumberArgs() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer, y_integer) >= (7, 5)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) >= 7);
                assertTrue(rs.getInt(1) == 7 ? rs.getInt(2) >= 5 : rs.getInt(2) >= 0);
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testBindVarsInRowValueConstructor() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) = (?, ?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setInt(2, 7);
            statement.setInt(3, 5);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) == 7);
                assertTrue(rs.getInt(2) == 5);
                count++;
            }
            assertTrue(count == 1); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnLHSAndLiteralExpressionOnRHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) >= 7";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnRHSLiteralExpressionOnLHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND 7 <= (a_integer, x_integer)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnLHSBuiltInFunctionOperatingOnIntegerLiteralRHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) >= to_number('7')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertEquals(3, count); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnRHSWithBuiltInFunctionOperatingOnIntegerLiteralOnLHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND to_number('7') <= (a_integer, x_integer)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertEquals(3, count); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnLHSWithBuiltInFunctionOperatingOnColumnRefOnRHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts - 1, getUrl(), null);
        String upsertQuery = "UPSERT INTO aTable(organization_id, entity_id, a_string) values (?, ?, ?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertQuery);
            statement.setString(1, tenantId);
            statement.setString(2, ROW1);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW3);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW4);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW5);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW6);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW7);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW8);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW9);
            statement.setString(3, "7");
            statement.executeUpdate();
            conn.commit();

            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
            conn = DriverManager.getConnection(getUrl(), props);
            statement = conn.prepareStatement("select a_string from atable where organization_id = ? and (6, x_integer) <= to_number(a_string)");
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowValueConstructorOnRHSWithBuiltInFunctionOperatingOnColumnRefOnLHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts - 1, getUrl(), null);
        String upsertQuery = "UPSERT INTO aTable(organization_id, entity_id, a_string) values (?, ?, ?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertQuery);
            statement.setString(1, tenantId);
            statement.setString(2, ROW1);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW3);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW4);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW5);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW6);
            statement.setString(3, "1");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW7);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW8);
            statement.setString(3, "7");
            statement.executeUpdate();
            statement.setString(1, tenantId);
            statement.setString(2, ROW9);
            statement.setString(3, "7");
            statement.executeUpdate();
            conn.commit();

            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
            conn = DriverManager.getConnection(getUrl(), props);
            statement = conn.prepareStatement("select a_string from atable where organization_id = ? and to_number(a_string) >= (6, 6)");
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3); 
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testQueryMoreWithInListRowValueConstructor() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        Date date = new Date(System.currentTimeMillis());
        initEntityHistoryTableValues(tenantId, getDefaultSplits(tenantId), date, ts);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        PreparedStatement statement = conn.prepareStatement("select parent_id from " + ENTITY_HISTORY_TABLE_NAME + 
                     " WHERE (organization_id, parent_id, created_date, entity_history_id) IN ((?, ?, ?, ?),(?,?,?,?))");
        statement.setString(1, tenantId);
        statement.setString(2, PARENTID3);
        statement.setDate(3, date);
        statement.setString(4, ENTITYHISTID3);
        statement.setString(5, tenantId);
        statement.setString(6, PARENTID7);
        statement.setDate(7, date);
        statement.setString(8, ENTITYHISTID7);
        ResultSet rs = statement.executeQuery();
        
        assertTrue(rs.next());
        assertEquals(PARENTID3, rs.getString(1));
        assertTrue(rs.next());
        assertEquals(PARENTID7, rs.getString(1));
        assertFalse(rs.next());
     }
    
    @Test
    public void testQueryMoreFunctionalityUsingAllPKColsInRowValueConstructor() throws Exception {
        _testQueryMoreFunctionalityUsingAllPkColsInRowValueConstructor(false);
    }

    @Test
    public void testQueryMoreFunctionalityUsingAllPKColsInRowValueConstructor_Salted() throws Exception {
        _testQueryMoreFunctionalityUsingAllPkColsInRowValueConstructor(true);
    }

    private void _testQueryMoreFunctionalityUsingAllPkColsInRowValueConstructor(boolean salted) throws Exception, SQLException {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        Date date = new Date(System.currentTimeMillis());
        if(salted) {
            initSaltedEntityHistoryTableValues(tenantId, null, date, ts - 1);
        } else {
            initEntityHistoryTableValues(tenantId, getDefaultSplits(tenantId), date, ts - 1);
        }
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        String startingOrgId = tenantId;
        String startingParentId = PARENTID1;
        Date startingDate = date;
        String startingEntityHistId = ENTITYHISTID1;
        PreparedStatement statement = null;
        if(salted) {
            statement = conn.prepareStatement("select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from " + ENTITY_HISTORY_SALTED_TABLE_NAME + 
                    " WHERE (organization_id, parent_id, created_date, entity_history_id) > (?, ?, ?, ?) ORDER BY organization_id, parent_id, created_date, entity_history_id LIMIT 3 ");
        } else {
            statement = conn.prepareStatement("select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from " + ENTITY_HISTORY_TABLE_NAME + 
                    " WHERE (organization_id, parent_id, created_date, entity_history_id) > (?, ?, ?, ?) ORDER BY organization_id, parent_id, created_date, entity_history_id LIMIT 3 ");
        }
        statement.setString(1, startingOrgId);
        statement.setString(2, startingParentId);
        statement.setDate(3, startingDate);
        statement.setString(4, startingEntityHistId);
        ResultSet rs = statement.executeQuery();

        int count = 0;
        int i = 1;
        //this loop should work on rows 2, 3, 4.
        while(rs.next()) {
            assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
            assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
            count++;
            i++;
            if(count == 3) {
                startingOrgId = rs.getString(1);
                startingParentId = rs.getString(2);
                startingDate = rs.getDate(3);
                startingEntityHistId = rs.getString(4);
            }
        }

        assertTrue("Number of rows returned: ", count == 3);
        //We will now use the row 4's pk values for bind variables. 
        statement.setString(1, startingOrgId);
        statement.setString(2, startingParentId);
        statement.setDate(3, startingDate);
        statement.setString(4, startingEntityHistId);
        rs = statement.executeQuery();
        //this loop now should work on rows 5, 6, 7.
        while(rs.next()) {
            assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
            assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
            i++;
            count++;
        }
        assertTrue("Number of rows returned: ", count == 6);
    }

    @Test
    public void testQueryMoreWithSubsetofPKColsInRowValueConstructor() throws Exception {
        _testQueryMoreWithSubsetofPKColsInRowValueConstructor(false);
    }

    @Test
    public void testQueryMoreWithSubsetofPKColsInRowValueConstructor_salted() throws Exception {
        _testQueryMoreWithSubsetofPKColsInRowValueConstructor(true);
    }

    /**
     * Entity History table has primary keys defined in the order
     * PRIMARY KEY (organization_id, parent_id, created_date, entity_history_id). 
     * This test uses (organization_id, parent_id, entity_history_id) in RVC and checks if the query more functionality
     * still works. 
     * @throws Exception
     */
    private void _testQueryMoreWithSubsetofPKColsInRowValueConstructor(boolean salted) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        Date date = new Date(System.currentTimeMillis());
        if(salted) {
            initSaltedEntityHistoryTableValues(tenantId, null, date, ts - 1);
        } else {
            initEntityHistoryTableValues(tenantId, getDefaultSplits(tenantId), date, ts - 1);
        }
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);

        //initial values of pk.
        String startingOrgId = tenantId;
        String startingParentId = PARENTID1;

        String startingEntityHistId = ENTITYHISTID1;

        PreparedStatement statement = null;
        if(salted) {
            statement = conn.prepareStatement("select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from " + ENTITY_HISTORY_SALTED_TABLE_NAME + 
                    " WHERE (organization_id, parent_id, entity_history_id) > (?, ?, ?) ORDER BY organization_id, parent_id, entity_history_id LIMIT 3 ");
        } else {
            statement = conn.prepareStatement("select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from " + ENTITY_HISTORY_TABLE_NAME + 
                    " WHERE (organization_id, parent_id, entity_history_id) > (?, ?, ?) ORDER BY organization_id, parent_id, entity_history_id LIMIT 3 ");
        }
        statement.setString(1, startingOrgId);
        statement.setString(2, startingParentId);
        statement.setString(3, startingEntityHistId);
        ResultSet rs = statement.executeQuery();
        int count = 0;
        //this loop should work on rows 2, 3, 4.
        int i = 1;
        while(rs.next()) {
            assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
            assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
            i++;
            count++;
            if(count == 3) {
                startingOrgId = rs.getString(1);
                startingParentId = rs.getString(2);
                startingEntityHistId = rs.getString(4);
            }
        }
        assertTrue("Number of rows returned: " + count, count == 3);
        //We will now use the row 4's pk values for bind variables. 
        statement.setString(1, startingOrgId);
        statement.setString(2, startingParentId);
        statement.setString(3, startingEntityHistId);
        rs = statement.executeQuery();
        //this loop now should work on rows 5, 6, 7.
        while(rs.next()) {
            assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
            assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
            i++;
            count++;
        }
        assertTrue("Number of rows returned: " + count, count == 6);
    }

    @Test
    public void testQueryMoreWithLeadingPKColSkippedInRowValueConstructor() throws Exception {
        _testQueryMoreWithLeadingPKColSkippedInRowValueConstructor(false);
    }

    @Test
    public void testQueryMoreWithLeadingPKColSkippedInRowValueConstructor_salted() throws Exception {
        _testQueryMoreWithLeadingPKColSkippedInRowValueConstructor(true);
    }

    /**
     * Entity History table has primary keys defined in the order
     * PRIMARY KEY (organization_id, parent_id, created_date, entity_history_id). 
     * This test skips the leading column organization_id and uses (parent_id, created_date, entity_history_id) in RVC.
     * In such a case Phoenix won't be able to optimize the hbase scan. However, the query more functionality
     * should still work. 
     * @throws Exception
     */
    private void _testQueryMoreWithLeadingPKColSkippedInRowValueConstructor(boolean salted) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        Date date = new Date(System.currentTimeMillis());
        if(salted) {
            initSaltedEntityHistoryTableValues(tenantId, null, date, ts - 1);
        } else {
            initEntityHistoryTableValues(tenantId, getDefaultSplits(tenantId), date, ts - 1);
        }
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String startingParentId = PARENTID1;
        Date startingDate = date;
        String startingEntityHistId = ENTITYHISTID1;
        PreparedStatement statement = null;
        if(salted) {
            statement = conn.prepareStatement("select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from " + ENTITY_HISTORY_SALTED_TABLE_NAME + 
                    " WHERE (parent_id, created_date, entity_history_id) > (?, ?, ?) ORDER BY parent_id, created_date, entity_history_id LIMIT 3 ");
        } else {
            statement = conn.prepareStatement("select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from " + ENTITY_HISTORY_TABLE_NAME + 
                    " WHERE (parent_id, created_date, entity_history_id) > (?, ?, ?) ORDER BY parent_id, created_date, entity_history_id LIMIT 3 ");
        }
        statement.setString(1, startingParentId);
        statement.setDate(2, startingDate);
        statement.setString(3, startingEntityHistId);
        ResultSet rs = statement.executeQuery();
        int count = 0;
        //this loop should work on rows 2, 3, 4.
        int i = 1;
        while(rs.next()) {
            assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
            assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
            i++;
            count++;
            if(count == 3) {
                startingParentId = rs.getString(2);
                startingDate = rs.getDate(3);
                startingEntityHistId = rs.getString(4);
            }
        }
        assertTrue("Number of rows returned: " + count, count == 3);
        //We will now use the row 4's pk values for bind variables. 
        statement.setString(1, startingParentId);
        statement.setDate(2, startingDate);
        statement.setString(3, startingEntityHistId);
        rs = statement.executeQuery();
        //this loop now should work on rows 5, 6, 7.
        while(rs.next()) {
            assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
            assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
            i++;
            count++;
        }
        assertTrue("Number of rows returned: " + count, count == 6);
    } 
    
    @Test
    public void testRVCWithNonLeadingPkColsOfTypesIntegerAndString() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, a_string FROM aTable WHERE ?=organization_id  AND (a_integer, a_string) <= (5, 'a')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            //we have 4 rows present in a table with (a_integer, a_string) <= (5, 'a'). All have a_string set to "a".
            while(rs.next()) {
                assertTrue(rs.getInt(1) <= 5);
                assertTrue(rs.getString(2).compareTo("a") == 0);
                count++;
            }
            assertTrue(count == 4);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRVCWithNonLeadingPkColsOfTypesTimeStampAndString() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_TIMESTAMP) " +
            "VALUES (?, ?, ?)";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true);
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        Timestamp tsValue = new Timestamp(System.nanoTime());
        stmt.setTimestamp(3, tsValue);
        stmt.execute();
        
        String query = "SELECT a_timestamp, a_string FROM aTable WHERE ?=organization_id  AND (a_timestamp, a_string) = (?, 'a')";
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setTimestamp(2, tsValue);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getTimestamp(1).equals(tsValue));
                assertTrue(rs.getString(2).compareTo("a") == 0);
                count++;
            }
            assertTrue(count == 1);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNestedRVCBasic() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        //all the three queries should return the same rows.
        String[] queries = {"SELECT organization_id, entity_id, a_string FROM aTable WHERE ((organization_id, entity_id), a_string) >= ((?, ?), ?)",
                            "SELECT organization_id, entity_id, a_string FROM aTable WHERE (organization_id, entity_id, a_string) >= (?, ?, ?)",
                            "SELECT organization_id, entity_id, a_string FROM aTable WHERE (organization_id, (entity_id, a_string)) >= (?, (?, ?))"
                           };
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = null;
        try {
            try {
                for (int i = 0; i <=2; i++) {
                    statement = conn.prepareStatement(queries[i]);
                    statement.setString(1, tenantId);
                    statement.setString(2, ROW1);
                    statement.setString(3, "a");
                    ResultSet rs = statement.executeQuery();
                    int count = 0;
                    while(rs.next()) {
                        count++;
                    }
                    assertEquals(9, count);
                } 
            } finally {
                if(statement != null) {
                    statement.close();
                }
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRVCWithInListClausePossibleNullValues() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        //we have a row present in aTable where x_integer = 5 and y_integer = NULL which gets translated to 0 when retriving from HBase. 
        String query = "SELECT x_integer, y_integer FROM aTable WHERE ? = organization_id AND (x_integer, y_integer) IN ((5))";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRVCWithInListClauseUsingSubsetOfPKColsInOrder() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        // Though we have a row present in aTable where organization_id = tenantId and  x_integer = 5,
        // we'd also need to have an entity_id that is null (which we don't have).
        String query = "SELECT organization_id, entity_id FROM aTable WHERE (organization_id, entity_id) IN (('" + tenantId + "')) AND x_integer = 5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            try {
                conn = DriverManager.getConnection(getUrl(), props);
                statement = conn.prepareStatement(query);
                ResultSet rs = statement.executeQuery();
                assertFalse(rs.next());
            } finally {
                if(statement != null) {
                    statement.close();
                }
            }
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
    
    @Test
    public void testRVCWithCeilAndFloorNeededForDecimal() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id  AND (a_integer, x_integer) < (8.6, 4.5) AND (a_integer, x_integer) > (6.8, 4)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
                assertEquals(7, rs.getInt(1));
                assertEquals(5, rs.getInt(2));
            }
            assertEquals(1, count); 
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRVCWithCeilAndFloorNeededForTimestamp() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        Date dateUpserted = DateUtil.parseDate("2012-01-01 14:25:28");
        dateUpserted = new Date(dateUpserted.getTime() + 660); // this makes the dateUpserted equivalent to 2012-01-01 14:25:28.660
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), dateUpserted, ts, getUrl(), null);
        String query = "SELECT a_integer, a_date FROM aTable WHERE ?=organization_id  AND (a_integer, a_date) <= (9, ?) AND (a_integer, a_date) >= (6, ?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            Timestamp timestampWithNanos = DateUtil.getTimestamp(dateUpserted.getTime() + 2 * MILLIS_IN_DAY, 300);
            timestampWithNanos.setNanos(0);
            statement.setTimestamp(2, timestampWithNanos);
            statement.setTimestamp(3, timestampWithNanos);
            ResultSet rs = statement.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            /*
             * We have following rows with values for (a_integer, a_date) present:
             * (7, date), (8, date + TestUtil.MILLIS_IN_DAY), (9, date 2 * TestUtil.MILLIS_IN_DAY) among others present.
             * Above query should return 3 rows since the CEIL operation on timestamp with non-zero nanoseconds
             * will bump up the milliseconds. That is (a_integer, a_date) < (6, CEIL(timestampWithNanos)).   
             */
            assertEquals(3, count);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRVCWithMultiCompKeysForIn() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE t (pk1 varchar, pk2 varchar, constraint pk primary key (pk1,pk2))");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO t VALUES('a','a')");
        conn.createStatement().execute("UPSERT INTO t VALUES('b','b')");
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t WHERE (pk1,pk2) IN (('a','a'),('b','b'))");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertFalse(rs.next());
    }
    
    private Connection nextConnection(String url) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        return DriverManager.getConnection(url, props);
    }
    
    //Table type - multi-tenant. Salted - No. Query against - tenant specific view. Connection used for running IN list query - tenant specific. 
    @Test
    public void testInListOfRVC1() throws Exception {
        String tenantId = "ABC";
        String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
        String baseTableDDL = "CREATE TABLE t (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) MULTI_TENANT=true";
        createTestTable(getUrl(), baseTableDDL, null, nextTimestamp());
        String tenantTableDDL = "CREATE VIEW t_view (tenant_col VARCHAR) AS SELECT *\n" + 
                "                FROM t";
        createTestTable(tenantSpecificUrl, tenantTableDDL, null, nextTimestamp());

        Connection conn = nextConnection(tenantSpecificUrl);
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo3', 3, 3)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo4', 4, 4)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo5', 5, 5)");
        conn.commit();
        conn.close();

        conn = nextConnection(tenantSpecificUrl);
        //order by needed on the query to make the order of rows returned deterministic.
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t_view WHERE (pk2, pk3) IN ((?, ?), (?, ?)) ORDER BY pk2");
        stmt.setString(1, "helo3");
        stmt.setInt(2, 3);
        stmt.setString(3, "helo5");
        stmt.setInt(4, 5);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        conn.close();
    }
    
    //Table type - multi-tenant. Salted - No. Query against - base table. Connection used for running IN list query - global. 
    @Test
    public void testInListOfRVC2() throws Exception {
        String tenantId = "ABC";
        String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
        String baseTableDDL = "CREATE TABLE t (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) MULTI_TENANT=true";
        createTestTable(getUrl(), baseTableDDL, null, nextTimestamp());
        String tenantTableDDL = "CREATE VIEW t_view (tenant_col VARCHAR) AS SELECT *\n" + 
                "                FROM t";
        createTestTable(tenantSpecificUrl, tenantTableDDL, null, nextTimestamp());

        Connection conn = nextConnection(tenantSpecificUrl);
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo3', 3, 3)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo4', 4, 4)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo5', 5, 5)");
        conn.commit();
        conn.close();

        conn = nextConnection(getUrl());
        //order by needed on the query to make the order of rows returned deterministic.
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo3");
        stmt.setInt(3, 3);
        stmt.setString(4, tenantId);
        stmt.setString(5, "helo5");
        stmt.setInt(6, 5);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        conn.close();
    }
    
    //Table type - non multi-tenant. Salted - No. Query against - Table. Connection used for running IN list query - global. 
    @Test
    public void testInListOfRVC3() throws Exception {
        String tenantId = "ABC";
        String tableDDL = "CREATE TABLE t (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3))";
        createTestTable(getUrl(), tableDDL, null, nextTimestamp());

        Connection conn = nextConnection(getUrl());
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo3', 3, 3)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo4', 4, 4)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo5', 5, 5)");
        conn.commit();
        conn.close();

        conn = nextConnection(getUrl());
        //order by needed on the query to make the order of rows returned deterministic.
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo3");
        stmt.setInt(3, 3);
        stmt.setString(4, tenantId);
        stmt.setString(5, "helo5");
        stmt.setInt(6, 5);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        conn.close();
    }
    
    //Table type - multi-tenant. Salted - Yes. Query against - base table. Connection used for running IN list query - global. 
    @Test 
    public void testInListOfRVC4() throws Exception {
        String tenantId = "ABC";
        String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
        String baseTableDDL = "CREATE TABLE t (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) SALT_BUCKETS=4, MULTI_TENANT=true";
        createTestTable(getUrl(), baseTableDDL, null, nextTimestamp());
        String tenantTableDDL = "CREATE VIEW t_view (tenant_col VARCHAR) AS SELECT *\n" + 
                "                FROM t";
        createTestTable(tenantSpecificUrl, tenantTableDDL, null, nextTimestamp());

        Connection conn = nextConnection(tenantSpecificUrl);
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo3', 3, 3)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo4', 4, 4)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo5', 5, 5)");
        conn.commit();
        conn.close();

        conn = nextConnection(getUrl());
        //order by needed on the query to make the order of rows returned deterministic.
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo3");
        stmt.setInt(3, 3);
        stmt.setString(4, tenantId);
        stmt.setString(5, "helo5");
        stmt.setInt(6, 5);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        conn.close();
    }
    
    //Table type - non multi-tenant. Salted - Yes. Query against - regular table. Connection used for running IN list query - global. 
    @Test 
    public void testInListOfRVC5() throws Exception {
        String tenantId = "ABC";
        String tableDDL = "CREATE TABLE t (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) SALT_BUCKETS=4";
        createTestTable(getUrl(), tableDDL, null, nextTimestamp());

        Connection conn = nextConnection(getUrl());
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo3', 3, 3)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo4', 4, 4)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo5', 5, 5)");
        conn.commit();
        conn.close();

        conn = nextConnection(getUrl());
        //order by needed on the query to make the order of rows returned deterministic.
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo3");
        stmt.setInt(3, 3);
        stmt.setString(4, tenantId);
        stmt.setString(5, "helo5");
        stmt.setInt(6, 5);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        conn.close();
    }
    
    @Test 
    public void testInListOfRVCColumnValuesSmallerLengthThanSchema() throws Exception {
        String tenantId = "ABC";
        String tableDDL = "CREATE TABLE t (tenantId char(15) NOT NULL, pk2 char(15) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3))";
        createTestTable(getUrl(), tableDDL, null, nextTimestamp());

        Connection conn = nextConnection(getUrl());
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'hel1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'hel2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'hel3', 3, 3)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'hel4', 4, 4)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'hel5', 5, 5)");
        conn.commit();
        conn.close();

        conn = nextConnection(getUrl());
        //order by needed on the query to make the order of rows returned deterministic.
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY PK2");
        stmt.setString(1, tenantId);
        stmt.setString(2, "hel3");
        stmt.setInt(3, 3);
        stmt.setString(4, tenantId);
        stmt.setString(5, "hel5");
        stmt.setInt(6, 5);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("hel3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("hel5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        conn.close();
    }
    
    @Test
    public void testRVCWithColumnValuesOfSmallerLengthThanSchema() throws Exception {
        testRVCWithComparisonOps(true);
    }
    
    @Test
    public void testRVCWithColumnValuesEqualToLengthInSchema() throws Exception {
        testRVCWithComparisonOps(false);
    }
    
    private void testRVCWithComparisonOps(boolean columnValueLengthSmaller) throws Exception {
        String tenantId = "ABC";
        String tableDDLFormat = "CREATE TABLE t (tenantId char(%s) NOT NULL, pk2 char(%s) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3))";
        String tableDDL;
        if (columnValueLengthSmaller) {
            tableDDL = String.format(tableDDLFormat, 15, 15);
        } else {
            tableDDL = String.format(tableDDLFormat, 3, 5);
        }
        createTestTable(getUrl(), tableDDL, null, nextTimestamp());

        Connection conn = nextConnection(getUrl());
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo3', 3, 3)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo4', 4, 4)");
        conn.createStatement().executeUpdate("upsert into t (tenantId, pk2, pk3, c1) values ('ABC', 'helo5', 5, 5)");
        conn.commit();
        conn.close();

        conn = nextConnection(getUrl());
        
        // >
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) > (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo3");
        stmt.setInt(3, 3);
        
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo4", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        assertFalse(rs.next());
        
        // >=
        stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) >= (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo4");
        stmt.setInt(3, 4);
        
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo4", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo5", rs.getString(1));
        assertEquals(5, rs.getInt(2));
        assertFalse(rs.next());
        
        // <
        stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) < (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo2");
        stmt.setInt(3, 2);
        
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo1", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
        
        // <=
        stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) <= (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo2");
        stmt.setInt(3, 2);
        rs = stmt.executeQuery(); 
        
        assertTrue(rs.next());
        assertEquals("helo1", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo2", rs.getString(1));
        assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
        
        // =
        stmt = conn.prepareStatement("select pk2, pk3 from t WHERE (tenantId, pk2, pk3) = (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, "helo4");
        stmt.setInt(3, 4);
        
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo4", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
    }

    @Test
    public void testForceSkipScan() throws Exception {
        String tempTableWithCompositePK = "TEMP_TABLE_COMPOSITE_PK";
        Connection conn = nextConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE " + tempTableWithCompositePK
                    + "   (col0 INTEGER NOT NULL, "
                    + "    col1 INTEGER NOT NULL, "
                    + "    col2 INTEGER NOT NULL, "
                    + "    col3 INTEGER "
                    + "   CONSTRAINT pk PRIMARY KEY (col0, col1, col2)) "
                    + "   SALT_BUCKETS=4");
            conn.close();

            conn = nextConnection(getUrl());
            PreparedStatement upsertStmt = conn.prepareStatement(
                    "upsert into " + tempTableWithCompositePK + "(col0, col1, col2, col3) " + "values (?, ?, ?, ?)");
            for (int i = 0; i < 3; i++) {
                upsertStmt.setInt(1, i + 1);
                upsertStmt.setInt(2, i + 2);
                upsertStmt.setInt(3, i + 3);
                upsertStmt.setInt(4, i + 5);
                upsertStmt.execute();
            }
            conn.commit();
            conn.close();

            conn = nextConnection(getUrl());
            String query = "SELECT * FROM " + tempTableWithCompositePK + " WHERE (col0, col1) in ((2, 3), (3, 4), (4, 5))";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 6);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 4);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 7);

            assertFalse(rs.next());

            String plan = "CLIENT PARALLEL 4-WAY SKIP SCAN ON 12 KEYS OVER TEMP_TABLE_COMPOSITE_PK [0,2] - [3,4]\n" +
                          "CLIENT MERGE SORT";
            String explainQuery = "EXPLAIN " + query;
            rs = conn.createStatement().executeQuery(explainQuery);
            assertEquals(query, plan, QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    // query against non-multitenant table. Salted - yes 
    @Test
    public void testComparisonAgainstRVCCombinedWithOrAnd_1() throws Exception {
    	String tableDDL = "CREATE TABLE RVC1 (tenantId char(15) NOT NULL, pk2 char(15) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) SALT_BUCKETS = 4";
        createTestTable(getUrl(), tableDDL, null, nextTimestamp());

        Connection conn = nextConnection(getUrl());
        conn.createStatement().executeUpdate("upsert into RVC1 (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into RVC1 (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into RVC1 (tenantId, pk2, pk3, c1) values ('DEF', 'helo3', 3, 3)");
        conn.commit();
        conn.close();

        conn = nextConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from RVC1 WHERE (tenantId = ? OR tenantId = ?) AND (tenantId, pk2, pk3) > (?, ?, ?) LIMIT 100");
        stmt.setString(1, "ABC");
        stmt.setString(2, "DEF");
        
        // give back all rows after row 1 - ABC|helo1|1
        stmt.setString(3, "ABC");
        stmt.setString(4, "helo1");
        stmt.setInt(5, 1);
        
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo2", rs.getString(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("helo3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertFalse(rs.next());
        
        stmt = conn.prepareStatement("select pk2, pk3 from RVC1 WHERE tenantId = ? AND (tenantId, pk2, pk3) BETWEEN (?, ?, ?) AND (?, ?, ?) LIMIT 100");
        stmt.setString(1, "ABC");
        stmt.setString(2, "ABC");
        stmt.setString(3, "helo2");
        stmt.setInt(4, 2);
        stmt.setString(5, "DEF");
        stmt.setString(6, "helo3");
        stmt.setInt(7, 3);
        
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo2", rs.getString(1));
        assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
    }
    
    // query against tenant specific view. Salted base table.
    @Test
    public void testComparisonAgainstRVCCombinedWithOrAnd_2() throws Exception {
        String tenantId = "ABC";
        String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
        String baseTableDDL = "CREATE TABLE RVC2 (tenant_id char(15) NOT NULL, pk2 char(15) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenant_id,pk2,pk3)) MULTI_TENANT=true, SALT_BUCKETS = 4";
        createTestTable(getUrl(), baseTableDDL, null, nextTimestamp());
        String tenantTableDDL = "CREATE VIEW t_view AS SELECT * FROM RVC2";
        createTestTable(tenantSpecificUrl, tenantTableDDL, null, nextTimestamp());

        Connection conn = nextConnection(tenantSpecificUrl);
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo1', 1, 1)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo2', 2, 2)");
        conn.createStatement().executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo3', 3, 3)");
        conn.commit();
        conn.close();

        conn = nextConnection(tenantSpecificUrl);
        PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from t_view WHERE (pk2 = ? OR pk2 = ?) AND (pk2, pk3) > (?, ?) LIMIT 100");
        stmt.setString(1, "helo1");
        stmt.setString(2, "helo3");
        
        // return rows after helo1|1 
        stmt.setString(3, "helo1");
        stmt.setInt(4, 1);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("helo3", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testRVCWithRowKeyNotLeading() throws Exception {
        String ddl = "CREATE TABLE sorttest4 (rownum BIGINT primary key, name varchar(16), age integer)";
        Connection conn = nextConnection(getUrl());
        conn.createStatement().execute(ddl);
        conn.close();
        conn = nextConnection(getUrl());
        String dml = "UPSERT INTO sorttest4 (rownum, name, age) values (?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setString(2, "A");
        stmt.setInt(3, 1);
        stmt.executeUpdate();
        stmt.setInt(1, 2);
        stmt.setString(2, "B");
        stmt.setInt(3, 2);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        // the below query should only return one record -> (1, "A", 1)
        String query = "SELECT rownum, name, age FROM sorttest4 where (age, rownum) < (2, 2)";
        conn = nextConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery(query);
        int numRecords = 0;
        while (rs.next()) {
            assertEquals(1, rs.getInt(1));
            assertEquals("A", rs.getString(2));
            assertEquals(1, rs.getInt(3));
            numRecords++;
        }
        assertEquals(1, numRecords);
    }

    @Test
    public void testRVCInView() throws Exception {
        Connection conn = nextConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE TEST_TABLE.TEST1 (\n" + 
                "PK1 CHAR(3) NOT NULL, \n" + 
                "PK2 CHAR(3) NOT NULL,\n" + 
                "DATA1 CHAR(10)\n" + 
                "CONSTRAINT PK PRIMARY KEY (PK1, PK2))");
        conn.close();
        conn = nextConnection(getUrl());
        conn.createStatement().execute("CREATE VIEW TEST_TABLE.FOO AS SELECT * FROM TEST_TABLE.TEST1 WHERE PK1 = 'FOO'");
        conn.close();
        conn = nextConnection(getUrl());
        conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','001','SOMEDATA')");
        conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','002','SOMEDATA')");
        conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','003','SOMEDATA')");
        conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','004','SOMEDATA')");
        conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','005','SOMEDATA')");
        conn.commit();
        conn.close();
        
        conn = nextConnection(getUrl());        
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST_TABLE.FOO WHERE PK2 < '004' AND (PK1,PK2) > ('FOO','002') LIMIT 2");
        assertTrue(rs.next());
        assertEquals("003", rs.getString("PK2"));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testCountDistinct1() throws Exception {
        Connection conn = nextConnection(getUrl());
        String tableName = "testCountDistinct1rvc";
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY, a INTEGER, b INTEGER)";
        conn.createStatement().execute(ddl);
        
        conn = nextConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', 6,3)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', 2,4)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', 6,3)");
        stmt.execute();
        conn.commit();

        conn = nextConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from " + tableName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        conn.close();
    }

    @Test
    public void testCountDistinct2() throws Exception {
        Connection conn = nextConnection(getUrl());
        String tableName = "testCountDistinct2rvc";
        String ddl = "CREATE TABLE  " + tableName + "  (region_name VARCHAR PRIMARY KEY, a VARCHAR, b VARCHAR)";
        conn.createStatement().execute(ddl);
        conn.commit();
        
        conn = nextConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', 'fooo','abc')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', 'off','bac')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', 'fooo', 'abc')");
        stmt.execute();
        conn.commit();

        conn = nextConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from  " + tableName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

    @Test
    public void testCountDistinct3() throws Exception {
        Connection conn = nextConnection(getUrl());
        String tableName = "testCountDistinct3rvc";
        String ddl = "CREATE TABLE  " + tableName + "  (region_name VARCHAR PRIMARY KEY, a Boolean, b Boolean)";
        conn.createStatement().execute(ddl);
        
        conn = nextConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', true, true)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', true, False)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', true, true)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('d', true, false)");
        stmt.execute();
        conn.commit();

        conn = nextConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from  " + tableName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

    @Test
    public void testCountDistinct4() throws Exception {
        Connection conn = nextConnection(getUrl());
        String tableName = "testCountDistinct4rvc";
        String ddl = "CREATE TABLE  " + tableName + "  (region_name VARCHAR PRIMARY KEY, a VARCHAR, b VARCHAR)";
        conn.createStatement().execute(ddl);
        
        conn = nextConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', 'fooo','abc')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', 'off','bac')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', 'foo', 'abc')");
        stmt.execute();
        conn.commit();
        
        conn = nextConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from  " + tableName);
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
    }

    @Test
    public void testRVCRequiringExtractNodeClear() throws Exception {
        Connection conn = nextConnection(getUrl());
        String tableName = "testRVCWithTrailingGT";
        String ddl = "CREATE TABLE  " + tableName + "  (k1 VARCHAR, k2 VARCHAR, k3 VARCHAR, k4 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2,k3,k4))";
        conn.createStatement().execute(ddl);
        
        conn = nextConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('a','b','c','d')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('b', 'b', 'c', 'e')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('c', 'b','c','f')");
        stmt.execute();
        conn.commit();
        
        conn = nextConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT k1 from  " + tableName + " WHERE k1 IN ('a','c') AND (k2,k3) IN (('b','c'),('f','g')) AND k4 > 'c'");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testRVCRequiringNoSkipScan() throws Exception {
        Connection conn = nextConnection(getUrl());
        String tableName = "testRVCWithTrailingGT";
        String ddl = "CREATE TABLE  " + tableName + "  (k1 VARCHAR, k2 VARCHAR, k3 VARCHAR, k4 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2,k3,k4))";
        conn.createStatement().execute(ddl);
        
        conn = nextConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('','','a')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('', '', 'a', 'a')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('', '','b')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('', '','b','a')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('a', '','c')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('a', '','c', 'a')");
        stmt.execute();
        conn.commit();
        
        conn = nextConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT k1,k3,k4 from  " + tableName + " WHERE (k1,k2,k3) IN (('','','a'),('','','b'),('a','','c')) AND k4 is not null");
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("a", rs.getString(2));
        assertEquals("a", rs.getString(3));
        
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("b", rs.getString(2));
        assertEquals("a", rs.getString(3));
        
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("c", rs.getString(2));
        assertEquals("a", rs.getString(3));
        
        assertFalse(rs.next());
    }

	/**
	 * PHOENIX-2327
	 * 
	 * Table's pks are (pk1, pk2, ... , pkn), n >= 3
	 * Index's pks are (pk2, ... , pkn, pk1), n >= 3
	 * RVC is (pk2, ... , pkn, pk1), n >= 3
	 * 
	 * Expalin select * from t where (pk2, ... , pkn, pk1) > ('201', ..., 'n01', '101') and pk[2-n] = '[2-n]03'
	 * 
	 * You will Get "DEGENERATE SCAN OVER TABLE_NAME"
	 * 
	 * @throws java.lang.Exception
	 */
	@Test
	public void testRVCLastPkIsTable1stPkIndex() throws Exception {
		Connection conn = nextConnection(getUrl());
		String tableName = "t";
		String ddl = "CREATE TABLE " + tableName 
				+ " (k1 VARCHAR, k2 VARCHAR, k3 VARCHAR, k4 VARCHAR,"
				+ " CONSTRAINT pk PRIMARY KEY (k1,k2,k3,k4))";
		conn.createStatement().execute(ddl);

		conn = nextConnection(getUrl());
		ddl = "CREATE INDEX  " + tableName + "_idx"
				+ " ON " + tableName + " (k2, k3, k4, k1)";
		conn.createStatement().execute(ddl);
		
		conn = nextConnection(getUrl());
		String upsert = "UPSERT INTO " + tableName + " VALUES(?, ?, ?, ?)";
		PreparedStatement stmt = conn.prepareStatement(upsert);
		for (int i = 0; i < 5; i++) {
			stmt.setString(1, "10" + i);
			stmt.setString(2, "20" + i);
			stmt.setString(3, "30" + i);
			stmt.setString(4, "40" + i);
			stmt.execute();
		}
		conn.commit();

		conn = nextConnection(getUrl());
		String query = "SELECT k1, k2, k3, k4 FROM " + tableName + " WHERE k2 = '203'";
		ResultSet rs = conn.createStatement().executeQuery(query);
		assertTrue(rs.next());
		assertEquals("103", rs.getString(1));
		assertEquals("203", rs.getString(2));
		assertEquals("303", rs.getString(3));
		assertEquals("403", rs.getString(4));

		conn = nextConnection(getUrl());
		query = "SELECT k1, k2, k3, k4 FROM " + tableName 
				+ " WHERE (k2, k3, k4, k1) > ('201', '301', '401', '101')"
				+ " AND k2 = '203'";
		rs = conn.createStatement().executeQuery(query);
		assertTrue(rs.next());
		assertEquals("103", rs.getString(1));
		assertEquals("203", rs.getString(2));
		assertEquals("303", rs.getString(3));
		assertEquals("403", rs.getString(4));
	}

    @Test
    public void testMultiTenantRVC() throws Exception {
        Connection conn = nextConnection(getUrl());
        String tableName = "mtRVC";
        String ddl = "CREATE TABLE " + tableName 
                + " (\n" + 
                "    pk1 VARCHAR NOT NULL,\n" + 
                "    pk2 DECIMAL NOT NULL,\n" + 
                "    v1 VARCHAR\n" + 
                "    CONSTRAINT PK PRIMARY KEY \n" + 
                "    (\n" + 
                "        pk1,\n" + 
                "        pk2\n" + 
                "    )\n" + 
                ") MULTI_TENANT=true,IMMUTABLE_ROWS=true";
        conn.createStatement().execute(ddl);

        conn = nextConnection(getUrl());
        ddl = "CREATE INDEX  " + tableName + "_idx"
                + " ON " + tableName + " (v1)";
        conn.createStatement().execute(ddl);
        
        conn = nextConnection(getUrl());
        String upsert = "UPSERT INTO " + tableName + " VALUES(?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        stmt.setString(1, "a");
        stmt.setInt(2, 1);
        stmt.setString(3, "value");
        stmt.execute();
        stmt.setString(1, "a");
        stmt.setInt(2, 2);
        stmt.setString(3, "value");
        stmt.execute();
        conn.commit();

        conn = nextConnection(getUrl());
        String query = "SELECT pk1, pk2, v1 FROM " + tableName + " WHERE pk1 = 'a' AND\n" + 
                "(pk1, pk2) > ('a', 1)\n" + 
                "ORDER BY PK1, PK2\n" + 
                "LIMIT 2";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertTrue(new BigDecimal("2").compareTo(rs.getBigDecimal(2)) == 0);
        assertEquals("value", rs.getString(3));
        assertFalse(rs.next());
    }
}
