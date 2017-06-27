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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;
import java.util.Random;

import static org.apache.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;


public class CursorWithRowValueConstructorIT extends ParallelStatsDisabledIT {
    private static final String TABLE_NAME = "CursorRVCTestTable";
    protected static final Log LOG = LogFactory.getLog(CursorWithRowValueConstructorIT.class);

    public void createAndInitializeTestTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
                "(a_id INTEGER NOT NULL, " +
                "a_data INTEGER, " +
                "CONSTRAINT my_pk PRIMARY KEY (a_id))");
        stmt.execute();
        synchronized (conn){
            conn.commit();
        }

        //Upsert test values into the test table
        Random rand = new Random();
        stmt = conn.prepareStatement("UPSERT INTO " + TABLE_NAME +
                "(a_id, a_data) VALUES (?,?)");
        int rowCount = 0;
        while(rowCount < 100){
            stmt.setInt(1, rowCount);
            stmt.setInt(2, rand.nextInt(501));
            stmt.execute();
            ++rowCount;
        }
        synchronized (conn){
            conn.commit();
        }
    }

    public void deleteTestTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS " + TABLE_NAME);
        stmt.execute();
        synchronized (conn){
            conn.commit();
        }
    }

    @Test
    public void testCursorsOnTestTablePK() throws SQLException {
        try{
            createAndInitializeTestTable();
            String querySQL = "SELECT a_id FROM " + TABLE_NAME;

            //Test actual cursor implementation
            String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN testCursor";
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM testCursor";
            ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            int rowID = 0;
            while(rs.next()){
                assertEquals(rowID,rs.getInt(1));
                ++rowID;
                rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
            }
        } finally{
            DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
            deleteTestTable();
        }

    }

    @Test
    public void testCursorsOnRandomTableData() throws SQLException {
        try{
            createAndInitializeTestTable();
            String querySQL = "SELECT a_id,a_data FROM " + TABLE_NAME + " ORDER BY a_data";
            String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN testCursor";
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM testCursor";
            ResultSet cursorRS = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(querySQL).executeQuery();
            int rowCount = 0;
            while(rs.next() && cursorRS.next()){
                assertEquals(rs.getInt(2),cursorRS.getInt(2));
                ++rowCount;
                cursorRS = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            }
            assertEquals(100, rowCount);
        } finally{
            DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
            deleteTestTable();
        }
    }

    @Test
    public void testCursorsOnTestTablePKDesc() throws SQLException {
        try{
            createAndInitializeTestTable();
            String dummySQL = "SELECT a_id FROM " + TABLE_NAME + " ORDER BY a_id DESC";

            String cursorSQL = "DECLARE testCursor CURSOR FOR " + dummySQL;
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN testCursor";
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM testCursor";
            ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            int rowCount = 0;
            while(rs.next()){
                assertEquals(99-rowCount, rs.getInt(1));
                rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
                ++rowCount;
            }
            assertEquals(100, rowCount);
        } finally{
            DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
            deleteTestTable();
        }
    }

    @Test
    public void testCursorsOnTestTableNonPKDesc() throws SQLException {
        try{
            createAndInitializeTestTable();
            String dummySQL = "SELECT a_data FROM " + TABLE_NAME + " ORDER BY a_data DESC";

            String cursorSQL = "DECLARE testCursor CURSOR FOR " + dummySQL;
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN testCursor";
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM testCursor";
            ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            int rowCount = 0;
            while(rs.next()){
                rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
                ++rowCount;
            }
            assertEquals(100, rowCount);
        } finally{
            DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
            deleteTestTable();
        }
    }

    @Test
    public void testCursorsOnWildcardSelect() throws SQLException {
        try{
            createAndInitializeTestTable();
            String querySQL = "SELECT * FROM " + TABLE_NAME;
            ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(querySQL).executeQuery();

            String cursorSQL = "DECLARE testCursor CURSOR FOR "+querySQL;
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN testCursor";
            DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM testCursor";
            ResultSet cursorRS = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            int rowCount = 0;
            while(rs.next() && cursorRS.next()){
                assertEquals(rs.getInt(1),cursorRS.getInt(1));
                ++rowCount;
                cursorRS = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            }
            assertEquals(100, rowCount);
        } finally{
            DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
            deleteTestTable();
        }
    }

    @Test
    public void testCursorsWithBindings() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ?=organization_id AND (a_integer, x_integer) = (7, 5)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cursor = "DECLARE testCursor CURSOR FOR "+query;
        try {
            PreparedStatement statement = conn.prepareStatement(cursor);
            statement.setString(1, tenantId);
            statement.execute();
        }catch(SQLException e){
            assertTrue(e.getMessage().equalsIgnoreCase("Cannot declare cursor, internal SELECT statement contains bindings!"));
            assertTrue(!CursorUtil.cursorDeclared("testCursor"));
            return;
        } finally {
            cursor = "CLOSE testCursor";
            conn.prepareStatement(cursor).execute();
            conn.close();
        }
        fail();
    }

    @Test
    public void testCursorsInWhereWithEqualsExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE '"+tenantId+"'=organization_id AND (a_integer, x_integer) = (7, 5)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cursor = "DECLARE testCursor CURSOR FOR "+query;
        try {
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";
            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) == 7);
                assertTrue(rs.getInt(2) == 5);
                count++;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            assertTrue(count == 1);
        } finally {
            cursor = "CLOSE testCursor";
            conn.prepareStatement(cursor).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsInWhereWithGreaterThanExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer) >= (4, 4)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cursor = "DECLARE testCursor CURSOR FOR "+query;
        try {
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";
            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) >= 4);
                assertTrue(rs.getInt(1) == 4 ? rs.getInt(2) >= 4 : rs.getInt(2) >= 0);
                count++;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            assertTrue(count == 5);
        } finally {
            cursor = "CLOSE testCursor";
            conn.prepareStatement(cursor).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsInWhereWithUnEqualNumberArgs() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer, y_integer) >= (7, 5)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cursor = "DECLARE testCursor CURSOR FOR "+query;
        try {
            double startTime = System.nanoTime();
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";
            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getInt(1) >= 7);
                assertTrue(rs.getInt(1) == 7 ? rs.getInt(2) >= 5 : rs.getInt(2) >= 0);
                count++;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3);
            double endTime = System.nanoTime();
            System.out.println("Method Time in milliseconds: "+Double.toString((endTime-startTime)/1000000));
        } finally {
            cursor = "CLOSE testCursor";
            conn.prepareStatement(cursor).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsOnLHSAndLiteralExpressionOnRHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer) >= 7";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cursor = "DECLARE testCursor CURSOR FOR "+query;
        try {
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";
            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3);
        } finally {
            cursor = "CLOSE testCursor";
            conn.prepareStatement(cursor).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsOnRHSLiteralExpressionOnLHS() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE '"+tenantId+"'=organization_id  AND 7 <= (a_integer, x_integer)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cursor = "DECLARE testCursor CURSOR FOR "+query;
        try {
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";
            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertTrue(count == 3);
        } finally {
            cursor = "CLOSE testCursor";
            conn.prepareStatement(cursor).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsOnBuiltInFunctionOperatingOnIntegerLiteral() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_integer, x_integer FROM aTable WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer) >= to_number('7')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cursor = "DECLARE testCursor CURSOR FOR "+query;
        try {
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";
            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
            assertEquals(3, count);
        } finally {
            cursor = "CLOSE testCursor";
            conn.prepareStatement(cursor).execute();
            conn.close();
        }
    }

    @Test
    /**
     * Test for the precision of Date datatype when used as part of a filter within the internal Select statement.
     */
    public void testCursorsWithDateDatatypeFilter() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        long currentTime = System.currentTimeMillis();
        java.sql.Date date = new java.sql.Date(currentTime);
        String strCurrentDate = date.toString();

        //Sets date to <yesterday's date> 23:59:59.999
        while(date.toString().equals(strCurrentDate)){
            currentTime -= 1;
            date = new Date(currentTime);
        }
        //Sets date to <today's date> 00:00:00.001
        date = new Date(currentTime+2);
        java.sql.Date midnight = new Date(currentTime+1);


        initEntityHistoryTableValues(tenantId, getDefaultSplits(tenantId), date, ts);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(getUrl(), props);


        String query = "select parent_id from " + ENTITY_HISTORY_TABLE_NAME +
                " WHERE (organization_id, parent_id, created_date, entity_history_id) IN ((?,?,?,?),(?,?,?,?))";

        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID3+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(date)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID3+"'");
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID7+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(date)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID7+"'");
        String cursor = "DECLARE testCursor CURSOR FOR "+query;

        conn.prepareStatement(cursor).execute();
        cursor = "OPEN testCursor";
        conn.prepareStatement(cursor).execute();
        cursor = "FETCH NEXT FROM testCursor";

        ResultSet rs = conn.prepareStatement(cursor).executeQuery();
        assertTrue(rs.next());
        assertEquals(PARENTID3, rs.getString(1));
        rs = conn.prepareStatement(cursor).executeQuery();
        assertTrue(rs.next());
        assertEquals(PARENTID7, rs.getString(1));
        assertFalse(rs.next());

        //Test against the same table for the same records, but this time use the 'midnight' java.sql.Date instance.
        //'midnight' is identical to 'date' to the tens of millisecond precision.
        query = "select parent_id from " + ENTITY_HISTORY_TABLE_NAME +
                " WHERE (organization_id, parent_id, created_date, entity_history_id) IN ((?,?,?,?),(?,?,?,?))";
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID3+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(midnight)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID3+"'");
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID7+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(midnight)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID7+"'");
        cursor = "DECLARE testCursor2 CURSOR FOR "+query;

        conn.prepareStatement(cursor).execute();
        cursor = "OPEN testCursor2";
        conn.prepareStatement(cursor).execute();
        cursor = "FETCH NEXT FROM testCursor2";

        rs = conn.prepareStatement(cursor).executeQuery();
        assertTrue(!rs.next());
        String sql = "CLOSE testCursor";
        conn.prepareStatement(sql).execute();
        sql = "CLOSE testCursor2";
        conn.prepareStatement(sql).execute();
    }

    @Test
    public void testCursorsWithNonLeadingPkColsOfTypesTimeStampAndVarchar() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
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
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_TIMESTAMP_FORMAT).format(tsValue)+"')");

        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String cursor = "DECLARE testCursor CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";

            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(rs.getTimestamp(1).equals(tsValue));
                assertTrue(rs.getString(2).compareTo("a") == 0);
                count++;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            assertTrue(count == 1);
        } finally {
            String sql = "CLOSE testCursor";
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsQueryMoreWithInListClausePossibleNullValues() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String updateStmt =
                "upsert into " +
                        "ATABLE(ORGANIZATION_ID, ENTITY_ID, Y_INTEGER, X_INTEGER) VALUES (?, ?, ?, ?)";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true);
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 4);
        stmt.setInt(4, 5);
        stmt.execute();

        //we have a row present in aTable where x_integer = 5 and y_integer = NULL which gets translated to 0 when retriving from HBase.
        String query = "SELECT x_integer, y_integer FROM aTable WHERE ? = organization_id AND (x_integer) IN ((5))";

        query = query.replaceFirst("\\?", "'"+tenantId+"'");

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            String cursor = "DECLARE testCursor CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";

            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertEquals(4, rs.getInt(2));
            rs = conn.prepareStatement(cursor).executeQuery();
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
        } finally {
            String sql = "CLOSE testCursor";
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsWithColsOfTypesDecimal() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        String query = "SELECT x_decimal FROM aTable WHERE ?=organization_id AND entity_id IN (?,?,?)";
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+ROW7+"'");
        query = query.replaceFirst("\\?", "'"+ROW8+"'");
        query = query.replaceFirst("\\?", "'"+ROW9+"'");

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String cursor = "DECLARE testCursor CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";

            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue(BigDecimal.valueOf(0.1).equals(rs.getBigDecimal(1)) || BigDecimal.valueOf(3.9).equals(rs.getBigDecimal(1)) || BigDecimal.valueOf(3.3).equals(rs.getBigDecimal(1)));
                count++;
                if(count == 3) break;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            assertTrue(count == 3);
        } finally {
            String sql = "CLOSE testCursor";
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsWithColsOfTypesTinyintSmallintFloatDouble() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(ATABLE_NAME, tenantId, getDefaultSplits(tenantId), null, ts-1, getUrl(), null);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String query = "SELECT a_byte,a_short,a_float,a_double FROM aTable WHERE ?=organization_id AND entity_id IN (?,?,?)";
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+ROW1+"'");
        query = query.replaceFirst("\\?", "'"+ROW2+"'");
        query = query.replaceFirst("\\?", "'"+ROW3+"'");

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String cursor = "DECLARE testCursor CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN testCursor";
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM testCursor";

            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            int count = 0;
            while(rs.next()) {
                assertTrue((byte)1 == (rs.getByte(1)) || (byte)2 == (rs.getByte(1)) || (byte)3 == (rs.getByte(1)));
                assertTrue((short)128 == (rs.getShort(2)) || (short)129 == (rs.getShort(2)) || (short)130 == (rs.getShort(2)));
                assertTrue(0.01f == (rs.getFloat(3)) || 0.02f == (rs.getFloat(3)) || 0.03f == (rs.getFloat(3)));
                assertTrue(0.0001 == (rs.getDouble(4)) || 0.0002 == (rs.getDouble(4)) || 0.0003 == (rs.getDouble(4)));
                count++;
                if(count == 3) break;
                rs = conn.prepareStatement(cursor).executeQuery();
            }
            assertTrue(count == 3);
        } finally {
            String sql = "CLOSE testCursor";
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }
}
