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

import static org.apache.phoenix.util.TestUtil.ENTITYHISTID3;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID7;
import static org.apache.phoenix.util.TestUtil.PARENTID3;
import static org.apache.phoenix.util.TestUtil.PARENTID7;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.phoenix.util.CursorUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class CursorWithRowValueConstructorIT extends ParallelStatsDisabledIT {
    private String tableName = generateUniqueName();

    public void createAndInitializeTestTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        tableName = generateUniqueName();
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE " + tableName +
                "(a_id INTEGER NOT NULL, " +
                "a_data INTEGER, " +
                "CONSTRAINT my_pk PRIMARY KEY (a_id))");
        stmt.execute();
        conn.commit();

        //Upsert test values into the test table
        Random rand = new Random();
        stmt = conn.prepareStatement("UPSERT INTO " + tableName +
                "(a_id, a_data) VALUES (?,?)");
        int rowCount = 0;
        while(rowCount < 100){
            stmt.setInt(1, rowCount);
            stmt.setInt(2, rand.nextInt(501));
            stmt.execute();
            ++rowCount;
        }
        conn.commit();
        conn.close();
    }

    @Test
    public void testCursorsOnTestTablePK() throws SQLException {
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createAndInitializeTestTable();
            String querySQL = "SELECT a_id FROM " + tableName;

            //Test actual cursor implementation
            String cursorSQL = "DECLARE " + cursorName + " CURSOR FOR " + querySQL;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN " + cursorName;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM " + cursorName;
            ResultSet rs = conn.prepareStatement(cursorSQL).executeQuery();
            int rowID = 0;
            while(rs.next()){
                assertEquals(rowID,rs.getInt(1));
                ++rowID;
                rs = conn.createStatement().executeQuery(cursorSQL);
            }
            conn.prepareStatement("CLOSE " + cursorName).execute();
        }

    }

    @Test
    public void testCursorsOnRandomTableData() throws SQLException {
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createAndInitializeTestTable();
            String querySQL = "SELECT a_id,a_data FROM " + tableName + " ORDER BY a_data";
            String cursorSQL = "DECLARE " + cursorName + " CURSOR FOR " + querySQL;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN " + cursorName;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM " + cursorName;
            ResultSet cursorRS = conn.prepareStatement(cursorSQL).executeQuery();
            ResultSet rs = conn.prepareStatement(querySQL).executeQuery();
            int rowCount = 0;
            while(rs.next() && cursorRS.next()){
                assertEquals(rs.getInt(2),cursorRS.getInt(2));
                ++rowCount;
                cursorRS = conn.prepareStatement(cursorSQL).executeQuery();
            }
            assertEquals(100, rowCount);
            conn.prepareStatement("CLOSE " + cursorName).execute();
        }
    }

    @Test
    public void testCursorsOnTestTablePKDesc() throws SQLException {
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createAndInitializeTestTable();
            String dummySQL = "SELECT a_id FROM " + tableName + " ORDER BY a_id DESC";

            String cursorSQL = "DECLARE " + cursorName + " CURSOR FOR " + dummySQL;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN " + cursorName;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM " + cursorName;
            ResultSet rs = conn.prepareStatement(cursorSQL).executeQuery();
            int rowCount = 0;
            while(rs.next()){
                assertEquals(99-rowCount, rs.getInt(1));
                rs = conn.prepareStatement(cursorSQL).executeQuery();
                ++rowCount;
            }
            assertEquals(100, rowCount);
            conn.prepareStatement("CLOSE " + cursorName).execute();
        }
    }

    @Test
    public void testCursorsOnTestTableNonPKDesc() throws SQLException {
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createAndInitializeTestTable();
            String dummySQL = "SELECT a_data FROM " + tableName + " ORDER BY a_data DESC";

            String cursorSQL = "DECLARE " + cursorName + " CURSOR FOR " + dummySQL;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN " + cursorName;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM " + cursorName;
            ResultSet rs = conn.prepareStatement(cursorSQL).executeQuery();
            int rowCount = 0;
            while(rs.next()){
                rs = conn.prepareStatement(cursorSQL).executeQuery();
                ++rowCount;
            }
            assertEquals(100, rowCount);
            conn.prepareStatement("CLOSE " + cursorName).execute();
        }
    }

    @Test
    public void testCursorsOnWildcardSelect() throws SQLException {
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createAndInitializeTestTable();
            String querySQL = "SELECT * FROM " + tableName;
            ResultSet rs = conn.prepareStatement(querySQL).executeQuery();

            String cursorSQL = "DECLARE " + cursorName + " CURSOR FOR "+querySQL;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "OPEN " + cursorName;
            conn.prepareStatement(cursorSQL).execute();
            cursorSQL = "FETCH NEXT FROM " + cursorName;
            ResultSet cursorRS = conn.prepareStatement(cursorSQL).executeQuery();
            int rowCount = 0;
            while(rs.next() && cursorRS.next()){
                assertEquals(rs.getInt(1),cursorRS.getInt(1));
                ++rowCount;
                cursorRS = conn.prepareStatement(cursorSQL).executeQuery();
            }
            assertEquals(100, rowCount);
            conn.prepareStatement("CLOSE " + cursorName).execute();
        }
    }

    @Test
    public void testCursorsWithBindings() throws Exception {
        String tenantId = getOrganizationId();
        String cursorName = generateUniqueName();
        final String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM "+ aTable +" WHERE ?=organization_id AND (a_integer, x_integer) = (7, 5)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            try {
                PreparedStatement statement = conn.prepareStatement(cursor);
                statement.setString(1, tenantId);
                statement.execute();
            }catch(SQLException e){
                assertTrue(e.getMessage().equalsIgnoreCase("Cannot declare cursor, internal SELECT statement contains bindings!"));
                assertFalse(CursorUtil.cursorDeclared(cursorName));
                return;
            } finally {
                cursor = "CLOSE " + cursorName;
                conn.prepareStatement(cursor).execute();
            }
            fail();
        }
    }

    @Test
    public void testCursorsInWhereWithEqualsExpression() throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM "+aTable+" WHERE '"+tenantId+"'=organization_id AND (a_integer, x_integer) = (7, 5)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            try {
                conn.prepareStatement(cursor).execute();
                cursor = "OPEN " + cursorName;
                conn.prepareStatement(cursor).execute();
                cursor = "FETCH NEXT FROM " + cursorName;
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
                cursor = "CLOSE " + cursorName;
                conn.prepareStatement(cursor).execute();
            }
        }
    }

    @Test
    public void testCursorsInWhereWithGreaterThanExpression() throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM "+aTable+" WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer) >= (4, 4)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            try {
                conn.prepareStatement(cursor).execute();
                cursor = "OPEN " + cursorName;
                conn.prepareStatement(cursor).execute();
                cursor = "FETCH NEXT FROM " + cursorName;
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
                cursor = "CLOSE " + cursorName;
                conn.prepareStatement(cursor).execute();
            }
        }
    }

    @Test
    public void testCursorsInWhereWithUnEqualNumberArgs() throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM "+ aTable+" WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer, y_integer) >= (7, 5)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            try {
                double startTime = System.nanoTime();
                conn.prepareStatement(cursor).execute();
                cursor = "OPEN " + cursorName;
                conn.prepareStatement(cursor).execute();
                cursor = "FETCH NEXT FROM " + cursorName;
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
                cursor = "CLOSE " + cursorName;
                conn.prepareStatement(cursor).execute();
            }
        }
    }

    @Test
    public void testCursorsOnLHSAndLiteralExpressionOnRHS() throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM "+ aTable +" WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer) >= 7";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            try {
                conn.prepareStatement(cursor).execute();
                cursor = "OPEN " + cursorName;
                conn.prepareStatement(cursor).execute();
                cursor = "FETCH NEXT FROM " + cursorName;
                ResultSet rs = conn.prepareStatement(cursor).executeQuery();
                int count = 0;
                while(rs.next()) {
                    count++;
                    rs = conn.prepareStatement(cursor).executeQuery();
                }
                // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
                assertTrue(count == 3);
            } finally {
                cursor = "CLOSE " + cursorName;
                conn.prepareStatement(cursor).execute();
            }
        }
    }

    @Test
    public void testCursorsOnRHSLiteralExpressionOnLHS() throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM "+ aTable+" WHERE '"+tenantId+"'=organization_id  AND 7 <= (a_integer, x_integer)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            try {
                conn.prepareStatement(cursor).execute();
                cursor = "OPEN " + cursorName;
                conn.prepareStatement(cursor).execute();
                cursor = "FETCH NEXT FROM " + cursorName;
                ResultSet rs = conn.prepareStatement(cursor).executeQuery();
                int count = 0;
                while(rs.next()) {
                    count++;
                    rs = conn.prepareStatement(cursor).executeQuery();
                }
                // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
                assertTrue(count == 3);
            } finally {
                cursor = "CLOSE " + cursorName;
                conn.prepareStatement(cursor).execute();
            }
        }
    }

    @Test
    public void testCursorsOnBuiltInFunctionOperatingOnIntegerLiteral() throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_integer, x_integer FROM "+aTable+" WHERE '"+tenantId+"'=organization_id  AND (a_integer, x_integer) >= to_number('7')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String cursorName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            try {
                conn.prepareStatement(cursor).execute();
                cursor = "OPEN " + cursorName;
                conn.prepareStatement(cursor).execute();
                cursor = "FETCH NEXT FROM " + cursorName;
                ResultSet rs = conn.prepareStatement(cursor).executeQuery();
                int count = 0;
                while(rs.next()) {
                    count++;
                    rs = conn.prepareStatement(cursor).executeQuery();
                }
                // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the 3 records.
                assertEquals(3, count);
            } finally {
                cursor = "CLOSE " + cursorName;
                conn.prepareStatement(cursor).execute();
            }
        }
    }

    @Test
    /**
     * Test for the precision of Date datatype when used as part of a filter within the internal Select statement.
     */
    public void testCursorsWithDateDatatypeFilter() throws Exception {
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


        String tableName = initEntityHistoryTableValues(null, tenantId, getDefaultSplits(tenantId), date, null);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);


        String query = "select parent_id from " + tableName +
                " WHERE (organization_id, parent_id, created_date, entity_history_id) IN ((?,?,?,?),(?,?,?,?))";

        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID3+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(date)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID3+"'");
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID7+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(date)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID7+"'");
        String cursorName = generateUniqueName();
        String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;

        conn.prepareStatement(cursor).execute();
        cursor = "OPEN " + cursorName;
        conn.prepareStatement(cursor).execute();
        cursor = "FETCH NEXT FROM " + cursorName;

        ResultSet rs = conn.prepareStatement(cursor).executeQuery();
        assertTrue(rs.next());
        assertEquals(PARENTID3, rs.getString(1));
        rs = conn.prepareStatement(cursor).executeQuery();
        assertTrue(rs.next());
        assertEquals(PARENTID7, rs.getString(1));
        assertFalse(rs.next());

        //Test against the same table for the same records, but this time use the 'midnight' java.sql.Date instance.
        //'midnight' is identical to 'date' to the tens of millisecond precision.
        query = "select parent_id from " + tableName +
                " WHERE (organization_id, parent_id, created_date, entity_history_id) IN ((?,?,?,?),(?,?,?,?))";
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID3+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(midnight)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID3+"'");
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+PARENTID7+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT).format(midnight)+"')");
        query = query.replaceFirst("\\?", "'"+ENTITYHISTID7+"'");
        String cursorName2 = generateUniqueName();
        cursor = "DECLARE " + cursorName2 + " CURSOR FOR "+query;

        conn.prepareStatement(cursor).execute();
        cursor = "OPEN " + cursorName2;
        conn.prepareStatement(cursor).execute();
        cursor = "FETCH NEXT FROM " + cursorName2;

        rs = conn.prepareStatement(cursor).executeQuery();
        assertTrue(!rs.next());
        String sql = "CLOSE " + cursorName;
        conn.prepareStatement(sql).execute();
        sql = "CLOSE " + cursorName2;
        conn.prepareStatement(sql).execute();
    }

    @Test
    public void testCursorsWithNonLeadingPkColsOfTypesTimeStampAndVarchar() throws Exception {
        String tenantId = getOrganizationId();
        String cursorName = generateUniqueName();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String updateStmt =
                "upsert into " + aTable+
                        "(" +
                        "    ORGANIZATION_ID, " +
                        "    ENTITY_ID, " +
                        "    A_TIMESTAMP) " +
                        "VALUES (?, ?, ?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true);
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        Timestamp tsValue = new Timestamp(System.nanoTime());
        stmt.setTimestamp(3, tsValue);
        stmt.execute();

        String query = "SELECT a_timestamp, a_string FROM "+aTable+" WHERE ?=organization_id  AND (a_timestamp, a_string) = (?, 'a')";
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "TO_DATE('"+DateUtil.getDateFormatter(DateUtil.DEFAULT_TIMESTAMP_FORMAT).format(tsValue)+"')");

        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN " + cursorName;
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM " + cursorName;

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
            String sql = "CLOSE " + cursorName;
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsQueryMoreWithInListClausePossibleNullValues() throws Exception {
        String tenantId = getOrganizationId();
        String cursorName = generateUniqueName();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String updateStmt =
                "upsert into " +aTable+
                        "(ORGANIZATION_ID, ENTITY_ID, Y_INTEGER, X_INTEGER) VALUES (?, ?, ?, ?)";
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
        String query = "SELECT x_integer, y_integer FROM "+ aTable+" WHERE ? = organization_id AND (x_integer) IN ((5))";

        query = query.replaceFirst("\\?", "'"+tenantId+"'");

        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN " + cursorName;
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM " + cursorName;

            ResultSet rs = conn.prepareStatement(cursor).executeQuery();
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertEquals(4, rs.getInt(2));
            rs = conn.prepareStatement(cursor).executeQuery();
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
        } finally {
            String sql = "CLOSE " + cursorName;
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsWithColsOfTypesDecimal() throws Exception {
        String cursorName = generateUniqueName();
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        String query = "SELECT x_decimal FROM "+ aTable+" WHERE ?=organization_id AND entity_id IN (?,?,?)";
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+ROW7+"'");
        query = query.replaceFirst("\\?", "'"+ROW8+"'");
        query = query.replaceFirst("\\?", "'"+ROW9+"'");

        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN " + cursorName;
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM " + cursorName;

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
            String sql = "CLOSE " + cursorName;
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorsWithColsOfTypesTinyintSmallintFloatDouble() throws Exception {
        String cursorName = generateUniqueName();
        String tenantId = getOrganizationId();
        String aTable = initATableValues(null, tenantId,
            getDefaultSplits(tenantId), null, null, getUrl(), null);
        String query = "SELECT a_byte,a_short,a_float,a_double FROM "+ aTable+" WHERE ?=organization_id AND entity_id IN (?,?,?)";
        query = query.replaceFirst("\\?", "'"+tenantId+"'");
        query = query.replaceFirst("\\?", "'"+ROW1+"'");
        query = query.replaceFirst("\\?", "'"+ROW2+"'");
        query = query.replaceFirst("\\?", "'"+ROW3+"'");

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String cursor = "DECLARE " + cursorName + " CURSOR FOR "+query;
            conn.prepareStatement(cursor).execute();
            cursor = "OPEN " + cursorName;
            conn.prepareStatement(cursor).execute();
            cursor = "FETCH NEXT FROM " + cursorName;

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
            String sql = "CLOSE " + cursorName;
            conn.prepareStatement(sql).execute();
            conn.close();
        }
    }

    @Test
    public void testCursorWithIndex() throws Exception {
        String cursorName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        Statement stmt = conn.createStatement();

        String createTable = "CREATE TABLE IF NOT EXISTS " + tableName +"\n" +
                "(  \n" +
                "   ID                             VARCHAR    NOT NULL,\n" +
                "   NAME                           VARCHAR    ,\n" +
                "   ANOTHER_VALUE                  VARCHAR    ,\n" +
                "   TRANSACTION_TIME               TIMESTAMP  ,\n" +
                "   CONSTRAINT pk PRIMARY KEY(ID)\n" +
                ")";
        stmt.execute(createTable);

        //Creating an index
        String createIndex = "CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(NAME, TRANSACTION_TIME DESC) INCLUDE(ANOTHER_VALUE)";
        stmt.execute(createIndex);

        //Insert Some  Items.
        DecimalFormat dmf = new DecimalFormat("0000");
        final String prefix = "ReferenceData.Country/";
        for (int i = 0; i < 10; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                PreparedStatement prstmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?,?)");
                prstmt.setString(1, UUID.randomUUID().toString());
                prstmt.setString(2,prefix + dmf.format(i+j*1000));
                prstmt.setString(3,UUID.randomUUID().toString());
                prstmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                prstmt.execute();
                conn.commit();
                prstmt.close();
            }
        }

        String countSQL = "SELECT COUNT(1) AS TOTAL_ITEMS FROM " + tableName + " where NAME like 'ReferenceData.Country/2%' ";
        ResultSet rs = stmt.executeQuery(countSQL);
        rs.next();
        final int totalCount = rs.getInt("TOTAL_ITEMS");
        rs.close();

        //Now a Cursor
        String cursorSQL = "DECLARE " + cursorName + " CURSOR FOR SELECT NAME,ANOTHER_VALUE FROM "
                + tableName + " where NAME like 'ReferenceData.Country/2%' ORDER BY TRANSACTION_TIME DESC";
        PreparedStatement cursorStatement = conn.prepareStatement(cursorSQL);
        cursorStatement.execute();
        PreparedStatement openCursorStatement = conn.prepareStatement("OPEN " + cursorName);
        openCursorStatement.execute();

        rs = stmt.executeQuery("EXPLAIN FETCH NEXT 10 ROWS FROM " + cursorName);
        rs.next();
        assertTrue(rs.getString(1)
                .contains("CLIENT PARALLEL 1-WAY RANGE SCAN"));
        PreparedStatement next10Rows = conn.prepareStatement("FETCH NEXT 10 ROWS FROM " + cursorName);
        int itemsReturnedByCursor = 0;
        while(true)
        {
            ResultSet cursorRS = next10Rows.executeQuery();
            int rowsReadBeforeEmpty = 0;
            while(cursorRS.next())
            {
                itemsReturnedByCursor++;
                rowsReadBeforeEmpty++;
            }
            if(rowsReadBeforeEmpty > 0 )
            {
                cursorRS.close();
            }
            else
            {
                conn.prepareStatement("CLOSE " + cursorName).executeUpdate();
                break;
            }

            if(itemsReturnedByCursor > (totalCount * 3))
            {
                break;
            }
        }
        assertEquals(totalCount, itemsReturnedByCursor);
    }
}
