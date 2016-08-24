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
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.Format;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.phoenix.util.DateUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class DateTimeIT extends BaseHBaseManagedTimeTableReuseIT {

    protected Connection conn;
    protected Date date;
    protected static final String tenantId = getOrganizationId();
    protected final static String ROW10 = "00D123122312312";
    protected  String tableName;

    public DateTimeIT() throws Exception {
        super();
        date = new Date(System.currentTimeMillis());
    }

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(getUrl());
        this.tableName = initAtable();
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }
    
    private String initAtable() throws SQLException {
        String tableName = generateRandomString();
        ensureTableCreated(getUrl(), tableName, ATABLE_NAME, (byte[][])null);
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " + tableName +
            "(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_STRING, " +
            "    B_STRING, " +
            "    A_INTEGER, " +
            "    A_DATE, " +
            "    X_DECIMAL, " +
            "    X_LONG, " +
            "    X_INTEGER," +
            "    Y_INTEGER," +
            "    A_BYTE," +
            "    A_SHORT," +
            "    A_FLOAT," +
            "    A_DOUBLE," +
            "    A_UNSIGNED_FLOAT," +
            "    A_UNSIGNED_DOUBLE)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        stmt.setString(3, A_VALUE);
        stmt.setString(4, B_VALUE);
        stmt.setInt(5, 1);
        stmt.setDate(6, date);
        stmt.setBigDecimal(7, null);
        stmt.setNull(8, Types.BIGINT);
        stmt.setNull(9, Types.INTEGER);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)1);
        stmt.setShort(12, (short) 128);
        stmt.setFloat(13, 0.01f);
        stmt.setDouble(14, 0.0001);
        stmt.setFloat(15, 0.01f);
        stmt.setDouble(16, 0.0001);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW2);
        stmt.setString(3, A_VALUE);
        stmt.setString(4, C_VALUE);
        stmt.setInt(5, 2);
        stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
        stmt.setBigDecimal(7, null);
        stmt.setNull(8, Types.BIGINT);
        stmt.setNull(9, Types.INTEGER);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)2);
        stmt.setShort(12, (short) 129);
        stmt.setFloat(13, 0.02f);
        stmt.setDouble(14, 0.0002);
        stmt.setFloat(15, 0.02f);
        stmt.setDouble(16, 0.0002);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW3);
        stmt.setString(3, A_VALUE);
        stmt.setString(4, E_VALUE);
        stmt.setInt(5, 3);
        stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
        stmt.setBigDecimal(7, null);
        stmt.setNull(8, Types.BIGINT);
        stmt.setNull(9, Types.INTEGER);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)3);
        stmt.setShort(12, (short) 130);
        stmt.setFloat(13, 0.03f);
        stmt.setDouble(14, 0.0003);
        stmt.setFloat(15, 0.03f);
        stmt.setDouble(16, 0.0003);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setString(3, A_VALUE);
        stmt.setString(4, B_VALUE);
        stmt.setInt(5, 4);
        stmt.setDate(6, date == null ? null : date);
        stmt.setBigDecimal(7, null);
        stmt.setNull(8, Types.BIGINT);
        stmt.setNull(9, Types.INTEGER);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)4);
        stmt.setShort(12, (short) 131);
        stmt.setFloat(13, 0.04f);
        stmt.setDouble(14, 0.0004);
        stmt.setFloat(15, 0.04f);
        stmt.setDouble(16, 0.0004);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.setString(3, B_VALUE);
        stmt.setString(4, C_VALUE);
        stmt.setInt(5, 5);
        stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
        stmt.setBigDecimal(7, null);
        stmt.setNull(8, Types.BIGINT);
        stmt.setNull(9, Types.INTEGER);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)5);
        stmt.setShort(12, (short) 132);
        stmt.setFloat(13, 0.05f);
        stmt.setDouble(14, 0.0005);
        stmt.setFloat(15, 0.05f);
        stmt.setDouble(16, 0.0005);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW6);
        stmt.setString(3, B_VALUE);
        stmt.setString(4, E_VALUE);
        stmt.setInt(5, 6);
        stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
        stmt.setBigDecimal(7, null);
        stmt.setNull(8, Types.BIGINT);
        stmt.setNull(9, Types.INTEGER);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)6);
        stmt.setShort(12, (short) 133);
        stmt.setFloat(13, 0.06f);
        stmt.setDouble(14, 0.0006);
        stmt.setFloat(15, 0.06f);
        stmt.setDouble(16, 0.0006);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW7);
        stmt.setString(3, B_VALUE);
        stmt.setString(4, B_VALUE);
        stmt.setInt(5, 7);
        stmt.setDate(6, date == null ? null : date);
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.1));
        stmt.setLong(8, 5L);
        stmt.setInt(9, 5);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)7);
        stmt.setShort(12, (short) 134);
        stmt.setFloat(13, 0.07f);
        stmt.setDouble(14, 0.0007);
        stmt.setFloat(15, 0.07f);
        stmt.setDouble(16, 0.0007);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW8);
        stmt.setString(3, B_VALUE);
        stmt.setString(4, C_VALUE);
        stmt.setInt(5, 8);
        stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
        stmt.setBigDecimal(7, BigDecimal.valueOf(3.9));
        long l = Integer.MIN_VALUE - 1L;
        assert(l < Integer.MIN_VALUE);
        stmt.setLong(8, l);
        stmt.setInt(9, 4);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)8);
        stmt.setShort(12, (short) 135);
        stmt.setFloat(13, 0.08f);
        stmt.setDouble(14, 0.0008);
        stmt.setFloat(15, 0.08f);
        stmt.setDouble(16, 0.0008);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW9);
        stmt.setString(3, C_VALUE);
        stmt.setString(4, E_VALUE);
        stmt.setInt(5, 9);
        stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
        stmt.setBigDecimal(7, BigDecimal.valueOf(3.3));
        l = Integer.MAX_VALUE + 1L;
        assert(l > Integer.MAX_VALUE);
        stmt.setLong(8, l);
        stmt.setInt(9, 3);
        stmt.setInt(10, 300);
        stmt.setByte(11, (byte)9);
        stmt.setShort(12, (short) 0);
        stmt.setFloat(13, 0.09f);
        stmt.setDouble(14, 0.0009);
        stmt.setFloat(15, 0.09f);
        stmt.setDouble(16, 0.0009);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW10);
        stmt.setString(3, B_VALUE);
        stmt.setString(4, B_VALUE);
        stmt.setInt(5, 7);
        // Intentionally null
        stmt.setDate(6, null);
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.1));
        stmt.setLong(8, 5L);
        stmt.setInt(9, 5);
        stmt.setNull(10, Types.INTEGER);
        stmt.setByte(11, (byte)7);
        stmt.setShort(12, (short) 134);
        stmt.setFloat(13, 0.07f);
        stmt.setDouble(14, 0.0007);
        stmt.setFloat(15, 0.07f);
        stmt.setDouble(16, 0.0007);
        stmt.execute();

        conn.commit();
        return  tableName;

    }

    @Test
    public void selectBetweenDates() throws Exception {
        Format formatter = DateUtil.getDateFormatter("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        java.util.Date dateToday = cal.getTime();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        java.util.Date dateTomorrow = cal.getTime();
        String tableName = generateRandomString();
        String today = formatter.format(dateToday);
        String tomorrow = formatter.format(dateTomorrow);
        String query = "SELECT entity_id FROM " + this.tableName + " WHERE a_integer < 4 AND a_date BETWEEN date '" + today + "' AND date '" + tomorrow + "' ";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectLiteralDate() throws Exception {
        String s = DateUtil.DEFAULT_DATE_FORMATTER.format(date);
        String query = "SELECT DATE '" + s + "' FROM " + this.tableName;
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(date, rs.getDate(1));
    }

    @Test
    public void testSelectLiteralDateCompare() throws Exception {
        String query = "SELECT (DATE '" + date + "' = DATE '" + date + "') FROM " + this.tableName;
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    public void testSelectWhereDatesEqual() throws Exception {
        String query = "SELECT entity_id FROM " + this.tableName + " WHERE  a_integer < 4 AND DATE '" + date + "' = DATE '" + date + "'";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());

    }

    @Test
    public void testSelectWhereDateAndToDateEqual() throws Exception {
        String query = "SELECT entity_id FROM " + this.tableName + " WHERE  a_integer < 4 AND DATE '" + date + "' = TO_DATE ('" + date + "')";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());

    }

    @Test
    public void testSelectWhereDateAndTimestampEqual() throws Exception {
        final String timestamp = "2012-09-08 07:08:23";
        String query = "SELECT entity_id FROM " + this.tableName + " WHERE  a_integer < 4 AND DATE '" + timestamp + "' = TIMESTAMP '" + timestamp + "'";

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
    }

    @Test
    public void testSelectWhereSameDatesUnequal() throws Exception {
        String query = "SELECT entity_id FROM " + this.tableName + " WHERE  a_integer < 4 AND DATE '" + date + "' > DATE '" + date + "'";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertFalse(rs.next());
    }

    @Test
    public void testDateInList() throws Exception {
        String query = "SELECT entity_id FROM " + this.tableName + " WHERE a_date IN (?,?) AND a_integer < 4";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(0));
            statement.setDate(2, date);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
    }  

    @Test
    public void testDateBetweenLiterals() throws Exception {
        Format formatter = DateUtil.getDateFormatter("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        java.util.Date dateToday = cal.getTime();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        java.util.Date dateTomorrow = cal.getTime();
        String today = formatter.format(dateToday);
        String tomorrow = formatter.format(dateTomorrow);
        String query = "SELECT entity_id FROM " + this.tableName + " WHERE a_integer < 4 AND a_date BETWEEN date '" + today + "' AND date '" + tomorrow + "' ";
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
    }

    private static int callYearFunction(Connection conn, String invocation) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs =
                stmt.executeQuery(String
                    .format("SELECT %s FROM SYSTEM.CATALOG LIMIT 1", invocation));
        assertTrue(rs.next());
        int returnValue = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        stmt.close();
        return returnValue;
    }

    private int callYearFunction(String invocation) throws SQLException {
        return callYearFunction(conn, invocation);
    }

    @Test
    public void testYearFunctionDate() throws SQLException {

        assertEquals(2008, callYearFunction("YEAR(TO_DATE('2008-01-01', 'yyyy-MM-dd', 'local'))"));

        assertEquals(2004,
            callYearFunction("YEAR(TO_DATE('2004-12-13 10:13:18', 'yyyy-MM-dd hh:mm:ss'))"));

        assertEquals(2015, callYearFunction("YEAR(TO_DATE('2015-01-27T16:17:57+00:00'))"));

        assertEquals(2005, callYearFunction("YEAR(TO_DATE('2005-12-13 10:13:18'))"));

        assertEquals(2006, callYearFunction("YEAR(TO_DATE('2006-12-13'))"));

        assertEquals(2015, callYearFunction("YEAR(TO_DATE('2015-W05'))"));

        assertEquals(
            2008,
            callYearFunction("YEAR(TO_DATE('Sat, 3 Feb 2008 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'UTC'))"));
    }

    @Test
    public void testYearFunctionTimestamp() throws SQLException {

        assertEquals(2015, callYearFunction("YEAR(TO_TIMESTAMP('2015-01-27T16:17:57+00:00'))"));

        assertEquals(2015, callYearFunction("YEAR(TO_TIMESTAMP('2015-01-27T16:17:57Z'))"));

        assertEquals(2015, callYearFunction("YEAR(TO_TIMESTAMP('2015-W10-3'))"));

        assertEquals(2015, callYearFunction("YEAR(TO_TIMESTAMP('2015-W05'))"));

        assertEquals(2015, callYearFunction("YEAR(TO_TIMESTAMP('2015-063'))"));

        assertEquals(2006, callYearFunction("YEAR(TO_TIMESTAMP('2006-12-13'))"));

        assertEquals(2004,
            callYearFunction("YEAR(TO_TIMESTAMP('2004-12-13 10:13:18', 'yyyy-MM-dd hh:mm:ss'))"));

        assertEquals(
            2008,
            callYearFunction("YEAR(TO_TIMESTAMP('Sat, 3 Feb 2008 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'UTC'))"));
    }

    @Test
    public void testYearFuncAgainstColumns() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME, " +
                        "unsignedDates UNSIGNED_DATE, unsignedTimestamps UNSIGNED_TIMESTAMP, unsignedTimes UNSIGNED_TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2004-03-01 00:00:00'), TO_TIMESTAMP('2006-02-01 00:00:00'), TO_TIME('2008-02-01 00:00:00'), " +
                "TO_DATE('2010-03-01 00:00:00:896', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-02-01'), TO_TIME('2015-02-01 00:00:00'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('2005-03-01 00:00:00'), TO_TIMESTAMP('2006-02-01 00:00:00'), TO_TIME('2008-02-01 00:00:00'), " +
                "TO_DATE('2010-03-01 00:00:00:896', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-02-01'), TO_TIME('2015-02-01 00:00:00'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (3, TO_DATE('2006-03-01 00:00:00'), TO_TIMESTAMP('2006-02-01 00:00:00'), TO_TIME('2008-02-01 00:00:00'), " +
                "TO_DATE('2010-03-01 00:00:00:896', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-02-01'), TO_TIME('2015-02-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, YEAR(timestamps), YEAR(times), Year(unsignedDates), YEAR(unsignedTimestamps), " +
                "YEAR(unsignedTimes) FROM " + tableName + " where YEAR(dates) = 2004");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2006, rs.getInt(2));
        assertEquals(2008, rs.getInt(3));
        assertEquals(2010, rs.getInt(4));
        assertEquals(2012, rs.getInt(5));
        assertEquals(2015, rs.getInt(6));
        assertFalse(rs.next());
    }

    @Test
    public void testMonthFuncAgainstColumns() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME, " +
                        "unsignedDates UNSIGNED_DATE, unsignedTimestamps UNSIGNED_TIMESTAMP, unsignedTimes UNSIGNED_TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2004-03-10 00:00:00'), TO_TIMESTAMP('2006-04-12 00:00:00'), TO_TIME('2008-05-16 00:00:00'), " +
                "TO_DATE('2010-06-20 00:00:00:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:00:00'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('2004-04-10 00:00:00'), TO_TIMESTAMP('2006-04-12 00:00:00'), TO_TIME('2008-05-16 00:00:00'), " +
                "TO_DATE('2010-06-20 00:00:00:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:00:00'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (3, TO_DATE('2004-05-10 00:00:00'), TO_TIMESTAMP('2006-04-12 00:00:00'), TO_TIME('2008-05-16 00:00:00'), " +
                "TO_DATE('2010-06-20 00:00:00:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, MONTH(timestamps), MONTH(times), MONTH(unsignedDates), MONTH(unsignedTimestamps), " +
                "MONTH(unsignedTimes) FROM " + tableName + " where MONTH(dates) = 3");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertEquals(5, rs.getInt(3));
        assertEquals(6, rs.getInt(4));
        assertEquals(7, rs.getInt(5));
        assertEquals(12, rs.getInt(6));
        assertFalse(rs.next());
    }

    @Test
    public void testUnsignedTimeDateWithLiteral() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + "  (k1 INTEGER NOT NULL," +
                        "unsignedDates UNSIGNED_DATE, unsignedTimestamps UNSIGNED_TIMESTAMP, unsignedTimes UNSIGNED_TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, " +
                "'2010-06-20 12:00:00', '2012-07-28 12:00:00', '2015-12-25 12:00:00')";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, unsignedDates, " +
                "unsignedTimestamps, unsignedTimes FROM " + tableName + " where k1 = 1");
        assertTrue(rs.next());
        assertEquals(DateUtil.parseDate("2010-06-20 12:00:00"), rs.getDate(2));
        assertEquals(DateUtil.parseTimestamp("2012-07-28 12:00:00"), rs.getTimestamp(3));
        assertEquals(DateUtil.parseTime("2015-12-25 12:00:00"), rs.getTime(4));
        assertFalse(rs.next());
    }

    @Test
    public void testSecondFuncAgainstColumns() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME, " +
                        "unsignedDates UNSIGNED_DATE, unsignedTimestamps UNSIGNED_TIMESTAMP, unsignedTimes UNSIGNED_TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2004-03-01 00:00:10'), TO_TIMESTAMP('2006-04-12 00:00:20'), TO_TIME('2008-05-16 10:00:30'), " +
                "TO_DATE('2010-06-20 00:00:40:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:00:50'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + "  VALUES (2, TO_DATE('2004-03-01 00:00:10'), TO_TIMESTAMP('2006-04-12 00:20:30'), TO_TIME('2008-05-16 10:00:30'), " +
                "TO_DATE('2010-06-20 00:00:40:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:00:50'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (3, TO_DATE('2004-03-01 00:00:10'), TO_TIMESTAMP('2006-04-12 00:50:30'), TO_TIME('2008-05-16 10:00:30'), " +
                "TO_DATE('2010-06-20 00:00:40:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:00:50'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, SECOND(dates), SECOND(times), SECOND(unsignedDates), SECOND(unsignedTimestamps), " +
                "SECOND(unsignedTimes) FROM " + tableName + " where SECOND(timestamps)=20");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        assertEquals(30, rs.getInt(3));
        assertEquals(40, rs.getInt(4));
        assertEquals(0, rs.getInt(5));
        assertEquals(50, rs.getInt(6));
        assertFalse(rs.next());
    }

    @Test
    public void testWeekFuncAgainstColumns() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + "  (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2004-01-10 10:00:10'), TO_TIMESTAMP('2006-04-12 08:00:20'), TO_TIME('2008-05-16 10:00:30'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('2004-01-10 10:00:10'), TO_TIMESTAMP('2006-05-18 08:00:20'), TO_TIME('2008-05-16 10:00:30'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (3, TO_DATE('2004-01-10 10:00:10'), TO_TIMESTAMP('2006-05-18 08:00:20'), TO_TIME('2008-05-16 10:00:30'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, WEEK(dates), WEEK(times) FROM " + tableName + " where WEEK(timestamps)=15");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(20, rs.getInt(3));
        assertFalse(rs.next());
    }

    @Test
    public void testHourFuncAgainstColumns() throws Exception {
        String tableName  = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('Sat, 3 Feb 2008 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'UTC'), TO_TIMESTAMP('2006-04-12 15:10:20'), " +
                "TO_TIME('2008-05-16 20:40:30'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('Sat, 3 Feb 2008 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'UTC'), TO_TIMESTAMP('2006-04-12 10:10:20'), " +
                "TO_TIME('2008-05-16 20:40:30'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (3, TO_DATE('Sat, 3 Feb 2008 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'UTC'), TO_TIMESTAMP('2006-04-12 08:10:20'), " +
                "TO_TIME('2008-05-16 20:40:30'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, HOUR(dates), HOUR(times) FROM " + tableName + " where HOUR(timestamps)=15");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(3, rs.getInt(2));
        assertEquals(20, rs.getInt(3));
        assertFalse(rs.next());
    }

    @Test
    public void testNowFunction() throws Exception {
        String tableName = generateRandomString();
        Date date = new Date(System.currentTimeMillis());
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, timestamps TIMESTAMP CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setDate(2, new Date(date.getTime()-500));
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setDate(2, new Date(date.getTime()+600000));
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * from " + tableName + "  where now() > timestamps");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(new Date(date.getTime()-500), rs.getDate(2));
        assertFalse(rs.next());
    }

    @Test
    public void testMinuteFuncAgainstColumns() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME, " +
                        "unsignedDates UNSIGNED_DATE, unsignedTimestamps UNSIGNED_TIMESTAMP, unsignedTimes UNSIGNED_TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2004-03-01 00:10:10'), TO_TIMESTAMP('2006-04-12 00:20:20'), TO_TIME('2008-05-16 10:30:30'), " +
                "TO_DATE('2010-06-20 00:40:40:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:50:50'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('2004-03-01 00:10:10'), TO_TIMESTAMP('2006-04-12 00:50:20'), TO_TIME('2008-05-16 10:30:30'), " +
                "TO_DATE('2010-06-20 00:40:40:789', 'yyyy-MM-dd HH:mm:ss:SSS'), TO_TIMESTAMP('2012-07-28'), TO_TIME('2015-12-25 00:50:50'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, MINUTE(dates), MINUTE(times), MINUTE(unsignedDates), MINUTE(unsignedTimestamps), " +
                "MINUTE(unsignedTimes) FROM " + tableName + " where MINUTE(timestamps)=20");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        assertEquals(30, rs.getInt(3));
        assertEquals(40, rs.getInt(4));
        assertEquals(0, rs.getInt(5));
        assertEquals(50, rs.getInt(6));
        assertFalse(rs.next());
    }
    
    @Test
    public void testDayOfMonthFuncAgainstColumns() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2004-01-08 10:00:10'), TO_TIMESTAMP('2006-04-12 08:00:20'), TO_TIME('2008-05-26 11:00:30'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('2004-01-18 10:00:10'), TO_TIMESTAMP('2006-05-22 08:00:20'), TO_TIME('2008-12-30 11:00:30'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, DAYOFMONTH(dates), DAYOFMONTH(times) FROM " + tableName + " where DAYOFMONTH(timestamps)=12");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(8, rs.getInt(2));
        assertEquals(26, rs.getInt(3));
        assertFalse(rs.next());
    }

    @Test
    public void testNullDate() throws Exception {

        ResultSet rs = conn.createStatement().executeQuery("SELECT a_date, entity_id from " + this.tableName + " WHERE entity_id = '" + ROW10 + "'");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(ROW10, rs.getString(2));
        assertNull(rs.getDate(1));
        assertNull(rs.getDate(1, GregorianCalendar.getInstance()));
        assertFalse(rs.next());
    }
    
    @Test
    public void testCurrentDateWithNoTable() throws Exception {
        long expectedTime = System.currentTimeMillis();
        ResultSet rs = conn.createStatement().executeQuery("SELECT CURRENT_DATE()");
        assertTrue(rs.next());
        long actualTime = rs.getDate(1).getTime();
        assertTrue(Math.abs(actualTime - expectedTime) < MILLIS_IN_DAY);
    }
    @Test
    public void testSelectBetweenNanos() throws Exception {
        String tableName = generateRandomString();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL PRIMARY KEY, ts " +
                        "TIMESTAMP(3))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TIMESTAMP'2015-01-01 00:00:00.111111111')";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TIMESTAMP'2015-01-01 00:00:00.111111115')";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (3, TIMESTAMP'2015-01-01 00:00:00.111111113')";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1,ts from " + tableName + " where ts between" +
                " TIMESTAMP'2015-01-01 00:00:00.111111112' AND TIMESTAMP'2015-01-01 00:00:00" +
                ".111111114'");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(111111113, rs.getTimestamp(2).getNanos());
        assertFalse(rs.next());
    }

    @Test
    public void testCurrentTimeWithProjectedTable () throws Exception {
        String tableName1 = generateRandomString();
        String tableName2 = generateRandomString();
        String ddl = "CREATE TABLE " + tableName1 + " ( ID integer primary key)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE TABLE " + tableName2 + " ( ID integer primary key)";
        conn.createStatement().execute(ddl);
        String ups = "UPSERT INTO " + tableName1 + " VALUES (1)";
        conn.createStatement().execute(ups);
        ups = "UPSERT INTO " + tableName2 + " VALUES (1)";
        conn.createStatement().execute(ups);
        conn.commit();
        ResultSet rs = conn.createStatement().executeQuery("select /*+ USE_SORT_MERGE_JOIN */ op" +
                ".id, current_time() from " +tableName1 + " op where op.id in (select id from " + tableName2 + ")");
        assertTrue(rs.next());
        assertEquals(new java.util.Date().getYear(),rs.getTimestamp(2).getYear());
    }
    
    @Test
    public void testLiteralDateComparison() throws Exception {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "select DATE '2016-05-10 00:00:00' > DATE '2016-05-11 00:00:00'");

        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testLiteralTimestampComparison() throws Exception {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "select TIMESTAMP '2016-05-10 00:00:00' > TIMESTAMP '2016-05-11 00:00:00'");

        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testLiteralDateTimestampComparison() throws Exception {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "select DATE '2016-05-10 00:00:00' > TIMESTAMP '2016-05-11 00:00:00'");

        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testLiteralDateTimestampComparison2() throws Exception {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "select TIMESTAMP '2016-05-10 00:00:00' > DATE '2016-05-11 00:00:00'");

        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.next());
    }
}
