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
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.Format;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.List;
import java.util.Arrays;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class DateTimeIT extends ParallelStatsDisabledIT {
    private static final String PRODUCT_METRICS_NAME = "PRODUCT_METRICS";
    private static final Date SPLIT1 = toDate("1970-01-01 01:30:00");
    private static final Date SPLIT2 = toDate("1970-01-01 02:00:00");
    private static final String R1 = "R1";
    private static final String R2 = "R2";

    protected Connection conn;
    protected Date date;
    protected static final String tenantId = getOrganizationId();
    protected final static String ROW10 = "00D123122312312";
    protected  String tableName;

    private static void initDateTableValues(String tablename, Connection conn, String tenantId, Date startDate) throws Exception {
        double dateIncrement = 2.0;
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +tablename+
                        "(" +
                        "    ORGANIZATION_ID, " +
                        "    \"DATE\", " +
                        "    FEATURE, " +
                        "    UNIQUE_USERS, " +
                        "    TRANSACTIONS, " +
                        "    CPU_UTILIZATION, " +
                        "    DB_UTILIZATION, " +
                        "    REGION, " +
                        "    IO_TIME)" +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "A");
        stmt.setInt(4, 10);
        stmt.setLong(5, 100L);
        stmt.setBigDecimal(6, BigDecimal.valueOf(0.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.2));
        stmt.setString(8, R2);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "B");
        stmt.setInt(4, 20);
        stmt.setLong(5, 200);
        stmt.setBigDecimal(6, BigDecimal.valueOf(1.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.4));
        stmt.setString(8, null);
        stmt.setLong(9, 2000);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "C");
        stmt.setInt(4, 30);
        stmt.setLong(5, 300);
        stmt.setBigDecimal(6, BigDecimal.valueOf(2.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.6));
        stmt.setString(8, R1);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "D");
        stmt.setInt(4, 40);
        stmt.setLong(5, 400);
        stmt.setBigDecimal(6, BigDecimal.valueOf(3.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.8));
        stmt.setString(8, R1);
        stmt.setLong(9, 4000);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "E");
        stmt.setInt(4, 50);
        stmt.setLong(5, 500);
        stmt.setBigDecimal(6, BigDecimal.valueOf(3.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(1.2));
        stmt.setString(8, R2);
        stmt.setLong(9, 5000);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "F");
        stmt.setInt(4, 60);
        stmt.setLong(5, 600);
        stmt.setBigDecimal(6, BigDecimal.valueOf(4.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(1.4));
        stmt.setString(8, null);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();
    }

    private static void initDateTableValues(String tablename, String tenantId, byte[][] splits, Date startDate) throws Exception {
        ensureTableCreated(getUrl(), tablename, PRODUCT_METRICS_NAME, splits, null, null);

       Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
       Connection conn = DriverManager.getConnection(getUrl(), props);
       try {
           initDateTableValues(tablename, conn, tenantId, startDate);
           conn.commit();
       } finally {
           conn.close();
       }
    }


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
    public void tearDown() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        conn.close();
        assertFalse("refCount leaked", refCountLeaked);
    }
    
    private String initAtable() throws SQLException {
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, ATABLE_NAME, (byte[][])null, null);
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
        String tableName = generateUniqueName();
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
    public void testNowWithSubquery() throws Exception {
        String query =
                "SELECT now(), reference_date FROM (select now() as "
                        + "reference_date union all select now() as reference_date) limit 1";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertTrue(Math.abs(rs.getTime(1).getTime()-rs.getTime(2).getTime())<10000);
        assertEquals(rs.getDate(2).toString(), rs.getDate(1).toString());
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
                    .format("SELECT %s FROM \"SYSTEM\".\"CATALOG\" LIMIT 1", invocation));
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

        assertEquals(2008, callYearFunction("\"YEAR\"(TO_DATE('2008-01-01', 'yyyy-MM-dd'))"));

        assertEquals(2004,
            callYearFunction("\"YEAR\"(TO_DATE('2004-12-13 10:13:18', 'yyyy-MM-dd hh:mm:ss'))"));

        assertEquals(2015, callYearFunction("\"YEAR\"(TO_DATE('2015-01-27T16:17:57+00:00'))"));

        assertEquals(2005, callYearFunction("\"YEAR\"(TO_DATE('2005-12-13 10:13:18'))"));

        assertEquals(2006, callYearFunction("\"YEAR\"(TO_DATE('2006-12-13'))"));

        assertEquals(2015, callYearFunction("\"YEAR\"(TO_DATE('2015-W05'))"));

        assertEquals(
            2008,
            callYearFunction("\"YEAR\"(TO_DATE('Sat, 3 Feb 2008 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'UTC'))"));
    }

    @Test
    public void testYearFunctionTimestamp() throws SQLException {

        assertEquals(2015, callYearFunction("\"YEAR\"(TO_TIMESTAMP('2015-01-27T16:17:57+00:00'))"));

        assertEquals(2015, callYearFunction("\"YEAR\"(TO_TIMESTAMP('2015-01-27T16:17:57Z'))"));

        assertEquals(2015, callYearFunction("\"YEAR\"(TO_TIMESTAMP('2015-W10-3'))"));

        assertEquals(2015, callYearFunction("\"YEAR\"(TO_TIMESTAMP('2015-W05'))"));

        assertEquals(2015, callYearFunction("\"YEAR\"(TO_TIMESTAMP('2015-063'))"));

        assertEquals(2006, callYearFunction("\"YEAR\"(TO_TIMESTAMP('2006-12-13'))"));

        assertEquals(2004,
            callYearFunction("\"YEAR\"(TO_TIMESTAMP('2004-12-13 10:13:18', 'yyyy-MM-dd hh:mm:ss'))"));

        assertEquals(
            2008,
            callYearFunction("\"YEAR\"(TO_TIMESTAMP('Sat, 3 Feb 2008 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'UTC'))"));
    }

    @Test
    public void testYearFuncAgainstColumns() throws Exception {
        String tableName = generateUniqueName();
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

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, \"YEAR\"(timestamps), \"YEAR\"(times), \"YEAR\"(unsignedDates), \"YEAR\"(unsignedTimestamps), " +
                "\"YEAR\"(unsignedTimes) FROM " + tableName + " where \"YEAR\"(dates) = 2004");
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
        String tableName = generateUniqueName();
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

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, \"MONTH\"(timestamps), \"MONTH\"(times), \"MONTH\"(unsignedDates), \"MONTH\"(unsignedTimestamps), " +
                "\"MONTH\"(unsignedTimes) FROM " + tableName + " where \"MONTH\"(dates) = 3");
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
        String tableName = generateUniqueName();
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
        String tableName = generateUniqueName();
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
        String tableName = generateUniqueName();
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

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, \"WEEK\"(dates), \"WEEK\"(times) FROM " + tableName + " where \"WEEK\"(timestamps)=15");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(20, rs.getInt(3));
        assertFalse(rs.next());
    }

    @Test
    public void testHourFuncAgainstColumns() throws Exception {
        String tableName  = generateUniqueName();
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

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, \"HOUR\"(dates), \"HOUR\"(times) FROM " + tableName + " where \"HOUR\"(timestamps)=15");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(3, rs.getInt(2));
        assertEquals(20, rs.getInt(3));
        assertFalse(rs.next());
    }

    @Test
    public void testNowFunction() throws Exception {
        String tableName = generateUniqueName();
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
        String tableName = generateUniqueName();
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

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, \"MINUTE\"(dates), \"MINUTE\"(times), \"MINUTE\"(unsignedDates), \"MINUTE\"(unsignedTimestamps), " +
                "\"MINUTE\"(unsignedTimes) FROM " + tableName + " where \"MINUTE\"(timestamps)=20");
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
        String tableName = generateUniqueName();
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

    /*
    Reference for dates used in the test
    2013-04-09 - Tuesday (2)
     2014-05-18 - Sunday (7)
    2015-06-27 - Saturday (6)
     */
    @Test
    public void testDayOfWeekFuncAgainstColumns() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2012-03-08 11:01:10'), TO_TIMESTAMP('2013-06-16 12:02:20'), TO_TIME('2014-09-23 13:03:30'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('2013-04-09 11:02:10'), TO_TIMESTAMP('2014-05-18 12:03:20'), TO_TIME('2015-06-27 13:04:30'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, DAYOFWEEK(dates), DAYOFWEEK(timestamps) FROM " + tableName + " where DAYOFWEEK(times)=6");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(7, rs.getInt(3));
        assertFalse(rs.next());
    }

    @Test
    public void testDayOfYearFuncAgainstColumns() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES (1, TO_DATE('2012-03-01 11:01:10'), TO_TIMESTAMP('2013-02-01 12:02:20'), TO_TIME('2014-01-15 13:03:30'))";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES (2, TO_DATE('2013-04-09 11:02:10'), TO_TIMESTAMP('2014-05-18 12:03:20'), TO_TIME('2015-06-27 13:04:30'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, DAYOFYEAR(dates), DAYOFYEAR(timestamps) FROM " + tableName + " where DAYOFYEAR(times)=15");

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(61, rs.getInt(2));
        assertEquals(32, rs.getInt(3));
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
        String tableName = generateUniqueName();
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
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
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
                    "select \"DATE\" '2016-05-10 00:00:00' > \"TIMESTAMP\" '2016-05-11 00:00:00'");

        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testLiteralDateTimestampComparison2() throws Exception {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "select \"TIMESTAMP\" '2016-05-10 00:00:00' > \"DATE\" '2016-05-11 00:00:00'");

        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testFunctionOnNullDate() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("SELECT \"YEAR\"(a_date), entity_id from " + this.tableName + " WHERE entity_id = '" + ROW10 + "'");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(ROW10, rs.getString(2));
        assertNull(rs.getDate(1));
        assertNull(rs.getDate(1, GregorianCalendar.getInstance()));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedDateTimestampUnequal() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates DATE, timestamps TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setDate(2, new Date(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedTimeTimestampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, times TIME, timestamps TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIME('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setTime(2, new Time(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT times = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedDateTimeCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates DATE, times TIME)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIME('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = times FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedDateUnsignedTimestampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates DATE, timestamps UNSIGNED_TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setDate(2, new Date(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedTimeUnsignedTimestampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, times TIME, timestamps UNSIGNED_TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIME('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setTime(2, new Time(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT times = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedDateUnsignedTimeCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates DATE, times UNSIGNED_TIME)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIME('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = times FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedDateTimestampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates UNSIGNED_DATE, timestamps TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setDate(2, new Date(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedTimeTimestampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, times UNSIGNED_TIME, timestamps TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIME('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setTime(2, new Time(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT times = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedDateTimeCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates UNSIGNED_DATE, times TIME)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIME('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = times FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedDateUnsignedTimestampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates UNSIGNED_DATE, timestamps UNSIGNED_TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setDate(2, new Date(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedTimeUnsignedTimestampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, times UNSIGNED_TIME, timestamps UNSIGNED_TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIME('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        stmt.setTime(2, new Time(1000));
        Timestamp ts = new Timestamp(1000);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT times = timestamps FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedDateUnsignedTimeCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, dates UNSIGNED_DATE, times UNSIGNED_TIME)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_TIME('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT dates = times FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedDateDateCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, date1 DATE, date2 DATE)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_DATE('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_DATE('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_DATE('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT date1 = date2 FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedDateUnsignedDateCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, date1 UNSIGNED_DATE, date2 UNSIGNED_DATE)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_DATE('2004-02-04 00:10:10')," +
                "TO_DATE('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_DATE('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_DATE('2004-02-04 00:10:10'), " +
                "TO_DATE('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT date1 = date2 FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedTimeTimeCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, time1 TIME, time2 TIME)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIME('2004-02-04 00:10:10')," +
                "TO_TIME('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT time1 = time2 FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedTimeUnsignedTimeCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, time1 UNSIGNED_TIME, time2 UNSIGNED_TIME)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIME('2004-02-04 00:10:10')," +
                "TO_TIME('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (3," +
                "TO_TIME('2004-02-04 00:10:10'), " +
                "TO_TIME('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT time1 = time2 FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedTimeStampTimeStampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, timestamp1 TIMESTAMP, timestamp2 TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIMESTAMP('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIMESTAMP('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        Timestamp ts = new Timestamp(1000);
        stmt.setTimestamp(2, ts);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_TIMESTAMP('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT timestamp1 = timestamp2 FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }

    @Test
    public void testProjectedUnsignedTimeStampUnsignedTimeStampCompare() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY, timestamp1 UNSIGNED_TIMESTAMP, timestamp2 UNSIGNED_TIMESTAMP)";
        conn.createStatement().execute(ddl);
        // Differ by date
        String dml = "UPSERT INTO " + tableName + " VALUES (1," +
                "TO_TIMESTAMP('2004-02-04 00:10:10')," +
                "TO_TIMESTAMP('2006-04-12 00:10:10'))";
        conn.createStatement().execute(dml);
        // Differ by time
        dml = "UPSERT INTO " + tableName + " VALUES (2," +
                "TO_TIMESTAMP('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 15:10:20'))";
        conn.createStatement().execute(dml);
        // Differ by nanoseconds
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)");
        stmt.setInt(1, 3);
        Timestamp ts = new Timestamp(1000);
        stmt.setTimestamp(2, ts);
        ts.setNanos(100);
        stmt.setTimestamp(3, ts);
        stmt.execute();
        // Equality
        dml = "UPSERT INTO " + tableName + " VALUES (4," +
                "TO_TIMESTAMP('2004-02-04 00:10:10'), " +
                "TO_TIMESTAMP('2004-02-04 00:10:10'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT timestamp1 = timestamp2 FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.next());
    }
    
    private static byte[][] getSplits(String tenantId) {
        return new byte[][] {
                ByteUtil.concat(Bytes.toBytes(tenantId), PDate.INSTANCE.toBytes(SPLIT1)),
                ByteUtil.concat(Bytes.toBytes(tenantId), PDate.INSTANCE.toBytes(SPLIT2)),
        };
    }

    private static Date toDate(String dateString) {
        return DateUtil.parseDate(dateString);
    }

    @Test
    public void testDateSubtractionCompareNumber() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and ? - \"DATE\" > 3";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            Date startDate = new Date(System.currentTimeMillis());
            Date endDate = new Date(startDate.getTime() + 6 * QueryConstants.MILLIS_IN_DAY);
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, endDate);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("B", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateSubtractionLongToDecimalCompareNumber() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and ? - \"DATE\" - 1.5 > 3";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            Date startDate = new Date(System.currentTimeMillis());
            Date endDate = new Date(startDate.getTime() + 9 * QueryConstants.MILLIS_IN_DAY);
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, endDate);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("B", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("C", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateSubtractionCompareDate() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and date - 1 >= ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            Date startDate = new Date(System.currentTimeMillis());
            Date endDate = new Date(startDate.getTime() + 9 * QueryConstants.MILLIS_IN_DAY);
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, endDate);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("F", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateAddCompareDate() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and date + 1 >= ?";
        Connection conn = DriverManager.getConnection(url);
        try {
            Date startDate = new Date(System.currentTimeMillis());
            Date endDate = new Date(startDate.getTime() + 8 * QueryConstants.MILLIS_IN_DAY);
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, endDate);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("E", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("F", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCurrentDate() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and \"DATE\" - current_date() > 8";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            Date startDate = new Date(System.currentTimeMillis());
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("F", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCurrentTime() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and \"DATE\" - current_time() > 8";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            Date startDate = new Date(System.currentTimeMillis());
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("F", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCastTimeStampToDate() throws Exception {
        String tablename = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tablename +
                " (PK INTEGER PRIMARY KEY, A_TIMESTAMP TIMESTAMP)";
        Properties props = new Properties();
        props.setProperty("phoenix.query.dateFormatTimeZone", TimeZone.getDefault().toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String localTime = LocalDate.now().toString();
        conn.createStatement().execute("UPSERT INTO " + tablename +
                " VALUES(1,TO_TIMESTAMP('"+ localTime + "'))");

        conn.setAutoCommit(true);
        try {
            PreparedStatement statement =
                    conn.prepareStatement("SELECT CAST(A_TIMESTAMP AS DATE) as A_DATE FROM " + tablename);

            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue (rs.getString(1).contains(localTime));
            assertFalse (rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTimestamp() throws Exception {
        String updateStmt = 
            "upsert into " + tableName +
            " (" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_TIMESTAMP) " +
            "VALUES (?, ?, ?)";
        // Override value that was set at creation time
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        Timestamp tsValue1 = new Timestamp(5000);
        byte[] ts1 = PTimestamp.INSTANCE.toBytes(tsValue1);
        stmt.setTimestamp(3, tsValue1);
        stmt.execute();
        
        Connection conn1 = DriverManager.getConnection(url, props);
        TestUtil.analyzeTable(conn1, tableName);
        conn1.close();
        
        updateStmt = 
            "upsert into " + tableName +
            " (" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_TIMESTAMP," +
            "    A_TIME) " +
            "VALUES (?, ?, ?, ?)";
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        Timestamp tsValue2 = new Timestamp(5000);
        tsValue2.setNanos(200);
        byte[] ts2 = PTimestamp.INSTANCE.toBytes(tsValue2);
        stmt.setTimestamp(3, tsValue2);
        stmt.setTime(4, new Time(tsValue2.getTime()));
        stmt.execute();
        upsertConn.close();
        
        assertTrue(TestUtil.compare(CompareOp.GREATER, new ImmutableBytesWritable(ts2), new ImmutableBytesWritable(ts1)));
        assertFalse(TestUtil.compare(CompareOp.GREATER, new ImmutableBytesWritable(ts1), new ImmutableBytesWritable(ts1)));

        String query = "SELECT entity_id, a_timestamp, a_time FROM " + tableName + " WHERE organization_id=? and a_timestamp > ?";
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setTimestamp(2, new Timestamp(5000));
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertEquals(rs.getTimestamp("A_TIMESTAMP"), tsValue2);
            assertEquals(rs.getTime("A_TIME"), new Time(tsValue2.getTime()));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateFormatTimeZone()throws Exception {
        String[] timeZoneIDs = {DateUtil.DEFAULT_TIME_ZONE_ID, "Asia/Yerevan", "Australia/Adelaide", "Asia/Tokyo"};
        for (String timeZoneID : timeZoneIDs) {
            testDateFormatTimeZone(timeZoneID);
        }
    }

    private String getFormattedDate(List<String> dateList) {
        return String.join("-", dateList.subList(0, 3)) + " "
                + String.join(":", dateList.subList(3, 6)) + "." + dateList.get(6);
    }

    @Test
    public void testAncientDates() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        Statement stmt = conn.createStatement();
        String tableName = generateUniqueName();

        List<String> date1list = Arrays.asList("0010", "10", "10", "10", "10", "10", "111");
        List<String> date2list = Arrays.asList("1001", "02", "03", "04", "05", "06", "000");
        List<String> date3list = Arrays.asList("0001", "12", "31", "23", "59", "59", "000");
        List<List<String>> dateLists = Arrays.asList(date1list, date2list, date3list, date2list);

        String date1 = getFormattedDate(date1list); // 0010-10-10 10:10:10.111
        String date2 = getFormattedDate(date2list); // 1000-02-03 04:05:06.000
        String date3 = getFormattedDate(date3list); // 0001-12-31 23:59:59.000
        List<String> dates = Arrays.asList(date1, date2, date3, date2);


        stmt.execute("CREATE TABLE " + tableName + " ( id INTEGER not null PRIMARY KEY," +
                " date DATE, time TIME, timestamp TIMESTAMP)");

        stmt.execute("UPSERT INTO " + tableName + " VALUES(1, TO_DATE('" + date1
                + "'), TO_TIME('" + date1 + "'), TO_TIMESTAMP('" + date1 + "'))");

        PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO " + tableName + " values (?, ?, ?, ?)");
        pstmt.setInt(1, 2);
        Timestamp t = new Timestamp(DateUtil.parseDate(date2).getTime());
        pstmt.setDate(2, new Date(t.getTime()));
        pstmt.setTime(3, new Time(t.getTime()));
        pstmt.setTimestamp(4, t);
        pstmt.execute();

        pstmt.setInt(1, 3);
        t = new Timestamp(DateUtil.parseDate(date3).getTime());
        pstmt.setDate(2, new Date(t.getTime()));
        pstmt.setTime(3, new Time(t.getTime()));
        pstmt.setTimestamp(4, t);
        pstmt.execute();

        String f = " GMT', 'yyyy-MM-dd HH:mm:ss.SSS z', 'UTC";
        stmt.execute("UPSERT INTO " + tableName + " VALUES(4, TO_DATE('" + date2 + f
                + "'), TO_TIME('" + date2 + f + "'), TO_TIMESTAMP('" + date2 + f + "'))");
        conn.commit();

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id");

        for (int i = 0; i < dates.size(); i++) {
            assertTrue(rs.next());
            String actualDate = dates.get(i);
            Timestamp expectedTimestamp = DateUtil.parseTimestamp(actualDate);

            assertEquals(i + 1, rs.getInt(1));
            Date expectedDate = new Date(expectedTimestamp.getTime());
            assertEquals(expectedDate, rs.getDate(2));
            assertEquals(rs.getDate(2), rs.getObject(2));
            assertEquals(actualDate, rs.getString(2));

            Time expectedTime = new Time(expectedTimestamp.getTime());
            assertEquals(new Timestamp(expectedTime.getTime()), new Timestamp(rs.getTime(3).getTime()));
            assertEquals(expectedTime, rs.getTime(3));
            assertEquals(rs.getTime(3), rs.getObject(3));
            assertEquals(actualDate, rs.getString(3));

            assertEquals(expectedTimestamp, rs.getTimestamp(4));
            assertEquals(rs.getTimestamp(4), rs.getObject(4));
            assertEquals(actualDate, rs.getString(4));
        }

        String query = "SELECT year(timestamp), month(timestamp), dayofmonth(timestamp),"
                + " hour(timestamp), minute(timestamp), second(timestamp) FROM " + tableName
                + " ORDER BY id";
        rs = stmt.executeQuery(query);

        for (int i = 0; i < dates.size(); i++) {
            assertTrue(rs.next());
            List<String> dateList = dateLists.get(i);
            for (int j = 0; j < 6; j++) {
                int expected = Integer.parseInt(dateList.get(j));
                int value = rs.getInt(j + 1);
                String readFunc = query.split("\\s+")[j + 1];
                assertTrue("Expected for " + readFunc.substring(0, readFunc.length() - 1) + ": " + expected + ", got: " + value,
                        expected == value);
            }
        }

        pstmt = conn.prepareStatement("UPSERT INTO " + tableName + " values (?, ?, ?, ?)");
        pstmt.setInt(1, 5);
        long l = -123456789100000L;
        Timestamp inserted = new Timestamp(l);
        pstmt.setDate(2, new Date(inserted.getTime()));
        pstmt.setTime(3, new Time(inserted.getTime()));
        pstmt.setTimestamp(4, inserted);
        pstmt.execute();
        conn.commit();

        rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id=5 ORDER BY id");
        assertTrue(rs.next());
        Timestamp read = rs.getTimestamp(4);
        assertEquals(inserted, read);
    }

    @Test
    public void testAncientDatesWithJavaTime() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        Statement stmt = conn.createStatement();
        String tableName = generateUniqueName();

        stmt.execute("CREATE TABLE " + tableName + " ( id INTEGER not null PRIMARY KEY," +
                "timestamp TIMESTAMP)");

        String date = "0010-10-10T10:10:10.111Z";

        java.time.Instant instant = java.time.Instant.parse(date);
        // 0010-10-10T10:10:10.111Z parsed in ISO chronology

        PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO " + tableName + " values (?, ?)");
        pstmt.setInt(1, 1);
        Timestamp inserted = new Timestamp(instant.toEpochMilli());
        // java.sql.timestamp value will be 0010-10-12 because of GJ chronology used by java.sql.timestamp

        pstmt.setTimestamp(2, inserted);
        pstmt.execute();
        conn.commit();

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id=1");
        assertTrue(rs.next());
        Timestamp read = rs.getTimestamp(2); // read back the 0010-10-12 in GJ
        assertEquals(inserted, read);
        // java.time.Instant value will be 0010-10-10 in ISO
        assertEquals(instant, java.time.Instant.ofEpochMilli(read.getTime()));

        assertEquals(instant.toEpochMilli(), read.getTime());
        // same long value interpreted in different chronology results different date.
        // Note that the string output is inconsistent but it is expected because of the different chronologies
        assertEquals(read.toString().split("\\s+")[0], "0010-10-12");
        assertEquals(instant.toString().split("T")[0], "0010-10-10");

        // test for getdayofMonth will be broken see below because we stored the 0010-10-12 data

        java.time.LocalDateTime localDateTime = java.time.LocalDateTime.of(10, 10, 10, 10, 10, 10);
        pstmt.setInt(1, 2);
        inserted = java.sql.Timestamp.valueOf(localDateTime);

        pstmt.setTimestamp(2, inserted);
        pstmt.execute();
        conn.commit();

        rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id=2");
        assertTrue(rs.next());
        read = rs.getTimestamp(2);
        assertEquals(inserted, read);
        assertEquals(localDateTime, read.toLocalDateTime());
        assertEquals(localDateTime.toEpochSecond(ZoneOffset.UTC),
                read.toLocalDateTime().toEpochSecond(ZoneOffset.UTC));

        // Inserting via java.sql.Timestamp.valueOf(localDateTime) will result the same toString result

        assertEquals(read.toString().split("\\s+")[0], "0010-10-10");
        assertEquals(instant.toString().split("T")[0], "0010-10-10");

        // the long value that represents the the Date will be different thought
        // can be expected because it resulted the same output string using different chronology
        assertNotEquals(instant.toEpochMilli(), read.getTime());

        // Converting Instant to LocalDateTime will behave like the LocalDateTime example
        pstmt.setInt(1, 3);
        java.time.LocalDateTime localDateTime1 = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneId.of("GMT"));
        inserted = java.sql.Timestamp.valueOf(localDateTime1);

        pstmt.setTimestamp(2, inserted);
        pstmt.execute();
        conn.commit();

        rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id=3");
        assertTrue(rs.next());
        read = rs.getTimestamp(2);
        assertEquals(inserted, read);
        assertEquals(localDateTime1, read.toLocalDateTime());
        assertEquals(read.toString().split("\\s+")[0], "0010-10-10");
        assertEquals(localDateTime1.toString().split("T")[0], "0010-10-10");

        java.time.Instant inst = localDateTime1.toInstant(ZoneOffset.UTC);
        assertEquals(instant, inst);
        assertNotEquals(inst.toEpochMilli(), inserted.getTime());


        String query = "SELECT dayofmonth(timestamp) FROM " + tableName;
        rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        // Timestamp inserted = new Timestamp(instant.toEpochMilli());
        assertEquals(12, rs.getInt(1));
        assertTrue(rs.next());
        // inserted = java.sql.Timestamp.valueOf(localDateTime);
        assertEquals(10, rs.getInt(1));
        assertTrue(rs.next());
        // inserted = java.sql.Timestamp.valueOf(java.time.LocalDateTime.ofInstant(instant, java.time.ZoneId.of("GMT")));
        assertEquals(10, rs.getInt(1));
        assertFalse(rs.next());
    }

    public void testDateFormatTimeZone(String timeZoneId) throws Exception {
        Properties props = new Properties();
        props.setProperty("phoenix.query.dateFormatTimeZone", timeZoneId);
        Connection conn1 = DriverManager.getConnection(getUrl(), props);

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName +
                " (k1 INTEGER PRIMARY KEY," +
                " v_date DATE," +
                " v_time TIME," +
                " v_timestamp TIMESTAMP)";
        try {
            conn1.createStatement().execute(ddl);

            PhoenixConnection pConn = conn1.unwrap(PhoenixConnection.class);
            verifyTimeZoneIDWithConn(pConn, PDate.INSTANCE, timeZoneId);
            verifyTimeZoneIDWithConn(pConn, PTime.INSTANCE, timeZoneId);
            verifyTimeZoneIDWithConn(pConn, PTimestamp.INSTANCE, timeZoneId);

            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId));
            cal.setTime(date);
            String dateStr = DateUtil.getDateFormatter(DateUtil.DEFAULT_MS_DATE_FORMAT).format(date);

            String dml = "UPSERT INTO " + tableName + " VALUES (" +
                    "1," +
                    "'" + dateStr + "'," +
                    "'" + dateStr + "'," +
                    "'" + dateStr + "'" +
                    ")";
            conn1.createStatement().execute(dml);
            conn1.commit();

            PhoenixStatement stmt = conn1.createStatement().unwrap(PhoenixStatement.class);
            ResultSet rs = stmt.executeQuery("SELECT v_date, v_time, v_timestamp FROM " + tableName);

            assertTrue(rs.next());
            assertEquals(rs.getDate(1).toString(), new Date(cal.getTimeInMillis()).toString());
            assertEquals(rs.getTime(2).toString(), new Time(cal.getTimeInMillis()).toString());
            assertEquals(rs.getTimestamp(3).getTime(), cal.getTimeInMillis());
            assertFalse(rs.next());

            StatementContext stmtContext = stmt.getQueryPlan().getContext();
            verifyTimeZoneIDWithFormatter(stmtContext.getDateFormatter(), timeZoneId);
            verifyTimeZoneIDWithFormatter(stmtContext.getTimeFormatter(), timeZoneId);
            verifyTimeZoneIDWithFormatter(stmtContext.getTimestampFormatter(), timeZoneId);

            stmt.close();
        } finally {
            conn1.close();
        }
    }

    private void verifyTimeZoneIDWithConn(PhoenixConnection conn, PDataType dataType, String timeZoneId) {
        Format formatter = conn.getFormatter(dataType);
        verifyTimeZoneIDWithFormatter(formatter, timeZoneId);
    }

    private void verifyTimeZoneIDWithFormatter(Format formatter, String timeZoneId) {
        assertTrue(formatter instanceof FastDateFormat);
        assertEquals(((FastDateFormat)formatter).getTimeZone().getID(), timeZoneId);
    }
}
