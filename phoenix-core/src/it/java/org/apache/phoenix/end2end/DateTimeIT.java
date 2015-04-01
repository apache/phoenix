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

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.MILLIS_IN_DAY;
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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.text.Format;
import java.util.Calendar;

import org.apache.phoenix.util.DateUtil;
import org.junit.Test;


public class DateTimeIT extends BaseHBaseManagedTimeIT {

    protected Connection conn;
    protected Date date;
    protected static final String tenantId = getOrganizationId();

    public DateTimeIT() throws Exception {
        super();
        conn = DriverManager.getConnection(getUrl());
        date = new Date(System.currentTimeMillis());
        initAtable();
    }

    protected void initAtable() throws Exception { 
        ensureTableCreated(getUrl(), ATABLE_NAME, (byte[][])null);
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " + ATABLE_NAME +
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

        conn.commit();
    }

    @Test
    public void selectBetweenDates() throws Exception {
        Format formatter = DateUtil.getDateFormatter("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        java.util.Date dateToday = cal.getTime();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        java.util.Date dateTomorrow = cal.getTime();
        String today = formatter.format(dateToday);
        String tomorrow = formatter.format(dateTomorrow);
        String query = "SELECT entity_id FROM ATABLE WHERE a_integer < 4 AND a_date BETWEEN date '" + today + "' AND date '" + tomorrow + "' ";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(ROW1, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectLiteralDate() throws Exception {
        String s = DateUtil.DEFAULT_DATE_FORMATTER.format(date);
        String query = "SELECT DATE '" + s + "' FROM ATABLE";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(date, rs.getDate(1));
    }

    @Test
    public void testSelectLiteralDateCompare() throws Exception {
        String query = "SELECT (DATE '" + date + "' = DATE '" + date + "') FROM ATABLE";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    public void testSelectWhereDatesEqual() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE  a_integer < 4 AND DATE '" + date + "' = DATE '" + date + "'";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());

    }

    @Test
    public void testSelectWhereDateAndToDateEqual() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE  a_integer < 4 AND DATE '" + date + "' = TO_DATE ('" + date + "')";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());

    }

    @Test
    public void testSelectWhereDateAndTimestampEqual() throws Exception {
        final String timestamp = "2012-09-08 07:08:23";
        String query = "SELECT entity_id FROM ATABLE WHERE  a_integer < 4 AND DATE '" + timestamp + "' = TIMESTAMP '" + timestamp + "'";

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertTrue(rs.next());
    }

    @Test
    public void testSelectWhereSameDatesUnequal() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE  a_integer < 4 AND DATE '" + date + "' > DATE '" + date + "'";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);
        assertFalse(rs.next());
    }

    @Test
    public void testDateInList() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE a_date IN (?,?) AND a_integer < 4";
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
        String query = "SELECT entity_id FROM ATABLE WHERE a_integer < 4 AND a_date BETWEEN date '" + today + "' AND date '" + tomorrow + "' ";
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
    }
}
