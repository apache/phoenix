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

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TypeMismatchException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;


public class ToDateFunctionIT extends ParallelStatsDisabledIT {

    private static final long ONE_HOUR_IN_MILLIS = 1000L * 60L * 60L;

    private Connection conn;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(getUrl());
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    private static java.util.Date callToDateFunction(Connection conn, String invocation) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format("SELECT %s FROM \"SYSTEM\".CATALOG LIMIT 1", invocation));
        assertTrue(rs.next());
        java.util.Date returnValue = (java.util.Date)rs.getObject(1);
        rs.close();
        stmt.close();
        return returnValue;
    }

    private Date callToDateFunction(String invocation) throws SQLException {
        return (Date)callToDateFunction(conn, invocation);
    }

    private Time callToTimeFunction(String invocation) throws SQLException {
        return (Time)callToDateFunction(conn, invocation);
    }

    private Timestamp callToTimestampFunction(String invocation) throws SQLException {
        return (Timestamp)callToDateFunction(conn, invocation);
    }

    @Test
    public void testToDate_Default() throws SQLException {
        // Default time zone is GMT, so this is timestamp 0
        assertEquals(0L, callToDateFunction("TO_DATE('1970-01-01 00:00:00')").getTime());
        assertEquals(0L, callToDateFunction("TO_DATE('1970-01-01 00:00:00.000')").getTime());
        assertEquals(0L, callToDateFunction("TO_DATE('1970-01-01')").getTime());
        assertEquals(0L, callToDateFunction("TO_DATE('1970/01/01','yyyy/MM/dd')").getTime());

        // Test other ISO 8601 Date Compliant Formats to verify they can be parsed
        try {
            callToDateFunction("TO_DATE('2015-01-27T16:17:57+00:00')");
            callToDateFunction("TO_DATE('2015-01-27T16:17:57Z')");
            callToDateFunction("TO_DATE('2015-W05')");
            callToDateFunction("TO_DATE('2015-W05-2')");
        } catch (Exception ex) {
            fail("TO_DATE Parse ISO8601 Time Failed due to:" + ex);
        }
    }

    @Test
    public void testToTime_Default() throws SQLException {
        // Default time zone is GMT, so this is timestamp 0
        assertEquals(0L, callToTimeFunction("TO_TIME('1970-01-01 00:00:00')").getTime());
        assertEquals(0L, callToTimeFunction("TO_TIME('1970-01-01 00:00:00.000')").getTime());
        assertEquals(0L, callToTimeFunction("TO_TIME('1970-01-01')").getTime());
        assertEquals(0L, callToTimeFunction("TO_TIME('1970/01/01','yyyy/MM/dd')").getTime());

        // Test other ISO 8601 Date Compliant Formats to verify they can be parsed
        try {
            callToTimeFunction("TO_TIME('2015-01-27T16:17:57+00:00')");
            callToTimeFunction("TO_TIME('2015-01-27T16:17:57Z')");
            callToTimeFunction("TO_TIME('2015-W05')");
            callToTimeFunction("TO_TIME('2015-W05-2')");
        } catch (Exception ex) {
            fail("TO_TIME Parse ISO8601 Time Failed due to:" + ex);
        }
    }

    @Test
    public void testToTimestamp_Default() throws SQLException {
        // Default time zone is GMT, so this is timestamp 0
        assertEquals(0L, callToTimestampFunction("TO_TIMESTAMP('1970-01-01 00:00:00')").getTime());
        assertEquals(0L, callToTimestampFunction("TO_TIMESTAMP('1970-01-01 00:00:00.000')").getTime());
        assertEquals(0L, callToTimestampFunction("TO_TIMESTAMP('1970-01-01')").getTime());
        assertEquals(0L, callToTimestampFunction("TO_TIMESTAMP('1970/01/01','yyyy/MM/dd')").getTime());

        // Test other ISO 8601 Date Compliant Formats to verify they can be parsed
        try {
            callToTimestampFunction("TO_TIMESTAMP('2015-01-27T16:17:57+00:00')");
            callToTimestampFunction("TO_TIMESTAMP('2015-01-27T16:17:57Z')");
            callToTimestampFunction("TO_TIMESTAMP('2015-W05')");
            callToTimestampFunction("TO_TIMESTAMP('2015-W05-2')");
        } catch (Exception ex) {
            fail("TO_TIMESTAMP Parse ISO8601 Time Failed due to:" + ex);
        }
    }

    @Test
    public void testToDate_CustomDateFormat() throws SQLException {
        // A date without time component is at midnight
        assertEquals(0L, callToDateFunction("TO_DATE('1970-01-01', 'yyyy-MM-dd')").getTime());
    }

    @Test
    public void testToDate_CustomTimeZone() throws SQLException {
        // We're using GMT+1, so that's an hour before the Java epoch
        assertEquals(
                -ONE_HOUR_IN_MILLIS,
                callToDateFunction("TO_DATE('1970-01-01', 'yyyy-MM-dd', 'GMT+1')").getTime());
    }

    @Test
    public void testToDate_LocalTimeZone() throws SQLException {
        assertEquals(
                Date.valueOf("1970-01-01"),
                callToDateFunction("TO_DATE('1970-01-01', 'yyyy-MM-dd', 'local')"));
    }

    @Test
    public void testToDate_CustomTimeZoneViaQueryServices() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB, "GMT+1");
        Connection customTimeZoneConn = DriverManager.getConnection(getUrl(), props);

        assertEquals(
                -ONE_HOUR_IN_MILLIS,
                callToDateFunction(customTimeZoneConn, "TO_DATE('1970-01-01 00:00:00.000')").getTime());
    }

    @Test
    public void testToDate_CustomTimeZoneViaQueryServicesAndCustomFormat() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB, "GMT+1");
        Connection customTimeZoneConn = DriverManager.getConnection(getUrl(), props);

        assertEquals(
                -ONE_HOUR_IN_MILLIS,
                callToDateFunction(
                        customTimeZoneConn, "TO_DATE('1970-01-01', 'yyyy-MM-dd')").getTime());
    }
    
    @Test
    public void testTimestampCast() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB, "GMT+1");
        Connection customTimeZoneConn = DriverManager.getConnection(getUrl(), props);

        assertEquals(
            1426188807198L,
                callToDateFunction(
                        customTimeZoneConn, "CAST(1426188807198 AS TIMESTAMP)").getTime());
        

        try {
            callToDateFunction(
                    customTimeZoneConn, "CAST(22005 AS TIMESTAMP)");
            fail();
        } catch (TypeMismatchException e) {

        }
    }
    
    @Test
    public void testUnsignedLongToTimestampCast() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB, "GMT+1");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            conn.prepareStatement(
                "create table TT("
                        + "a unsigned_int not null, "
                        + "b unsigned_int not null, "
                        + "ts unsigned_long not null "
                        + "constraint PK primary key (a, b, ts))").execute();
            conn.commit();

            conn.prepareStatement("upsert into TT values (0, 22120, 1426188807198)").execute();
            conn.commit();
            
            ResultSet rs = conn.prepareStatement("select a, b, ts, CAST(ts AS TIMESTAMP) from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals(new Date(1426188807198L), rs.getObject(4));
            rs.close();

            try {
                rs = conn.prepareStatement("select a, b, ts, CAST(b AS TIMESTAMP) from TT").executeQuery();
                fail();
            } catch (TypeMismatchException e) {

            }

        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarcharToDateComparision() throws SQLException {
        final String dateString1 = "1900-01-02";
        final String dateString2 = "2100-01-01";

        conn.prepareStatement(
                "CREATE TABLE SB(" +
                        "DATE_STRING VARCHAR(50) NOT NULL " +
                        "CONSTRAINT PK PRIMARY KEY (DATE_STRING))").execute();
        conn.commit();

        String upsertSql = String.format("upsert into SB values (?)");
        PreparedStatement stmt = conn.prepareStatement(upsertSql);
        stmt.setString(1, dateString1);
        stmt.execute();
        stmt.setString(1, dateString2);
        stmt.execute();
        conn.commit();

        String selectSql = "SELECT DATE_STRING FROM SB WHERE TO_DATE(DATE_STRING) > TO_DATE('2001-01-01')";
        ResultSet rs = conn.prepareStatement(selectSql).executeQuery();
        assertTrue(rs.next());
        String obtainedString = rs.getString("DATE_STRING");
        assertEquals("Did not get value that was inserted!!", dateString2, obtainedString);
        assertFalse("No more rows expected!!", rs.next());
    }
    
    @Test
    public void testToDateWithCloneMethod() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
    	String ddl = "create table " + tableName + " (k varchar primary key, v varchar[])";
        conn.createStatement().execute(ddl);
        String dateStr = "2100-01-01";
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('x',ARRAY['"+dateStr+"'])");
        conn.commit();
        ResultSet rs = conn.createStatement().executeQuery("select to_date(v[1], 'yyyy-MM-dd', 'local') from " + tableName);
        
        assertTrue(rs.next());
        assertEquals("Unexpected value for date ", Date.valueOf(dateStr), rs.getDate(1));
        assertFalse(rs.next());
    }
}
