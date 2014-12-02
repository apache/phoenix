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

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ToDateFunctionIT extends BaseHBaseManagedTimeIT {

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

    private static Date callToDateFunction(Connection conn, String invocation) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format("SELECT %s FROM SYSTEM.CATALOG", invocation));
        assertTrue(rs.next());
        Date returnValue = rs.getDate(1);
        rs.close();
        stmt.close();
        return returnValue;
    }

    private Date callToDateFunction(String invocation) throws SQLException {
        return callToDateFunction(conn, invocation);
    }

    @Test
    public void testToDate_Default() throws SQLException {
        // Default time zone is GMT, so this is timestamp 0
        assertEquals(0L, callToDateFunction("TO_DATE('1970-01-01 00:00:00')").getTime());
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
                callToDateFunction(customTimeZoneConn, "TO_DATE('1970-01-01 00:00:00')").getTime());
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
}
