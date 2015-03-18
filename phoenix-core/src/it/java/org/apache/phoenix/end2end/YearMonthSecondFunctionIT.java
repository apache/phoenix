/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class YearMonthSecondFunctionIT extends BaseHBaseManagedTimeIT {
    private Connection conn;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(getUrl());
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
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

        assertEquals(2015, callYearFunction("YEAR(current_date())"));

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
        String ddl =
                "CREATE TABLE IF NOT EXISTS T1 (k1 INTEGER NOT NULL, dates DATE, timestamps TIMESTAMP, times TIME, " +
                        "unsignedDates UNSIGNED_DATE, unsignedTimestamps UNSIGNED_TIMESTAMP, unsignedTimes UNSIGNED_TIME CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO T1 VALUES (1, TO_DATE('2004-03-01 00:00:00'), TO_TIMESTAMP('2006-02-01 00:00:00'), TO_TIME('2008-02-01 00:00:00'), " +
                "TO_DATE('2010-03-01 00:00:00'), TO_TIMESTAMP('2012-02-01'), TO_TIME('2015-02-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, YEAR(timestamps), YEAR(times), Year(unsignedDates), YEAR(unsignedTimestamps), " +
                "YEAR(unsignedTimes) FROM T1 where YEAR(dates) = 2004");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2006, rs.getInt(2));
        assertEquals(2008, rs.getInt(3));
        assertEquals(2010, rs.getInt(4));
        assertEquals(2012, rs.getInt(5));
        assertEquals(2015, rs.getInt(6));
        assertFalse(rs.next());
    }
}