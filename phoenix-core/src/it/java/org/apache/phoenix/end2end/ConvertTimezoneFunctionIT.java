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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.phoenix.exception.SQLExceptionCode;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

/**
 * CONVERT_TZ(date, 'from_timezone', 'to_timezone') tests
 *
 */

public class ConvertTimezoneFunctionIT extends ParallelStatsDisabledIT {

    @Test
    public void testDateConvertTimezoneEurope() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String timezone_offset_test = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
            + " (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + timezone_offset_test
            + " (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'Europe/Prague') FROM "
                + timezone_offset_test);

        assertTrue(rs.next());
        assertEquals(1393635600000L, rs.getDate(3).getTime()); //Sat, 01 Mar 2014 01:00:00
    }

    @Test
    public void testTimestampConvertTimezoneEurope() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String timezone_offset_test = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
            + " (k1 INTEGER NOT NULL, timestamps TIMESTAMP CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + timezone_offset_test
            + " (k1, timestamps) VALUES (1, TO_TIMESTAMP('2014-03-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT k1, timestamps, CONVERT_TZ(timestamps, 'UTC', 'Europe/Prague') FROM "
                + timezone_offset_test);

        assertTrue(rs.next());
        assertEquals(1393635600000L, rs.getDate(3).getTime()); //Sat, 01 Mar 2014 01:00:00
    }

    @Test
    public void testConvertTimezoneAmerica() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String timezone_offset_test = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
            + " (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + timezone_offset_test
            + " (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'America/Adak') FROM "
                + timezone_offset_test);

        assertTrue(rs.next());
        assertEquals(1393596000000L, rs.getDate(3).getTime()); //Fri, 28 Feb 2014 14:00:00
    }

    @Test
    public void nullInDateParameter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String timezone_offset_test = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
            + " (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + timezone_offset_test + " (k1) VALUES (1)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'America/Adak') FROM "
                + timezone_offset_test);

        assertTrue(rs.next());
        rs.getDate(3);
        assertTrue(rs.wasNull());
    }

    @Test
    public void nullInFirstTimezoneParameter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String timezone_offset_test = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
            + " (k1 INTEGER NOT NULL, dates DATE, tz VARCHAR, CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + timezone_offset_test
            + " (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT k1, dates, CONVERT_TZ(dates, tz, 'America/Adak') FROM " + timezone_offset_test);

        assertTrue(rs.next());
        rs.getDate(3);
        assertTrue(rs.wasNull());
    }

    @Test
    public void nullInSecondTimezoneParameter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String timezone_offset_test = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
            + " (k1 INTEGER NOT NULL, dates DATE, tz VARCHAR, CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + timezone_offset_test
            + " (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT k1, dates, CONVERT_TZ(dates, 'America/Adak', tz) FROM " + timezone_offset_test);

        assertTrue(rs.next());
        rs.getDate(3);
        assertTrue(rs.wasNull());
    }

    @Test
    public void unknownTimezone() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String timezone_offset_test = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
            + " (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + timezone_offset_test
            + " (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        try {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT k1, dates, CONVERT_TZ(dates, 'UNKNOWN_TIMEZONE', 'America/Adak') FROM "
                    + timezone_offset_test);

            rs.next();
            rs.getDate(3).getTime();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }

	@Test
	public void testConvertMultipleRecords() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
      String timezone_offset_test = generateUniqueName();
      String ddl = "CREATE TABLE IF NOT EXISTS " + timezone_offset_test
          + " (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		Statement stmt = conn.createStatement();
		stmt.execute(ddl);
		stmt.execute("UPSERT INTO " + timezone_offset_test
        + " (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))");
		stmt.execute("UPSERT INTO " + timezone_offset_test
        + " (k1, dates) VALUES (2, TO_DATE('2014-03-01 00:00:00'))");
		conn.commit();

		ResultSet rs = stmt.executeQuery(
        "SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'America/Adak') FROM " + timezone_offset_test);

		assertTrue(rs.next());
		assertEquals(1393596000000L, rs.getDate(3).getTime()); //Fri, 28 Feb 2014 14:00:00
		assertTrue(rs.next());
		assertEquals(1393596000000L, rs.getDate(3).getTime()); //Fri, 28 Feb 2014 14:00:00
		assertFalse(rs.next());
	}
}
