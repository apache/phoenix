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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;


public class TimezoneOffsetFunctionIT extends BaseHBaseManagedTimeIT {

	@Test
	public void testTimezoneOffset() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST"
				+ " (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-02-02 00:00:00'))";
		conn.createStatement().execute(dml);
		dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (2, TO_DATE('2014-06-02 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, TIMEZONE_OFFSET('Indian/Cocos', dates) FROM TIMEZONE_OFFSET_TEST");

		assertTrue(rs.next());
		assertEquals(390, rs.getInt(3));
		assertTrue(rs.next());
		assertEquals(390, rs.getInt(3));

	}

	@Test
	public void testUnknownTimezone() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST"
				+ " (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-02-02 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		try {
			ResultSet rs = conn.createStatement().executeQuery(
					"SELECT k1, dates, TIMEZONE_OFFSET('Unknown_Timezone', dates) FROM TIMEZONE_OFFSET_TEST");

			rs.next();
			assertEquals(0, rs.getInt(3));
			fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
	}

	@Test
	public void testInRowKeyDSTTimezoneDesc() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST "
				+ "(k1 INTEGER NOT NULL, dates DATE NOT NULL CONSTRAINT pk PRIMARY KEY (k1, dates DESC))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-02-02 00:00:00'))";
		conn.createStatement().execute(dml);
		dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (2, TO_DATE('2014-06-02 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, TIMEZONE_OFFSET('Europe/Prague', dates)"
				+ "FROM TIMEZONE_OFFSET_TEST ORDER BY k1 ASC");

		assertTrue(rs.next());
		assertEquals(60, rs.getInt(3));
		assertTrue(rs.next());
		assertEquals(120, rs.getInt(3));
	}

	@Test
	public void testBothParametersNull() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST "
				+ "(k1 INTEGER NOT NULL, dates DATE, v1 VARCHAR CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates, v1) VALUES (2, null, null)";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, TIMEZONE_OFFSET(v1, dates)"
				+ "FROM TIMEZONE_OFFSET_TEST ORDER BY k1 ASC");

		assertTrue(rs.next());
		rs.getInt(3);
		assertTrue(rs.wasNull());
	}

	@Test
	public void timezoneParameterNull() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST "
				+ "(k1 INTEGER NOT NULL, dates DATE, v1 VARCHAR CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates, v1) VALUES (2, TO_DATE('2014-06-02 00:00:00'), null)";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, TIMEZONE_OFFSET(v1, dates)"
				+ "FROM TIMEZONE_OFFSET_TEST ORDER BY k1 ASC");

		assertTrue(rs.next());
		rs.getInt(3);
		assertTrue(rs.wasNull());
	}

	@Test
	public void dateParameterNull() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST "
				+ "(k1 INTEGER NOT NULL, dates DATE, v1 VARCHAR CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates, v1) VALUES (2, null, 'Asia/Aden')";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, TIMEZONE_OFFSET(v1, dates)"
				+ "FROM TIMEZONE_OFFSET_TEST ORDER BY k1 ASC");

		assertTrue(rs.next());
		rs.getInt(3);
		assertTrue(rs.wasNull());
	}
	
	@Test
	public void testInsertingRetrivingTimestamp() throws Exception {
	    Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE T (K INTEGER NOT NULL PRIMARY KEY, V TIMESTAMP)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO T VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("US/Hawaii"));
        long time = System.currentTimeMillis();
        stmt.setTimestamp(2, new Timestamp(time), cal);
        stmt.executeUpdate();
        conn.commit();
        String query = "SELECT V FROM T";
        ResultSet rs = conn.createStatement().executeQuery(query);
        rs.next();
        assertEquals(new Timestamp(time), rs.getTimestamp(1));
        assertEquals(new Timestamp(time), rs.getTimestamp("V"));
        assertEquals(new Timestamp(time), rs.getTimestamp(1, cal));
        assertEquals(new Timestamp(time), rs.getTimestamp("V", cal));
    }

}
