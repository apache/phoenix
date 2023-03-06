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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsEnabledTest.class)
public class TimeZoneDisplacementIT extends ParallelStatsEnabledIT {

    @Test
    public void testCompliantCorrection() throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.APPLY_TIME_ZONE_DISPLACMENT_ATTRIB, Boolean.TRUE.toString());
        try (PhoenixConnection conn =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
                PreparedStatement insertPstmt =
                        conn.prepareStatement("upsert into " + tableName
                                + " (ID, D1, D2, T1, T2, S1, S2, UD1, UD2, UT1, UT2, US1, US2) VALUES"
                                + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ")) {
            conn.setAutoCommit(true);
            stmt.executeUpdate("create table " + tableName + " (id integer primary key,"
                    + "D1 DATE, D2 DATE," + "T1 TIME, T2 TIME," + "S1 TIMESTAMP, S2 TIMESTAMP,"
                    + "UD1 UNSIGNED_DATE, UD2 UNSIGNED_DATE,"
                    + "UT1 UNSIGNED_TIME, UT2 UNSIGNED_TIME,"
                    + "US1 UNSIGNED_TIMESTAMP, US2 UNSIGNED_TIMESTAMP)");

            java.util.Date nowJud = new java.util.Date();
            java.sql.Date nowSqlDate = new java.sql.Date(nowJud.getTime());
            java.sql.Time nowSqlTime = new java.sql.Time(nowJud.getTime());
            java.sql.Timestamp nowSqlTimestamp = new java.sql.Timestamp(nowJud.getTime());

            String nowString =
                    DateUtil.getDateFormatter(conn.getDatePattern(), TimeZone.getDefault().getID())
                            .format(nowJud);

            insertPstmt.setInt(1, 1);
            insertPstmt.setString(2, nowString);
            insertPstmt.setDate(3, nowSqlDate);
            insertPstmt.setString(4, nowString);
            insertPstmt.setTime(5, nowSqlTime);
            insertPstmt.setString(6, nowString);
            insertPstmt.setTimestamp(7, nowSqlTimestamp);
            insertPstmt.setString(8, nowString);
            insertPstmt.setDate(9, nowSqlDate);
            insertPstmt.setString(10, nowString);
            insertPstmt.setTime(11, nowSqlTime);
            insertPstmt.setString(12, nowString);
            insertPstmt.setTimestamp(13, nowSqlTimestamp);
            insertPstmt.execute();

            ResultSet rs =
                    stmt.executeQuery("select * from " + tableName + " where "
                            + "D1 = D2 AND T1 = T2 and S1 = S2 and UD1 = UD2 and UT1 = UT2 and US1 = US2");
            assertTrue(rs.next());
            assertEquals(nowSqlDate, rs.getDate("D1"));
            assertEquals(nowSqlDate, rs.getDate("D2"));
            assertEquals(nowSqlTime, rs.getDate("T1"));
            assertEquals(nowSqlTime, rs.getDate("T2"));
            assertEquals(nowSqlTimestamp, rs.getTimestamp("S1"));
            assertEquals(nowSqlTimestamp, rs.getTimestamp("S2"));
            assertEquals(nowSqlDate, rs.getDate("UD1"));
            assertEquals(nowSqlDate, rs.getDate("UD2"));
            assertEquals(nowSqlTime, rs.getDate("UT1"));
            assertEquals(nowSqlTime, rs.getDate("UT2"));
            assertEquals(nowSqlTimestamp, rs.getTimestamp("US1"));
            assertEquals(nowSqlTimestamp, rs.getTimestamp("US2"));

            assertEquals(nowString, rs.getString("D1"));
            assertEquals(nowString, rs.getString("D2"));
            assertEquals(nowString, rs.getString("T1"));
            assertEquals(nowString, rs.getString("T2"));
            assertEquals(nowString, rs.getString("S1"));
            assertEquals(nowString, rs.getString("S2"));
            assertEquals(nowString, rs.getString("UD1"));
            assertEquals(nowString, rs.getString("UD2"));
            assertEquals(nowString, rs.getString("UT1"));
            assertEquals(nowString, rs.getString("UT2"));
            assertEquals(nowString, rs.getString("US1"));
            assertEquals(nowString, rs.getString("US2"));
        }
    }

    @Test
    public void testPhoenix5066() throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.APPLY_TIME_ZONE_DISPLACMENT_ATTRIB, Boolean.TRUE.toString());
        try (PhoenixConnection conn =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
                PreparedStatement insertPstmt =
                        conn.prepareStatement("upsert into " + tableName + " (ID, D, T, S) VALUES"
                                + " (?, ?, ?, ?) ")) {
            conn.setAutoCommit(true);
            stmt.executeUpdate("create table " + tableName + " (id integer primary key,"
                    + "D DATE, T TIME, S TIMESTAMP)");

            String dateString = "2018-12-10 15:40:47";
            java.util.Date dateJud =
                    new java.util.Date(LocalDateTime.parse(dateString.replace(" ", "T"))
                            .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
            java.sql.Date sqlDate = new java.sql.Date(dateJud.getTime());
            java.sql.Time sqlTime = new java.sql.Time(dateJud.getTime());
            java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(dateJud.getTime());

            stmt.executeUpdate("upsert into " + tableName + "(ID, D, T, S) VALUES (" + "1," + "'"
                    + dateString + "'," + "'" + dateString + "'," + "'" + dateString + "')");

            stmt.executeUpdate("upsert into " + tableName + "(ID, D, T, S) VALUES (" + "2,"
                    + "to_date('" + dateString + "')," + "to_time('" + dateString + "'),"
                    + "to_timestamp('" + dateString + "'))");

            insertPstmt.setInt(1, 3);
            insertPstmt.setDate(2, sqlDate);
            insertPstmt.setTime(3, sqlTime);
            insertPstmt.setTimestamp(4, sqlTimestamp);
            insertPstmt.executeUpdate();

            ResultSet rs = stmt.executeQuery("select * from " + tableName + " order by id asc");
            for (int i = 0; i < 3; i++) {
                assertTrue(rs.next());
                assertEquals(dateString + ".000", rs.getString("D"));
                assertEquals(dateString + ".000", rs.getString("T"));
                assertEquals(dateString + ".000", rs.getString("S"));
                assertEquals(sqlDate, rs.getDate("D"));
                assertEquals(sqlTime, rs.getTime("T"));
                assertEquals(sqlTimestamp, rs.getTimestamp("S"));
            }

        }
    }

    @Test
    public void testCurrent() throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // NOTE that this succeeds with either TRUE or FALSE, but the actual HBase value stored
        // is different
        // We are testing that the displacement gets applied both ways
        props.put(QueryServices.APPLY_TIME_ZONE_DISPLACMENT_ATTRIB, Boolean.TRUE.toString());
        try (PhoenixConnection conn =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();) {
            conn.setAutoCommit(true);
            stmt.executeUpdate("create table " + tableName + " (ID integer primary key,"
                    + " D DATE," + " T TIME," + " S TIMESTAMP," + " UD UNSIGNED_DATE,"
                    + " UT UNSIGNED_TIME," + " US UNSIGNED_TIMESTAMP)");

            java.util.Date nowJud = new java.util.Date();

            stmt.executeUpdate("UPSERT INTO " + tableName + " (ID, D, T, S, UD, UT, US) "
                    + "VALUES (1, CURRENT_DATE(), CURRENT_TIME(), CURRENT_DATE(),"
                    + "CURRENT_DATE(), CURRENT_TIME(), CURRENT_DATE())");

            ResultSet rs = stmt.executeQuery("select * from " + tableName + " order by id asc");
            assertTrue(rs.next());
            assertTrue(Math.abs(nowJud.getTime() - rs.getDate("D").getTime()) < 1000);
            assertTrue(Math.abs(nowJud.getTime() - rs.getTime("T").getTime()) < 1000);
            assertTrue(Math.abs(nowJud.getTime() - rs.getTimestamp("S").getTime()) < 1000);
            assertTrue(Math.abs(nowJud.getTime() - rs.getDate("UD").getTime()) < 1000);
            assertTrue(Math.abs(nowJud.getTime() - rs.getTime("UT").getTime()) < 1000);
            assertTrue(Math.abs(nowJud.getTime() - rs.getTimestamp("US").getTime()) < 1000);
        }
    }

    @Test
    public void testRowTimestamp() throws Exception {
        // The lack of DATE WITH TIMEZONE type in Phoenix causes the rowTimestamp function
        // to behave erratically with COMPLIANT_TIMEZONE_HANDLING

        // For normal operation HBase expects the TS to be a Instant-like UTC timestamp.
        // However we apply the TZ displacement on setting and retrieving Temporal objects from/to
        // java temporal types.

        // This can cause a lot of problems as if the HBase TS is displaced relative to UTC
        // by setting it explicitly to the current timestamp by setDate() or similar, then
        // The the valid value may not be actual latest TS if clients from more than one TZ
        // are writing the row.

        // When using ROW_TIMESTAMP with COMPLIANT_TIMEZONE_HANDLING, the ROWTS should always be
        // set from string or long types, NOT from java temporal types to avoid this.

        TimeZone tz = TimeZone.getDefault();

        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.APPLY_TIME_ZONE_DISPLACMENT_ATTRIB, Boolean.TRUE.toString());
        try (PhoenixConnection conn =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
                PreparedStatement upsertStmt =
                        conn.prepareStatement(
                            "upsert into " + tableName + " (ID, ROWTS, D) VALUES (?, ?, ?)");) {
            conn.setAutoCommit(true);
            stmt.executeUpdate(
                "create table " + tableName + " (ID integer not null," + " ROWTS DATE NOT NULL,"
                        + " D DATE," + " CONSTRAINT pk PRIMARY KEY (ID, ROWTS ROW_TIMESTAMP) )");

            String dateString = "2018-12-10 15:40:47";
            java.util.Date dateJudLocal =
                    new java.util.Date(LocalDateTime.parse(dateString.replace(" ", "T"))
                            .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
            java.sql.Date sqlDateLocal = new java.sql.Date(dateJudLocal.getTime());

            // Default ROWTS
            stmt.executeUpdate(
                "upsert into " + tableName + " (ID, D) VALUES (1, '" + dateString + "')");
            // Set ROWTS from literal, displacement doesn't apply
            stmt.executeUpdate("upsert into " + tableName + " (ID, ROWTS, D) VALUES (2, '"
                    + dateString + "','" + dateString + "')");
            // Set ROWTS from parameter, displacement DOES apply
            upsertStmt.setInt(1, 3);
            upsertStmt.setDate(2, sqlDateLocal);
            upsertStmt.setDate(3, sqlDateLocal);
            upsertStmt.executeUpdate();

            java.sql.Date nowDate = new java.sql.Date(new java.util.Date().getTime());
            upsertStmt.setInt(1, 4);
            upsertStmt.setDate(2, nowDate);
            upsertStmt.setDate(3, nowDate);
            upsertStmt.executeUpdate();

            ResultSet rs = stmt.executeQuery("select * from " + tableName + " order by id asc");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("ID"));
            assertEquals(sqlDateLocal, rs.getDate("D"));
            // UTC now() rowTs gets gets applied the displacement when read as java.sql.Date
            // This is NOT intuitive, but at least the TS contains the current UTC epoch.
            assertTrue(Math.abs(DateUtil
                    .applyOutputDisplacement(new java.sql.Date(new java.util.Date().getTime()), tz)
                    .getTime() - rs.getDate("ROWTS").getTime()) < 10000);

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("ID"));
            assertEquals(sqlDateLocal, rs.getDate("D"));
            // No displacement on write, because the date literal gets parsed as UTC.
            // So the HBase Ttimestamp is "2018-12-10 15:40:47 UTC"
            // getDate() applies the TZ displacement on read as normal
            // so we get "2018-12-10 15:40:47" parsed in the client TZ (which is sqlDate)
            assertEquals(sqlDateLocal, rs.getDate("ROWTS"));
            assertEquals(dateString + ".000", rs.getString("ROWTS"));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("ID"));
            assertEquals(sqlDateLocal, rs.getDate("D"));
            assertEquals(sqlDateLocal, rs.getDate("ROWTS"));
            // the stored timestamp is in UTC, but only because sqlDateLocal pre-applies the
            // displacement by parsing the date in the local TZ
            assertEquals(dateString + ".000", rs.getString("ROWTS"));

            assertTrue(rs.next());
            assertEquals(4, rs.getInt("ID"));
            assertEquals(nowDate, rs.getDate("D"));
            // The stored Hbase timestamp is NOT the current UTC timestamp. We have applied the
            // displacement on setting the date value.
            // This is very much not ideal behaviour, and probably NOT what the user wanted
            // in this case
            assertEquals(nowDate, rs.getDate("ROWTS"));
            // To further demonstrate that the HBase TS is NOT the current epoch value.
            assertEquals(DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT)
                    .format(DateUtil.applyInputDisplacement(nowDate, tz)),
                rs.getString("ROWTS"));
        }
    }
}
