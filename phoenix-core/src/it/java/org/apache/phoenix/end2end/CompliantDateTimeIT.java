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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.DeleteCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.UpsertCompiler.ClientUpsertSelectMutationPlan;
import org.apache.phoenix.compile.UpsertCompiler.ServerUpsertSelectMutationPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import junit.framework.AssertionFailedError;

@Category(ParallelStatsDisabledTest.class)
public class CompliantDateTimeIT extends ParallelStatsDisabledIT {

    // FIXME ADD NANOSECONDS
    // Using standard APIs to setup to reduce the the chance of errors cancelling out each other
    private static final ZoneId EST_ZONE_ID = ZoneId.of("America/New_York", ZoneId.SHORT_IDS);
    private static final ZoneId JST_ZONE_ID = ZoneId.of("JST", ZoneId.SHORT_IDS);

    private static final java.time.Instant TS1_INSTANT =
            java.time.Instant.parse("2020-01-01T13:30:00.123456789Z");
    private static final long TS1_INSTANT_MS_NUMERIC = TS1_INSTANT.getEpochSecond()*1000 + TS1_INSTANT.getNano()/1000000;
    private static final java.sql.Timestamp TS1_TS = Timestamp.from(TS1_INSTANT);
    private static final java.sql.Timestamp TS1_TS_MS;
    private static final LocalDateTime TS1_LD_EST =
            LocalDateTime.ofInstant(TS1_INSTANT, EST_ZONE_ID);
    private static final DateTimeFormatter EST_LOCAL_FORMATTER =
            DateTimeFormatter.ISO_DATE.withZone(EST_ZONE_ID);
    private static final String TS1_EST_STR_ISO = TS1_LD_EST.toString();
    private static final String TS1_EST_STR = TS1_EST_STR_ISO.replaceAll("T", " ");
    private static final String TS1_EST_MS_STR;

    
    private static final LocalDateTime TS1_LD_JST =
            LocalDateTime.ofInstant(TS1_INSTANT, JST_ZONE_ID);
    private static final String TS1_JST_STR_ISO = TS1_LD_JST.toString();
    private static final String TS1_JST_STR = TS1_JST_STR_ISO.replaceAll("T", " ");
    private static final String TS1_JST_MS_STR;
    
    private static final String UNDERSCORE_FORMAT = QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT.replaceAll(" ","___");
    private static final String TS1_EST_STR_UNDERSCORE = TS1_EST_STR_ISO.replaceAll("T", "___");
    private static final String TS1_EST_MS_STR_UNDERSCORE;

    private static final int SEPARATION_TEST_ROWCNT = 500;
    private static final int SEPARATION_TEST_OFFSET = 1000000;

    private static boolean contextSeparationTestFailed = false;
    
    static {
        // No fluent API
        TS1_TS_MS = (java.sql.Timestamp) TS1_TS.clone();
        TS1_TS_MS.setNanos((TS1_TS.getNanos() / 1000000) * 1000000);
        TS1_EST_MS_STR = TS1_LD_EST.withNano((TS1_LD_EST.getNano() / 1000000) * 1000000).toString().replaceAll("T", " ");
        TS1_EST_MS_STR_UNDERSCORE = TS1_LD_EST.withNano((TS1_LD_EST.getNano() / 1000000) * 1000000).toString().replaceAll("T", "___");
        
        TS1_JST_MS_STR = TS1_LD_JST.withNano((TS1_LD_JST.getNano() / 1000000) * 1000000).toString().replaceAll("T", " ");
    }

    private Connection getConn() throws SQLException {
        return getConn(null);
    }

    private Connection getConn(String tz) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.JDBC_COMPLIANT_TZ_HANDLING, "true");
        if (tz != null) {
            props.setProperty(QueryServices.TIMEZONE_OVERRIDE, tz);
        }
        return DriverManager.getConnection(url, props);
    }

    private String insertStr(String tableName, int id, String d1, String d2, String d3, String d4,
            String d5, String d6) {
        String insertFormat = "UPSERT INTO " + tableName + " VALUES ( %d, %s, %s, %s, %s, %s, %s )";
        return String.format(insertFormat, id, d1, d2, d3, d4, d5, d6);
    }

    private String insertStr(String tableName, int id, String date) {
        return insertStr(tableName, id, date, date, date, date, date, date);
    }

    private Result getRowForIntPK(Table t, int pk) throws IOException {
        return t.get(
            new Get(PInteger.INSTANCE.toBytes(PInteger.INSTANCE.toObject(Integer.toString(pk)))));
    }

    private byte[] getEncodedCellValue(Result r, int encodedQ) {
        Cell c =
                r.getColumnCells(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                    PSmallint.INSTANCE
                            .toBytes(PSmallint.INSTANCE.toObject(Integer.toString(encodedQ))))
                        .get(0);
        return Bytes.copy(c.getValueArray(), c.getValueOffset(), c.getValueLength());
    }

    private void checkValues(Table t, int id, java.sql.Timestamp ts) throws IOException {
        checkValues(t, id, ts, ts, ts, ts, ts, ts);
    }

    private void checkValues(Table t, int id, java.sql.Timestamp ts1, java.sql.Timestamp ts2,
            java.sql.Timestamp ts3, java.sql.Timestamp ts4, java.sql.Timestamp ts5,
            java.sql.Timestamp ts6) throws IOException {

        Result r = getRowForIntPK(t, id);
        assertTrue(r.size() > 0);

        if (ts1 != null) {
            byte[] tsBytes = getEncodedCellValue(r, 11);
            assertEquals(12, tsBytes.length);
            assertTrue("cell:" + Bytes.toStringBinary(tsBytes) +" expected:" + Bytes.toStringBinary(PTimestamp.INSTANCE.toBytes(ts1)), Bytes.equals(tsBytes, PTimestamp.INSTANCE.toBytes(ts1)));
        }
        if (ts2 != null) {
            byte[] dateBytes = getEncodedCellValue(r, 12);
            assertEquals(8, dateBytes.length);
            assertTrue("cell:" + Bytes.toStringBinary(dateBytes) +" expected:" + Bytes.toStringBinary(PDate.INSTANCE.toBytes(ts2)), Bytes.equals(dateBytes, PDate.INSTANCE.toBytes(ts2)));
        }
        if (ts3 != null) {
            byte[] timeBytes = getEncodedCellValue(r, 13);
            assertEquals(8, timeBytes.length);
            assertTrue("cell:" + Bytes.toStringBinary(timeBytes) +" expected:" + Bytes.toStringBinary(PTime.INSTANCE.toBytes(ts3)), Bytes.equals(timeBytes, PTime.INSTANCE.toBytes(ts3)));
        }
        if (ts4 != null) {
            byte[] utsBytes = getEncodedCellValue(r, 14);
            assertEquals(12, utsBytes.length);
            assertTrue("cell:" + Bytes.toStringBinary(utsBytes) +" expected:" + Bytes.toStringBinary(PUnsignedTimestamp.INSTANCE.toBytes(ts4)), Bytes.equals(utsBytes, PUnsignedTimestamp.INSTANCE.toBytes(ts4)));
        }
        if (ts5 != null) {
            byte[] udateBytes = getEncodedCellValue(r, 15);
            assertEquals(8, udateBytes.length);
            assertTrue("cell:" + Bytes.toStringBinary(udateBytes) +" expected:" + Bytes.toStringBinary(PUnsignedDate.INSTANCE.toBytes(ts5)), Bytes.equals(udateBytes, PUnsignedDate.INSTANCE.toBytes(ts5)));
        }
        if (ts6 != null) {
            byte[] utimeBytes = getEncodedCellValue(r, 16);
            assertEquals(8, utimeBytes.length);
            assertTrue("cell:" + Bytes.toStringBinary(utimeBytes) +" expected:" + Bytes.toStringBinary(PUnsignedTime.INSTANCE.toBytes(ts6)), Bytes.equals(utimeBytes, PUnsignedTime.INSTANCE.toBytes(ts6)));
        }
    }

    @Test
    public void testUpsertTZ() throws SQLException, IOException {
        // Test all methods of writing directly to the table
        String timeZone = "America/New_York";

        try (Connection conn = getConn(timeZone);
                Statement stmt = conn.createStatement()) {
            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( ID integer primary key, "
                    + " TIMESTAMPFIELD TIMESTAMP," + " DATEFIELD DATE," + " TIMEFIELD TIME,"
                    + " UTIMESTAMPFIELD UNSIGNED_TIMESTAMP, " + " UDATEFIELD UNSIGNED_DATE, "
                    + " UTIMEFIELD UNSIGNED_TIME )");

            Table hTable =
                    ((PhoenixConnection) conn).getQueryServices()
                            .getTable(TableName.valueOf(tableName).toBytes());

            int rowId = 0;
            
            // Insert timestamp object via Timestamp in PreparedStatement (should be
            // timezone-independent)
            PreparedStatement ps =
                    conn.prepareStatement(
                        "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?, ?)");
            ps.setInt(1, ++rowId);
            ps.setTimestamp(2, TS1_TS);
            ps.setTimestamp(3, TS1_TS);
            ps.setTimestamp(4, TS1_TS);
            ps.setTimestamp(5, TS1_TS);
            ps.setTimestamp(6, TS1_TS);
            ps.setTimestamp(7, TS1_TS);
            ps.executeUpdate();
            conn.commit();
            checkValues(hTable, rowId, TS1_TS);

            // Insert object via String literal
            stmt.executeUpdate(insertStr(tableName, ++rowId, "'" + TS1_EST_STR + "'"));
            conn.commit();
            checkValues(hTable, rowId, TS1_TS);

            // Insert object via Timestamp literal
            stmt.executeUpdate(insertStr(tableName, ++rowId, "TIMESTAMP '" + TS1_EST_STR + "'"));
            conn.commit();
            //nanosecond resolution
            checkValues(hTable, rowId, TS1_TS);
            
            // Insert object via Date literal
            stmt.executeUpdate(insertStr(tableName, ++rowId, "DATE '" + TS1_EST_STR + "'"));
            conn.commit();
            checkValues(hTable, rowId, TS1_TS_MS);
            
            // Insert object via Time literal
            stmt.executeUpdate(insertStr(tableName, ++rowId, "TIME '" + TS1_EST_STR + "'"));
            conn.commit();
            checkValues(hTable, rowId, TS1_TS_MS);

            
            // Insert object via TO_TIMESTAMP with default
            // Timestamp is not coercible to Date or Time
            String toTimestampNoTz = "TO_TIMESTAMP('" + TS1_EST_STR + "')";
            stmt.executeUpdate(
                insertStr(tableName, ++rowId, toTimestampNoTz, null, null, toTimestampNoTz, null, null));
            conn.commit();
            // FIXME I'm pretty sure that we're testing for bad behaviour See PHOENIX-6792
            // These should have nanosecond resolution
            checkValues(hTable, rowId, TS1_TS_MS, null, null, TS1_TS_MS, null, null);

            // Insert object via TO_TIME with default
            String toTimeNoTz = "TO_TIME('" + TS1_EST_STR + "')";
            stmt.executeUpdate(insertStr(tableName, ++rowId, toTimeNoTz));
            conn.commit();
            //millisecond resolution
            checkValues(hTable, rowId, TS1_TS_MS);

            // Insert object via TO_DATE with default
            String toDateNoTz = "TO_DATE('" + TS1_EST_STR + "')";
            stmt.executeUpdate(insertStr(tableName, ++rowId, toDateNoTz));
            conn.commit();
            //millisecond resolution
            checkValues(hTable, rowId, TS1_TS_MS);
            
            
            // Insert object via TO_TIMESTAMP with JST TZ
            // Timestamp is not coercible to Date or Time
            String toTimestampJst = "TO_TIMESTAMP('" + TS1_JST_STR + "', '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST')";
            stmt.executeUpdate(
                insertStr(tableName, ++rowId, toTimestampJst, null, null, toTimestampJst, null, null));
            conn.commit();
            // FIXME I'm pretty sure that we're testing for bad behaviour See PHOENIX-6792
            // These should have nanosecond resolution
            checkValues(hTable, rowId, TS1_TS_MS, null, null, TS1_TS_MS, null, null);

            // Insert object via TO_TIME with JST TZ
            String toTimeJst = "TO_TIME('" + TS1_JST_STR + "', '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST')";
            stmt.executeUpdate(insertStr(tableName, ++rowId, toTimeJst));
            conn.commit();
            //millisecond resolution
            checkValues(hTable, rowId, TS1_TS_MS);

            // Insert object via TO_DATE with JST TZ
            String toDateJst = "TO_DATE('" + TS1_JST_STR + "', '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST')";
            stmt.executeUpdate(insertStr(tableName, ++rowId, toDateJst));
            conn.commit();
            //millisecond resolution
            checkValues(hTable, rowId, TS1_TS_MS);

            // Insert object via TO_TIMESTAMP with custom format
            // Timestamp is not coercible to Date or Time
            String toTimestampXYZ = "TO_TIMESTAMP('" + TS1_EST_STR_UNDERSCORE + "', '"+UNDERSCORE_FORMAT+"')";
            stmt.executeUpdate(
                insertStr(tableName, ++rowId, toTimestampXYZ, null, null, toTimestampXYZ, null, null));
            conn.commit();
            checkValues(hTable, rowId, TS1_TS_MS, null, null, TS1_TS_MS, null, null);

            // Insert object via TO_TIME with with custom format
            String toTimeXyz = "TO_TIME('" + TS1_EST_STR_UNDERSCORE + "', '"+UNDERSCORE_FORMAT+"')";
            stmt.executeUpdate(insertStr(tableName, ++rowId, toTimeXyz));
            conn.commit();
            //millisecond resolution
            checkValues(hTable, rowId, TS1_TS_MS);

            // Insert object via TO_DATE with with custom format
            String toDateXyz = "TO_DATE('" + TS1_EST_STR_UNDERSCORE + "', '"+UNDERSCORE_FORMAT+"')";
            stmt.executeUpdate(insertStr(tableName, ++rowId, toDateXyz));
            conn.commit();
            //millisecond resolution
            checkValues(hTable, rowId, TS1_TS_MS);
            
            // Insert object via numeric timestamp
            String epochMs = Long.toString(TS1_INSTANT_MS_NUMERIC);
            stmt.executeUpdate(insertStr(tableName, ++rowId, epochMs));
            conn.commit();
            //millisecond resolution
            checkValues(hTable, rowId, TS1_TS_MS);

        }
    }

    @Test
    public void testSelectTZ() throws SQLException, IOException {
        // Test all methods of selecting from the table
        String timeZone = "America/New_York";

        try (Connection conn = getConn(timeZone); Statement stmt = conn.createStatement()) {
            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( ID integer primary key, "
                    + " TIMESTAMPFIELD TIMESTAMP," + " DATEFIELD DATE," + " TIMEFIELD TIME,"
                    + " UTIMESTAMPFIELD UNSIGNED_TIMESTAMP, " + " UDATEFIELD UNSIGNED_DATE, "
                    + " UTIMEFIELD UNSIGNED_TIME, TIMESTAMPSTRINGFIELD VARCHAR, "
                    + " DATESTRINGFIELD VARCHAR, TIMESTRINGFIELD VARCHAR, NUMERICFIELD DECIMAL )");

            Table hTable =
                    ((PhoenixConnection) conn).getQueryServices()
                            .getTable(TableName.valueOf(tableName).toBytes());

            int rowId = 0;

            // Insert timestamp object via Timestamp in PreparedStatement (should be
            // timezone-independent)
            PreparedStatement ps =
                    conn.prepareStatement(
                        "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            ps.setInt(1, ++rowId);
            ps.setTimestamp(2, TS1_TS);
            ps.setTimestamp(3, TS1_TS);
            ps.setTimestamp(4, TS1_TS);
            ps.setTimestamp(5, TS1_TS);
            ps.setTimestamp(6, TS1_TS);
            ps.setTimestamp(7, TS1_TS);
            ps.setString(8, TS1_EST_STR);
            ps.setString(9, TS1_EST_STR);
            ps.setString(10, TS1_EST_STR);
            ps.executeUpdate();
            conn.commit();
            checkValues(hTable, rowId, TS1_TS);
            
            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals(rs.getTimestamp("TIMESTAMPFIELD"), TS1_TS);
            assertEquals(rs.getDate("DATEFIELD"), TS1_TS_MS);
            assertEquals(rs.getTime("TIMEFIELD"), TS1_TS_MS);
            assertEquals(rs.getTimestamp("UTIMESTAMPFIELD"), TS1_TS);
            assertEquals(rs.getDate("UDATEFIELD"), TS1_TS_MS);
            assertEquals(rs.getTime("UTIMEFIELD"), TS1_TS_MS);

            //FIXME this should have nanosecond resolution
            assertEquals(rs.getString("TIMESTAMPFIELD"), TS1_EST_MS_STR);
            assertEquals(rs.getString("DATEFIELD"), TS1_EST_MS_STR);
            assertEquals(rs.getString("TIMEFIELD"), TS1_EST_MS_STR);
            //FIXME this should have nanosecond resolution
            assertEquals(rs.getString("UTIMESTAMPFIELD"), TS1_EST_MS_STR);
            assertEquals(rs.getString("UDATEFIELD"), TS1_EST_MS_STR);
            assertEquals(rs.getString("UTIMEFIELD"), TS1_EST_MS_STR);

            rs.close();
            rs = stmt.executeQuery("select TO_CHAR(TIMESTAMPFIELD), TO_CHAR(DATEFIELD), TO_CHAR(TIMEFIELD),TO_CHAR(UTIMESTAMPFIELD), TO_CHAR(UDATEFIELD), TO_CHAR(UTIMEFIELD) from " + tableName);
            assertTrue(rs.next());
            //FIXME this should have nanosecond resolution
            assertEquals(rs.getString(1), TS1_EST_MS_STR);
            assertEquals(rs.getString(2), TS1_EST_MS_STR);
            assertEquals(rs.getString(3), TS1_EST_MS_STR);
            //FIXME this should have nanosecond resolution
            assertEquals(rs.getString(4), TS1_EST_MS_STR);
            assertEquals(rs.getString(5), TS1_EST_MS_STR);
            assertEquals(rs.getString(6), TS1_EST_MS_STR);

            rs.close();
            rs = stmt.executeQuery("select TO_TIMESTAMP(TO_CHAR(TIMESTAMP '"+TS1_EST_STR+"')), TO_TIMESTAMP(TO_CHAR(TIMESTAMP '"+TS1_EST_STR+"')), TO_TIMESTAMP(TO_CHAR(TIMESTAMP '"+TS1_EST_STR+"')), TO_TIMESTAMP(TO_CHAR(TIMESTAMP '"+TS1_EST_STR+"')), TO_TIMESTAMP(TO_CHAR(TIMESTAMP '"+TS1_EST_STR+"')), TO_TIMESTAMP(TO_CHAR(TIMESTAMP '"+TS1_EST_STR+"')) from " + tableName);
            assertTrue(rs.next());
            //FIXME this should have nanosecond resolution
            assertEquals(rs.getString(1), TS1_EST_MS_STR);
            assertEquals(rs.getString(2), TS1_EST_MS_STR);
            assertEquals(rs.getString(3), TS1_EST_MS_STR);
            //FIXME this should have nanosecond resolution
            assertEquals(rs.getString(4), TS1_EST_MS_STR);
            assertEquals(rs.getString(5), TS1_EST_MS_STR);
            assertEquals(rs.getString(6), TS1_EST_MS_STR);
            
            rs.close();

            //Nano precision works for parsing literals(but not for formatting and for onversion functions)
            rs = stmt.executeQuery("select * from " + tableName + " WHERE TIMESTAMPFIELD = TIMESTAMP '"+TS1_EST_STR+"' AND DATEFIELD = DATE '"+TS1_EST_MS_STR+"' AND TIMEFIELD = TIME '"+TS1_EST_MS_STR+"'");
            assertTrue(rs.next());
            
            PreparedStatement pstmt1 = conn.prepareStatement("select * from " + tableName + " WHERE TIMESTAMPFIELD = ?");
            pstmt1.setTimestamp(1, TS1_TS);
            rs = pstmt1.executeQuery();
            assertTrue(rs.next());

            //These are the interesting ones, where the string parse/format is done on the server side

            //FIXME TS should have nano precision
            rs = stmt.executeQuery("select * from " + tableName + " WHERE TO_CHAR(TIMESTAMPFIELD) = '"+TS1_EST_MS_STR+"' AND TO_CHAR(DATEFIELD) = '"+TS1_EST_MS_STR+"' AND TO_CHAR(TIMEFIELD) = '"+TS1_EST_MS_STR+"'");
            assertTrue(rs.next());

            //FIXME TS should have nano precision
            rs = stmt.executeQuery("select * from " + tableName + " WHERE TO_TIMESTAMP(TIMESTAMPSTRINGFIELD) = TIMESTAMP '"+TS1_EST_MS_STR+"' AND TO_DATE(DATESTRINGFIELD) = DATE '"+TS1_EST_MS_STR+"' AND TO_TIME(TIMESTRINGFIELD) = TIME '"+TS1_EST_MS_STR+"'");
            assertTrue(rs.next());

            //FIXME TS should have nano precision
            rs = stmt.executeQuery("select * from " + tableName + " WHERE TO_TIMESTAMP(TIMESTAMPSTRINGFIELD) = DATEFIELD AND TO_DATE(DATESTRINGFIELD) = DATEFIELD AND TO_TIME(TIMESTRINGFIELD) = TIMEFIELD");
            assertTrue(rs.next());

        }
    }

    private void assertMutationPlan(Statement stmt, String sql, Class clazz) throws SQLException {
        MutationPlan plan = stmt.unwrap(PhoenixStatement.class).compileMutation(sql);
        assertTrue(clazz.isInstance(plan));
    }
    
    @Test
    public void testUpsertSelectTZ() throws SQLException, IOException {
        // Test all methods of writing directly to the table

        String tz = "America/New_York";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.JDBC_COMPLIANT_TZ_HANDLING, "true");
        if (tz != null) {
            props.setProperty(QueryServices.TIMEZONE_OVERRIDE, tz);
        }
        props.setProperty(QueryServices.ENABLE_SERVER_UPSERT_SELECT, Boolean.TRUE.toString());

        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt = conn.createStatement()) {
            //Need this for server side execution
            conn.setAutoCommit(true);
            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( ID integer primary key, "
                    + " TIMESTAMPFIELD TIMESTAMP," + " DATEFIELD DATE," + " TIMEFIELD TIME,"
                    + " UTIMESTAMPFIELD UNSIGNED_TIMESTAMP, " + " UDATEFIELD UNSIGNED_DATE, "
                    + " UTIMEFIELD UNSIGNED_TIME, TIMESTAMPSTRINGFIELD VARCHAR, "
                    + " DATESTRINGFIELD VARCHAR, TIMESTRINGFIELD VARCHAR, NUMERICFIELD DECIMAL )");

            int rowId = 0;

            // Insert timestamp object via Timestamp in PreparedStatement (should be
            // timezone-independent)
            PreparedStatement ps =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            ps.setInt(1, ++rowId);
            ps.setTimestamp(2, TS1_TS);
            ps.setTimestamp(3, TS1_TS);
            ps.setTimestamp(4, TS1_TS);
            ps.setTimestamp(5, TS1_TS);
            ps.setTimestamp(6, TS1_TS);
            ps.setTimestamp(7, TS1_TS);
            ps.setString(8, TS1_EST_STR);
            ps.setString(9, TS1_EST_STR);
            ps.setString(10, TS1_EST_STR);
            ps.setLong(11, TS1_INSTANT_MS_NUMERIC);
            ps.executeUpdate();
            
            ps.setInt(1, ++rowId);
            ps.setString(8, TS1_EST_STR_UNDERSCORE);
            ps.setString(9, TS1_EST_STR_UNDERSCORE);
            ps.setString(10, TS1_EST_STR_UNDERSCORE);
            ps.executeUpdate();
            
            ps.setInt(1, ++rowId);
            ps.setString(8, TS1_JST_STR);
            ps.setString(9, TS1_JST_STR);
            ps.setString(10, TS1_JST_STR);
            ps.executeUpdate();


            Table hTable =
                    ((PhoenixConnection) conn).getQueryServices()
                            .getTable(TableName.valueOf(tableName).toBytes());

            rowId = 100;
            //No conversion needed
            String upsertSql1 = "UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD ) "
                    + " select " + ++rowId + ", TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD FROM " + tableName 
                    + " WHERE ID = 1";
            assertMutationPlan(stmt, upsertSql1, ServerUpsertSelectMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(upsertSql1));
            checkValues(hTable, rowId, TS1_TS);
            
            upsertSql1 += " LIMIT 1";
            assertMutationPlan(stmt, upsertSql1, ClientUpsertSelectMutationPlan.class);
            //We overwrite the previous row, but we don't care
            assertEquals(1, stmt.executeUpdate(upsertSql1));
            checkValues(hTable, rowId, TS1_TS);

            // Coercion from varchar and decimal is not supported, unlike with literals

            //test TO_TIMESTAMP
            String upsertSql2 = "UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                        + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD ) "
                        + " select " + ++rowId + ", TO_TIMESTAMP(TIMESTAMPSTRINGFIELD), null, null, "
                        + "  TO_TIMESTAMP(TIMESTAMPSTRINGFIELD), null, null FROM " + tableName 
                        + " WHERE ID = 1";
            assertMutationPlan(stmt, upsertSql2, ServerUpsertSelectMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(upsertSql2));
            // FIXME I'm pretty sure that we're testing for bad behaviour See PHOENIX-6792
            // These should have nanosecond resolution
            checkValues(hTable, rowId, TS1_TS_MS, null, null, TS1_TS_MS, null, null);
            
            upsertSql2 += " LIMIT 1";
            assertMutationPlan(stmt, upsertSql2, ClientUpsertSelectMutationPlan.class);
            //We overwrite the previous row, but we don't care
            assertEquals(1, stmt.executeUpdate(upsertSql2));
            checkValues(hTable, rowId, TS1_TS_MS, null, null, TS1_TS_MS, null, null);
            
            //test TO_DATE
            String upsertSql3 = 
                "UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                        + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD ) "
                        + " select " + ++rowId + ", TO_DATE(DATESTRINGFIELD), TO_DATE(DATESTRINGFIELD), TO_DATE(DATESTRINGFIELD), "
                        + "  TO_DATE(DATESTRINGFIELD), TO_DATE(DATESTRINGFIELD), TO_DATE(DATESTRINGFIELD) FROM " + tableName 
                        + " WHERE ID = 1";
            assertMutationPlan(stmt, upsertSql3, ServerUpsertSelectMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(upsertSql3));
            checkValues(hTable, rowId, TS1_TS_MS);

            upsertSql3 += " LIMIT 1";
            assertMutationPlan(stmt, upsertSql3, ClientUpsertSelectMutationPlan.class);
            //We overwrite the previous row, but we don't care
            assertEquals(1, stmt.executeUpdate(upsertSql3));
            checkValues(hTable, rowId, TS1_TS_MS);
            
            //test TO_TIME
            String upsertSql4 = 
                "UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                        + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD ) "
                        + " select " + ++rowId + ", TO_TIME(TIMESTRINGFIELD), TO_TIME(TIMESTRINGFIELD), TO_TIME(TIMESTRINGFIELD), "
                        + "  TO_TIME(TIMESTRINGFIELD), TO_TIME(TIMESTRINGFIELD), TO_TIME(TIMESTRINGFIELD) FROM " + tableName 
                        + " WHERE ID = 1";
            assertMutationPlan(stmt, upsertSql4, ServerUpsertSelectMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(upsertSql4));
            checkValues(hTable, rowId, TS1_TS_MS);

            upsertSql4 += " LIMIT 1";
            assertMutationPlan(stmt, upsertSql4, ClientUpsertSelectMutationPlan.class);
            //We overwrite the previous row, but we don't care
            assertEquals(1, stmt.executeUpdate(upsertSql4));
            checkValues(hTable, rowId, TS1_TS_MS);
            
            //test TO_DATE with custom format
            String upsertSql5 = 
                "UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                        + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD ) "
                        + " select " + ++rowId + ", TO_DATE(DATESTRINGFIELD, '"+UNDERSCORE_FORMAT+"'), TO_DATE(DATESTRINGFIELD, '"+UNDERSCORE_FORMAT+"'), TO_DATE(DATESTRINGFIELD, '"+UNDERSCORE_FORMAT+"'), "
                        + "  TO_DATE(DATESTRINGFIELD, '"+UNDERSCORE_FORMAT+"'), TO_DATE(DATESTRINGFIELD, '"+UNDERSCORE_FORMAT+"'), TO_DATE(DATESTRINGFIELD, '"+UNDERSCORE_FORMAT+"') FROM " + tableName 
                        + " WHERE ID = 2";
            assertMutationPlan(stmt, upsertSql5, ServerUpsertSelectMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(upsertSql5));
            checkValues(hTable, rowId, TS1_TS_MS);
            
            upsertSql5 += " LIMIT 1";
            assertMutationPlan(stmt, upsertSql5, ClientUpsertSelectMutationPlan.class);
            //We overwrite the previous row, but we don't care
            assertEquals(1, stmt.executeUpdate(upsertSql5));
            checkValues(hTable, rowId, TS1_TS_MS);
            
            //test TO_DATE with custom TZ
            String upsertSql6 = 
                "UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                        + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD ) "
                        + " select " + ++rowId + ", TO_DATE(DATESTRINGFIELD, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST'), TO_DATE(DATESTRINGFIELD, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST'), TO_DATE(DATESTRINGFIELD, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST'), "
                        + " TO_DATE(DATESTRINGFIELD, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST'), TO_DATE(DATESTRINGFIELD, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST'), TO_DATE(DATESTRINGFIELD, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST') FROM " + tableName 
                        + " WHERE ID = 3";
            assertMutationPlan(stmt, upsertSql6, ServerUpsertSelectMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(upsertSql6));
            checkValues(hTable, rowId, TS1_TS_MS);
            
            upsertSql6 += " LIMIT 1";
            assertMutationPlan(stmt, upsertSql6, ClientUpsertSelectMutationPlan.class);
            //We overwrite the previous row, but we don't care
            assertEquals(1, stmt.executeUpdate(upsertSql6));
            checkValues(hTable, rowId, TS1_TS_MS);
            
            //test upserting to temporal from numeric (TZ is not used here)
            String upsertSql7 = 
                "UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                        + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD ) "
                        + " select " + ++rowId + ", NUMERICFIELD, NUMERICFIELD, NUMERICFIELD, "
                        + " NUMERICFIELD, NUMERICFIELD, NUMERICFIELD FROM " + tableName 
                        + " WHERE ID = 1";
            assertMutationPlan(stmt, upsertSql7, ServerUpsertSelectMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(upsertSql7));
            checkValues(hTable, rowId, TS1_TS_MS);
            
            upsertSql7 += " LIMIT 1";
            assertMutationPlan(stmt, upsertSql7, ClientUpsertSelectMutationPlan.class);
            //We overwrite the previous row, but we don't care
            assertEquals(1, stmt.executeUpdate(upsertSql7));
            checkValues(hTable, rowId, TS1_TS_MS);
        }

    }

    
    @Test
    public void testDeleteTZ() throws SQLException, IOException {
        String timeZone = "America/New_York";

        try (Connection conn = getConn(timeZone); Statement stmt = conn.createStatement()) {
            //Need this for server side execution
            conn.setAutoCommit(true);
            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( ID integer primary key, "
                    + " TIMESTAMPFIELD TIMESTAMP," + " DATEFIELD DATE," + " TIMEFIELD TIME,"
                    + " UTIMESTAMPFIELD UNSIGNED_TIMESTAMP, " + " UDATEFIELD UNSIGNED_DATE, "
                    + " UTIMEFIELD UNSIGNED_TIME, "
                    + " ESTSTRING VARCHAR(100), UNDERSCORESTR VARCHAR(100), JSTSTRING VARCHAR(100))");

            int rowId = 0;

            PreparedStatement ps =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            while(rowId<20) {
                ps.setInt(1, ++rowId);
                ps.setTimestamp(2, TS1_TS);
                ps.setTimestamp(3, TS1_TS);
                ps.setTimestamp(4, TS1_TS);
                ps.setTimestamp(5, TS1_TS);
                ps.setTimestamp(6, TS1_TS);
                ps.setTimestamp(7, TS1_TS);
                ps.setString(8, TS1_EST_MS_STR);
                ps.setString(9, TS1_EST_MS_STR_UNDERSCORE);
                ps.setString(10, TS1_JST_STR);
                ps.executeUpdate();
            }

            // FIXME This is strange. We accept string literals as dates for upserts, but not for comparison ????
//            assertEquals( 1, stmt.executeUpdate("DELETE FROM " + tableName + " WHERE id = 1 "
//                    + " AND DATEFIELD = DATE '" + TS1_EST_STR_SPACE + "'"));
            
            String deleteStr1 = "DELETE FROM " + tableName + " WHERE id = 1 "
                    + " AND DATEFIELD = DATE '" + TS1_EST_STR + "'";
            assertMutationPlan(stmt, deleteStr1, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr1));

            String deleteStr2 = "DELETE FROM " + tableName + " WHERE id = 2 "
                    + " AND UDATEFIELD = DATE '" + TS1_EST_STR + "'";
            assertMutationPlan(stmt, deleteStr2, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr2));

            String deleteStr3 = "DELETE FROM " + tableName + " WHERE id = 3 "
                    + " AND DATEFIELD = TO_DATE('" + TS1_EST_STR + "')";
            assertMutationPlan(stmt, deleteStr3, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr3));
            
            String deleteStr4 = "DELETE FROM " + tableName + " WHERE id = 4 "
                    + " AND DATEFIELD = TO_DATE('" + TS1_JST_STR + "', '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST')";
            assertMutationPlan(stmt, deleteStr4, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr4));

            String deleteStr5 = "DELETE FROM " + tableName + " WHERE id = 5 "
                    + " AND TO_CHAR(DATEFIELD) = '" + TS1_EST_MS_STR + "'";
            assertMutationPlan(stmt, deleteStr5, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr5));

            //FIXME should return nanos
            String deleteStr6 = "DELETE FROM " + tableName + " WHERE id = 6 "
                    + " AND TO_CHAR(TIMESTAMPFIELD, '"+UNDERSCORE_FORMAT+"') = '"+TS1_EST_MS_STR_UNDERSCORE+"'";
            assertMutationPlan(stmt, deleteStr6, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr6));

            String deleteStr7 = "DELETE FROM " + tableName + " WHERE id = 7 "
                    + " AND TO_DATE(ESTSTRING) = DATEFIELD";
            assertMutationPlan(stmt, deleteStr7, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr7));

            String deleteStr8 = "DELETE FROM " + tableName + " WHERE id = 8 "
                    + " AND TO_DATE(UNDERSCORESTR, '"+UNDERSCORE_FORMAT+"' ) = DATEFIELD";
            assertMutationPlan(stmt, deleteStr8, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr8));

            String deleteStr9 = "DELETE FROM " + tableName + " WHERE id = 9 "
                    + " AND TO_DATE(JSTSTRING, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST' ) = DATEFIELD";
            assertMutationPlan(stmt, deleteStr9, DeleteCompiler.ServerSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr9));
                        
            //The same queries, but with LIMIT, to test the client side execution path
            
            String deleteStr11 = "DELETE FROM " + tableName + " WHERE id = 11 "
                    + " AND DATEFIELD = DATE '" + TS1_EST_STR + "' LIMIT 1";
            assertMutationPlan(stmt, deleteStr11, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr11));
            
            String deleteStr12 = "DELETE FROM " + tableName + " WHERE id = 12 "
                    + " AND UDATEFIELD = DATE '" + TS1_EST_STR + "' LIMIT 1";
            assertMutationPlan(stmt, deleteStr12, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr12));
            
            String deleteStr13 = "DELETE FROM " + tableName + " WHERE id = 13 "
                    + " AND DATEFIELD = TO_DATE('" + TS1_EST_STR + "') LIMIT 1";
            assertMutationPlan(stmt, deleteStr13, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr13));
            
            String deleteStr14 = "DELETE FROM " + tableName + " WHERE id = 14 "
                    + " AND DATEFIELD = TO_DATE('" + TS1_JST_STR + "', '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST') LIMIT 1";
            assertMutationPlan(stmt, deleteStr14, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr14));
            
            String deleteStr15 = "DELETE FROM " + tableName + " WHERE id = 15 "
                    + " AND TO_CHAR(DATEFIELD) = '" + TS1_EST_MS_STR + "' LIMIT 1";
            assertMutationPlan(stmt, deleteStr15, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr15));

            //FIXME should return nanos
            String deleteStr16 = "DELETE FROM " + tableName + " WHERE id = 16 "
                    + " AND TO_CHAR(TIMESTAMPFIELD, '"+UNDERSCORE_FORMAT+"') = '"+TS1_EST_MS_STR_UNDERSCORE+"' LIMIT 1";
            assertMutationPlan(stmt, deleteStr16, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr16));
            
            String deleteStr17 = "DELETE FROM " + tableName + " WHERE id = 17 "
                    + " AND TO_DATE(ESTSTRING) = DATEFIELD LIMIT 1";
            assertMutationPlan(stmt, deleteStr17, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr17));

            String deleteStr18 = "DELETE FROM " + tableName + " WHERE id = 18 "
                    + " AND TO_DATE(UNDERSCORESTR, '"+UNDERSCORE_FORMAT+"' ) = DATEFIELD LIMIT 1 ";
            assertMutationPlan(stmt, deleteStr18, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr18));

            String deleteStr19 = "DELETE FROM " + tableName + " WHERE id = 19 "
                    + " AND TO_DATE(JSTSTRING, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','JST' ) = DATEFIELD LIMIT 1";
            assertMutationPlan(stmt, deleteStr19, DeleteCompiler.ClientSelectDeleteMutationPlan.class);
            assertEquals(1, stmt.executeUpdate(deleteStr19));
        }
    }

    //@Ignore
    @Test
    public void testDefaultValue() throws SQLException, IOException {
        String timeZone = "America/New_York";

        // Because of PHOENIX-6795 which causes defaults to act as read-time replacements for null, 
        // instead of being used as default values for 
        
        // when specifying temporal literals as defaults, there is a semantic quirk.
        // One could expect that that a "TIMESTAMP 2000-01-01 01:01:01" literal get written
        // literally into the SYSCAT default field.
        //
        // What actually happens is that the TIMESTAMP literal gets parsed, and the time value
        // is converted to an epoch (UTC) value, then stored in a LiterExpression.
        //
        // This expression is then sent to 
        
        try (Connection conn = getConn(timeZone);
                Statement stmt = conn.createStatement()) {
            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( ID integer primary key, "
                    + " TIMESTAMPFIELD TIMESTAMP DEFAULT TIMESTAMP '"+TS1_EST_STR+"'," + " DATEFIELD DATE DEFAULT DATE '"+TS1_EST_STR+"'," + " TIMEFIELD TIME,"
                    + " UTIMESTAMPFIELD UNSIGNED_TIMESTAMP DEFAULT TO_TIMESTAMP('"+TS1_EST_STR+"') , " + " UDATEFIELD UNSIGNED_DATE, "
                    + " UTIMEFIELD UNSIGNED_TIME )");

            Table hTable =
                    ((PhoenixConnection) conn).getQueryServices()
                            .getTable(TableName.valueOf(tableName).toBytes());

            stmt.executeUpdate("UPSERT INTO " + tableName + " (ID) VALUES (1)");
            conn.commit();

            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals(rs.getTimestamp("TIMESTAMPFIELD"), TS1_TS_MS);

            // FIXME PHOENIX-6795
            //checkValues(hTable, 1, TS1_TS_MS, TS1_TS_MS, null, null, null, null);
        }
        
    }
    
    @Test
    public void testFunctions() throws SQLException, IOException {
        String timeZone = "America/New_York";

        try (Connection conn = getConn(timeZone); Statement stmt = conn.createStatement()) {
            //Need this for server side execution
            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( ID integer primary key, "
                    + " TIMESTAMPFIELD TIMESTAMP," + " DATEFIELD DATE," + " TIMEFIELD TIME,"
                    + " UTIMESTAMPFIELD UNSIGNED_TIMESTAMP, " + " UDATEFIELD UNSIGNED_DATE, "
                    + " UTIMEFIELD UNSIGNED_TIME) ");


            PreparedStatement ps =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " VALUES (?, ?, ?, ?, ?, ?, ?)");
            
            Table hTable =
                    ((PhoenixConnection) conn).getQueryServices()
                            .getTable(TableName.valueOf(tableName).toBytes());
            
            // One record for every hour for 10 days between 2020-12-25 and 2021-01-05
            // We don't need to care for Chronology for recent dates
            LocalDateTime ld = LocalDateTime.of(2020, 12, 25, 11, 12, 13, 444444444);
            LocalDateTime end = LocalDateTime.of(2021, 1, 5, 11, 12, 13, 444444444);

            int rowId = 0;
            while (ld.isBefore(end)) {
                ZoneOffset offset = EST_ZONE_ID.getRules().getOffset(ld);
                java.sql.Timestamp ts = 
                        java.sql.Timestamp.from(ld.toInstant(offset));
                ps.setInt(1, ++rowId);
                ps.setTimestamp(2, ts);
                ps.setTimestamp(3, ts);
                ps.setTimestamp(4, ts);
                ps.setTimestamp(5, ts);
                ps.setTimestamp(6, ts);
                ps.setTimestamp(7, ts);
                ps.executeUpdate();
                ld = ld.plusHours(1);
            }
            conn.commit();
            
            LocalDateTime expected = LocalDateTime.of(2020, 12, 25, 11, 12, 13, 444444444);
            ZoneOffset estOffset = EST_ZONE_ID.getRules().getOffset(expected);
            Instant i = expected.toInstant(estOffset);
            java.sql.Timestamp expectectedTs = java.sql.Timestamp.from(i);
            java.sql.Timestamp expectectedTsMs = java.sql.Timestamp.from(expected.withNano(444000000).toInstant(estOffset));

            //This test doesn't do much, seconds are not affected by TZ
            ResultSet rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " SECOND(TIMESTAMPFIELD) as mts , SECOND(DATEFIELD) as md, SECOND(TIMEFIELD) as mt,"
                    + " SECOND(UTIMESTAMPFIELD) as muts, SECOND(UDATEFIELD) as mud, SECOND(UTIMEFIELD) as mut FROM " + tableName 
                    + " WHERE SECOND(TIMESTAMPFIELD) = 13 AND SECOND(DATEFIELD)= 13 AND SECOND(TIMEFIELD) = 13 "
                    + " AND SECOND(UTIMESTAMPFIELD) = 13 AND SECOND(UDATEFIELD) = 13 AND SECOND(UTIMEFIELD) = 13"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(13, rs.getInt("mts"));
            assertEquals(13, rs.getInt("md"));
            assertEquals(13, rs.getInt("mt"));
            assertEquals(13, rs.getInt("muts"));
            assertEquals(13, rs.getInt("mud"));
            assertEquals(13, rs.getInt("mut"));
            checkValues(hTable, 1, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);
            
            //This test doesn't do much, we'd need to use TZ with a fractional hour offset
            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " MINUTE(TIMESTAMPFIELD) as mts , MINUTE(DATEFIELD) as md, MINUTE(TIMEFIELD) as mt,"
                    + " MINUTE(UTIMESTAMPFIELD) as muts, MINUTE(UDATEFIELD) as mud, MINUTE(UTIMEFIELD) as mut FROM " + tableName 
                    + " WHERE MINUTE(TIMESTAMPFIELD) = 12 AND MINUTE(DATEFIELD)= 12 AND MINUTE(TIMEFIELD) = 12 "
                    + " AND MINUTE(UTIMESTAMPFIELD) = 12 AND MINUTE(UDATEFIELD) = 12 AND MINUTE(UTIMEFIELD) = 12"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(12, rs.getInt("mts"));
            assertEquals(12, rs.getInt("md"));
            assertEquals(12, rs.getInt("mt"));
            assertEquals(12, rs.getInt("muts"));
            assertEquals(12, rs.getInt("mud"));
            assertEquals(12, rs.getInt("mut"));
            checkValues(hTable, 1, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);
            
            expected = LocalDateTime.of(2020, 12, 25, 13, 12, 13, 444444444);
            estOffset = EST_ZONE_ID.getRules().getOffset(expected);
            i = expected.toInstant(estOffset);
            expectectedTs = java.sql.Timestamp.from(i);
            expectectedTsMs = java.sql.Timestamp.from(expected.withNano(444000000).toInstant(estOffset));

            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " hour(TIMESTAMPFIELD) as hts , hour(DATEFIELD) as hd, hour(TIMEFIELD) as ht,"
                    + " hour(UTIMESTAMPFIELD) as huts, hour(UDATEFIELD) as hud, hour(UTIMEFIELD) as hut FROM " + tableName 
                    + " WHERE hour(TIMESTAMPFIELD) = 13 AND hour(DATEFIELD)= 13 AND hour(TIMEFIELD) = 13 "
                    + " AND hour(UTIMESTAMPFIELD) = 13 AND hour(UDATEFIELD) = 13 AND hour(UTIMEFIELD) = 13"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals(13, rs.getInt("hts"));
            assertEquals(13, rs.getInt("hd"));
            assertEquals(13, rs.getInt("ht"));
            assertEquals(13, rs.getInt("huts"));
            assertEquals(13, rs.getInt("hud"));
            assertEquals(13, rs.getInt("hut"));
            checkValues(hTable, 3, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);

            expected = LocalDateTime.of(2021, 1, 1, 0, 12, 13, 444444444);
            estOffset = EST_ZONE_ID.getRules().getOffset(expected);
            i = expected.toInstant(estOffset);
            expectectedTs = java.sql.Timestamp.from(i);
            expectectedTsMs = java.sql.Timestamp.from(expected.withNano(444000000).toInstant(estOffset));

            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " DAYOFMONTH(TIMESTAMPFIELD) as dts , DAYOFMONTH(DATEFIELD) as dd, DAYOFMONTH(TIMEFIELD) as dt,"
                    + " DAYOFMONTH(UTIMESTAMPFIELD) as duts, DAYOFMONTH(UDATEFIELD) as dud, DAYOFMONTH(UTIMEFIELD) as dut FROM " + tableName 
                    + " WHERE DAYOFMONTH(TIMESTAMPFIELD) = 1 AND DAYOFMONTH(TIMESTAMPFIELD)= 1 AND DAYOFMONTH(TIMEFIELD) = 1 "
                    + " AND DAYOFMONTH(UTIMESTAMPFIELD) = 1 AND DAYOFMONTH(UDATEFIELD) = 1 AND DAYOFMONTH(UTIMEFIELD) = 1"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(158, rs.getInt("id"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dt"));
            assertEquals(1, rs.getInt("duts"));
            assertEquals(1, rs.getInt("dud"));
            assertEquals(1, rs.getInt("dut"));
            checkValues(hTable, 158, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);
            
            //Same time
            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " DAYOFYEAR(TIMESTAMPFIELD) as dts , DAYOFYEAR(DATEFIELD) as dd, DAYOFYEAR(TIMEFIELD) as dt,"
                    + " DAYOFYEAR(UTIMESTAMPFIELD) as duts, DAYOFYEAR(UDATEFIELD) as dud, DAYOFYEAR(UTIMEFIELD) as dut FROM " + tableName 
                    + " WHERE DAYOFYEAR(TIMESTAMPFIELD) = 1 AND DAYOFYEAR(TIMESTAMPFIELD)= 1 AND DAYOFYEAR(TIMEFIELD) = 1 "
                    + " AND DAYOFYEAR(UTIMESTAMPFIELD) = 1 AND DAYOFYEAR(UDATEFIELD) = 1 AND DAYOFYEAR(UTIMEFIELD) = 1"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(158, rs.getInt("id"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dt"));
            assertEquals(1, rs.getInt("duts"));
            assertEquals(1, rs.getInt("dud"));
            assertEquals(1, rs.getInt("dut"));
            checkValues(hTable, 158, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);
            
            //Same time
            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " YEAR(TIMESTAMPFIELD) as dts , YEAR(DATEFIELD) as dd, YEAR(TIMEFIELD) as dt,"
                    + " YEAR(UTIMESTAMPFIELD) as duts, YEAR(UDATEFIELD) as dud, YEAR(UTIMEFIELD) as dut FROM " + tableName 
                    + " WHERE YEAR(TIMESTAMPFIELD) = 2021 AND YEAR(TIMESTAMPFIELD)= 2021 AND YEAR(TIMEFIELD) = 2021 "
                    + " AND YEAR(UTIMESTAMPFIELD) = 2021 AND YEAR(UDATEFIELD) = 2021 AND YEAR(UTIMEFIELD) = 2021"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(158, rs.getInt("id"));
            assertEquals(2021, rs.getInt("dts"));
            assertEquals(2021, rs.getInt("dts"));
            assertEquals(2021, rs.getInt("dt"));
            assertEquals(2021, rs.getInt("duts"));
            assertEquals(2021, rs.getInt("dud"));
            assertEquals(2021, rs.getInt("dut"));
            checkValues(hTable, 158, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);

            //Same time
            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " MONTH(TIMESTAMPFIELD) as dts , MONTH(DATEFIELD) as dd, MONTH(TIMEFIELD) as dt,"
                    + " MONTH(UTIMESTAMPFIELD) as duts, MONTH(UDATEFIELD) as dud, MONTH(UTIMEFIELD) as dut FROM " + tableName 
                    + " WHERE MONTH(TIMESTAMPFIELD) = 1 AND MONTH(TIMESTAMPFIELD)= 1 AND MONTH(TIMEFIELD) = 1 "
                    + " AND MONTH(UTIMESTAMPFIELD) = 1 AND MONTH(UDATEFIELD) = 1 AND MONTH(UTIMEFIELD) = 1"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(158, rs.getInt("id"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dt"));
            assertEquals(1, rs.getInt("duts"));
            assertEquals(1, rs.getInt("dud"));
            assertEquals(1, rs.getInt("dut"));
            checkValues(hTable, 158, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);

            
            expected = LocalDateTime.of(2020, 12, 28, 0, 12, 13, 444444444);
            estOffset = EST_ZONE_ID.getRules().getOffset(expected);
            i = expected.toInstant(estOffset);
            expectectedTs = java.sql.Timestamp.from(i);
            expectectedTsMs = java.sql.Timestamp.from(expected.withNano(444000000).toInstant(estOffset));
            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " DAYOFWEEK(TIMESTAMPFIELD) as dts , DAYOFWEEK(DATEFIELD) as dd, DAYOFWEEK(TIMEFIELD) as dt,"
                    + " DAYOFWEEK(UTIMESTAMPFIELD) as duts, DAYOFWEEK(UDATEFIELD) as dud, DAYOFWEEK(UTIMEFIELD) as dut FROM " + tableName 
                    + " WHERE DAYOFWEEK(TIMESTAMPFIELD) = 1 AND DAYOFWEEK(TIMESTAMPFIELD)= 1 AND DAYOFWEEK(TIMEFIELD) = 1 "
                    + " AND DAYOFWEEK(UTIMESTAMPFIELD) = 1 AND DAYOFWEEK(UDATEFIELD) = 1 AND DAYOFWEEK(UTIMEFIELD) = 1"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(62, rs.getInt("id"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dt"));
            assertEquals(1, rs.getInt("duts"));
            assertEquals(1, rs.getInt("dud"));
            assertEquals(1, rs.getInt("dut"));
            checkValues(hTable, 62, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);

            expected = LocalDateTime.of(2021, 1, 4, 0, 12, 13, 444444444);
            estOffset = EST_ZONE_ID.getRules().getOffset(expected);
            i = expected.toInstant(estOffset);
            expectectedTs = java.sql.Timestamp.from(i);
            expectectedTsMs = java.sql.Timestamp.from(expected.withNano(444000000).toInstant(estOffset));
            rs = stmt.executeQuery("select ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                    + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                    + " WEEK(TIMESTAMPFIELD) as dts , WEEK(DATEFIELD) as dd, WEEK(TIMEFIELD) as dt,"
                    + " WEEK(UTIMESTAMPFIELD) as duts, WEEK(UDATEFIELD) as dud, WEEK(UTIMEFIELD) as dut FROM " + tableName 
                    + " WHERE WEEK(TIMESTAMPFIELD) = 1 AND WEEK(TIMESTAMPFIELD)= 1 AND WEEK(TIMEFIELD) = 1 "
                    + " AND WEEK(UTIMESTAMPFIELD) = 1 AND WEEK(UDATEFIELD) = 1 AND WEEK(UTIMEFIELD) = 1"
                    + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(230, rs.getInt("id"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dts"));
            assertEquals(1, rs.getInt("dt"));
            assertEquals(1, rs.getInt("duts"));
            assertEquals(1, rs.getInt("dud"));
            assertEquals(1, rs.getInt("dut"));
            checkValues(hTable, 230, expectectedTs, expectectedTsMs, expectectedTsMs, expectectedTs, expectectedTsMs, expectectedTsMs);
        }
    }

    @Test
    public void testRoundFunctions() throws SQLException, IOException {
        String timeZone = "America/New_York";

        java.time.LocalDateTime LD2 = java.time.LocalDateTime.of(2022, 2, 2, 2, 2, 2, 222);
//        java.time.LocalDateTime LD2A = java.time.LocalDateTime.of(2022, 2, 2, 2, 3, 2, 222);
        java.time.LocalDateTime LD3 = java.time.LocalDateTime.of(2023, 11, 20, 23, 59, 59, 999);

        ZoneOffset offset2 = EST_ZONE_ID.getRules().getOffset(LD2);
        java.time.Instant TS2_INSTANT = LD2.toInstant(offset2);
//        java.time.Instant TS2A_INSTANT = LD2A.toInstant(offset2);
        
        ZoneOffset offset3 = EST_ZONE_ID.getRules().getOffset(LD2);
        java.time.Instant TS3_INSTANT = LD3.toInstant(offset3);

        String TS2_EST_STR_ISO = LD2.toString();
        String TS2_EST_STR = TS2_EST_STR_ISO.replaceAll("T", " ");

        String TS3_EST_STR_ISO = LD3.toString();
        String TS3_EST_STR = TS3_EST_STR_ISO.replaceAll("T", " ");

        try (Connection conn = getConn(timeZone);
                Statement stmt = conn.createStatement())
        {

            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( "
                    + " df DATE primary key, dfv DATE) ");

            PreparedStatement ps =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " VALUES (?, ?)");

            ps.setTimestamp(1, java.sql.Timestamp.from(TS2_INSTANT));
            ps.setTimestamp(2, java.sql.Timestamp.from(TS2_INSTANT));
            ps.executeUpdate();
//            ps.setTimestamp(1, java.sql.Timestamp.from(TS2A_INSTANT));
//            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(TS3_INSTANT));
            ps.setTimestamp(2, java.sql.Timestamp.from(TS3_INSTANT));
            ps.executeUpdate();
            conn.commit();

            // YEARS

            ResultSet rs = stmt.executeQuery("select df, round(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where  round(df, 'YEAR', 1) = DATE '2022-01-01 00:00:00' "
                + " and round(dfv, 'YEAR', 1) = DATE '2022-01-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where round(df, 'YEAR', 1) = DATE '2024-01-01 00:00:00' "
                + " and round(dfv, 'YEAR', 1) = DATE '2024-01-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2024-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where floor(df, 'YEAR', 1) = DATE '2023-01-01 00:00:00' "
                + " and floor(dfv, 'YEAR', 1) = DATE '2023-01-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'YEAR', 1) = DATE '2023-01-01 00:00:00' "
                + " and ceil(dfv, 'YEAR', 1) = DATE '2023-01-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where round(df, 'YEAR', 1) >= DATE '2022-01-01 00:00:00' and round(df, 'YEAR', 1) <= DATE '2022-01-01 00:00:00' "
                + " and round(dfv, 'YEAR', 1) >= DATE '2022-01-01 00:00:00' and round(dfv, 'YEAR', 1) <= DATE '2022-01-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where floor(df, 'YEAR', 1) >= DATE '2022-01-01 00:00:00' and floor(df, 'YEAR', 1) <= DATE '2022-01-01 00:00:00' "
                + " and floor(dfv, 'YEAR', 1) >= DATE '2022-01-01 00:00:00' and floor(dfv, 'YEAR', 1) <= DATE '2022-01-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'YEAR', 1) >= DATE '2023-01-01 00:00:00' and ceil(df, 'YEAR', 1) <= DATE '2023-01-01 00:00:00' "
                + " and ceil(dfv, 'YEAR', 1) >= DATE '2023-01-01 00:00:00' and ceil(dfv, 'YEAR', 1) <= DATE '2023-01-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where round(df, 'YEAR', 1) > DATE '2022-01-01 00:00:00' "
                + " and round(dfv, 'YEAR', 1) > DATE '2022-01-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2024-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where floor(df, 'YEAR', 1) > DATE '2022-01-01 00:00:00'"
                + " and floor(dfv, 'YEAR', 1) > DATE '2022-01-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'YEAR', 1) > DATE '2023-01-01 00:00:00' "
                + " and ceil(dfv, 'YEAR', 1) > DATE '2023-01-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2024-01-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select df, round(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where round(df, 'YEAR', 1) < DATE '2022-01-01 00:00:00' "
                + " and round(dfv, 'YEAR', 1) < DATE '2022-01-01 00:00:00' ");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where floor(df, 'YEAR', 1) < DATE '2022-01-01 00:00:00'"
                + " and floor(dfv, 'YEAR', 1) < DATE '2022-01-01 00:00:00'");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'YEAR', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'YEAR', 1) < DATE '2023-01-01 00:00:00' "
                + " and ceil(dfv, 'YEAR', 1) < DATE '2023-01-01 00:00:00'");
            assertFalse(rs.next());
            
            // MONTHS

            rs = stmt.executeQuery("select df, round(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where round(df, 'MONTH', 1) = DATE '2022-02-01 00:00:00' "
                + " and  round(dfv, 'MONTH', 1) = DATE '2022-02-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where round(df, 'MONTH', 1) = DATE '2023-12-01 00:00:00' "
                + " and round(dfv, 'MONTH', 1) = DATE '2023-12-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-12-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where floor(df, 'MONTH', 1) = DATE '2023-11-01 00:00:00' "
                + " and floor(dfv, 'MONTH', 1) = DATE '2023-11-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) = DATE '2022-03-01 00:00:00' "
                + " and ceil(dfv, 'MONTH', 1) = DATE '2022-03-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-03-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where round(df, 'MONTH', 1) >= DATE '2022-02-01 00:00:00' and round(df, 'MONTH', 1) <= DATE '2022-02-01 00:00:00' "
                + " and round(dfv, 'MONTH', 1) >= DATE '2022-02-01 00:00:00' and round(dfv, 'MONTH', 1) <= DATE '2022-02-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where floor(df, 'MONTH', 1) >= DATE '2022-02-01 00:00:00' and floor(df, 'MONTH', 1) <= DATE '2022-02-01 00:00:00' "
                + " and floor(dfv, 'MONTH', 1) >= DATE '2022-02-01 00:00:00' and floor(dfv, 'MONTH', 1) <= DATE '2022-02-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) >= DATE '2022-03-01 00:00:00' and ceil(df, 'MONTH', 1) <= DATE '2022-03-01 00:00:00' "
                + " and ceil(dfv, 'MONTH', 1) >= DATE '2022-03-01 00:00:00' and ceil(dfv, 'MONTH', 1) <= DATE '2022-03-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-03-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where round(df, 'MONTH', 1) > DATE '2022-02-01 00:00:00' "
                + " and round(dfv, 'MONTH', 1) > DATE '2022-02-01 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-12-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where floor(df, 'MONTH', 1) > DATE '2022-02-01 00:00:00'"
                + " and floor(dfv, 'MONTH', 1) > DATE '2022-02-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) > DATE '2022-03-01 00:00:00' "
                + " and ceil(dfv, 'MONTH', 1) > DATE '2022-03-01 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-12-01 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select df, round(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where round(df, 'MONTH', 1) < DATE '2022-02-01 00:00:00' "
                + " and round(dfv, 'MONTH', 1) < DATE '2022-02-01 00:00:00' ");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where floor(df, 'MONTH', 1) < DATE '2022-02-01 00:00:00'"
                + " and floor(dfv, 'MONTH', 1) < DATE '2022-02-01 00:00:00'");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MONTH', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) < DATE '2022-03-01 00:00:00' "
                + " and ceil(dfv, 'MONTH', 1) < DATE '2022-03-01 00:00:00'");
            assertFalse(rs.next());
            
            // WEEKS

            rs = stmt.executeQuery("select df, round(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where round(df, 'WEEK', 1) = DATE '2022-01-31 00:00:00' "
                + " and round(dfv, 'WEEK', 1) = DATE '2022-01-31 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-01-31 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where round(df, 'WEEK', 1) = DATE '2023-11-20 00:00:00' "
                + " and round(dfv, 'WEEK', 1) = DATE '2023-11-20 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-20 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where floor(df, 'WEEK', 1) = DATE '2022-01-31 00:00:00' "
                + " and floor(dfv, 'WEEK', 1) = DATE '2022-01-31 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-01-31 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'WEEK', 1) = DATE '2022-02-07 00:00:00' "
                + " and ceil(dfv, 'WEEK', 1) = DATE '2022-02-07 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-07 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where round(df, 'WEEK', 1) >= DATE '2022-01-31 00:00:00' and round(df, 'WEEK', 1) <= DATE '2022-01-31 00:00:00' "
                + " and round(dfv, 'WEEK', 1) >= DATE '2022-01-31 00:00:00' and round(dfv, 'WEEK', 1) <= DATE '2022-01-31 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-01-31 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where floor(df, 'WEEK', 1) >= DATE '2022-01-31 00:00:00' and floor(df, 'WEEK', 1) <= DATE '2022-01-31 00:00:00' "
                + " and floor(dfv, 'WEEK', 1) >= DATE '2022-01-31 00:00:00' and floor(dfv, 'WEEK', 1) <= DATE '2022-01-31 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-01-31 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'WEEK', 1) >= DATE '2022-02-07 00:00:00' and ceil(df, 'WEEK', 1) <= DATE '2022-02-07 00:00:00' "
                + " and ceil(dfv, 'WEEK', 1) >= DATE '2022-02-07 00:00:00' and ceil(dfv, 'WEEK', 1) <= DATE '2022-02-07 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-07 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where round(df, 'WEEK', 1) > DATE '2022-01-31 00:00:00' "
                + " and round(dfv, 'WEEK', 1) > DATE '2022-01-31 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-20 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where floor(df, 'WEEK', 1) > DATE '2022-01-31 00:00:00'"
                + " and floor(dfv, 'WEEK', 1) > DATE '2022-01-31 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-20 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'WEEK', 1) > DATE '2022-02-07 00:00:00' "
                + " and ceil(dfv, 'WEEK', 1) > DATE '2022-02-07 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-27 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select df, round(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where round(df, 'WEEK', 1) < DATE '2022-01-31 00:00:00' "
                + " and round(dfv, 'WEEK', 1) < DATE '2022-01-31 00:00:00' ");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where floor(df, 'WEEK', 1) < DATE '2022-01-31 00:00:00'"
                + " and floor(dfv, 'WEEK', 1) < DATE '2022-01-31 00:00:00'");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'WEEK', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'WEEK', 1) < DATE '2022-01-31 00:00:00' "
                + " and ceil(dfv, 'WEEK', 1) < DATE '2022-01-31 00:00:00'");
            assertFalse(rs.next());
            
            //DAYS

            // Day, Hour, Minute, Second do support the multiplier
            rs = stmt.executeQuery("select df, round(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where round(df, 'DAY', 1) = DATE '2022-02-02 00:00:00' "
                + " and round(dfv, 'DAY', 1) = DATE '2022-02-02 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'DAY', 10) "
                    + " from " + tableName 
                + " where round(df, 'DAY', 10) = DATE '2022-02-07 00:00:00' "
                + " and round(dfv, 'DAY', 10) = DATE '2022-02-07 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-07 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'DAY', 10) "
                    + " from " + tableName 
                + " where round(df, 'DAY', 10) = DATE '2023-11-19 00:00:00' "
                + " and round(dfv, 'DAY', 10) = DATE '2023-11-19 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-19 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'DAY', 10) "
                    + " from " + tableName 
                + " where floor(df, 'DAY', 10) = DATE '2022-01-28 00:00:00' "
                + " and floor(dfv, 'DAY', 10) = DATE '2022-01-28 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-01-28 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'DAY', 10) "
                    + " from " + tableName 
                + " where ceil(df, 'DAY', 10) = DATE '2022-02-07 00:00:00' "
                + " and ceil(dfv, 'DAY', 10) = DATE '2022-02-07 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-07 00:00:00.000", rs.getString(2));

            rs = stmt.executeQuery("select df, round(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where round(df, 'DAY', 1) >= DATE '2022-02-02 00:00:00' and round(df, 'DAY', 1) <= DATE '2022-02-02 00:00:00' "
                + " and round(dfv, 'DAY', 1) >= DATE '2022-02-02 00:00:00' and round(dfv, 'DAY', 1) <= DATE '2022-02-02 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where floor(df, 'DAY', 1) >= DATE '2022-02-02 00:00:00' and floor(df, 'DAY', 1) <= DATE '2022-02-02 00:00:00' "
                + " and floor(dfv, 'DAY', 1) >= DATE '2022-02-02 00:00:00' and floor(dfv, 'DAY', 1) <= DATE '2022-02-02 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'DAY', 1) >= DATE '2022-02-03 00:00:00' and ceil(df, 'DAY', 1) <= DATE '2022-02-03 00:00:00' "
                + " and ceil(dfv, 'DAY', 1) >= DATE '2022-02-03 00:00:00' and ceil(dfv, 'DAY', 1) <= DATE '2022-02-03 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-03 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where round(df, 'DAY', 1) > DATE '2022-02-02 00:00:00' "
                + " and round(dfv, 'DAY', 1) > DATE '2022-02-02 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where floor(df, 'DAY', 1) > DATE '2022-02-02 00:00:00'"
                + " and floor(dfv, 'DAY', 1) > DATE '2022-02-02 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-20 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'DAY', 1) > DATE '2022-02-03 00:00:00' "
                + " and ceil(dfv, 'DAY', 1) > DATE '2022-02-03 00:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select df, round(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where round(df, 'DAY', 1) < DATE '2022-02-02 00:00:00' "
                + " and round(dfv, 'DAY', 1) < DATE '2022-02-02 00:00:00' ");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where floor(df, 'DAY', 1) < DATE '2022-02-02 00:00:00'"
                + " and floor(dfv, 'DAY', 1) < DATE '2022-02-02 00:00:00'");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'DAY', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'DAY', 1) < DATE '2022-02-03 00:00:00' "
                + " and ceil(dfv, 'DAY', 1) < DATE '2022-02-03 00:00:00'");
            assertFalse(rs.next());
            
            //HOURS

            rs = stmt.executeQuery("select df, round(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where round(df, 'HOUR', 1) = DATE '2022-02-02 02:00:00' "
                + " and round(dfv, 'HOUR', 1) = DATE '2022-02-02 02:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'HOUR', 10) "
                    + " from " + tableName 
                + " where round(df, 'HOUR', 10) = DATE '2022-02-02 00:00:00' "
                + " and round(dfv, 'HOUR', 10) = DATE '2022-02-02 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'HOUR', 10) "
                    + " from " + tableName 
                + "  where round(df, 'HOUR', 10) = DATE '2023-11-21 02:00:00' "
                + " and round(dfv, 'HOUR', 10) = DATE '2023-11-21 02:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-21 02:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'HOUR', 10) "
                    + " from " + tableName 
                + " where floor(df, 'HOUR', 10) = DATE '2022-02-02 00:00:00' "
                + " and floor(dfv, 'HOUR', 10) = DATE '2022-02-02 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'HOUR', 10) "
                    + " from " + tableName 
                + " where ceil(df, 'HOUR', 10) = DATE '2022-02-02 10:00:00' "
                + " and ceil(dfv, 'HOUR', 10) = DATE '2022-02-02 10:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 10:00:00.000", rs.getString(2));

            rs = stmt.executeQuery("select df, round(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where round(df, 'HOUR', 1) >= DATE '2022-02-02 02:00:00' and round(df, 'HOUR', 1) <= DATE '2022-02-02 02:00:00' "
                + " and round(dfv, 'HOUR', 1) >= DATE '2022-02-02 02:00:00' and round(dfv, 'HOUR', 1) <= DATE '2022-02-02 02:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where floor(df, 'HOUR', 1) >= DATE '2022-02-02 02:00:00' and floor(df, 'HOUR', 1) <= DATE '2022-02-02 02:00:00' "
                + " and floor(dfv, 'HOUR', 1) >= DATE '2022-02-02 02:00:00' and floor(dfv, 'HOUR', 1) <= DATE '2022-02-02 02:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'HOUR', 1) >= DATE '2022-02-02 03:00:00' and ceil(df, 'HOUR', 1) <= DATE '2022-02-02 03:00:00' "
                + " and ceil(dfv, 'HOUR', 1) >= DATE '2022-02-02 03:00:00' and ceil(dfv, 'HOUR', 1) <= DATE '2022-02-02 03:00:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 03:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where round(df, 'HOUR', 1) > DATE '2022-02-02 02:00:00' "
                + " and round(dfv, 'HOUR', 1) > DATE '2022-02-02 02:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where floor(df, 'HOUR', 1) > DATE '2022-02-02 02:00:00'"
                + " and floor(dfv, 'HOUR', 1) > DATE '2022-02-02 02:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-20 23:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'HOUR', 1) > DATE '2022-02-02 03:00:00' "
                + " and ceil(dfv, 'HOUR', 1) > DATE '2022-02-02 03:00:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select df, round(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where round(df, 'HOUR', 1) < DATE '2022-02-02 02:00:00' "
                + " and round(dfv, 'HOUR', 1) < DATE '2022-02-02 02:00:00' ");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where floor(df, 'HOUR', 1) < DATE '2022-02-02 02:00:00'"
                + " and floor(dfv, 'HOUR', 1) < DATE '2022-02-02 02:00:00'");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'HOUR', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'HOUR', 1) < DATE '2022-02-02 03:00:00' "
                + " and ceil(dfv, 'HOUR', 1) < DATE '2022-02-02 03:00:00'");
            assertFalse(rs.next());
            
            //MINUTES

            rs = stmt.executeQuery("select df, round(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) = DATE '2022-02-02 02:02:00' "
                + " and round(dfv, 'MINUTE', 1) = DATE '2022-02-02 02:02:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:00.000", rs.getString(2));
            assertFalse(rs.next());

            //We're rounding to hours since epoch here, anything else would not be idempotent
            //This is similar to what the GMT implementation does.
            rs = stmt.executeQuery("select df, round(df, 'MINUTE', 10) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 10) = DATE '2022-02-02 02:00:00' "
                + " and round(dfv, 'MINUTE', 10) = DATE '2022-02-02 02:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'MINUTE', 10) "
                    + " from " + tableName 
                + "  where round(df, 'MINUTE', 10) = DATE '2023-11-21 00:00:00' "
                + " and round(dfv, 'MINUTE', 10) = DATE '2023-11-21 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MINUTE', 10) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 10) = DATE '2022-02-02 02:00:00' "
                + " and floor(dfv, 'MINUTE', 10) = DATE '2022-02-02 02:00:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MINUTE', 10) "
                    + " from " + tableName 
                + " where ceil(df, 'MINUTE', 10) = DATE '2022-02-02 02:10:00' "
                + " and ceil(dfv, 'MINUTE', 10) = DATE '2022-02-02 02:10:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:10:00.000", rs.getString(2));

            rs = stmt.executeQuery("select df, round(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) >= DATE '2022-02-02 02:02:00' and round(df, 'MINUTE', 1) <= DATE '2022-02-02 02:02:00' "
                + " and round(dfv, 'MINUTE', 1) >= DATE '2022-02-02 02:02:00' and round(dfv, 'MINUTE', 1) <= DATE '2022-02-02 02:02:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) >= DATE '2022-02-02 02:02:00' and floor(df, 'MINUTE', 1) <= DATE '2022-02-02 02:02:00' "
                + " and floor(dfv, 'MINUTE', 1) >= DATE '2022-02-02 02:02:00' and floor(dfv, 'MINUTE', 1) <= DATE '2022-02-02 02:02:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'MINUTE', 1) >= DATE '2022-02-02 02:03:00' and ceil(df, 'MINUTE', 1) <= DATE '2022-02-02 02:03:00' "
                + " and ceil(dfv, 'MINUTE', 1) >= DATE '2022-02-02 02:03:00' and ceil(dfv, 'MINUTE', 1) <= DATE '2022-02-02 02:03:00'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:03:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) > DATE '2022-02-02 02:02:00' "
                + " and round(dfv, 'MINUTE', 1) > DATE '2022-02-02 02:02:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) > DATE '2022-02-02 02:02:00'"
                + " and floor(dfv, 'MINUTE', 1) > DATE '2022-02-02 02:02:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-20 23:59:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'MINUTE', 1) > DATE '2022-02-02 02:03:00' "
                + " and ceil(dfv, 'MINUTE', 1) > DATE '2022-02-02 02:03:00'");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select df, round(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) < DATE '2022-02-02 02:02:00' "
                + " and round(dfv, 'MINUTE', 1) < DATE '2022-02-02 02:02:00' ");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) < DATE '2022-02-02 02:00:00'"
                + " and floor(dfv, 'MINUTE', 1) < DATE '2022-02-02 02:00:00'");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'MINUTE', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'MINUTE', 1) < DATE '2022-02-02 02:03:00' "
                + " and ceil(dfv, 'MINUTE', 1) < DATE '2022-02-02 02:03:00'");
            assertFalse(rs.next());
            
            //SECONDS

            rs = stmt.executeQuery("select df, round(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where round(df, 'SECOND', 1) = DATE '2022-02-02 02:02:02' "
                + " and round(dfv, 'SECOND', 1) = DATE '2022-02-02 02:02:02'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:02.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'SECOND', 10) "
                    + " from " + tableName 
                + " where round(df, 'SECOND', 10) = DATE '2022-02-02 02:02:00' "
                + " and round(dfv, 'SECOND', 10) = DATE '2022-02-02 02:02:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, round(df, 'SECOND', 10) "
                    + " from " + tableName 
                + "  where round(df, 'SECOND', 10) = DATE '2023-11-21 00:00:00' "
                + " and round(dfv, 'SECOND', 10) = DATE '2023-11-21 00:00:00' ");
            assertTrue(rs.next());
            assertEquals("2023-11-21 00:00:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'SECOND', 10) "
                    + " from " + tableName 
                + " where floor(df, 'SECOND', 10) = DATE '2022-02-02 02:02:00' "
                + " and floor(dfv, 'SECOND', 10) = DATE '2022-02-02 02:02:00' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:00.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, ceil(df, 'SECOND', 10) "
                    + " from " + tableName 
                + " where ceil(df, 'SECOND', 10) = DATE '2022-02-02 02:02:10' "
                + " and ceil(dfv, 'SECOND', 10) = DATE '2022-02-02 02:02:10' ");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:10.000", rs.getString(2));

            rs = stmt.executeQuery("select df, round(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where round(df, 'SECOND', 1) >= DATE '2022-02-02 02:02:02' and round(df, 'SECOND', 1) <= DATE '2022-02-02 02:02:02' "
                + " and round(dfv, 'SECOND', 1) >= DATE '2022-02-02 02:02:02' and round(dfv, 'SECOND', 1) <= DATE '2022-02-02 02:02:02'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:02.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where floor(df, 'SECOND', 1) >= DATE '2022-02-02 02:02:02' and floor(df, 'SECOND', 1) <= DATE '2022-02-02 02:02:02' "
                + " and floor(dfv, 'SECOND', 1) >= DATE '2022-02-02 02:02:02' and floor(dfv, 'SECOND', 1) <= DATE '2022-02-02 02:02:02'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:02.000", rs.getString(2));
            assertFalse(rs.next());

            //when rounding from miliseconds, we always truncate.
            //This is quite inconsistent
            rs = stmt.executeQuery("select df, ceil(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'SECOND', 1) >= DATE '2022-02-02 02:02:02' and ceil(df, 'SECOND', 1) <= DATE '2022-02-02 02:02:02' "
                + " and ceil(dfv, 'SECOND', 1) >= DATE '2022-02-02 02:02:02' and ceil(dfv, 'SECOND', 1) <= DATE '2022-02-02 02:02:02'");
            assertTrue(rs.next());
            assertEquals("2022-02-02 02:02:02.000", rs.getString(2));
            assertFalse(rs.next());

            //when rounding from miliseconds, we always truncate.
            //This is quite inconsistent
            rs = stmt.executeQuery("select df, round(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where round(df, 'SECOND', 1) > DATE '2022-02-02 02:02:02' "
                + " and round(dfv, 'SECOND', 1) > DATE '2022-02-02 02:02:02' ");
            assertTrue(rs.next());
            assertEquals("2023-11-20 23:59:59.000", rs.getString(2));
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where floor(df, 'SECOND', 1) > DATE '2022-02-02 02:02:02'"
                + " and floor(dfv, 'SECOND', 1) > DATE '2022-02-02 02:02:02'");
            assertTrue(rs.next());
            assertEquals("2023-11-20 23:59:59.000", rs.getString(2));
            assertFalse(rs.next());

            //when rounding from miliseconds, we always truncate.
            //This is quite inconsistent
            rs = stmt.executeQuery("select df, ceil(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'SECOND', 1) > DATE '2022-02-02 02:02:03' "
                + " and ceil(dfv, 'SECOND', 1) > DATE '2022-02-02 02:02:03'");
            assertTrue(rs.next());
            assertEquals("2023-11-20 23:59:59.000", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select df, round(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where round(df, 'SECOND', 1) < DATE '2022-02-02 02:02:02' "
                + " and round(dfv, 'SECOND', 1) < DATE '2022-02-02 02:02:02' ");
            assertFalse(rs.next());

            rs = stmt.executeQuery("select df, floor(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where floor(df, 'SECOND', 1) < DATE '2022-02-02 02:02:02'"
                + " and floor(dfv, 'SECOND', 1) < DATE '2022-02-02 02:02:02'");
            assertFalse(rs.next());

            //when rounding from miliseconds, we always truncate.
            rs = stmt.executeQuery("select df, ceil(df, 'SECOND', 1) "
                    + " from " + tableName 
                + " where ceil(df, 'SECOND', 1) < DATE '2022-02-02 02:02:02' "
                + " and ceil(dfv, 'SECOND', 1) < DATE '2022-02-02 02:02:02'");
            assertFalse(rs.next());

        }
    }

    @Test
    public void testRoundFunctionsRowKey() throws SQLException, IOException {
        // Exercise the newKeyPart push down logic in RoundDateExpression

        String timeZone = "America/New_York";

        java.time.LocalDateTime LD_PAST = java.time.LocalDateTime.of(2000, 6, 6, 12, 0, 30, 0);
        ZoneOffset offsetPast = EST_ZONE_ID.getRules().getOffset(LD_PAST);
        java.time.Instant INSTANT_LD_PAST = LD_PAST.toInstant(offsetPast);
        
        java.time.LocalDateTime LD_FUTURE = java.time.LocalDateTime.of(2040, 6, 6, 12, 0, 30, 0);
        ZoneOffset offsetFuture = EST_ZONE_ID.getRules().getOffset(LD_FUTURE);
        java.time.Instant INSTANT_LD_FUTURE = LD_FUTURE.toInstant(offsetFuture);
        
        java.time.LocalDateTime LD_MID_MIN = java.time.LocalDateTime.of(2022, 6, 6, 12, 0, 30, 0);
        ZoneOffset offset = EST_ZONE_ID.getRules().getOffset(LD_MID_MIN);
        java.time.Instant INSTANT_MID_MIN = LD_MID_MIN.toInstant(offset);

        try (Connection conn = getConn(timeZone);
                Statement stmt = conn.createStatement())
        {

            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( "
                    + " df DATE primary key, dfv DATE) ");

            PreparedStatement ps =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " VALUES (?, ?)");

            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_LD_PAST));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_LD_PAST));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN.minusMillis(1)));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN.minusMillis(1)));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN.plusMillis(1)));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN.plusMillis(1)));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_LD_FUTURE));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_LD_FUTURE));
            ps.executeUpdate();
            conn.commit();

            ResultSet rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) = DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) = DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // MID_MIN -1
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) > DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // FUTURE
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) >= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1, FUTURE
            assertEquals(3, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) < DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) <= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN, MID_MIN +1
            assertEquals(4, rs.getInt(1));

            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(df, 'MINUTE', 1) <= DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
            
            //
            // The same, but without range scan:
            //
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(dfv, 'MINUTE', 1) = DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(dfv, 'MINUTE', 1) = DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // MID_MIN -1
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(dfv, 'MINUTE', 1) > DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // FUTURE
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(dfv, 'MINUTE', 1) >= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1, FUTURE
            assertEquals(3, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(dfv, 'MINUTE', 1) < DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(dfv, 'MINUTE', 1) <= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN, MID_MIN +1
            assertEquals(4, rs.getInt(1));

            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where round(dfv, 'MINUTE', 1) <= DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    public void testFloorFunctionsRowKey() throws SQLException, IOException {
        // Exercise the newKeyPart push down logic in RoundDateExpression

        String timeZone = "America/New_York";

        java.time.LocalDateTime LD_PAST = java.time.LocalDateTime.of(2000, 6, 6, 12, 1, 0, 0);
        ZoneOffset offsetPast = EST_ZONE_ID.getRules().getOffset(LD_PAST);
        java.time.Instant INSTANT_LD_PAST = LD_PAST.toInstant(offsetPast);
        
        java.time.LocalDateTime LD_FUTURE = java.time.LocalDateTime.of(2040, 6, 6, 12, 1, 0, 0);
        ZoneOffset offsetFuture = EST_ZONE_ID.getRules().getOffset(LD_FUTURE);
        java.time.Instant INSTANT_LD_FUTURE = LD_FUTURE.toInstant(offsetFuture);
        
        java.time.LocalDateTime LD_MID_MIN = java.time.LocalDateTime.of(2022, 6, 6, 12, 1, 0, 0);
        ZoneOffset offset = EST_ZONE_ID.getRules().getOffset(LD_MID_MIN);
        java.time.Instant INSTANT_MID_MIN = LD_MID_MIN.toInstant(offset);

        try (Connection conn = getConn(timeZone);
                Statement stmt = conn.createStatement())
        {

            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( "
                    + " df DATE primary key, dfv DATE) ");

            PreparedStatement ps =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " VALUES (?, ?)");

            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_LD_PAST));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_LD_PAST));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN.minusMillis(1)));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN.minusMillis(1)));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN.plusMillis(1)));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN.plusMillis(1)));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_LD_FUTURE));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_LD_FUTURE));
            ps.executeUpdate();
            conn.commit();

            ResultSet rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) = DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) = DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // MID_MIN -1
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) > DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // FUTURE
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) >= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1, FUTURE
            assertEquals(3, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) < DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) <= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN, MID_MIN +1
            assertEquals(4, rs.getInt(1));

            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(df, 'MINUTE', 1) <= DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
            
            //
            // The same, but without range scan:
            //
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(dfv, 'MINUTE', 1) = DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(dfv, 'MINUTE', 1) = DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // MID_MIN -1
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(dfv, 'MINUTE', 1) > DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // FUTURE
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(dfv, 'MINUTE', 1) >= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // MID_MIN, MID_MIN +1, FUTURE
            assertEquals(3, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(dfv, 'MINUTE', 1) < DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(dfv, 'MINUTE', 1) <= DATE '2022-06-06 12:01:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN, MID_MIN +1
            assertEquals(4, rs.getInt(1));

            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where floor(dfv, 'MINUTE', 1) <= DATE '2022-06-06 12:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1
            assertEquals(2, rs.getInt(1));
        }
    }
    
    @Test
    public void testCeilFunctionsRowKey() throws SQLException, IOException {
        // Exercise the newKeyPart push down logic in RoundDateExpression

        String timeZone = "America/New_York";

        java.time.LocalDateTime LD_PAST = java.time.LocalDateTime.of(2000, 6, 6, 12, 1, 0, 0);
        ZoneOffset offsetPast = EST_ZONE_ID.getRules().getOffset(LD_PAST);
        java.time.Instant INSTANT_LD_PAST = LD_PAST.toInstant(offsetPast);
        
        java.time.LocalDateTime LD_FUTURE = java.time.LocalDateTime.of(2040, 6, 6, 12, 0, 0, 0);
        ZoneOffset offsetFuture = EST_ZONE_ID.getRules().getOffset(LD_FUTURE);
        java.time.Instant INSTANT_LD_FUTURE = LD_FUTURE.toInstant(offsetFuture);
        
        java.time.LocalDateTime LD_MID_MIN = java.time.LocalDateTime.of(2022, 6, 1, 0, 0, 0, 0);
        ZoneOffset offset = EST_ZONE_ID.getRules().getOffset(LD_MID_MIN);
        java.time.Instant INSTANT_MID_MIN = LD_MID_MIN.toInstant(offset);

        try (Connection conn = getConn(timeZone);
                Statement stmt = conn.createStatement())
        {

            String tableName = generateUniqueName();
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( "
                    + " df DATE primary key, dfv DATE) ");

            PreparedStatement ps =
                    conn.prepareStatement("UPSERT INTO " + tableName
                            + " VALUES (?, ?)");

            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_LD_PAST));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_LD_PAST));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN.minusMillis(1)));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN.minusMillis(1)));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_MID_MIN.plusMillis(1)));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_MID_MIN.plusMillis(1)));
            ps.executeUpdate();
            ps.setTimestamp(1, java.sql.Timestamp.from(INSTANT_LD_FUTURE));
            ps.setTimestamp(2, java.sql.Timestamp.from(INSTANT_LD_FUTURE));
            ps.executeUpdate();
            conn.commit();

            ResultSet rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) = DATE '2022-06-01 00:00:00'");
            assertTrue(rs.next());
            // MID_MIN-1, MID_MIN
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) = DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // MID_MIN +1
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) > DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // FUTURE
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) >= DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            //MID_MIN +1, FUTURE
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) < DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN
            assertEquals(3, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) <= DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN, MID_MIN +1
            assertEquals(4, rs.getInt(1));

            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(df, 'MONTH', 1) <= DATE '2022-06-01 00:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN
            assertEquals(3, rs.getInt(1));
            
            //
            // The same, but without range scan:
            //
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(dfv, 'MONTH', 1) = DATE '2022-06-01 00:00:00'");
            assertTrue(rs.next());
            // MID_MIN-1, MID_MIN
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(dfv, 'MONTH', 1) = DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // MID_MIN +1
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(dfv, 'MONTH', 1) > DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // FUTURE
            assertEquals(1, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(dfv, 'MONTH', 1) >= DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            //MID_MIN +1, FUTURE
            assertEquals(2, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(dfv, 'MONTH', 1) < DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN
            assertEquals(3, rs.getInt(1));
            
            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(dfv, 'MONTH', 1) <= DATE '2022-07-01 00:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN, MID_MIN +1
            assertEquals(4, rs.getInt(1));

            rs = stmt.executeQuery("select count(*) "
                    + " from " + tableName 
                + " where ceil(dfv, 'MONTH', 1) <= DATE '2022-06-01 00:00:00'");
            assertTrue(rs.next());
            // PAST, MID_MIN-1, MID_MIN
            assertEquals(3, rs.getInt(1));
        }
    }
    
    @Test
    public void testContextSeparation() throws SQLException, IOException, InterruptedException {
        String tableName = generateUniqueName();

        try (Connection conn = getConn();
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + tableName + " ( ID integer PRIMARY KEY, "
                    + " TIMESTAMPFIELD TIMESTAMP," + " DATEFIELD DATE," + " TIMEFIELD TIME,"
                    + " UTIMESTAMPFIELD UNSIGNED_TIMESTAMP, "
                    + " UDATEFIELD UNSIGNED_DATE, "
                    + " UTIMEFIELD UNSIGNED_TIME, TIMESTAMPSTRINGFIELD VARCHAR, "
                    + " DATESTRINGFIELD VARCHAR, TIMESTRINGFIELD VARCHAR, "
                    + " TZ VARCHAR )");
        }

        AtomicInteger phase = new AtomicInteger();

        
        Runnable estRunnable = new SeparationRunnable(tableName, "America/New_York", phase);
        Runnable jstRunnable = new SeparationRunnable(tableName, "JST", phase);
        
        Thread t1 = new Thread(estRunnable);
        Thread t2 = new Thread(jstRunnable);
        t1.start();
        t2.start();
        
        t1.join();
        t2.join();
        
        assertFalse("Error in the test threads", contextSeparationTestFailed);
    }

    private class SeparationRunnable implements Runnable {
        private final String tableName;
        private final String timeZoneId;
        private final AtomicInteger phase;


        public SeparationRunnable(String fullTableName, String timeZoneId, AtomicInteger phase) {
            this.tableName = fullTableName;
            this.timeZoneId = timeZoneId;
            this.phase = phase;
        }

        @Override
        public void run() {
            String tsString =
                    timeZoneId.equals("America/New_York") ? TS1_EST_STR : TS1_JST_STR;
            
            String tsMsString =
                    timeZoneId.equals("America/New_York") ? TS1_EST_MS_STR : TS1_JST_MS_STR;
            
            int tzIdOffset = (timeZoneId.equals("America/New_York")) ? 0 : SEPARATION_TEST_OFFSET;
            
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(QueryServices.JDBC_COMPLIANT_TZ_HANDLING, "true");
            if (timeZoneId != null) {
                props.setProperty(QueryServices.TIMEZONE_OVERRIDE, timeZoneId);
            }
            props.setProperty(QueryServices.ENABLE_SERVER_UPSERT_SELECT, Boolean.TRUE.toString());
            
            try (Connection conn = DriverManager.getConnection(url, props);
                    Statement stmt = conn.createStatement();) {
                conn.setAutoCommit(true);

                Table hTable =
                        ((PhoenixConnection) conn).getQueryServices()
                                .getTable(TableName.valueOf(tableName).toBytes());

                for (int i = tzIdOffset; i < tzIdOffset+SEPARATION_TEST_ROWCNT; i++) {

                    // Insert the test rows 
                    stmt.executeUpdate("UPSERT INTO " + tableName
                                    + " VALUES ( "+i+",'"+tsString+"', '"+tsString+"', '"+tsString+"', "
                                    +"'"+tsString+"', '"+tsString+"', '"+tsString+"', "
                                    +"'"+tsString+"', '"+tsMsString+"', '"+tsMsString+"', '"+timeZoneId+"')");

                    checkValues(hTable, i, TS1_TS, TS1_TS_MS, TS1_TS_MS, TS1_TS, TS1_TS_MS, TS1_TS_MS);
                }

                
                //Very crude synchronization
                phase.incrementAndGet();
                while(true) {
                    if(phase.get()==2) {
                        break;
                    }
                }
                

                for (int i = tzIdOffset; i < tzIdOffset+SEPARATION_TEST_ROWCNT; i++) {
                    assertEquals(1, stmt.executeUpdate("UPSERT INTO " + tableName + " ( ID, TIMESTAMPFIELD, DATEFIELD, TIMEFIELD, "
                            + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD, TIMESTAMPSTRINGFIELD, DATESTRINGFIELD, TIMESTRINGFIELD, TZ ) "
                            + " select ID +"+ SEPARATION_TEST_ROWCNT +", TO_TIMESTAMP(DATESTRINGFIELD), TO_DATE(DATESTRINGFIELD), TO_TIME(DATESTRINGFIELD), "
                            + "  TO_TIMESTAMP(DATESTRINGFIELD), TO_DATE(DATESTRINGFIELD), TO_TIME(DATESTRINGFIELD), TIMESTAMPSTRINGFIELD, DATESTRINGFIELD, TIMESTRINGFIELD, TZ FROM " + tableName 
                            + " WHERE ID = " + i + " AND TO_CHAR(DATEFIELD) = DATESTRINGFIELD "));
                }

                //Very crude synchronization
                phase.incrementAndGet();
                while(true) {
                    if(phase.get()==4) {
                        break;
                    }
                }

                //The rows written by us
                for (int i = tzIdOffset; i < tzIdOffset+SEPARATION_TEST_ROWCNT*2; i++) {
                    ResultSet rs = stmt.executeQuery("SELECT TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                            + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                            + " TO_CHAR(TIMESTAMPFIELD), TO_CHAR(DATEFIELD), TO_CHAR(TIMEFIELD), "
                            + " TO_CHAR(UTIMESTAMPFIELD), TO_CHAR(UDATEFIELD), TO_CHAR(UTIMEFIELD)"
                            + " FROM " + tableName 
                            + " WHERE ID = " + i + " AND TO_DATE(DATESTRINGFIELD) = DATEFIELD");
                    assertTrue("Missing row for id " + i, rs.next());
                    for (int col =1; col <=6; col++) {
                        if(col % 3 == 1 && (i % SEPARATION_TEST_OFFSET < SEPARATION_TEST_ROWCNT)) {
                            // Only the original TS records have NS precision, The upsert selected
                            // records have lost the nano precision because of the TO_DATETIME bug
                            assertEquals("Bad timestamp value", rs.getTimestamp(col), TS1_TS);
                        } else {
                            assertEquals("Bad date/time value", rs.getTimestamp(col), TS1_TS_MS);
                        }
                    }
                    for (int col =1; col <=12; col++) {
                        assertTrue(rs.getString(col) + "should have been " + tsMsString, rs.getString(col).startsWith(tsMsString));
                    }
                    assertFalse("Extra rows for id " + i, rs.next());
                    rs.close();
                }
                
                //The rows written by the other thread
                String otherTz = (timeZoneId == "America/New_York") ? "JST" : "America/New_York";
                int otherTzIdOffset = (tzIdOffset == 0) ? SEPARATION_TEST_OFFSET : 0;
                for (int i = otherTzIdOffset; i < otherTzIdOffset+SEPARATION_TEST_ROWCNT*2; i++) {
                    ResultSet rs = stmt.executeQuery("SELECT TIMESTAMPFIELD, DATEFIELD, TIMEFIELD,"
                            + " UTIMESTAMPFIELD, UDATEFIELD, UTIMEFIELD,"
                            + " TO_CHAR(TIMESTAMPFIELD), TO_CHAR(DATEFIELD), TO_CHAR(TIMEFIELD), "
                            + " TO_CHAR(UTIMESTAMPFIELD), TO_CHAR(UDATEFIELD), TO_CHAR(UTIMEFIELD)"
                            + " FROM " + tableName 
                            + " WHERE ID = " + i + " AND TO_DATE(DATESTRINGFIELD, '"+QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT+"','"+otherTz+"') = DATEFIELD");
                    assertTrue("Missing row for id " + i, rs.next());
                    for (int col =1; col <=6; col++) {
                        if(col % 3 == 1 && (i % SEPARATION_TEST_OFFSET < SEPARATION_TEST_ROWCNT)) {
                            // Only the original TS records have NS precision, The upsert selected
                            // records have lost the nano precision because of the TO_DATETIME bug
                            assertEquals("Bad timestamp value", rs.getTimestamp(col), TS1_TS);
                        } else {
                            assertEquals("Bad date/time value", rs.getTimestamp(col), TS1_TS_MS);
                        }
                    }
                    //We'd get the wrong TO_CHAR result beacuse of the TZ mismatch
                    assertFalse("Extra rows for id " + i, rs.next());
                    rs.close();
                }
            } catch (Throwable e) {
                contextSeparationTestFailed = true;
                assertTrue("Error in worker "+ e.getMessage(), false);
            }

        }
    }
    
    //FIXME add tests for the addColumn Metadata op (with default value)
    
}
