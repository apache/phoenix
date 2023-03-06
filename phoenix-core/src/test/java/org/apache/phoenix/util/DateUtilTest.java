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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TimeZone;

import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.junit.Test;

/**
 * Test class for {@link DateUtil}
 *
 * @since 2.1.3
 */
public class DateUtilTest {

    private static final long ONE_HOUR_IN_MILLIS = 1000L * 60L * 60L;
    
    @Test
    public void testDemonstrateSetNanosOnTimestampLosingMillis() {
        Timestamp ts1 = new Timestamp(120055);
        ts1.setNanos(60);
        
        Timestamp ts2 = new Timestamp(120100);
        ts2.setNanos(60);
        
        /*
         * This really should have been assertFalse() because we started with timestamps that 
         * had different milliseconds 120055 and 120100. THe problem is that the timestamp's 
         * constructor converts the milliseconds passed into seconds and assigns the left-over
         * milliseconds to the nanos part of the timestamp. If setNanos() is called after that
         * then the previous value of nanos gets overwritten resulting in loss of milliseconds.
         */
        assertTrue(ts1.equals(ts2));
        
        /*
         * The right way to deal with timestamps when you have both milliseconds and nanos to assign
         * is to use the DateUtil.getTimestamp(long millis, int nanos).
         */
        ts1 = DateUtil.getTimestamp(120055,  60);
        ts2 = DateUtil.getTimestamp(120100, 60);
        assertFalse(ts1.equals(ts2));
        assertTrue(ts2.after(ts1));
    }

    @Test
    public void testGetDateParser_DefaultTimeZone() throws ParseException {
        Date date = new Date(DateUtil.getDateTimeParser("yyyy-MM-dd", PDate.INSTANCE).parseDateTime("1970-01-01"));
        assertEquals(0, date.getTime());
    }

    @Test
    public void testGetDateParser_CustomTimeZone() throws ParseException {
        Date date = new Date(DateUtil.getDateTimeParser(
                "yyyy-MM-dd", PDate.INSTANCE, TimeZone.getTimeZone("GMT+1").getID()).parseDateTime("1970-01-01"));
        assertEquals(-ONE_HOUR_IN_MILLIS, date.getTime());
    }

    @Test
    public void testGetDateParser_LocalTimeZone() throws ParseException {
        Date date = new Date(DateUtil.getDateTimeParser(
                "yyyy-MM-dd", PDate.INSTANCE, TimeZone.getDefault().getID()).parseDateTime("1970-01-01"));
        assertEquals(Date.valueOf("1970-01-01"), date);
    }

    @Test
    public void testGetTimestampParser_DefaultTimeZone() throws ParseException {
        Timestamp ts = new Timestamp(DateUtil.getDateTimeParser("yyyy-MM-dd HH:mm:ss", PTimestamp.INSTANCE)
                .parseDateTime("1970-01-01 00:00:00"));
        assertEquals(0, ts.getTime());
    }

    @Test
    public void testGetTimestampParser_CustomTimeZone() throws ParseException {
        Timestamp ts = new Timestamp(DateUtil.getDateTimeParser("yyyy-MM-dd HH:mm:ss", PTimestamp.INSTANCE, TimeZone.getTimeZone("GMT+1").getID())
                .parseDateTime("1970-01-01 00:00:00"));
        assertEquals(-ONE_HOUR_IN_MILLIS, ts.getTime());
    }

    @Test
    public void testGetTimestampParser_LocalTimeZone() throws ParseException {
        Timestamp ts = new Timestamp(DateUtil.getDateTimeParser(
                "yyyy-MM-dd HH:mm:ss",
                PTimestamp.INSTANCE, TimeZone.getDefault().getID()).parseDateTime("1970-01-01 00:00:00"));
        assertEquals(Timestamp.valueOf("1970-01-01 00:00:00"), ts);
    }

    @Test
    public void testGetTimeParser_DefaultTimeZone() throws ParseException {
        Time time = new Time(DateUtil.getDateTimeParser("HH:mm:ss", PTime.INSTANCE).parseDateTime("00:00:00"));
        assertEquals(0, time.getTime());
    }

    @Test
    public void testGetTimeParser_CustomTimeZone() throws ParseException {
        Time time = new Time(DateUtil.getDateTimeParser(
                "HH:mm:ss",
                PTime.INSTANCE, TimeZone.getTimeZone("GMT+1").getID()).parseDateTime("00:00:00"));
        assertEquals(-ONE_HOUR_IN_MILLIS, time.getTime());
    }

    @Test
    public void testGetTimeParser_LocalTimeZone() throws ParseException {
        Time time = new Time(DateUtil.getDateTimeParser(
                "HH:mm:ss", PTime.INSTANCE, TimeZone.getDefault().getID()).parseDateTime("00:00:00"));
        assertEquals(Time.valueOf("00:00:00"), time);
    }

    @Test
    public void testParseDate() {
        assertEquals(10000L, DateUtil.parseDate("1970-01-01 00:00:10").getTime());
    }

    @Test
    public void testParseDate_PureDate() {
        assertEquals(0L, DateUtil.parseDate("1970-01-01").getTime());
    }

    @Test(expected = IllegalDataException.class)
    public void testParseDate_InvalidDate() {
        DateUtil.parseDate("not-a-date");
    }

    @Test
    public void testParseTime() {
        assertEquals(10000L, DateUtil.parseTime("1970-01-01 00:00:10").getTime());
    }

    @Test(expected=IllegalDataException.class)
    public void testParseTime_InvalidTime() {
        DateUtil.parseDate("not-a-time");
    }

    @Test
    public void testParseTimestamp() {
        assertEquals(10000L, DateUtil.parseTimestamp("1970-01-01 00:00:10").getTime());
    }

    @Test
    public void testParseTimestamp_WithMillis() {
        assertEquals(10123L, DateUtil.parseTimestamp("1970-01-01 00:00:10.123").getTime());
    }

    @Test
    public void testParseTimestamp_WithNanos() {
        assertEquals(123000000, DateUtil.parseTimestamp("1970-01-01 00:00:10.123").getNanos());

        assertEquals(123456780, DateUtil.parseTimestamp("1970-01-01 00:00:10.12345678").getNanos
                ());
        assertEquals(999999999, DateUtil.parseTimestamp("1970-01-01 00:00:10.999999999").getNanos
                ());

    }

    @Test(expected=IllegalDataException.class)
    public void testParseTimestamp_tooLargeNanos() {
        DateUtil.parseTimestamp("1970-01-01 00:00:10.9999999999");
    }

    @Test(expected=IllegalDataException.class)
    public void testParseTimestamp_missingNanos() {
        DateUtil.parseTimestamp("1970-01-01 00:00:10.");
    }
    @Test(expected=IllegalDataException.class)
    public void testParseTimestamp_negativeNanos() {
        DateUtil.parseTimestamp("1970-01-01 00:00:10.-1");
    }

    @Test(expected=IllegalDataException.class)
    public void testParseTimestamp_InvalidTimestamp() {
        DateUtil.parseTimestamp("not-a-timestamp");
    }

    // This test absolutely relies on JVM TZ being set to America/Los_Angeles,
    // and is going to fail otherwise. Maven already sets this.
    @Test
    public void testTZCorrection() {

        TimeZone tz = TimeZone.getDefault();

        // First with the current time
        LocalDateTime nowLDT = LocalDateTime.now();
        Instant nowInstantLocal = nowLDT.atZone(ZoneId.systemDefault()).toInstant();
        Instant nowInstantGMT = nowLDT.atZone(ZoneOffset.UTC).toInstant();

        java.sql.Date sqlDateNowLocal = new java.sql.Date(nowInstantLocal.toEpochMilli());
        java.sql.Time sqlTimeNowLocal = new java.sql.Time(nowInstantLocal.toEpochMilli());
        java.sql.Timestamp sqlTimestampNowLocal =
                new java.sql.Timestamp(nowInstantLocal.toEpochMilli());

        java.sql.Date sqlDateNowGMT = new java.sql.Date(nowInstantGMT.toEpochMilli());
        java.sql.Time sqlTimeNowGMT = new java.sql.Time(nowInstantGMT.toEpochMilli());
        java.sql.Timestamp sqlTimestampNowGMT =
                new java.sql.Timestamp(nowInstantGMT.toEpochMilli());

        assertEquals(DateUtil.applyInputDisplacement(sqlDateNowLocal, tz), sqlDateNowGMT);
        assertEquals(DateUtil.applyInputDisplacement(sqlTimeNowLocal, tz), sqlTimeNowGMT);
        assertEquals(DateUtil.applyInputDisplacement(sqlTimestampNowLocal, tz), sqlTimestampNowGMT);

        assertEquals(DateUtil.applyOutputDisplacement(sqlDateNowGMT, tz), sqlDateNowLocal);
        assertEquals(DateUtil.applyOutputDisplacement(sqlTimeNowGMT, tz), sqlTimeNowLocal);
        assertEquals(DateUtil.applyOutputDisplacement(sqlTimestampNowGMT, tz),
            sqlTimestampNowLocal);

        // Make sure that we don't use a fixed offset

        LocalDateTime summerLDT = LocalDateTime.of(2023, 6, 01, 10, 10, 10);
        LocalDateTime winterLDT = LocalDateTime.of(2023, 1, 01, 10, 10, 10);

        Instant summerInstantLocal = summerLDT.atZone(ZoneId.systemDefault()).toInstant();
        Instant summerInstantDisplaced = summerLDT.atZone(ZoneOffset.UTC).toInstant();

        Instant winterInstantLocal = winterLDT.atZone(ZoneId.systemDefault()).toInstant();
        Instant winterInstantDisplaced = winterLDT.atZone(ZoneOffset.UTC).toInstant();

        java.sql.Date sqlDateSummerLocal = new java.sql.Date(summerInstantLocal.toEpochMilli());
        java.sql.Time sqlTimeSummerLocal = new java.sql.Time(summerInstantLocal.toEpochMilli());
        java.sql.Timestamp sqlTimestampSummerLocal =
                new java.sql.Timestamp(summerInstantLocal.toEpochMilli());

        java.sql.Date sqlDateSummerDisplaced =
                new java.sql.Date(summerInstantDisplaced.toEpochMilli());
        java.sql.Time sqlTimeSummerDisplaced =
                new java.sql.Time(summerInstantDisplaced.toEpochMilli());
        java.sql.Timestamp sqlTimestampSummerDisplaced =
                new java.sql.Timestamp(summerInstantDisplaced.toEpochMilli());

        java.sql.Date sqlDateWinterLocal = new java.sql.Date(winterInstantLocal.toEpochMilli());
        java.sql.Time sqlTimeWinterLocal = new java.sql.Time(winterInstantLocal.toEpochMilli());
        java.sql.Timestamp sqlTimestampWinterLocal =
                new java.sql.Timestamp(winterInstantLocal.toEpochMilli());

        java.sql.Date sqlDateWinterDisplaced =
                new java.sql.Date(winterInstantDisplaced.toEpochMilli());
        java.sql.Time sqlTimeWinterDisplaced =
                new java.sql.Time(winterInstantDisplaced.toEpochMilli());
        java.sql.Timestamp sqlTimestampWinterDisplaced =
                new java.sql.Timestamp(winterInstantDisplaced.toEpochMilli());

        assertEquals(DateUtil.applyInputDisplacement(sqlDateSummerLocal, tz),
            sqlDateSummerDisplaced);
        assertEquals(DateUtil.applyInputDisplacement(sqlTimeSummerLocal, tz),
            sqlTimeSummerDisplaced);
        assertEquals(DateUtil.applyInputDisplacement(sqlTimestampSummerLocal, tz),
            sqlTimestampSummerDisplaced);

        assertEquals(DateUtil.applyOutputDisplacement(sqlDateSummerDisplaced, tz),
            sqlDateSummerLocal);
        assertEquals(DateUtil.applyOutputDisplacement(sqlTimeSummerDisplaced, tz),
            sqlTimeSummerLocal);
        assertEquals(DateUtil.applyOutputDisplacement(sqlTimestampSummerDisplaced, tz),
            sqlTimestampSummerLocal);

        assertEquals(DateUtil.applyInputDisplacement(sqlDateWinterLocal, tz),
            sqlDateWinterDisplaced);
        assertEquals(DateUtil.applyInputDisplacement(sqlTimeWinterLocal, tz),
            sqlTimeWinterDisplaced);
        assertEquals(DateUtil.applyInputDisplacement(sqlTimestampWinterLocal, tz),
            sqlTimestampWinterDisplaced);

        assertEquals(DateUtil.applyOutputDisplacement(sqlDateWinterDisplaced, tz),
            sqlDateWinterLocal);
        assertEquals(DateUtil.applyOutputDisplacement(sqlTimeWinterDisplaced, tz),
            sqlTimeWinterLocal);
        assertEquals(DateUtil.applyOutputDisplacement(sqlTimestampWinterDisplaced, tz),
            sqlTimestampWinterLocal);

        // This also demonstrates why you SHOULD NOT use the java.sql. temporal types with
        // WITHOUT TIMEZONE types.

        // Check the dates around DST switch
        ZoneId pacific = ZoneId.of("America/Los_Angeles");
        assertEquals("Test must be run in America/Los_Angeles time zone", ZoneId.systemDefault(),
            pacific);
        LocalDateTime endOfWinter = LocalDateTime.of(2023, 3, 12, 1, 59, 59);
        // There is no 2:00, the next time is 3:00
        LocalDateTime nonExistent = LocalDateTime.of(2023, 3, 12, 2, 0, 0);
        LocalDateTime startOfSummer = LocalDateTime.of(2023, 3, 12, 3, 0, 0);
        LocalDateTime endOfSummer = LocalDateTime.of(2023, 1, 05, 00, 59, 59);
        // Time warps back to 1:00 instead of reaching 2:00 the first time
        LocalDateTime ambiguous = LocalDateTime.of(2023, 1, 05, 1, 30, 0);
        LocalDateTime startOfWinter = LocalDateTime.of(2023, 1, 05, 2, 0, 0);

        java.sql.Timestamp endOfWinterLocal =
                java.sql.Timestamp.from(endOfWinter.atZone(pacific).toInstant());
        java.sql.Timestamp endOfWinterDisplaced =
                java.sql.Timestamp.from(endOfWinter.atZone(ZoneOffset.UTC).toInstant());
        assertEquals(DateUtil.applyInputDisplacement(endOfWinterLocal, tz), endOfWinterDisplaced);
        assertEquals(DateUtil.applyOutputDisplacement(endOfWinterDisplaced, tz), endOfWinterLocal);

        java.sql.Timestamp startOfSummerLocal =
                java.sql.Timestamp.from(startOfSummer.atZone(pacific).toInstant());
        java.sql.Timestamp startOfSummerDisplaced =
                java.sql.Timestamp.from(startOfSummer.atZone(ZoneOffset.UTC).toInstant());
        assertEquals(DateUtil.applyInputDisplacement(startOfSummerLocal, tz),
            startOfSummerDisplaced);
        assertEquals(DateUtil.applyOutputDisplacement(startOfSummerDisplaced, tz),
            startOfSummerLocal);

        // This just gives us 3:00
        java.sql.Timestamp nonExistentLocal =
                java.sql.Timestamp.from(nonExistent.atZone(pacific).toInstant());
        assertEquals(nonExistentLocal, startOfSummerLocal);
        java.sql.Timestamp nonExistentDisplaced =
                java.sql.Timestamp.from(nonExistent.atZone(ZoneOffset.UTC).toInstant());
        // we get a valid date
        assertEquals(DateUtil.applyInputDisplacement(nonExistentLocal, tz), startOfSummerDisplaced);
        // This conversion is ambigiuous, but in this direction we get one Local date for two
        // different displaced dates
        assertNotEquals(nonExistentDisplaced, startOfSummerDisplaced);
        assertEquals(DateUtil.applyOutputDisplacement(nonExistentDisplaced, tz), nonExistentLocal);
        assertEquals(DateUtil.applyOutputDisplacement(startOfSummerDisplaced, tz),
            nonExistentLocal);

        java.sql.Timestamp endOfSummerLocal =
                java.sql.Timestamp.from(endOfSummer.atZone(pacific).toInstant());
        java.sql.Timestamp endOfSummerDisplaced =
                java.sql.Timestamp.from(endOfSummer.atZone(ZoneOffset.UTC).toInstant());
        assertEquals(DateUtil.applyInputDisplacement(endOfSummerLocal, tz), endOfSummerDisplaced);
        assertEquals(DateUtil.applyOutputDisplacement(endOfSummerDisplaced, tz), endOfSummerLocal);

        // Confirm that we do the same thing as Java
        java.sql.Timestamp ambiguousLocal =
                java.sql.Timestamp.from(ambiguous.atZone(pacific).toInstant());
        java.sql.Timestamp ambiguousDisplaced =
                java.sql.Timestamp.from(ambiguous.atZone(ZoneOffset.UTC).toInstant());
        assertEquals(DateUtil.applyInputDisplacement(ambiguousLocal, tz), ambiguousDisplaced);
        assertEquals(DateUtil.applyOutputDisplacement(ambiguousDisplaced, tz), ambiguousLocal);

        java.sql.Timestamp startOfWinterLocal =
                java.sql.Timestamp.from(startOfWinter.atZone(pacific).toInstant());
        java.sql.Timestamp startOfWinterDisplaced =
                java.sql.Timestamp.from(startOfWinter.atZone(ZoneOffset.UTC).toInstant());
        assertEquals(DateUtil.applyInputDisplacement(startOfWinterLocal, tz),
            startOfWinterDisplaced);
        assertEquals(DateUtil.applyOutputDisplacement(startOfWinterDisplaced, tz),
            startOfWinterLocal);
    }
}
