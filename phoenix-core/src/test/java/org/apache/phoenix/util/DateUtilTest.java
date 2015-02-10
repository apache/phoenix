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
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
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

    @Test(expected=IllegalDataException.class)
    public void testParseTimestamp_InvalidTimestamp() {
        DateUtil.parseTimestamp("not-a-timestamp");
    }
}
